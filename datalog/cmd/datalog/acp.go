package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	acp "github.com/coder/acp-go-sdk"
)

// acpDriver wraps github.com/coder/acp-go-sdk's client side around a
// spawned agent subprocess (doc/features/acp-integration.md phase 2, work
// item 8): stdin/stdout carry JSON-RPC, stderr passes through to serve's
// own stderr so a misbehaving agent's diagnostics are visible rather than
// swallowed. One ACP session is created lazily on the first Prompt and
// reused for the driver's lifetime — the design's "created/spawned lazily
// ... recreated ... after an exit or fatal error" is handled one layer up,
// by workbench.dropAgentDriver discarding the whole driver (subprocess and
// all) on a fatal turn error, so a fresh acpDriver/subprocess/session is
// built together on the next prompt.
type acpDriver struct {
	cmd  *exec.Cmd
	conn *acp.ClientSideConnection

	agentCaps acp.AgentCapabilities // recorded at Initialize

	// exited closes once the subprocess has been waited on by the single
	// monitor goroutine started in newACPDriver (exec.Cmd.Wait's contract
	// permits exactly one caller per process); exitErr and exitCode are
	// written before the close, so any goroutine that observes <-exited
	// closed is guaranteed to see both fields correctly per the memory
	// model's happens-before rule for channel closes. Prompt and cancelTurn
	// both select on this channel and share the exitError helper below
	// rather than each racing their own Wait call.
	exited   chan struct{}
	exitErr  error
	exitCode int

	mcpURL   string
	mcpToken string

	mu        sync.Mutex
	sessionID acp.SessionId
	haveSess  bool

	// pending parks in-flight session/request_permission RPCs keyed by a
	// driver-generated request ID, so Answer (called from the pane's POST
	// handler, on an entirely different goroutine than the SDK's read loop)
	// can hand the waiting RPC its outcome. See RequestPermission below.
	pendingMu sync.Mutex
	pending   map[string]chan acp.RequestPermissionOutcome

	// sink is the current turn's event target, valid only while a Prompt
	// call is in flight; acpClient's callbacks (arriving on the connection's
	// own goroutine, per the SDK's contract) read it under mu. There is only
	// ever one turn at a time (the agentDriver interface's contract), so a
	// single field suffices rather than a per-call registry.
	sink func(agentEvent)

	nextReqID uint64
}

// newACPDriver spawns cfg.AgentCommand (split shell-style — a simple
// strings.Fields split is enough for v1; operators needing quoting/
// escaping should reach for a wrapper script, noted here rather than
// pulling in a shell-lexer dependency for phase 2) and performs the ACP
// handshake: initialize with fs/terminal capabilities declined (observation
// 5 — the agent's only lever on the workspace is the MCP tools), recording
// the agent's capabilities for the session/new call that Prompt makes
// lazily on first use.
func newACPDriver(cfg agentConfig) (*acpDriver, error) {
	fields := strings.Fields(cfg.AgentCommand)
	if len(fields) == 0 {
		return nil, fmt.Errorf("--agent command is empty")
	}

	cmd := exec.Command(fields[0], fields[1:]...)
	cmd.Stderr = os.Stderr // visible diagnostics, never swallowed

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("agent stdin pipe: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("agent stdout pipe: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("starting agent %q: %w", cfg.AgentCommand, err)
	}

	d := &acpDriver{
		cmd:      cmd,
		exited:   make(chan struct{}),
		mcpURL:   cfg.MCPURL,
		mcpToken: cfg.MCPToken,
		pending:  map[string]chan acp.RequestPermissionOutcome{},
	}
	d.conn = acp.NewClientSideConnection(d, stdin, stdout)

	// The one and only Wait call for this subprocess (exec.Cmd.Wait must be
	// called exactly once per process). Started here, immediately after
	// Start, rather than per-turn in Prompt, so a mid-turn crash on any turn
	// — not just the first — is observed and unblocks whichever of
	// Prompt/cancelTurn is currently selecting on d.exited.
	go func() {
		err := cmd.Wait()
		d.exitErr = err
		if cmd.ProcessState != nil {
			d.exitCode = cmd.ProcessState.ExitCode()
		}
		close(d.exited)
	}()

	initCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	resp, err := d.conn.Initialize(initCtx, acp.InitializeRequest{
		ProtocolVersion: acp.ProtocolVersionNumber,
		ClientCapabilities: acp.ClientCapabilities{
			// Capability refusal per observation 5: the agent's only lever
			// on the workspace is the session's MCP tools, never direct
			// file or terminal access through the client.
			Fs:       acp.FileSystemCapabilities{ReadTextFile: false, WriteTextFile: false},
			Terminal: false,
		},
		ClientInfo: &acp.Implementation{Name: "datalog-serve", Version: "0.1.0"},
	})
	if err != nil {
		_ = cmd.Process.Kill()
		return nil, fmt.Errorf("agent initialize: %w", err)
	}
	d.agentCaps = resp.AgentCapabilities
	return d, nil
}

// ensureSession creates the driver's one ACP session on first use, per the
// design's "ONE ACP session per driver" — the whole conversation, across
// every Prompt call this driver serves, is one session/new. cwd is the
// server process's own working directory (acp-integration.md's handshake
// step 3): the workbench is not a coding workspace, so there is no more
// specific project root to offer.
func (d *acpDriver) ensureSession(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.haveSess {
		return nil
	}
	if !d.agentCaps.McpCapabilities.Http {
		// The proxy shim (datalog mcp --proxy <url>, acp-integration.md
		// work item 7) is a separate task; until it lands, an agent without
		// HTTP MCP support cannot reach the workbench's session at all.
		return fmt.Errorf("agent does not support HTTP MCP servers, and the stdio proxy shim (datalog mcp --proxy) is not yet implemented")
	}
	cwd, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("resolving cwd for agent session: %w", err)
	}
	resp, err := d.conn.NewSession(ctx, acp.NewSessionRequest{
		Cwd: cwd,
		McpServers: []acp.McpServer{
			{Http: &acp.McpServerHttpInline{
				Name: "datalog",
				Url:  d.mcpURL,
				Headers: []acp.HttpHeader{
					{Name: "Authorization", Value: "Bearer " + d.mcpToken},
				},
			}},
		},
	})
	if err != nil {
		return fmt.Errorf("agent session/new: %w", err)
	}
	d.sessionID = resp.SessionId
	d.haveSess = true
	return nil
}

// Prompt sends one user turn and blocks until the agent ends it, mapping
// every session/update notification into an agentEvent via mapSessionUpdate
// as it arrives (SDK callbacks fire on the connection's read-loop
// goroutine, so sink itself must be safe to call concurrently with
// whatever else that goroutine is doing — runAgentTurn's sink is
// mutex-guarded, satisfying that).
func (d *acpDriver) Prompt(ctx context.Context, text string, sink func(agentEvent)) (string, error) {
	if err := d.ensureSession(ctx); err != nil {
		return "", err
	}

	d.mu.Lock()
	d.sink = sink
	sessionID := d.sessionID
	d.mu.Unlock()
	defer func() {
		d.mu.Lock()
		d.sink = nil
		d.mu.Unlock()
	}()

	// d.exited fires if the subprocess dies mid-turn — surfaced as a Prompt
	// error so runAgentTurn's dropAgentDriver path fires and the next
	// prompt respawns a fresh subprocess (per the design's "a subprocess
	// exit surfaces as a Prompt error ... so that path fires"). The wait
	// itself happened on the single monitor goroutine started in
	// newACPDriver; this just reads its recorded outcome.
	promptDone := make(chan promptResult, 1)
	go func() {
		resp, err := d.conn.Prompt(ctx, acp.PromptRequest{
			SessionId: sessionID,
			Prompt:    []acp.ContentBlock{acp.TextBlock(text)},
		})
		promptDone <- promptResult{resp, err}
	}()

	select {
	case r := <-promptDone:
		if r.err != nil {
			return "", r.err
		}
		return string(r.resp.StopReason), nil
	case <-d.exited:
		return "", d.exitError()
	case <-ctx.Done():
		return d.cancelTurn(sessionID, promptDone)
	}
}

// exitError formats the subprocess's recorded exit outcome (set once, by
// the single monitor goroutine started in newACPDriver) as the "agent
// exited (code N)" error shared by Prompt and cancelTurn — both select on
// d.exited and want the same message.
func (d *acpDriver) exitError() error {
	if d.exitErr == nil {
		return fmt.Errorf("agent exited (code %d)", d.exitCode)
	}
	return fmt.Errorf("agent exited (code %d): %w", d.exitCode, d.exitErr)
}

// promptResult carries one Prompt RPC's outcome across goroutines — a
// named type rather than an inline struct so cancelTurn's signature stays
// readable.
type promptResult struct {
	resp acp.PromptResponse
	err  error
}

// cancelTurn implements the design's cancellation sequence: send
// session/cancel and give the agent 5 seconds to end the turn with stop
// reason "cancelled"; if it wedges, kill the subprocess (the doc's "kill
// timer"). Any pending permission requests are also resolved with the
// cancelled outcome here via cancelPending, matching ACP's requirement that
// a client cancelling a turn must answer every outstanding
// request_permission with Cancelled.
func (d *acpDriver) cancelTurn(sessionID acp.SessionId, promptDone <-chan promptResult) (string, error) {
	d.cancelPending()

	cancelCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = d.conn.Cancel(cancelCtx, acp.CancelNotification{SessionId: sessionID})

	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case r := <-promptDone:
		if r.err != nil {
			return "", r.err
		}
		return string(r.resp.StopReason), nil
	case <-d.exited:
		return "", d.exitError()
	case <-timer.C:
		// The subprocess ignored session/cancel — kill it outright (the
		// doc's kill timer). The next prompt respawns a fresh driver via
		// dropAgentDriver, since this Prompt call returns ctx.Err() and
		// runAgentTurn's caller checks ctx.Err() before the error branch,
		// rendering "turn cancelled" rather than a scary error — but the
		// subprocess is still gone, so the *next* prompt must fail loudly.
		// Killing here (rather than leaving it to Close) ensures that
		// failure happens promptly instead of leaving a wedged process
		// running unbounded.
		_ = d.cmd.Process.Kill()
		return "", context.Canceled
	}
}

// cancelPending resolves every outstanding permission request with the
// Cancelled outcome and empties the map, per ACP's cancellation contract
// (RequestPermissionOutcome.Cancelled's doc: "When a client sends a
// session/cancel notification ... it MUST respond to all pending
// session/request_permission requests with this Cancelled outcome").
func (d *acpDriver) cancelPending() {
	d.pendingMu.Lock()
	defer d.pendingMu.Unlock()
	for id, ch := range d.pending {
		ch <- acp.NewRequestPermissionOutcomeCancelled()
		delete(d.pending, id)
	}
}

// Answer resolves a pending permission request (agent.go's agentDriver
// interface) by handing the parked RequestPermission call its outcome.
// Unknown requestID is an error, per the interface's contract.
func (d *acpDriver) Answer(requestID, optionID string) error {
	d.pendingMu.Lock()
	ch, ok := d.pending[requestID]
	if ok {
		delete(d.pending, requestID)
	}
	d.pendingMu.Unlock()
	if !ok {
		return fmt.Errorf("no pending permission request %q", requestID)
	}
	ch <- acp.NewRequestPermissionOutcomeSelected(acp.PermissionOptionId(optionID))
	return nil
}

// Close terminates the subprocess (doc/features/acp-integration.md
// "Cancellation": "Close() terminates the subprocess"). SIGTERM's Go
// equivalent (Process.Kill, i.e. SIGKILL) is used directly rather than a
// graceful-then-forceful sequence — Close is only called after the driver
// is already being discarded (dropAgentDriver, Clear), so there is no
// in-flight turn whose graceful stop reason matters anymore.
func (d *acpDriver) Close() error {
	if d.cmd.Process == nil {
		return nil
	}
	return d.cmd.Process.Kill()
}

// --- acp.Client implementation -------------------------------------------
//
// acpDriver itself is the Client the SDK's ClientSideConnection calls back
// into (SessionUpdate for streamed turn events, RequestPermission for
// approval gates). The fs/terminal methods are unreachable in practice —
// initialize declined every capability that would cause the agent to call
// them — but the Client interface requires implementations, so each
// returns a clear "not supported" error rather than a nil-pointer panic if
// a noncompliant agent calls one anyway.

func (d *acpDriver) ReadTextFile(ctx context.Context, params acp.ReadTextFileRequest) (acp.ReadTextFileResponse, error) {
	return acp.ReadTextFileResponse{}, fmt.Errorf("fs.readTextFile was declined at initialize")
}

func (d *acpDriver) WriteTextFile(ctx context.Context, params acp.WriteTextFileRequest) (acp.WriteTextFileResponse, error) {
	return acp.WriteTextFileResponse{}, fmt.Errorf("fs.writeTextFile was declined at initialize")
}

func (d *acpDriver) CreateTerminal(ctx context.Context, params acp.CreateTerminalRequest) (acp.CreateTerminalResponse, error) {
	return acp.CreateTerminalResponse{}, fmt.Errorf("terminal capability was declined at initialize")
}

func (d *acpDriver) KillTerminal(ctx context.Context, params acp.KillTerminalRequest) (acp.KillTerminalResponse, error) {
	return acp.KillTerminalResponse{}, fmt.Errorf("terminal capability was declined at initialize")
}

func (d *acpDriver) TerminalOutput(ctx context.Context, params acp.TerminalOutputRequest) (acp.TerminalOutputResponse, error) {
	return acp.TerminalOutputResponse{}, fmt.Errorf("terminal capability was declined at initialize")
}

func (d *acpDriver) ReleaseTerminal(ctx context.Context, params acp.ReleaseTerminalRequest) (acp.ReleaseTerminalResponse, error) {
	return acp.ReleaseTerminalResponse{}, fmt.Errorf("terminal capability was declined at initialize")
}

func (d *acpDriver) WaitForTerminalExit(ctx context.Context, params acp.WaitForTerminalExitRequest) (acp.WaitForTerminalExitResponse, error) {
	return acp.WaitForTerminalExitResponse{}, fmt.Errorf("terminal capability was declined at initialize")
}

// SessionUpdate is the SDK's per-notification callback — one call per
// session/update the agent sends during a Prompt call. It maps the update
// to an agentEvent via mapSessionUpdate and forwards to the turn's sink, if
// a turn is currently in flight (a notification arriving with no active
// Prompt would be a protocol violation by the agent; silently dropping is
// safer than panicking on a nil sink).
func (d *acpDriver) SessionUpdate(ctx context.Context, params acp.SessionNotification) error {
	d.mu.Lock()
	sink := d.sink
	d.mu.Unlock()
	if sink == nil {
		return nil
	}
	if ev, ok := mapSessionUpdate(params.Update); ok {
		sink(ev)
	}
	return nil
}

// RequestPermission implements session/request_permission: it parks a
// channel keyed by a freshly generated request ID, emits a "permission"
// agentEvent carrying that ID so the pane (and, in a future task, Answer's
// caller) can respond, and blocks until Answer resolves it or the RPC's own
// ctx is cancelled (which happens when the client tears down the
// connection, e.g. Close during a wedged turn).
func (d *acpDriver) RequestPermission(ctx context.Context, params acp.RequestPermissionRequest) (acp.RequestPermissionResponse, error) {
	d.mu.Lock()
	d.nextReqID++
	reqID := fmt.Sprintf("perm-%d", d.nextReqID)
	sink := d.sink
	d.mu.Unlock()

	ch := make(chan acp.RequestPermissionOutcome, 1)
	d.pendingMu.Lock()
	d.pending[reqID] = ch
	d.pendingMu.Unlock()

	if sink != nil {
		sink(permissionEvent(reqID, params))
	}

	select {
	case outcome := <-ch:
		return acp.RequestPermissionResponse{Outcome: outcome}, nil
	case <-ctx.Done():
		d.pendingMu.Lock()
		delete(d.pending, reqID)
		d.pendingMu.Unlock()
		return acp.RequestPermissionResponse{Outcome: acp.NewRequestPermissionOutcomeCancelled()}, nil
	}
}

// permissionEvent builds the "permission" agentEvent for one
// RequestPermission call — factored out of the RPC method so it can be
// unit-tested without a live connection.
func permissionEvent(reqID string, params acp.RequestPermissionRequest) agentEvent {
	opts := make([]agentOption, len(params.Options))
	for i, o := range params.Options {
		opts[i] = agentOption{ID: string(o.OptionId), Name: o.Name, Kind: string(o.Kind)}
	}
	return agentEvent{
		Kind:       "permission",
		RequestID:  reqID,
		ToolCallID: string(params.ToolCall.ToolCallId),
		ToolName:   toolCallTitle(strPtrOrEmpty(params.ToolCall.Title), params.ToolCall.ToolCallId),
		ToolArgs:   rawInputJSON(params.ToolCall.RawInput),
		Options:    opts,
	}
}

// mapSessionUpdate maps one ACP SessionUpdate into an agentEvent, per
// acp-integration.md's Prompt mapping: agent_message_chunk → "message",
// agent_thought_chunk → "thought", tool_call → "tool-call", tool_call_update
// with a terminal status → "tool-result", plan → "plan". Anything else
// falls through to whatever text it carries rather than being dropped
// silently (the design's "pass through anything text-bearing"); updates
// with nothing renderable (mode changes, config updates, ...) report ok =
// false so the caller emits nothing. Factored out of SessionUpdate so the
// mapping is unit-testable against synthetic updates without a live
// subprocess.
func mapSessionUpdate(u acp.SessionUpdate) (agentEvent, bool) {
	switch {
	case u.AgentMessageChunk != nil:
		return agentEvent{Kind: "message", Text: contentBlockText(u.AgentMessageChunk.Content)}, true
	case u.AgentThoughtChunk != nil:
		return agentEvent{Kind: "thought", Text: contentBlockText(u.AgentThoughtChunk.Content)}, true
	case u.ToolCall != nil:
		tc := u.ToolCall
		return agentEvent{
			Kind:       "tool-call",
			ToolCallID: string(tc.ToolCallId),
			ToolName:   toolCallTitle(tc.Title, tc.ToolCallId),
			ToolArgs:   rawInputJSON(tc.RawInput),
		}, true
	case u.ToolCallUpdate != nil:
		tu := u.ToolCallUpdate
		status := acp.ToolCallStatusInProgress
		if tu.Status != nil {
			status = *tu.Status
		}
		if status != acp.ToolCallStatusCompleted && status != acp.ToolCallStatusFailed {
			// Non-terminal updates (pending → in_progress, content
			// streaming in) don't map to a "tool-result" — the design
			// specifies terminal status only. Nothing else about a
			// non-terminal update is currently rendered.
			return agentEvent{}, false
		}
		ev := agentEvent{
			Kind:       "tool-result",
			ToolCallID: string(tu.ToolCallId),
			IsError:    status == acp.ToolCallStatusFailed,
		}
		if tu.Title != nil {
			ev.ToolName = *tu.Title
		}
		ev.ToolArgs = rawInputJSON(tu.RawInput)
		if result := toolCallContentText(tu.Content); result != "" {
			ev.Result = result
		} else {
			ev.Result = rawOutputJSON(tu.RawOutput)
		}
		return ev, true
	case u.Plan != nil:
		entries := make([]agentPlanEntry, len(u.Plan.Entries))
		for i, e := range u.Plan.Entries {
			entries[i] = agentPlanEntry{Content: e.Content, Status: string(e.Status)}
		}
		return agentEvent{Kind: "plan", PlanEntries: entries}, true
	case u.UserMessageChunk != nil:
		// The client is the one who sent the user message; an agent
		// echoing it back is rare but not text we want to drop per the
		// "pass through anything text-bearing" rule.
		if text := contentBlockText(u.UserMessageChunk.Content); text != "" {
			return agentEvent{Kind: "message", Text: text}, true
		}
		return agentEvent{}, false
	default:
		return agentEvent{}, false
	}
}

// strPtrOrEmpty dereferences an optional string field (ACP represents
// several "unset means empty" fields as *string), returning "" for nil.
func strPtrOrEmpty(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// toolCallTitle prefers the human-readable title ACP provides; falling
// back to the raw tool-call ID keeps the pane's summary line non-empty
// even for a minimal agent that omits titles.
func toolCallTitle(title string, id acp.ToolCallId) string {
	if title != "" {
		return title
	}
	return string(id)
}

// contentBlockText extracts the display text from a ContentBlock — message
// and thought chunks are baseline-required to support ContentBlockText
// (ContentBlockText's doc comment), so non-text variants (image/audio/
// resource) are rare in this position; they render as empty rather than
// erroring, consistent with "never drop a turn over an unexpected content
// shape".
func contentBlockText(c acp.ContentBlock) string {
	if c.Text != nil {
		return c.Text.Text
	}
	return ""
}

// toolCallContentText joins a tool call's content blocks into one string
// for the console's result rendering — the pane already knows how to
// pretty-print JSON and prose (agent.go's formatToolResult/toolResultBody),
// so plain text is enough; diffs and terminal refs are rarer tool-call
// shapes not rendered specially here.
func toolCallContentText(blocks []acp.ToolCallContent) string {
	var parts []string
	for _, b := range blocks {
		if b.Content != nil {
			if t := contentBlockText(b.Content.Content); t != "" {
				parts = append(parts, t)
			}
		}
	}
	return strings.Join(parts, "\n")
}

// rawInputJSON and rawOutputJSON re-marshal ACP's `any`-typed raw
// input/output into the JSON string agentEvent.ToolArgs/Result already
// expect (kitDriver's fields are JSON strings too, per its ToolArgs/Result
// doc comments) — nil marshals to "" rather than the literal "null" so the
// pane's "no args" rendering stays empty.
func rawInputJSON(v any) string  { return marshalOrEmpty(v) }
func rawOutputJSON(v any) string { return marshalOrEmpty(v) }

func marshalOrEmpty(v any) string {
	if v == nil {
		return ""
	}
	b, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	return string(b)
}

var _ acp.Client = (*acpDriver)(nil)
var _ io.Closer = (*acpDriver)(nil)
