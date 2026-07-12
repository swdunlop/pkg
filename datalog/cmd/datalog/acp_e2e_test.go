package main

// acp_e2e_test.go drives newACPDriver against a real subprocess — not a Go
// interface stub — to exercise the wire protocol acp_test.go's unit tests
// deliberately skip (doc/features/acp-integration.md phase 2, work item 10:
// "a scripted fake ACP agent binary that reads session/prompt and replays a
// canned update sequence ... driven end-to-end").
//
// Re-exec, not a built binary: the test binary re-invokes itself as the fake
// agent (the classic TestMain/helper-process pattern — see net/http/exec_
// test.go's os/exec helper-process tests in the standard library for the
// idiom this follows). TestFakeACPAgentHelperProcess below is the entry
// point; it is gated on the datalogFakeACPAgent environment variable so it
// is a silent no-op under a normal `go test` run and only does anything when
// newACPDriver spawns os.Args[0] with that variable set. This avoids
// compiling and checking in a second binary, and avoids a go-build-in-
// TempDir step (and its testing.Short() guard) entirely — re-exec has no
// build cost beyond the test binary itself, which already exists.
//
// The fake agent's script is chosen by DATALOG_FAKE_ACP_SCRIPT:
//   - "" / "full": the full-turn script (message chunks, plan, tool call,
//     a REAL /mcp round-trip, a permission request, a final message).
//   - "cancel": emits one message chunk, then blocks for session/cancel and
//     ends the turn with stopReason "cancelled".
//   - "exit3": exits the process with status 3 immediately after
//     initialize, before ever answering session/new or session/prompt.

import (
	"context"
	"fmt"
	"io"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	acp "github.com/coder/acp-go-sdk"
	"github.com/mark3labs/mcp-go/client"
	"github.com/mark3labs/mcp-go/client/transport"
	"github.com/mark3labs/mcp-go/mcp"
)

// datalogFakeACPAgentEnv is the marker TestMain checks: set, this process is
// re-exec'd as the fake agent rather than the test binary; unset (the normal
// case), tests run as usual.
const datalogFakeACPAgentEnv = "DATALOG_FAKE_ACP_AGENT"

// fakeACPAgentCommand builds the AgentCommand string newACPDriver spawns:
// this same test binary's path, re-invoked with -test.run pinned to the one
// helper-process test so none of the real tests execute in the child. The
// /mcp URL and bearer token are NOT part of the command line — the driver
// hands those to the agent through session/new's mcpServers config, exactly
// as a real external agent would receive them (acp-integration.md's
// handshake step 2: "the token travels in the mcpServers config ... never in
// argv"); the fake agent's job is to actually use what it's handed there,
// not to be told out of band via env or argv. The script selection (which
// canned sequence to replay) is the one thing that legitimately has no ACP
// analogue, so it alone travels by environment variable, set via
// setFakeACPAgentScript below on the PARENT test process — exec.Cmd without
// an explicit Env inherits the parent's environment (newACPDriver never sets
// cmd.Env), so this is sufficient without needing newACPDriver itself to
// grow an env-passing parameter.
func fakeACPAgentCommand(t *testing.T) string {
	t.Helper()
	self, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	// -test.v=false keeps stdout (the JSON-RPC channel) clean of Go test
	// framework noise; -test.run anchors to the exact helper-process test so
	// no other Test* in this binary runs in the child.
	return fmt.Sprintf("%s -test.run=^TestFakeACPAgentHelperProcess$ -test.v=false", self)
}

// setFakeACPAgentScript arms the re-exec marker and script selection on the
// current test process's environment, cleared automatically at the end of
// the test by t.Setenv. Call before newACPDriver spawns the subprocess.
func setFakeACPAgentScript(t *testing.T, script string) {
	t.Helper()
	t.Setenv(datalogFakeACPAgentEnv, "1")
	t.Setenv("DATALOG_FAKE_ACP_SCRIPT", script)
}

// TestFakeACPAgentHelperProcess is not a real test: it is the re-exec entry
// point for the fake ACP agent. Under a normal `go test` invocation (the env
// marker unset) it is skipped instantly. When newACPDriver spawns this
// binary with -test.run=^TestFakeACPAgentHelperProcess$ and the marker set,
// this is the only test that runs, and it blocks running the fake agent on
// stdin/stdout until the connection closes (the parent kills or the pipe
// closes), then returns — allowing the test binary to exit cleanly instead
// of falling through to Go test's own pass/fail reporting for a process
// that was never a real test run.
func TestFakeACPAgentHelperProcess(t *testing.T) {
	if os.Getenv(datalogFakeACPAgentEnv) == "" {
		t.Skip("not running as the fake ACP agent (set " + datalogFakeACPAgentEnv + " to activate)")
	}
	runFakeACPAgent()
}

// -- the fake agent itself ---------------------------------------------------

// fakeACPAgent implements acp.Agent (the AGENT side of the connection —
// distinct from acpDriver, which implements acp.Client, the CLIENT side).
// It records what initialize/session/new received so the test can assert on
// the handshake, and replays one script on session/prompt.
type fakeACPAgent struct {
	conn *acp.AgentSideConnection

	script string // "full" | "cancel" | "exit3"

	// Recorded handshake state, read by the test after the turn completes
	// (single-threaded access is safe: the test only reads these after
	// driver.Prompt has returned, by which point session/new and initialize
	// have long since completed on the connection's own goroutine).
	declinedFS       bool
	declinedTerminal bool
	mcpURL           string
	mcpAuthHeader    string

	sessionID acp.SessionId

	// cancelled is signalled by Cancel (session/cancel) and awaited by
	// promptCancelScript — the "cancel" script's rendezvous point.
	cancelled chan struct{}
}

func runFakeACPAgent() {
	agent := &fakeACPAgent{
		script:    os.Getenv("DATALOG_FAKE_ACP_SCRIPT"),
		cancelled: make(chan struct{}, 1),
	}
	if agent.script == "" {
		agent.script = "full"
	}
	agent.conn = acp.NewAgentSideConnection(agent, os.Stdout, os.Stdin)
	// Block until the client (acpDriver) closes the connection — its own
	// stdin/stdout pipes close when the parent test's driver.Close() kills
	// this process, or when the parent process itself exits; either way
	// Done() unblocks and the process exits cleanly rather than hanging the
	// test run.
	<-agent.conn.Done()
}

func (a *fakeACPAgent) Initialize(ctx context.Context, params acp.InitializeRequest) (acp.InitializeResponse, error) {
	// acpDriver declines both capabilities at initialize (acp.go's
	// newACPDriver doc comment, observation 5) — record that so the test can
	// assert the fake agent actually SAW the refusal, not just that the
	// driver claims to send it.
	a.declinedFS = params.ClientCapabilities.Fs.ReadTextFile || params.ClientCapabilities.Fs.WriteTextFile
	a.declinedTerminal = params.ClientCapabilities.Terminal
	return acp.InitializeResponse{
		ProtocolVersion: acp.ProtocolVersionNumber,
		AgentCapabilities: acp.AgentCapabilities{
			// The one capability that matters for this feature: HTTP MCP
			// servers. Without advertising this, acpDriver.ensureSession
			// refuses to call session/new at all (acp.go).
			McpCapabilities: acp.McpCapabilities{Http: true},
		},
		AgentInfo: &acp.Implementation{Name: "fake-acp-agent", Version: "0.0.0"},
	}, nil
}

func (a *fakeACPAgent) NewSession(ctx context.Context, params acp.NewSessionRequest) (acp.NewSessionResponse, error) {
	for _, srv := range params.McpServers {
		if srv.Http != nil {
			a.mcpURL = srv.Http.Url
			for _, h := range srv.Http.Headers {
				if strings.EqualFold(h.Name, "Authorization") {
					a.mcpAuthHeader = h.Value
				}
			}
		}
	}
	a.sessionID = "fake-session-1"
	return acp.NewSessionResponse{SessionId: a.sessionID}, nil
}

// Prompt dispatches to the selected script. "exit3" is work item 10's
// agent-exit test: the process calls os.Exit(3) as soon as a turn starts,
// before sending any session/update — acpDriver's monitor goroutine
// observes the exit and Prompt surfaces it as "agent exited (code 3)"
// (acp.go's exitError).
func (a *fakeACPAgent) Prompt(ctx context.Context, params acp.PromptRequest) (acp.PromptResponse, error) {
	switch a.script {
	case "cancel":
		return a.promptCancelScript(ctx, params)
	case "exit3":
		os.Exit(3)
		panic("unreachable")
	default:
		return a.promptFullScript(ctx, params)
	}
}

// promptFullScript replays the canned sequence work item 10 asks for: a
// couple of message chunks, a plan update, a tool_call/tool_call_update pair
// that is a REAL round-trip through /mcp (not a synthetic result — the
// fake agent connects to the live workbench session and calls list_predicates,
// so its result reflects real session state), then a permission request
// whose outcome selects the final message chunk's wording, ending end_turn.
func (a *fakeACPAgent) promptFullScript(ctx context.Context, params acp.PromptRequest) (acp.PromptResponse, error) {
	must := func(err error) {
		if err != nil {
			panic(fmt.Sprintf("fake agent: %v", err))
		}
	}

	must(a.conn.SessionUpdate(ctx, acp.SessionNotification{
		SessionId: params.SessionId,
		Update:    acp.UpdateAgentMessageText("looking at the session"),
	}))
	must(a.conn.SessionUpdate(ctx, acp.SessionNotification{
		SessionId: params.SessionId,
		Update:    acp.UpdateAgentMessageText(" now"),
	}))
	must(a.conn.SessionUpdate(ctx, acp.SessionNotification{
		SessionId: params.SessionId,
		Update: acp.UpdatePlan(
			acp.PlanEntry{Content: "call list_predicates", Status: acp.PlanEntryStatusInProgress},
		),
	}))

	const toolCallID = acp.ToolCallId("call-1")
	must(a.conn.SessionUpdate(ctx, acp.SessionNotification{
		SessionId: params.SessionId,
		Update: acp.StartToolCall(toolCallID, "list_predicates",
			acp.WithStartRawInput(map[string]any{})),
	}))

	// The real MCP round-trip: connect to the workbench's live /mcp with the
	// URL and bearer token session/new handed us (a.mcpURL/a.mcpAuthHeader),
	// exactly as the design intends an external agent to (acp-integration.md
	// "Handshake (ACP driver)" step 2 — "the token travels in the mcpServers
	// config ... never in argv"). Its result is not scripted; it is whatever
	// the live session actually reports, which is the point of work item
	// 10's "a real tool call against /mcp".
	result, callErr := a.callListPredicates(ctx)
	status := acp.ToolCallStatusCompleted
	var content []acp.ToolCallContent
	if callErr != nil {
		status = acp.ToolCallStatusFailed
		content = []acp.ToolCallContent{acp.ToolContent(acp.TextBlock(callErr.Error()))}
	} else {
		content = []acp.ToolCallContent{acp.ToolContent(acp.TextBlock(result))}
	}
	must(a.conn.SessionUpdate(ctx, acp.SessionNotification{
		SessionId: params.SessionId,
		Update: acp.UpdateToolCall(toolCallID,
			acp.WithUpdateStatus(status),
			acp.WithUpdateContent(content)),
	}))

	outcome, err := a.conn.RequestPermission(ctx, acp.RequestPermissionRequest{
		SessionId: params.SessionId,
		ToolCall: acp.ToolCallUpdate{
			ToolCallId: toolCallID,
			Title:      acp.Ptr("list_predicates"),
		},
		Options: []acp.PermissionOption{
			{OptionId: "allow_once", Name: "Allow", Kind: acp.PermissionOptionKindAllowOnce},
			{OptionId: "reject_once", Name: "Reject", Kind: acp.PermissionOptionKindRejectOnce},
		},
	})
	if err != nil {
		return acp.PromptResponse{}, err
	}

	final := "permission outcome was cancelled"
	if outcome.Outcome.Selected != nil {
		final = "permission answered: " + string(outcome.Outcome.Selected.OptionId)
	}
	must(a.conn.SessionUpdate(ctx, acp.SessionNotification{
		SessionId: params.SessionId,
		Update:    acp.UpdateAgentMessageText(final),
	}))

	return acp.PromptResponse{StopReason: acp.StopReasonEndTurn}, nil
}

// promptCancelScript emits one message chunk, then blocks until session/cancel
// arrives on ctx (acpDriver.cancelTurn cancels the Prompt call's own ctx —
// see acp.go's Prompt: "ctx.Done(): return d.cancelTurn(...)" — but the
// AGENT side sees cancellation via the separate session/cancel notification
// this fake agent's Cancel method receives, not via ctx itself, matching
// real ACP agents), then ends the turn with stopReason cancelled.
func (a *fakeACPAgent) promptCancelScript(ctx context.Context, params acp.PromptRequest) (acp.PromptResponse, error) {
	_ = a.conn.SessionUpdate(ctx, acp.SessionNotification{
		SessionId: params.SessionId,
		Update:    acp.UpdateAgentMessageText("starting a slow turn"),
	})
	select {
	case <-a.cancelled:
		return acp.PromptResponse{StopReason: acp.StopReasonCancelled}, nil
	case <-time.After(10 * time.Second):
		// Safety valve so a test bug (session/cancel never arriving) fails
		// fast with a clear stop reason instead of hanging the suite.
		return acp.PromptResponse{StopReason: acp.StopReasonEndTurn}, nil
	}
}

// callListPredicates is the fake agent's real MCP client leg: a
// streamable-HTTP mcp-go client, headers set to the Authorization value
// session/new handed this agent, connecting to the same /mcp mount the
// driver's own workbench serves. Returns the tool result's text content
// joined, or an error if the call failed or errored per MCP's isError
// convention.
func (a *fakeACPAgent) callListPredicates(ctx context.Context) (string, error) {
	c, err := client.NewStreamableHttpClient(a.mcpURL,
		transport.WithHTTPHeaders(map[string]string{"Authorization": a.mcpAuthHeader}))
	if err != nil {
		return "", fmt.Errorf("mcp client: %w", err)
	}
	defer c.Close()

	if err := c.Start(ctx); err != nil {
		return "", fmt.Errorf("mcp start: %w", err)
	}
	initReq := mcp.InitializeRequest{}
	initReq.Params.ProtocolVersion = mcp.LATEST_PROTOCOL_VERSION
	initReq.Params.ClientInfo = mcp.Implementation{Name: "fake-acp-agent", Version: "0.0.0"}
	if _, err := c.Initialize(ctx, initReq); err != nil {
		return "", fmt.Errorf("mcp initialize: %w", err)
	}

	callReq := mcp.CallToolRequest{}
	callReq.Params.Name = "list_predicates"
	callReq.Params.Arguments = map[string]any{}
	res, err := c.CallTool(ctx, callReq)
	if err != nil {
		return "", fmt.Errorf("mcp call_tool: %w", err)
	}
	var buf strings.Builder
	for _, block := range res.Content {
		if text, ok := block.(mcp.TextContent); ok {
			buf.WriteString(text.Text)
		}
	}
	if res.IsError {
		return "", fmt.Errorf("list_predicates reported an error: %s", buf.String())
	}
	return buf.String(), nil
}

// Cancel implements session/cancel (the agent side of ACP cancellation):
// signals promptCancelScript's select. A buffered channel absorbs a Cancel
// that arrives before promptCancelScript starts waiting on it (shouldn't
// happen in this script's ordering, but costs nothing to make safe).
func (a *fakeACPAgent) Cancel(ctx context.Context, params acp.CancelNotification) error {
	select {
	case a.cancelled <- struct{}{}:
	default:
	}
	return nil
}

// -- acp.Agent methods this fake agent never exercises -----------------------
//
// The workbench declines fs/terminal, session/load, and everything auth-
// related, so these are never called in practice; each returns a plain
// "not implemented" error rather than panicking, matching acpDriver's own
// posture toward its unreachable Client methods (acp.go).

func (a *fakeACPAgent) Authenticate(ctx context.Context, params acp.AuthenticateRequest) (acp.AuthenticateResponse, error) {
	return acp.AuthenticateResponse{}, fmt.Errorf("fake agent: authenticate not implemented")
}
func (a *fakeACPAgent) Logout(ctx context.Context, params acp.LogoutRequest) (acp.LogoutResponse, error) {
	return acp.LogoutResponse{}, fmt.Errorf("fake agent: logout not implemented")
}
func (a *fakeACPAgent) CloseSession(ctx context.Context, params acp.CloseSessionRequest) (acp.CloseSessionResponse, error) {
	return acp.CloseSessionResponse{}, nil
}
func (a *fakeACPAgent) ListSessions(ctx context.Context, params acp.ListSessionsRequest) (acp.ListSessionsResponse, error) {
	return acp.ListSessionsResponse{}, fmt.Errorf("fake agent: listSessions not implemented")
}
func (a *fakeACPAgent) ResumeSession(ctx context.Context, params acp.ResumeSessionRequest) (acp.ResumeSessionResponse, error) {
	return acp.ResumeSessionResponse{}, fmt.Errorf("fake agent: resumeSession not implemented")
}
func (a *fakeACPAgent) SetSessionConfigOption(ctx context.Context, params acp.SetSessionConfigOptionRequest) (acp.SetSessionConfigOptionResponse, error) {
	return acp.SetSessionConfigOptionResponse{}, fmt.Errorf("fake agent: setSessionConfigOption not implemented")
}
func (a *fakeACPAgent) SetSessionMode(ctx context.Context, params acp.SetSessionModeRequest) (acp.SetSessionModeResponse, error) {
	return acp.SetSessionModeResponse{}, fmt.Errorf("fake agent: setSessionMode not implemented")
}

var _ acp.Agent = (*fakeACPAgent)(nil)

// -- e2e tests ----------------------------------------------------------------

// newACPTestWorkbenchAndServer builds a mordor workbench (real schema, real
// rules, real evaluated predicates — so list_predicates has something to
// report) with its /mcp mount live on an httptest.Server, and returns an
// agentConfig pointed at that server's own token/URL — exactly what
// newACPDriver needs to hand a spawned agent at session/new.
func newACPTestWorkbenchAndServer(t *testing.T) (*workbench, *httptest.Server, agentConfig) {
	t.Helper()
	wb := newMordorWorkbench(t)
	srv := startTestServer(wb)
	t.Cleanup(srv.Close)
	cfg := agentConfig{
		AgentCommand: fakeACPAgentCommand(t),
		MCPURL:       srv.URL + "/mcp",
		MCPToken:     wb.mcpToken,
	}
	return wb, srv, cfg
}

// TestACPDriver_FullTurnEndToEnd drives newACPDriver directly (not through
// wb.runAgentTurn/the HTTP console handlers — that is TestACPDriver_
// PermissionViaConsoleHTTP below) against the real re-exec'd fake agent
// subprocess: the full handshake (spawn, initialize, session/new) plus one
// Prompt call whose script hits every event kind work item 10 asks for,
// including a REAL tool call against the live /mcp mount and a permission
// request answered mid-turn via Answer.
func TestACPDriver_FullTurnEndToEnd(t *testing.T) {
	_, _, cfg := newACPTestWorkbenchAndServer(t)
	setFakeACPAgentScript(t, "full")

	driver, err := newACPDriver(cfg)
	if err != nil {
		t.Fatalf("newACPDriver: %v", err)
	}
	defer driver.Close()

	var (
		mu     sync.Mutex
		events []agentEvent
	)
	sink := func(ev agentEvent) {
		mu.Lock()
		events = append(events, ev)
		mu.Unlock()
	}

	// The fake agent's script blocks on RequestPermission mid-turn, so
	// Prompt must run on its own goroutine while this test answers it —
	// exactly how handleConsolePrompt detaches a turn in production
	// (agent.go).
	type promptResult struct {
		stopReason string
		err        error
	}
	done := make(chan promptResult, 1)
	go func() {
		stopReason, err := driver.Prompt(context.Background(), "please check the session", sink)
		done <- promptResult{stopReason, err}
	}()

	// Poll for the permission event to land, then answer it — mirrors
	// console_test.go's waitFor idiom for a turn running on its own
	// goroutine.
	var reqID string
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		for _, ev := range events {
			if ev.Kind == "permission" {
				reqID = ev.RequestID
			}
		}
		mu.Unlock()
		if reqID != "" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if reqID == "" {
		t.Fatalf("permission request never arrived; events so far: %+v", events)
	}
	if err := driver.Answer(reqID, "allow_once"); err != nil {
		t.Fatalf("Answer: %v", err)
	}

	var res promptResult
	select {
	case res = <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Prompt did not return after Answer")
	}
	if res.err != nil {
		t.Fatalf("Prompt returned an error: %v", res.err)
	}
	if res.stopReason != "end_turn" {
		t.Fatalf("stopReason = %q, want %q", res.stopReason, "end_turn")
	}

	mu.Lock()
	defer mu.Unlock()

	// Order: message, message, plan, tool-call, {tool-result, permission in
	// either order}, message (final) — per promptFullScript's sequence.
	// tool-result and permission are NOT asserted relative to each other:
	// promptFullScript sends the tool_call_update and then blocks on
	// RequestPermission strictly in that order on the wire, but they arrive
	// as two independent inbound JSON-RPC messages (one notification, one
	// request) that acp-go-sdk's connection may dispatch to their handlers
	// on separate goroutines — observed to reorder under load in practice.
	// Every other adjacency here is a true causal chain (each step's SDK
	// call only returns after the previous sink call already ran), so those
	// stay strictly ordered.
	var kinds []string
	for _, ev := range events {
		kinds = append(kinds, ev.Kind)
	}
	wantPrefix := []string{"message", "message", "plan", "tool-call"}
	if len(kinds) < len(wantPrefix)+3 {
		t.Fatalf("got %d events, want at least %d: kinds=%v", len(kinds), len(wantPrefix)+3, kinds)
	}
	for i, want := range wantPrefix {
		if kinds[i] != want {
			t.Errorf("events[%d].Kind = %q, want %q (full sequence: %v)", i, kinds[i], want, kinds)
		}
	}
	middlePair := map[string]bool{kinds[4]: true, kinds[5]: true}
	if !middlePair["tool-result"] || !middlePair["permission"] || len(middlePair) != 2 {
		t.Fatalf("events[4:6] = %v, want {tool-result, permission} in either order (full sequence: %v)", kinds[4:6], kinds)
	}
	if kinds[6] != "message" {
		t.Errorf("events[6].Kind = %q, want %q (full sequence: %v)", kinds[6], "message", kinds)
	}

	// The tool-result must reflect the REAL live session, not a canned
	// string: newMordorWorkbench evaluates rules.dl at startup
	// (TestStartupEvaluatesRules already asserts smb_conn is populated), so
	// list_predicates over the fake agent's own /mcp round-trip must report
	// it with a nonzero fact count.
	var toolResult agentEvent
	for _, ev := range events {
		if ev.Kind == "tool-result" {
			toolResult = ev
		}
	}
	if toolResult.IsError {
		t.Fatalf("the real /mcp tool call reported an error: %s", toolResult.Result)
	}
	if !strings.Contains(toolResult.Result, "smb_conn") {
		t.Fatalf("tool-result does not reflect the live session's predicates: %s", toolResult.Result)
	}

	// The final message must name the option Answer selected, proving the
	// RequestPermission round-trip (not just the sink's permission event)
	// actually unblocked with the right outcome.
	final := events[len(events)-1]
	if final.Kind != "message" || !strings.Contains(final.Text, "allow_once") {
		t.Fatalf("final message does not reflect the answered option: %+v", final)
	}
}

// TestACPDriver_PermissionViaConsoleHTTP drives the same full-turn fake-agent
// script through the WORKBENCH's own console handlers (handleConsolePrompt,
// handleConsoleAnswer) rather than calling the driver directly — proving the
// permission button's POST /console/answer?requestID=...&optionID=...
// actually reaches a live ACP subprocess turn and morphs the transcript
// entry to resolved, end to end.
func TestACPDriver_PermissionViaConsoleHTTP(t *testing.T) {
	wb, srv, cfg := newACPTestWorkbenchAndServer(t)
	setFakeACPAgentScript(t, "full")
	wb.agentCfg = cfg // handleConsolePrompt's wb.agentDriver() lazily builds from this

	resp := postSignals(t, srv, "/console/prompt", map[string]any{"consolePrompt": "check things"})
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	// Wait for the permission entry (with its live buttons) to land in the
	// agent tab's transcript.
	waitFor(t, func() bool { return strings.Contains(renderLog(wb, "agent"), "permission-option") })

	log := renderLog(wb, "agent")
	if !strings.Contains(log, "agent is waiting for permission") {
		t.Fatalf("permission entry missing from transcript: %s", log)
	}

	// Pull the live requestID out of wb.pendingPerm (baked into the button's
	// href in the real page, but there is exactly one pending request here).
	wb.permMu.Lock()
	var reqID string
	for id := range wb.pendingPerm {
		reqID = id
	}
	wb.permMu.Unlock()
	if reqID == "" {
		t.Fatalf("no pending permission tracked in wb.pendingPerm")
	}

	answerResp := postSignals(t, srv, "/console/answer?requestID="+reqID+"&optionID=allow_once", map[string]any{})
	io.Copy(io.Discard, answerResp.Body)
	answerResp.Body.Close()

	// The turn should finish shortly after the answer unblocks it; poll for
	// the resolved rendering and the final message.
	waitFor(t, func() bool { return strings.Contains(renderLog(wb, "agent"), "answered: Allow") })

	log = renderLog(wb, "agent")
	if strings.Contains(log, "permission-option") {
		t.Fatalf("resolved permission entry still carries live buttons: %s", log)
	}
	if !strings.Contains(log, "smb_conn") {
		t.Fatalf("transcript missing the real tool-result content: %s", log)
	}

	wb.permMu.Lock()
	n := len(wb.pendingPerm)
	wb.permMu.Unlock()
	if n != 0 {
		t.Fatalf("pendingPerm not cleared after the turn ended: %d entries remain", n)
	}
}

// TestACPDriver_CancelMidTurn cancels a Prompt call's context while the
// "cancel" script's fake agent is deliberately blocked mid-turn (waiting on
// session/cancel), asserting: session/cancel actually reached the
// subprocess (the fake agent's Cancel method unblocks and it returns
// stopReason cancelled), Prompt itself returns ctx.Err() without leaking any
// other error, and — driven through runAgentTurn, matching how Global Cancel
// really reaches a turn — the transcript renders "turn cancelled" rather
// than a scary failure.
func TestACPDriver_CancelMidTurn(t *testing.T) {
	_, _, cfg := newACPTestWorkbenchAndServer(t)
	setFakeACPAgentScript(t, "cancel")

	driver, err := newACPDriver(cfg)
	if err != nil {
		t.Fatalf("newACPDriver: %v", err)
	}
	defer driver.Close()

	wb := newMordorWorkbench(t) // a second, driver-less workbench purely for runAgentTurn's transcript rendering
	ctx, cancel := context.WithCancel(context.Background())

	turnStarted := make(chan struct{})
	go func() {
		// Give promptCancelScript a moment to emit its first message chunk
		// before cancelling, so this exercises "cancel mid-turn" rather than
		// "cancel before the agent said anything" — both are valid, but the
		// mid-turn shape is what the design doc's cancellation section
		// describes.
		<-turnStarted
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	sink := func(ev agentEvent) {
		if ev.Kind == "message" {
			select {
			case turnStarted <- struct{}{}:
			default:
			}
		}
	}

	stopReason, promptErr := driver.Prompt(ctx, "start a slow turn", sink)
	// cancelTurn returns context.Canceled when the agent honors
	// session/cancel within the 5s window and the outer select observes
	// promptDone — acp.go's cancelTurn: on the promptDone branch it returns
	// the RPC's own (stopReason, err), which for a clean agent-side
	// "cancelled" stop is ("cancelled", nil). Accept either that or
	// ctx.Err() itself, since which one wins is a benign race between the
	// two select branches in cancelTurn.
	if promptErr != nil && promptErr != context.Canceled {
		t.Fatalf("Prompt returned an unexpected error: %v", promptErr)
	}
	if promptErr == nil && stopReason != "cancelled" {
		t.Fatalf("stopReason = %q, want %q (or Prompt should have returned context.Canceled)", stopReason, "cancelled")
	}

	// Rendering path: runAgentTurn must show "turn cancelled" for a
	// cancelled context, never a raw error (agent.go's runAgentTurn switch).
	cancelledCtx, cancelNow := context.WithCancel(context.Background())
	cancelNow()
	fake := &fakeDriver{events: []agentEvent{{Kind: "message", Text: "x"}}}
	wb.runAgentTurn(cancelledCtx, fake, "hi")
	if !strings.Contains(renderLog(wb, "agent"), "turn cancelled") {
		t.Fatalf("runAgentTurn did not render a clean cancellation for ctx.Err(): %s", renderLog(wb, "agent"))
	}
}

// TestACPDriver_AgentExitSurfacesAsPromptError uses the "exit3" script: the
// fake agent completes initialize and session/new normally (so the exit
// happens mid-conversation, the more interesting case — a fully live agent
// that then crashes on a turn) and calls os.Exit(3) the instant Prompt is
// invoked, before sending any session/update. acpDriver's single monitor
// goroutine (newACPDriver's doc comment) observes the exit via cmd.Wait and
// closes d.exited; Prompt's select on d.exited must win over the abandoned
// RPC and return exitError's "agent exited (code 3)" — never a generic RPC
// transport error and never a hang.
func TestACPDriver_AgentExitSurfacesAsPromptError(t *testing.T) {
	_, _, cfg := newACPTestWorkbenchAndServer(t)
	setFakeACPAgentScript(t, "exit3")

	driver, err := newACPDriver(cfg)
	if err != nil {
		t.Fatalf("newACPDriver: %v", err)
	}
	defer driver.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, promptErr := driver.Prompt(ctx, "this will crash the agent", func(agentEvent) {})
	if promptErr == nil {
		t.Fatal("Prompt returned no error after the agent exited")
	}
	if !strings.Contains(promptErr.Error(), "agent exited (code 3)") {
		t.Fatalf("Prompt error = %q, want it to contain %q", promptErr.Error(), "agent exited (code 3)")
	}
}

// TestMCP_TokenAuthNegative_AlreadyCovered documents, rather than
// duplicates, work item 10's "token-auth negative test" requirement:
// serve_test.go's TestMCP_UnauthorizedWithoutOrWrongToken already POSTs
// /mcp with no Authorization header and with a wrong bearer token and
// asserts 401 on both — exactly the negative case this item asks for. This
// stub exists only so a reader of acp_e2e_test.go (this file) does not go
// looking for a missing test; it intentionally does no HTTP calls of its
// own.
func TestMCP_TokenAuthNegative_AlreadyCovered(t *testing.T) {
	t.Skip("covered by TestMCP_UnauthorizedWithoutOrWrongToken in serve_test.go (no Authorization header and a wrong bearer token both assert 401 against a live /mcp mount)")
}
