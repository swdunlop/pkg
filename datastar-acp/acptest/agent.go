package acptest

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	acp "github.com/coder/acp-go-sdk"
	"github.com/mark3labs/mcp-go/client"
	"github.com/mark3labs/mcp-go/client/transport"
	"github.com/mark3labs/mcp-go/mcp"
)

// Main runs this process as the scripted ACP agent when the activate marker is
// set in the environment (as Profile arranges via the re-exec Command), then
// exits the process.  When the marker is unset it returns immediately, so a
// host that calls it unconditionally from TestMain is a no-op under a normal
// `go test` run.  Main is the reusable analogue of datalog's
// TestFakeACPAgentHelperProcess, extracted so the agent no longer needs a
// *testing.T entry point.
//
// Main never returns when it activates: it serves the agent on stdio until the
// client closes the connection, then os.Exit(0), so the re-exec'd test binary
// exits cleanly rather than falling through to the test framework's own
// reporting for a process that was never a real test run.
func Main() {
	if os.Getenv(activateEnv) == "" {
		return
	}
	script, err := decodeScript(os.Getenv(ScriptEnv))
	if err != nil {
		fmt.Fprintf(os.Stderr, "acptest: bad script: %v\n", err)
		os.Exit(2)
	}
	ag := newAgent(script)
	conn := acp.NewAgentSideConnection(ag, os.Stdout, os.Stdin)
	ag.setConn(conn)
	<-conn.Done()
	os.Exit(0)
}

// agent implements acp.Agent (the agent side of the connection, distinct from
// the chat package's driver, which is the client side).  It records the
// handshake for its own MCP callbacks and replays one Script per session/prompt.
type agent struct {
	// conn is set by setConn after NewAgentSideConnection returns; ready closes
	// then, establishing a happens-before edge for the connection's receive
	// goroutine (which reads conn only in a script replay, well after ready) so
	// the race detector sees the write synchronized with every read.
	conn  *acp.AgentSideConnection
	ready chan struct{}

	script Script

	// mcpURL/mcpAuth are captured from session/new's mcpServers config — the
	// exact URL and Authorization header the component handed the agent — and
	// used by StepMCPCall to prove the plumbing round-trips.
	mcpURL  string
	mcpAuth string

	// cancelled is signalled by session/cancel and awaited by
	// StepBlockUntilCancel.
	cancelled chan struct{}
}

func newAgent(script Script) *agent {
	return &agent{script: script, ready: make(chan struct{}), cancelled: make(chan struct{}, 1)}
}

// setConn stores the connection and unblocks any callback waiting on it.
func (a *agent) setConn(conn *acp.AgentSideConnection) {
	a.conn = conn
	close(a.ready)
}

func (a *agent) Initialize(ctx context.Context, params acp.InitializeRequest) (acp.InitializeResponse, error) {
	return acp.InitializeResponse{
		ProtocolVersion: acp.ProtocolVersionNumber,
		AgentCapabilities: acp.AgentCapabilities{
			// HTTP MCP is the one capability the chat driver's session/new path
			// requires before it offers a mount; without it ensureSession would
			// report an error instead of handing over the endpoint.
			McpCapabilities: acp.McpCapabilities{Http: true},
		},
		AgentInfo: &acp.Implementation{Name: "acptest-agent", Version: "0.0.0"},
	}, nil
}

func (a *agent) NewSession(ctx context.Context, params acp.NewSessionRequest) (acp.NewSessionResponse, error) {
	for _, srv := range params.McpServers {
		if srv.Http == nil {
			continue
		}
		a.mcpURL = srv.Http.Url
		for _, h := range srv.Http.Headers {
			if strings.EqualFold(h.Name, "Authorization") {
				a.mcpAuth = h.Value
			}
		}
	}
	return acp.NewSessionResponse{SessionId: "acptest-session-1"}, nil
}

// Prompt replays the Script.  runScript walks the steps in order; StepExit and
// StepRPCError short-circuit it.
func (a *agent) Prompt(ctx context.Context, params acp.PromptRequest) (acp.PromptResponse, error) {
	return a.runScript(ctx, params)
}

// Cancel implements session/cancel, unblocking StepBlockUntilCancel.  The
// buffered channel absorbs a cancel that races ahead of the wait.
func (a *agent) Cancel(ctx context.Context, params acp.CancelNotification) error {
	select {
	case a.cancelled <- struct{}{}:
	default:
	}
	return nil
}

// runScript executes the script's steps in order against one prompt turn.  It
// returns the turn's stop reason and error; the default terminal state is
// end_turn.
func (a *agent) runScript(ctx context.Context, params acp.PromptRequest) (acp.PromptResponse, error) {
	<-a.ready // a.conn is set (happens-before the connection's receive goroutine)
	sid := params.SessionId
	vars := map[string]string{} // branch outcomes recorded by earlier steps

	if a.script.EchoPrompt != "" {
		if err := a.emitMessage(ctx, sid, a.script.EchoPrompt+promptText(params)); err != nil {
			return acp.PromptResponse{}, err
		}
	}

	for _, step := range a.script.Steps {
		switch step.Kind {
		case StepMessage:
			if err := a.emitMessage(ctx, sid, expand(step.Text, vars)); err != nil {
				return acp.PromptResponse{}, err
			}
		case StepThought:
			if err := a.conn.SessionUpdate(ctx, acp.SessionNotification{
				SessionId: sid, Update: acp.UpdateAgentThoughtText(expand(step.Text, vars)),
			}); err != nil {
				return acp.PromptResponse{}, err
			}
		case StepPlan:
			if err := a.emitPlan(ctx, sid, step.Plan); err != nil {
				return acp.PromptResponse{}, err
			}
		case StepToolCall:
			if err := a.conn.SessionUpdate(ctx, acp.SessionNotification{
				SessionId: sid,
				Update: acp.StartToolCall(acp.ToolCallId(step.ToolCallID), step.Title,
					acp.WithStartRawInput(rawAny(step.RawInput))),
			}); err != nil {
				return acp.PromptResponse{}, err
			}
		case StepToolResult:
			if err := a.emitToolResult(ctx, sid, step); err != nil {
				return acp.PromptResponse{}, err
			}
		case StepMCPCall:
			if err := a.doMCPCall(ctx, sid, step, vars); err != nil {
				return acp.PromptResponse{}, err
			}
		case StepPermission:
			if err := a.doPermission(ctx, sid, step, vars); err != nil {
				return acp.PromptResponse{}, err
			}
		case StepBlockUntilCancel:
			select {
			case <-a.cancelled:
				return acp.PromptResponse{StopReason: acp.StopReasonCancelled}, nil
			case <-time.After(10 * time.Second):
				// Safety valve: a missing session/cancel fails the host's test
				// fast with a clear stop reason rather than hanging the suite.
				return acp.PromptResponse{StopReason: acp.StopReasonEndTurn}, nil
			}
		case StepExit:
			os.Exit(step.Code)
			panic("unreachable")
		case StepRPCError:
			msg := step.Text
			if msg == "" {
				msg = "scripted rpc error"
			}
			return acp.PromptResponse{}, acp.NewInvalidParams(msg)
		default:
			return acp.PromptResponse{}, fmt.Errorf("acptest: unknown step kind %q", step.Kind)
		}
	}
	return acp.PromptResponse{StopReason: acp.StopReasonEndTurn}, nil
}

func (a *agent) emitMessage(ctx context.Context, sid acp.SessionId, text string) error {
	return a.conn.SessionUpdate(ctx, acp.SessionNotification{
		SessionId: sid, Update: acp.UpdateAgentMessageText(text),
	})
}

func (a *agent) emitPlan(ctx context.Context, sid acp.SessionId, entries []PlanEntry) error {
	planned := make([]acp.PlanEntry, len(entries))
	for i, e := range entries {
		planned[i] = acp.PlanEntry{
			Content:  e.Content,
			Status:   acp.PlanEntryStatus(e.Status),
			Priority: acp.PlanEntryPriority(e.Priority),
		}
	}
	return a.conn.SessionUpdate(ctx, acp.SessionNotification{
		SessionId: sid, Update: acp.UpdatePlan(planned...),
	})
}

// emitToolResult sends a tool_call_update.  A sparse result carries only
// status+content, reproducing the real adapter's terminal update shape; a
// non-sparse result also restates title and rawInput.
func (a *agent) emitToolResult(ctx context.Context, sid acp.SessionId, step Step) error {
	status := acp.ToolCallStatusCompleted
	if step.Status == "failed" {
		status = acp.ToolCallStatusFailed
	}
	opts := []acp.ToolCallUpdateOpt{
		acp.WithUpdateStatus(status),
		acp.WithUpdateContent([]acp.ToolCallContent{acp.ToolContent(acp.TextBlock(step.Text))}),
	}
	if !step.Sparse {
		if step.Title != "" {
			opts = append(opts, acp.WithUpdateTitle(step.Title))
		}
		if len(step.RawInput) > 0 {
			opts = append(opts, acp.WithUpdateRawInput(rawAny(step.RawInput)))
		}
	}
	return a.conn.SessionUpdate(ctx, acp.SessionNotification{
		SessionId: sid, Update: acp.UpdateToolCall(acp.ToolCallId(step.ToolCallID), opts...),
	})
}

// doPermission issues a request_permission and records the chosen option under
// the step's branch key, so a later message can echo it.
func (a *agent) doPermission(ctx context.Context, sid acp.SessionId, step Step, vars map[string]string) error {
	opts := make([]acp.PermissionOption, len(step.Options))
	for i, o := range step.Options {
		opts[i] = acp.PermissionOption{
			OptionId: acp.PermissionOptionId(o.ID),
			Name:     o.Name,
			Kind:     acp.PermissionOptionKind(o.Kind),
		}
	}
	outcome, err := a.conn.RequestPermission(ctx, acp.RequestPermissionRequest{
		SessionId: sid,
		ToolCall:  acp.ToolCallUpdate{ToolCallId: acp.ToolCallId(step.ToolCallID), Title: acp.Ptr(step.Title)},
		Options:   opts,
	})
	if err != nil {
		return err
	}
	key := step.Branch
	if key == "" {
		key = "permission"
	}
	if outcome.Outcome.Selected != nil {
		vars[key] = string(outcome.Outcome.Selected.OptionId)
	} else {
		vars[key] = "cancelled"
	}
	return nil
}

// doMCPCall connects to the HTTP MCP endpoint session/new handed the agent,
// calls step.Tool, and emits the joined result as a tool_call_update.  A
// missing/wrong token surfaces as this call's own error (rendered as a failed
// tool result), which is exactly the plumbing this step verifies.
func (a *agent) doMCPCall(ctx context.Context, sid acp.SessionId, step Step, vars map[string]string) error {
	result, callErr := a.callMCP(ctx, step.Tool, rawAny(step.RawInput))
	failStep := step
	if callErr != nil {
		failStep.Status = "failed"
		failStep.Text = callErr.Error()
	} else {
		failStep.Text = result
		if step.StoreResultAs != "" {
			vars[step.StoreResultAs] = result
		}
	}
	// The initial tool_call carried title+rawInput (a StepToolCall precedes this
	// in practice); the result restates neither, matching a real adapter — the
	// driver's toolState fills them.  Force sparse so this exercises that path.
	failStep.Sparse = true
	return a.emitToolResult(ctx, sid, failStep)
}

// callMCP is the agent's MCP client leg: a streamable-HTTP mcp-go client with
// the Authorization header session/new handed it, calling one tool and joining
// its text content.
func (a *agent) callMCP(ctx context.Context, tool string, args any) (string, error) {
	if a.mcpURL == "" {
		return "", fmt.Errorf("no MCP endpoint was offered at session/new")
	}
	c, err := client.NewStreamableHttpClient(a.mcpURL,
		transport.WithHTTPHeaders(map[string]string{"Authorization": a.mcpAuth}))
	if err != nil {
		return "", fmt.Errorf("mcp client: %w", err)
	}
	defer c.Close()
	if err := c.Start(ctx); err != nil {
		return "", fmt.Errorf("mcp start: %w", err)
	}
	initReq := mcp.InitializeRequest{}
	initReq.Params.ProtocolVersion = mcp.LATEST_PROTOCOL_VERSION
	initReq.Params.ClientInfo = mcp.Implementation{Name: "acptest-agent", Version: "0.0.0"}
	if _, err := c.Initialize(ctx, initReq); err != nil {
		return "", fmt.Errorf("mcp initialize: %w", err)
	}
	callReq := mcp.CallToolRequest{}
	callReq.Params.Name = tool
	callReq.Params.Arguments = args
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
		return "", fmt.Errorf("tool %q reported an error: %s", tool, buf.String())
	}
	return buf.String(), nil
}

// --- acp.Agent methods the scripted agent never exercises ------------------
//
// The chat driver declines fs/terminal and never authenticates, loads, or
// resumes a session, so these are unreachable in practice; each returns a plain
// error rather than panicking.

func (a *agent) Authenticate(ctx context.Context, params acp.AuthenticateRequest) (acp.AuthenticateResponse, error) {
	return acp.AuthenticateResponse{}, fmt.Errorf("acptest: authenticate not implemented")
}
func (a *agent) Logout(ctx context.Context, params acp.LogoutRequest) (acp.LogoutResponse, error) {
	return acp.LogoutResponse{}, fmt.Errorf("acptest: logout not implemented")
}
func (a *agent) CloseSession(ctx context.Context, params acp.CloseSessionRequest) (acp.CloseSessionResponse, error) {
	return acp.CloseSessionResponse{}, nil
}
func (a *agent) ListSessions(ctx context.Context, params acp.ListSessionsRequest) (acp.ListSessionsResponse, error) {
	return acp.ListSessionsResponse{}, fmt.Errorf("acptest: listSessions not implemented")
}
func (a *agent) ResumeSession(ctx context.Context, params acp.ResumeSessionRequest) (acp.ResumeSessionResponse, error) {
	return acp.ResumeSessionResponse{}, fmt.Errorf("acptest: resumeSession not implemented")
}
func (a *agent) SetSessionConfigOption(ctx context.Context, params acp.SetSessionConfigOptionRequest) (acp.SetSessionConfigOptionResponse, error) {
	return acp.SetSessionConfigOptionResponse{}, fmt.Errorf("acptest: setSessionConfigOption not implemented")
}
func (a *agent) SetSessionMode(ctx context.Context, params acp.SetSessionModeRequest) (acp.SetSessionModeResponse, error) {
	return acp.SetSessionModeResponse{}, fmt.Errorf("acptest: setSessionMode not implemented")
}

var _ acp.Agent = (*agent)(nil)

// --- small helpers ---------------------------------------------------------

// promptText joins a prompt request's text content blocks, used by EchoPrompt.
func promptText(params acp.PromptRequest) string {
	var b strings.Builder
	for _, blk := range params.Prompt {
		if blk.Text != nil {
			b.WriteString(blk.Text.Text)
		}
	}
	return b.String()
}

// expand substitutes {{key}} placeholders in text with recorded branch values.
func expand(text string, vars map[string]string) string {
	for k, v := range vars {
		text = strings.ReplaceAll(text, "{{"+k+"}}", v)
	}
	return text
}

// rawAny unmarshals a step's JSON rawInput into the any-typed argument ACP and
// mcp-go expect; an empty raw input becomes an empty object.
func rawAny(raw []byte) any {
	if len(raw) == 0 {
		return map[string]any{}
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return map[string]any{}
	}
	return v
}
