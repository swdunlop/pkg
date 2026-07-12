package main

import (
	"os"
	"testing"

	acp "github.com/coder/acp-go-sdk"
)

// TestMapSessionUpdate feeds synthetic ACP session/update notifications
// through mapSessionUpdate — the pure mapping function acpDriver.
// SessionUpdate delegates to — and checks the resulting agentEvent against
// acp-integration.md's Prompt mapping table (work item 8): message chunk →
// "message", thought chunk → "thought", tool_call → "tool-call",
// tool_call_update with a terminal status → "tool-result", plan → "plan",
// and a non-terminal tool_call_update produces nothing.
func TestMapSessionUpdate(t *testing.T) {
	cases := []struct {
		name string
		in   acp.SessionUpdate
		want agentEvent
		ok   bool
	}{
		{
			name: "message chunk",
			in:   acp.UpdateAgentMessageText("hello"),
			want: agentEvent{Kind: "message", Text: "hello"},
			ok:   true,
		},
		{
			name: "thought chunk",
			in:   acp.UpdateAgentThoughtText("thinking..."),
			want: agentEvent{Kind: "thought", Text: "thinking..."},
			ok:   true,
		},
		{
			name: "tool call start",
			in: acp.StartToolCall("call-1", "query",
				acp.WithStartRawInput(map[string]any{"query": "foo(X)?"})),
			want: agentEvent{
				Kind:       "tool-call",
				ToolCallID: "call-1",
				ToolName:   "query",
				ToolArgs:   `{"query":"foo(X)?"}`,
			},
			ok: true,
		},
		{
			name: "tool call update in progress is not a result",
			in: acp.UpdateToolCall("call-1",
				acp.WithUpdateStatus(acp.ToolCallStatusInProgress)),
			ok: false,
		},
		{
			name: "tool call update completed",
			in: acp.UpdateToolCall("call-1",
				acp.WithUpdateStatus(acp.ToolCallStatusCompleted),
				acp.WithUpdateContent([]acp.ToolCallContent{
					acp.ToolContent(acp.TextBlock("14 rows")),
				})),
			want: agentEvent{
				Kind:       "tool-result",
				ToolCallID: "call-1",
				Result:     "14 rows",
				IsError:    false,
			},
			ok: true,
		},
		{
			name: "tool call update failed",
			in: acp.UpdateToolCall("call-1",
				acp.WithUpdateStatus(acp.ToolCallStatusFailed),
				acp.WithUpdateContent([]acp.ToolCallContent{
					acp.ToolContent(acp.TextBlock("boom")),
				})),
			want: agentEvent{
				Kind:       "tool-result",
				ToolCallID: "call-1",
				Result:     "boom",
				IsError:    true,
			},
			ok: true,
		},
		{
			name: "plan",
			in: acp.UpdatePlan(
				acp.PlanEntry{Content: "read schema", Status: acp.PlanEntryStatusCompleted},
				acp.PlanEntry{Content: "write rules", Status: acp.PlanEntryStatusInProgress},
			),
			want: agentEvent{
				Kind: "plan",
				PlanEntries: []agentPlanEntry{
					{Content: "read schema", Status: "completed"},
					{Content: "write rules", Status: "in_progress"},
				},
			},
			ok: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ok := mapSessionUpdate(c.in)
			if ok != c.ok {
				t.Fatalf("ok = %v, want %v (event: %+v)", ok, c.ok, got)
			}
			if !ok {
				return
			}
			if got.Kind != c.want.Kind {
				t.Errorf("Kind = %q, want %q", got.Kind, c.want.Kind)
			}
			if got.Text != c.want.Text {
				t.Errorf("Text = %q, want %q", got.Text, c.want.Text)
			}
			if got.ToolCallID != c.want.ToolCallID {
				t.Errorf("ToolCallID = %q, want %q", got.ToolCallID, c.want.ToolCallID)
			}
			if c.want.ToolName != "" && got.ToolName != c.want.ToolName {
				t.Errorf("ToolName = %q, want %q", got.ToolName, c.want.ToolName)
			}
			if c.want.ToolArgs != "" && got.ToolArgs != c.want.ToolArgs {
				t.Errorf("ToolArgs = %q, want %q", got.ToolArgs, c.want.ToolArgs)
			}
			if got.Result != c.want.Result {
				t.Errorf("Result = %q, want %q", got.Result, c.want.Result)
			}
			if got.IsError != c.want.IsError {
				t.Errorf("IsError = %v, want %v", got.IsError, c.want.IsError)
			}
			if c.want.Kind == "plan" {
				if len(got.PlanEntries) != len(c.want.PlanEntries) {
					t.Fatalf("PlanEntries = %+v, want %+v", got.PlanEntries, c.want.PlanEntries)
				}
				for i := range got.PlanEntries {
					if got.PlanEntries[i] != c.want.PlanEntries[i] {
						t.Errorf("PlanEntries[%d] = %+v, want %+v", i, got.PlanEntries[i], c.want.PlanEntries[i])
					}
				}
			}
		})
	}
}

// TestHarvestAndMergeToolState exercises the fix for tool summaries going
// blank on a terminal tool_call_update (found live against the real
// claude-agent-acp adapter, whose terminal update carries only status and
// content — see acpDriver.toolState's doc comment in acp.go): an initial
// tool_call with title+rawInput, a non-terminal update supplying neither
// (dropped as an EVENT by mapSessionUpdate, but still harvested), then a
// terminal update also supplying neither — the merged "tool-result" event
// must still carry the original title and args, exactly as if the terminal
// update had repeated them.
func TestHarvestAndMergeToolState(t *testing.T) {
	state := map[acp.ToolCallId]toolCallState{}

	// tool_call: title + rawInput present.
	start := acp.StartToolCall("call-1", "query", acp.WithStartRawInput(map[string]any{"query": "foo(X)?"}))
	harvestToolState(state, start)
	ev, ok := mapSessionUpdate(start)
	if !ok {
		t.Fatalf("mapSessionUpdate(start) ok = false")
	}
	ev = mergeToolState(ev, state)
	if ev.ToolName != "query" || ev.ToolArgs != `{"query":"foo(X)?"}` {
		t.Fatalf("tool-call event = %+v, want ToolName=query ToolArgs={\"query\":\"foo(X)?\"}", ev)
	}

	// Non-terminal tool_call_update: neither title nor rawInput. Dropped as
	// an event (ok = false), but its (nonexistent) fields must not blank
	// out what tool_call already established.
	inProgress := acp.UpdateToolCall("call-1", acp.WithUpdateStatus(acp.ToolCallStatusInProgress))
	harvestToolState(state, inProgress)
	if _, ok := mapSessionUpdate(inProgress); ok {
		t.Fatalf("mapSessionUpdate(in-progress update) ok = true, want false (non-terminal)")
	}
	if state["call-1"].title != "query" || state["call-1"].args != `{"query":"foo(X)?"}` {
		t.Fatalf("state after non-terminal update = %+v, want title/args preserved", state["call-1"])
	}

	// Terminal tool_call_update: status + content only, exactly the sparse
	// shape the real adapter sends — Title and RawInput both absent. The
	// merged event must still carry the original title+args from state.
	terminal := acp.UpdateToolCall("call-1",
		acp.WithUpdateStatus(acp.ToolCallStatusCompleted),
		acp.WithUpdateContent([]acp.ToolCallContent{acp.ToolContent(acp.TextBlock("14 rows"))}))
	harvestToolState(state, terminal)
	got, ok := mapSessionUpdate(terminal)
	if !ok {
		t.Fatalf("mapSessionUpdate(terminal) ok = false")
	}
	if got.ToolName != "" || got.ToolArgs != "" {
		t.Fatalf("mapSessionUpdate(terminal) = %+v, want empty ToolName/ToolArgs before merge (this is the bug's raw shape)", got)
	}
	merged := mergeToolState(got, state)
	if merged.Kind != "tool-result" {
		t.Fatalf("merged.Kind = %q, want tool-result", merged.Kind)
	}
	if merged.ToolName != "query" {
		t.Errorf("merged.ToolName = %q, want %q (filled from accumulated state)", merged.ToolName, "query")
	}
	if merged.ToolArgs != `{"query":"foo(X)?"}` {
		t.Errorf("merged.ToolArgs = %q, want %q (filled from accumulated state)", merged.ToolArgs, `{"query":"foo(X)?"}`)
	}
	if merged.Result != "14 rows" {
		t.Errorf("merged.Result = %q, want %q", merged.Result, "14 rows")
	}
}

// TestPermissionEvent checks permissionEvent's mapping of a
// RequestPermissionRequest into the "permission" agentEvent's fields —
// RequestID is generated by the caller (acpDriver.RequestPermission), so
// this only exercises the params → agentEvent projection.
func TestPermissionEvent(t *testing.T) {
	req := acp.RequestPermissionRequest{
		ToolCall: acp.ToolCallUpdate{
			ToolCallId: "call-9",
			Title:      acp.Ptr("set_rules"),
			RawInput:   map[string]any{"text": "p(X) :- q(X)."},
		},
		Options: []acp.PermissionOption{
			{OptionId: "allow", Name: "Allow", Kind: acp.PermissionOptionKindAllowOnce},
			{OptionId: "reject", Name: "Reject", Kind: acp.PermissionOptionKindRejectOnce},
		},
	}

	ev := permissionEvent("perm-1", req)

	if ev.Kind != "permission" {
		t.Fatalf("Kind = %q, want %q", ev.Kind, "permission")
	}
	if ev.RequestID != "perm-1" {
		t.Errorf("RequestID = %q, want %q", ev.RequestID, "perm-1")
	}
	if ev.ToolCallID != "call-9" {
		t.Errorf("ToolCallID = %q, want %q", ev.ToolCallID, "call-9")
	}
	if ev.ToolName != "set_rules" {
		t.Errorf("ToolName = %q, want %q", ev.ToolName, "set_rules")
	}
	if len(ev.Options) != 2 {
		t.Fatalf("Options = %+v, want 2 entries", ev.Options)
	}
	if ev.Options[0].ID != "allow" || ev.Options[0].Kind != "allow_once" {
		t.Errorf("Options[0] = %+v", ev.Options[0])
	}
	if ev.Options[1].ID != "reject" || ev.Options[1].Kind != "reject_once" {
		t.Errorf("Options[1] = %+v", ev.Options[1])
	}
}

// TestMcpServerConfig_HTTP checks the mcpServers entry ensureSession builds
// when the agent DID advertise mcpCapabilities.http at initialize
// (acp-integration.md handshake step 3): the HTTP transport, URL, and
// bearer token as an Authorization header — never the stdio shim.
func TestMcpServerConfig_HTTP(t *testing.T) {
	d := &acpDriver{
		mcpURL:   "http://127.0.0.1:8080/mcp",
		mcpToken: "the-token",
	}
	d.agentCaps.McpCapabilities.Http = true

	cfg, err := d.mcpServerConfig()
	if err != nil {
		t.Fatalf("mcpServerConfig: %v", err)
	}
	if cfg.Http == nil {
		t.Fatalf("mcpServerConfig with Http capability: got %+v, want an Http entry", cfg)
	}
	if cfg.Http.Url != d.mcpURL {
		t.Errorf("Http.Url = %q, want %q", cfg.Http.Url, d.mcpURL)
	}
	found := false
	for _, h := range cfg.Http.Headers {
		if h.Name == "Authorization" && h.Value == "Bearer the-token" {
			found = true
		}
	}
	if !found {
		t.Errorf("Http.Headers = %+v, want an Authorization: Bearer the-token header", cfg.Http.Headers)
	}
}

// TestMcpServerConfig_StdioFallback checks the mcpServers entry
// ensureSession builds for an agent that did NOT advertise
// mcpCapabilities.http: the datalog mcp --proxy stdio shim
// (doc/features/acp-integration.md work item 7), command = this process's
// own executable path, args = ["mcp", "--proxy", <url>], and the bearer
// token carried in the environment (DATALOG_MCP_TOKEN) — never argv, per
// the handshake's "the token travels ... never in argv" rule.
func TestMcpServerConfig_StdioFallback(t *testing.T) {
	d := &acpDriver{
		mcpURL:   "http://127.0.0.1:8080/mcp",
		mcpToken: "the-token",
	}
	// agentCaps.McpCapabilities.Http defaults to false.

	cfg, err := d.mcpServerConfig()
	if err != nil {
		t.Fatalf("mcpServerConfig: %v", err)
	}
	if cfg.Stdio == nil {
		t.Fatalf("mcpServerConfig without Http capability: got %+v, want a Stdio entry", cfg)
	}
	self, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	if cfg.Stdio.Command != self {
		t.Errorf("Stdio.Command = %q, want %q (datalog's own executable)", cfg.Stdio.Command, self)
	}
	wantArgs := []string{"mcp", "--proxy", d.mcpURL}
	if len(cfg.Stdio.Args) != len(wantArgs) {
		t.Fatalf("Stdio.Args = %v, want %v", cfg.Stdio.Args, wantArgs)
	}
	for i, a := range wantArgs {
		if cfg.Stdio.Args[i] != a {
			t.Errorf("Stdio.Args[%d] = %q, want %q", i, cfg.Stdio.Args[i], a)
		}
	}
	for _, a := range cfg.Stdio.Args {
		if a == d.mcpToken {
			t.Fatalf("Stdio.Args = %v: bearer token leaked into argv", cfg.Stdio.Args)
		}
	}
	tokenFound := false
	for _, e := range cfg.Stdio.Env {
		if e.Name == "DATALOG_MCP_TOKEN" {
			tokenFound = true
			if e.Value != d.mcpToken {
				t.Errorf("DATALOG_MCP_TOKEN env value = %q, want %q", e.Value, d.mcpToken)
			}
		}
	}
	if !tokenFound {
		t.Errorf("Stdio.Env = %+v, want a DATALOG_MCP_TOKEN entry", cfg.Stdio.Env)
	}
}
