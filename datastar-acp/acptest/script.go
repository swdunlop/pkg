// Package acptest is a public, reusable scripted ACP agent for exercising a
// chat embedding end to end.  A host wires it once from TestMain (see Example)
// and then registers a chat.AgentProfile whose Command re-execs the test binary
// into Main; the agent speaks real ACP JSON-RPC over its stdio, replaying a
// caller-supplied Script.  The whole scenario travels in one environment
// variable — the JSON-encoded Script — so a host composes new scenarios without
// forking this package (unlike datalog's named-script switch, which required
// editing the agent to add a case).
//
// The core (Main, Script, and the agent) never imports testing; the *testing.T
// conveniences live in the small wrappers Profile, Command, and Env.
package acptest

import "encoding/json"

// StepKind names one primitive the scripted agent replays during a turn; a
// Script is an ordered list of Steps run start to finish.  The vocabulary is
// exactly what is needed to reproduce every scenario datalog's fake agent
// covered.
type StepKind string

const (
	// StepMessage emits an agent_message_chunk carrying Text.
	StepMessage StepKind = "message"
	// StepThought emits an agent_thought_chunk carrying Text.
	StepThought StepKind = "thought"
	// StepPlan emits a plan update built from Plan.
	StepPlan StepKind = "plan"
	// StepToolCall emits a tool_call start with ToolCallID, Title, and (JSON)
	// RawInput.  It records the id so a later StepToolResult can update it.
	StepToolCall StepKind = "tool_call"
	// StepToolResult emits a tool_call_update for ToolCallID.  When Sparse is
	// true it carries ONLY status+content (no title, no rawInput) — the shape
	// the real claude-agent-acp adapter's terminal update sends, and the
	// regression the driver's toolState mechanism guards.  Status defaults to
	// "completed"; set Status:"failed" for an error result.
	StepToolResult StepKind = "tool_result"
	// StepMCPCall connects back to the HTTP MCP endpoint handed to the agent at
	// session/new, calls the tool named Tool with (JSON) RawInput as arguments,
	// and emits the joined text result as a tool_call_update for ToolCallID.
	// This verifies the URL + Authorization header plumbing end to end: a wrong
	// or missing token surfaces as the call's own error.  StoreResultAs, when
	// set, saves the joined result text under that key for later Branch use.
	StepMCPCall StepKind = "mcp_call"
	// StepPermission issues a session/request_permission for ToolCallID/Title
	// offering Options, then records the chosen option id under Branch's key (or
	// "permission" when Branch is empty) so a following StepMessage can echo it
	// via a {{key}} placeholder.  A cancelled outcome records "cancelled".
	StepPermission StepKind = "permission"
	// StepBlockUntilCancel blocks the turn until session/cancel arrives (or a
	// 10s safety valve fires), then ends the turn with stopReason cancelled.
	// Any steps after it are unreachable.
	StepBlockUntilCancel StepKind = "block_until_cancel"
	// StepExit calls os.Exit(Code) immediately, killing the subprocess
	// mid-turn.  The driver surfaces this as "agent exited (code N)".
	StepExit StepKind = "exit"
	// StepRPCError ends the turn by returning a genuine JSON-RPC error
	// (acp.NewInvalidParams(Text)) — the connection stays healthy, unlike
	// StepExit.
	StepRPCError StepKind = "rpc_error"
)

// Step is one scripted primitive; only the fields its Kind uses are read.  A
// message or thought Text may contain {{key}} placeholders substituted from
// values recorded by earlier StepPermission/StepMCPCall steps, so a message can
// report a branch outcome.
type Step struct {
	Kind StepKind `json:"kind"`

	Text string `json:"text,omitempty"` // message/thought/rpc_error text

	ToolCallID string          `json:"toolCallID,omitempty"`
	Title      string          `json:"title,omitempty"`
	RawInput   json.RawMessage `json:"rawInput,omitempty"`
	Status     string          `json:"status,omitempty"` // tool_result: completed|failed
	Sparse     bool            `json:"sparse,omitempty"` // tool_result: status+content only

	Tool          string `json:"tool,omitempty"`          // mcp_call: tool name
	StoreResultAs string `json:"storeResultAs,omitempty"` // mcp_call: recorded key

	Options []PermissionOption `json:"options,omitempty"` // permission choices
	Branch  string             `json:"branch,omitempty"`  // permission: recorded key

	Plan []PlanEntry `json:"plan,omitempty"` // plan entries

	Code int `json:"code,omitempty"` // exit code
}

// PermissionOption is one choice a StepPermission offers; it mirrors ACP's own
// option shape so the driver and views render it unchanged.
type PermissionOption struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Kind string `json:"kind,omitempty"` // e.g. allow_once, reject_once
}

// PlanEntry is one step of a StepPlan update.
type PlanEntry struct {
	Content  string `json:"content"`
	Status   string `json:"status,omitempty"`   // pending|in_progress|completed
	Priority string `json:"priority,omitempty"` // low|medium|high
}

// Script is an ordered list of Steps the agent replays on each session/prompt.
type Script struct {
	// EchoPrompt, when set, emits the exact prompt text the agent received as
	// the turn's first agent_message_chunk, prefixed with this string.  Hosts
	// use it to assert the profile preamble arrived (and arrived exactly once
	// per spawn) by inspecting the transcript.
	EchoPrompt string `json:"echoPrompt,omitempty"`

	Steps []Step `json:"steps"`
}

// ScriptEnv is the environment variable Main reads the JSON-encoded Script from
// and Profile writes it to.  It is the single channel scenarios travel by, so a
// host composes any Script without editing this package.
const ScriptEnv = "ACPTEST_SCRIPT"

// activateEnv gates Main: set (to any value), a re-exec'd test binary runs as
// the scripted agent instead of the test suite; unset, Main is a no-op.
const activateEnv = "ACPTEST_ACTIVE"

// Encode marshals a Script to the string form ScriptEnv carries.
func (s Script) Encode() (string, error) {
	b, err := json.Marshal(s)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// decodeScript parses the ScriptEnv value; an empty value yields an empty
// script (a turn that immediately ends).
func decodeScript(s string) (Script, error) {
	var sc Script
	if s == "" {
		return sc, nil
	}
	err := json.Unmarshal([]byte(s), &sc)
	return sc, err
}
