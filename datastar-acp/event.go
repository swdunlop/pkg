package chat

import (
	"encoding/json"
	"time"
)

// Event is the unified vocabulary shared by the driver, the store, and the views: live rendering and replay from
// the store run the same path over the same shapes.  Tool call events are finalized before they reach the store —
// ACP tool_call_update notifications are partial patches accumulated by the driver, so a stored tool call carries
// its settled title, input, and result rather than a patch sequence.
type Event struct {
	Kind EventKind `json:"kind"`
	Time time.Time `json:"time"`

	// Text carries message and thought content, error text, or a tool result summary, depending on Kind.
	Text string `json:"text,omitempty"`

	// ToolCallID correlates tool-call and tool-result events; RequestID correlates permission events with
	// their answers.
	ToolCallID string `json:"toolCallID,omitempty"`
	RequestID  string `json:"requestID,omitempty"`

	// Title and RawInput describe a tool call or permission request; RawInput is the tool's input as reported
	// by the agent, preserved verbatim for host permission renderers.
	Title    string          `json:"title,omitempty"`
	RawInput json.RawMessage `json:"rawInput,omitempty"`

	// Status is the ACP tool-call status a tool-result event settled on, e.g. completed or failed; empty for
	// non-tool events.  IsError distinguishes a failed tool call from a successful one so views can badge it
	// without re-parsing Status.
	Status  string `json:"status,omitempty"`
	IsError bool   `json:"isError,omitempty"`

	// Options lists the choices offered by a permission request.
	Options []PermissionOption `json:"options,omitempty"`

	// Plan carries the agent's plan entries when Kind is EventPlan.
	Plan []PlanEntry `json:"plan,omitempty"`
}

// EventKind discriminates the Event union.
type EventKind string

const (
	EventMessage    EventKind = `message`     // assistant text
	EventThought    EventKind = `thought`     // assistant reasoning the agent chose to surface
	EventToolCall   EventKind = `tool-call`   // a finalized tool invocation
	EventToolResult EventKind = `tool-result` // the result of a tool invocation
	EventError      EventKind = `error`       // a turn-level failure surfaced to the operator
	EventPermission EventKind = `permission`  // an ACP RequestPermission awaiting an answer
	EventPlan       EventKind = `plan`        // the agent's current plan
)

// PermissionOption is one choice offered by an ACP permission request; answering posts its ID back to the agent.
type PermissionOption struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Kind string `json:"kind,omitempty"` // ACP option kind, e.g. allow_once, reject_once
}

// PlanEntry is one step of an agent plan.
type PlanEntry struct {
	Content  string `json:"content"`
	Status   string `json:"status,omitempty"`
	Priority string `json:"priority,omitempty"`
}
