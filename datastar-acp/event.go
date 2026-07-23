package chat

import "swdunlop.dev/pkg/datastar-acp/agent"

// The event vocabulary and the Driver contract are defined in the agent
// subpackage so agent.DriverFactory can be typed without importing this
// package; they are aliased here because the component's store, bus, and
// views speak the same shapes.
type (
	Event            = agent.Event
	EventKind        = agent.EventKind
	PermissionOption = agent.PermissionOption
	PlanEntry        = agent.PlanEntry
	Driver           = agent.Driver
)

const (
	EventMessage    = agent.EventMessage
	EventThought    = agent.EventThought
	EventToolCall   = agent.EventToolCall
	EventToolResult = agent.EventToolResult
	EventError      = agent.EventError
	EventPermission = agent.EventPermission
	EventPlan       = agent.EventPlan
)
