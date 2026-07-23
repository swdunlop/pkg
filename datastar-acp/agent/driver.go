package agent

import "context"

// Driver abstracts one conversational agent behind the chat pane.  The chat
// component ships one implementation — its ACP subprocess driver — and hosts
// may supply their own via the DriverFactory option to put an in-process
// agent (e.g. an embedded LLM library wired straight to the host's tools)
// behind the same pane, transcript, and turn engine.
//
// Contract, identical for both sides of the seam:
//
//   - One turn at a time: the component serializes turns globally, so Prompt
//     is never called concurrently with itself.
//   - Prompt's sink may be called from any goroutine the driver owns, but the
//     driver must not call it after Prompt returns.
//   - A permission Event (Kind EventPermission) parks the turn until Answer
//     is called with its RequestID, the turn is cancelled, or the driver
//     resolves it internally; Answer with an unknown RequestID is an error.
//   - Prompt observes ctx: on cancellation it winds the turn down and
//     returns, conventionally with stop reason "cancelled" or ctx.Err().
//   - On a Prompt error the component discards the whole driver (Close) and
//     the next prompt constructs a fresh one, so drivers need no internal
//     recovery; Close must be safe to call at any point, including twice.
type Driver interface {
	// Prompt starts one turn; events stream to sink until the turn ends.
	Prompt(ctx context.Context, text string, sink func(Event)) (stopReason string, err error)
	// Answer resolves a pending permission request (a permission Event's
	// RequestID) by option ID, unblocking the driver's in-flight
	// session/request_permission RPC.  Unknown requestID is an error.
	Answer(requestID, optionID string) error
	Close() error
}
