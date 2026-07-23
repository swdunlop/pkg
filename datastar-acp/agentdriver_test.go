package chat

import (
	"context"
	"net/url"
	"strings"
	"testing"

	"swdunlop.dev/pkg/datastar-acp/agent"
)

// TestAgentDriverServesTurn pins the host-supplied driver seam end-to-end
// through the public API: an agent registered with agent.DriverFactory serves
// a turn through the same engine as an ACP agent — factory called lazily on
// the first send, Instructions preamble prepended to the wire, events stored.
func TestAgentDriverServesTurn(t *testing.T) {
	fake := newFakeDriver(func(ctx context.Context, text string, sink func(Event), d *fakeDriver) (string, error) {
		sink(Event{Kind: EventMessage, Text: "embedded says hi"})
		return "end_turn", nil
	})
	var calls int
	iface, err := New(Agent(agent.Name("embedded"), agent.DriverFactory(func() (Driver, error) {
		calls++
		return fake, nil
	}), agent.Instructions("PREAMBLE")))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	rt := iface.(*runtime)
	defer rt.Shutdown()
	cap := newCapture(t, rt)

	postForm(rt, "/agent/conversations", url.Values{"profile": {"embedded"}})
	id := rt.currentActive()
	if id == "" {
		t.Fatal("create did not select a conversation")
	}
	if calls != 0 {
		t.Fatalf("factory called %d times before the first send; must be lazy", calls)
	}

	postSignals(rt, "/agent/send", `{"prompt":"hello"}`)
	cap.waitFor(t, "embedded says hi")
	cap.waitFor(t, `"busy":""`)

	if calls != 1 {
		t.Errorf("factory called %d times, want 1", calls)
	}
	fake.mu.Lock()
	wire := fake.prompts[len(fake.prompts)-1]
	fake.mu.Unlock()
	if !strings.HasPrefix(wire, "PREAMBLE\n\nhello") {
		t.Errorf("wire text missing preamble: %q", wire)
	}

	_, entries, err := rt.cfg.store.Read(id)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(entries) != 2 || entries[0].Prompt != "hello" || entries[1].Event == nil || entries[1].Event.Text != "embedded says hi" {
		t.Errorf("stored entries = %+v, want prompt + message", entries)
	}
}
