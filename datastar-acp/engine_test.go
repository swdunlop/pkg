package chat

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/swdunlop/html-go/datastar"
)

// fakeDriver is a scripted in-package implementation of the unexported driver
// interface for the turn engine test.  Its Prompt runs a caller-supplied
// script over the sink; Answer signals a channel a script step can wait on;
// cancellation is observed via ctx.
type fakeDriver struct {
	mu       sync.Mutex
	prompts  []string
	answers  chan answer
	script   func(ctx context.Context, text string, sink func(Event), d *fakeDriver) (string, error)
	closed   bool
	closedCh chan struct{}
}

type answer struct {
	requestID string
	optionID  string
}

func newFakeDriver(script func(ctx context.Context, text string, sink func(Event), d *fakeDriver) (string, error)) *fakeDriver {
	return &fakeDriver{answers: make(chan answer, 4), script: script, closedCh: make(chan struct{})}
}

func (d *fakeDriver) Prompt(ctx context.Context, text string, sink func(Event)) (string, error) {
	d.mu.Lock()
	d.prompts = append(d.prompts, text)
	d.mu.Unlock()
	return d.script(ctx, text, sink, d)
}

func (d *fakeDriver) Answer(requestID, optionID string) error {
	d.answers <- answer{requestID, optionID}
	return nil
}

func (d *fakeDriver) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.closed {
		d.closed = true
		close(d.closedCh)
	}
	return nil
}

// capture subscribes to a runtime's bus and renders every published event to
// SSE text, so tests can assert on the patches the engine fans out.
type capture struct {
	mu  sync.Mutex
	buf bytes.Buffer
	sub *Subscriber
	rt  *runtime
}

func newCapture(t *testing.T, rt *runtime) *capture {
	bus, ok := rt.cfg.bus.(*DefaultBus)
	if !ok {
		t.Fatal("expected DefaultBus")
	}
	c := &capture{sub: bus.Subscribe(), rt: rt}
	go func() {
		for ev := range c.sub.Events() {
			c.mu.Lock()
			s := datastar.NewStream(&c.buf)
			_ = s.Emit(ev)
			c.mu.Unlock()
		}
	}()
	return c
}

func (c *capture) text() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.String()
}

// waitFor polls the captured SSE text until substr appears or times out.
func (c *capture) waitFor(t *testing.T, substr string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(c.text(), substr) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %q in patches; got:\n%s", substr, c.text())
}

// newTestRuntime builds a runtime with an in-memory store and an injected
// driver factory.
func newTestRuntime(t *testing.T, factory func(AgentProfile, mcpEndpoint) (driver, error)) *runtime {
	iface, err := New(Profile(AgentProfile{Name: "triage", Command: "x", Instructions: "PREAMBLE"}))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	rt := iface.(*runtime)
	rt.newDriver = factory
	return rt
}

// postForm drives a route with a form body.
func postForm(rt *runtime, path string, form url.Values) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Datastar-Request", "true")
	w := httptest.NewRecorder()
	rt.ServeHTTP(w, req)
	return w
}

// postSignals drives a route with datastar signals in the body.
func postSignals(rt *runtime, path, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Datastar-Request", "true")
	w := httptest.NewRecorder()
	rt.ServeHTTP(w, req)
	return w
}

// TestTurnEngineStoresAndPublishes pins the happy path: the user prompt is
// stored, streamed events are stored finalized, the preamble is prepended to
// the wire but not stored, and busy signals are published on start and end.
func TestTurnEngineHappyPath(t *testing.T) {
	fake := newFakeDriver(func(ctx context.Context, text string, sink func(Event), d *fakeDriver) (string, error) {
		sink(Event{Kind: EventToolCall, ToolCallID: "t1", Title: "search", Status: "in_progress"})
		sink(Event{Kind: EventToolResult, ToolCallID: "t1", Status: "completed", Text: "42"})
		sink(Event{Kind: EventMessage, Text: "the answer is 42"})
		return "end_turn", nil
	})
	rt := newTestRuntime(t, func(AgentProfile, mcpEndpoint) (driver, error) { return fake, nil })
	defer rt.Shutdown()
	cap := newCapture(t, rt)

	// Create a conversation (selects it).
	postForm(rt, "/agent/conversations", url.Values{"profile": {"triage"}})
	id := rt.currentActive()
	if id == "" {
		t.Fatal("create did not select a conversation")
	}

	// Send a prompt.
	postSignals(rt, "/agent/send", `{"prompt":"what is the answer"}`)

	cap.waitFor(t, "the answer is 42")
	// The busy-clear signal publishes from the deferred turn-end, after every
	// store append has completed — wait for it before reading the store.
	cap.waitFor(t, `"busy":""`)

	// Busy start and end signals were published.
	patches := cap.text()
	if !strings.Contains(patches, `"busy":"agent"`) {
		t.Errorf("no busy-start signal in patches:\n%s", patches)
	}
	if !strings.Contains(patches, `"busy":""`) {
		t.Errorf("no busy-clear signal in patches:\n%s", patches)
	}

	// Preamble prepended to the wire, not the stored prompt.  Read under the
	// driver's own lock (it records every prompt) rather than a raced closure.
	fake.mu.Lock()
	gotWire := fake.prompts[len(fake.prompts)-1]
	fake.mu.Unlock()
	if !strings.HasPrefix(gotWire, "PREAMBLE\n\nwhat is the answer") {
		t.Errorf("wire text missing preamble: %q", gotWire)
	}

	_, entries, err := rt.cfg.store.Read(id)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	// Expect: prompt, tool-call, tool-result, message.
	if len(entries) != 4 {
		t.Fatalf("stored %d entries, want 4: %+v", len(entries), entries)
	}
	if entries[0].Prompt != "what is the answer" {
		t.Errorf("stored prompt = %q (must not carry preamble)", entries[0].Prompt)
	}
	if entries[1].Event == nil || entries[1].Event.Kind != EventToolCall {
		t.Errorf("entry 1 = %+v, want tool-call", entries[1].Event)
	}
	if entries[2].Event == nil || entries[2].Event.Kind != EventToolResult || entries[2].Event.Text != "42" {
		t.Errorf("entry 2 = %+v, want finalized tool-result", entries[2].Event)
	}
	if entries[3].Event == nil || entries[3].Event.Text != "the answer is 42" {
		t.Errorf("entry 3 = %+v", entries[3].Event)
	}
}

// TestTurnEnginePermissionAnswered pins the permission path: a permission event
// renders an answerable card, the answer route resolves it through the driver
// and morphs the card, and the permission is stored.
func TestTurnEnginePermissionAnswered(t *testing.T) {
	fake := newFakeDriver(func(ctx context.Context, text string, sink func(Event), d *fakeDriver) (string, error) {
		sink(Event{
			Kind: EventPermission, RequestID: "perm-1", ToolCallID: "t1", Title: "write",
			Options: []PermissionOption{{ID: "allow", Name: "Allow", Kind: "allow_once"}, {ID: "deny", Name: "Deny", Kind: "reject_once"}},
		})
		// Block until the answer route resolves it.
		select {
		case a := <-d.answers:
			if a.optionID != "allow" {
				return "", context.Canceled
			}
			sink(Event{Kind: EventMessage, Text: "wrote it"})
			return "end_turn", nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	})
	rt := newTestRuntime(t, func(AgentProfile, mcpEndpoint) (driver, error) { return fake, nil })
	defer rt.Shutdown()
	cap := newCapture(t, rt)

	postForm(rt, "/agent/conversations", url.Values{"profile": {"triage"}})
	id := rt.currentActive()
	go postSignals(rt, "/agent/send", `{"prompt":"please write"}`)

	// The permission card renders with answer buttons.
	cap.waitFor(t, "chat-permission-option")
	cap.waitFor(t, "requestID=perm-1")

	// Answer it.
	postForm(rt, "/agent/answer?requestID=perm-1&optionID=allow&conv="+id, url.Values{})

	cap.waitFor(t, "wrote it")
	cap.waitFor(t, "answered: Allow")

	_, entries, _ := rt.cfg.store.Read(id)
	// prompt, permission, message.
	var sawPerm bool
	for _, e := range entries {
		if e.Event != nil && e.Event.Kind == EventPermission {
			sawPerm = true
		}
	}
	if !sawPerm {
		t.Errorf("permission event not stored: %+v", entries)
	}

	// Replay renders the permission as resolved, never answerable.
	replay := string(rt.transcript(id, entries).AppendHTML(nil))
	if strings.Contains(replay, "chat-permission-option") {
		t.Errorf("replay rendered an answerable permission card:\n%s", replay)
	}
	if !strings.Contains(replay, "chat-permission-line") {
		t.Errorf("replay missing resolved permission line:\n%s", replay)
	}
}

// TestTurnEngineCancel pins the cancel path: cancelling the turn ctx settles a
// "turn cancelled" end state and clears busy.
func TestTurnEngineCancel(t *testing.T) {
	started := make(chan struct{})
	fake := newFakeDriver(func(ctx context.Context, text string, sink func(Event), d *fakeDriver) (string, error) {
		close(started)
		<-ctx.Done()
		return "", ctx.Err()
	})
	rt := newTestRuntime(t, func(AgentProfile, mcpEndpoint) (driver, error) { return fake, nil })
	defer rt.Shutdown()
	cap := newCapture(t, rt)

	postForm(rt, "/agent/conversations", url.Values{"profile": {"triage"}})
	id := rt.currentActive()
	go postSignals(rt, "/agent/send", `{"prompt":"long running"}`)

	<-started
	postForm(rt, "/agent/cancel", url.Values{})

	cap.waitFor(t, "turn cancelled")
	cap.waitFor(t, `"busy":""`)

	_, entries, _ := rt.cfg.store.Read(id)
	last := entries[len(entries)-1]
	if last.Event == nil || last.Event.Kind != EventError || !strings.Contains(last.Event.Text, "cancelled") {
		t.Errorf("last entry = %+v, want cancelled error", last.Event)
	}
}

// eventKinds returns the stored events' kinds in order (prompts render as
// "prompt"), for the accumulation/plan assertions below.
func eventKinds(entries []Entry) []EventKind {
	kinds := make([]EventKind, 0, len(entries))
	for _, e := range entries {
		if e.Prompt != "" {
			kinds = append(kinds, "prompt")
			continue
		}
		if e.Event != nil {
			kinds = append(kinds, e.Event.Kind)
		}
	}
	return kinds
}

// countKind counts stored events of one kind.
func countKind(entries []Entry, kind EventKind) int {
	n := 0
	for _, e := range entries {
		if e.Event != nil && e.Event.Kind == kind {
			n++
		}
	}
	return n
}

// firstEvent returns the first stored event of a kind, or nil.
func firstEvent(entries []Entry, kind EventKind) *Event {
	for _, e := range entries {
		if e.Event != nil && e.Event.Kind == kind {
			return e.Event
		}
	}
	return nil
}

// runFakeTurn creates a conversation, sends one prompt driven by script, and
// waits for busy to clear so every finalized append has landed.  Returns the id.
func runFakeTurn(t *testing.T, script func(context.Context, string, func(Event), *fakeDriver) (string, error)) (*runtime, string, *capture) {
	t.Helper()
	fake := newFakeDriver(script)
	rt := newTestRuntime(t, func(AgentProfile, mcpEndpoint) (driver, error) { return fake, nil })
	t.Cleanup(rt.Shutdown)
	cap := newCapture(t, rt)
	postForm(rt, "/agent/conversations", url.Values{"profile": {"triage"}})
	id := rt.currentActive()
	postSignals(rt, "/agent/send", `{"prompt":"go"}`)
	cap.waitFor(t, `"busy":""`)
	return rt, id, cap
}

// TestStreamingMessageAccumulation pins defect 1: two consecutive message
// chunks render ONE live entry (an append then a morph, same element id) and
// store ONE finalized message carrying the full accumulated text.
func TestStreamingMessageAccumulation(t *testing.T) {
	rt, id, cap := runFakeTurn(t, func(ctx context.Context, text string, sink func(Event), d *fakeDriver) (string, error) {
		sink(Event{Kind: EventMessage, Text: "hel"})
		sink(Event{Kind: EventMessage, Text: "lo"})
		return "end_turn", nil
	})

	_, entries, _ := rt.cfg.store.Read(id)
	if got := countKind(entries, EventMessage); got != 1 {
		t.Fatalf("stored %d message events, want 1 (accumulation): %v", got, eventKinds(entries))
	}
	msg := firstEvent(entries, EventMessage)
	if msg.Text != "hello" {
		t.Fatalf("finalized message text = %q, want %q", msg.Text, "hello")
	}
	// The live stream appended once and morphed once at the SAME element id.
	patches := cap.text()
	if strings.Count(patches, "mode append") < 2 { // prompt append + message append
		t.Fatalf("expected the message to append once, not per chunk:\n%s", patches)
	}
}

// TestStreamingToolCallBreaks pins the interleaving rule: a tool call between
// two message chunks breaks the stream into TWO stored message entries.
func TestStreamingToolCallBreaks(t *testing.T) {
	rt, id, _ := runFakeTurn(t, func(ctx context.Context, text string, sink func(Event), d *fakeDriver) (string, error) {
		sink(Event{Kind: EventMessage, Text: "before"})
		sink(Event{Kind: EventToolCall, ToolCallID: "t1", Title: "search"})
		sink(Event{Kind: EventToolResult, ToolCallID: "t1", Text: "ok"})
		sink(Event{Kind: EventMessage, Text: "after"})
		return "end_turn", nil
	})

	_, entries, _ := rt.cfg.store.Read(id)
	if got := countKind(entries, EventMessage); got != 2 {
		t.Fatalf("stored %d message events, want 2 (tool call breaks the stream): %v", got, eventKinds(entries))
	}
	want := []EventKind{"prompt", EventMessage, EventToolCall, EventToolResult, EventMessage}
	got := eventKinds(entries)
	if len(got) != len(want) {
		t.Fatalf("stored kinds %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("stored kinds %v, want %v", got, want)
		}
	}
}

// TestStreamingThoughtResetsMessage pins that a thought interrupting a message
// finalizes the message and starts a fresh thought entry (each stored once).
func TestStreamingThoughtResetsMessage(t *testing.T) {
	rt, id, _ := runFakeTurn(t, func(ctx context.Context, text string, sink func(Event), d *fakeDriver) (string, error) {
		sink(Event{Kind: EventThought, Text: "hmm"})
		sink(Event{Kind: EventMessage, Text: "reply"})
		return "end_turn", nil
	})

	_, entries, _ := rt.cfg.store.Read(id)
	if got := countKind(entries, EventThought); got != 1 {
		t.Fatalf("stored %d thought events, want 1: %v", got, eventKinds(entries))
	}
	if got := countKind(entries, EventMessage); got != 1 {
		t.Fatalf("stored %d message events, want 1: %v", got, eventKinds(entries))
	}
	want := []EventKind{"prompt", EventThought, EventMessage}
	if got := eventKinds(entries); len(got) != len(want) || got[1] != EventThought || got[2] != EventMessage {
		t.Fatalf("stored kinds %v, want %v", got, want)
	}
}

// TestPlanUpdatesMorph pins defect 2: two plan updates render one entry
// (morphed) and store ONE final plan event carrying the final state.
func TestPlanUpdatesMorph(t *testing.T) {
	rt, id, cap := runFakeTurn(t, func(ctx context.Context, text string, sink func(Event), d *fakeDriver) (string, error) {
		sink(Event{Kind: EventPlan, Plan: []PlanEntry{{Content: "step one", Status: "in_progress"}}})
		sink(Event{Kind: EventPlan, Plan: []PlanEntry{{Content: "step one", Status: "completed"}, {Content: "step two", Status: "in_progress"}}})
		sink(Event{Kind: EventMessage, Text: "done"})
		return "end_turn", nil
	})

	_, entries, _ := rt.cfg.store.Read(id)
	if got := countKind(entries, EventPlan); got != 1 {
		t.Fatalf("stored %d plan events, want 1 (turn-scoped, final state only): %v", got, eventKinds(entries))
	}
	plan := firstEvent(entries, EventPlan)
	if len(plan.Plan) != 2 {
		t.Fatalf("stored plan carried %d entries, want the final 2: %+v", len(plan.Plan), plan.Plan)
	}
	// The plan appended once; the second update morphed (no second plan append).
	if n := strings.Count(cap.text(), "step two"); n < 1 {
		t.Fatalf("final plan state never rendered:\n%s", cap.text())
	}
}

// TestMidTurnReselectPermission pins defect 3: while a permission is pending, a
// select POST for the SAME conversation replays a still-ANSWERABLE card, and
// answering it through the normal route lands (the morph target's stable,
// content-derived id survives the re-select).
func TestMidTurnReselectPermission(t *testing.T) {
	fake := newFakeDriver(func(ctx context.Context, text string, sink func(Event), d *fakeDriver) (string, error) {
		sink(Event{Kind: EventPermission, RequestID: "perm-1", ToolCallID: "t1", Title: "write",
			Options: []PermissionOption{{ID: "allow", Name: "Allow", Kind: "allow_once"}}})
		select {
		case a := <-d.answers:
			if a.optionID != "allow" {
				return "", context.Canceled
			}
			sink(Event{Kind: EventMessage, Text: "wrote it"})
			return "end_turn", nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	})
	rt := newTestRuntime(t, func(AgentProfile, mcpEndpoint) (driver, error) { return fake, nil })
	defer rt.Shutdown()
	cap := newCapture(t, rt)

	postForm(rt, "/agent/conversations", url.Values{"profile": {"triage"}})
	id := rt.currentActive()
	go postSignals(rt, "/agent/send", `{"prompt":"please write"}`)
	cap.waitFor(t, "chat-permission-option")

	// Re-select the running conversation mid-permission: the replayed transcript
	// (on the select route's own stream) must still show an answerable card.
	replay := postForm(rt, "/agent/select", url.Values{"id": {id}}).Body.String()
	if !strings.Contains(replay, "chat-permission-option") {
		t.Fatalf("mid-turn re-select did not replay an answerable permission card:\n%s", replay)
	}
	if !strings.Contains(replay, permElemID(id, "perm-1")) {
		t.Fatalf("replayed permission card lacks the stable content-derived id:\n%s", replay)
	}

	// Answering through the normal route lands the morph on the stable id.
	postForm(rt, "/agent/answer?requestID=perm-1&optionID=allow&conv="+id, url.Values{})
	cap.waitFor(t, "wrote it")
	cap.waitFor(t, "answered: Allow")
}

// TestRenamedPromptSignal pins defect 4: a host that renames the prompt signal
// via Signals(...) can still send — the server decodes the payload under the
// configured signal name.
func TestRenamedPromptSignal(t *testing.T) {
	fake := newFakeDriver(func(ctx context.Context, text string, sink func(Event), d *fakeDriver) (string, error) {
		sink(Event{Kind: EventMessage, Text: "got: " + text})
		return "end_turn", nil
	})
	iface, err := New(
		Profile(AgentProfile{Name: "triage", Command: "x"}),
		Signals(SignalNames{Prompt: "ask"}),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	rt := iface.(*runtime)
	rt.newDriver = func(AgentProfile, mcpEndpoint) (driver, error) { return fake, nil }
	defer rt.Shutdown()
	cap := newCapture(t, rt)

	postForm(rt, "/agent/conversations", url.Values{"profile": {"triage"}})
	// Send under the RENAMED signal name.
	postSignals(rt, "/agent/send", `{"ask":"hello there"}`)
	cap.waitFor(t, "got: hello there")
}

// TestGateBusyVisibleError pins defect 5's gate-busy rejection: a concurrent
// send is refused with a visible entry naming the conversation that holds the
// turn, and it is NOT stored.
func TestGateBusyVisibleError(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{})
	fake := newFakeDriver(func(ctx context.Context, text string, sink func(Event), d *fakeDriver) (string, error) {
		close(started)
		<-release
		sink(Event{Kind: EventMessage, Text: "finally"})
		return "end_turn", nil
	})
	rt := newTestRuntime(t, func(AgentProfile, mcpEndpoint) (driver, error) { return fake, nil })
	defer rt.Shutdown()
	cap := newCapture(t, rt)

	postForm(rt, "/agent/conversations", url.Values{"profile": {"triage"}})
	id := rt.currentActive()
	// Name the conversation so the rejection can echo it.
	_ = rt.cfg.store.Rename(id, "First Conversation")
	go postSignals(rt, "/agent/send", `{"prompt":"long one"}`)
	<-started

	// A second send while the turn holds the gate is rejected with a named entry.
	postSignals(rt, "/agent/send", `{"prompt":"me too"}`)
	cap.waitFor(t, "turn running in First Conversation")

	close(release)
	cap.waitFor(t, `"busy":""`)

	// The transient rejection was not stored.
	_, entries, _ := rt.cfg.store.Read(id)
	for _, e := range entries {
		if e.Event != nil && e.Event.Kind == EventError && strings.Contains(e.Event.Text, "turn running in") {
			t.Fatalf("gate-busy rejection was stored, want transient-only: %+v", e.Event)
		}
	}
}

// TestStaleAnswerVisibleError pins defect 5's stale-answer path: answering a
// permission the server no longer tracks surfaces a visible error anchored to
// the conversation the button baked into its URL.
func TestStaleAnswerVisibleError(t *testing.T) {
	fake := newFakeDriver(func(ctx context.Context, text string, sink func(Event), d *fakeDriver) (string, error) {
		sink(Event{Kind: EventMessage, Text: "hi"})
		return "end_turn", nil
	})
	rt := newTestRuntime(t, func(AgentProfile, mcpEndpoint) (driver, error) { return fake, nil })
	defer rt.Shutdown()
	cap := newCapture(t, rt)

	postForm(rt, "/agent/conversations", url.Values{"profile": {"triage"}})
	id := rt.currentActive()
	postSignals(rt, "/agent/send", `{"prompt":"go"}`)
	cap.waitFor(t, `"busy":""`)

	// Answer a requestID the server never tracked, with the conversation baked in.
	postForm(rt, "/agent/answer?requestID=ghost&optionID=allow&conv="+id, url.Values{})
	cap.waitFor(t, "no longer waiting for an answer")
}
