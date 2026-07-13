package main

import (
	"context"
	"errors"
	"io"
	"iter"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	html "github.com/swdunlop/html-go"

	"swdunlop.dev/pkg/datalog/seminaive"
)

// renderLog joins one tab's rendered scrollback into a string for
// substring assertions.
func renderLog(wb *workbench, tab string) string {
	var buf []byte
	for _, e := range wb.console.Render(tab) {
		buf = html.Append(buf, e)
	}
	return string(buf)
}

// -- console query tab -------------------------------------------------------

// TestConsoleQueryStop exercises the query Stop path end to end: a query
// parked mid-Transform on a blocking external is cancelled via POST /cancel
// (what the Run button's Stop morph posts), and the scrollback reports
// "query stopped" rather than the timeout wording.
func TestConsoleQueryStop(t *testing.T) {
	wb := newMordorWorkbench(t)
	srv := startTestServer(wb)
	defer srv.Close()

	inTransform := make(chan struct{})
	var once sync.Once
	wb.h.mu.Lock()
	wb.h.sess.engineOpts = append(wb.h.sess.engineOpts,
		seminaive.WithExternal("slow", 1, func(ctx context.Context, _ seminaive.Bindings) iter.Seq[[]any] {
			return func(yield func([]any) bool) {
				once.Do(func() { close(inTransform) })
				<-ctx.Done() // parked until Stop cancels the job ctx
			}
		}))
	wb.h.mu.Unlock()

	queryDone := make(chan struct{})
	go func() {
		defer close(queryDone)
		resp := postSignals(t, srv, "/console/query", map[string]any{
			"consoleQuery": "slow(X)",
		})
		defer resp.Body.Close()
		_, _ = io.ReadAll(resp.Body) // EOF == handler returned
	}()

	select {
	case <-inTransform:
	case <-time.After(10 * time.Second):
		t.Fatal("query never reached the blocking external")
	}

	resp, err := http.Post(srv.URL+"/cancel", "", nil)
	if err != nil {
		t.Fatalf("POST /cancel: %v", err)
	}
	resp.Body.Close()

	select {
	case <-queryDone:
	case <-time.After(10 * time.Second):
		t.Fatal("query handler did not return after /cancel")
	}

	log := renderLog(wb, "query")
	if !strings.Contains(log, "query stopped") {
		t.Fatalf("scrollback missing 'query stopped': %s", log)
	}
	if strings.Contains(log, "timed out") {
		t.Fatalf("user cancel misreported as timeout: %s", log)
	}
}

// TestConsoleQueryMultiQueryCompletedResultsSurviveMidBatchCancel is the
// handler-level regression for the round-two review's finding, exercised
// through the exact multi-query shape the review called out: a batch of two
// queries where the FIRST query's own Transform is cancelled mid-flight
// (mirroring one query consuming the shared budget or a Stop landing while
// it runs) must still render that first query's halt-status entry, and must
// NOT go on to silently run or render anything for the second query — the
// classifyQueryOutcome ordering rule's Continue=false is what stops the loop
// here. This complements the deterministic classifyQueryOutcome unit tests
// in rules_editor_test.go, which pin the qErr==nil/ctx-already-dead ordering
// directly; reproducing THAT exact instruction-level gap through the real
// HTTP handler is not reliably reachable (semi-naive's own ctx check inside
// Transform races any external POST /cancel and almost always wins), so this
// test instead pins the reachable, equally real regression shape: a batch
// input must never drop a query's own halt reporting nor silently continue
// past it.
func TestConsoleQueryMultiQueryCompletedResultsSurviveMidBatchCancel(t *testing.T) {
	wb := newMordorWorkbench(t)
	srv := startTestServer(wb)
	defer srv.Close()

	inTransform := make(chan struct{})
	var once sync.Once
	wb.h.mu.Lock()
	wb.h.sess.engineOpts = append(wb.h.sess.engineOpts,
		seminaive.WithExternal("slow3", 1, func(ctx context.Context, _ seminaive.Bindings) iter.Seq[[]any] {
			return func(yield func([]any) bool) {
				once.Do(func() { close(inTransform) })
				<-ctx.Done()
			}
		}))
	wb.h.mu.Unlock()

	queryDone := make(chan struct{})
	go func() {
		defer close(queryDone)
		resp := postSignals(t, srv, "/console/query", map[string]any{
			// Two queries in one batch — the second must never run once the
			// first is cancelled mid-Transform.
			"consoleQuery": "slow3(X)?\ncopied_to(F, H)?",
		})
		defer resp.Body.Close()
		_, _ = io.ReadAll(resp.Body)
	}()

	select {
	case <-inTransform:
	case <-time.After(10 * time.Second):
		t.Fatal("query never reached the blocking external")
	}

	resp, err := http.Post(srv.URL+"/cancel", "", nil)
	if err != nil {
		t.Fatalf("POST /cancel: %v", err)
	}
	resp.Body.Close()

	select {
	case <-queryDone:
	case <-time.After(10 * time.Second):
		t.Fatal("query handler did not return after /cancel")
	}

	log := renderLog(wb, "query")
	if !strings.Contains(log, "query stopped") {
		t.Fatalf("scrollback missing 'query stopped' for the cancelled first query: %s", log)
	}
	if strings.Contains(log, "context canceled") {
		t.Fatalf("halt-status entry duplicated with a raw ctx error: %s", log)
	}
	if strings.Contains(log, "copied_to") {
		t.Fatalf("second query in the batch ran after the first was cancelled: %s", log)
	}
}

func TestConsoleQuery(t *testing.T) {
	wb := newMordorWorkbench(t)
	srv := startTestServer(wb)
	defer srv.Close()

	// A bare query body — the handler supplies the `?` terminator.
	resp := postSignals(t, srv, "/console/query", map[string]any{
		"consoleQuery": "copied_to(F, H)",
	})
	defer resp.Body.Close()
	// Drain the POST's stream to EOF: the entry lands in the scrollback via
	// the bus, so the handler must have RETURNED before asserting, and EOF
	// is the "handler returned" signal.
	_, _ = io.ReadAll(resp.Body)

	log := renderLog(wb, "query")
	if !strings.Contains(log, "copied_to(F, H)?") {
		t.Fatalf("query echo missing from scrollback: %s", log)
	}
	if !strings.Contains(log, "<table>") {
		t.Fatalf("result table missing from scrollback: %s", log)
	}
}

func TestConsoleQueryRejectsRules(t *testing.T) {
	wb := newMordorWorkbench(t)
	srv := startTestServer(wb)
	defer srv.Close()

	resp := postSignals(t, srv, "/console/query", map[string]any{
		"consoleQuery": "bad(X) :- copied_to(X, _).",
	})
	defer resp.Body.Close()
	_, _ = io.ReadAll(resp.Body)

	log := renderLog(wb, "query")
	if !strings.Contains(log, "queries only") {
		t.Fatalf("expected rules rejection in scrollback: %s", log)
	}
}

func TestConsoleQueryParseError(t *testing.T) {
	wb := newMordorWorkbench(t)
	srv := startTestServer(wb)
	defer srv.Close()

	resp := postSignals(t, srv, "/console/query", map[string]any{
		"consoleQuery": "copied_to(F,",
	})
	defer resp.Body.Close()
	_, _ = io.ReadAll(resp.Body)

	log := renderLog(wb, "query")
	if !strings.Contains(log, `class='console-entry error'`) {
		t.Fatalf("expected an error entry in scrollback: %s", log)
	}
}

// -- console log mechanics ----------------------------------------------------

func TestConsoleLogCapAndUpdate(t *testing.T) {
	c := &consoleLog{}
	for range consoleLogCap + 10 {
		c.Append("query", "note", html.Text("x"))
	}
	if got := len(c.Render("query")); got != consoleLogCap {
		t.Fatalf("cap not enforced: %d entries", got)
	}

	id, _ := c.Append("agent", "agent", html.Text("hello"))
	if c.Update(id, "agent", html.Text("hello world")) == nil {
		t.Fatalf("Update returned nil for a live entry")
	}
	if c.Update(1, "agent", html.Text("gone")) != nil {
		t.Fatalf("Update should return nil for an entry that fell off the cap")
	}
}

// -- console clear ------------------------------------------------------------

func TestConsoleClearQueryTab(t *testing.T) {
	wb := newMordorWorkbench(t)
	srv := startTestServer(wb)
	defer srv.Close()

	wb.console.Append("query", "note", html.Text("probe"))
	wb.console.Append("agent", "agent", html.Text("kept"))

	resp := postSignals(t, srv, "/console/clear?tab=query", map[string]any{})
	defer resp.Body.Close()
	_, _ = io.ReadAll(resp.Body)

	if got := len(wb.console.Render("query")); got != 0 {
		t.Fatalf("query scrollback not cleared: %d entries", got)
	}
	if !strings.Contains(renderLog(wb, "agent"), "kept") {
		t.Fatalf("clearing query tab touched the agent scrollback")
	}
}

func TestConsoleClearAgentResetsDriver(t *testing.T) {
	wb := newMordorWorkbench(t)
	srv := startTestServer(wb)
	defer srv.Close()

	driver := &fakeDriver{}
	wb.agent = driver
	wb.console.Append("agent", "agent", html.Text("old conversation"))

	resp := postSignals(t, srv, "/console/clear?tab=agent", map[string]any{})
	defer resp.Body.Close()
	_, _ = io.ReadAll(resp.Body)

	if got := len(wb.console.Render("agent")); got != 0 {
		t.Fatalf("agent scrollback not cleared: %d entries", got)
	}
	wb.agentMu.Lock()
	live := wb.agent
	wb.agentMu.Unlock()
	if live != nil {
		t.Fatalf("driver not dropped; the model would keep its conversation memory")
	}
	if !driver.closed {
		t.Fatalf("dropped driver was not closed")
	}
}

func TestConsoleClearAgentCancelsRunningTurn(t *testing.T) {
	wb := newMordorWorkbench(t)
	srv := startTestServer(wb)
	defer srv.Close()

	// A driver that blocks until its context is cancelled — Clear must
	// cancel the turn itself (it implies Stop), wait it out, then reset.
	driver := &blockingDriver{release: make(chan struct{})}
	wb.agent = driver
	resp1 := postSignals(t, srv, "/console/prompt", map[string]any{"consolePrompt": "hi"})
	_, _ = io.ReadAll(resp1.Body)
	resp1.Body.Close()

	resp2 := postSignals(t, srv, "/console/clear?tab=agent", map[string]any{})
	_, _ = io.ReadAll(resp2.Body)
	resp2.Body.Close()

	if log := renderLog(wb, "agent"); log != "" {
		t.Fatalf("agent scrollback not empty after mid-turn clear: %s", log)
	}
	wb.agentMu.Lock()
	live := wb.agent
	wb.agentMu.Unlock()
	if live != nil {
		t.Fatalf("mid-turn clear did not drop the driver")
	}
	if !driver.closed {
		t.Fatalf("cancelled turn's driver was not closed")
	}
}

// -- agent turn runner --------------------------------------------------------

// fakeDriver scripts one turn's event sequence, standing in for kitDriver so
// the turn runner's entry lifecycle is testable without a provider
// (acp-integration.md phase-1 work item 4: test the event mapping in
// isolation). answerable/answerErr let the same fake stand in for an ACP-like
// driver that DOES issue permission requests (work item 9's tests) without a
// separate type: Answer records the call so a handler test can assert on it,
// returning answerErr when set so the "Answer fails" path is reachable too.
type fakeDriver struct {
	events     []agentEvent
	stopReason string
	err        error
	closed     bool

	answerable bool
	answerErr  error
	answered   []answerCall
}

type answerCall struct {
	requestID, optionID string
}

func (d *fakeDriver) Prompt(ctx context.Context, text string, sink func(agentEvent)) (string, error) {
	for _, ev := range d.events {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		sink(ev)
	}
	return d.stopReason, d.err
}

func (d *fakeDriver) Answer(requestID, optionID string) error {
	if !d.answerable {
		return errors.New("fakeDriver does not issue permission requests")
	}
	d.answered = append(d.answered, answerCall{requestID, optionID})
	return d.answerErr
}

func (d *fakeDriver) Close() error { d.closed = true; return nil }

func TestRunAgentTurnTranscript(t *testing.T) {
	wb := newMordorWorkbench(t)
	driver := &fakeDriver{
		events: []agentEvent{
			{Kind: "thought", Text: "let me look"},
			{Kind: "tool-call", ToolCallID: "t1", ToolName: "datalog__query", ToolArgs: `{"query":"copied_to(F,H)?"}`},
			{Kind: "tool-result", ToolCallID: "t1", ToolName: "datalog__query", ToolArgs: `{"query":"copied_to(F,H)?"}`, Result: "3 rows"},
			{Kind: "message", Text: "Found "},
			{Kind: "message", Text: "3 copies."},
		},
		stopReason: "stop",
	}

	wb.runAgentTurn(context.Background(), driver, "how many copies?")

	log := renderLog(wb, "agent")
	for _, want := range []string{
		"let me look",           // thought accumulated
		"query copied_to(F,H)?", // tool line: the query text, not its JSON envelope
		"3 rows",                // tool result behind the disclosure
		"Found 3 copies.",       // message chunks accumulated into one entry
	} {
		if !strings.Contains(log, want) {
			t.Fatalf("transcript missing %q: %s", want, log)
		}
	}
	// Chunked message must be ONE entry (morph, not append): the full text
	// appears exactly once, with no "Found " fragment entry beside it.
	if strings.Count(log, "Found 3 copies.") != 1 {
		t.Fatalf("chunked message rendered more than once: %s", log)
	}
	if strings.Contains(log, "turn failed") || strings.Contains(log, "turn ended") {
		t.Fatalf("clean stop should not add a terminal entry: %s", log)
	}
}

// TestRunAgentTurnMessageInterleavedWithToolBreaksAccumulator is the
// regression test for the transcript-ordering bug observed live against a
// real Claude Code ACP agent: text, then a tool call, then more text. Before
// the fix, every "message" chunk for the whole turn morphed the SAME entry
// created by the first chunk, so the tool call ended up sandwiched
// underneath one giant pooled reply instead of between two separate ones.
// runAgentTurn's sink now resets msgID/msgText whenever a DIFFERENT kind
// appends a new entry (breakStreaming in agent.go), so the tool-call here
// must end the first message entry and the post-tool chunks must start a
// second, separate one — asserted both by entry COUNT/ORDER (entryKinds)
// and by the second entry's text containing only the post-tool content, no
// duplication of the first.
func TestRunAgentTurnMessageInterleavedWithToolBreaksAccumulator(t *testing.T) {
	wb := newMordorWorkbench(t)
	driver := &fakeDriver{
		events: []agentEvent{
			{Kind: "message", Text: "let me check"},
			{Kind: "tool-call", ToolCallID: "t1", ToolName: "datalog__query", ToolArgs: `{"query":"copied_to(F,H)?"}`},
			{Kind: "tool-result", ToolCallID: "t1", ToolName: "datalog__query", ToolArgs: `{"query":"copied_to(F,H)?"}`, Result: "3 rows"},
			{Kind: "message", Text: "here's what I "},
			{Kind: "message", Text: "found."},
		},
		stopReason: "stop",
	}

	wb.runAgentTurn(context.Background(), driver, "how many copies?")

	kinds := entryKinds(t, wb, "agent")
	want := []string{"agent", "tool", "agent"}
	if len(kinds) != len(want) {
		t.Fatalf("entry kinds = %v, want %v (transcript must read message, tool, message in order)", kinds, want)
	}
	for i, k := range want {
		if kinds[i] != k {
			t.Fatalf("entry kinds = %v, want %v", kinds, want)
		}
	}

	entries := wb.console.Render("agent")
	first := renderContent(entries[0])
	second := renderContent(entries[2])
	if !strings.Contains(first, "let me check") {
		t.Fatalf("first message entry missing its text: %s", first)
	}
	if strings.Contains(first, "found.") {
		t.Fatalf("first message entry absorbed post-tool text: %s", first)
	}
	if !strings.Contains(second, "found.") {
		t.Fatalf("second message entry missing the post-tool text: %s", second)
	}
	if strings.Contains(second, "let me check") {
		t.Fatalf("second message entry duplicated the first entry's text: %s", second)
	}
}

// TestRunAgentTurnThoughtInterleavedWithMessageBreaksAccumulator covers the
// cheaper interleaving shape breakStreaming's doc comment calls out
// explicitly: a thought interrupting a message must end the message entry,
// and the message that follows the thought must start its own new entry —
// three entries in chronological order (message, thought, message), not two
// pooled ones.
func TestRunAgentTurnThoughtInterleavedWithMessageBreaksAccumulator(t *testing.T) {
	wb := newMordorWorkbench(t)
	driver := &fakeDriver{
		events: []agentEvent{
			{Kind: "message", Text: "first reply"},
			{Kind: "thought", Text: "reconsidering"},
			{Kind: "message", Text: "second reply"},
		},
		stopReason: "stop",
	}

	wb.runAgentTurn(context.Background(), driver, "hi")

	kinds := entryKinds(t, wb, "agent")
	want := []string{"agent", "thought", "agent"}
	if len(kinds) != len(want) {
		t.Fatalf("entry kinds = %v, want %v (message, thought, message in order)", kinds, want)
	}
	for i, k := range want {
		if kinds[i] != k {
			t.Fatalf("entry kinds = %v, want %v", kinds, want)
		}
	}

	entries := wb.console.Render("agent")
	if !strings.Contains(renderContent(entries[0]), "first reply") {
		t.Fatalf("first entry missing its text: %s", renderContent(entries[0]))
	}
	if !strings.Contains(renderContent(entries[2]), "second reply") {
		t.Fatalf("third entry missing its text: %s", renderContent(entries[2]))
	}
	if strings.Contains(renderContent(entries[2]), "first reply") {
		t.Fatalf("third entry duplicated the first message's text: %s", renderContent(entries[2]))
	}
}

// entryKinds renders tab's scrollback and extracts each entry's "kind"
// class, one per rendered html.Content, in transcript order — view.
// ConsoleEntry always sets class='console-entry <kind>' as the SECOND class
// token, so a simple split suffices without parsing the markup. This is the
// tool the interleaving-order tests use to assert entries land message,
// tool, message rather than pooling all the message chunks into one entry
// near the top (the bug this file's TestRunAgentTurn* interleaving tests
// guard against).
func entryKinds(t *testing.T, wb *workbench, tab string) []string {
	t.Helper()
	var kinds []string
	for _, c := range wb.console.Render(tab) {
		rendered := renderContent(c)
		const marker = "class='console-entry "
		i := strings.Index(rendered, marker)
		if i < 0 {
			t.Fatalf("entry missing console-entry class: %s", rendered)
		}
		rest := rendered[i+len(marker):]
		j := strings.IndexByte(rest, '\'')
		if j < 0 {
			t.Fatalf("entry class attribute not closed: %s", rendered)
		}
		kinds = append(kinds, rest[:j])
	}
	return kinds
}

func renderContent(c html.Content) string {
	return string(html.Append(nil, c))
}

func TestToolEntryQueryTable(t *testing.T) {
	ev := agentEvent{
		Kind:     "tool-result",
		ToolName: "datalog__query",
		ToolArgs: `{"query":"smb_conn(H, S, D)?"}`,
		Result:   `{"vars":["H","S","D"],"rows":[["host1","10.0.0.5",445]],"total":150,"truncated":true}`,
	}
	got := renderContent(toolEntry(ev, true))
	if !strings.HasPrefix(got, "<details>") {
		t.Fatalf("tool entry should be one collapsed disclosure: %s", got)
	}
	for _, want := range []string{
		"query smb_conn(H, S, D)?", // summary: the query itself, not JSON args
		"<th>H</th>",               // variable-named header
		"<td>host1</td>",           // string cell as-is
		"<td>445</td>",             // numeric cell without float formatting
		"showing 1 of 150 rows",    // truncation note
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("query entry missing %q: %s", want, got)
		}
	}
	if strings.Contains(got, `"vars"`) {
		t.Fatalf("query result should render as a table, not raw JSON: %s", got)
	}
}

func TestToolEntryErrorRendered(t *testing.T) {
	ev := agentEvent{
		Kind:     "tool-result",
		ToolName: "datalog__query",
		ToolArgs: `{"query":"smb_conn(?)?"}`,
		Result:   "query: anonymous variables are not allowed",
		IsError:  true,
	}
	got := renderContent(toolEntry(ev, true))
	if !strings.Contains(got, "anonymous variables are not allowed") {
		t.Fatalf("error text not rendered: %s", got)
	}
	// A collapsed entry must still signal failure: the summary's status span
	// carries the error class.
	if !strings.Contains(got, "tool-status error") {
		t.Fatalf("summary line does not flag the error: %s", got)
	}

	ev.ToolName = "datalog__set_rules"
	ev.ToolArgs = `{"rules":"bad(X :-"}`
	ev.Result = "parse error at line 1"
	got = renderContent(toolEntry(ev, true))
	if !strings.Contains(got, "parse error at line 1") || !strings.Contains(got, "tool-status error") {
		t.Fatalf("generic tool error not rendered: %s", got)
	}
}

func TestToolErrorText(t *testing.T) {
	// The operator reads prose, not the transport's JSON envelope: every
	// error shape seen in practice unwraps to its message.
	for _, tc := range []struct{ in, want string }{
		{"plain parse error", "plain parse error"},
		{`"quoted message"`, "quoted message"},
		{`{"content":[{"type":"text","text":"query: no such predicate"}],"isError":true}`,
			"query: no such predicate"},
		{`[{"type":"text","text":"first"},{"type":"text","text":"second"}]`, "first\nsecond"},
		{`{"error":"boom"}`, "boom"},
		{`{"error":{"message":"nested boom"}}`, "nested boom"},
		{`{"message":"top-level message"}`, "top-level message"},
	} {
		if got := toolErrorText(tc.in); got != tc.want {
			t.Errorf("toolErrorText(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
	// Unrecognized JSON still shows in full, pretty-printed — never dropped.
	if got := toolErrorText(`{"weird":true}`); !strings.Contains(got, `"weird": true`) {
		t.Errorf("unrecognized envelope not preserved: %q", got)
	}
}

func TestToolEntryElidedArgsInBody(t *testing.T) {
	inner := strings.Repeat("suspicious(H) :- smb_conn(H, S, D). ", 10)
	long := `{"rules":"` + inner + `"}`
	ev := agentEvent{Kind: "tool-call", ToolName: "datalog__set_rules", ToolArgs: long}
	got := renderContent(toolEntry(ev, false))
	if !strings.Contains(got, "…") {
		t.Fatalf("long args not elided on the summary line: %s", got)
	}
	if !strings.Contains(got, inner) {
		t.Fatalf("elided args must stay reachable in the disclosure body: %s", got)
	}

	short := renderContent(toolEntry(agentEvent{Kind: "tool-call", ToolName: "datalog__list_predicates", ToolArgs: `{}`}, false))
	if strings.Contains(short, "<pre>") {
		t.Fatalf("un-elided args need no body copy: %s", short)
	}
}

func TestFormatToolResult(t *testing.T) {
	got := formatToolResult(`{"total":3,"rows":[["a","b"]]}`)
	want := "{\n  \"total\": 3,\n  \"rows\": [\n    [\n      \"a\",\n      \"b\"\n    ]\n  ]\n}"
	if got != want {
		t.Fatalf("JSON not indented:\n%s", got)
	}
	for _, plain := range []string{"3 rows", "", "{not json", "error: boom"} {
		if formatToolResult(plain) != plain {
			t.Fatalf("non-JSON result %q was altered: %q", plain, formatToolResult(plain))
		}
	}
}

func TestRunAgentTurnNoReplyNote(t *testing.T) {
	wb := newMordorWorkbench(t)
	// A turn that runs tools but never composes a reply — small models end
	// on a tool round routinely; silence would read as a hang.
	driver := &fakeDriver{
		events: []agentEvent{
			{Kind: "tool-call", ToolCallID: "t1", ToolName: "datalog__query", ToolArgs: `{}`},
			{Kind: "tool-result", ToolCallID: "t1", ToolName: "datalog__query", ToolArgs: `{}`, Result: "0 rows"},
		},
		stopReason: "tool_calls",
	}

	wb.runAgentTurn(context.Background(), driver, "which hosts?")

	log := renderLog(wb, "agent")
	if !strings.Contains(log, "without a reply (stop reason: tool_calls)") {
		t.Fatalf("expected a no-reply note: %s", log)
	}
}

func TestRunAgentTurnErrorDropsDriver(t *testing.T) {
	wb := newMordorWorkbench(t)
	driver := &fakeDriver{err: errors.New("provider exploded")}
	wb.agent = driver

	wb.runAgentTurn(context.Background(), driver, "hi")

	log := renderLog(wb, "agent")
	if !strings.Contains(log, "turn failed: provider exploded") {
		t.Fatalf("expected terminal error entry: %s", log)
	}
	if !driver.closed {
		t.Fatalf("failed driver was not closed")
	}
	wb.agentMu.Lock()
	defer wb.agentMu.Unlock()
	if wb.agent != nil {
		t.Fatalf("failed driver was not dropped; next prompt would reuse it")
	}
}

func TestRunAgentTurnCancelled(t *testing.T) {
	wb := newMordorWorkbench(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	driver := &fakeDriver{events: []agentEvent{{Kind: "message", Text: "never"}}}

	wb.runAgentTurn(ctx, driver, "hi")

	log := renderLog(wb, "agent")
	if !strings.Contains(log, "turn cancelled") {
		t.Fatalf("expected cancellation entry: %s", log)
	}
}

// -- permission requests -------------------------------------------------------

// blockingPermissionDriver emits one permission event, then blocks Prompt
// until Answer is called (or ctx is cancelled) — the real shape a
// session/request_permission RPC takes in acpDriver, and the shape this
// test needs to answer the request from an HTTP handler WHILE the turn is
// still in flight, matching how the pane actually works (blockingDriver
// above is the same idiom for a plain message turn).
type blockingPermissionDriver struct {
	answered chan answerCall
}

func newBlockingPermissionDriver() *blockingPermissionDriver {
	return &blockingPermissionDriver{answered: make(chan answerCall, 1)}
}

func (d *blockingPermissionDriver) Prompt(ctx context.Context, text string, sink func(agentEvent)) (string, error) {
	sink(agentEvent{
		Kind: "permission", RequestID: "req-1", ToolName: "datalog__set_rules",
		ToolArgs: `{"rules":"bad(X) :- copied_to(X, _)."}`,
		Options: []agentOption{
			{ID: "allow", Name: "Allow", Kind: "allow_once"},
			{ID: "reject", Name: "Reject", Kind: "reject_once"},
		},
	})
	select {
	case call := <-d.answered:
		sink(agentEvent{Kind: "message", Text: "ok, chose " + call.optionID})
		return "stop", nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func (d *blockingPermissionDriver) Answer(requestID, optionID string) error {
	d.answered <- answerCall{requestID, optionID}
	return nil
}

func (d *blockingPermissionDriver) Close() error { return nil }

func TestRunAgentTurnPermissionRendersOptions(t *testing.T) {
	wb := newMordorWorkbench(t)
	driver := newBlockingPermissionDriver()
	wb.agent = driver
	srv := startTestServer(wb)
	defer srv.Close()

	turnDone := make(chan struct{})
	go func() {
		wb.runAgentTurn(context.Background(), driver, "change the rules")
		close(turnDone)
	}()

	// Wait for the permission entry to land before asserting on it or
	// answering — runAgentTurn is running on its own goroutine now, mirroring
	// how the real prompt handler detaches the turn (handleConsolePrompt).
	waitFor(t, func() bool { return strings.Contains(renderLog(wb, "agent"), "permission-option") })

	log := renderLog(wb, "agent")
	for _, want := range []string{
		"agent is waiting for permission",
		"set_rules",
		">Allow<",
		">Reject<",
		"permission-option allow",
		"permission-option reject",
		"requestID=req-1",
		"optionID=allow",
		"optionID=reject",
	} {
		if !strings.Contains(log, want) {
			t.Fatalf("permission entry missing %q: %s", want, log)
		}
	}

	// The pending entry must be tracked so the answer endpoint (outside the
	// turn goroutine's sink) can find it.
	wb.permMu.Lock()
	_, ok := wb.pendingPerm["req-1"]
	wb.permMu.Unlock()
	if !ok {
		t.Fatalf("permission request not tracked in wb.pendingPerm")
	}

	resp := postSignals(t, srv, "/console/answer?requestID=req-1&optionID=allow", map[string]any{})
	defer resp.Body.Close()
	_, _ = io.ReadAll(resp.Body)

	<-turnDone // the turn's own message/cleanup steps race the assertions below otherwise

	log = renderLog(wb, "agent")
	if !strings.Contains(log, "answered: Allow") {
		t.Fatalf("resolved entry does not name the chosen option: %s", log)
	}
	if strings.Contains(log, "permission-option") {
		t.Fatalf("resolved entry still carries buttons: %s", log)
	}

	wb.permMu.Lock()
	_, stillPending := wb.pendingPerm["req-1"]
	wb.permMu.Unlock()
	if stillPending {
		t.Fatalf("answered request was not cleared from wb.pendingPerm")
	}
}

func TestConsoleAnswerUnknownRequestRendersError(t *testing.T) {
	wb := newMordorWorkbench(t)
	srv := startTestServer(wb)
	defer srv.Close()

	// No turn ever ran, so "req-does-not-exist" was never tracked — this
	// covers both a late answer (arrived after the turn already resolved or
	// cancelled it) and a stale page replaying an old requestID. The
	// handler must render an error entry, not panic on the missing map key
	// or a nil driver.
	resp := postSignals(t, srv, "/console/answer?requestID=req-does-not-exist&optionID=allow", map[string]any{})
	defer resp.Body.Close()
	_, _ = io.ReadAll(resp.Body)

	log := renderLog(wb, "agent")
	if !strings.Contains(log, "no longer waiting for an answer") {
		t.Fatalf("expected an error entry for the unknown request: %s", log)
	}
}

// cancelAfterPermissionDriver emits one permission event, then cancels its
// own context (simulating Global Cancel firing while the turn is blocked on
// a permission request) and returns as a cancelled turn does.
type cancelAfterPermissionDriver struct {
	cancel context.CancelFunc
}

func (d *cancelAfterPermissionDriver) Prompt(ctx context.Context, text string, sink func(agentEvent)) (string, error) {
	sink(agentEvent{Kind: "permission", RequestID: "req-2", ToolName: "datalog__query",
		Options: []agentOption{{ID: "allow", Name: "Allow", Kind: "allow_once"}}})
	d.cancel()
	return "", ctx.Err()
}

func (d *cancelAfterPermissionDriver) Answer(requestID, optionID string) error {
	return errors.New("turn already ended")
}

func (d *cancelAfterPermissionDriver) Close() error { return nil }

func TestRunAgentTurnCancelledResolvesPendingPermission(t *testing.T) {
	wb := newMordorWorkbench(t)
	ctx, cancel := context.WithCancel(context.Background())
	driver := &cancelAfterPermissionDriver{cancel: cancel}

	wb.runAgentTurn(ctx, driver, "hi")

	log := renderLog(wb, "agent")
	if !strings.Contains(log, "cancelled: turn ended before the agent received an answer") {
		t.Fatalf("expected the pending permission to morph to cancelled: %s", log)
	}
	if strings.Contains(log, "permission-option") {
		t.Fatalf("cancelled entry still carries live buttons: %s", log)
	}

	wb.permMu.Lock()
	n := len(wb.pendingPerm)
	wb.permMu.Unlock()
	if n != 0 {
		t.Fatalf("pendingPerm not cleared at turn end: %d entries remain", n)
	}
}

// -- auto-allow policy (doc/features/acp-integration.md's "Permission
// requests" bullet: a request gating one of the workbench's own read-only
// tools — query, sample_facts, list_predicates — is answered "allow"
// without the human) --------------------------------------------------------

// TestRunAgentTurnAutoAllowsReadOnlyTool exercises the happy path: a
// permission request titled for the read-only "query" tool, offering both
// an allow_once and a reject_once option. runAgentTurn's sink must call
// driver.Answer with the allow_once option right there (fakeDriver.answered
// records the call), render a "auto-allowed: ..." note instead of buttons,
// and never track the request in wb.pendingPerm at all.
func TestRunAgentTurnAutoAllowsReadOnlyTool(t *testing.T) {
	wb := newMordorWorkbench(t)
	driver := &fakeDriver{
		answerable: true,
		events: []agentEvent{
			{
				Kind: "permission", RequestID: "req-auto-1", ToolName: "query",
				ToolArgs: `{"query":"copied_to(F,H)?"}`,
				Options: []agentOption{
					{ID: "allow_once", Name: "Allow", Kind: "allow_once"},
					{ID: "reject_once", Name: "Reject", Kind: "reject_once"},
				},
			},
			{Kind: "message", Text: "done"},
		},
		stopReason: "stop",
	}

	wb.runAgentTurn(context.Background(), driver, "how many copies?")

	if len(driver.answered) != 1 {
		t.Fatalf("Answer called %d times, want 1: %+v", len(driver.answered), driver.answered)
	}
	if got := driver.answered[0]; got.requestID != "req-auto-1" || got.optionID != "allow_once" {
		t.Fatalf("Answer called with %+v, want {req-auto-1 allow_once}", got)
	}

	log := renderLog(wb, "agent")
	if !strings.Contains(log, "auto-allowed: query") {
		t.Fatalf("expected an auto-allowed note in the transcript: %s", log)
	}
	if strings.Contains(log, "permission-option") {
		t.Fatalf("auto-allowed request still rendered buttons: %s", log)
	}
	if strings.Contains(log, "agent is waiting for permission") {
		t.Fatalf("auto-allowed request rendered the pending-button phrasing: %s", log)
	}

	wb.permMu.Lock()
	n := len(wb.pendingPerm)
	wb.permMu.Unlock()
	if n != 0 {
		t.Fatalf("auto-allowed request was tracked in wb.pendingPerm: %d entries", n)
	}
}

// TestRunAgentTurnMutatingToolStillPrompts is the auto-allow policy's
// negative case: a permission request for set_rules (a MUTATING workbench
// tool) must render the normal interactive buttons and must NOT call
// driver.Answer on the agent's behalf — set_schema/set_rules/sample_input
// replace the human's working documents, so the human decides.
func TestRunAgentTurnMutatingToolStillPrompts(t *testing.T) {
	wb := newMordorWorkbench(t)
	driver := newBlockingPermissionDriver() // emits ToolName "datalog__set_rules"

	turnDone := make(chan struct{})
	go func() {
		wb.runAgentTurn(context.Background(), driver, "change the rules")
		close(turnDone)
	}()

	waitFor(t, func() bool { return strings.Contains(renderLog(wb, "agent"), "permission-option") })

	log := renderLog(wb, "agent")
	if !strings.Contains(log, "agent is waiting for permission") {
		t.Fatalf("expected the pending-button phrasing for a mutating tool: %s", log)
	}
	if len(driver.answered) != 0 {
		t.Fatalf("set_rules should not be auto-answered")
	}

	wb.permMu.Lock()
	_, tracked := wb.pendingPerm["req-1"]
	wb.permMu.Unlock()
	if !tracked {
		t.Fatalf("mutating tool's request must be tracked in wb.pendingPerm")
	}

	// Resolve it manually so the turn (and its goroutine) finishes cleanly.
	if err := driver.Answer("req-1", "allow"); err != nil {
		t.Fatalf("Answer: %v", err)
	}
	<-turnDone
}

// readOnlyNoAllowPermissionDriver emits one permission event for a
// read-only tool whose options carry no allow kind at all (e.g. an agent
// that only offers "always allow"/"always reject" or some future kind this
// package doesn't recognize), then blocks Prompt until Answer is called —
// mirroring blockingPermissionDriver above, needed here (rather than the
// simpler non-blocking fakeDriver) so the turn stays open long enough to
// assert the request is still tracked in wb.pendingPerm before resolving it.
type readOnlyNoAllowPermissionDriver struct {
	answered chan answerCall
}

func (d *readOnlyNoAllowPermissionDriver) Prompt(ctx context.Context, text string, sink func(agentEvent)) (string, error) {
	sink(agentEvent{
		Kind: "permission", RequestID: "req-no-allow", ToolName: "sample_facts",
		Options: []agentOption{
			{ID: "reject_once", Name: "Reject", Kind: "reject_once"},
			{ID: "reject_always", Name: "Always reject", Kind: "reject_always"},
		},
	})
	select {
	case <-d.answered:
		return "stop", nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func (d *readOnlyNoAllowPermissionDriver) Answer(requestID, optionID string) error {
	d.answered <- answerCall{requestID, optionID}
	return nil
}

func (d *readOnlyNoAllowPermissionDriver) Close() error { return nil }

// TestRunAgentTurnReadOnlyToolWithoutAllowOptionStillPrompts covers a
// read-only tool whose options carry no allow kind at all — autoAllowOption
// returns ok=false, so the request must fall through to the normal buttons
// rather than guessing.
func TestRunAgentTurnReadOnlyToolWithoutAllowOptionStillPrompts(t *testing.T) {
	wb := newMordorWorkbench(t)
	driver := &readOnlyNoAllowPermissionDriver{answered: make(chan answerCall, 1)}

	turnDone := make(chan struct{})
	go func() {
		wb.runAgentTurn(context.Background(), driver, "sample something")
		close(turnDone)
	}()

	waitFor(t, func() bool { return strings.Contains(renderLog(wb, "agent"), "permission-option") })

	log := renderLog(wb, "agent")
	if !strings.Contains(log, "agent is waiting for permission") {
		t.Fatalf("expected the pending-button phrasing when no allow option exists: %s", log)
	}
	if strings.Contains(log, "auto-allowed") {
		t.Fatalf("must not claim auto-allowed when no allow option exists: %s", log)
	}

	wb.permMu.Lock()
	_, tracked := wb.pendingPerm["req-no-allow"]
	wb.permMu.Unlock()
	if !tracked {
		t.Fatalf("request without an allow option must be tracked in wb.pendingPerm")
	}

	// Resolve it manually so the turn (and its goroutine) finishes cleanly.
	if err := driver.Answer("req-no-allow", "reject_once"); err != nil {
		t.Fatalf("Answer: %v", err)
	}
	<-turnDone
}

// -- plan checklist ------------------------------------------------------------

func TestRunAgentTurnPlanMorphsInPlace(t *testing.T) {
	wb := newMordorWorkbench(t)
	driver := &fakeDriver{
		events: []agentEvent{
			{Kind: "plan", PlanEntries: []agentPlanEntry{
				{Content: "inspect schema", Status: "in_progress"},
				{Content: "run query", Status: "pending"},
			}},
			{Kind: "plan", PlanEntries: []agentPlanEntry{
				{Content: "inspect schema", Status: "completed"},
				{Content: "run query", Status: "in_progress"},
			}},
		},
		stopReason: "stop",
	}

	wb.runAgentTurn(context.Background(), driver, "plan it out")

	log := renderLog(wb, "agent")
	if strings.Count(log, "plan-checklist") != 1 {
		t.Fatalf("plan should morph one entry in place, not append a second: %s", log)
	}
	if !strings.Contains(log, "inspect schema") || !strings.Contains(log, "run query") {
		t.Fatalf("plan entries missing: %s", log)
	}
	if !strings.Contains(log, "plan-line completed") {
		t.Fatalf("completed status not reflected in the final morph: %s", log)
	}
}

// -- prompt endpoint gating ---------------------------------------------------

func TestConsolePromptGatesOneTurn(t *testing.T) {
	wb := newMordorWorkbench(t)
	srv := startTestServer(wb)
	defer srv.Close()

	// A driver that blocks until released, so the second POST lands while
	// the first turn is still "running".
	release := make(chan struct{})
	blocking := &blockingDriver{release: release}
	wb.agent = blocking

	resp1 := postSignals(t, srv, "/console/prompt", map[string]any{"consolePrompt": "first"})
	_, _ = io.ReadAll(resp1.Body)
	resp1.Body.Close()

	resp2 := postSignals(t, srv, "/console/prompt", map[string]any{"consolePrompt": "second"})
	_, _ = io.ReadAll(resp2.Body)
	resp2.Body.Close()

	close(release)
	waitFor(t, func() bool { return strings.Contains(renderLog(wb, "agent"), "done") })

	log := renderLog(wb, "agent")
	if !strings.Contains(log, "a turn is already running") {
		t.Fatalf("second prompt was not rejected: %s", log)
	}
	if strings.Count(log, `class='console-entry user'`) != 1 {
		t.Fatalf("rejected prompt should not add a user entry: %s", log)
	}
}

type blockingDriver struct {
	release chan struct{}
	closed  bool
}

func (d *blockingDriver) Prompt(ctx context.Context, text string, sink func(agentEvent)) (string, error) {
	select {
	case <-d.release:
	case <-ctx.Done():
		return "", ctx.Err()
	}
	sink(agentEvent{Kind: "message", Text: "done"})
	return "stop", nil
}

func (d *blockingDriver) Answer(requestID, optionID string) error {
	return errors.New("blockingDriver does not issue permission requests")
}

func (d *blockingDriver) Close() error { d.closed = true; return nil }

// waitFor polls cond until it holds or the deadline passes — the prompt
// endpoint hands its turn to a goroutine, so tests must wait for the
// transcript rather than the response.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("condition not met within deadline")
}
