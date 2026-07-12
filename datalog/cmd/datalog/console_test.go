package main

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	html "github.com/swdunlop/html-go"
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
// isolation).
type fakeDriver struct {
	events     []agentEvent
	stopReason string
	err        error
	closed     bool
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
	return errors.New("fakeDriver does not issue permission requests")
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
