package main

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	html "github.com/swdunlop/html-go"
	"github.com/swdunlop/html-go/datastar"
	"github.com/swdunlop/html-go/tag"
	"swdunlop.dev/pkg/datalog/cmd/datalog/view"
	"swdunlop.dev/pkg/datalog/syntax"
)

// consoleLog is the server-owned scrollback behind the console drawer
// (doc/features/web-ui.md "Console drawer"): entries live here, page loads
// render the whole log into the drawer, and live pages receive each
// append/update over the /events bus. Rendered html.Content is stored
// rather than source data because entries are write-once-then-maybe-morphed
// display artifacts, not queryable state — the session's canonical
// documents stay where they are.
type consoleLog struct {
	mu      sync.Mutex
	seq     uint64
	entries []consoleEntry
}

// consoleEntry is one scrollback entry: which tab's log it belongs to
// ("query" or "agent") and its current rendering. content is replaced
// wholesale on streamed updates (the agent's message/tool entries); the id
// baked into the rendering (view.ConsoleEntry) keeps morphs targeted.
type consoleEntry struct {
	id      uint64
	tab     string
	content html.Content
}

// consoleLogCap bounds the scrollback. Oldest entries fall off the server
// log (so page loads render at most this many); pages already showing a
// dropped entry keep it until their next full navigation, which is fine —
// single user, and the cap exists to bound memory, not enforce display.
const consoleLogCap = 200

// Append adds a new entry to tab's log and returns its id (for later
// Update calls) plus the rendered entry.
func (c *consoleLog) Append(tab, kind string, content ...html.Content) (uint64, html.Content) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seq++
	id := c.seq
	rendered := view.ConsoleEntry(id, kind, content...)
	c.entries = append(c.entries, consoleEntry{id: id, tab: tab, content: rendered})
	if len(c.entries) > consoleLogCap {
		c.entries = c.entries[len(c.entries)-consoleLogCap:]
	}
	return id, rendered
}

// Update replaces entry id's rendering (streamed agent chunks, tool status
// transitions) and returns the new rendering, or nil if the entry has
// fallen off the capped log.
func (c *consoleLog) Update(id uint64, kind string, content ...html.Content) html.Content {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := range c.entries {
		if c.entries[i].id == id {
			rendered := view.ConsoleEntry(id, kind, content...)
			c.entries[i].content = rendered
			return rendered
		}
	}
	return nil
}

// Clear drops every entry belonging to tab, leaving the other tab's
// scrollback (and the id sequence) alone.
func (c *consoleLog) Clear(tab string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	kept := c.entries[:0]
	for _, e := range c.entries {
		if e.tab != tab {
			kept = append(kept, e)
		}
	}
	c.entries = kept
}

// Render returns tab's full scrollback for page-load rendering.
func (c *consoleLog) Render(tab string) []html.Content {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]html.Content, 0, len(c.entries))
	for _, e := range c.entries {
		if e.tab == tab {
			out = append(out, e.content)
		}
	}
	return out
}

// consoleAppend appends an entry and fans the append out to every open page
// over the bus (datastar Mode append into the tab's log div, medea's
// AppendEvent idiom). Returns the entry id for later consoleUpdate calls.
func (wb *workbench) consoleAppend(tab, kind string, content ...html.Content) uint64 {
	id, rendered := wb.console.Append(tab, kind, content...)
	wb.bus.Publish(datastar.Elements(rendered,
		datastar.Selector("#console-"+tab+"-log"),
		datastar.Mode("append"),
	))
	return id
}

// consoleUpdate re-renders entry id in place and morphs it on every open
// page (default morph mode matches by the entry's own id, medea's
// MorphEvent idiom — no selector needed).
func (wb *workbench) consoleUpdate(id uint64, kind string, content ...html.Content) {
	rendered := wb.console.Update(id, kind, content...)
	if rendered == nil {
		return // fell off the capped log; nothing to morph
	}
	wb.bus.Publish(datastar.Elements(rendered))
}

// consoleSignals mirrors the console drawer's server-sent input signals
// (view/console.go's data-bind:console-query / data-bind:console-prompt).
type consoleSignals struct {
	ConsoleQuery  string `json:"consoleQuery"`
	ConsolePrompt string `json:"consolePrompt"`
}

// consoleQueryJobKey gates the Query tab on the jobs set: one console query
// at a time, and Global Cancel (Stop) reaches it like everything else.
const consoleQueryJobKey = "console-query"

// handleConsoleQuery is the Query tab's action (POST /console/query): an
// ad-hoc probe through the exact pipeline Run uses — session.runQuery under
// evalTimeout, job-gated, goroutine-isolated — with the result appended to
// the Query scrollback as an entry instead of replacing a results div. The
// POST's own stream only clears the input; the entry itself arrives via the
// page's /events connection like every other console entry, so the posting
// page and any other open page render it identically.
func (wb *workbench) handleConsoleQuery(w http.ResponseWriter, r *http.Request) {
	// Decode signals BEFORE opening the SSE stream (see handleRulesCheck:
	// RequestStream leaves r.Body unreadable afterward).
	var sig consoleSignals
	decodeErr := datastar.Decode(&sig, r)

	stream, err := datastar.RequestStream(w, r)
	if err != nil {
		return
	}

	if decodeErr != nil {
		wb.consoleAppend("query", "error", html.Text(decodeErr.Error()))
		return
	}
	text := strings.TrimSpace(sig.ConsoleQuery)
	if text == "" {
		return
	}
	// The REPL's `?` terminator is the document convention, but the console
	// input is one query by construction — accept the bare body and add the
	// terminator rather than making the user type it every probe. A `.`
	// terminator is left alone so pasted rules parse as rules and hit the
	// queries-only rejection below instead of a confusing syntax error.
	if !strings.HasSuffix(text, "?") && !strings.HasSuffix(text, ".") {
		text += "?"
	}

	jobCtx, done := wb.jobs.Begin(r.Context(), consoleQueryJobKey)
	if jobCtx == nil {
		wb.consoleAppend("query", "error", html.Text("a console query is already running"))
		return
	}
	defer done()
	// "query", not consoleQueryJobKey: publishBusy speaks the UI's $busy
	// vocabulary (see its doc) — this is what morphs the console Run button
	// into Stop and spins the Query tab while the drawer is closed.
	wb.publishBusy("query")
	defer wb.publishBusy("")

	_ = stream.Emit(datastar.Signal(map[string]any{"consoleQuery": ""}))

	ruleset, err := syntax.ParseAll(text)
	if err != nil {
		wb.consoleAppend("query", "error", queryEcho(text), html.Text(err.Error()))
		return
	}
	if len(ruleset.Rules) > 0 || len(ruleset.AggRules) > 0 {
		wb.consoleAppend("query", "error", queryEcho(text),
			html.Text("the console runs queries only — edit rules in the Datalog Editor"))
		return
	}
	if len(ruleset.Queries) == 0 {
		wb.consoleAppend("query", "error", queryEcho(text), html.Text("no query found"))
		return
	}

	ctx, cancel := context.WithTimeout(jobCtx, evalTimeout)
	defer cancel()

	for i := range ruleset.Queries {
		q := ruleset.Queries[i]
		var blk queryResultBlock
		qErr := <-runRecovered(func() error {
			// Hold h.mu only for the snapshot; the Transform below can
			// run up to evalTimeout and must not freeze the other panes
			// or the MCP tools sharing this mutex.
			wb.h.mu.Lock()
			snap, err := wb.h.sess.snapshotForQuery()
			wb.h.mu.Unlock()
			if err != nil {
				return err
			}
			rs, vars, _, err := snap.runQuery(ctx, &q)
			if err == nil {
				blk = renderQueryResult(q.String(), vars, rs)
			}
			return err
		})
		if ctx.Err() != nil {
			msg := "evaluation timed out, results may be incomplete"
			if ctx.Err() == context.Canceled {
				msg = "query stopped" // user hit Stop (/cancel), not the deadline
			}
			wb.consoleAppend("query", "error", queryEcho(q.String()), html.Text(msg))
			return
		}
		if qErr != nil {
			wb.consoleAppend("query", "error", queryEcho(q.String()), html.Text(qErr.Error()))
			continue
		}
		wb.consoleAppend("query", "query", resultBlock(blk))
	}
}

// handleConsoleClear (POST /console/clear?tab=query|agent) wipes one tab's
// scrollback and morphs the emptied log div onto every open page. Clearing
// the Agent tab is a conversation reset, not just a display wipe: the kit
// driver carries the model's memory across turns, so it is dropped too and
// the next prompt starts a fresh conversation. A turn still running is
// cancelled, not refused — Clear means "start over", so it implies Stop;
// the driver just cannot be closed until the turn's goroutine has released
// the job key, hence the acquire loop below.
func (wb *workbench) handleConsoleClear(w http.ResponseWriter, r *http.Request) {
	tab := r.URL.Query().Get("tab")
	if tab != "query" && tab != "agent" {
		http.Error(w, "unknown console tab", http.StatusBadRequest)
		return
	}
	if _, err := datastar.RequestStream(w, r); err != nil {
		return
	}

	if tab == "agent" {
		wb.jobs.Cancel(agentTurnJobKey)
		jobCtx, done := wb.jobs.Begin(context.Background(), agentTurnJobKey)
		for deadline := time.Now().Add(5 * time.Second); jobCtx == nil; {
			if time.Now().After(deadline) {
				// The turn ignored its cancelled context (a wedged provider
				// call); closing the driver under it would be worse. Leave
				// the transcript alone and say so.
				wb.consoleAppend("agent", "error",
					html.Text("the running turn is not stopping; clear again once it ends"))
				return
			}
			time.Sleep(10 * time.Millisecond)
			jobCtx, done = wb.jobs.Begin(context.Background(), agentTurnJobKey)
		}
		defer done()
		wb.agentMu.Lock()
		d := wb.agent
		wb.agent = nil
		wb.agentMu.Unlock()
		if d != nil {
			_ = d.Close()
		}
	}

	wb.console.Clear(tab)
	wb.bus.Publish(datastar.Elements(view.ConsoleLog(tab)))
}

// handleConsoleAnswer (POST /console/answer?requestID=...&optionID=...) is
// one permission button's click target (agent.go's permissionEntry). The
// query string, not signals, carries the pair — each button is a distinct,
// static (requestID, optionID) baked in at render time, the same idiom
// handleConsoleClear uses for ?tab= (view/console.go's clearButton is the
// precedent this follows).
//
// The driver resolved here is the CURRENT wb.agent read directly under
// agentMu — deliberately NOT wb.agentDriver(), which lazily constructs a
// fresh driver on a nil agent; an Answer with no live turn has nothing to
// construct a driver FOR (acp-integration.md work item 9's explicit
// instruction). requestID unknown to wb.pendingPerm (already resolved,
// already cancelled by turn-end cleanup, or simply never existed — a stale
// browser tab replaying an old page) renders an error entry rather than
// touching the driver at all; Answer erroring on a requestID it does
// recognize (the driver's own bookkeeping disagrees) also renders an error,
// never panics either way.
func (wb *workbench) handleConsoleAnswer(w http.ResponseWriter, r *http.Request) {
	requestID := r.URL.Query().Get("requestID")
	optionID := r.URL.Query().Get("optionID")

	if _, err := datastar.RequestStream(w, r); err != nil {
		return
	}

	wb.permMu.Lock()
	pending, ok := wb.pendingPerm[requestID]
	if ok {
		delete(wb.pendingPerm, requestID)
	}
	wb.permMu.Unlock()
	if !ok {
		wb.consoleAppend("agent", "error",
			html.Text("this permission request is no longer waiting for an answer"))
		return
	}

	wb.agentMu.Lock()
	driver := wb.agent
	wb.agentMu.Unlock()
	if driver == nil {
		wb.consoleUpdate(pending.entryID, "permission",
			permissionResolvedEntry(&pending.event, "error: no agent is running to receive this answer"))
		return
	}

	if err := driver.Answer(requestID, optionID); err != nil {
		wb.consoleUpdate(pending.entryID, "permission",
			permissionResolvedEntry(&pending.event, fmt.Sprintf("error: %v", err)))
		return
	}

	chosen := optionID
	for _, opt := range pending.event.Options {
		if opt.ID == optionID {
			chosen = opt.Name
			break
		}
	}
	wb.consoleUpdate(pending.entryID, "permission",
		permissionResolvedEntry(&pending.event, "answered: "+chosen))
}

// queryEcho renders the query text an entry responds to, so the scrollback
// reads as a transcript even when entries arrive out of typing order.
func queryEcho(q string) html.Content {
	return tag.New("p.query-text", html.Text(strings.TrimSpace(q)))
}
