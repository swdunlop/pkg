package main

import (
	"context"
	"net/http"
	"strings"
	"sync"

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
			wb.h.mu.Lock()
			defer wb.h.mu.Unlock()
			rs, vars, _, err := wb.h.sess.runQuery(ctx, &q)
			if err == nil {
				blk = renderQueryResult(q.String(), vars, rs)
			}
			return err
		})
		if ctx.Err() != nil {
			wb.consoleAppend("query", "error", queryEcho(q.String()),
				html.Text("evaluation timed out, results may be incomplete"))
			return
		}
		if qErr != nil {
			wb.consoleAppend("query", "error", queryEcho(q.String()), html.Text(qErr.Error()))
			continue
		}
		wb.consoleAppend("query", "query", resultBlock(blk))
	}
}

// queryEcho renders the query text an entry responds to, so the scrollback
// reads as a transcript even when entries arrive out of typing order.
func queryEcho(q string) html.Content {
	return tag.New("p.query-text", html.Text(strings.TrimSpace(q)))
}
