package main

import (
	"fmt"
	"net/http"
	"strings"
	"sync"

	html "github.com/swdunlop/html-go"
	"github.com/swdunlop/html-go/datastar"
	"github.com/swdunlop/html-go/tag"
	"swdunlop.dev/pkg/datalog/cmd/datalog/view"
)

// consoleLog is the server-owned transcript scrollback: entries live here,
// page loads render a conversation's log into its transcript div, and live
// pages receive each append/update over the /events bus. tab is the
// conversation id (a vestigial name from the v1 console drawer's
// query/agent tabs; the mechanism carried over unchanged into phase 2's
// per-conversation transcripts). Rendered html.Content is stored rather
// than source data because entries are write-once-then-maybe-morphed
// display artifacts, not queryable state — the conversation's canonical
// history is its kit session file (conversations_http.go's seedTranscript
// rebuilds this cache from it after a restart).
type consoleLog struct {
	mu      sync.Mutex
	seq     uint64
	entries []consoleEntry
}

// consoleEntry is one scrollback entry: which conversation's log it
// belongs to and its current rendering. content is replaced wholesale on
// streamed updates (the agent's message/tool entries); the id baked into
// the rendering (view.ConsoleEntry) keeps morphs targeted.
type consoleEntry struct {
	id      uint64
	tab     string
	content html.Content
}

// consoleLogCap bounds the scrollback across ALL conversations. Oldest
// entries fall off the server log; a long-history conversation re-renders
// from its session file on the next restart-then-open anyway
// (seedTranscript), so the cap bounds memory, not history.
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

// Clear drops every entry belonging to tab (conversation delete), leaving
// other conversations' scrollback (and the id sequence) alone.
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
// over the bus (datastar Mode append into the conversation's transcript
// div, medea's AppendEvent idiom). Returns the entry id for later
// consoleUpdate calls.
func (wb *workbench) consoleAppend(tab, kind string, content ...html.Content) uint64 {
	id, rendered := wb.console.Append(tab, kind, content...)
	wb.bus.Publish(datastar.Elements(rendered,
		datastar.Selector(view.LogSelector(tab)),
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

// handleAnswer (POST /answer?requestID=...&optionID=...) is one permission
// button's click target (agent.go's permissionEntry). The query string,
// not signals, carries the pair — each button is a distinct, static
// (requestID, optionID) baked in at render time.
//
// The driver resolved here is the CURRENT wb.agent read directly under
// agentMu — deliberately NOT constructed on demand; an Answer with no live
// turn has nothing to construct a driver FOR (acp-integration.md work item
// 9's explicit instruction). requestID unknown to wb.pendingPerm (already
// resolved, already cancelled by turn-end cleanup, or simply never existed
// — a stale browser tab replaying an old page) renders an error entry into
// the running conversation's transcript rather than touching the driver at
// all; Answer erroring on a requestID it does recognize (the driver's own
// bookkeeping disagrees) also renders an error, never panics either way.
func (wb *workbench) handleAnswer(w http.ResponseWriter, r *http.Request) {
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
		// The button bakes its own conversation id into the URL (see
		// permissionEntry), so even a request the server no longer tracks
		// can anchor its error to the transcript the click came from; the
		// running conversation is the fallback for a URL predating that.
		tab := r.URL.Query().Get("tab")
		if tab == "" {
			tab, _ = wb.turnGate.RunningID()
		}
		if tab != "" {
			wb.consoleAppend(tab, "error",
				html.Text("this permission request is no longer waiting for an answer"))
		}
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

// queryEcho renders the query text an entry responds to, so the transcript
// reads as a dialogue even when entries arrive out of typing order (the
// `?` composer command's result rendering, plus agent query surfaces).
func queryEcho(q string) html.Content {
	return tag.New("p.query-text", html.Text(strings.TrimSpace(q)))
}
