package view

import (
	"strconv"

	html "github.com/swdunlop/html-go"
	"github.com/swdunlop/html-go/tag"
)

// Console renders the console drawer (doc/features/web-ui.md "Console
// drawer"): a full-width strip beneath both views' three columns, collapsed
// by default to its one-line bar. Two tabs — Query (ad-hoc queries through
// the same query pipeline as Run) and Agent (the chat pane from
// doc/features/acp-integration.md) — with per-tab scrollbacks whose entries
// the server owns; queryLog and agentLog are the session-owned scrollbacks
// rendered at page load, and later entries patch in over the page's /events
// subscription (append for new entries, morph for streamed updates to an
// existing one, following medea's block idiom).
//
// Drawer chrome state is client-local Datastar signals (underscore-prefixed
// so they never travel in POST signal payloads): $_consoleOpen toggles the
// body, $_consoleTab picks the visible tab. agentBusy is NOT local — the
// server flips it over SSE at turn start/end so the send button disables
// for every open page, not just the one that clicked.
func Console(queryLog, agentLog []html.Content) html.Content {
	return tag.New("section#console").
		Set("data-signals", `{_consoleOpen: false, _consoleTab: 'query', agentBusy: false, consoleQuery: '', consolePrompt: ''}`).
		Add(
			consoleBar(),
			tag.New("div#console-body").Set("data-show", "$_consoleOpen").Add(
				consoleTabPanel("query", queryLog, queryInputRow()),
				consoleTabPanel("agent", agentLog, promptInputRow()),
			),
		)
}

// consoleBar is the drawer's always-visible one-line bar: a disclosure
// toggle, the two tab buttons (which also open the drawer, so a collapsed
// console is one click from either tab), and a status span the agent turn
// runner patches (#console-status).
func consoleBar() html.Content {
	return tag.New("div#console-bar",
		tag.New("button#console-toggle.console-tab").
			Set("data-on:click", "$_consoleOpen = !$_consoleOpen").
			Set("data-text", "$_consoleOpen ? '▾ Console' : '▸ Console'").
			Add(html.Text("▸ Console")),
		consoleTabButton("query", "Query"),
		consoleTabButton("agent", "Agent"),
		tag.New("span#console-status").Set("data-text", "$agentBusy ? 'agent turn running…' : ''"),
	)
}

func consoleTabButton(tab, label string) html.Content {
	return tag.New("button.console-tab").
		Set("data-on:click", "$_consoleTab = '"+tab+"'; $_consoleOpen = true").
		Set("data-class:active", "$_consoleTab === '"+tab+"' && $_consoleOpen").
		Add(html.Text(label))
}

// consoleTabPanel wraps one tab's scrollback and input row. The scrollback
// div's id (#console-query-log / #console-agent-log) is the append target
// for new entries; entries carry their own ids (#c<seq>) so streamed
// updates morph in place without touching siblings.
func consoleTabPanel(tab string, log []html.Content, input html.Content) html.Content {
	return tag.New("div.console-panel").
		Set("data-show", "$_consoleTab === '"+tab+"'").
		Add(
			tag.New("div.console-log#console-"+tab+"-log").Add(log...),
			input,
		)
}

// queryInputRow is the Query tab's input: Enter (or the Run button) posts
// the query. The input keeps focus, so a probe-refine loop is all keyboard.
func queryInputRow() html.Content {
	return tag.New("div.console-input",
		tag.New("input#console-query[type=text][placeholder=ancestor(X, Y)?]").
			Set("spellcheck", "false").
			Set("data-bind:console-query").
			Set("data-on:keydown", "evt.key === 'Enter' && @post('/console/query')"),
		ActionButton.
			Set("data-on:click", "@post('/console/query')").
			Add(html.Text("Run")),
	)
}

// promptInputRow is the Agent tab's input. Send disables while a turn runs
// ($agentBusy is server-patched); Enter sends, Shift+Enter inserts a
// newline via the browser's default textarea behavior.
func promptInputRow() html.Content {
	return tag.New("div.console-input",
		tag.New("textarea#console-prompt[rows=2][placeholder=Ask the agent…]").
			Set("spellcheck", "false").
			Set("data-bind:console-prompt").
			Set("data-on:keydown", "evt.key === 'Enter' && !evt.shiftKey && (evt.preventDefault(), @post('/console/prompt'))"),
		ActionButton.
			Set("data-attr:disabled", "$agentBusy").
			Set("data-on:click", "@post('/console/prompt')").
			Add(html.Text("Send")),
	)
}

// ConsoleEntry wraps one scrollback entry. id is the workbench console
// log's sequence number — stable across append and every later morph of the
// same entry — and kind is a CSS hook ("query", "user", "agent", "thought",
// "tool", "error", "note").
func ConsoleEntry(id uint64, kind string, content ...html.Content) html.Content {
	return tag.New("div.console-entry").
		Set("id", consoleEntryID(id)).
		Class(kind).
		Add(content...)
}

func consoleEntryID(id uint64) string {
	return "c" + strconv.FormatUint(id, 10)
}

// ConsoleEntrySelector returns the CSS selector for one entry, for morph
// events targeting it.
func ConsoleEntrySelector(id uint64) string { return "#" + consoleEntryID(id) }
