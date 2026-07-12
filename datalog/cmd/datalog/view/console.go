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
// body, $_consoleTab picks the visible tab. busy is NOT local — it is the
// page-wide run/apply/agent mutex signal ('', 'run', 'apply' or 'agent'),
// flipped over SSE by whichever handler starts/ends a job so every action
// button on every open page reflects it, not just the one that clicked.
func Console(queryLog, agentLog []html.Content) html.Content {
	return tag.New("section#console").
		Set("data-signals", `{_consoleOpen: false, _consoleTab: 'query', busy: '', consoleQuery: '', consolePrompt: ''}`).
		Add(
			consoleBar(),
			tag.New("div#console-body").Set("data-show", "$_consoleOpen").Add(
				consoleTabPanel("query", queryLog, queryInputRow()),
				consoleTabPanel("agent", agentLog, html.Group{agentActivity(), promptInputRow()}),
			),
		)
}

// consoleBar is the drawer's always-visible one-line bar: a disclosure
// toggle, the two tab buttons (which also open the drawer, so a collapsed
// console is one click from either tab), and — pushed to the right edge —
// the busy-status span plus the Clear button. The Agent tab button carries
// oat's [aria-busy] spinner while a turn runs, so even a collapsed drawer
// telegraphs agent activity.
func consoleBar() html.Content {
	return tag.New("div#console-bar",
		tag.New("button#console-toggle.console-tab").
			Set("data-on:click", "$_consoleOpen = !$_consoleOpen").
			Set("data-text", "$_consoleOpen ? '▾ Console' : '▸ Console'").
			Add(html.Text("▸ Console")),
		consoleTabButton("query", "Query"),
		consoleTabButton("agent", "Agent").
			Set("data-spinner", "small").
			Set("data-attr:aria-busy", "$busy === 'agent' ? 'true' : false"),
		tag.New("span#console-status").Set("data-text", busyStatusExpr),
		clearButton(),
	)
}

// busyStatusExpr renders the busy mutex as words: which of run/apply/agent
// holds it, or nothing. This is the one place the whole mutex is spelled
// out, complementing the per-button Stop morphs.
const busyStatusExpr = `$busy === 'agent' ? 'agent turn running…' : $busy === 'run' ? 'run in flight…' : $busy === 'apply' ? 'apply in flight…' : ''`

func consoleTabButton(tab, label string) tag.Interface {
	return tag.New("button.console-tab").
		Set("data-on:click", "$_consoleTab = '"+tab+"'; $_consoleOpen = true").
		Set("data-class:active", "$_consoleTab === '"+tab+"' && $_consoleOpen").
		Add(html.Text(label))
}

// clearButton wipes the visible tab's scrollback (POST /console/clear).
// $_consoleTab is client-local and never travels in signal payloads, so the
// tab rides in the URL instead. Hidden while the drawer is collapsed —
// clearing a log you cannot see would be a surprise. Styled as a bordered
// ghost button on the bar's right edge, NOT as .console-tab: it is a
// destructive action and should not read as navigation.
func clearButton() html.Content {
	return tag.New("button#console-clear").
		Set("data-show", "$_consoleOpen").
		Set("data-on:click", "@post('/console/clear?tab=' + $_consoleTab)").
		Add(html.Text("Clear"))
}

// consoleTabPanel wraps one tab's scrollback and input row. The scrollback
// div's id (#console-query-log / #console-agent-log) is the append target
// for new entries; entries carry their own ids (#c<seq>) so streamed
// updates morph in place without touching siblings.
func consoleTabPanel(tab string, log []html.Content, input html.Content) html.Content {
	return tag.New("div.console-panel").
		Set("data-show", "$_consoleTab === '"+tab+"'").
		Add(
			ConsoleLog(tab, log...),
			input,
		)
}

// ConsoleLog renders one tab's scrollback div (#console-query-log /
// #console-agent-log). Besides page load, it is the morph target a clear
// publishes: an empty ConsoleLog morphs every entry away on open pages.
func ConsoleLog(tab string, entries ...html.Content) html.Content {
	return tag.New("div.console-log#console-" + tab + "-log").Add(entries...)
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

// agentActivity is the chat pane's turn-level activity line, pinned between
// the scrollback and the composer whenever the agent holds the $busy mutex.
// The composer button's overlay ring proved too subtle a tell on its own;
// this line is unmissable while a turn runs and takes no space when idle.
// aria-busy is static — data-show removes the whole line when the mutex
// isn't 'agent', so the spinner never spins unseen.
func agentActivity() html.Content {
	return tag.New("div.agent-activity").
		Set("data-show", "$busy === 'agent'").
		Set("aria-busy", "true").
		Set("data-spinner", "small").
		Add(html.Text("agent turn running…"))
}

// promptInputRow is the Agent tab's input: a chat-style composer — the send
// control is a round icon button sitting inside the textarea's bottom-right
// corner (.prompt-box positions it; the textarea reserves padding so text
// never runs under it). The button is one control with three faces off the
// shared $busy mutex:
//
//   - idle:       ↑ posts the prompt
//   - agent busy: ■ posts /cancel, ringed by oat's [aria-busy] spinner
//     (data-spinner "small overlay" draws the ring over the ■ rather than
//     beside it — overlay mode positions the spinner absolutely)
//   - run/apply busy: disabled, making the mutex visible from this side too
//
// Enter sends (guarded on !$busy so it cannot double-fire mid-turn),
// Shift+Enter inserts a newline via the browser's default behavior.
func promptInputRow() html.Content {
	return tag.New("div.console-input",
		tag.New("div.prompt-box",
			tag.New("textarea#console-prompt[rows=3][placeholder=Ask the agent…]").
				Set("spellcheck", "false").
				Set("data-bind:console-prompt").
				Set("data-on:keydown", "evt.key === 'Enter' && !evt.shiftKey && (evt.preventDefault(), !$busy && @post('/console/prompt'))"),
			tag.New("button#console-send").
				Set("data-spinner", "small overlay").
				Set("data-attr:aria-busy", "$busy === 'agent' ? 'true' : false").
				Set("data-attr:disabled", "$busy === 'run' || $busy === 'apply'").
				Set("data-attr:title", "$busy === 'agent' ? 'stop the running turn' : 'send'").
				Set("data-text", "$busy === 'agent' ? '■' : '↑'").
				Set("data-on:click", "$busy === 'agent' ? @post('/cancel') : @post('/console/prompt')").
				Add(html.Text("↑")),
		),
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
