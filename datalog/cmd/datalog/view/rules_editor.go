package view

import (
	html "github.com/swdunlop/html-go"
	"github.com/swdunlop/html-go/tag"
)

// RulesEditor renders the Datalog Editor pane shell: one textarea using the
// REPL's `.`/`?` terminator convention so `.dl` files paste directly.
// data-bind:rules-text per doc/notes/datastar.md §6 (the editor's own
// content is the canonical document, per design constraint 1). Debounced
// 500ms keydown posts to /rules/check (parse+compile only, refreshing the
// error list — observation 5 keeps keystroke Transforms off the pipeline).
// Run applies via setRulesWithQueries + runQuery under the 5s timeout,
// streaming a #status fragment (§9) so the button doesn't freeze; the
// server-patched $busy mutex (view/console.go) gates concurrent clicks and
// morphs Run into Stop while its own job runs (see BusyActionButton).
//
// rulesText is the session's current canonical document, rendered into the
// textarea at page load (and whenever an agent-side set_rules patches this
// pane — see fact_browser.go's publishSessionChanged, which does not
// currently repaint this pane; agent-authored rules-editor patch-back is
// left for a later wave since this task only covers Run/check).
//
//   - #rules-text    — the program textarea (data-bind:rules-text)
//   - #rules-error   — parse/compile error list, line:col prefixed, rendered
//     between the textarea and the actions row so it pushes the buttons
//     down instead of the textarea above it as errors accumulate
//   - #rules-results — Run's query results (one block per query)
//   - #status        — Run's streamed progress / timeout report
func RulesEditor(rulesText string) html.Content {
	rulesTextarea := Textarea.
		Set("id", "rules-text").
		Set("data-bind:rules-text").
		Set("data-on:keydown__debounce.500ms", "@post('/rules/check')").
		Add(html.Text(rulesText))

	runButton := BusyActionButton("rules-run", "run", "Run", "/rules/run")

	// No Save button: disk is canonical and the fsnotify watcher reloads it
	// (doc/features/workbench-v2.md design decision 3 — "The Save button and
	// save-time git commits are gone; git is the human's job in the
	// terminal"). Run still applies the document to session memory only.

	// Cursor-position indicator: the design mentions one to aid navigating
	// line:col errors. A live line:col readout needs the textarea's
	// selectionStart translated into a line/column pair, which requires a
	// scan over the text content on every keystroke/click — more than a
	// trivial inline data-* expression can do (data-text only evaluates
	// expressions against signals, and there is no built-in Datastar
	// expression for "count newlines before selectionStart"). Per the task's
	// explicit instruction, this is skipped rather than reached for
	// client-side JS; noted here as a deviation from the design doc.

	return PaneSection.Set("id", "pane-rules-editor").Add(
		PaneHeadingWithNav("Datalog Editor", "rules"),
		rulesTextarea,
		// ErrorList renders between the textarea and the actions row so a
		// growing error list pushes the buttons down instead of the
		// textarea above it out from under the cursor.
		ErrorList.Set("id", "rules-error"),
		tag.New("div.actions", runButton),
		StatusDiv.Set("id", "status"),
		tag.New("div#rules-results"),
	)
}
