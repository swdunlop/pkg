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
// streaming a #status fragment (§9) so the button doesn't freeze;
// data-indicator:_running + data-attr:disabled="$_running" gates concurrent
// Run clicks.
//
// rulesText is the session's current canonical document, rendered into the
// textarea at page load (and whenever an agent-side set_rules patches this
// pane — see fact_browser.go's publishSessionChanged, which does not
// currently repaint this pane; agent-authored rules-editor patch-back is
// left for a later wave since this task only covers Run/check).
//
//   - #rules-text    — the program textarea (data-bind:rules-text)
//   - #rules-error   — parse/compile error list, line:col prefixed
//   - #rules-results — Run's query results (one block per query)
//   - #status        — Run's streamed progress / timeout report
func RulesEditor(rulesText string) html.Content {
	rulesTextarea := Textarea.
		Set("id", "rules-text").
		Set("data-bind:rules-text").
		Set("data-on:keydown__debounce.500ms", "@post('/rules/check')").
		Add(html.Text(rulesText))

	runButton := ActionButton.
		Set("id", "rules-run").
		Set("data-indicator:_running").
		Set("data-attr:disabled", "$_running").
		Set("data-on:click", "@post('/rules/run')").
		Add(html.Text("Run"))

	// Save writes the SESSION's canonical rulesText to disk — whatever was
	// last Run, not any un-Run draft still sitting in the textarea above.
	// The title attribute calls this out explicitly, since it's an easy
	// trap: type, forget to click Run, click Save, and get the old document
	// on disk.
	saveButton := ActionButton.
		Set("id", "rules-save").
		Set("title", "writes the applied rules document to disk (not any unapplied draft)").
		Set("data-on:click", "@post('/save/rules')").
		Add(html.Text("Save"))

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
		PaneHeading.Add(html.Text("Datalog Editor")),
		ErrorList.Set("id", "rules-error"),
		rulesTextarea,
		runButton,
		saveButton,
		StatusDiv.Set("id", "status"),
		tag.New("div#rules-results"),
	)
}
