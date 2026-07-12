package view

import (
	html "github.com/swdunlop/html-go"
)

// RulesEditor renders the Datalog Editor pane shell: one textarea using the
// REPL's `.`/`?` terminator convention so `.dl` files paste directly.
// data-bind:rules-text per doc/notes/datastar.md §6 (the editor's own
// content is the canonical document, per design constraint 1). Debounced
// 500ms keydown posts to /rules/check (parse+compile only, refreshing the
// error list — observation 5 keeps keystroke Transforms off the pipeline).
// Run applies via set_rules + query under the 5s timeout, streaming a
// #status fragment (§9) so the button doesn't freeze; data-indicator:_running
// + data-attr:disabled="$_running" gates concurrent Run clicks.
//
//   - #rules-text    — the program textarea (data-bind:rules-text)
//   - #rules-error   — parse/compile error list, line:col prefixed
//   - #status        — Run's streamed progress / timeout report
func RulesEditor() html.Content {
	rulesTextarea := Textarea.
		Set("id", "rules-text").
		Set("data-bind:rules-text").
		Set("data-on:keydown__debounce.500ms", "@post('/rules/check')")

	runButton := ActionButton.
		Set("id", "rules-run").
		Set("data-indicator:_running").
		Set("data-attr:disabled", "$_running").
		Set("data-on:click", "@post('/rules/run')").
		Add(html.Text("Run"))

	return PaneSection.Set("id", "pane-rules-editor").Add(
		PaneHeading.Add(html.Text("Datalog Editor")),
		ErrorList.Set("id", "rules-error"),
		rulesTextarea,
		runButton,
		StatusDiv.Set("id", "status"),
	)
}
