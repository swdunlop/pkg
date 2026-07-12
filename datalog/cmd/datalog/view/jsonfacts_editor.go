package view

import (
	html "github.com/swdunlop/html-go"
	"github.com/swdunlop/html-go/tag"
)

// JSONFactsEditor renders the jsonfacts Editor pane shell: a three-pane grid
// (selected row, config textarea, live output). The config textarea binds
// data-bind:schema-text per doc/notes/datastar.md §6 — the content is
// genuinely observed keystroke-by-keystroke — with a 500ms-debounced
// keydown posting to /jsonfacts/preview (extracts the single selected row
// only, per the design's "fast feedback against a representative sample").
// Apply follows the streaming-progress shape (§9): data-indicator:_applying
// + data-attr:disabled="$_applying" on the button.
//
//   - #jsonfacts-row     — selected source row, pretty-printed
//   - #schema-text       — the config textarea (data-bind:schema-text)
//   - #jsonfacts-output  — live single-row extraction output
//   - #jsonfacts-error   — in-form error list, line:col prefixed
func JSONFactsEditor() html.Content {
	schemaTextarea := Textarea.
		Set("id", "schema-text").
		Set("data-bind:schema-text").
		Set("data-on:keydown__debounce.500ms", "@post('/jsonfacts/preview')")

	applyButton := ActionButton.
		Set("id", "jsonfacts-apply").
		Set("data-indicator:_applying").
		Set("data-attr:disabled", "$_applying").
		Set("data-on:click", "@post('/jsonfacts/apply')").
		Add(html.Text("Apply"))

	return PaneSection.Set("id", "pane-jsonfacts-editor").Add(
		PaneHeading.Add(html.Text("jsonfacts Editor")),
		ErrorList.Set("id", "jsonfacts-error"),
		tag.New("div#jsonfacts-row"),
		schemaTextarea,
		applyButton,
		tag.New("div#jsonfacts-output"),
	)
}
