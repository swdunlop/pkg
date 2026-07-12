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
// schemaText is the session's CURRENT canonical document (design constraint
// 1: the editor content IS the document), rendered as the textarea's
// initial value at page load. selectedRow and output render the current
// jsonfacts-test selection, if any, so a page reload doesn't lose context.
//
//   - #jsonfacts-row     — selected source row, pretty-printed
//   - #schema-text       — the config textarea (data-bind:schema-text)
//   - #jsonfacts-error   — in-form error list, line:col prefixed, rendered
//     between the textarea and the actions row so it pushes the buttons
//     down instead of the textarea above it as errors accumulate
//   - #jsonfacts-output  — live single-row extraction output
func JSONFactsEditor(schemaText string, selectedRow html.Content, output html.Content) html.Content {
	schemaTextarea := Textarea.
		Set("id", "schema-text").
		Set("data-bind:schema-text").
		Set("data-on:keydown__debounce.500ms", "@post('/jsonfacts/preview')").
		Add(html.Text(schemaText))

	applyButton := ActionButton.
		Set("id", "jsonfacts-apply").
		Set("data-indicator:_applying").
		Set("data-attr:disabled", "$_applying").
		Set("data-on:click", "@post('/jsonfacts/apply')").
		Add(html.Text("Apply"))

	// Save writes the SESSION's canonical schemaText to disk — whatever was
	// last Applied, not any unApplied draft still sitting in the textarea
	// above. The title attribute calls this out explicitly per the task's
	// instruction, since it's an easy trap: type, forget to click Apply,
	// click Save, and get the old document on disk.
	saveButton := ActionButton.
		Set("id", "jsonfacts-save").
		Set("title", "writes the applied schema document to disk (not any unapplied draft)").
		Set("data-on:click", "@post('/save/schema')").
		Add(html.Text("Save"))

	return PaneSection.Set("id", "pane-jsonfacts-editor").Add(
		PaneHeading.Add(html.Text("jsonfacts Editor")),
		selectedRow,
		schemaTextarea,
		// ErrorList renders between the textarea and the actions row so a
		// growing error list pushes the buttons down instead of the
		// textarea above it out from under the cursor.
		ErrorList.Set("id", "jsonfacts-error"),
		tag.New("div.actions", applyButton, saveButton),
		output,
	)
}

// JSONFactsRow renders the #jsonfacts-row fragment: the selected source
// record, pretty-printed. pretty is the caller's json.MarshalIndent output
// (or a placeholder message when nothing is selected yet).
func JSONFactsRow(pretty string) html.Content {
	return tag.New("div#jsonfacts-row",
		tag.New("pre", html.Text(pretty)),
	)
}

// JSONFactsNoSelection renders the #jsonfacts-row fragment's placeholder
// state, before any row has been selected via the Data Browser's Test
// button.
func JSONFactsNoSelection() html.Content {
	return tag.New("div#jsonfacts-row",
		tag.New("p.text-light", html.Text("No row selected yet — use \"Test\" in the Data Browser.")),
	)
}

// JSONFactsOutput renders the #jsonfacts-output fragment: the extracted
// facts for the single selected row, one predicate(args...) line per fact.
func JSONFactsOutput(lines []string) html.Content {
	if len(lines) == 0 {
		return tag.New("div#jsonfacts-output",
			tag.New("p.text-light", html.Text("no facts extracted for this row")),
		)
	}
	return tag.New("div#jsonfacts-output",
		tag.New("pre",
			html.Text(joinLines(lines)),
		),
	)
}

// JSONFactsOutputMessage renders the #jsonfacts-output fragment with a
// plain status message (e.g. "no row selected", "N predicates loaded").
func JSONFactsOutputMessage(msg string) html.Content {
	return tag.New("div#jsonfacts-output",
		tag.New("p", html.Text(msg)),
	)
}

// JSONFactsErrors renders the #jsonfacts-error fragment: an in-form error
// list per doc/notes/datastar.md §4, verbatim from the parser/compiler,
// line:col-prefixed where available. An empty errs clears the list (the
// :empty CSS rule hides it).
func JSONFactsErrors(errs []string) html.Content {
	return ErrorList.Set("id", "jsonfacts-error").Add(
		html.Map(errs, func(e string) html.Content {
			return tag.New("li", html.Text(e))
		})...,
	)
}

// joinLines joins lines with newlines for a <pre> block.
func joinLines(lines []string) string {
	out := lines[0]
	for _, l := range lines[1:] {
		out += "\n" + l
	}
	return out
}
