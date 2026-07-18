package view

import (
	"strings"

	html "github.com/swdunlop/html-go"
	"github.com/swdunlop/html-go/tag"
)

// JSONFactsEditor renders the jsonfacts Editor pane shell: a config textarea
// plus live output. The tested row itself lives in the Data Browser (it
// highlights the selected row there, per doc/features/web-ui.md — no need
// to duplicate the record here too). The config textarea binds
// data-bind:schema-text per doc/notes/datastar.md §6 — the content is
// genuinely observed keystroke-by-keystroke — with a 500ms-debounced
// keydown posting to /jsonfacts/preview (extracts the single selected row
// only, per the design's "fast feedback against a representative sample").
// Apply follows the streaming-progress shape (§9), gated by the
// server-patched $busy mutex which also morphs it into Stop while its own
// job runs (see BusyActionButton).
//
// schemaText is the session's CURRENT canonical document (design constraint
// 1: the editor content IS the document), rendered as the textarea's
// initial value at page load. output renders the current jsonfacts-test
// selection's extraction, if any, so a page reload doesn't lose context.
//
//   - #schema-text       — the config textarea (data-bind:schema-text)
//   - #jsonfacts-output  — live single-row extraction output, rendered
//     between the textarea and the actions row so a test run doesn't
//     reflow the buttons below it
//   - #jsonfacts-error   — in-form error list, line:col prefixed, rendered
//     between the output and the actions row so it pushes the buttons
//     down instead of the content above it as errors accumulate
func JSONFactsEditor(schemaText string, output html.Content) html.Content {
	schemaTextarea := Textarea.
		Set("id", "schema-text").
		Set("data-bind:schema-text").
		Set("data-on:keydown__debounce.500ms", "@post('/jsonfacts/preview')").
		Add(html.Text(schemaText))

	applyButton := BusyActionButton("jsonfacts-apply", "apply", "Apply", "/jsonfacts/apply")

	// No Save button: disk is canonical and the fsnotify watcher reloads it
	// (doc/features/workbench-v2.md design decision 3). Apply still applies
	// the document to session memory only.

	return PaneSection.Set("id", "pane-jsonfacts-editor").Add(
		PaneHeadingWithNav("jsonfacts Editor", "facts"),
		schemaTextarea,
		// output renders directly below the textarea, above the actions
		// row, so running a test doesn't reflow the buttons underneath it.
		output,
		// ErrorList renders between the output and the actions row so a
		// growing error list pushes the buttons down instead of the
		// content above it out from under the cursor.
		ErrorList.Set("id", "jsonfacts-error"),
		tag.New("div.actions", applyButton),
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
	return strings.Join(lines, "\n")
}
