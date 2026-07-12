// Package view builds the workbench's HTML with html-go rather than
// html/template: four panes share table/pagination/error-list shapes, and
// compile-time checking on field access matters more here than
// designer-editable markup (doc/features/web-ui.md design constraint 4).
package view

import (
	html "github.com/swdunlop/html-go"
	"github.com/swdunlop/html-go/tag"
)

// Cached tag prototypes shared across panes. Every mutator on tag.Interface
// returns a copy (never mutates the receiver), so these package-level vars
// are safe to share and derive from at every call site — see
// doc/notes/datastar.md Appendix A.4/A.6. Factoring the Datastar attribute
// noise here lets pane code read like markup.
var (
	// PaneSection is the outer shell for one of the four workbench panes.
	// Callers Set("id", ...) and Add(...) their pane's content.
	PaneSection = tag.New("section.pane")

	// PaneHeading is an h2 heading for a pane section.
	PaneHeading = tag.New("h2")

	// Table/TableHead/TableBody/TableRow/TableCell are the semantic table
	// shapes the Data Browser and Fact Browser both need for paginated rows.
	Table      = tag.New("table")
	TableHead  = tag.New("thead")
	TableBody  = tag.New("tbody")
	TableRow   = tag.New("tr")
	TableCell  = tag.New("td")
	TableHCell = tag.New("th")

	// ErrorList is the in-form error surface per doc/notes/datastar.md §4:
	// line:col-prefixed parser/compiler errors, not a toast, since the fix
	// is the user's to make. Rendered with no whitespace between tags so
	// the :empty CSS rule in page.go can hide it when there are no errors.
	ErrorList = tag.New("ul.errors")

	// ActionButton is a long-running-action button per doc/notes/datastar.md
	// §9: data-indicator sets a local busy signal while the request is in
	// flight, data-attr:disabled prevents double-submission. Callers derive
	// with .Set("data-indicator:_name", ...) style suffixes are not
	// supported by html-go's Set signature directly on the attribute name,
	// so the indicator/disabled attributes are threaded through the
	// selector string at the call site (e.g.
	// tag.New("button[data-indicator:_running][data-attr:disabled=$_running]")).
	ActionButton = tag.New("button.action")

	// Textarea is the shared shape for the jsonfacts/rules editors' raw
	// text documents (data-bind wiring added at the call site, since the
	// bound signal name differs per editor).
	Textarea = tag.New("textarea.editor")

	// StatusDiv is a small status/progress fragment target
	// (doc/notes/datastar.md §9's `#status` div), rendered empty by default
	// so the `:empty { display: none }` rule in page.go hides it.
	StatusDiv = tag.New("div.status")
)

// When returns content if cond is true, otherwise an empty html.Group{} —
// never nil, per doc/notes/datastar.md Appendix A.5 (nil html.Content
// panics at append time).
func When(cond bool, content html.Content) html.Content {
	if cond {
		return content
	}
	return html.Group{}
}
