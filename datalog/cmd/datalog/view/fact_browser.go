package view

import (
	html "github.com/swdunlop/html-go"
	"github.com/swdunlop/html-go/tag"
)

// FactBrowser renders the Fact Browser pane shell: a predicate list (EDB/IDB
// labeled) that expands into paged facts. This pane owns the page's one
// long-lived subscription connection (doc/notes/datastar.md §8): the
// data-init div opens /events on page load with openWhenHidden so agent-
// triggered Transforms still patch the page when the tab is backgrounded,
// and requestCancellation: 'disabled' so the subscription survives
// Datastar's default abort-on-new-request behavior.
//
//   - #predicates    — predicate list, patched by the /events subscription
//     and by GET /facts/{predicate}/{arity} row expansion
func FactBrowser() html.Content {
	subscribe := tag.New("div").
		Set("data-init", "@get('/events', {openWhenHidden: true, requestCancellation: 'disabled'})")

	return PaneSection.Set("id", "pane-fact-browser").Add(
		PaneHeading.Add(html.Text("Fact Browser")),
		subscribe,
		tag.New("div#predicates"),
	)
}
