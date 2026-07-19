package view

import (
	"fmt"

	html "github.com/swdunlop/html-go"
	"github.com/swdunlop/html-go/tag"
)

// This file is the one place cross-links into the browser are built
// (doc/features/workbench-v2.md phase 4, design decision 2: "the
// conversation is the index into the browser"). Every surface that names a
// predicate or a rule group — the Schema and Rules tabs, the why? tree,
// transcript query results and CRUD tool entries — renders its link through
// FactsLink or RulesLink, so navigation behavior can never drift between
// surfaces. Names spliced into the click expressions are datalog
// identifiers ([a-zA-Z0-9_]), the same splice-safety argument WhyButton's
// doc comment makes.

// FactsLink renders a predicate/arity as a deep link into the Facts tab:
// the click flips the client-local $_browserTab, loads the predicate's
// first facts page into its (always-present) container, and scrolls that
// container into view. The scroll runs before the fragment lands — the
// empty container already sits at the predicate's row, so the viewport is
// right even while the page loads. A predicate absent from the current
// evaluation simply has no container; the patch and scroll are then no-ops
// and the reader still lands on the Facts tab.
func FactsLink(name string, arity int, label string) html.Content {
	return tag.New("a.pred-link").
		Set("data-on:click", fmt.Sprintf(
			"$_browserTab = 'facts'; @get('/facts/%s/%d'); document.getElementById('%s')?.scrollIntoView()",
			name, arity, factsContainerID(name, arity))).
		Add(html.Text(label))
}

// RulesLink renders a rule group as a deep link into the Rules tab: the
// click flips $_browserTab and loads the group's detail pane. A group
// deleted since the link rendered lands on the detail pane's own in-pane
// error (RuleGroupDetailError), not a broken page.
func RulesLink(head string, arity int, label string) html.Content {
	return tag.New("a.pred-link").
		Set("data-on:click", fmt.Sprintf(
			"$_browserTab = 'rules'; @get('/rules/%s/%d')", head, arity)).
		Add(html.Text(label))
}

// SchemaLink renders a link that flips to the Schema tab — the coarse
// navigation for schema-side surfaces (matchers, sources, declarations),
// which have no per-item load endpoint; the tab itself is the detail.
func SchemaLink(label string) html.Content {
	return tag.New("a.pred-link").
		Set("data-on:click", "$_browserTab = 'schema'").
		Add(html.Text(label))
}
