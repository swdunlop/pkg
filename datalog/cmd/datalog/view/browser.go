package view

import (
	html "github.com/swdunlop/html-go"
	"github.com/swdunlop/html-go/tag"
)

// Browser renders the page's right half (doc/features/workbench-v2.md
// design decision 2): a read-only browser with four tabs —
// Data | Schema | Rules | Facts — selected by the client-local
// $_browserTab signal (underscore-prefixed so it never travels in POST
// signal payloads; doc/notes/datastar.md's chrome-state convention).
//
// The Schema and Rules panel contents are built by package main (phase 3's
// structural renderings read engine state) and carry their own stable ids
// (#schema-panel, #rules-panel) so publishSessionChanged can re-patch them
// after agent CRUD or an fsnotify reload without a page load. baseFacts and
// derivedFacts are likewise package main's initial #predicates-{base,derived}
// fragments (renderPredicates), so the Facts tab — loading tell included —
// is present on first paint rather than only after /events connects (see
// FactBrowser's doc comment).
func Browser(schema, rules, baseFacts, derivedFacts html.Content) html.Content {
	return tag.New("section#browser").
		Set("data-signals", `{_browserTab: 'data'}`).
		Add(
			browserTabBar(),
			browserPanel("data", DataBrowser()),
			browserPanel("schema", schema),
			browserPanel("rules", rules),
			browserPanel("facts", factsPanel(baseFacts, derivedFacts)),
		)
}

func browserTabBar() html.Content {
	return tag.New("div#browser-tabs",
		browserTabButton("data", "Data"),
		browserTabButton("schema", "Schema"),
		browserTabButton("rules", "Rules"),
		browserTabButton("facts", "Facts"),
	)
}

func browserTabButton(tabName, label string) html.Content {
	return tag.New("button.browser-tab").
		Set("data-on:click", "$_browserTab = '"+tabName+"'").
		Set("data-class:active", "$_browserTab === '"+tabName+"'").
		Add(html.Text(label))
}

func browserPanel(tabName string, content html.Content) html.Content {
	return tag.New("div.browser-panel#browser-"+tabName).
		Set("data-show", "$_browserTab === '"+tabName+"'").
		Add(content)
}

// factsPanel is the Facts tab: the v1 base and derived Fact Browser shells
// stacked, plus the why? output surface (WhyOutput) their derived-fact
// rows' why? buttons render into.
func factsPanel(baseFacts, derivedFacts html.Content) html.Content {
	return html.Group{
		tag.New("div#why-output"),
		FactBrowser("base", "Base Facts", baseFacts),
		FactBrowser("derived", "Derived Facts", derivedFacts),
	}
}

// WhyOutput renders the #why-output fragment the Facts tab's why? buttons
// patch (fact_browser.go's handleWhy): a WhyTree (why_tree.go's structural
// derivation rendering) on success. An error replaces the same div, so only
// one why? result shows at a time.
func WhyOutput(content html.Content) html.Content {
	return tag.New("div#why-output").Add(content)
}

// WhyError is the failed why? rendering.
func WhyError(msg string) html.Content {
	return tag.New("ul.errors", tag.New("li", html.Text(msg)))
}
