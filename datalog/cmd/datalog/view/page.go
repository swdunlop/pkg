package view

import (
	html "github.com/swdunlop/html-go"
	"github.com/swdunlop/html-go/tag"
)

// Page is the full-page shell for GET / — doctype, head, and the four-pane
// body — implementing html.Content per doc/notes/datastar.md Appendix A.8's
// layout-as-struct idiom. Fragment responses (the SSE action/subscription
// endpoints) never construct a Page; they emit pane-scoped html.Content
// directly.
type Page struct {
	Title string

	// DataBrowser, JSONFactsEditor, RulesEditor, FactBrowser are the four
	// workspace panes (doc/features/web-ui.md). Each pane's view builder
	// lives in its own view/<pane>.go file so later waves can flesh out
	// pane content without touching this shell.
	DataBrowser     html.Content
	JSONFactsEditor html.Content
	RulesEditor     html.Content
	FactBrowser     html.Content
}

// emptyCSS hides the shared error/status/toast divs when they render with
// no content. Rendered with NO whitespace between the divs' open/close tags
// at the call site (see tags.go's ErrorList/StatusDiv) — the :empty
// pseudo-class is whitespace-sensitive, so a stray newline or space between
// tags defeats the rule (doc/notes/datastar.md §3).
const emptyCSS = `#toast:empty,.errors:empty,.status:empty{display:none}`

func (p Page) AppendHTML(buf []byte) []byte {
	buf = append(buf, "<!doctype html>\n"...)
	doc := tag.New("html[lang=en]",
		head(p),
		body(p),
	)
	return doc.AppendHTML(buf)
}

func head(p Page) html.Content {
	title := p.Title
	if title == "" {
		title = "datalog workbench"
	}
	return tag.New("head",
		tag.New("meta[charset=utf-8]"),
		tag.New("meta[name=viewport][content=width=device-width, initial-scale=1]"),
		tag.New("title", html.Text(title)),
		oatTag,
		tag.New("style", html.HTML(emptyCSS)),
		datastarTag,
	)
}

func body(p Page) html.Content {
	return tag.New("body",
		tag.New("header#chrome",
			tag.New("h1", html.Text("datalog workbench")),
			cancelButton,
		),
		tag.New("div#toast"),
		tag.New("main#workbench",
			p.DataBrowser,
			p.JSONFactsEditor,
			p.RulesEditor,
			p.FactBrowser,
		),
	)
}

var (
	// oatTag links the self-hosted oat.css base (view.OatCSS, served by
	// serve.go's GET /oat.css handler).
	oatTag = tag.New("link[rel=stylesheet][href=/oat.css]")

	// datastarTag loads Datastar pinned to a specific version with a
	// Subresource Integrity hash, copied verbatim from
	// ~/src/medea/ui/skeleton.go so a compromised CDN cannot silently swap
	// the file. Bump the version and regenerate the integrity hash together.
	datastarTag = tag.New("script[type=module][crossorigin=anonymous]").
			Set("src", "https://cdn.jsdelivr.net/gh/starfederation/datastar@v1.0.1/bundles/datastar.js").
			Set("integrity", "sha384-dWn5jta+MrFAhwrzi4llarDQkaQE0zW2lreXrV0yK15W0A7TrtfGyIyji04PLUY7")

	// cancelButton is the Global Cancel emergency brake (doc/features/web-ui.md
	// "Execution sandbox"): fires every in-flight job's CancelFunc.
	cancelButton = tag.New("button#cancel[data-on:click=@post('/cancel')]", html.Text("Cancel"))
)
