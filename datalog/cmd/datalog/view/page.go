package view

import (
	html "github.com/swdunlop/html-go"
	"github.com/swdunlop/html-go/tag"
)

// Page is the full-page shell for the workbench — doctype, head, and a
// two-half body (doc/features/workbench-v2.md design decision 2:
// conversations left, browser right), implementing html.Content per
// doc/notes/datastar.md Appendix A.8's layout-as-struct idiom. Fragment
// responses (the SSE action/subscription endpoints) never construct a
// Page; they emit fragment-scoped html.Content directly.
type Page struct {
	Title string

	// Left is the conversation half (ConversationPane): rail, transcript,
	// composer. Right is the read-only browser half (Browser): the
	// Data | Schema | Rules | Facts tab set.
	Left  html.Content
	Right html.Content
}

// emptyCSS hides the shared error/status/toast divs when they render with
// no content. Rendered with NO whitespace between the divs' open/close tags
// at the call site (see tags.go's ErrorList/StatusDiv) — the :empty
// pseudo-class is whitespace-sensitive, so a stray newline or space between
// tags defeats the rule (doc/notes/datastar.md §3).
const emptyCSS = `#toast:empty,.errors:empty,.status:empty,#why-output:empty{display:none}`

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
		title = "Datalog Workbench"
	}
	return tag.New("head",
		tag.New("meta[charset=utf-8]"),
		tag.New("meta[name=viewport][content=width=device-width, initial-scale=1]"),
		tag.New("title", html.Text(title)),
		oatTag,
		workbenchTag,
		tag.New("style", html.HTML(emptyCSS)),
		datastarTag,
	)
}

func body(p Page) html.Content {
	return tag.New("body",
		tag.New("div#toast"),
		// The page's one long-lived /events subscription
		// (doc/notes/datastar.md §8) is page-scoped: predicate lists, busy
		// signals, transcript appends, and rail refreshes all ride this one
		// connection regardless of which half they morph.
		tag.New("div").Set("data-init", "@get('/events', {openWhenHidden: true, requestCancellation: 'disabled'})"),
		tag.New("main#workbench", p.Left, p.Right),
	)
}

// Toast renders the #toast fragment: a system-level surface per
// doc/notes/datastar.md §4 (toast vs in-form errors) — used for conditions
// that aren't the user's fault to fix in a form. isError adds an "error"
// class so the two states can be styled differently; rendered with no
// whitespace between the outer div's tags so page.go's `#toast:empty` CSS
// rule still applies once a subsequent empty Toast (if ever needed) clears
// it.
func Toast(msg string, isError bool) html.Content {
	t := tag.New("div#toast")
	if isError {
		t = t.Class("error")
	}
	return t.Add(html.Text(msg))
}

var (
	// oatTag links the self-hosted oat.css base (view.OatCSS, served by
	// serve.go's GET /oat.css handler).
	oatTag = tag.New("link[rel=stylesheet][href=/oat.css]")

	// workbenchTag links the workbench's chrome layer (view.WorkbenchCSS),
	// linked AFTER oat so its unlayered rules also win the source-order
	// tiebreak against any of oat's own unlayered rules.
	workbenchTag = tag.New("link[rel=stylesheet][href=/workbench.css]")

	// datastarTag loads Datastar pinned to a specific version with a
	// Subresource Integrity hash, copied verbatim from
	// ~/src/medea/ui/skeleton.go so a compromised CDN cannot silently swap
	// the file. Bump the version and regenerate the integrity hash together.
	datastarTag = tag.New("script[type=module][crossorigin=anonymous]").
			Set("src", "https://cdn.jsdelivr.net/gh/starfederation/datastar@v1.0.1/bundles/datastar.js").
			Set("integrity", "sha384-dWn5jta+MrFAhwrzi4llarDQkaQE0zW2lreXrV0yK15W0A7TrtfGyIyji04PLUY7")
)

// BusyActionButton derives an ActionButton participating in the page-wide
// busy mutex ($busy): while $busy holds this button's own key it morphs
// into a spinner-ringed Stop posting /cancel (there is no standalone Stop
// button — Global Cancel lives on whichever button started the work), and
// while $busy holds a DIFFERENT key it greys out, making the mutex visible
// at every action row.
func BusyActionButton(id, key, label, action string) tag.Interface {
	return ActionButton.
		Set("id", id).
		Set("data-spinner", "small").
		Set("data-attr:aria-busy", "$busy === '"+key+"' ? 'true' : false").
		Set("data-attr:disabled", "$busy && $busy !== '"+key+"'").
		Set("data-text", "$busy === '"+key+"' ? 'Stop' : '"+label+"'").
		Set("data-on:click", "$busy === '"+key+"' ? @post('/cancel') : @post('"+action+"')").
		Add(html.Text(label))
}
