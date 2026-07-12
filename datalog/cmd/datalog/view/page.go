package view

import (
	html "github.com/swdunlop/html-go"
	"github.com/swdunlop/html-go/tag"
)

// Page is the full-page shell for the workbench's two views — doctype,
// head, and a three-column body, implementing html.Content per
// doc/notes/datastar.md Appendix A.8's layout-as-struct idiom. There is no
// page-level header bar; the Facts/Rules nav lives instead in the editor
// pane's heading (see PaneHeadingWithNav). Fragment responses (the SSE
// action/subscription endpoints) never construct a Page; they emit
// pane-scoped html.Content directly.
//
// The single four-pane page was split into two three-pane views (Facts,
// Rules) because four disjoint panes on one screen was unworkable — each
// view now pairs the editor under test with the panes that show its input
// and its effect:
//
//   - Facts view (/facts): Data Browser | jsonfacts Editor | Fact Browser
//     (base) — authoring how base facts are extracted from JSONL.
//   - Rules view (/rules): Fact Browser (base) | Datalog Editor | Fact
//     Browser (derived) — authoring how rules derive facts from base facts.
//
// Active names the current view ("facts" or "rules") for nav highlighting.
type Page struct {
	Title  string
	Active string

	// Columns holds exactly the view's three panes, left to right. A slice
	// rather than named fields since the two views' pane compositions
	// differ (view/facts.go and view/rules.go build them), and Page itself
	// only needs to lay three columns out, not know what's in them.
	Columns []html.Content

	// Console is the drawer beneath the columns (view/console.go), shared
	// by both views. Optional so fragment-only tests can render a bare
	// Page; nil renders nothing (view.When's never-nil rule applies at the
	// render site, not here).
	Console html.Content
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
		// The Fact Browser subscription (doc/notes/datastar.md §8) is
		// page-scoped, not pane-scoped: whichever of #predicates-base /
		// #predicates-derived exist on the current view, this one
		// connection keeps both in sync (publishSessionChanged always
		// fans out both fragments; Datastar morphs whichever id is
		// present and no-ops on the other).
		tag.New("div").Set("data-init", "@get('/events', {openWhenHidden: true, requestCancellation: 'disabled'})"),
		tag.New("main#workbench", p.Columns...),
		When(p.Console != nil, p.Console),
	)
}

// PaneHeading renders an editor pane's h2 title alongside the Facts/Rules
// view switcher, since the page's old header#chrome bar (title, nav,
// Cancel) was removed in favor of putting the nav next to whichever
// editor's heading is currently on screen. active names the current view
// ("facts" or "rules") so navLinks can style the matching link distinctly.
func PaneHeadingWithNav(title, active string) html.Content {
	return tag.New("div.pane-heading",
		PaneHeading.Add(html.Text(title)),
		navLinks(active),
	)
}

// navLinks renders the Facts/Rules view switcher. active names the current
// view ("facts" or "rules") so its link can be styled distinctly; full
// page navigation (doc/notes/datastar.md §7), not a Datastar action, since
// switching views is a real URL change a user should be able to bookmark
// or reload.
func navLinks(active string) html.Content {
	return tag.New("nav#views",
		navLink("/facts", "Facts", active == "facts"),
		navLink("/rules", "Rules", active == "rules"),
	)
}

func navLink(href, label string, active bool) html.Content {
	a := tag.New("a").Set("href", href).Add(html.Text(label))
	if active {
		a = a.Class("active")
	}
	return a
}

// Toast renders the #toast fragment: a system-level surface per
// doc/notes/datastar.md §4 (toast vs in-form errors) — used for conditions
// that aren't the user's fault to fix in a form (Save's write/git outcome,
// currently the only Toast caller). isError adds an "error" class so the
// two states can be styled differently; rendered with no whitespace between
// the outer div's tags so page.go's `#toast:empty` CSS rule still applies
// once a subsequent empty Toast (if ever needed) clears it.
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
// run/apply/agent mutex ($busy, see view/console.go): while $busy holds this
// button's own key it morphs into a spinner-ringed Stop posting /cancel
// (there is no standalone Stop button anymore — Global Cancel lives on
// whichever button started the work), and while $busy holds a DIFFERENT key
// it greys out, making the mutex visible at every action row.
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
