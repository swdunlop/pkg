package view

import (
	"fmt"

	html "github.com/swdunlop/html-go"
	"github.com/swdunlop/html-go/tag"
)

// FactBrowser renders a Fact Browser pane shell: a predicate list (EDB or
// IDB, per kind) that expands into paged facts. kind is "base" or
// "derived", selecting which half of publishSessionChanged's fan-out this
// pane's #predicates-{kind} div responds to (page.go's body-level div owns
// the actual /events subscription connection now — a page can hold two
// Fact Browser panes, e.g. the Rules view's base+derived pair, and they
// share one connection rather than opening two).
//
//   - #predicates-{kind}    — predicate list, patched by the /events
//     subscription and by GET /facts/{predicate}/{arity} row expansion
func FactBrowser(kind, heading string) html.Content {
	return PaneSection.Set("id", "pane-fact-browser-"+kind).Add(
		PaneHeading.Add(html.Text(heading)),
		tag.New("div#"+predicatesID(kind)),
	)
}

// predicatesID names the predicate-list div for a given kind ("base" or
// "derived"), shared between FactBrowser and the handler's Predicates
// fragment so both agree on the id without duplicating the naming scheme.
func predicatesID(kind string) string { return "predicates-" + kind }

// PredicateEntry is one row of the Fact Browser's predicate listing: a
// predicate/arity pair, its fact count, and whether it is base (EDB, present
// in the loaded data source) or derived (IDB, a rule/aggregate-rule head).
type PredicateEntry struct {
	Name    string
	Arity   int
	Facts   int
	Derived bool // true = IDB (has rules deriving it); false = base (EDB)
}

// Predicates renders the #predicates-{kind} fragment: one expandable entry
// per predicate/arity pair, sorted by the caller. Clicking an entry loads
// its facts via GET /facts/{name}/{arity} (design doc's literal wiring:
// data-on:click="@get('/facts/<name>/<arity>')"), which replaces the
// entry's (initially empty) facts container with the first page.
func Predicates(kind string, entries []PredicateEntry) html.Content {
	return tag.New("div#"+predicatesID(kind),
		html.Map(entries, predicateEntry),
	)
}

func predicateEntry(e PredicateEntry) html.Content {
	label := "base"
	if e.Derived {
		label = "derived"
	}
	id := factsContainerID(e.Name, e.Arity)
	header := tag.New("div.predicate-row").
		Set("data-on:click", fmt.Sprintf("@get('/facts/%s/%d')", e.Name, e.Arity)).
		Add(
			html.Text(fmt.Sprintf("%s/%d", e.Name, e.Arity)),
			tag.New("span.badge", html.Text(" "+label)),
			tag.New("span.count", html.Text(fmt.Sprintf(" (%d facts)", e.Facts))),
		)
	return tag.New("div.predicate-entry",
		header,
		tag.New("div").Set("id", id),
	)
}

// factsContainerID names the div a predicate's paged fact table renders
// into, shared between the predicate listing and the facts handler so
// offset=0 responses can target it by id.
func factsContainerID(name string, arity int) string {
	return fmt.Sprintf("facts-%s-%d", name, arity)
}

// FactsContainerID exposes the facts container's element id so the handler
// can target it without duplicating the naming scheme.
func FactsContainerID(name string, arity int) string { return factsContainerID(name, arity) }

// FactsTable renders a predicate's initial (offset=0) facts page: rows is
// the page of term slices already rendered to html.Content (composites as
// <details> via CompositeDetail, scalars via constantToJSON — package
// main's job, since view must stay engine-type-agnostic). This replaces the
// predicate's whole facts container — including the "Load more" control,
// keyed by loadMoreID so a later append page can replace just that control
// with a fresh one carrying the next offset.
func FactsTable(name string, arity int, header []string, rows [][]html.Content, offset, total int, hasMore bool) html.Content {
	id := factsContainerID(name, arity)
	tbody := TableBody.Set("id", tbodyID(name, arity)).Add(factRowContents(rows)...)

	table := Table.Add(
		TableHead.Add(TableRow.Add(headerCells(header)...)),
		tbody,
	)

	return tag.New("div").Set("id", id).Add(
		table,
		loadMoreControl(name, arity, offset, len(rows), total, hasMore),
	)
}

// FactRows renders just the <tr> rows for an appended page (offset > 0).
// The handler patches these into the existing tbody (identified by
// tbodyID) using datastar.Mode("append") rather than replacing it, and
// separately patches loadMoreID's control with a fresh one (or an empty
// Group once exhausted) via loadMoreControl.
func FactRows(rows [][]html.Content) html.Content {
	return html.Group(factRowContents(rows))
}

func factRowContents(rows [][]html.Content) []html.Content {
	trs := make([]html.Content, len(rows))
	for i, row := range rows {
		cells := make([]html.Content, len(row))
		for j, c := range row {
			cells[j] = TableCell.Add(c)
		}
		trs[i] = TableRow.Add(cells...)
	}
	return trs
}

func tbodyID(name string, arity int) string {
	return factsContainerID(name, arity) + "-body"
}

// TbodyID exposes the fact table body's element id so the handler can
// target it with Mode("append") without duplicating the naming scheme.
func TbodyID(name string, arity int) string { return tbodyID(name, arity) }

// LoadMoreID exposes the Load More control's element id so the handler can
// replace it after appending a page.
func LoadMoreID(name string, arity int) string { return factsContainerID(name, arity) + "-more" }

func headerCells(names []string) []html.Content {
	out := make([]html.Content, len(names))
	for i, n := range names {
		out[i] = TableHCell.Add(html.Text(n))
	}
	return out
}

// LoadMoreControl renders the "Load more" control for a predicate's facts
// page, or an empty (id-carrying) placeholder once hasMore is false — kept
// non-nil and id-bearing so a subsequent Elements patch targeting
// LoadMoreID always has an element to morph, even after exhaustion.
func LoadMoreControl(name string, arity, offset, pageLen, total int, hasMore bool) html.Content {
	return loadMoreControl(name, arity, offset, pageLen, total, hasMore)
}

func loadMoreControl(name string, arity, offset, pageLen, total int, hasMore bool) html.Content {
	id := LoadMoreID(name, arity)
	if !hasMore {
		return tag.New("div").Set("id", id)
	}
	next := offset + pageLen
	return tag.New("div").Set("id", id).Add(
		tag.New("button.action").
			Set("data-on:click", fmt.Sprintf("@get('/facts/%s/%d?offset=%d')", name, arity, next)).
			Add(html.Text(fmt.Sprintf("Load more (%d of %d)", next, total))),
	)
}

// CompositeDetail renders a composite constant as a one-level <details>:
// the summary shows a truncated canonical JSON preview, the body a full
// pretty-printed <pre> block.
func CompositeDetail(summary, full string) html.Content {
	return tag.New("details",
		tag.New("summary", html.Text(summary)),
		tag.New("pre", html.Text(full)),
	)
}
