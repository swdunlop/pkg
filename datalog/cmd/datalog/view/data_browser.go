package view

import (
	"fmt"
	"net/url"

	html "github.com/swdunlop/html-go"
	"github.com/swdunlop/html-go/tag"
)

// DataBrowser renders the Data Browser pane shell (doc/features/web-ui.md):
// a file list and a paginated raw-record list. Records are just a single
// column of raw-line previews (plus a click target), so a plain list
// carries this better than a <table> — there's no second column to align.
//
//   - #data-files      — file list, patched by GET /data
//   - #data-table-body — paginated row list, patched (append mode) by
//     GET /data/{file}?offset=N
//   - #data-error      — in-pane error surface
func DataBrowser() html.Content {
	return PaneSection.Set("id", "pane-data-browser").Add(
		PaneHeading.Add(html.Text("Data Browser")),
		ErrorList.Set("id", "data-error"),
		// data-init fetches the configured file list as soon as the pane
		// mounts — without it the pane renders empty until something else
		// happens to request /data, which nothing does.
		tag.New("div#data-files").Set("data-init", "@get('/data')"),
		tag.New("ul.unstyled#data-table-body"),
		// The Load More control is a SIBLING of the row list, never a child:
		// the list is an append target, so anything rendered inside it gets
		// buried under appended pages (the stranded-mid-list bug). Mirrors
		// fact_browser.go's tbody/LoadMoreControl split.
		tag.New("div").Set("id", dataLoadMoreID),
	)
}

// DataFileList renders the #data-files fragment: a <select> with one
// <option> per configured source file. Changing the selection fetches that
// file's first page of rows into #data-table-body; data-bind:data-file
// gives the change handler's action a $dataFile signal to read (see
// doc/notes/datastar.md §"data-bind" — the select's own selected option is
// canonical, so binding it is fine even though the value is server-owned).
// The browser defaults to the first <option> (files[0]), which the caller
// (handleDataList) also auto-loads so the pane isn't left empty.
//
// encodeURIComponent($dataFile) is the JS-side counterpart of pathEscape
// (used below for the row-level /data/{file}/... links, and by
// handleJSONFactsTest's confine call): GET /data/{file} is a single-segment
// route (serve.go), and a source file naming a subdirectory (e.g.
// "logs/events.jsonl") would otherwise widen the request into an extra path
// segment the route never matches, 404ing silently. Escaping keeps it one
// segment; ServeMux matches against the still-escaped path (each segment
// unescaped only for the comparison, per net/http's pattern matching) and
// r.PathValue("file") returns the decoded "logs/events.jsonl" — a "/"
// escapes to "%2F" the same way url.PathEscape and encodeURIComponent both
// treat it, so this reaches the handler intact.
func DataFileList(files []string) html.Content {
	return tag.New("div#data-files").Add(
		tag.New("select").
			Set("data-bind:data-file").
			Set("data-on:change", "@get('/data/' + encodeURIComponent($dataFile))").
			Add(
				html.Map(files, func(f string) html.Content {
					return tag.New("option").Set("value", f).Add(html.Text(f))
				}),
			),
	)
}

// DataErrors renders the #data-error fragment: an in-form error list,
// verbatim, per doc/notes/datastar.md §4.
func DataErrors(errs []string) html.Content {
	return ErrorList.Set("id", "data-error").Add(
		html.Map(errs, func(e string) html.Content {
			return tag.New("li", html.Text(e))
		})...,
	)
}

// DataRowInfo is one raw record's index and source line, passed to
// DataTableBody.
type DataRowInfo struct {
	Index int
	Raw   string
}

// DataRow renders one <li> for a raw JSONL record: a preview of the raw
// line. Clicking the row selects it as the jsonfacts editor's evaluation
// target — the row index is only a lookup key (into the source file, and
// into wb.selRow), not something worth displaying. The row carries a
// stable #data-row-N id so handleJSONFactsTest can patch it (and any
// previously-selected row) in place; selected marks it as the jsonfacts
// editor's current evaluation target (doc/features/web-ui.md: the Data
// Browser highlights the tested row instead of the jsonfacts Editor
// duplicating it).
func DataRow(file string, row int, raw string, selected bool) html.Content {
	li := tag.New("li").
		Set("id", fmt.Sprintf("data-row-%d", row)).
		Set("data-on:click", fmt.Sprintf("@get('/data/select/%s/%d')", pathEscape(file), row)).
		Add(html.Text(raw))
	if selected {
		li = li.Class("selected")
	}
	return li
}

// dataLoadMoreID is the Load More control's fixed element id. Unlike the
// Fact Browser's per predicate/arity LoadMoreID (fact_browser.go), the Data
// Browser shows exactly one file's rows at a time, so a single static id is
// enough.
const dataLoadMoreID = "data-load-more"

// DataLoadMore renders the Load More control for file's next chunk, or an
// empty (id-carrying) placeholder once hasMore is false — kept non-nil and
// id-bearing so a later patch targeting dataLoadMoreID always has an element
// to morph, even after exhaustion (mirrors fact_browser.go's
// loadMoreControl).
func DataLoadMore(file string, nextOffset int, hasMore bool) html.Content {
	div := tag.New("div").Set("id", dataLoadMoreID)
	if !hasMore {
		return div
	}
	return div.Add(
		ActionButton.
			Set("data-on:click", fmt.Sprintf("@get('/data/%s?offset=%d')", pathEscape(file), nextOffset)).
			Add(html.Text("Load More")),
	)
}

// DataRows renders just the <li> row elements for an appended page
// (offset > 0). The handler patches these into the existing #data-table-body
// list via datastar.Mode("append") rather than replacing it, and separately
// patches dataLoadMoreID's control with a fresh one via DataLoadMore
// (mirrors fact_browser.go's FactRows/TbodyID/LoadMoreControl split).
// Appending the whole DataTableBody fragment, as this used to do, nested a
// duplicate #data-table-body <ul> inside itself (its own root element
// carries the same id) and left the previous, now stale-offset, Load More
// button live alongside the new one.
func DataRows(file string, rows []DataRowInfo, selFile string, selRow int, selValid bool) html.Content {
	children := make([]html.Content, len(rows))
	for i, r := range rows {
		selected := selValid && selFile == file && selRow == r.Index
		children[i] = DataRow(file, r.Index, r.Raw, selected)
	}
	return html.Group(children)
}

// DataTableBody renders the #data-table-body fragment for a chunk of rows —
// rows ONLY; the Load More control is a sibling element the handler patches
// separately via DataLoadMore (rendering it inside this ul would bury it
// under appended pages, since the ul is the append target). The handler
// chooses the Mode (replace for offset==0, append for offset>0) at the
// SSE-event level; this function only builds the HTML. selFile/selRow/
// selValid are the jsonfacts Editor's current evaluation target
// (wb.selFile/selRow/selValid) so the tested row renders highlighted even
// across a file reload or Load More.
func DataTableBody(file string, rows []DataRowInfo, selFile string, selRow int, selValid bool) html.Content {
	children := make([]html.Content, 0, len(rows))
	for _, r := range rows {
		selected := selValid && selFile == file && selRow == r.Index
		children = append(children, DataRow(file, r.Index, r.Raw, selected))
	}
	return tag.New("ul.unstyled#data-table-body").Add(children...)
}

// pathEscape escapes a file reference for embedding in a URL path segment
// within a Datastar action string. Source file names are simple relative
// paths (no query/fragment chars expected), but escaping is cheap insurance
// against any file name containing characters that would otherwise break
// the action string or the resulting request path.
func pathEscape(s string) string {
	return url.PathEscape(s)
}
