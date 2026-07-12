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
func DataFileList(files []string) html.Content {
	return tag.New("div#data-files").Add(
		tag.New("select").
			Set("data-bind:data-file").
			Set("data-on:change", "@get('/data/' + $dataFile)").
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
		Set("data-on:click", fmt.Sprintf("@get('/jsonfacts/test/%s/%d')", pathEscape(file), row)).
		Add(html.Text(raw))
	if selected {
		li = li.Class("selected")
	}
	return li
}

// LoadMoreRow renders a list item carrying a "Load More" button for the
// next offset, appended after the last row of a chunk when more rows
// remain in the file.
func LoadMoreRow(file string, nextOffset int) html.Content {
	return tag.New("li#data-load-more").Add(
		ActionButton.
			Set("data-on:click", fmt.Sprintf("@get('/data/%s?offset=%d')", pathEscape(file), nextOffset)).
			Add(html.Text("Load More")),
	)
}

// DataTableBody renders the #data-table-body fragment for a chunk of rows.
// The handler chooses the Mode (replace for offset==0, append for
// offset>0) at the SSE-event level; this function only builds the HTML.
// selFile/selRow/selValid are the jsonfacts Editor's current evaluation
// target (wb.selFile/selRow/selValid) so the tested row renders highlighted
// even across a file reload or Load More.
func DataTableBody(file string, rows []DataRowInfo, nextOffset int, hasMore bool, selFile string, selRow int, selValid bool) html.Content {
	children := make([]html.Content, 0, len(rows)+1)
	for _, r := range rows {
		selected := selValid && selFile == file && selRow == r.Index
		children = append(children, DataRow(file, r.Index, r.Raw, selected))
	}
	if hasMore {
		children = append(children, LoadMoreRow(file, nextOffset))
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
