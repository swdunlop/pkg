package view

import (
	"fmt"
	"net/url"
	"strconv"

	html "github.com/swdunlop/html-go"
	"github.com/swdunlop/html-go/tag"
)

// DataBrowser renders the Data Browser pane shell (doc/features/web-ui.md):
// a file list and a paginated raw-record table.
//
//   - #data-files    — file list, patched by GET /data
//   - #data-table    — paginated row table, patched (append mode) by
//     GET /data/{file}?offset=N
//   - #data-error    — in-pane error surface
func DataBrowser() html.Content {
	return PaneSection.Set("id", "pane-data-browser").Add(
		PaneHeading.Add(html.Text("Data Browser")),
		ErrorList.Set("id", "data-error"),
		// data-init fetches the configured file list as soon as the pane
		// mounts — without it the pane renders empty until something else
		// happens to request /data, which nothing does.
		tag.New("div#data-files").Set("data-init", "@get('/data')"),
		Table.Set("id", "data-table").Add(
			TableBody.Set("id", "data-table-body"),
		),
	)
}

// DataFileList renders the #data-files fragment: one clickable entry per
// configured source file. Each entry fetches the first page of that file's
// rows into #data-table-body.
func DataFileList(files []string) html.Content {
	return tag.New("div#data-files").Add(
		tag.New("ul.unstyled",
			html.Map(files, func(f string) html.Content {
				return tag.New("li",
					tag.New("a[href=#]").
						Set("data-on:click", fmt.Sprintf("@get('/data/%s')", pathEscape(f))).
						Add(html.Text(f)),
				)
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

// DataRow renders one <tr> for a raw JSONL record: the row index, a preview
// of the raw line, and a "Test" button that selects it as the jsonfacts
// editor's evaluation target.
func DataRow(file string, row int, raw string) html.Content {
	return TableRow.Add(
		TableCell.Add(html.Text(strconv.Itoa(row))),
		TableCell.Add(html.Text(raw)),
		TableCell.Add(
			ActionButton.
				Set("data-on:click", fmt.Sprintf("@get('/jsonfacts/test/%s/%d')", pathEscape(file), row)).
				Add(html.Text("Test")),
		),
	)
}

// LoadMoreRow renders a table row carrying a "Load More" button for the
// next offset, appended after the last row of a chunk when more rows
// remain in the file.
func LoadMoreRow(file string, nextOffset int) html.Content {
	return TableRow.Set("id", "data-load-more").Add(
		TableCell.Set("colspan", "3").Add(
			ActionButton.
				Set("data-on:click", fmt.Sprintf("@get('/data/%s?offset=%d')", pathEscape(file), nextOffset)).
				Add(html.Text("Load More")),
		),
	)
}

// DataTableBody renders the #data-table-body fragment for a chunk of rows.
// The handler chooses the Mode (replace for offset==0, append for
// offset>0) at the SSE-event level; this function only builds the HTML.
func DataTableBody(file string, rows []DataRowInfo, nextOffset int, hasMore bool) html.Content {
	children := make([]html.Content, 0, len(rows)+1)
	for _, r := range rows {
		children = append(children, DataRow(file, r.Index, r.Raw))
	}
	if hasMore {
		children = append(children, LoadMoreRow(file, nextOffset))
	}
	return TableBody.Set("id", "data-table-body").Add(children...)
}

// pathEscape escapes a file reference for embedding in a URL path segment
// within a Datastar action string. Source file names are simple relative
// paths (no query/fragment chars expected), but escaping is cheap insurance
// against any file name containing characters that would otherwise break
// the action string or the resulting request path.
func pathEscape(s string) string {
	return url.PathEscape(s)
}
