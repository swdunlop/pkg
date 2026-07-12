package view

import (
	html "github.com/swdunlop/html-go"
	"github.com/swdunlop/html-go/tag"
)

// DataBrowser renders the Data Browser pane shell (doc/features/web-ui.md):
// a file list and a paginated raw-record table. Wave 5 fills in the table
// body and Test-button wiring; this placeholder carries the stable ids
// serve.go's routes patch against.
//
//   - #data-files    — file list, patched by GET /data
//   - #data-table    — paginated row table, patched (append mode) by
//     GET /data/{file}?offset=N
//   - #data-error    — in-pane error surface
func DataBrowser() html.Content {
	return PaneSection.Set("id", "pane-data-browser").Add(
		PaneHeading.Add(html.Text("Data Browser")),
		ErrorList.Set("id", "data-error"),
		tag.New("div#data-files"),
		Table.Set("id", "data-table").Add(
			TableBody.Set("id", "data-table-body"),
		),
	)
}
