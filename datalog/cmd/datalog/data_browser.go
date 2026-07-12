package main

import (
	"net/http"

	"github.com/swdunlop/html-go/datastar"
)

// handleDataList is the Data Browser's file-list stub (GET /data). Wave 5
// fills this in with the file list from sources[].file.
func (wb *workbench) handleDataList(w http.ResponseWriter, r *http.Request) {
	stream, err := datastar.RequestStream(w, r)
	if err != nil {
		return
	}
	_ = stream.Emit(datastar.Elements(stubFragment("data-files")))
}

// handleDataFile is the Data Browser's paginated-row stub
// (GET /data/{file}?offset=N). Wave 5 fills this in: files re-read per
// request, zip members decompressed to a temp file on first access,
// MergeFragments-appended with Mode("append") rather than replacing the
// table body.
func (wb *workbench) handleDataFile(w http.ResponseWriter, r *http.Request) {
	stream, err := datastar.RequestStream(w, r)
	if err != nil {
		return
	}
	_ = stream.Emit(datastar.Elements(stubFragment("data-table-body")))
}

// handleJSONFactsTest is the Data Browser's "Test" button stub
// (GET /jsonfacts/test/{file}/{row}). Wave 5/6 fills this in: selects the
// row as the jsonfacts editor's evaluation target and patches the editor's
// row pane and live-output pane in the same response.
func (wb *workbench) handleJSONFactsTest(w http.ResponseWriter, r *http.Request) {
	stream, err := datastar.RequestStream(w, r)
	if err != nil {
		return
	}
	_ = stream.Emit(datastar.Elements(stubFragment("jsonfacts-row")))
}
