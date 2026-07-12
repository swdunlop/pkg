package main

import (
	"net/http"

	"github.com/swdunlop/html-go/datastar"
)

// handleJSONFactsPreview is the debounced single-row-extraction stub
// (POST /jsonfacts/preview). Wave 6 fills this in: parses the YAML,
// compiles the expr mappings, and extracts from the single selected row
// only — cheap enough to skip the sandbox's job-gating machinery.
func (wb *workbench) handleJSONFactsPreview(w http.ResponseWriter, r *http.Request) {
	stream, err := datastar.RequestStream(w, r)
	if err != nil {
		return
	}
	_ = stream.Emit(datastar.Elements(stubFragment("jsonfacts-error")))
}

// handleJSONFactsApply is the Apply stub (POST /jsonfacts/apply). Wave 6
// fills this in: gated through wb.jobs.Begin so a second Apply click while
// one is in flight is a no-op, re-extracts everything, and runs a full
// Transform via the set_schema handler (in-memory only).
func (wb *workbench) handleJSONFactsApply(w http.ResponseWriter, r *http.Request) {
	stream, err := datastar.RequestStream(w, r)
	if err != nil {
		return
	}
	_ = stream.Emit(datastar.Elements(stubFragment("jsonfacts-error")))
}
