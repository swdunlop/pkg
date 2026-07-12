package main

import (
	"net/http"

	"github.com/swdunlop/html-go/datastar"
)

// handleRulesCheck is the debounced parse/compile-only stub
// (POST /rules/check). Wave 7 fills this in: refreshes the error list with
// line:col-prefixed parser/compiler errors and a cursor-position indicator,
// keeping keystroke Transforms off the serialized mutation pipeline
// (observation 5).
func (wb *workbench) handleRulesCheck(w http.ResponseWriter, r *http.Request) {
	stream, err := datastar.RequestStream(w, r)
	if err != nil {
		return
	}
	_ = stream.Emit(datastar.Elements(stubFragment("rules-error")))
}

// handleRulesRun is the Run stub (POST /rules/run). Wave 7 fills this in:
// applies the document via set_rules and executes its queries through the
// query handler under evalTimeout, streaming a #status fragment per
// doc/notes/datastar.md §9 and gated through wb.jobs.Begin so the Global
// Cancel button has a job to cancel.
func (wb *workbench) handleRulesRun(w http.ResponseWriter, r *http.Request) {
	stream, err := datastar.RequestStream(w, r)
	if err != nil {
		return
	}
	_ = stream.Emit(datastar.Elements(stubFragment("status")))
}
