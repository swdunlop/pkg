package main

import (
	"net/http"

	"github.com/swdunlop/html-go/datastar"
)

// handleFacts is the paged-facts stub (GET /facts/{predicate}/{arity}).
// Wave 8 fills this in: pages the predicate's facts 50 at a time via
// Database.Facts, rendering composite terms as a one-level <details>.
func (wb *workbench) handleFacts(w http.ResponseWriter, r *http.Request) {
	stream, err := datastar.RequestStream(w, r)
	if err != nil {
		return
	}
	_ = stream.Emit(datastar.Elements(stubFragment("predicates")))
}

// publishSessionChanged is the one call every mutating handler (Apply, Run,
// and — once mounted — agent set_schema/set_rules over /mcp) makes after a
// Transform completes: it re-renders the Fact Browser's #predicates
// fragment from current session state and fans it out over the bus, so
// every open tab's subscription connection repaints (design constraint 3's
// SSE patch-back). The fragment is rendered once here, at publish time, and
// the same bytes go to every subscriber (doc/notes/datastar.md §8's
// pre-rendered fan-out). Callers must hold wb.h.mu, since rendering reads
// session state. The Fact Browser wave replaces stubFragment with the real
// predicate listing; callers do not change.
func (wb *workbench) publishSessionChanged() {
	wb.bus.Publish(datastar.Elements(stubFragment("predicates")))
}
