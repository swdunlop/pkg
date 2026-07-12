package main

import (
	"encoding/json"
	"net/http"
	"sort"
	"strconv"

	html "github.com/swdunlop/html-go"
	"github.com/swdunlop/html-go/datastar"
	"swdunlop.dev/pkg/datalog"
	"swdunlop.dev/pkg/datalog/cmd/datalog/view"
)

// factsPageSize is the number of facts paged per request, per
// doc/features/web-ui.md's Fact Browser spec (50 at a time, mirroring the
// Data Browser's row pagination).
const factsPageSize = 50

// compositeSummaryLen is how many characters of a composite's canonical
// JSON show in its collapsed <details> summary before truncating, per the
// design doc ("~80 chars of canonical JSON").
const compositeSummaryLen = 80

// handleFacts pages a predicate's facts 50 at a time (GET
// /facts/{predicate}/{arity}?offset=N). offset=0 replaces the predicate's
// whole facts container (including its Load More control); offset>0 appends
// the new rows to the existing tbody via Mode("append") and replaces the
// Load More control with a fresh one (or an empty placeholder once
// exhausted).
func (wb *workbench) handleFacts(w http.ResponseWriter, r *http.Request) {
	stream, err := datastar.RequestStream(w, r)
	if err != nil {
		return
	}

	name := r.PathValue("predicate")
	arity, err := strconv.Atoi(r.PathValue("arity"))
	if err != nil {
		_ = stream.Emit(datastar.Elements(view.FactsTable(name, 0, nil, nil, 0, 0, false)))
		return
	}

	offset := 0
	if v := r.URL.Query().Get("offset"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			offset = n
		}
	}

	wb.h.mu.Lock()
	db, dbErr := wb.h.sess.evaluatedDB()
	wb.h.mu.Unlock()
	if dbErr != nil {
		_ = stream.Emit(datastar.Elements(view.FactsTable(name, arity, nil, nil, offset, 0, false)))
		return
	}

	total := 0
	var page [][]html.Content
	i := 0
	for row := range db.Facts(name, arity) {
		if i >= offset && len(page) < factsPageSize {
			page = append(page, renderFactRow(row))
		}
		i++
		total = i
	}
	hasMore := offset+len(page) < total

	header := make([]string, arity)
	for i := range header {
		header[i] = "col" + strconv.Itoa(i)
	}

	if offset == 0 {
		_ = stream.Emit(datastar.Elements(view.FactsTable(name, arity, header, page, offset, total, hasMore)))
		return
	}

	_ = stream.Emit(
		datastar.Elements(view.FactRows(page), datastar.Selector("#"+view.TbodyID(name, arity)), datastar.Mode("append")),
		datastar.Elements(view.LoadMoreControl(name, arity, offset, len(page), total, hasMore)),
	)
}

// renderFactRow renders one fact's terms: scalars via constantToJSON's
// display conventions, composites as a one-level <details> (design doc:
// summary ~80 chars of canonical JSON, expansion the full
// json.MarshalIndent output).
func renderFactRow(row []datalog.Constant) []html.Content {
	cells := make([]html.Content, len(row))
	for i, c := range row {
		if comp, ok := c.(*datalog.Composite); ok {
			cells[i] = compositeDetail(comp)
			continue
		}
		cells[i] = html.Text(jsonScalarText(constantToJSON(c)))
	}
	return cells
}

// jsonScalarText renders a scalar constantToJSON value as display text: nil
// shows as "null", everything else via its natural string form. Using
// json.Marshal keeps strings quoted the same way the MCP tool surface's
// JSON output would, so the fact browser and an agent's query results agree
// visually.
func jsonScalarText(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	return string(b)
}

func compositeDetail(c *datalog.Composite) html.Content {
	canon := c.Canonical()
	summary := canon
	if len(summary) > compositeSummaryLen {
		summary = summary[:compositeSummaryLen] + "…"
	}
	full, err := json.MarshalIndent(c.Value(), "", "  ")
	if err != nil {
		full = []byte(canon)
	}
	return view.CompositeDetail(summary, string(full))
}

// publishSessionChanged is the one call every mutating handler (Apply, Run,
// and — once mounted — agent set_schema/set_rules over /mcp) makes after a
// Transform completes: it re-renders both Fact Browser fragments
// (#predicates-base and #predicates-derived) from current session state and
// fans them out as one Batch over the bus, so every open tab's subscription
// connection repaints whichever of the two panes it has on screen (design
// constraint 3's SSE patch-back; page.go's body-level subscription div is
// page-scoped, so a page with both panes — the Rules view — gets both
// updates over its one connection). The fragments are rendered once here,
// at publish time, and the same bytes go to every subscriber
// (doc/notes/datastar.md §8's pre-rendered fan-out). Callers must hold
// wb.h.mu, since rendering reads session state.
func (wb *workbench) publishSessionChanged() {
	base, derived := renderPredicates(wb.h.sess)
	wb.bus.Publish(datastar.Batch(datastar.Elements(base), datastar.Elements(derived)))
}

// renderPredicates builds the #predicates-base and #predicates-derived
// fragments from session state. Callers must hold wb.h.mu (or otherwise
// ensure exclusive access to sess), since it reads facts/rules/aggRules/
// dataDB directly.
//
// EDB/IDB labeling rule: a predicate/arity pair is "derived" (IDB) if any
// loaded rule or aggregate rule has it as a head; otherwise "base" (EDB). A
// predicate that is both a rule head AND has raw loaded facts (unusual, but
// possible if a data source and a rule agree on a name/arity) is still
// labeled derived — the ruleset's claim on the predicate takes precedence,
// since the point of the label is "does a rule explain this data," and if a
// rule exists for it the answer is yes regardless of what else populated
// the same predicate/arity.
func renderPredicates(sess *session) (base, derived html.Content) {
	type key struct {
		name  string
		arity int
	}
	counts := map[key]int{}

	db, err := sess.evaluatedDB()
	if err == nil {
		for name, arity := range db.Predicates() {
			k := key{name, arity}
			n := 0
			for range db.Facts(name, arity) {
				n++
			}
			counts[k] = n
		}
	}

	isDerived := map[key]bool{}
	for _, rule := range sess.rules {
		k := key{rule.Head.Pred, len(rule.Head.Terms)}
		isDerived[k] = true
		if _, ok := counts[k]; !ok {
			counts[k] = 0
		}
	}
	for _, ar := range sess.aggRules {
		k := key{ar.Head.Pred, len(ar.Head.Terms)}
		isDerived[k] = true
		if _, ok := counts[k]; !ok {
			counts[k] = 0
		}
	}

	var baseEntries, derivedEntries []view.PredicateEntry
	for k, n := range counts {
		e := view.PredicateEntry{
			Name:    k.name,
			Arity:   k.arity,
			Facts:   n,
			Derived: isDerived[k],
		}
		if e.Derived {
			derivedEntries = append(derivedEntries, e)
		} else {
			baseEntries = append(baseEntries, e)
		}
	}
	byNameArity := func(entries []view.PredicateEntry) {
		sort.Slice(entries, func(i, j int) bool {
			if entries[i].Name != entries[j].Name {
				return entries[i].Name < entries[j].Name
			}
			return entries[i].Arity < entries[j].Arity
		})
	}
	byNameArity(baseEntries)
	byNameArity(derivedEntries)

	return view.Predicates("base", baseEntries), view.Predicates("derived", derivedEntries)
}
