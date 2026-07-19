package main

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"strconv"
	"strings"

	html "github.com/swdunlop/html-go"
	"github.com/swdunlop/html-go/datastar"
	"swdunlop.dev/pkg/datalog"
	"swdunlop.dev/pkg/datalog/cmd/datalog/view"
	"swdunlop.dev/pkg/datalog/memory"
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
		_ = stream.Emit(datastar.Elements(view.FactsTable(name, 0, nil, nil, 0, 0, false, "")))
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
	derived := isDerivedPredicate(wb.h.sess, name, arity)
	provOK := wb.h.sess.provenanceEnabled
	decl, use := declarationForHeader(wb.h.sess, name, arity)
	wb.h.mu.Unlock()
	if dbErr != nil {
		_ = stream.Emit(datastar.Elements(view.FactsTable(name, arity, nil, nil, offset, 0, false, "")))
		return
	}
	// The "why?" affordance (doc/features/provenance.md's Fact Browser
	// surface) only makes sense for a derived (IDB) predicate with
	// provenance actually enabled — a base-fact row has no witness to
	// explain by construction (see seminaive.Provenance's Base marker), and
	// showing the button when provenance is off would just produce an error
	// on every click.
	showWhy := derived && provOK

	total := 0
	var page [][]html.Content
	i := 0
	for row := range db.Facts(name, arity) {
		if i >= offset && len(page) < factsPageSize {
			page = append(page, renderFactRow(name, arity, row, showWhy))
		}
		i++
		total = i
	}
	hasMore := offset+len(page) < total

	header := factHeaderNames(decl, arity)
	if showWhy {
		// One blank trailing header cell for the "why?" button column
		// renderFactRow appends to every row when showWhy is true, so the
		// header row's cell count matches the body rows'.
		header = append(header, "")
	}

	if offset == 0 {
		_ = stream.Emit(datastar.Elements(view.FactsTable(name, arity, header, page, offset, total, hasMore, use)))
		return
	}

	_ = stream.Emit(
		datastar.Elements(view.FactRows(page), datastar.Selector("#"+view.TbodyID(name, arity)), datastar.Mode("append")),
		datastar.Elements(view.LoadMoreControl(name, arity, offset, len(page), total, hasMore)),
	)
}

// handleWhy is the Fact Browser's "why?" affordance (POST
// /why/{predicate}/{arity}?fact=<literal>, view.WhyButton's click target):
// parse the literal fact text (query string, view.WhyButton's doc comment)
// with parseFactStatement — the SAME parser the MCP explain tool and the
// REPL's .why use, so a browser click, an agent's explain call, and a human
// typing .why all resolve one fact through one code path — explain it
// against the session, and append the rendered tree to the console
// drawer's Query tab (kind "explain") so it repaints/persists exactly like
// every other console entry: the drawer's own SSE fan-out (wb.consoleAppend
// -> wb.bus.Publish) is what satisfies "the drawer must repaint or close on
// generation change like every other pane" (doc/features/provenance.md's
// Risks section) — a rule-group edit landing after this entry renders does
// not retroactively invalidate it (same as a completed query result block), but
// a NEW query or Run after that point resolves against the new generation,
// and old entries read as historical scrollback like any other console
// entry, not as a live view.
//
// No $busy/job gating: explaining one already-produced fact is a map lookup
// plus a bounded tree walk (see seminaive.Provenance's doc comment), not a
// long-running action — matching WhyButton's own doc comment and every
// other plain-@post row action in this pane (predicateEntry, loadMoreControl).
func (wb *workbench) handleWhy(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("predicate")
	arity, arityErr := strconv.Atoi(r.PathValue("arity"))
	factText := r.URL.Query().Get("fact")

	if _, err := datastar.RequestStream(w, r); err != nil {
		return
	}

	if arityErr != nil {
		wb.publishWhyError("why: invalid arity in request path")
		return
	}
	fact, err := parseFactStatement(factText)
	if err != nil {
		wb.publishWhyError("why: " + err.Error())
		return
	}
	if fact.Name != name || len(fact.Terms) != arity {
		wb.publishWhyError("why: fact does not match the row's predicate/arity")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), evalTimeout)
	defer cancel()

	// Reuse mcpHandlers.explain wholesale — same lock-free-Transform/
	// writeback split as query (h.lockedSnapshot/h.cacheDerivedQuery), same
	// cache-beside-generation discipline, same rendering. A browser "why?"
	// click and an agent's explain tool call are the same operation on the
	// same session, matching every other workbench action's "one pipeline,
	// N frontends" rule (serve.go).
	out, err := wb.h.explain(ctx, explainInput{Fact: factText})
	if err != nil {
		wb.publishWhyError("why: " + err.Error())
		return
	}
	wb.bus.Publish(datastar.Elements(view.WhyOutput(view.WhyResult(factText, out.Tree))))
}

// publishWhyError renders a why? failure into the Facts tab's #why-output
// surface on every open page — the same div a success replaces, so only
// one why? outcome shows at a time.
func (wb *workbench) publishWhyError(msg string) {
	wb.bus.Publish(datastar.Elements(view.WhyOutput(view.WhyError(msg))))
}

// renderFactRow renders one fact's terms: scalars via constantToJSON's
// display conventions, composites as a one-level <details> (design doc:
// summary ~80 chars of canonical JSON, expansion the full
// json.MarshalIndent output). When showWhy is true (a derived predicate,
// provenance enabled — see handleFacts) AND every term round-trips through
// Datalog source syntax (see factLiteral), an extra trailing cell carries
// the row's "why?" button (view.WhyButton), which posts the fact's own
// literal text to /why/{name}/{arity} and appends the rendered derivation
// tree to the console drawer's Query tab; a base-fact row, or a derived row
// whose terms can't round-trip, gets no such cell at all
// (doc/features/provenance.md's Fact Browser surface: "base-fact rows get
// none").
func renderFactRow(name string, arity int, row []datalog.Constant, showWhy bool) []html.Content {
	cells := make([]html.Content, len(row), len(row)+1)
	for i, c := range row {
		if comp, ok := c.(*datalog.Composite); ok {
			cells[i] = compositeDetail(comp)
			continue
		}
		cells[i] = html.Text(jsonScalarText(constantToJSON(c)))
	}
	if showWhy {
		if lit, ok := factLiteral(name, row); ok {
			cells = append(cells, view.WhyButton(name, arity, lit))
		}
	}
	return cells
}

// factLiteral renders name(row...) as Datalog source text — reusing each
// term's own Constant.String() (the exact rendering syntax.Rule.String()
// itself uses for a fact) — for /why/{name}/{arity}'s "terms" query
// parameter, which handleWhy parses back with parseFactStatement (the same
// parser the REPL's .why and the MCP explain tool use, so a "why?" click
// resolves through the identical path a human typing the fact would). ok is
// false when row holds a term type the parser's grammar cannot read back as
// a literal (datalog.ID, *datalog.Composite — see syntax/parse.go's
// parseTerm, which has no syntax for a synthetic loader ID and treats a
// composite's own JSON literal syntax as a destructuring pattern rather
// than a value): in that case there is nothing valid to post, so the caller
// omits the button rather than emitting one that would always error.
func factLiteral(name string, row []datalog.Constant) (string, bool) {
	var buf strings.Builder
	buf.WriteString(name)
	buf.WriteByte('(')
	for i, c := range row {
		switch c.(type) {
		case datalog.String, datalog.Integer, datalog.Float, datalog.Bool, datalog.Null:
			// round-trippable
		default:
			return "", false
		}
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(c.String())
	}
	buf.WriteByte(')')
	return buf.String(), true
}

// isDerivedPredicate reports whether name/arity is an IDB predicate — the
// head of some loaded rule or aggregate rule — mirroring renderPredicates'
// EDB/IDB labeling rule (fact_browser.go) exactly, so a predicate the Fact
// Browser's own listing already labels "derived" gets the "why?" button and
// nothing else does. Callers must hold wb.h.mu.
func isDerivedPredicate(sess *session, name string, arity int) bool {
	for _, rule := range sess.rules {
		if rule.Head.Pred == name && len(rule.Head.Terms) == arity {
			return true
		}
	}
	for _, ar := range sess.aggRules {
		if ar.Head.Pred == name && len(ar.Head.Terms) == arity {
			return true
		}
	}
	return false
}

// declarationForHeader looks up name/arity's declaration (if any) from the
// session's evaluated database — the same source session.describe reads
// (describe.go) — so the Fact Browser's predicate header renders the exact
// Use text and term names an agent's describe call would see, per
// doc/features/predicate-docs.md's "describe: the mechanical access
// surface": one declaration, read by both surfaces, never two independent
// renderings that could drift. Returns a nil declaration and empty use
// string on any lookup failure (session evaluate error, unexpected
// database type, no declaration for this predicate/arity) — callers must
// treat that as "no docs known," not an error, since a predicate can be
// perfectly valid with zero declared documentation. Callers must hold
// wb.h.mu (mirrors isDerivedPredicate's contract).
func declarationForHeader(sess *session, name string, arity int) (*datalog.Declaration, string) {
	db, err := sess.evaluatedDB()
	if err != nil {
		return nil, ""
	}
	mdb, ok := db.(*memory.Database)
	if !ok {
		return nil, ""
	}
	for d := range mdb.Declarations() {
		if d.Name == name && len(d.Terms) == arity {
			d := d
			return &d, d.Use
		}
	}
	return nil, ""
}

// factHeaderNames builds the Fact Browser's header row for one predicate/
// arity: the declaration's named terms (decl.Terms[i].Name) when a
// declaration is known and names every position, falling back to
// positional "col0, col1, ..." exactly as before — per
// doc/features/predicate-docs.md work item 4, "named term columns instead
// of positional 0, 1, 2." A declaration with fewer terms than arity (should
// not happen — DocOnly declarations are assembled per-rule-head arity — but
// defensively handled) or with an unnamed term at some position falls back
// to the positional name for that column ONLY, so a partially-named
// declaration still improves readability instead of being discarded
// wholesale.
func factHeaderNames(decl *datalog.Declaration, arity int) []string {
	header := make([]string, arity)
	for i := range header {
		if decl != nil && i < len(decl.Terms) && decl.Terms[i].Name != "" {
			header[i] = decl.Terms[i].Name
			continue
		}
		header[i] = "col" + strconv.Itoa(i)
	}
	return header
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
// and — once mounted — agent set_schema/put_rule_group/delete_rule_group
// over /mcp) makes after a Transform completes: it re-renders both Fact
// Browser fragments
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
		// evaluatedDB always yields a *memory.Database under the hood (see
		// mcp.go's listPredicates); PredicateCounts gives each count in O(1)
		// instead of scanning every fact of every predicate under wb.h.mu.
		if mdb, ok := db.(*memory.Database); ok {
			for pa, n := range mdb.PredicateCounts() {
				counts[key{pa.Name, pa.Arity}] = n
			}
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
