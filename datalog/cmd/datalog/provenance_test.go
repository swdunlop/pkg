package main

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"swdunlop.dev/pkg/datalog"
)

// -- session-level cache-beside-recorder swap ------------------------------

// TestExplain_ResolvesAgainstCachedRecorder is the core correctness
// assertion from doc/features/provenance.md's "Session cache interaction":
// after a query populates the derivedDB cache, a later explain must resolve
// against the recorder that ACTUALLY PRODUCED that cached database, not a
// recorder built fresh from (possibly stale) session state. This pins it by
// asserting derivedProv is populated beside derivedDB, and that the
// PROVENANCE OBJECT ITSELF (pointer identity, not just its content) is
// exactly what a second explain call reuses without recomputation.
func TestExplain_ResolvesAgainstCachedRecorder(t *testing.T) {
	h, done := newTestHandlers(t, t.TempDir())
	defer done()
	h.sess.provenanceEnabled = true

	if _, err := h.setRules(setRulesInput{Source: `
event("h1").
derived(X) :- event(X).
`}); err != nil {
		t.Fatalf("set_rules: %v", err)
	}

	if _, err := h.query(context.Background(), queryInput{Query: `derived(X)?`}); err != nil {
		t.Fatalf("query: %v", err)
	}
	if h.sess.derivedDB == nil {
		t.Fatal("query: expected derivedDB to be populated")
	}
	if h.sess.derivedProv == nil {
		t.Fatal("query: expected derivedProv to be populated BESIDE derivedDB (cache-beside-recorder)")
	}
	cachedProv := h.sess.derivedProv

	out, err := h.explain(context.Background(), explainInput{Fact: `derived("h1")`})
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	if !strings.Contains(out.Tree, `derived("h1")`) {
		t.Fatalf("explain: tree does not mention the derived fact: %q", out.Tree)
	}
	if !strings.Contains(out.Tree, `event("h1")`) {
		t.Fatalf("explain: tree does not cite the base fact it was derived from: %q", out.Tree)
	}
	if !strings.Contains(out.Tree, "[base fact]") {
		t.Fatalf("explain: base fact not marked [base fact]: %q", out.Tree)
	}

	// A second explain must not recompute — the cached recorder is reused
	// verbatim (pointer identity), exactly like a second query reuses
	// derivedDB (TestQuery_PopulatesAndReusesDerivedCache).
	if _, err := h.explain(context.Background(), explainInput{Fact: `derived("h1")`}); err != nil {
		t.Fatalf("explain (second): %v", err)
	}
	if h.sess.derivedProv != cachedProv {
		t.Error("explain: derivedProv pointer changed on a second call; expected the cached recorder to be reused")
	}
}

// TestExplain_GenerationBumpInvalidatesCache asserts the other half of the
// spec's cache discipline: a set_rules landing after a query has cached
// derivedDB/derivedProv must invalidate BOTH together (the same five
// mutators that clear derivedDB also clear derivedProv — session.go), so a
// later explain resolves against the NEW ruleset's derivation, never a
// stale recorder left over from the old one. This is also the "set_rules
// invalidates explanations" risk from doc/features/provenance.md's Risks
// section, exercised at the session/cache layer rather than the browser's
// SSE repaint layer.
func TestExplain_GenerationBumpInvalidatesCache(t *testing.T) {
	h, done := newTestHandlers(t, t.TempDir())
	defer done()
	h.sess.provenanceEnabled = true

	if _, err := h.setRules(setRulesInput{Source: `
event("h1").
derived(X) :- event(X).
`}); err != nil {
		t.Fatalf("set_rules (first): %v", err)
	}
	if _, err := h.query(context.Background(), queryInput{Query: `derived(X)?`}); err != nil {
		t.Fatalf("query: %v", err)
	}
	if h.sess.derivedDB == nil || h.sess.derivedProv == nil {
		t.Fatal("query: expected derivedDB and derivedProv to be populated")
	}

	// A new ruleset that no longer derives "derived" from event at all —
	// set_rules must clear both derivedDB and derivedProv (session.go's five
	// gen-bumping mutators).
	if _, err := h.setRules(setRulesInput{Source: `
event("h1").
other(X) :- event(X).
`}); err != nil {
		t.Fatalf("set_rules (second): %v", err)
	}
	if h.sess.derivedDB != nil {
		t.Fatal("set_rules: derivedDB should be nil after a rules change")
	}
	if h.sess.derivedProv != nil {
		t.Fatal("set_rules: derivedProv should be nil after a rules change (cache-beside-recorder invalidation)")
	}

	// explain against the fact from the OLD ruleset must now report
	// not-found — it recomputes against the CURRENT (new) ruleset, which no
	// longer derives "derived" at all.
	if _, err := h.explain(context.Background(), explainInput{Fact: `derived("h1")`}); err == nil {
		t.Fatal("explain: expected an error for a fact the current ruleset no longer derives, got none")
	}

	// The new ruleset's own derived fact explains fine, and the recorder
	// this landed is a fresh one, not the old (now-cleared) cache.
	out, err := h.explain(context.Background(), explainInput{Fact: `other("h1")`})
	if err != nil {
		t.Fatalf("explain (new ruleset's fact): %v", err)
	}
	if !strings.Contains(out.Tree, `other("h1")`) {
		t.Fatalf("explain: tree does not mention the new ruleset's derived fact: %q", out.Tree)
	}
}

// TestExplain_QueryScopedSynthStageDoesNotPolluteBaseRecorder asserts the
// spec's "_q_ stage is query-scoped and discarded" clause: the synthetic
// _q_ rule runQuery compiles per query runs its OWN Transform (with its own
// fresh Provenance, if any) over the cached base — that stage's recorder
// must never overwrite session.derivedProv, so a later explain of a
// perfectly ordinary user predicate still resolves against the BASE
// ruleset's recorder, not something contaminated by (or missing because of)
// the _q_ stage.
func TestExplain_QueryScopedSynthStageDoesNotPolluteBaseRecorder(t *testing.T) {
	h, done := newTestHandlers(t, t.TempDir())
	defer done()
	h.sess.provenanceEnabled = true

	if _, err := h.setRules(setRulesInput{Source: `
event("h1").
derived(X) :- event(X).
`}); err != nil {
		t.Fatalf("set_rules: %v", err)
	}

	if _, err := h.query(context.Background(), queryInput{Query: `derived(X)?`}); err != nil {
		t.Fatalf("query: %v", err)
	}
	baseProv := h.sess.derivedProv
	if baseProv == nil {
		t.Fatal("query: expected derivedProv to be populated")
	}

	// Explain a user predicate under the _q_ stage: must resolve fine
	// against the cached BASE recorder, and must not have been replaced by
	// anything from the query's synthetic stage.
	out, err := h.explain(context.Background(), explainInput{Fact: `derived("h1")`})
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	if !strings.Contains(out.Tree, `derived("h1")`) {
		t.Fatalf("explain: tree missing the fact: %q", out.Tree)
	}
	if h.sess.derivedProv != baseProv {
		t.Error("session.derivedProv changed after a query; the _q_ stage's recorder must never overwrite the base recorder")
	}

	// Explaining a _q_ head itself is explicitly out of scope (spec: "not a
	// goal") — it must not panic or corrupt state; a not-found error is
	// acceptable.
	_, _ = h.explain(context.Background(), explainInput{Fact: `_q_("h1")`})
	if h.sess.derivedProv != baseProv {
		t.Error("session.derivedProv changed after explaining a _q_ predicate; base recorder must stay intact")
	}
}

// TestExplain_UnknownFactReportsNoSuchDerivedFact asserts the "unknown fact
// -> a clear error, not an empty tree" requirement from the feature's MCP
// surface section.
func TestExplain_UnknownFactReportsNoSuchDerivedFact(t *testing.T) {
	h, done := newTestHandlers(t, t.TempDir())
	defer done()
	h.sess.provenanceEnabled = true

	if _, err := h.setRules(setRulesInput{Source: `
event("h1").
derived(X) :- event(X).
`}); err != nil {
		t.Fatalf("set_rules: %v", err)
	}

	_, err := h.explain(context.Background(), explainInput{Fact: `derived("nonexistent")`})
	if err == nil {
		t.Fatal("explain: expected an error for a fact the evaluation never produced, got none")
	}
	if !strings.Contains(err.Error(), "no such derived fact") {
		t.Fatalf("explain: error does not read as a clear not-found message: %v", err)
	}
}

// TestExplain_DisabledWhenProvenanceOff asserts the library default stays
// off and a session with provenance disabled (as most direct &session{}
// tests are, and as any future caller outside cmd/datalog would be) reports
// a distinct, clear error rather than silently behaving as "not found".
func TestExplain_DisabledWhenProvenanceOff(t *testing.T) {
	h, done := newTestHandlers(t, t.TempDir())
	defer done()
	// h.sess.provenanceEnabled left false (the zero value) deliberately.

	if _, err := h.setRules(setRulesInput{Source: `
event("h1").
derived(X) :- event(X).
`}); err != nil {
		t.Fatalf("set_rules: %v", err)
	}

	_, err := h.explain(context.Background(), explainInput{Fact: `derived("h1")`})
	if err == nil {
		t.Fatal("explain: expected an error when provenance is disabled, got none")
	}
	if !strings.Contains(err.Error(), "provenance") {
		t.Fatalf("explain: error does not mention provenance being disabled: %v", err)
	}
}

// TestExplain_DepthOption asserts the "depth" input threads through to
// seminaive.MaxDepth, capping how much of a deep chain renders.
func TestExplain_DepthOption(t *testing.T) {
	h, done := newTestHandlers(t, t.TempDir())
	defer done()
	h.sess.provenanceEnabled = true

	if _, err := h.setRules(setRulesInput{Source: `
event("h1").
d1(X) :- event(X).
d2(X) :- d1(X).
d3(X) :- d2(X).
`}); err != nil {
		t.Fatalf("set_rules: %v", err)
	}
	if _, err := h.query(context.Background(), queryInput{Query: `d3(X)?`}); err != nil {
		t.Fatalf("query: %v", err)
	}

	out, err := h.explain(context.Background(), explainInput{Fact: `d3("h1")`, Depth: 1})
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	if strings.Contains(out.Tree, "[base fact]") {
		t.Fatalf("explain: depth=1 tree unexpectedly reached the base fact: %q", out.Tree)
	}
	if !strings.Contains(out.Tree, "truncated") {
		t.Fatalf("explain: depth=1 tree does not report truncation: %q", out.Tree)
	}
}

// -- REPL .why --------------------------------------------------------------

func TestReplWhy(t *testing.T) {
	var buf strings.Builder
	r := &repl{session: &session{provenanceEnabled: true}, out: &buf}

	if err := r.execStatement(`event("h1").`); err != nil {
		t.Fatalf("execStatement (fact): %v", err)
	}
	if err := r.execStatement(`derived(X) :- event(X).`); err != nil {
		t.Fatalf("execStatement (rule): %v", err)
	}

	if err := cmdWhy(r, `derived("h1")`); err != nil {
		t.Fatalf(".why: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, `derived("h1")`) {
		t.Fatalf(".why: output does not mention the derived fact: %q", out)
	}
	if !strings.Contains(out, `event("h1")`) {
		t.Fatalf(".why: output does not cite the base fact: %q", out)
	}
}

func TestReplWhy_NoArgs(t *testing.T) {
	r := &repl{session: &session{provenanceEnabled: true}, out: io.Discard}
	if err := cmdWhy(r, ""); err == nil {
		t.Fatal(".why: expected a usage error for no args, got none")
	}
}

func TestReplWhy_UnknownFact(t *testing.T) {
	var buf strings.Builder
	r := &repl{session: &session{provenanceEnabled: true}, out: &buf}
	if err := r.execStatement(`event("h1").`); err != nil {
		t.Fatalf("execStatement: %v", err)
	}
	if err := r.execStatement(`derived(X) :- event(X).`); err != nil {
		t.Fatalf("execStatement (rule): %v", err)
	}
	err := cmdWhy(r, `derived("nope")`)
	if err == nil {
		t.Fatal(".why: expected an error for an unknown fact, got none")
	}
	if !strings.Contains(err.Error(), "no such derived fact") {
		t.Fatalf(".why: error does not read as not-found: %v", err)
	}
}

// TestParseFactStatement covers parseFactStatement's normalization
// (trailing "." optional) and its rejections (a body, a non-ground head).
func TestParseFactStatement(t *testing.T) {
	fact, err := parseFactStatement(`concern("ws01", 87)`)
	if err != nil {
		t.Fatalf("parseFactStatement (no trailing dot): %v", err)
	}
	if fact.Name != "concern" || len(fact.Terms) != 2 {
		t.Fatalf("parseFactStatement: got %+v, want concern/2", fact)
	}

	fact2, err := parseFactStatement(`concern("ws01", 87).`)
	if err != nil {
		t.Fatalf("parseFactStatement (trailing dot): %v", err)
	}
	if fact2.Name != "concern" {
		t.Fatalf("parseFactStatement: got %+v", fact2)
	}

	if _, err := parseFactStatement(`concern(X) :- event(X).`); err == nil {
		t.Fatal("parseFactStatement: expected an error for a rule with a body, got none")
	}
	if _, err := parseFactStatement(`concern(X).`); err == nil {
		t.Fatal("parseFactStatement: expected an error for a non-ground fact, got none")
	}
	if _, err := parseFactStatement(``); err == nil {
		t.Fatal("parseFactStatement: expected an error for empty input, got none")
	}
}

// -- session policy: cmd/datalog defaults provenance on ----------------------

// TestSessionPolicy_MCPHandlersEnableProvenanceByDefault pins
// doc/features/provenance.md's "Session policy": every cmd/datalog session
// (both `datalog mcp` and `datalog serve`, which both build through
// newMCPHandlers) enables provenance by default — the library default
// (seminaive's own, unchanged) stays off.
func TestSessionPolicy_MCPHandlersEnableProvenanceByDefault(t *testing.T) {
	h, closeFn, err := newMCPHandlers(t.TempDir(), "", nil, 5_000_000_000)
	if err != nil {
		t.Fatalf("newMCPHandlers: %v", err)
	}
	defer closeFn()
	if !h.sess.provenanceEnabled {
		t.Fatal("newMCPHandlers: expected provenanceEnabled = true by default")
	}
}

// TestSessionPolicy_REPLEnablesProvenanceByDefault is newREPL's half of the
// same default.
func TestSessionPolicy_REPLEnablesProvenanceByDefault(t *testing.T) {
	r := newREPL()
	if !r.session.provenanceEnabled {
		t.Fatal("newREPL: expected provenanceEnabled = true by default")
	}
}

// -- Fact Browser "why?" affordance (HTTP) -----------------------------------

// TestHandleWhy_RendersExplanationIntoConsole drives the "why?" button's
// actual HTTP target end to end: POST /why/{predicate}/{arity}?fact=<literal>
// must append a rendered derivation tree to the console drawer's Query tab
// scrollback (view.ExplainEntry), the same surface handleConsoleQuery's
// results land in.
func TestHandleWhy_RendersExplanationIntoConsole(t *testing.T) {
	wb := newTestWorkbench(t, t.TempDir(), "", nil, "test-token")
	srv := startTestServer(wb)
	defer srv.Close()

	resp := postSignals(t, srv, "/rules/run", map[string]any{
		"rulesText": "event(\"h1\").\nderived(X) :- event(X).\n",
	})
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	req, err := http.NewRequest(http.MethodPost, srv.URL+`/why/derived/1?fact=`+`derived("h1")`, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Accept", "text/event-stream")
	whyResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /why: %v", err)
	}
	defer whyResp.Body.Close()
	io.Copy(io.Discard, whyResp.Body)

	log := renderLog(wb, "query")
	if !strings.Contains(log, "explain") {
		t.Fatalf("expected an explain-kind entry in the query scrollback: %s", log)
	}
	if !strings.Contains(log, `derived(&quot;h1&quot;)`) && !strings.Contains(log, `derived("h1")`) {
		t.Fatalf("expected the derived fact in the rendered tree: %s", log)
	}
	if !strings.Contains(log, "event") {
		t.Fatalf("expected the base fact citation in the rendered tree: %s", log)
	}
}

// TestHandleWhy_UnknownFactRendersError covers the not-found path through
// the HTTP handler.
func TestHandleWhy_UnknownFactRendersError(t *testing.T) {
	wb := newTestWorkbench(t, t.TempDir(), "", nil, "test-token")
	srv := startTestServer(wb)
	defer srv.Close()

	resp := postSignals(t, srv, "/rules/run", map[string]any{
		"rulesText": "event(\"h1\").\nderived(X) :- event(X).\n",
	})
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	req, err := http.NewRequest(http.MethodPost, srv.URL+`/why/derived/1?fact=`+`derived("nope")`, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Accept", "text/event-stream")
	whyResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /why: %v", err)
	}
	defer whyResp.Body.Close()
	io.Copy(io.Discard, whyResp.Body)

	log := renderLog(wb, "query")
	if !strings.Contains(log, "no such derived fact") {
		t.Fatalf("expected a not-found error entry: %s", log)
	}
}

// TestHandleFacts_WhyButtonOnlyOnDerivedPredicate asserts the row-level
// affordance rule from doc/features/provenance.md's Fact Browser surface:
// "base-fact rows get none" — a derived predicate's /facts page carries the
// why? button per row, a base (EDB) predicate's does not, even though both
// predicates coexist in the same evaluated session.
func TestHandleFacts_WhyButtonOnlyOnDerivedPredicate(t *testing.T) {
	dir := t.TempDir()
	writeSyntheticData(t, dir, 2)
	wb := newTestWorkbench(t, dir, "", nil, "test-token")
	srv := startTestServer(wb)
	defer srv.Close()

	resp, err := postSignalsSetSchema(t, srv)
	if err != nil {
		t.Fatalf("set schema: %v", err)
	}
	resp.Body.Close()

	runResp := postSignals(t, srv, "/rules/run", map[string]any{
		"rulesText": `derived(X) :- event(X, _, _).`,
	})
	io.Copy(io.Discard, runResp.Body)
	runResp.Body.Close()

	baseResp, err := http.Get(srv.URL + "/facts/event/3")
	if err != nil {
		t.Fatalf("GET /facts/event/3: %v", err)
	}
	defer baseResp.Body.Close()
	baseBody, _ := io.ReadAll(baseResp.Body)
	if strings.Contains(string(baseBody), "why?") {
		t.Fatalf("base predicate's facts page unexpectedly carries a why? button: %s", baseBody)
	}

	derivedResp, err := http.Get(srv.URL + "/facts/derived/1")
	if err != nil {
		t.Fatalf("GET /facts/derived/1: %v", err)
	}
	defer derivedResp.Body.Close()
	derivedBody, _ := io.ReadAll(derivedResp.Body)
	if !strings.Contains(string(derivedBody), "why?") {
		t.Fatalf("derived predicate's facts page missing a why? button: %s", derivedBody)
	}
}

// -- true/false/null constant literals unlock explain on Bool/Null facts ----
//
// Before the syntax package learned true/false/null as constant literals
// (parseTerm), a derived fact carrying a datalog.Bool or datalog.Null term
// could not be named as Datalog source text: printing it produced
// "flagged(true)", but re-parsing that text bound Variable("true") instead
// of Bool(true), so parseFactStatement's IsFact/ground check rejected it and
// factLiteral omitted the why? button rather than emit a literal that would
// always fail to resolve. These three tests pin the fix across all three
// explain surfaces named in doc/features/provenance.md: MCP explain, REPL
// .why, and the Fact Browser's why? button.

// TestExplain_ResolvesFactWithBoolAndNullTerms is the MCP explain surface's
// half: a ruleset deriving both a Bool-carrying and a Null-carrying fact,
// each explained by literal text containing "true"/"null".
func TestExplain_ResolvesFactWithBoolAndNullTerms(t *testing.T) {
	h, done := newTestHandlers(t, t.TempDir())
	defer done()
	h.sess.provenanceEnabled = true

	if _, err := h.setRules(setRulesInput{Source: `
event("h1").
flagged(X, true) :- event(X).
unset(X, null) :- event(X).
`}); err != nil {
		t.Fatalf("set_rules: %v", err)
	}

	out, err := h.explain(context.Background(), explainInput{Fact: `flagged("h1", true)`})
	if err != nil {
		t.Fatalf("explain (bool term): %v", err)
	}
	if !strings.Contains(out.Tree, `flagged("h1", true)`) {
		t.Fatalf("explain: tree does not mention the derived fact: %q", out.Tree)
	}
	if !strings.Contains(out.Tree, `event("h1")`) {
		t.Fatalf("explain: tree does not cite the base fact: %q", out.Tree)
	}

	out2, err := h.explain(context.Background(), explainInput{Fact: `unset("h1", null)`})
	if err != nil {
		t.Fatalf("explain (null term): %v", err)
	}
	if !strings.Contains(out2.Tree, `unset("h1", null)`) {
		t.Fatalf("explain: tree does not mention the derived fact: %q", out2.Tree)
	}
	if !strings.Contains(out2.Tree, `event("h1")`) {
		t.Fatalf("explain: tree does not cite the base fact: %q", out2.Tree)
	}
}

// TestReplWhy_BoolAndNullTerms is the REPL .why surface's half of the same
// fix.
func TestReplWhy_BoolAndNullTerms(t *testing.T) {
	var buf strings.Builder
	r := &repl{session: &session{provenanceEnabled: true}, out: &buf}

	if err := r.execStatement(`event("h1").`); err != nil {
		t.Fatalf("execStatement (fact): %v", err)
	}
	if err := r.execStatement(`flagged(X, true) :- event(X).`); err != nil {
		t.Fatalf("execStatement (bool rule): %v", err)
	}
	if err := r.execStatement(`unset(X, null) :- event(X).`); err != nil {
		t.Fatalf("execStatement (null rule): %v", err)
	}

	if err := cmdWhy(r, `flagged("h1", true)`); err != nil {
		t.Fatalf(".why (bool term): %v", err)
	}
	if err := cmdWhy(r, `unset("h1", null)`); err != nil {
		t.Fatalf(".why (null term): %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, `flagged("h1", true)`) {
		t.Fatalf(".why: output does not mention the bool-carrying fact: %q", out)
	}
	if !strings.Contains(out, `unset("h1", null)`) {
		t.Fatalf(".why: output does not mention the null-carrying fact: %q", out)
	}
	if !strings.Contains(out, `event("h1")`) {
		t.Fatalf(".why: output does not cite the base fact: %q", out)
	}
}

// TestHandleFacts_WhyButtonOnBoolAndNullTerms is the Fact Browser surface's
// half: a derived predicate whose rows carry Bool/Null terms must still get
// the why? button (factLiteral's doc comment now excludes only ID and
// Composite, not Bool/Null).
func TestHandleFacts_WhyButtonOnBoolAndNullTerms(t *testing.T) {
	wb := newTestWorkbench(t, t.TempDir(), "", nil, "test-token")
	srv := startTestServer(wb)
	defer srv.Close()

	resp := postSignals(t, srv, "/rules/run", map[string]any{
		"rulesText": "event(\"h1\").\nflagged(X, true) :- event(X).\nunset(X, null) :- event(X).\n",
	})
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	flaggedResp, err := http.Get(srv.URL + "/facts/flagged/2")
	if err != nil {
		t.Fatalf("GET /facts/flagged/2: %v", err)
	}
	defer flaggedResp.Body.Close()
	flaggedBody, _ := io.ReadAll(flaggedResp.Body)
	if !strings.Contains(string(flaggedBody), "why?") {
		t.Fatalf("bool-carrying derived row missing a why? button: %s", flaggedBody)
	}

	unsetResp, err := http.Get(srv.URL + "/facts/unset/2")
	if err != nil {
		t.Fatalf("GET /facts/unset/2: %v", err)
	}
	defer unsetResp.Body.Close()
	unsetBody, _ := io.ReadAll(unsetResp.Body)
	if !strings.Contains(string(unsetBody), "why?") {
		t.Fatalf("null-carrying derived row missing a why? button: %s", unsetBody)
	}
}

// TestFactLiteral_IncludesBoolAndNull pins factLiteral's own contract
// directly: Bool and Null are round-trippable (ok=true), ID and Composite
// remain excluded (ok=false), matching the doc comment's exact enumeration.
func TestFactLiteral_IncludesBoolAndNull(t *testing.T) {
	lit, ok := factLiteral("flagged", []datalog.Constant{datalog.String("h1"), datalog.Bool(true)})
	if !ok || lit != `flagged("h1", true)` {
		t.Fatalf("factLiteral (bool): got (%q, %v), want (%q, true)", lit, ok, `flagged("h1", true)`)
	}
	lit2, ok2 := factLiteral("unset", []datalog.Constant{datalog.String("h1"), datalog.Null{}})
	if !ok2 || lit2 != `unset("h1", null)` {
		t.Fatalf("factLiteral (null): got (%q, %v), want (%q, true)", lit2, ok2, `unset("h1", null)`)
	}
	if _, ok := factLiteral("idfact", []datalog.Constant{datalog.ID(1)}); ok {
		t.Fatal("factLiteral (ID): expected ok=false, got true")
	}
	if _, ok := factLiteral("compfact", []datalog.Constant{&datalog.Composite{}}); ok {
		t.Fatal("factLiteral (Composite): expected ok=false, got true")
	}
}
