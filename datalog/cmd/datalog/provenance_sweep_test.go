package main

import (
	"context"
	"strings"
	"testing"
)

// multiStratumRules is a join + negation + aggregate program spanning several
// strata: base events, a join-derived indicator, a negation-gated flag, and an
// aggregate rolling the indicators up into a score. It is the end-to-end sweep
// fixture from the provenance validation charter.
const multiStratumRules = `
event("ws01", "port_scan").
event("ws01", "new_admin").
event("ws02", "port_scan").
allow("ws02", "port_scan").
indicator(H, K) :- event(H, K).
alert(H, K) :- indicator(H, K), not allow(H, K).
score(H, N) :- N = count : alert(H, K).
`

// TestProvenanceSweep_EndToEnd drives the whole feature through one session:
// explain right after a Transform must equal explain after a cache hit; the
// aggregate head renders the spec's group-summary shape; and set_rules then
// explain yields a fresh tree or a not-found, never a stale one.
func TestProvenanceSweep_EndToEnd(t *testing.T) {
	h, done := newTestHandlers(t, t.TempDir())
	defer done()
	h.sess.provenanceEnabled = true

	if _, err := h.setRules(setRulesInput{Source: multiStratumRules}); err != nil {
		t.Fatalf("set_rules: %v", err)
	}

	// Cold explain: no query has run, so this drives the explain cold path
	// (computes the fixpoint, caches beside it). This is "explain right after
	// Transform" — the recorder is freshly minted here.
	cold, err := h.explain(context.Background(), explainInput{Fact: `alert("ws01", "port_scan")`})
	if err != nil {
		t.Fatalf("explain (cold): %v", err)
	}
	if !strings.Contains(cold.Tree, `alert("ws01", "port_scan")`) {
		t.Fatalf("explain: missing head fact: %q", cold.Tree)
	}
	if !strings.Contains(cold.Tree, `not allow(`) {
		t.Fatalf("explain: negation detail not rendered: %q", cold.Tree)
	}
	if !strings.Contains(cold.Tree, `indicator("ws01", "port_scan")`) {
		t.Fatalf("explain: join body fact not cited: %q", cold.Tree)
	}

	// After the cold explain, derivedProv is cached. A second explain must
	// return byte-identical output — resolving against the cached recorder,
	// not a fresh Transform.
	warm, err := h.explain(context.Background(), explainInput{Fact: `alert("ws01", "port_scan")`})
	if err != nil {
		t.Fatalf("explain (warm): %v", err)
	}
	if cold.Tree != warm.Tree {
		t.Fatalf("explain-after-cache-hit differs from explain-right-after-Transform:\ncold=%q\nwarm=%q", cold.Tree, warm.Tree)
	}

	// ws02's port_scan is allow-listed, so no alert — explain must report
	// not-found (negation genuinely suppressed the derivation).
	if _, err := h.explain(context.Background(), explainInput{Fact: `alert("ws02", "port_scan")`}); err == nil {
		t.Fatal("explain: expected not-found for an allow-listed (suppressed) alert, got none")
	}

	// Aggregate head: score("ws01", 2) rolls up ws01's two alerts (port_scan
	// and new_admin; ws02's port_scan is allow-listed and suppressed).
	agg, err := h.explain(context.Background(), explainInput{Fact: `score("ws01", 2)`})
	if err != nil {
		t.Fatalf("explain (aggregate): %v", err)
	}
	if !strings.Contains(agg.Tree, "aggregated over 2 solutions") {
		t.Fatalf("explain: aggregate summary line missing/incorrect: %q", agg.Tree)
	}
	if !strings.Contains(agg.Tree, "(all shown)") {
		t.Fatalf("explain: aggregate should say all shown for a 2-solution group under the cap: %q", agg.Tree)
	}

	// set_rules then explain: the old fact must vanish (not-found), never a
	// stale tree.
	if _, err := h.setRules(setRulesInput{Source: `
event("ws01", "port_scan").
other(H) :- event(H, _).
`}); err != nil {
		t.Fatalf("set_rules (second): %v", err)
	}
	if _, err := h.explain(context.Background(), explainInput{Fact: `alert("ws01", "port_scan")`}); err == nil {
		t.Fatal("explain: expected not-found after set_rules dropped the alert rule, got a tree")
	}
	fresh, err := h.explain(context.Background(), explainInput{Fact: `other("ws01")`})
	if err != nil {
		t.Fatalf("explain (fresh ruleset): %v", err)
	}
	if !strings.Contains(fresh.Tree, `other("ws01")`) {
		t.Fatalf("explain: fresh ruleset's fact not explained: %q", fresh.Tree)
	}
}

// TestProvenanceSweep_QueryThenExplainMatchesColdExplain asserts the cache
// path and the cold path agree: an explain served from the recorder a query
// cached must match one computed fresh from an unqueried session with the same
// ruleset.
func TestProvenanceSweep_QueryThenExplainMatchesColdExplain(t *testing.T) {
	// Session A: query first (populates cache), then explain (cache hit).
	ha, doneA := newTestHandlers(t, t.TempDir())
	defer doneA()
	ha.sess.provenanceEnabled = true
	if _, err := ha.setRules(setRulesInput{Source: multiStratumRules}); err != nil {
		t.Fatalf("A set_rules: %v", err)
	}
	if _, err := ha.query(context.Background(), queryInput{Query: `alert(H, K)?`}); err != nil {
		t.Fatalf("A query: %v", err)
	}
	viaCache, err := ha.explain(context.Background(), explainInput{Fact: `alert("ws01", "port_scan")`})
	if err != nil {
		t.Fatalf("A explain: %v", err)
	}

	// Session B: explain directly (cold path), no query.
	hb, doneB := newTestHandlers(t, t.TempDir())
	defer doneB()
	hb.sess.provenanceEnabled = true
	if _, err := hb.setRules(setRulesInput{Source: multiStratumRules}); err != nil {
		t.Fatalf("B set_rules: %v", err)
	}
	viaCold, err := hb.explain(context.Background(), explainInput{Fact: `alert("ws01", "port_scan")`})
	if err != nil {
		t.Fatalf("B explain: %v", err)
	}

	if viaCache.Tree != viaCold.Tree {
		t.Fatalf("query-cached explain differs from cold explain:\ncache=%q\ncold=%q", viaCache.Tree, viaCold.Tree)
	}
}
