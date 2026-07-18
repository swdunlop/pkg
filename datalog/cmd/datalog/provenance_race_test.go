package main

import (
	"context"
	"sync"
	"testing"
)

// TestProvenanceRace_ConcurrentExplainQuerySetRules hammers the three session
// surfaces that read or invalidate the cache-beside-recorder pair
// concurrently, under -race: explain (reads derivedDB/derivedProv, may write
// them on a cold path), query (writes them via cacheDerivedQuery), and
// set_rules (invalidates both). The correctness invariant this guards is that
// derivedDB and derivedProv never diverge — an explain either resolves a fact
// against the recorder that produced the DB it is holding, or reports
// not-found; it must never resolve a cache-hit DB against a recorder from a
// different Transform. A divergence would surface here either as a -race
// report on the shared fields or as a panic inside ExplainTree resolving a key
// against the wrong dict.
func TestProvenanceRace_ConcurrentExplainQuerySetRules(t *testing.T) {
	h, done := newTestHandlers(t, t.TempDir())
	defer done()
	h.sess.provenanceEnabled = true

	if _, err := h.sess.setRules(multiStratumRules); err != nil {
		t.Fatalf("set_rules: %v", err)
	}

	const workers = 8
	const iters = 60
	var wg sync.WaitGroup
	ctx := context.Background()

	// explainers
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				// Errors are fine (a set_rules may have dropped the fact); a
				// panic or a -race report is the failure this test hunts.
				_, _ = h.explain(ctx, explainInput{Fact: `alert("ws01", "port_scan")`})
				_, _ = h.explain(ctx, explainInput{Fact: `score("ws01", 2)`})
			}
		}()
	}

	// queriers
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				_, _ = h.query(ctx, queryInput{Query: `alert(H, K)?`})
			}
		}()
	}

	// mutators: flip between two rulesets so the cache is repeatedly
	// invalidated and repopulated against a genuinely different derivation.
	rulesetB := `
event("ws01", "port_scan").
event("ws01", "new_admin").
event("ws02", "port_scan").
allow("ws02", "port_scan").
indicator(H, K) :- event(H, K).
alert(H, K) :- indicator(H, K), not allow(H, K).
score(H, N) :- N = count : indicator(H, K).
`
	for w := 0; w < 2; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				src := multiStratumRules
				if (i+id)%2 == 0 {
					src = rulesetB
				}
				h.mu.Lock()
				_, err := h.sess.setRules(src)
				h.mu.Unlock()
				if err != nil {
					t.Errorf("set_rules: %v", err)
					return
				}
			}
		}(w)
	}

	wg.Wait()

	// Final coherence check: after all the churn, whatever pair is cached must
	// be internally consistent — an explain of a fact the current ruleset
	// derives must succeed, resolving the cached DB against the cached
	// recorder without panicking.
	if _, err := h.sess.setRules(multiStratumRules); err != nil {
		t.Fatalf("final set_rules: %v", err)
	}
	if _, err := h.query(ctx, queryInput{Query: `alert(H, K)?`}); err != nil {
		t.Fatalf("final query: %v", err)
	}
	out, err := h.explain(ctx, explainInput{Fact: `alert("ws01", "port_scan")`})
	if err != nil {
		t.Fatalf("final explain: %v", err)
	}
	if out.Tree == "" {
		t.Fatal("final explain: empty tree")
	}
}
