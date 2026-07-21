package main

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
)

// This file pins doc/features/workbench-scale.md work items 5 and 6:
// --provenance as an operator switch (design decision 4) and the Fact
// Browser's total coming from PredicateCounts instead of a full fact scan
// (design decision 5). Items 1-3 are pinned in mcp_test.go's cap/timeout
// tests, item 4 in serve_async_test.go.

// TestProvenanceFlagOffDisablesExplain constructs handlers with the
// --provenance flag's off value and asserts the one behavior the flag
// controls: session provenance is disabled, so explain refuses with the
// established "not started with provenance enabled" wording rather than
// recording per-fact derivations (design decision 4's headroom trade).
func TestProvenanceFlagOffDisablesExplain(t *testing.T) {
	h, closeFn, err := newMCPHandlers(t.TempDir(), "", nil, "", 5_000_000_000, defaultMaxFacts, false, false)
	if err != nil {
		t.Fatalf("newMCPHandlers: %v", err)
	}
	defer closeFn()

	if h.sess.provenanceEnabled {
		t.Fatal("provenance=false did not disable session provenance")
	}
	_, err = h.explainDerivation(context.Background(), explainInput{Fact: "x(1)"})
	if err == nil || !strings.Contains(err.Error(), "provenance") {
		t.Fatalf("explain on a provenance-off session: err = %v, want the provenance-disabled refusal", err)
	}
}

// TestHandleFacts_TotalFromPredicateCountsAndPaging loads 120 base facts —
// more than two pages — and asserts the Fact Browser's paging contract
// survives the PredicateCounts total (design decision 5): the first page's
// Load More names the true total, a deep offset returns exactly the tail
// window, and an exhausted predicate renders no Load More button.
func TestHandleFacts_TotalFromPredicateCountsAndPaging(t *testing.T) {
	dir := t.TempDir()
	writeSyntheticData(t, dir, 120)
	wb := newTestWorkbench(t, dir, "", nil, "test-token")
	srv := startTestServer(wb)
	defer srv.Close()
	applyTestSchema(t, wb, syntheticSchemaYAML)

	get := func(path string) string {
		t.Helper()
		resp, err := srv.Client().Get(srv.URL + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET %s: status %d", path, resp.StatusCode)
		}
		return string(body)
	}

	first := get("/facts/event/3")
	if !strings.Contains(first, "Load more (50 of 120)") {
		t.Fatalf("first page's Load More must name the PredicateCounts total: %s", first)
	}
	if n := strings.Count(first, "<tr"); n != 51 { // 50 rows + 1 header row
		t.Fatalf("first page rows = %d <tr>s, want 51 (50 facts + header)", n)
	}

	tail := get("/facts/event/3?offset=100")
	if n := strings.Count(tail, "<tr"); n != 20 {
		t.Fatalf("offset=100 rows = %d <tr>s, want the 20-fact tail", n)
	}
	if strings.Contains(tail, "Load more") {
		t.Fatalf("exhausted predicate still offers Load More: %s", tail)
	}
}
