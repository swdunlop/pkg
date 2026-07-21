package main

import (
	"context"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
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

// gateFS wraps a fs.FS so a test can hold prepareSchema's data load open at
// its first file access: entered closes when the load reaches Open, and Open
// then blocks until the test closes release. This is how the tests below get
// a deterministic "load in flight" window without a multi-gigabyte dataset.
type gateFS struct {
	fs.FS
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (g *gateFS) Open(name string) (fs.File, error) {
	g.once.Do(func() { close(g.entered) })
	<-g.release
	return g.FS.Open(name)
}

// newGatedDeferredHandlers builds deferred-load handlers over a synthetic
// dataset and swaps a gateFS under them, returning the gate alongside.
func newGatedDeferredHandlers(t *testing.T) (*mcpHandlers, *gateFS) {
	t.Helper()
	dir := t.TempDir()
	writeSyntheticData(t, dir, 5)
	cfgPath := filepath.Join(dir, "schema.yaml")
	if err := os.WriteFile(cfgPath, []byte(syntheticSchemaYAML), 0o644); err != nil {
		t.Fatalf("writing schema: %v", err)
	}
	h, closeFn, err := newMCPHandlers(dir, cfgPath, nil, "", time.Minute, defaultMaxFacts, true, true)
	if err != nil {
		t.Fatalf("newMCPHandlers: %v", err)
	}
	t.Cleanup(func() { closeFn() })
	g := &gateFS{FS: h.fsys, entered: make(chan struct{}), release: make(chan struct{})}
	h.fsys = g
	return h, g
}

// TestLoadDeferredSchema_PrepareRunsWithoutHoldingLock pins THE property the
// background startup load exists for (workbench-scale.md design decision 3):
// h.mu stays acquirable while the multi-minute dataset load runs, because
// every pane render and tool call takes h.mu — a load that held it would
// leave the listener accepting connections whose responses never come, the
// exact dead-workbench symptom observed on the first real OpTC run.
func TestLoadDeferredSchema_PrepareRunsWithoutHoldingLock(t *testing.T) {
	h, g := newGatedDeferredHandlers(t)

	errCh := make(chan error, 1)
	go func() { errCh <- h.loadDeferredSchema() }()

	select {
	case <-g.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("load never reached the data read")
	}

	locked := make(chan struct{})
	go func() {
		h.mu.Lock()
		h.mu.Unlock() //nolint:staticcheck // probing acquirability, not guarding state
		close(locked)
	}()
	select {
	case <-locked:
	case <-time.After(2 * time.Second):
		t.Fatal("h.mu is held across the deferred load's data read; panes would hang for the whole load")
	}

	close(g.release)
	if err := <-errCh; err != nil {
		t.Fatalf("loadDeferredSchema: %v", err)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.sess.schemaText == "" || h.sess.dataDB == nil {
		t.Fatal("deferred load did not commit the schema after release")
	}
}

// TestLoadDeferredSchema_ConcurrentSchemaWriteWins pins the stale-swap guard:
// the listener is up during the load, so a schema write can land in the
// lock-free window; the deferred load's older snapshot must be discarded
// rather than clobbering it (the same posture reloadSchema takes for a save
// landing during its prepare).
func TestLoadDeferredSchema_ConcurrentSchemaWriteWins(t *testing.T) {
	h, g := newGatedDeferredHandlers(t)

	errCh := make(chan error, 1)
	go func() { errCh <- h.loadDeferredSchema() }()
	select {
	case <-g.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("load never reached the data read")
	}

	const concurrent = "declarations: []\n"
	h.mu.Lock()
	h.sess.schemaText = concurrent
	h.sess.gen++
	genAfterWrite := h.sess.gen
	h.mu.Unlock()

	close(g.release)
	if err := <-errCh; err != nil {
		t.Fatalf("loadDeferredSchema: %v", err)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.sess.schemaText != concurrent {
		t.Fatalf("deferred load clobbered a concurrent schema write: schemaText = %q", h.sess.schemaText)
	}
	if h.sess.gen != genAfterWrite {
		t.Fatalf("deferred load bumped gen (%d -> %d) despite discarding its snapshot", genAfterWrite, h.sess.gen)
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
