package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

// jobActive reports whether key is currently registered in wb.jobs — used
// here to poll for "the Apply goroutine has reached wb.jobs.Begin and is now
// waiting on wb.h.mu" before the test cancels it, since the whole point of
// these tests is to land the cancel inside that specific window.
func jobActive(wb *workbench, key string) bool {
	wb.jobs.mu.Lock()
	defer wb.jobs.mu.Unlock()
	_, ok := wb.jobs.active[key]
	return ok
}

// -- jsonfacts Apply: cancel-vs-timeout wording and no-late-mutation --------

// TestJSONFactsApplyStopReportedAsStopped is the regression for the
// round-two review's finding that handleJSONFactsApply hard-coded "Apply
// timed out" on <-ctx.Done() regardless of WHY ctx ended, missing the sweep
// that made evalHaltStatus the single classifier for cancel-vs-timeout
// wording (handleRulesRun and handleConsoleQuery already went through it).
// This test drives the user-cancel branch specifically: wb.h.mu is held by
// the test so runApplySchema's goroutine parks on the lock after
// wb.jobs.Begin, giving the test a deterministic window to fire /cancel
// (CancelAll, what the Apply button's Stop morph posts) before the mutation
// could possibly run, then release the lock and inspect the response.
func TestJSONFactsApplyStopReportedAsStopped(t *testing.T) {
	wb := newMordorWorkbench(t)
	srv := startTestServer(wb)
	defer srv.Close()

	wb.h.mu.Lock() // forces runApplySchema's goroutine to block after Begin

	respCh := make(chan *http.Response, 1)
	go func() {
		resp := postSignals(t, srv, "/jsonfacts/apply", map[string]any{"schemaText": syntheticSchemaYAML})
		respCh <- resp
	}()

	waitFor(t, func() bool { return jobActive(wb, jsonfactsApplyJobKey) })

	cancelResp, err := http.Post(srv.URL+"/cancel", "", nil)
	if err != nil {
		t.Fatalf("POST /cancel: %v", err)
	}
	cancelResp.Body.Close()

	wb.h.mu.Unlock() // let the parked goroutine observe the now-cancelled ctx

	resp := <-respCh
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	joined := string(body)

	if !strings.Contains(joined, "Apply stopped") {
		t.Fatalf("cancel not reported with the stopped wording: %s", joined)
	}
	if strings.Contains(joined, "timed out") {
		t.Fatalf("user cancel misreported as a timeout: %s", joined)
	}
}

// TestJSONFactsApplyNoLateMutationAfterCancel is the second half of the same
// review item, pinned directly at the mechanism instead of raced through
// HTTP timing: it calls runApplySchema exactly as handleJSONFactsApply does,
// but with a ctx that is ALREADY cancelled before the call — precisely the
// state an abandoned Apply's goroutine observes once it finally acquires
// wb.h.mu after the handler has already given up, reported failure, and
// returned (the wording test above demonstrates that gap is real: holding
// wb.h.mu from the test reliably parks the goroutine there). Before this
// fix, runApplySchema took no ctx parameter and called wb.h.setSchema
// unconditionally, so it would have mutated the schema right here. This
// asserts setSchema was never reached: the predicate synthetic-schema.yaml
// would add ("event") must still be absent, and the channel must deliver
// ctx.Err() itself, not a setSchemaOutput.
func TestJSONFactsApplyNoLateMutationAfterCancel(t *testing.T) {
	wb := newMordorWorkbench(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // ctx is already done before runApplySchema is ever called

	resultCh := runApplySchema(ctx, wb, syntheticSchemaYAML)
	res := <-resultCh

	if res.err == nil {
		t.Fatalf("expected an error for an already-cancelled ctx, got result %+v", res.out)
	}
	if !errors.Is(res.err, context.Canceled) {
		t.Fatalf("runApplySchema's error = %v, want context.Canceled (the ctx guard's own error), not a setSchema failure", res.err)
	}

	wb.h.mu.Lock()
	out, err := wb.h.listPredicates(listPredicatesInput{})
	wb.h.mu.Unlock()
	if err != nil {
		t.Fatalf("list_predicates: %v", err)
	}
	for _, p := range out.Predicates {
		if p.Name == "event" {
			t.Fatalf("schema was applied despite an already-cancelled ctx: %+v", out.Predicates)
		}
	}
}
