package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"swdunlop.dev/pkg/datalog/seminaive"
)

// -- classifyQueryOutcome: the shared ordering-rule mechanism ----------------

// TestClassifyQueryOutcomeRendersCompletedResultDespiteCancelledCtx pins the
// round-two review's fix directly at the mechanism: a query that finished
// successfully (qErr nil, blk populated) must always be rendered, even when
// ctx is ALREADY cancelled by the time the caller checks — the exact gap
// where a Stop or shared-budget deadline landing right after runQuery
// returned used to discard the completed result and show only halt-status
// wording. Before the fix, both handleConsoleQuery and handleRulesRun
// checked ctx.Err() before looking at whether a result existed at all.
func TestClassifyQueryOutcomeRendersCompletedResultDespiteCancelledCtx(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // ctx is already Done before classifyQueryOutcome ever runs

	want := queryResultBlock{Query: "foo(X)?", Vars: []string{"X"}, Rows: [][]string{{"1"}}}
	out := classifyQueryOutcome(ctx, "foo(X)?", want, nil, "query stopped")

	if !out.RenderBlock {
		t.Fatalf("a completed result must always be rendered, even under a dead ctx")
	}
	if out.Block.Err != "" {
		t.Fatalf("completed result must not be turned into an error block: %+v", out.Block)
	}
	if len(out.Block.Rows) != 1 || out.Block.Rows[0][0] != "1" {
		t.Fatalf("completed result rows discarded: %+v", out.Block)
	}
	if out.Halt == "" {
		t.Fatalf("a trailing halt-status message must still follow a completed result under a dead ctx")
	}
	if out.Halt != "query stopped" {
		t.Fatalf("halt wording = %q, want the cancel wording via evalHaltStatus", out.Halt)
	}
	if out.Continue {
		t.Fatalf("Continue must be false once ctx has ended, even though this query's own result succeeded")
	}
}

// TestClassifyQueryOutcomeLiveCtxNoHalt is the control case: a live ctx
// produces no halt message and Continue stays true, so a multi-query loop
// keeps going.
func TestClassifyQueryOutcomeLiveCtxNoHalt(t *testing.T) {
	blk := queryResultBlock{Query: "foo(X)?"}
	out := classifyQueryOutcome(context.Background(), "foo(X)?", blk, nil, "query stopped")

	if out.Halt != "" {
		t.Fatalf("live ctx must not produce a halt message, got %q", out.Halt)
	}
	if !out.Continue {
		t.Fatalf("live ctx must let the loop continue")
	}
}

// TestClassifyQueryOutcomeQueryErrorIsNotAHalt covers the other axis: a
// query's own failure (qErr non-nil) under a still-live ctx is a normal
// per-query error, not a halt, and the loop must continue to the next query
// exactly as it did before this fix.
func TestClassifyQueryOutcomeQueryErrorIsNotAHalt(t *testing.T) {
	out := classifyQueryOutcome(context.Background(), "bad(X)?", queryResultBlock{}, errBoom, "query stopped")

	if !out.RenderBlock {
		t.Fatalf("a query's own error must always be rendered")
	}
	if out.Block.Err != errBoom.Error() {
		t.Fatalf("error block missing the query's own error: %+v", out.Block)
	}
	if out.Halt != "" {
		t.Fatalf("a query's own error must not produce halt-status wording, got %q", out.Halt)
	}
	if !out.Continue {
		t.Fatalf("a query's own error must not stop the multi-query loop")
	}
}

// TestClassifyQueryOutcomeCtxErrorNotDuplicated covers the other qErr-is-set
// axis: when qErr IS ctx's own cancellation/deadline (semi-naive's
// mid-Transform ctx check unwound the query and returned ctx.Err() verbatim,
// exactly what a real Stop landing mid-Transform produces), the block must
// be suppressed — the halt message alone says everything there is to say,
// and showing a bare "context canceled" error entry right next to "query
// stopped" would just duplicate the same event.
func TestClassifyQueryOutcomeCtxErrorNotDuplicated(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	out := classifyQueryOutcome(ctx, "slow(X)?", queryResultBlock{}, ctx.Err(), "query stopped")

	if out.RenderBlock {
		t.Fatalf("qErr that IS ctx.Err() must not also render a redundant error block: %+v", out.Block)
	}
	if out.Halt != "query stopped" {
		t.Fatalf("halt wording = %q, want the cancel wording", out.Halt)
	}
	if out.Continue {
		t.Fatalf("Continue must be false")
	}
}

// TestClassifyQueryOutcomeTimeoutWording exercises the deadline branch
// (rather than user cancel) to confirm evalHaltStatus's shared timeout
// wording comes through unchanged.
func TestClassifyQueryOutcomeTimeoutWording(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 0)
	defer cancel()
	<-ctx.Done()

	out := classifyQueryOutcome(ctx, "foo(X)?", queryResultBlock{}, nil, "run stopped")
	if out.Halt != "evaluation timed out, results may be incomplete" {
		t.Fatalf("timeout wording = %q", out.Halt)
	}
}

var errBoom = errors.New("boom")

// -- Run: ctx-after-lock guard, over-cap caching, lock-free Transform ------

// TestRunApplyRulesDocumentNoLateMutationAfterCancel is the mechanism-level
// regression for the missing ctx guard: runApplyRulesDocument is called with
// a ctx that is ALREADY cancelled, exactly the state a stopped Run's
// goroutine observes once it finally acquires wb.h.mu after the handler has
// already given up and reported "run stopped" (mirrors
// TestJSONFactsApplyNoLateMutationAfterCancel's proof for Apply). Before
// this fix, nothing gated the call at all: it always called
// setRulesWithQueries regardless of ctx, so this would have mutated
// rulesText right here.
func TestRunApplyRulesDocumentNoLateMutationAfterCancel(t *testing.T) {
	wb := newMordorWorkbench(t)

	wb.h.mu.Lock()
	before := wb.h.sess.rulesText
	wb.h.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // ctx is already done before runApplyRulesDocument is ever called

	res := <-runApplyRulesDocument(ctx, wb, "zzz_marker(X) :- event(_, X, _).\n")

	if res.err == nil {
		t.Fatalf("expected an error for an already-cancelled ctx, got queries %v", res.queries)
	}
	if !errors.Is(res.err, context.Canceled) {
		t.Fatalf("runApplyRulesDocument's error = %v, want context.Canceled (the ctx guard's own error), not a setRulesWithQueries failure", res.err)
	}

	wb.h.mu.Lock()
	after := wb.h.sess.rulesText
	wb.h.mu.Unlock()
	if after != before {
		t.Fatalf("rules were mutated despite an already-cancelled ctx: rulesText changed from %q to %q", before, after)
	}
}

// TestHTTP_RulesRunStopReportedAsStoppedAndPublishes drives the same
// deterministic cancel window TestJSONFactsApplyStopReportedAsStopped uses
// for Apply — wb.h.mu is held by the test so runApplyRulesDocument's
// goroutine parks on the lock after wb.jobs.Begin, giving a reliable window
// to fire /cancel before the mutation could possibly run — and additionally
// asserts the ctx-error return still publishes a session-changed event: the
// second half of item 2, that both early ctx-error returns must leave every
// open pane's Fact Browser in sync rather than stale, not just word the
// status line correctly.
func TestHTTP_RulesRunStopReportedAsStoppedAndPublishes(t *testing.T) {
	wb := newMordorWorkbench(t)
	srv := startTestServer(wb)
	defer srv.Close()

	sub := wb.bus.Subscribe()
	defer sub.Close()

	wb.h.mu.Lock() // forces runApplyRulesDocument's goroutine to block after Begin

	respCh := make(chan *http.Response, 1)
	go func() {
		resp := postSignals(t, srv, "/rules/run", map[string]any{"rulesText": "zzz_marker(X) :- event(_, X, _).\n"})
		respCh <- resp
	}()

	waitFor(t, func() bool { return jobActive(wb, rulesRunJobKey) })

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

	if !strings.Contains(joined, "run stopped") {
		t.Fatalf("cancel not reported with the stopped wording: %s", joined)
	}

	select {
	case <-sub.Events():
	case <-time.After(2 * time.Second):
		t.Fatal("expected a session-changed publish on the bus after a stopped Run; panes would be left stale")
	}
}

// TestHTTP_RulesRunOverCapDoesNotCache is the regression for item 3: Run
// must check the fact cap BEFORE caching or publishing, matching the
// startup evaluation's order (serve.go's newWorkbench). Before this fix,
// derivedDB = evaluated ran unconditionally on a successful Transform, and
// checkFactCap only gated what error text was SHOWN — so an over-cap
// evaluation was cached (and, via publishSessionChanged, patched into every
// open Fact Browser pane) even though the status line said "halting."
func TestHTTP_RulesRunOverCapDoesNotCache(t *testing.T) {
	dir := t.TempDir()
	writeSyntheticData(t, dir, 40) // cross product below: 40*40 = 1600 > factCap (1000)
	wb := newTestWorkbench(t, dir, "", nil, "test-token")
	srv := startTestServer(wb)
	defer srv.Close()

	if _, err := postSignalsSetSchema(t, srv); err != nil {
		t.Fatalf("priming schema: %v", err)
	}

	resp := postSignals(t, srv, "/rules/run", map[string]any{
		"rulesText": "pair(A,B) :- event(A,_,_), event(B,_,_).\n",
	})
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	joined := string(body)

	if !strings.Contains(joined, "rule too broad") {
		t.Fatalf("expected an over-cap error, got:\n%s", joined)
	}

	wb.h.mu.Lock()
	derived := wb.h.sess.derivedDB
	wb.h.mu.Unlock()
	if derived != nil {
		t.Fatalf("over-cap evaluation was cached into derivedDB despite exceeding factCap")
	}
}

// TestHTTP_RulesRunDoesNotHoldLockDuringTransform is the regression for item
// 4: Run previously held wb.h.mu across the entire evaluate() Transform,
// freezing every page load, SSE connect, and MCP call for up to evalTimeout.
// This drives a Run whose Transform takes a reliable, non-trivial amount of
// wall time (a cross product over 1200 synthetic facts — comfortably under
// evalTimeout but comfortably above scheduler noise) and polls for a window
// where wb.h.mu is acquirable WHILE the job is still registered as active.
// Before the fix, this loop would spin until the job finished without ever
// observing a successful TryLock, since the whole Transform ran under the
// lock.
func TestHTTP_RulesRunDoesNotHoldLockDuringTransform(t *testing.T) {
	dir := t.TempDir()
	writeSyntheticData(t, dir, 1200)
	wb := newTestWorkbench(t, dir, "", nil, "test-token")
	srv := startTestServer(wb)
	defer srv.Close()

	// This test wants a Transform that takes real wall-clock time (a 1200x1200
	// cross product), purely to observe the lock being released mid-Transform
	// — unrelated to what it's actually testing. newMCPHandlers now defaults
	// every session to WithFactLimit(factCap) (mcp.go, sandbox.go), which
	// would otherwise halt this cross product within microseconds of crossing
	// factCap (1000), long before the polling loop below gets a chance to
	// observe anything. Appending WithFactLimit(0) here overrides it back to
	// unlimited (options apply in order; see WithFactLimit's doc comment) for
	// this test's session only.
	wb.h.mu.Lock()
	wb.h.sess.engineOpts = append(wb.h.sess.engineOpts, seminaive.WithFactLimit(0))
	wb.h.mu.Unlock()

	if _, err := postSignalsSetSchema(t, srv); err != nil {
		t.Fatalf("priming schema: %v", err)
	}

	respCh := make(chan *http.Response, 1)
	go func() {
		resp := postSignals(t, srv, "/rules/run", map[string]any{
			"rulesText": "pair(A,B) :- event(A,_,_), event(B,_,_).\n",
		})
		respCh <- resp
	}()

	waitFor(t, func() bool { return jobActive(wb, rulesRunJobKey) })

	sawUnlocked := false
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) && jobActive(wb, rulesRunJobKey) {
		if wb.h.mu.TryLock() {
			wb.h.mu.Unlock()
			sawUnlocked = true
			break
		}
		time.Sleep(2 * time.Millisecond)
	}

	resp := <-respCh
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	if !sawUnlocked {
		t.Fatal("wb.h.mu was never acquirable while the Run job was active — the Transform appears to hold the lock for its full duration")
	}
}
