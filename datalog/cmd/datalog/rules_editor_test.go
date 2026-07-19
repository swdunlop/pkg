package main

import (
	"context"
	"errors"
	"testing"
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
