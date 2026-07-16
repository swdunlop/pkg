package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"swdunlop.dev/pkg/datalog"
	"swdunlop.dev/pkg/datalog/seminaive"
)

// evalTimeout bounds Run, Apply, and agent query calls (doc/features/web-ui.md
// "Execution sandbox"); the fixpoint loop checks ctx.Err() per iteration and
// between strata, so even a hung ruleset is cleanly cancellable. Keystroke
// evaluation (parse+compile only) needs no timeout at all.
const evalTimeout = 5 * time.Second

// factCap is the hard per-evaluation output-fact limit (doc/features/web-ui.md):
// hitting it halts evaluation and reports "rule too broad" rather than
// letting a combinatorial ruleset run away. Every seminaive.Engine the
// workbench builds is constructed with seminaive.WithFactLimit(factCap) (see
// session.engineOpts, set once in newMCPHandlers and threaded through every
// Compile call — evaluateSnapshot, runQuery's two stages, and the trial
// compiles in handleRulesCheck/setRules), so the cap is enforced DURING
// Transform: evaluation halts with a seminaive.FactLimitError the moment the
// derived-fact count crosses factCap, rather than after a runaway cross
// product has already materialized every tuple and exhausted memory.
// translateFactLimit turns that typed error into the same "rule too broad"
// wording checkFactCap below used to produce post-hoc.
const factCap = 1000

// jobs is the Global Cancel cancel-set (doc/notes/datastar.md §9): each
// running operation registers a CancelFunc under a job name key
// ((userID, resourceID, jobName) collapses to just jobName since the
// workbench is single-user, per the design). Begin returns (nil, nil) when
// the key is already busy so a second click while one is in flight is a
// no-op rather than a race; CancelAll is the Global Cancel's "/cancel"
// handler, the emergency brake rather than surgical per-operation
// cancellation.
type jobs struct {
	mu     sync.Mutex
	active map[string]context.CancelFunc
}

// newJobs constructs an empty job set.
func newJobs() *jobs {
	return &jobs{active: make(map[string]context.CancelFunc)}
}

// Begin registers a new job under key, deriving a cancellable context from
// parent. It returns (nil, nil) if key is already busy. The returned done
// func must be deferred by the caller; it unregisters the job and cancels
// its context (releasing any resources tied to ctx.Done()) exactly once.
func (j *jobs) Begin(parent context.Context, key string) (context.Context, func()) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if _, busy := j.active[key]; busy {
		return nil, nil
	}
	ctx, cancel := context.WithCancel(parent)
	j.active[key] = cancel
	return ctx, func() {
		j.mu.Lock()
		delete(j.active, key)
		j.mu.Unlock()
		cancel()
	}
}

// Cancel fires the CancelFunc registered under key, if any.
func (j *jobs) Cancel(key string) {
	j.mu.Lock()
	if c, ok := j.active[key]; ok {
		c()
	}
	j.mu.Unlock()
}

// CancelAll fires every registered CancelFunc — the Global Cancel's
// emergency brake (doc/features/web-ui.md). Single-user makes the blunt
// instrument acceptable: there's exactly one human and their delegate
// agent, and Cancel is meant to stop everything they're waiting on.
func (j *jobs) CancelAll() {
	j.mu.Lock()
	defer j.mu.Unlock()
	for _, c := range j.active {
		c()
	}
}

// runRecovered runs fn in a goroutine (doc/features/web-ui.md's goroutine
// isolation: evaluation must never block the handler that started it) and
// translates any panic into a formatted error, delivering the result over
// the returned channel exactly once. Callers select on the channel and
// ctx.Done() so a cancelled or timed-out caller isn't stuck waiting on a
// goroutine that ignores its context (fn is expected to honor ctx itself
// for prompt cancellation; runRecovered only guarantees the caller gets
// *an* answer eventually, not that fn stops early).
func runRecovered(fn func() error) <-chan error {
	done := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				done <- fmt.Errorf("evaluation panicked: %v", r)
			}
		}()
		done <- fn()
	}()
	return done
}

// checkFactCap counts db's total facts across every predicate — base facts
// loaded from the input source PLUS everything derived — and reports an
// error identifying the ruleset as too broad if that total exceeds factCap.
// This measures something WithFactLimit does not and so is NOT subsumed by
// it: WithFactLimit (wired into every session.engineOpts, see factCap's doc
// comment) only counts facts derived DURING one Transform call, deliberately
// excluding whatever was already loaded, so a query against a large but
// legitimate already-loaded dataset still runs to completion. checkFactCap's
// total-size check exists for a different purpose — deciding whether Run's
// result is small enough to cache into session.derivedDB (rules_editor.go's
// handleRulesRun) or to keep resident after startup evaluation (serve.go's
// newWorkbench) — where what matters is how much this evaluation will hold
// in memory as the workbench's cached "derived" view, not just how much of
// it this one Transform call newly computed. It remains a post-Transform
// check (there is no cheaper way to size a whole database), called only
// after Transform has already succeeded — WithFactLimit's mid-evaluation
// halt still bounds peak allocation during that Transform itself.
func checkFactCap(db datalog.Database) error {
	total := 0
	for name, arity := range db.Predicates() {
		for range db.Facts(name, arity) {
			total++
			if total > factCap {
				return fmt.Errorf("rule too broad: output exceeds %d facts, halting", factCap)
			}
		}
	}
	return nil
}

// translateFactLimit rewords a seminaive.FactLimitError (matchable via
// errors.As — see WithFactLimit's doc comment in seminaive/engine.go) into
// the same "rule too broad ... halting" phrasing checkFactCap's post-hoc
// check used, so a caller can't tell from the message alone which mechanism
// caught the runaway ruleset: the mid-evaluation halt and the old post-hoc
// check are meant to read identically to the human or agent on the other
// end. err is returned unchanged if it does not wrap a FactLimitError.
// Callers wrap every Transform call whose error can reach a user-facing
// surface (evaluateSnapshot, runQuery's two stages) with this.
func translateFactLimit(err error) error {
	var limitErr seminaive.FactLimitError
	if errors.As(err, &limitErr) {
		return fmt.Errorf("rule too broad: evaluation derived more than %d facts, halting", limitErr.Limit)
	}
	return err
}

