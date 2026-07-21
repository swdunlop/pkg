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

// defaultMaxFacts and defaultEvalTimeout are the operator-facing defaults for
// --max-facts (datalog serve) / -max-facts (datalog mcp) and --eval-timeout /
// -timeout respectively (doc/features/workbench-scale.md design decision 1):
// sized for a real dataset (the OpTC starter slice derives ~300k facts over
// ~6 minutes of base load), not the old 1000-fact/5-second demo guillotine
// tuned for examples/mordor's 506 events. Both flags accept 0 to disable
// their respective mechanism entirely ("no cap, rely on Stop + OOM" /
// "no deadline, Stop is the only brake"); these constants are only the
// starting point an operator gets without passing either flag.
const (
	defaultMaxFacts    = 10_000_000
	defaultEvalTimeout = 5 * time.Minute
)

// evalContext derives ctx from parent for one bounded evaluation (Run, Apply,
// agent query, the Fact Browser's "why?", autoReevaluate, the composer's
// query command — every site that used to hardcode context.WithTimeout(ctx,
// evalTimeout), doc/features/workbench-scale.md work item 2). h.timeout is
// the operator-resolved --eval-timeout ("datalog mcp"'s -timeout flag, or
// "datalog serve"'s --eval-timeout, both defaulting to a value sized for real
// data, not the old 5s demo guillotine); this is the ONE place that turns
// that duration into a context, so no site can silently keep a stale
// deadline. h.timeout <= 0 means "no deadline — Stop is the only brake": the
// returned context is WithCancel, never WithDeadline, so it does not
// pre-expire the instant it's created. The fixpoint loop's own ctx.Err()
// checks per iteration/stratum still make even an unbounded evaluation
// cleanly cancellable via Global Cancel. Keystroke evaluation (parse+compile
// only) must keep bypassing this helper entirely — it needs no timeout at
// all, bounded or not.
func (h *mcpHandlers) evalContext(parent context.Context) (context.Context, context.CancelFunc) {
	if h.timeout <= 0 {
		return context.WithCancel(parent)
	}
	return context.WithTimeout(parent, h.timeout)
}

// checkFactCap counts db's total facts across every predicate — base facts
// loaded from the input source PLUS everything derived — and reports an
// error identifying the evaluation as exceeding the operator's --max-facts
// budget if that total exceeds h.maxFacts. This measures something
// WithFactLimit does not and so is NOT subsumed by it: WithFactLimit (wired
// into every session.engineOpts, see newMCPHandlers) only counts facts
// derived DURING one Transform call, deliberately excluding whatever was
// already loaded, so a query against a large but legitimate already-loaded
// dataset still runs to completion. checkFactCap's total-size check exists
// for a different purpose — deciding whether Run's result is small enough to
// cache into session.derivedDB (rules_editor.go's handleRulesRun) or to keep
// resident after startup evaluation (serve.go's newWorkbench) — where what
// matters is how much this evaluation will hold in memory as the workbench's
// cached "derived" view, not just how much of it this one Transform call
// newly computed. It remains a post-Transform check (there is no cheaper way
// to size a whole database), called only after Transform has already
// succeeded — WithFactLimit's mid-evaluation halt still bounds peak
// allocation during that Transform itself. h.maxFacts <= 0 means "no cap":
// every db passes, matching --max-facts 0's "no cap, rely on Stop + OOM"
// contract.
func (h *mcpHandlers) checkFactCap(db datalog.Database) error {
	if h.maxFacts <= 0 {
		return nil
	}
	total := 0
	for name, arity := range db.Predicates() {
		for range db.Facts(name, arity) {
			total++
			if total > h.maxFacts {
				return fmt.Errorf(capExceededMsg, h.maxFacts)
			}
		}
	}
	return nil
}

// capExceededMsg is the wording shared by checkFactCap's post-hoc check and
// translateFactLimit's mid-Transform translation (doc/features/
// workbench-scale.md work item 3): both mechanisms catch the same class of
// runaway ruleset by different means, and must read identically to a human
// or agent regardless of which one fired. It names the limit and how to
// raise it — replacing the old "rule too broad" wording, which reads as "you
// wrote a bad rule" even when a developer is legitimately deriving millions
// of facts and just hasn't raised the flag yet.
const capExceededMsg = "evaluation exceeded the --max-facts limit (%d); raise it with --max-facts or stop the run"

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

// translateFactLimit rewords a seminaive.FactLimitError (matchable via
// errors.As — see WithFactLimit's doc comment in seminaive/engine.go) into
// the same capExceededMsg phrasing checkFactCap's post-hoc check uses, so a
// caller can't tell from the message alone which mechanism caught the
// runaway ruleset: the mid-evaluation halt and the post-hoc check are meant
// to read identically to the human or agent on the other end. err is
// returned unchanged if it does not wrap a FactLimitError.
// Callers wrap every Transform call whose error can reach a user-facing
// surface (evaluateSnapshot, runQuery's two stages) with this.
func translateFactLimit(err error) error {
	var limitErr seminaive.FactLimitError
	if errors.As(err, &limitErr) {
		return fmt.Errorf(capExceededMsg, limitErr.Limit)
	}
	return err
}
