package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"swdunlop.dev/pkg/datalog"
)

// evalTimeout bounds Run, Apply, and agent query calls (doc/features/web-ui.md
// "Execution sandbox"); the fixpoint loop checks ctx.Err() per iteration and
// between strata, so even a hung ruleset is cleanly cancellable. Keystroke
// evaluation (parse+compile only) needs no timeout at all.
const evalTimeout = 5 * time.Second

// factCap is the hard per-evaluation output-fact limit (doc/features/web-ui.md):
// hitting it halts evaluation and reports "rule too broad" rather than
// letting a combinatorial ruleset run away. seminaive has no mid-evaluation
// hook to enforce this during Transform (checked via `go doc`; see
// checkFactCap's doc for the resulting post-hoc approach) — WithMaxIterations
// bounds fixpoint iterations, not output cardinality, and there is no
// per-stratum or per-fact callback to abort from mid-Transform.
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

// checkFactCap counts db's total facts across every predicate and reports
// an error identifying the ruleset as too broad if the count exceeds
// factCap. This is a post-Transform check, not a mid-evaluation halt:
// `go doc swdunlop.dev/pkg/datalog/seminaive` exposes WithMaxIterations
// (bounds fixpoint iterations) and WithProfile (post-hoc per-stratum
// stats), but no option or hook that aborts Transform once output
// cardinality crosses a threshold while it's still running. Enforcing the
// cap mid-evaluation would need a new seminaive.Option (e.g. a per-stratum
// fact-count callback that Transform consults after each iteration) — out
// of scope here per the task's instruction not to modify seminaive; noted
// for a follow-up engine change.
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

// generation is a monotonically increasing counter used for stale
// suppression (doc/features/web-ui.md): a newer evaluation supersedes an
// in-flight one, and only the result for the latest editor content is
// patched. Handlers call Next() when starting an evaluation and pass the
// returned token to Current() when the result is ready; if the token no
// longer matches, the result is discarded rather than patched.
type generation struct {
	n atomic.Uint64
}

// Next advances the generation and returns the new token for an
// in-flight evaluation to check against later.
func (g *generation) Next() uint64 { return g.n.Add(1) }

// Current returns the latest token. A caller holding an older token from
// Next knows its result is stale if Current() != its token.
func (g *generation) Current() uint64 { return g.n.Load() }

// Stale reports whether token no longer matches the current generation —
// i.e. a newer evaluation has started since this one did, so the caller's
// result should be discarded rather than patched.
func (g *generation) Stale(token uint64) bool { return g.Current() != token }
