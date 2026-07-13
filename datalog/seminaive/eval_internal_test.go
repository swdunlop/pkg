package seminaive

// White-box tests that call unexported evaluator internals directly. These
// exist for cases where proving a mechanism (as opposed to end-to-end
// Transform behavior) needs a measurement -- like ev.steps -- that isn't
// observable from outside the package, or where an end-to-end wall-clock
// assertion would be swamped by unrelated costs (e.g. InternedFactSet.Scan's
// column-index build, which dwarfs the scan loop itself at any fact count
// large enough to make a timing assertion meaningful).

import (
	"context"
	"testing"

	"swdunlop.dev/pkg/datalog"
	"swdunlop.dev/pkg/datalog/internal/interned"
	"swdunlop.dev/pkg/datalog/syntax"
)

// TestMatchesAnyVCancelsDuringScan confirms matchesAnyV's own scan loops
// sample ctx via countStep, rather than relying on some coarser caller to
// notice cancellation only after the scan finishes. Before countStep calls
// were added inside matchesAnyV's two scan loops, this negation check had no
// cancellation point of its own: with a cancelled ctx it still visited every
// fact in the blocked set before returning, and ev.steps would end up equal
// to the full fact count. With the fix, the evalCancelled panic unwinds out
// of matchesAnyV as soon as countStep's periodic sample (every
// evalStepsPerCheck steps) observes the cancellation, so ev.steps stops
// within one sampling window of where cancellation happened -- far short of
// the full scan.
func TestMatchesAnyVCancelsDuringScan(t *testing.T) {
	dict := interned.NewDict()
	// NewLightInternedFactSet has a nil ByCol, so Scan always returns the
	// predicate's full unfiltered fact slice (see Scan's "fs.ByCol == nil"
	// fast path) instead of building a column index. A real evaluator run
	// never hits this: existing/emitted in evalRules and queryInternedFacts
	// are always full InternedFactSets with indexing enabled, so a bound
	// term that matches nothing there resolves in O(1) via the index rather
	// than a linear scan. Using the light variant here isolates exactly the
	// code path this test targets -- matchesAnyV's own loop over a Scan
	// result -- from Scan's unrelated (and, at realistic negation-check
	// selectivities, usually beneficial) indexing behavior.
	existing := interned.NewLightInternedFactSet()

	blockedID := dict.Intern("blocked")
	const factCount = 100_000
	for i := range factCount {
		var f interned.InternedFact
		f.Pred = blockedID
		f.Arity = 2
		f.Values[0] = dict.Intern(int64(1)) // X = 1 in every fact
		f.Values[1] = dict.Intern(int64(i))
		existing.Add(f)
	}

	// X is bound to 2, which never appears (every fact has X = 1), so
	// MatchesBound rejects every fact and the loop must walk the entire
	// slice without an early match; Y is left unbound.
	varMap := map[string]int8{"X": 0, "Y": 1}
	ca := interned.CompileAtomV("blocked", []datalog.Term{
		datalog.Variable("X"), datalog.Variable("Y"),
	}, dict, varMap)

	var sub interned.VarSub
	sub.Set(0, dict.Intern(int64(2)))

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled before the scan starts

	ev := &evaluator{ctx: ctx, dict: dict}
	noEmitted := interned.InternedFactSet{}

	func() {
		defer func() {
			r := recover()
			if r != evalCancelled {
				t.Fatalf("expected matchesAnyV to panic with evalCancelled, got: %v", r)
			}
		}()
		ev.matchesAnyV(ca, &sub, existing, noEmitted)
		t.Fatal("matchesAnyV returned normally; expected it to panic with evalCancelled on a cancelled ctx")
	}()

	// The scan should abort within one sampling window of countStep, not
	// after visiting every fact.
	if ev.steps == 0 {
		t.Fatal("ev.steps is 0; matchesAnyV's scan loop never called countStep")
	}
	if ev.steps > 2*evalStepsPerCheck {
		t.Fatalf("matchesAnyV visited %d facts before aborting (fact set has %d); "+
			"expected it to abort within roughly one countStep sampling window (%d)",
			ev.steps, factCount, evalStepsPerCheck)
	}
}

// TestEvaluatorCtxUsableBeforeAnyEntryPoint confirms ev.ctx is usable by
// countStep the moment an evaluator is constructed the way transformer.go's
// Transform builds one (the only construction site in production code:
// &evaluator{ctx: ctx, dict: ..., ...}), with no call to evalRules or
// queryInternedFacts having run first to backfill ev.ctx as a side effect.
// Before ctx was injected in the evaluator literal, ev.ctx was the zero
// value (nil) until evalRules or queryInternedFacts assigned it via their
// own "ev.ctx = ctx" ritual line, and evalAggregates' group loop -- which
// now calls countStep instead of its own inline ctx.Err() counter -- only
// happened to work because every real call path reaches queryInternedFacts
// (which set ev.ctx as a side effect) before the group loop. This test
// constructs an evaluator using only the literal transformer.go uses and
// calls countStep with neither evalRules nor queryInternedFacts having run;
// on the old code (no ctx field in the literal, ritual assignment removed)
// this panics with a nil pointer dereference instead of evalCancelled --
// proving the field is wired at construction, not by ritual.
func TestEvaluatorCtxUsableBeforeAnyEntryPoint(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Mirrors transformer.go's `ev := &evaluator{ctx: ctx, dict: dict, ...}`
	// exactly -- no evalRules/queryInternedFacts call precedes this.
	ev := &evaluator{ctx: ctx, dict: interned.NewDict()}
	ev.steps = evalStepsPerCheck - 1 // next countStep call lands on a sampled step

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("countStep returned normally; expected it to observe the already-cancelled ctx")
		}
		if r != evalCancelled {
			t.Fatalf("expected countStep to panic with evalCancelled, got a different panic (likely a nil ctx dereference): %v", r)
		}
	}()
	ev.countStep()
}

// TestEvalAggregatesSharesCountStepCounter confirms evalAggregates' group
// loop advances the same ev.steps counter evalBodyRecursiveV's join scans
// and matchesAnyV use, rather than an independent inline counter of its own
// (the old "groupsChecked % evalStepsPerCheck", which this consolidation
// replaced). It isolates the group loop's contribution from
// queryInternedFacts' join-scan contribution by running the identical body
// query twice with fresh evaluators -- once alone (queryBaseline, giving the
// join-only step count) and once as part of a full evalAggregates call --
// and asserting the full call adds strictly more steps than the join alone
// accounts for. Before this consolidation, evalAggregates' group loop step
// count would be exactly queryBaseline (the group loop didn't touch
// ev.steps at all).
func TestEvalAggregatesSharesCountStepCounter(t *testing.T) {
	dict := interned.NewDict()
	memFacts := interned.NewInternedFactSet()
	vID := dict.Intern("v")
	const groupCount = 5
	for i := range groupCount {
		var f interned.InternedFact
		f.Pred = vID
		f.Arity = 1
		f.Values[0] = dict.Intern(int64(i))
		memFacts.Add(f)
	}

	rs, err := syntax.ParseAll(`total(X, S) :- S = count : v(X).`)
	if err != nil {
		t.Fatal(err)
	}
	if len(rs.AggRules) != 1 {
		t.Fatalf("expected exactly one aggregate rule, got %d", len(rs.AggRules))
	}
	body := rs.AggRules[0].Body

	baseline := &evaluator{ctx: context.Background(), dict: dict}
	if _, err := baseline.queryInternedFacts(context.Background(), body, memFacts); err != nil {
		t.Fatalf("queryInternedFacts baseline: %v", err)
	}

	full := &evaluator{ctx: context.Background(), dict: dict}
	if _, err := full.evalAggregates(context.Background(), rs.AggRules, memFacts); err != nil {
		t.Fatalf("evalAggregates: %v", err)
	}

	if full.steps <= baseline.steps {
		t.Fatalf("evalAggregates advanced ev.steps to %d, no more than queryInternedFacts' own join-scan baseline of %d; "+
			"the group loop isn't driving the shared countStep counter (a leftover independent counter would produce exactly this)",
			full.steps, baseline.steps)
	}
}
