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
	"errors"
	"math"
	"strings"
	"testing"

	"swdunlop.dev/pkg/datalog"
	"swdunlop.dev/pkg/datalog/internal/interned"
	"swdunlop.dev/pkg/datalog/memory"
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
	ca, err := interned.CompileAtomV("blocked", []datalog.Term{
		datalog.Variable("X"), datalog.Variable("Y"),
	}, dict, varMap)
	if err != nil {
		t.Fatalf("CompileAtomV: %v", err)
	}

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
	if _, err := full.evalAggregates(context.Background(), rs.AggRules, nil, memFacts); err != nil {
		t.Fatalf("evalAggregates: %v", err)
	}

	if full.steps <= baseline.steps {
		t.Fatalf("evalAggregates advanced ev.steps to %d, no more than queryInternedFacts' own join-scan baseline of %d; "+
			"the group loop isn't driving the shared countStep counter (a leftover independent counter would produce exactly this)",
			full.steps, baseline.steps)
	}
}

// overArityTerms builds MaxFactArity+1 distinct integer terms -- one more
// term than interned.CompileAtomV accepts -- for constructing atoms that
// bypass Engine.Compile's checkRuleArity gate entirely. These tests call
// compileBody/evalRules/evalAggregates/queryInternedFacts directly (the way
// a future second compile path inside this package might, without routing
// through checkRuleArity first) to prove the arity guard now surfaces as an
// error from the interned package's own chokepoint, not a panic.
func overArityTerms() []datalog.Term {
	terms := make([]datalog.Term, interned.MaxFactArity+1)
	for i := range terms {
		terms[i] = datalog.Integer(i)
	}
	return terms
}

// TestCompileBodyRejectsOverArityAtom confirms compileBody -- the single
// atom-classification path shared by evalRules and evalAggregates via
// queryInternedFacts -- surfaces CompileAtomV's arity error as its own error
// return instead of letting the panic (now removed) escape uncaught.
func TestCompileBodyRejectsOverArityAtom(t *testing.T) {
	dict := interned.NewDict()
	body := []syntax.Atom{{Pred: "wide", Terms: overArityTerms()}}

	_, _, _, _, err := compileBody(body, dict, nil, nil)
	if err == nil {
		t.Fatalf("expected compileBody to return an error for an atom wider than MaxFactArity")
	}
	if !strings.Contains(err.Error(), "exceeds maximum") {
		t.Fatalf("expected a labeled arity-exceeded error, got %v", err)
	}
}

// TestEvalRulesRejectsOverArityHead confirms evalRules surfaces an over-arity
// rule head as a compile-time error rather than panicking, even when called
// directly and bypassing Engine.Compile's checkRuleArity gate -- exactly the
// "future compile path" scenario CompileAtomV's error return exists for.
func TestEvalRulesRejectsOverArityHead(t *testing.T) {
	dict := interned.NewDict()
	rules := []syntax.Rule{{
		Head: syntax.Atom{Pred: "wide", Terms: overArityTerms()},
		Body: []syntax.Atom{{Pred: "v", Terms: []datalog.Term{datalog.Variable("X")}}},
	}}

	ev := &evaluator{ctx: context.Background(), dict: dict}
	existing := interned.NewInternedFactSet()
	if _, _, err := ev.evalRules(context.Background(), rules, nil, existing, 10); err == nil {
		t.Fatalf("expected evalRules to return an error for an over-arity rule head")
	} else if !strings.Contains(err.Error(), "exceeds maximum") {
		t.Fatalf("expected a labeled arity-exceeded error, got %v", err)
	}
}

// TestEvalRulesRejectsOverArityBodyAtom is TestEvalRulesRejectsOverArityHead's
// counterpart for a body atom rather than the head, confirming compileBody's
// error (not just the head's separate CompileAtomV call in evalRules) reaches
// the caller.
func TestEvalRulesRejectsOverArityBodyAtom(t *testing.T) {
	dict := interned.NewDict()
	rules := []syntax.Rule{{
		Head: syntax.Atom{Pred: "out", Terms: []datalog.Term{datalog.Variable("X")}},
		Body: []syntax.Atom{{Pred: "wide", Terms: overArityTerms()}},
	}}

	ev := &evaluator{ctx: context.Background(), dict: dict}
	existing := interned.NewInternedFactSet()
	if _, _, err := ev.evalRules(context.Background(), rules, nil, existing, 10); err == nil {
		t.Fatalf("expected evalRules to return an error for an over-arity body atom")
	} else if !strings.Contains(err.Error(), "exceeds maximum") {
		t.Fatalf("expected a labeled arity-exceeded error, got %v", err)
	}
}

// TestQueryInternedFactsRejectsOverArityAtom confirms queryInternedFacts --
// the query-compilation path (also used by evalAggregates to run an
// aggregate's body) -- surfaces compileBody's arity error rather than
// panicking.
func TestQueryInternedFactsRejectsOverArityAtom(t *testing.T) {
	dict := interned.NewDict()
	body := []syntax.Atom{{Pred: "wide", Terms: overArityTerms()}}

	ev := &evaluator{ctx: context.Background(), dict: dict}
	if _, err := ev.queryInternedFacts(context.Background(), body, interned.NewInternedFactSet()); err == nil {
		t.Fatalf("expected queryInternedFacts to return an error for an over-arity atom")
	} else if !strings.Contains(err.Error(), "exceeds maximum") {
		t.Fatalf("expected a labeled arity-exceeded error, got %v", err)
	}
}

// TestEvalAggregatesRejectsOverArityHead confirms evalAggregates surfaces an
// over-arity aggregate head (compiled via the CompileAtom convenience
// wrapper, not CompileAtomV) as an error rather than panicking.
func TestEvalAggregatesRejectsOverArityHead(t *testing.T) {
	dict := interned.NewDict()
	vID := dict.Intern("v")
	memFacts := interned.NewInternedFactSet()
	var f interned.InternedFact
	f.Pred = vID
	f.Arity = 1
	f.Values[0] = dict.Intern(int64(1))
	memFacts.Add(f)

	headTerms := append(overArityTerms(), datalog.Variable("S"))
	aggRules := []syntax.AggregateRule{{
		Head:      syntax.Atom{Pred: "wide", Terms: headTerms},
		ResultVar: "S",
		Kind:      syntax.AggCount,
		Body:      []syntax.Atom{{Pred: "v", Terms: []datalog.Term{datalog.Variable("X")}}},
	}}

	ev := &evaluator{ctx: context.Background(), dict: dict}
	if _, err := ev.evalAggregates(context.Background(), aggRules, nil, memFacts); err == nil {
		t.Fatalf("expected evalAggregates to return an error for an over-arity aggregate head")
	} else if !strings.Contains(err.Error(), "exceeds maximum") {
		t.Fatalf("expected a labeled arity-exceeded error, got %v", err)
	}
}

// TestInt64OverflowSharedSentinel confirms both int64 arithmetic overflow
// paths -- the is-expression path (applyBinOp, which panics
// arithmeticOverflowError recovered by recoverEvalError) and the AggSum path
// (which returns an error directly) -- surface errors that satisfy
// errors.Is(err, errInt64Overflow). The two paths share one overflow reason
// so a caller can detect "int64 overflow" without branching on which
// arithmetic surface produced it.
func TestInt64OverflowSharedSentinel(t *testing.T) {
	// is-expression path: MaxInt64 + 1 overflows via applyBinOp.
	{
		b := memory.NewBuilder()
		b.AddFact(datalog.Fact{Name: "v", Terms: []datalog.Constant{datalog.Integer(math.MaxInt64)}})
		rs, err := syntax.ParseAll(`r(R) :- v(N), R is N + 1.`)
		if err != nil {
			t.Fatal(err)
		}
		tr, err := New().Compile(rs)
		if err != nil {
			t.Fatal(err)
		}
		_, err = tr.Transform(context.Background(), b.Build())
		if err == nil {
			t.Fatal("is-expression overflow: expected an error, got nil")
		}
		if !errors.Is(err, errInt64Overflow) {
			t.Fatalf("is-expression overflow: errors.Is(err, errInt64Overflow) is false; got %v", err)
		}
	}

	// AggSum path: two near-MaxInt64 values sum past the range.
	{
		b := memory.NewBuilder()
		b.AddFact(datalog.Fact{Name: "v", Terms: []datalog.Constant{datalog.String("a"), datalog.Integer(math.MaxInt64 - 1)}})
		b.AddFact(datalog.Fact{Name: "v", Terms: []datalog.Constant{datalog.String("b"), datalog.Integer(math.MaxInt64 - 1)}})
		rs, err := syntax.ParseAll(`total(T) :- T = sum(X) : v(?, X).`)
		if err != nil {
			t.Fatal(err)
		}
		tr, err := New().Compile(rs)
		if err != nil {
			t.Fatal(err)
		}
		_, err = tr.Transform(context.Background(), b.Build())
		if err == nil {
			t.Fatal("AggSum overflow: expected an error, got nil")
		}
		if !errors.Is(err, errInt64Overflow) {
			t.Fatalf("AggSum overflow: errors.Is(err, errInt64Overflow) is false; got %v", err)
		}
	}
}
