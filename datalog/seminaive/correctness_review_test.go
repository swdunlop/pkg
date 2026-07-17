package seminaive_test

// This file collects regression tests for a batch of four correctness bugs
// found in a code review of the seminaive package (see review doctrine: fix
// at the compile-time mechanism, not the runtime call site that surfaced the
// symptom). Kept separate from the other _test.go files so it doesn't
// collide with other in-flight edits to them.

import (
	"context"
	"errors"
	"iter"
	"math"
	"testing"

	"swdunlop.dev/pkg/datalog"
	"swdunlop.dev/pkg/datalog/memory"
	"swdunlop.dev/pkg/datalog/seminaive"
	"swdunlop.dev/pkg/datalog/syntax"
)

// --- BUG #4: invalid constant @regex_match pattern must fail at Compile ---

// TestRegexMatchInvalidConstantPatternFailsCompile is the regression test
// for a constant @regex_match pattern that regexp.Compile rejects: before
// the fix, cachedRegexp's compile error was swallowed at eval time
// (checkConstraintV returned false), so the rule compiled cleanly and then
// NEVER fired -- indistinguishable from "no matching data". A bad constant
// pattern is a rule-authoring mistake, not a data condition, and should fail
// loudly at Compile.
func TestRegexMatchInvalidConstantPatternFailsCompile(t *testing.T) {
	rs, err := syntax.ParseAll(`hit(X) :- msg(X), @regex_match(X, "[unclosed").`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = seminaive.New().Compile(rs)
	if err == nil {
		t.Fatal("expected Compile to reject the invalid constant regex pattern, got nil error")
	}
	t.Logf("got expected error: %v", err)
}

// TestRegexMatchValidConstantPatternStillCompilesAndFires is the companion
// sanity check: a valid constant pattern must still compile and the rule
// must still derive facts, so the new compile-time validation doesn't
// reject legitimate patterns.
func TestRegexMatchValidConstantPatternStillCompilesAndFires(t *testing.T) {
	b := memory.NewBuilder()
	b.AddFact(datalog.Fact{Name: "msg", Terms: []datalog.Constant{datalog.String("goodbye")}})
	input := b.Build()

	rs, err := syntax.ParseAll(`hit(X) :- msg(X), @regex_match(X, "^go.*bye$").`)
	if err != nil {
		t.Fatal(err)
	}
	tr, err := seminaive.New().Compile(rs)
	if err != nil {
		t.Fatalf("expected valid pattern to compile, got error: %v", err)
	}
	output, err := tr.Transform(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for range output.Query("hit", datalog.Variable("X")) {
		found = true
	}
	if !found {
		t.Fatal("expected hit(\"goodbye\") to be derived")
	}
}

// TestRegexMatchVariablePatternStillCompiles ensures the compile-time
// constant-pattern check doesn't reject (or attempt to validate) a
// @regex_match whose pattern argument is a variable bound at runtime --
// only a constant pattern is knowable at compile time.
func TestRegexMatchVariablePatternStillCompiles(t *testing.T) {
	b := memory.NewBuilder()
	b.AddFact(datalog.Fact{Name: "row", Terms: []datalog.Constant{datalog.String("hello"), datalog.String("^he")}})
	input := b.Build()

	rs, err := syntax.ParseAll(`hit(V) :- row(V, P), @regex_match(V, P).`)
	if err != nil {
		t.Fatal(err)
	}
	tr, err := seminaive.New().Compile(rs)
	if err != nil {
		t.Fatalf("expected variable-pattern rule to compile, got error: %v", err)
	}
	output, err := tr.Transform(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for range output.Query("hit", datalog.Variable("V")) {
		found = true
	}
	if !found {
		t.Fatal("expected hit(\"hello\") to be derived")
	}
}

// --- BUG #5: unknown/wrong-arity @-builtins must fail at Compile ---

// TestUnknownBuiltinFailsCompile is the regression test for a misspelled
// @-builtin (@json_ge instead of @json_get): before the fix,
// checkBodySafety/compileBody routed the unrecognized "@" atom through the
// default join case, so the rule compiled cleanly and derived zero facts --
// silently indistinguishable from "ev has no matching rows". The parser
// already reserves "@" for builtins (validateHeadAtom rejects an "@" head),
// so Compile can and must reject an unrecognized "@" body atom by name.
func TestUnknownBuiltinFailsCompile(t *testing.T) {
	rs, err := syntax.ParseAll(`out(V) :- ev(X), @json_ge(X, "a", V).`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = seminaive.New().Compile(rs)
	if err == nil {
		t.Fatal("expected Compile to reject the unknown builtin @json_ge, got nil error")
	}
	t.Logf("got expected error: %v", err)
}

// TestUnknownBuiltinFailsCompileInAggregateRule covers the same unknown-"@"
// shape inside an aggregate rule body, which goes through a separate
// checkAggRuleSafety/compile path from plain rules.
func TestUnknownBuiltinFailsCompileInAggregateRule(t *testing.T) {
	rs, err := syntax.ParseAll(`total(S) :- S = sum(V) : ev(X), @json_ge(X, "a", V).`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = seminaive.New().Compile(rs)
	if err == nil {
		t.Fatal("expected Compile to reject the unknown builtin @json_ge in an aggregate rule, got nil error")
	}
	t.Logf("got expected error: %v", err)
}

// TestConstraintBuiltinWrongArityFailsCompile is the regression test for a
// wrong-arity call to a known constraint builtin (@contains needs two
// arguments). Before the fix, checkConstraintV's `len(terms) < 2` guard
// just silently evaluated to false on every call, so the rule compiled
// cleanly and never fired -- the same silent-no-op failure mode as the
// unknown-builtin case, just for a real builtin called wrong.
func TestConstraintBuiltinWrongArityFailsCompile(t *testing.T) {
	rs, err := syntax.ParseAll(`out(X) :- ev(X), @contains(X).`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = seminaive.New().Compile(rs)
	if err == nil {
		t.Fatal("expected Compile to reject the wrong-arity @contains(X) call, got nil error")
	}
	t.Logf("got expected error: %v", err)
}

// TestJSONBuiltinWrongArityFailsCompile covers the same wrong-arity shape
// for one of the always-registered JSON destructuring builtins (@json_get
// needs three arguments: Obj/Arr, Key/Idx, V).
func TestJSONBuiltinWrongArityFailsCompile(t *testing.T) {
	rs, err := syntax.ParseAll(`out(V) :- ev(X), @json_get(X, V).`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = seminaive.New().Compile(rs)
	if err == nil {
		t.Fatal("expected Compile to reject the wrong-arity @json_get(X, V) call, got nil error")
	}
	t.Logf("got expected error: %v", err)
}

// TestJSONMultiBuiltinWrongArityFailsCompile covers the multi-output shape
// (@json_items needs three: Obj, K, V).
func TestJSONMultiBuiltinWrongArityFailsCompile(t *testing.T) {
	rs, err := syntax.ParseAll(`out(K) :- ev(X), @json_items(X, K).`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = seminaive.New().Compile(rs)
	if err == nil {
		t.Fatal("expected Compile to reject the wrong-arity @json_items(X, K) call, got nil error")
	}
	t.Logf("got expected error: %v", err)
}

// TestCorrectlySpelledCorrectArityBuiltinsStillCompileAndFire is the
// companion sanity check: correctly-spelled, correct-arity builtins must
// still compile and fire, so the new unknown/arity checks don't reject
// legitimate rules.
func TestCorrectlySpelledCorrectArityBuiltinsStillCompileAndFire(t *testing.T) {
	b := memory.NewBuilder()
	obj, err := datalog.NewComposite(map[string]any{"a": "x"})
	if err != nil {
		t.Fatal(err)
	}
	b.AddFact(datalog.Fact{Name: "ev", Terms: []datalog.Constant{obj}})
	input := b.Build()

	rs, err := syntax.ParseAll(`out(V) :- ev(X), @json_get(X, "a", V).`)
	if err != nil {
		t.Fatal(err)
	}
	tr, err := seminaive.New().Compile(rs)
	if err != nil {
		t.Fatalf("expected correctly-spelled correct-arity builtin to compile, got error: %v", err)
	}
	output, err := tr.Transform(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for row := range output.Query("out", datalog.Variable("V")) {
		if row[0] == datalog.String("x") {
			found = true
		}
	}
	if !found {
		t.Fatal("expected out(\"x\") to be derived")
	}
}

// --- BUG #6: TimeDiff must not float->int64-saturate on arm64 ---

// TestTimeDiffOutOfRangeStaysFloatPlatformIndependent is the regression test
// for TimeDiff's whole-seconds int64 conversion: before the fix,
// `diff == float64(int64(diff))` performed an out-of-range float64->int64
// conversion for a diff of exactly 2^63 (or beyond), which Go leaves
// implementation-defined -- arm64's FCVTZS saturates to MaxInt64, and
// float64(MaxInt64) rounds back up to exactly 2^63, so the naive round-trip
// check would accept diff == 2^63 as int64 MaxInt64 on arm64, while amd64's
// conversion yields a different result for the same input: a
// platform-dependent derived fact. The fixed TimeDiff routes through
// interned.NormalizeNumeric's guarded range check, so any diff >= 2^63 (or
// < -2^63) always stays float64, identically on every platform.
func TestTimeDiffOutOfRangeStaysFloatPlatformIndependent(t *testing.T) {
	const twoTo63 = 9223372036854775808.0 // 2^63, one past math.MaxInt64

	cases := []struct {
		name string
		a, b float64
	}{
		{"exactly-two-to-63", twoTo63, 0},
		{"just-out-of-range", twoTo63 * 2, 0},
		{"negative-out-of-range", -(twoTo63 * 2), 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			result, ok := seminaive.TimeDiff([]any{c.a, c.b})
			if !ok {
				t.Fatal("expected TimeDiff to succeed")
			}
			f, isFloat := result.(float64)
			if !isFloat {
				t.Fatalf("expected out-of-range diff to stay float64, got %T (%v) -- platform-dependent int64 saturation", result, result)
			}
			if f != c.a-c.b {
				t.Fatalf("expected diff %v, got %v", c.a-c.b, f)
			}
		})
	}
}

// TestTimeDiffInRangeWholeSecondsStillIntegerizes is the companion sanity
// check: an ordinary in-range whole-seconds diff must still come back as
// int64, so the fix doesn't regress the documented "int64 when both inputs
// are whole seconds" behavior.
func TestTimeDiffInRangeWholeSecondsStillIntegerizes(t *testing.T) {
	result, ok := seminaive.TimeDiff([]any{int64(100), int64(40)})
	if !ok {
		t.Fatal("expected TimeDiff to succeed")
	}
	i, isInt := result.(int64)
	if !isInt {
		t.Fatalf("expected in-range whole-second diff to be int64, got %T (%v)", result, result)
	}
	if i != 60 {
		t.Fatalf("expected diff 60, got %v", i)
	}
}

// TestTimeDiffMinInt64BoundaryIsInclusive pins the inclusive lower bound:
// a diff of exactly -2^63 (math.MinInt64) is itself a valid, exactly
// representable int64 and must still integerize, matching
// interned.NormalizeNumeric's documented inclusive lower bound.
func TestTimeDiffMinInt64BoundaryIsInclusive(t *testing.T) {
	const minInt64AsFloat = -9223372036854775808.0
	result, ok := seminaive.TimeDiff([]any{minInt64AsFloat, 0.0})
	if !ok {
		t.Fatal("expected TimeDiff to succeed")
	}
	i, isInt := result.(int64)
	if !isInt {
		t.Fatalf("expected -2^63 diff to integerize to math.MinInt64, got %T (%v)", result, result)
	}
	if i != math.MinInt64 {
		t.Fatalf("expected diff math.MinInt64, got %v", i)
	}
}

// TestTimeDiffFractionalStillFloat is a sanity check that a fractional diff
// (not a whole number of seconds) still comes back as float64.
func TestTimeDiffFractionalStillFloat(t *testing.T) {
	result, ok := seminaive.TimeDiff([]any{100.5, 40.0})
	if !ok {
		t.Fatal("expected TimeDiff to succeed")
	}
	f, isFloat := result.(float64)
	if !isFloat {
		t.Fatalf("expected fractional diff to be float64, got %T (%v)", result, result)
	}
	if f != 60.5 {
		t.Fatalf("expected diff 60.5, got %v", f)
	}
}

// --- BUG #7: MinInt64 / -1 and -1 * MinInt64 must raise overflow, not wrap ---

// TestDivisionMinInt64ByNegOneOverflows is the regression test for the
// integer division path: Go defines MinInt64 / -1 == MinInt64 (the true
// mathematical result, +2^63, is not representable as int64, so Go silently
// wraps), and applyBinOp's "/" case only checked for a zero divisor before
// the fix, so this case reached ordinary Go division and silently wrapped
// instead of raising arithmeticOverflowError like every other int64
// overflow the engine documents.
func TestDivisionMinInt64ByNegOneOverflows(t *testing.T) {
	b := memory.NewBuilder()
	b.AddFact(datalog.Fact{Name: "v", Terms: []datalog.Constant{datalog.Integer(math.MinInt64)}})
	rs, err := syntax.ParseAll(`r(R) :- v(N), R is N / -1.`)
	if err != nil {
		t.Fatal(err)
	}
	tr, err := seminaive.New().Compile(rs)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tr.Transform(context.Background(), b.Build())
	if err == nil {
		t.Fatal("expected overflow error for MinInt64 / -1, got nil")
	}
	t.Logf("got expected error: %v", err)
}

// TestMultiplicationNegOneTimesMinInt64Overflows is the regression test for
// mulInt64Checked's round-trip overflow check missing the a=-1, b=MinInt64
// order specifically: prod = -1 * MinInt64 wraps to MinInt64 (since +2^63
// isn't representable), and prod/a == MinInt64/-1 == MinInt64 == b still
// holds even though prod is wrong -- so the round-trip check alone passed
// this case through as "no overflow" before the fix, silently returning the
// wrapped (and outright wrong) value MinInt64 instead of the true magnitude
// +2^63.
func TestMultiplicationNegOneTimesMinInt64Overflows(t *testing.T) {
	b := memory.NewBuilder()
	b.AddFact(datalog.Fact{Name: "v", Terms: []datalog.Constant{datalog.Integer(math.MinInt64)}})
	rs, err := syntax.ParseAll(`r(R) :- v(N), R is -1 * N.`)
	if err != nil {
		t.Fatal(err)
	}
	tr, err := seminaive.New().Compile(rs)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tr.Transform(context.Background(), b.Build())
	if err == nil {
		t.Fatal("expected overflow error for -1 * MinInt64, got nil")
	}
	t.Logf("got expected error: %v", err)
}

// TestOrdinaryDivisionAndMultiplicationStillWork is the companion sanity
// check: the MinInt64 guards must not disturb ordinary division/
// multiplication results.
func TestOrdinaryDivisionAndMultiplicationStillWork(t *testing.T) {
	b := memory.NewBuilder()
	b.AddFact(datalog.Fact{Name: "v", Terms: []datalog.Constant{datalog.Integer(100)}})
	rs, err := syntax.ParseAll(`
		d(R) :- v(N), R is N / 4.
		m(R) :- v(N), R is N * 3.
	`)
	if err != nil {
		t.Fatal(err)
	}
	tr, err := seminaive.New().Compile(rs)
	if err != nil {
		t.Fatal(err)
	}
	output, err := tr.Transform(context.Background(), b.Build())
	if err != nil {
		t.Fatal(err)
	}
	for row := range output.Query("d", datalog.Variable("R")) {
		if row[0] != datalog.Integer(25) {
			t.Fatalf("expected d = 25, got %v", row[0])
		}
	}
	for row := range output.Query("m", datalog.Variable("R")) {
		if row[0] != datalog.Integer(300) {
			t.Fatalf("expected m = 300, got %v", row[0])
		}
	}
	// MinInt64 / -1 must still be reachable via errors.Is(err, ...) semantics
	// used elsewhere in the package -- a light double-check that Transform's
	// returned error is the documented arithmeticOverflowError family, not
	// some unrelated failure.
	b2 := memory.NewBuilder()
	b2.AddFact(datalog.Fact{Name: "v", Terms: []datalog.Constant{datalog.Integer(math.MinInt64)}})
	rs2, err := syntax.ParseAll(`r(R) :- v(N), R is N / -1.`)
	if err != nil {
		t.Fatal(err)
	}
	tr2, err := seminaive.New().Compile(rs2)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tr2.Transform(context.Background(), b2.Build())
	if err == nil {
		t.Fatal("expected overflow error")
	}
	if errors.Is(err, context.Canceled) {
		t.Fatal("overflow error must not look like context cancellation")
	}
}

// TestAtSignExternalNotRejectedAsUnknownBuiltin covers the completeness gap in
// the unknown-builtin check (#5): checkBodyBuiltins classifies "@"-prefixed
// body atoms, and externals are a fourth registry alongside
// constraint/bind/multi-bind builtins. WithExternal imposes no name
// restriction, so an "@"-prefixed external is a valid registration; without
// the externals case it would be wrongly rejected as "unknown builtin". This
// asserts such a program compiles and fires (arity is still enforced by
// checkBodySafety).
func TestAtSignExternalNotRejectedAsUnknownBuiltin(t *testing.T) {
	lookup := func(ctx context.Context, b seminaive.Bindings) iter.Seq[[]any] {
		return func(yield func([]any) bool) {
			yield([]any{"enriched"})
		}
	}
	rs, err := syntax.ParseAll(`out(X, V) :- ev(X), @enrich(V).`)
	if err != nil {
		t.Fatal(err)
	}
	eng := seminaive.New(seminaive.WithExternal("@enrich", 1, lookup))
	tr, err := eng.Compile(rs)
	if err != nil {
		t.Fatalf("an @-prefixed registered external must not be rejected as an unknown builtin: %v", err)
	}

	b := memory.NewBuilder()
	b.AddFact(datalog.Fact{Name: "ev", Terms: []datalog.Constant{datalog.String("e1")}})
	out, err := tr.Transform(context.Background(), b.Build())
	if err != nil {
		t.Fatal(err)
	}
	got := 0
	for range out.Facts("out", 2) {
		got++
	}
	if got != 1 {
		t.Errorf("expected 1 derived out fact from the @-external, got %d", got)
	}
}
