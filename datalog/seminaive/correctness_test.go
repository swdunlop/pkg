package seminaive_test

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"slices"
	"sort"
	"strings"
	"testing"

	"swdunlop.dev/pkg/datalog"
	"swdunlop.dev/pkg/datalog/memory"
	"swdunlop.dev/pkg/datalog/seminaive"
	"swdunlop.dev/pkg/datalog/syntax"
)

// TestIterationLimitError verifies that hitting the fixpoint iteration limit
// returns an error instead of silently truncating results.
func TestIterationLimitError(t *testing.T) {
	b := memory.NewBuilder()
	for i := range 10 {
		b.AddFact(datalog.Fact{Name: "edge", Terms: []datalog.Constant{
			datalog.Integer(i), datalog.Integer(i + 1),
		}})
	}
	input := b.Build()

	const rules = `
		path(X, Y) :- edge(X, Y).
		path(X, Z) :- edge(X, Y), path(Y, Z).
	`

	tr, err := syntax.Parse(seminaive.New(seminaive.WithMaxIterations(3)), rules)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, err = tr.Transform(context.Background(), input)
	if err == nil {
		t.Fatal("expected an error when the iteration limit is hit, got nil")
	}
	if !strings.Contains(err.Error(), "fixpoint not reached") {
		t.Fatalf("expected a fixpoint-not-reached error, got: %v", err)
	}

	// The same program converges under the default limit.
	tr, err = syntax.Parse(seminaive.New(), rules)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if _, err := tr.Transform(context.Background(), input); err != nil {
		t.Fatalf("expected convergence under the default limit, got: %v", err)
	}
}

// TestContextCancelMidFixpoint verifies that cancelling the context during
// fixpoint evaluation stops the loop, not just the next stratum.
func TestContextCancelMidFixpoint(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// @bump increments its input and cancels the context on first use, so
	// the recursion would run many more iterations if cancellation were
	// only checked between strata.
	bump := func(inputs []any) (any, bool) {
		cancel()
		v, ok := inputs[0].(int64)
		if !ok {
			return nil, false
		}
		return v + 1, true
	}

	tr, err := syntax.Parse(seminaive.New(seminaive.WithBuiltin("@bump", bump)), `
		counter(0).
		counter(Y) :- counter(X), X < 1000, @bump(X, Y).
	`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, err = tr.Transform(ctx, datalog.Empty{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got: %v", err)
	}
}

// TestExternalArityLimit verifies that external predicates wider than the
// engine's maximum fact arity are rejected at compile time.
func TestExternalArityLimit(t *testing.T) {
	ext := func(ctx context.Context, b seminaive.Bindings) iter.Seq[[]any] {
		return func(yield func([]any) bool) {}
	}
	engine := seminaive.New(seminaive.WithExternal("wide", 17, ext))
	_, err := syntax.Parse(engine, `p("x").`)
	if err == nil {
		t.Fatal("expected a compile error for external arity 17, got nil")
	}
	if !strings.Contains(err.Error(), "arity 17 out of range") {
		t.Fatalf("expected an arity-out-of-range error, got: %v", err)
	}
}

// TestExternalTupleArityMismatch verifies that an external function yielding
// tuples of the wrong width causes an error instead of corrupted facts.
func TestExternalTupleArityMismatch(t *testing.T) {
	ext := func(ctx context.Context, b seminaive.Bindings) iter.Seq[[]any] {
		return func(yield func([]any) bool) {
			yield([]any{"a", "b", "c"}) // declared arity is 2
		}
	}
	engine := seminaive.New(seminaive.WithExternal("ext", 2, ext))
	tr, err := syntax.Parse(engine, `q(X, Y) :- ext(X, Y).`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, err = tr.Transform(context.Background(), datalog.Empty{})
	if err == nil {
		t.Fatal("expected an error for a mismatched external tuple, got nil")
	}
	if !strings.Contains(err.Error(), "expected tuples of arity 2, got 3") {
		t.Fatalf("expected a tuple-arity error, got: %v", err)
	}
}

// TestUnstratifiableFailsAtCompile verifies that a negation cycle is rejected
// by Compile, not deferred to the first Transform.
func TestUnstratifiableFailsAtCompile(t *testing.T) {
	rs, err := syntax.ParseAll(`
		p(X) :- q(X), not r(X).
		r(X) :- q(X), not p(X).
	`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, err = seminaive.New().Compile(rs)
	if err == nil {
		t.Fatal("expected a stratification error at compile time, got nil")
	}
	if !strings.Contains(err.Error(), "unstratifiable") {
		t.Fatalf("expected an unstratifiable error, got: %v", err)
	}
}

// TestAggregateCycleFailsAtCompile verifies that a predicate cycle through a
// non-monotonic aggregate is rejected by Compile instead of silently
// producing a non-fixpoint result. Evaluation runs plain rules to fixpoint
// and then evalAggregates exactly once per stratum, so d(X) :- e(X) and
// e(N) :- N = count : d(?) can never converge: d(1) (derived because e(1)
// holds) is never fed back into the aggregate that produced e(1).
func TestAggregateCycleFailsAtCompile(t *testing.T) {
	rs, err := syntax.ParseAll(`
		d(10).
		d(X) :- e(X).
		e(N) :- N = count : d(?).
	`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, err = seminaive.New().Compile(rs)
	if err == nil {
		t.Fatal("expected a stratification error at compile time, got nil")
	}
	if !strings.Contains(err.Error(), "unstratifiable") {
		t.Fatalf("expected an unstratifiable error, got: %v", err)
	}
	if !strings.Contains(err.Error(), "aggregate") {
		t.Fatalf("expected the error to mention the aggregate, got: %v", err)
	}
	if !strings.Contains(err.Error(), "d") || !strings.Contains(err.Error(), "e") {
		t.Fatalf("expected the error to name the cycle's predicates (d, e), got: %v", err)
	}
}

// TestMutualAggregateCycleFailsAtCompile verifies that two aggregate rules
// whose heads feed each other's bodies are also rejected, not just a
// single self-referential aggregate.
func TestMutualAggregateCycleFailsAtCompile(t *testing.T) {
	rs, err := syntax.ParseAll(`
		p(0).
		q(0).
		p(N) :- N = count : q(?).
		q(N) :- N = count : p(?).
	`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, err = seminaive.New().Compile(rs)
	if err == nil {
		t.Fatal("expected a stratification error at compile time, got nil")
	}
	if !strings.Contains(err.Error(), "unstratifiable") {
		t.Fatalf("expected an unstratifiable error, got: %v", err)
	}
	if !strings.Contains(err.Error(), "aggregate") {
		t.Fatalf("expected the error to mention the aggregate, got: %v", err)
	}
}

// TestAggregateOverLowerRecursivePredicateCompiles verifies that a legitimate
// layered program -- a recursive predicate computed to fixpoint, then
// counted by an aggregate, then consumed by a plain rule -- still compiles
// and evaluates correctly. This is not a cycle: reachable's SCC never
// depends on out_degree or busy, so it stratifies cleanly beneath them.
func TestAggregateOverLowerRecursivePredicateCompiles(t *testing.T) {
	output := transformFacts(t, seminaive.New(), `
		reachable(X, Y) :- edge(X, Y).
		reachable(X, Z) :- edge(X, Y), reachable(Y, Z).
		out_degree(X, N) :- N = count : reachable(X, ?).
		busy(X) :- out_degree(X, N), N > 0.
	`,
		datalog.Fact{Name: "edge", Terms: []datalog.Constant{datalog.String("a"), datalog.String("b")}},
		datalog.Fact{Name: "edge", Terms: []datalog.Constant{datalog.String("b"), datalog.String("c")}},
	)

	got := map[string]int64{}
	for row := range output.Facts("out_degree", 2) {
		got[row[0].String()] = int64(row[1].(datalog.Integer))
	}
	want := map[string]int64{`"a"`: 2, `"b"`: 1}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("out_degree(%s) = %d, want %d (all: %v)", k, got[k], v, got)
		}
	}

	var busy []string
	for row := range output.Facts("busy", 1) {
		busy = append(busy, row[0].String())
	}
	sort.Strings(busy)
	if want := []string{`"a"`, `"b"`}; !slices.Equal(busy, want) {
		t.Errorf("busy = %v, want %v", busy, want)
	}
}

// TestIterationLimitErrorNamesStratum verifies that the iteration-limit error
// identifies the stratum that failed to converge.
func TestIterationLimitErrorNamesStratum(t *testing.T) {
	b := memory.NewBuilder()
	for i := range 10 {
		b.AddFact(datalog.Fact{Name: "edge", Terms: []datalog.Constant{
			datalog.Integer(i), datalog.Integer(i + 1),
		}})
	}

	tr, err := syntax.Parse(seminaive.New(seminaive.WithMaxIterations(3)), `
		path(X, Y) :- edge(X, Y).
		path(X, Z) :- edge(X, Y), path(Y, Z).
	`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, err = tr.Transform(context.Background(), b.Build())
	if err == nil || !strings.Contains(err.Error(), "stratum [") || !strings.Contains(err.Error(), "path") {
		t.Fatalf("expected the error to name the stratum's predicates, got: %v", err)
	}
}

// sliceDB is a minimal generic Database (not a memory.Database) used to
// exercise the loadFromGeneric path.
type sliceDB struct {
	facts map[string][][]datalog.Constant // keyed by predicate name
}

func (db sliceDB) Predicates() iter.Seq2[string, int] {
	return func(yield func(string, int) bool) {
		for pred, rows := range db.facts {
			if len(rows) > 0 && !yield(pred, len(rows[0])) {
				return
			}
		}
	}
}

func (db sliceDB) Declarations() iter.Seq[datalog.Declaration] {
	return func(yield func(datalog.Declaration) bool) {}
}

func (db sliceDB) Facts(pred string, arity int) iter.Seq[[]datalog.Constant] {
	return func(yield func([]datalog.Constant) bool) {
		for _, row := range db.facts[pred] {
			if len(row) == arity && !yield(row) {
				return
			}
		}
	}
}

func (db sliceDB) Query(pred string, terms ...datalog.Term) iter.Seq[[]datalog.Constant] {
	return func(yield func([]datalog.Constant) bool) {
		for _, row := range db.facts[pred] {
			if len(row) != len(terms) {
				continue
			}
			match := true
			for i, t := range terms {
				if c, ok := t.(datalog.Constant); ok && fmt.Sprint(c) != fmt.Sprint(row[i]) {
					match = false
					break
				}
			}
			if match && !yield(row) {
				return
			}
		}
	}
}

// TestGenericLoadKeepsUnreferencedPredicates verifies that Transform carries
// facts from a generic (non-memory) database into the output even when no
// rule or declaration references their predicate, matching the memory fast
// path.
func TestGenericLoadKeepsUnreferencedPredicates(t *testing.T) {
	input := sliceDB{facts: map[string][][]datalog.Constant{
		"used":   {{datalog.String("a"), datalog.String("b")}},
		"unused": {{datalog.String("keep"), datalog.Integer(1)}},
	}}

	tr, err := syntax.Parse(seminaive.New(), `derived(X, Y) :- used(X, Y).`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	output, err := tr.Transform(context.Background(), input)
	if err != nil {
		t.Fatalf("transform: %v", err)
	}

	var unused, derived int
	for range output.Facts("unused", 2) {
		unused++
	}
	for range output.Facts("derived", 2) {
		derived++
	}
	if unused != 1 {
		t.Errorf("expected 1 unused/2 fact in the output, got %d", unused)
	}
	if derived != 1 {
		t.Errorf("expected 1 derived/2 fact in the output, got %d", derived)
	}
}

// TestJoinFreeRuleDerivesFact verifies that a rule whose body has no
// positive join atom -- only an is-atom here -- still derives its head fact,
// instead of being silently skipped (the old joinCount == 0 guard in
// evalRules dropped such rules even though checkRuleSafety accepts them).
func TestJoinFreeRuleDerivesFact(t *testing.T) {
	output := transformFacts(t, seminaive.New(), `answer(Y) :- Y is 6*7.`)

	var got []int64
	for row := range output.Facts("answer", 1) {
		got = append(got, int64(row[0].(datalog.Integer)))
	}
	if want := []int64{42}; !slices.Equal(got, want) {
		t.Errorf("answer = %v, want %v", got, want)
	}
}

// TestJoinFreeNegationOnlyRuleDerivesFact verifies that a rule whose body is
// only a negated atom (no positive join atom at all) derives its head fact
// when the negated predicate is absent, and does not when it is present.
// Negation is stratified, so ok/0 is fully decided before alarm/0's stratum
// runs; evaluating the rule once on iteration 0 is sound and complete.
func TestJoinFreeNegationOnlyRuleDerivesFact(t *testing.T) {
	t.Run("absent", func(t *testing.T) {
		output := transformFacts(t, seminaive.New(), `alarm() :- not ok().`)
		count := 0
		for range output.Facts("alarm", 0) {
			count++
		}
		if count != 1 {
			t.Errorf("expected alarm() to be derived when ok() is absent, got %d facts", count)
		}
	})

	t.Run("present", func(t *testing.T) {
		output := transformFacts(t, seminaive.New(), `
			ok().
			alarm() :- not ok().
		`)
		count := 0
		for range output.Facts("alarm", 0) {
			count++
		}
		if count != 0 {
			t.Errorf("expected alarm() not to be derived when ok() is present, got %d facts", count)
		}
	})
}

// TestJoinFreeRuleFeedsDownstreamRule verifies that a join-free rule's
// output is visible to a downstream rule in the same stratum, i.e. it feeds
// the delta on the iteration after it runs rather than only landing in the
// final result set too late for other rules to join against it.
func TestJoinFreeRuleFeedsDownstreamRule(t *testing.T) {
	output := transformFacts(t, seminaive.New(), `
		base(Y) :- Y is 6*7.
		doubled(Z) :- base(Y), Z is Y*2.
	`)

	var got []int64
	for row := range output.Facts("doubled", 1) {
		got = append(got, int64(row[0].(datalog.Integer)))
	}
	if want := []int64{84}; !slices.Equal(got, want) {
		t.Errorf("doubled = %v, want %v", got, want)
	}
}

// TestAggregateNegationOrderIndependent verifies that negation in an
// aggregate rule body is checked against the fully-known fact set rather
// than at its lexical position. Previously queryInternedFacts treated
// negated atoms as inline bodyItemJoin steps evaluated in body order, so
// `not excluded(N)` written before `employee(N, S)` saw N unbound and
// matchesAnyV's "no bound args" fallback matched any excluded fact,
// pruning every binding and producing zero groups. Swapping the literals
// masked the bug by accident, since evaluation happened to reach the
// negation only after N was already bound. Both orderings must now agree.
func TestAggregateNegationOrderIndependent(t *testing.T) {
	facts := []datalog.Fact{
		{Name: "employee", Terms: []datalog.Constant{datalog.String("alice"), datalog.Integer(10)}},
		{Name: "employee", Terms: []datalog.Constant{datalog.String("carol"), datalog.Integer(20)}},
		{Name: "employee", Terms: []datalog.Constant{datalog.String("bob"), datalog.Integer(999)}},
		{Name: "excluded", Terms: []datalog.Constant{datalog.String("bob")}},
	}

	sumOf := func(t *testing.T, output datalog.Database) int64 {
		t.Helper()
		var total int64
		found := false
		for row := range output.Facts("total", 1) {
			total = int64(row[0].(datalog.Integer))
			found = true
		}
		if !found {
			t.Fatal("expected a total() fact, got none")
		}
		return total
	}

	t.Run("negation first", func(t *testing.T) {
		output := transformFacts(t, seminaive.New(), `
			total(S) :- S = sum(V) : not excluded(N), employee(N, V).
		`, facts...)
		if got := sumOf(t, output); got != 30 {
			t.Errorf("total = %d, want 30", got)
		}
	})

	t.Run("negation last", func(t *testing.T) {
		output := transformFacts(t, seminaive.New(), `
			total(S) :- S = sum(V) : employee(N, V), not excluded(N).
		`, facts...)
		if got := sumOf(t, output); got != 30 {
			t.Errorf("total = %d, want 30", got)
		}
	})
}

// TestAggregateConstraintBeforeJoinReorders verifies that an aggregate
// rule body written with a constraint before the join atom that binds its
// variable is reordered (via reorderBody) before evaluation, exactly like a
// plain rule body. Previously aggregate bodies bypassed reorderBody
// entirely, so a leading `X > 2` against an unbound X evaluated to false
// unconditionally and killed every binding.
func TestAggregateConstraintBeforeJoinReorders(t *testing.T) {
	facts := []datalog.Fact{
		{Name: "p", Terms: []datalog.Constant{datalog.Integer(1)}},
		{Name: "p", Terms: []datalog.Constant{datalog.Integer(3)}},
		{Name: "p", Terms: []datalog.Constant{datalog.Integer(5)}},
	}

	output := transformFacts(t, seminaive.New(), `
		c(N) :- N = count : X > 2, p(X).
	`, facts...)

	var got int64
	found := false
	for row := range output.Facts("c", 1) {
		got = int64(row[0].(datalog.Integer))
		found = true
	}
	if !found {
		t.Fatal("expected a c() fact, got none")
	}
	if got != 2 {
		t.Errorf("c = %d, want 2 (only 3 and 5 are > 2)", got)
	}
}

// TestAggregateUnboundGroupByVarFailsAtCompile verifies that an aggregate
// rule whose head has a group-by variable never bound by the body is
// rejected at Compile, matching plain-rule safety checking, instead of
// silently computing the group and then dropping it when GroundCompiled
// fails to ground the head.
func TestAggregateUnboundGroupByVarFailsAtCompile(t *testing.T) {
	rs, err := syntax.ParseAll(`
		total(Unbound, S) :- S = sum(V) : employee(N, V).
	`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, err = seminaive.New().Compile(rs)
	if err == nil {
		t.Fatal("expected a safety error for the unbound group-by variable, got nil")
	}
	if !strings.Contains(err.Error(), "Unbound") {
		t.Fatalf("expected the error to name the unbound variable, got: %v", err)
	}
}

func TestAggregateRuleVarLimitFailsAtCompile(t *testing.T) {
	rs, err := syntax.ParseAll(`
		big(A, T) :- T = count : p(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P), q(Q).
	`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, err = seminaive.New().Compile(rs)
	if err == nil {
		t.Fatal("expected a variable-limit error for an aggregate rule with 18 variables, got nil")
	}
	if !strings.Contains(err.Error(), "16") {
		t.Fatalf("expected the error to mention the 16-variable limit, got: %v", err)
	}
}
