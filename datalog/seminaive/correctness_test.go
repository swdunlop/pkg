package seminaive_test

import (
	"context"
	"errors"
	"fmt"
	"iter"
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
