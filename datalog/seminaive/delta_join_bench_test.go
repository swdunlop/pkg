package seminaive_test

import (
	"context"
	"fmt"
	"testing"

	"swdunlop.dev/pkg/datalog"
	"swdunlop.dev/pkg/datalog/memory"
	"swdunlop.dev/pkg/datalog/seminaive"
	"swdunlop.dev/pkg/datalog/syntax"
)

// setupChain builds an input database with n-1 edges forming a chain
// 0->1->2->...->n-1 and compiles a transitive-closure transformer using the
// given rule text (so callers can probe different join orderings of the
// recursive atom vs. the base atom).
func setupChain(b *testing.B, n int, rules string) (datalog.Database, datalog.Transformer) {
	b.Helper()
	builder := memory.NewBuilder()
	for i := range n - 1 {
		builder.AddFact(datalog.Fact{
			Name:  "edge",
			Terms: []datalog.Constant{datalog.String(fmt.Sprintf("n%d", i)), datalog.String(fmt.Sprintf("n%d", i+1))},
		})
	}
	input := builder.Build()

	rs, err := syntax.ParseAll(rules)
	if err != nil {
		b.Fatal(err)
	}
	tr, err := seminaive.New().Compile(rs)
	if err != nil {
		b.Fatal(err)
	}
	return input, tr
}

// runChainBenchmark evaluates the chain transformer and checks the expected
// transitive-closure count (n*(n-1)/2 pairs for an n-node chain).
func runChainBenchmark(b *testing.B, n int, rules string) {
	b.Helper()
	input, tr := setupChain(b, n, rules)
	expected := n * (n - 1) / 2

	b.ResetTimer()
	for b.Loop() {
		output, err := tr.Transform(context.Background(), input)
		if err != nil {
			b.Fatal(err)
		}
		count := 0
		for range output.Facts("reachable", 2) {
			count++
		}
		if count != expected {
			b.Fatalf("expected %d results, got %d", expected, count)
		}
	}
}

// edgeFirstRules puts the base (non-recursive) atom before the recursive
// atom in the rule body -- the shape most exposed to a delta-blind join
// order, since the base atom has no delta after iteration 0 and, if
// evaluated outermost against the full "existing" relation, is rescanned
// in full on every semi-naive round regardless of how small that round's
// delta is.
const edgeFirstRules = `
	reachable(X, Y) :- edge(X, Y).
	reachable(X, Y) :- edge(X, Z), reachable(Z, Y).
`

// recursiveFirstRules puts the recursive atom first; reorderBody's
// tie-breaking already happens to place it outermost at compile time, so
// this shape is expected to already perform reasonably even before any fix.
const recursiveFirstRules = `
	reachable(X, Y) :- edge(X, Y).
	reachable(X, Y) :- reachable(X, Z), edge(Z, Y).
`

func BenchmarkDeltaJoinEdgeFirst400(b *testing.B)  { runChainBenchmark(b, 400, edgeFirstRules) }
func BenchmarkDeltaJoinEdgeFirst800(b *testing.B)  { runChainBenchmark(b, 800, edgeFirstRules) }
func BenchmarkDeltaJoinEdgeFirst1600(b *testing.B) { runChainBenchmark(b, 1600, edgeFirstRules) }

func BenchmarkDeltaJoinRecursiveFirst400(b *testing.B)  { runChainBenchmark(b, 400, recursiveFirstRules) }
func BenchmarkDeltaJoinRecursiveFirst800(b *testing.B)  { runChainBenchmark(b, 800, recursiveFirstRules) }
func BenchmarkDeltaJoinRecursiveFirst1600(b *testing.B) { runChainBenchmark(b, 1600, recursiveFirstRules) }
