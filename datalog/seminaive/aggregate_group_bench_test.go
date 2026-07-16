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

// BenchmarkAggregateGroupingLarge exercises evalAggregates' grouping path at
// a scale where the per-body-solution InternedSub materialize-and-Get cost
// (aggregate.go's original name-keyed grouping) dominates: many rows funnel
// into relatively few groups, via a two-variable group-by plus a summed
// value, so both the grouping comparisons and the per-row aggregate-value
// resolution run once per body solution.
func BenchmarkAggregateGroupingLarge(b *testing.B) {
	const rows = 20000
	const groups = 50
	builder := memory.NewBuilder()
	for i := range rows {
		builder.AddFact(datalog.Fact{
			Name: "score",
			Terms: []datalog.Constant{
				datalog.String(fmt.Sprintf("dept%d", i%groups)),
				datalog.String(fmt.Sprintf("team%d", i%7)),
				datalog.Integer(int64(i % 1000)),
			},
		})
	}
	input := builder.Build()

	rs, err := syntax.ParseAll(`dept_total(Dept, Team, S) :- S = sum(V) : score(Dept, Team, V).`)
	if err != nil {
		b.Fatal(err)
	}
	tr, err := seminaive.New().Compile(rs)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for b.Loop() {
		output, err := tr.Transform(context.Background(), input)
		if err != nil {
			b.Fatal(err)
		}
		count := 0
		for range output.Facts("dept_total", 3) {
			count++
		}
		if count != groups*7 {
			b.Fatalf("expected %d groups, got %d", groups*7, count)
		}
	}
}
