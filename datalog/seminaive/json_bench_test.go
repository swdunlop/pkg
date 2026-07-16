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

// BenchmarkJSONEachExtraction repeatedly destructures a moderately nested
// JSON array via @json_each/@json_get in the fixpoint's inner loop: each
// yielded element is itself an object with a nested array, so jsonValue's
// map[string]any/[]any case (and jsonSlice's) runs many times per fact,
// exercising the re-normalize-on-every-extraction path that
// datalog.NewCompositeTrusted (used from seminaive/json.go's jsonValue and
// jsonSlice) is meant to avoid paying for.
func BenchmarkJSONEachExtraction(b *testing.B) {
	builder := memory.NewBuilder()
	for i := range 200 {
		arr := make([]any, 20)
		for j := range arr {
			arr[j] = map[string]any{
				"id":   int64(j),
				"tags": []any{"a", "b", "c", "d", "e"},
			}
		}
		c, err := datalog.NewComposite(arr)
		if err != nil {
			b.Fatal(err)
		}
		builder.AddFact(datalog.Fact{
			Name:  "doc",
			Terms: []datalog.Constant{datalog.String(fmt.Sprintf("d%d", i)), c},
		})
	}
	input := builder.Build()

	rs, err := syntax.ParseAll(`
		item(D, Elem) :- doc(D, Arr), @json_each(Arr, Elem).
		itemid(D, Id) :- item(D, Elem), @json_get(Elem, "id", Id).
		tag(D, T) :- item(D, Elem), @json_get(Elem, "tags", Tags), @json_each(Tags, T).
	`)
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
		for range output.Facts("tag", 2) {
			count++
		}
		// tag(D, T) is a set keyed by (D, T): every item in a doc shares the
		// same 5-element tags list, so distinct items collapse to the same 5
		// (D, T) facts per doc -- the point of this benchmark is the
		// per-item @json_get/@json_each extraction work done to reach that
		// fixpoint, not the final fact count.
		if count != 200*5 {
			b.Fatalf("expected %d tags, got %d", 200*5, count)
		}
	}
}
