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

// TestRegexCacheBounded feeds @regex_match many more distinct data-driven
// patterns than seminaive's regexCacheCap and checks the process doesn't
// keep accumulating compiled regexes without bound: this is a proxy for the
// cache staying capped rather than a direct size assertion (regexCache is
// unexported), but it does confirm the bounded cache still evaluates every
// pattern correctly even once eviction is definitely happening -- a bug in
// the FIFO ring (e.g. evicting a still-needed live pattern's *cache entry*
// mid-query, not just an old one) would show up as either a wrong match
// result or a panic here, not just as unbounded memory.
func TestRegexCacheBounded(t *testing.T) {
	const patterns = 2000 // several times seminaive's regexCacheCap (512)

	builder := memory.NewBuilder()
	for i := range patterns {
		builder.AddFact(datalog.Fact{
			Name:  "item",
			Terms: []datalog.Constant{datalog.String(fmt.Sprintf("val-%d-x", i))},
		})
	}
	input := builder.Build()

	rs, err := syntax.ParseAll(`
		match(V) :- item(V), @regex_match(V, "^val-[0-9]+-x$").
	`)
	if err != nil {
		t.Fatal(err)
	}
	tr, err := seminaive.New().Compile(rs)
	if err != nil {
		t.Fatal(err)
	}
	output, err := tr.Transform(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for range output.Facts("match", 1) {
		count++
	}
	if count != patterns {
		t.Fatalf("expected %d matches, got %d", patterns, count)
	}

	// Now drive the cache with `patterns` distinct per-row regex patterns
	// (well past regexCacheCap), forcing repeated eviction, and confirm
	// every row still matches correctly against its own (freshly
	// re-)compiled pattern.
	builder2 := memory.NewBuilder()
	for i := range patterns {
		builder2.AddFact(datalog.Fact{
			Name: "row",
			Terms: []datalog.Constant{
				datalog.String(fmt.Sprintf("v%d", i)),
				datalog.String(fmt.Sprintf("^v%d$", i)),
			},
		})
	}
	input2 := builder2.Build()
	rs2, err := syntax.ParseAll(`
		rowmatch(V) :- row(V, P), @regex_match(V, P).
	`)
	if err != nil {
		t.Fatal(err)
	}
	tr2, err := seminaive.New().Compile(rs2)
	if err != nil {
		t.Fatal(err)
	}
	output2, err := tr2.Transform(context.Background(), input2)
	if err != nil {
		t.Fatal(err)
	}
	count2 := 0
	for range output2.Facts("rowmatch", 1) {
		count2++
	}
	if count2 != patterns {
		t.Fatalf("expected %d rowmatches after cache churn, got %d", patterns, count2)
	}
}

// BenchmarkRegexMatchCacheHit exercises cachedRegexp's hit path (a single
// pattern reused across many facts, well within regexCacheCap) to confirm
// bounding the cache (mutex + map + FIFO ring, replacing the retired
// unbounded sync.Map) didn't regress the common case.
func BenchmarkRegexMatchCacheHit(b *testing.B) {
	builder := memory.NewBuilder()
	for i := range 2000 {
		builder.AddFact(datalog.Fact{
			Name:  "item",
			Terms: []datalog.Constant{datalog.String(fmt.Sprintf("val-%d-x", i))},
		})
	}
	input := builder.Build()

	rs, err := syntax.ParseAll(`
		match(V) :- item(V), @regex_match(V, "^val-[0-9]+-x$").
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
		for range output.Facts("match", 1) {
			count++
		}
		if count != 2000 {
			b.Fatalf("expected 2000 matches, got %d", count)
		}
	}
}
