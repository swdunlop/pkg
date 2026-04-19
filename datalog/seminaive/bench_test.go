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

// setupTransitiveClosure creates an input database with n edges forming a chain
// 0->1->2->...->n-1 and returns a compiled transformer for transitive closure.
func setupTransitiveClosure(b *testing.B, n int) (datalog.Database, datalog.Transformer) {
	b.Helper()
	builder := memory.NewBuilder()
	for i := range n - 1 {
		builder.AddFact(datalog.Fact{
			Name:  "edge",
			Terms: []datalog.Constant{datalog.String(fmt.Sprintf("n%d", i)), datalog.String(fmt.Sprintf("n%d", i+1))},
		})
	}
	input := builder.Build()

	rs, err := syntax.ParseAll(`
		reachable(X, Y) :- edge(X, Y).
		reachable(X, Y) :- reachable(X, Z), edge(Z, Y).
	`)
	if err != nil {
		b.Fatal(err)
	}
	tr, err := seminaive.New().Compile(rs)
	if err != nil {
		b.Fatal(err)
	}
	return input, tr
}

func BenchmarkTransitiveClosure50(b *testing.B) {
	input, tr := setupTransitiveClosure(b, 50)
	expected := 50 * 49 / 2 // 1225

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

func BenchmarkTransitiveClosure100(b *testing.B) {
	input, tr := setupTransitiveClosure(b, 100)
	expected := 100 * 99 / 2 // 4950

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

// BenchmarkFactLookup benchmarks Database.Query on a large fact set (no rules).
func BenchmarkFactLookup(b *testing.B) {
	builder := memory.NewBuilder()
	for i := range 1000 {
		builder.AddFact(datalog.Fact{
			Name:  "item",
			Terms: []datalog.Constant{datalog.String(fmt.Sprintf("key%d", i)), datalog.Integer(int64(i))},
		})
	}
	input := builder.Build()

	b.ResetTimer()
	for b.Loop() {
		count := 0
		for range input.Query("item", datalog.Variable("K"), datalog.Variable("V")) {
			count++
		}
		if count != 1000 {
			b.Fatalf("expected 1000, got %d", count)
		}
	}
}

// BenchmarkAggregateCount benchmarks aggregate computation.
func BenchmarkAggregateCount(b *testing.B) {
	builder := memory.NewBuilder()
	for i := range 500 {
		builder.AddFact(datalog.Fact{
			Name:  "score",
			Terms: []datalog.Constant{datalog.String(fmt.Sprintf("dept%d", i%10)), datalog.Integer(int64(i))},
		})
	}
	input := builder.Build()

	rs, err := syntax.ParseAll(`dept_count(Dept, C) :- C = count : score(Dept, ?).`)
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
		for range output.Facts("dept_count", 2) {
			count++
		}
		if count != 10 {
			b.Fatalf("expected 10 groups, got %d", count)
		}
	}
}

// BenchmarkJoinRule benchmarks multi-way joins.
func BenchmarkJoinRule(b *testing.B) {
	builder := memory.NewBuilder()
	for i := range 100 {
		aid := fmt.Sprintf("alert%d", i)
		host := fmt.Sprintf("host%d", i%5)
		builder.AddFact(datalog.Fact{
			Name:  "alert",
			Terms: []datalog.Constant{datalog.String(aid), datalog.String("High"), datalog.String("Malware"), datalog.String(host)},
		})
		for j := range 5 {
			builder.AddFact(datalog.Fact{
				Name:  "evidence",
				Terms: []datalog.Constant{datalog.String(aid), datalog.String(fmt.Sprintf("file%d", j)), datalog.String(fmt.Sprintf("hash%d", j))},
			})
		}
	}
	input := builder.Build()

	rs, err := syntax.ParseAll(`finding(AlertID, Host, File) :- alert(AlertID, "High", ?, Host), evidence(AlertID, File, ?).`)
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
		for range output.Facts("finding", 3) {
			count++
		}
		if count != 500 {
			b.Fatalf("expected 500, got %d", count)
		}
	}
}

// --- Same Generation (SG): non-linear recursion benchmark ---

func setupSameGeneration(b *testing.B, depth int) (datalog.Database, datalog.Transformer, int) {
	b.Helper()
	builder := memory.NewBuilder()

	// Complete binary tree: node i has children 2i+1 and 2i+2.
	n := (1 << (depth + 1)) - 1
	for i := range n {
		if left := 2*i + 1; left < n {
			builder.AddFact(datalog.Fact{
				Name:  "parent",
				Terms: []datalog.Constant{datalog.Integer(int64(i)), datalog.Integer(int64(left))},
			})
		}
		if right := 2*i + 2; right < n {
			builder.AddFact(datalog.Fact{
				Name:  "parent",
				Terms: []datalog.Constant{datalog.Integer(int64(i)), datalog.Integer(int64(right))},
			})
		}
	}
	input := builder.Build()

	rs, err := syntax.ParseAll(`
		sg(X, X) :- parent(?, X).
		sg(X, Y) :- parent(A, X), sg(A, B), parent(B, Y).
	`)
	if err != nil {
		b.Fatal(err)
	}
	tr, err := seminaive.New().Compile(rs)
	if err != nil {
		b.Fatal(err)
	}

	// Expected: 2*(4^depth - 1)/3
	exp := 1
	for range depth {
		exp *= 4
	}
	expected := 2 * (exp - 1) / 3
	return input, tr, expected
}

func BenchmarkSameGeneration127(b *testing.B) {
	input, tr, expected := setupSameGeneration(b, 6) // 127 nodes

	b.ResetTimer()
	for b.Loop() {
		output, err := tr.Transform(context.Background(), input)
		if err != nil {
			b.Fatal(err)
		}
		count := 0
		for range output.Facts("sg", 2) {
			count++
		}
		if count != expected {
			b.Fatalf("expected %d sg pairs, got %d", expected, count)
		}
	}
}

func BenchmarkSameGeneration255(b *testing.B) {
	input, tr, expected := setupSameGeneration(b, 7) // 255 nodes

	b.ResetTimer()
	for b.Loop() {
		output, err := tr.Transform(context.Background(), input)
		if err != nil {
			b.Fatal(err)
		}
		count := 0
		for range output.Facts("sg", 2) {
			count++
		}
		if count != expected {
			b.Fatalf("expected %d sg pairs, got %d", expected, count)
		}
	}
}

// --- Connected Components (CC): stratified aggregation benchmark ---

func setupConnectedComponents(b *testing.B, components, nodesPerComponent int) (datalog.Database, datalog.Transformer, int) {
	b.Helper()
	builder := memory.NewBuilder()

	for k := range components {
		base := k * nodesPerComponent
		for i := range nodesPerComponent - 1 {
			builder.AddFact(datalog.Fact{
				Name:  "edge",
				Terms: []datalog.Constant{datalog.Integer(int64(base + i)), datalog.Integer(int64(base + i + 1))},
			})
		}
	}
	input := builder.Build()

	rs, err := syntax.ParseAll(`
		bidir(X, Y) :- edge(X, Y).
		bidir(X, Y) :- edge(Y, X).
		reach(X, Y) :- bidir(X, Y).
		reach(X, Y) :- reach(X, Z), bidir(Z, Y).
		cc(X, C) :- C = min(Y) : reach(X, Y).
	`)
	if err != nil {
		b.Fatal(err)
	}
	tr, err := seminaive.New().Compile(rs)
	if err != nil {
		b.Fatal(err)
	}

	expected := components * nodesPerComponent
	return input, tr, expected
}

func BenchmarkConnectedComponents100(b *testing.B) {
	input, tr, expected := setupConnectedComponents(b, 10, 10)

	b.ResetTimer()
	for b.Loop() {
		output, err := tr.Transform(context.Background(), input)
		if err != nil {
			b.Fatal(err)
		}
		count := 0
		for range output.Facts("cc", 2) {
			count++
		}
		if count != expected {
			b.Fatalf("expected %d cc results, got %d", expected, count)
		}
	}
}

func BenchmarkConnectedComponents200(b *testing.B) {
	input, tr, expected := setupConnectedComponents(b, 10, 20)

	b.ResetTimer()
	for b.Loop() {
		output, err := tr.Transform(context.Background(), input)
		if err != nil {
			b.Fatal(err)
		}
		count := 0
		for range output.Facts("cc", 2) {
			count++
		}
		if count != expected {
			b.Fatalf("expected %d cc results, got %d", expected, count)
		}
	}
}

// --- Join1: 5-way chain join benchmark (no recursion) ---

func setupJoin1(b *testing.B, n int) (datalog.Database, datalog.Transformer) {
	b.Helper()
	builder := memory.NewBuilder()

	type rel struct {
		pred         string
		mult, offset int
	}
	rels := []rel{
		{"r1", 7, 1}, {"r2", 11, 3}, {"r3", 13, 5}, {"r4", 17, 7}, {"r5", 23, 11},
	}
	for _, r := range rels {
		for i := range n {
			builder.AddFact(datalog.Fact{
				Name:  r.pred,
				Terms: []datalog.Constant{datalog.Integer(int64(i)), datalog.Integer(int64((i*r.mult + r.offset) % n))},
			})
		}
	}
	input := builder.Build()

	rs, err := syntax.ParseAll(`result(X, Y) :- r1(X, Z1), r2(Z1, Z2), r3(Z2, Z3), r4(Z3, Z4), r5(Z4, Y).`)
	if err != nil {
		b.Fatal(err)
	}
	tr, err := seminaive.New().Compile(rs)
	if err != nil {
		b.Fatal(err)
	}
	return input, tr
}

func BenchmarkJoin1_1000(b *testing.B) {
	input, tr := setupJoin1(b, 1000)

	b.ResetTimer()
	for b.Loop() {
		output, err := tr.Transform(context.Background(), input)
		if err != nil {
			b.Fatal(err)
		}
		count := 0
		for range output.Facts("result", 2) {
			count++
		}
		if count != 1000 {
			b.Fatalf("expected 1000 results, got %d", count)
		}
	}
}

func BenchmarkJoin1_5000(b *testing.B) {
	input, tr := setupJoin1(b, 5000)

	b.ResetTimer()
	for b.Loop() {
		output, err := tr.Transform(context.Background(), input)
		if err != nil {
			b.Fatal(err)
		}
		count := 0
		for range output.Facts("result", 2) {
			count++
		}
		if count != 5000 {
			b.Fatalf("expected 5000 results, got %d", count)
		}
	}
}
