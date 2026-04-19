package seminaive_test

import (
	"context"
	"slices"
	"testing"

	"swdunlop.dev/pkg/datalog"
	"swdunlop.dev/pkg/datalog/memory"
	"swdunlop.dev/pkg/datalog/seminaive"
	"swdunlop.dev/pkg/datalog/syntax"
)

func TestFactPassthrough(t *testing.T) {
	b := memory.NewBuilder()
	b.AddFact(datalog.Fact{Name: "parent", Terms: []datalog.Constant{datalog.String("tom"), datalog.String("bob")}})
	b.AddFact(datalog.Fact{Name: "parent", Terms: []datalog.Constant{datalog.String("bob"), datalog.String("ann")}})
	input := b.Build()

	rs, err := syntax.ParseAll(`grandparent(X, Z) :- parent(X, Y), parent(Y, Z).`)
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

	// Input facts should still be present.
	count := 0
	for range output.Facts("parent", 2) {
		count++
	}
	if count != 2 {
		t.Errorf("expected 2 parent facts, got %d", count)
	}
}

func TestSimpleRule(t *testing.T) {
	b := memory.NewBuilder()
	b.AddFact(datalog.Fact{Name: "parent", Terms: []datalog.Constant{datalog.String("tom"), datalog.String("bob")}})
	b.AddFact(datalog.Fact{Name: "parent", Terms: []datalog.Constant{datalog.String("bob"), datalog.String("ann")}})
	input := b.Build()

	rs, err := syntax.ParseAll(`grandparent(X, Z) :- parent(X, Y), parent(Y, Z).`)
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

	var results []string
	for row := range output.Facts("grandparent", 2) {
		results = append(results, string(row[0].(datalog.String))+","+string(row[1].(datalog.String)))
	}
	if len(results) != 1 || results[0] != "tom,ann" {
		t.Errorf("expected [tom,ann], got %v", results)
	}
}

func TestRecursiveRule(t *testing.T) {
	b := memory.NewBuilder()
	b.AddFact(datalog.Fact{Name: "parent", Terms: []datalog.Constant{datalog.String("tom"), datalog.String("bob")}})
	b.AddFact(datalog.Fact{Name: "parent", Terms: []datalog.Constant{datalog.String("bob"), datalog.String("ann")}})
	b.AddFact(datalog.Fact{Name: "parent", Terms: []datalog.Constant{datalog.String("ann"), datalog.String("pat")}})
	input := b.Build()

	rs, err := syntax.ParseAll(`
		ancestor(X, Y) :- parent(X, Y).
		ancestor(X, Y) :- parent(X, Z), ancestor(Z, Y).
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
	for range output.Facts("ancestor", 2) {
		count++
	}
	// tom->bob, tom->ann, tom->pat, bob->ann, bob->pat, ann->pat = 6
	if count != 6 {
		t.Errorf("expected 6 ancestor facts, got %d", count)
	}
}

func TestQuery(t *testing.T) {
	b := memory.NewBuilder()
	b.AddFact(datalog.Fact{Name: "parent", Terms: []datalog.Constant{datalog.String("tom"), datalog.String("bob")}})
	b.AddFact(datalog.Fact{Name: "parent", Terms: []datalog.Constant{datalog.String("tom"), datalog.String("liz")}})
	b.AddFact(datalog.Fact{Name: "parent", Terms: []datalog.Constant{datalog.String("bob"), datalog.String("ann")}})
	input := b.Build()

	rs, err := syntax.ParseAll(`
		ancestor(X, Y) :- parent(X, Y).
		ancestor(X, Y) :- parent(X, Z), ancestor(Z, Y).
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

	// Query ancestors of tom.
	var results []string
	for row := range output.Query("ancestor", datalog.String("tom"), datalog.Variable("X")) {
		results = append(results, string(row[1].(datalog.String)))
	}
	slices.Sort(results)
	expected := []string{"ann", "bob", "liz"}
	if !slices.Equal(results, expected) {
		t.Errorf("expected %v, got %v", expected, results)
	}
}

func TestNegation(t *testing.T) {
	b := memory.NewBuilder()
	b.AddFact(datalog.Fact{Name: "node", Terms: []datalog.Constant{datalog.String("a")}})
	b.AddFact(datalog.Fact{Name: "node", Terms: []datalog.Constant{datalog.String("b")}})
	b.AddFact(datalog.Fact{Name: "node", Terms: []datalog.Constant{datalog.String("c")}})
	b.AddFact(datalog.Fact{Name: "edge", Terms: []datalog.Constant{datalog.String("a"), datalog.String("b")}})
	input := b.Build()

	rs, err := syntax.ParseAll(`
		reachable(X, Y) :- edge(X, Y).
		reachable(X, Y) :- edge(X, Z), reachable(Z, Y).
		unreachable(X, Y) :- node(X), node(Y), not reachable(X, Y), X != Y.
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
	for range output.Facts("unreachable", 2) {
		count++
	}
	// a->c, b->a, b->c, c->a, c->b = 5 unreachable pairs
	if count != 5 {
		t.Errorf("expected 5 unreachable pairs, got %d", count)
	}
}

func TestArithmetic(t *testing.T) {
	b := memory.NewBuilder()
	b.AddFact(datalog.Fact{Name: "val", Terms: []datalog.Constant{datalog.String("x"), datalog.Integer(10)}})
	b.AddFact(datalog.Fact{Name: "val", Terms: []datalog.Constant{datalog.String("y"), datalog.Integer(3)}})
	input := b.Build()

	rs, err := syntax.ParseAll(`
		sum_val(N, M, R) :- val(N, X), val(M, Y), R is X + Y, N != M.
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
	for row := range output.Facts("sum_val", 3) {
		r := int64(row[2].(datalog.Integer))
		if r != 13 {
			t.Errorf("expected sum 13, got %d", r)
		}
		count++
	}
	if count != 2 { // x+y and y+x
		t.Errorf("expected 2 sum_val facts, got %d", count)
	}
}

func TestStringContains(t *testing.T) {
	b := memory.NewBuilder()
	b.AddFact(datalog.Fact{Name: "msg", Terms: []datalog.Constant{datalog.String("hello world")}})
	b.AddFact(datalog.Fact{Name: "msg", Terms: []datalog.Constant{datalog.String("goodbye")}})
	input := b.Build()

	rs, err := syntax.ParseAll(`
		has_hello(X) :- msg(X), @contains(X, "hello").
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
	for range output.Facts("has_hello", 1) {
		count++
	}
	if count != 1 {
		t.Errorf("expected 1 has_hello fact, got %d", count)
	}
}

func TestAggregateCount(t *testing.T) {
	b := memory.NewBuilder()
	b.AddFact(datalog.Fact{Name: "edge", Terms: []datalog.Constant{datalog.String("a"), datalog.String("b")}})
	b.AddFact(datalog.Fact{Name: "edge", Terms: []datalog.Constant{datalog.String("a"), datalog.String("c")}})
	b.AddFact(datalog.Fact{Name: "edge", Terms: []datalog.Constant{datalog.String("b"), datalog.String("c")}})
	input := b.Build()

	rs, err := syntax.ParseAll(`
		out_degree(X, N) :- N = count : edge(X, ?).
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

	for row := range output.Query("out_degree", datalog.String("a"), datalog.Variable("N")) {
		n := int64(row[1].(datalog.Integer))
		if n != 2 {
			t.Errorf("expected out_degree(a) = 2, got %d", n)
		}
	}
}

func TestAggregateSum(t *testing.T) {
	b := memory.NewBuilder()
	b.AddFact(datalog.Fact{Name: "score", Terms: []datalog.Constant{datalog.String("alice"), datalog.Integer(10)}})
	b.AddFact(datalog.Fact{Name: "score", Terms: []datalog.Constant{datalog.String("alice"), datalog.Integer(20)}})
	b.AddFact(datalog.Fact{Name: "score", Terms: []datalog.Constant{datalog.String("bob"), datalog.Integer(15)}})
	input := b.Build()

	rs, err := syntax.ParseAll(`
		total(P, T) :- T = sum(S) : score(P, S).
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

	for row := range output.Query("total", datalog.String("alice"), datalog.Variable("T")) {
		total := int64(row[1].(datalog.Integer))
		if total != 30 {
			t.Errorf("expected total(alice) = 30, got %d", total)
		}
	}
}

func TestAggregateMin(t *testing.T) {
	b := memory.NewBuilder()
	b.AddFact(datalog.Fact{Name: "score", Terms: []datalog.Constant{datalog.String("alice"), datalog.Integer(10)}})
	b.AddFact(datalog.Fact{Name: "score", Terms: []datalog.Constant{datalog.String("alice"), datalog.Integer(20)}})
	b.AddFact(datalog.Fact{Name: "score", Terms: []datalog.Constant{datalog.String("bob"), datalog.Integer(15)}})
	input := b.Build()

	rs, err := syntax.ParseAll(`
		lowest(P, M) :- M = min(S) : score(P, S).
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

	for row := range output.Query("lowest", datalog.String("alice"), datalog.Variable("M")) {
		m := int64(row[1].(datalog.Integer))
		if m != 10 {
			t.Errorf("expected lowest(alice) = 10, got %d", m)
		}
	}
	for row := range output.Query("lowest", datalog.String("bob"), datalog.Variable("M")) {
		m := int64(row[1].(datalog.Integer))
		if m != 15 {
			t.Errorf("expected lowest(bob) = 15, got %d", m)
		}
	}
}

func TestAggregateMax(t *testing.T) {
	b := memory.NewBuilder()
	b.AddFact(datalog.Fact{Name: "score", Terms: []datalog.Constant{datalog.String("alice"), datalog.Integer(10)}})
	b.AddFact(datalog.Fact{Name: "score", Terms: []datalog.Constant{datalog.String("alice"), datalog.Integer(20)}})
	b.AddFact(datalog.Fact{Name: "score", Terms: []datalog.Constant{datalog.String("bob"), datalog.Integer(15)}})
	input := b.Build()

	rs, err := syntax.ParseAll(`
		highest(P, M) :- M = max(S) : score(P, S).
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

	for row := range output.Query("highest", datalog.String("alice"), datalog.Variable("M")) {
		m := int64(row[1].(datalog.Integer))
		if m != 20 {
			t.Errorf("expected highest(alice) = 20, got %d", m)
		}
	}
	for row := range output.Query("highest", datalog.String("bob"), datalog.Variable("M")) {
		m := int64(row[1].(datalog.Integer))
		if m != 15 {
			t.Errorf("expected highest(bob) = 15, got %d", m)
		}
	}
}

func TestUnsafeRule(t *testing.T) {
	rs, err := syntax.ParseAll(`bad(X) :- good(Y).`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = seminaive.New().Compile(rs)
	if err == nil {
		t.Error("expected error for unsafe rule, got nil")
	}
}

func TestRulesetFacts(t *testing.T) {
	// Facts embedded in the ruleset should appear in output.
	rs, err := syntax.ParseAll(`
		parent("tom", "bob").
		parent("bob", "ann").
		grandparent(X, Z) :- parent(X, Y), parent(Y, Z).
	`)
	if err != nil {
		t.Fatal(err)
	}
	tr, err := seminaive.New().Compile(rs)
	if err != nil {
		t.Fatal(err)
	}

	output, err := tr.Transform(context.Background(), datalog.Empty{})
	if err != nil {
		t.Fatal(err)
	}

	count := 0
	for range output.Facts("grandparent", 2) {
		count++
	}
	if count != 1 {
		t.Errorf("expected 1 grandparent fact, got %d", count)
	}
}

func TestContextCancellation(t *testing.T) {
	b := memory.NewBuilder()
	b.AddFact(datalog.Fact{Name: "edge", Terms: []datalog.Constant{datalog.String("a"), datalog.String("b")}})
	input := b.Build()

	rs, err := syntax.ParseAll(`reachable(X, Y) :- edge(X, Y). reachable(X, Y) :- edge(X, Z), reachable(Z, Y).`)
	if err != nil {
		t.Fatal(err)
	}
	tr, err := seminaive.New().Compile(rs)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	_, err = tr.Transform(ctx, input)
	if err == nil {
		t.Error("expected context cancellation error")
	}
}

func TestParseAndTransform(t *testing.T) {
	// Test the syntax.Parse convenience function.
	b := memory.NewBuilder()
	b.AddFact(datalog.Fact{Name: "parent", Terms: []datalog.Constant{datalog.String("tom"), datalog.String("bob")}})
	input := b.Build()

	tr, err := syntax.Parse(seminaive.New(), `child(Y, X) :- parent(X, Y).`)
	if err != nil {
		t.Fatal(err)
	}

	output, err := tr.Transform(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}

	count := 0
	for range output.Facts("child", 2) {
		count++
	}
	if count != 1 {
		t.Errorf("expected 1 child fact, got %d", count)
	}
}

func TestBuiltin(t *testing.T) {
	// Register a custom builtin that doubles a number.
	double := func(inputs []any) (any, bool) {
		if len(inputs) != 1 {
			return nil, false
		}
		switch v := inputs[0].(type) {
		case int64:
			return v * 2, true
		case float64:
			return v * 2, true
		}
		return nil, false
	}

	b := memory.NewBuilder()
	b.AddFact(datalog.Fact{Name: "val", Terms: []datalog.Constant{datalog.String("x"), datalog.Integer(5)}})
	b.AddFact(datalog.Fact{Name: "val", Terms: []datalog.Constant{datalog.String("y"), datalog.Integer(10)}})
	input := b.Build()

	rs, err := syntax.ParseAll(`doubled(N, D) :- val(N, V), @double(V, D).`)
	if err != nil {
		t.Fatal(err)
	}
	eng := seminaive.New(seminaive.WithBuiltin("@double", double))
	tr, err := eng.Compile(rs)
	if err != nil {
		t.Fatal(err)
	}

	output, err := tr.Transform(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}

	for row := range output.Query("doubled", datalog.String("x"), datalog.Variable("D")) {
		d := int64(row[1].(datalog.Integer))
		if d != 10 {
			t.Errorf("expected doubled(x) = 10, got %d", d)
		}
	}
	for row := range output.Query("doubled", datalog.String("y"), datalog.Variable("D")) {
		d := int64(row[1].(datalog.Integer))
		if d != 20 {
			t.Errorf("expected doubled(y) = 20, got %d", d)
		}
	}

	count := 0
	for range output.Facts("doubled", 2) {
		count++
	}
	if count != 2 {
		t.Errorf("expected 2 doubled facts, got %d", count)
	}
}

func TestBuiltinWithAggregates(t *testing.T) {
	// Builtin in a rule, then aggregate over derived facts.
	negate := func(inputs []any) (any, bool) {
		if len(inputs) != 1 {
			return nil, false
		}
		switch v := inputs[0].(type) {
		case int64:
			return -v, true
		case float64:
			return -v, true
		}
		return nil, false
	}

	b := memory.NewBuilder()
	b.AddFact(datalog.Fact{Name: "val", Terms: []datalog.Constant{datalog.Integer(3)}})
	b.AddFact(datalog.Fact{Name: "val", Terms: []datalog.Constant{datalog.Integer(7)}})
	input := b.Build()

	rs, err := syntax.ParseAll(`
		neg(N) :- val(V), @negate(V, N).
		total(S) :- S = sum(N) : neg(N).
	`)
	if err != nil {
		t.Fatal(err)
	}
	eng := seminaive.New(seminaive.WithBuiltin("@negate", negate))
	tr, err := eng.Compile(rs)
	if err != nil {
		t.Fatal(err)
	}

	output, err := tr.Transform(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}

	for row := range output.Facts("total", 1) {
		s := int64(row[0].(datalog.Integer))
		if s != -10 {
			t.Errorf("expected total = -10, got %d", s)
		}
	}
}

func TestTimeDiff(t *testing.T) {
	b := memory.NewBuilder()
	b.AddFact(datalog.Fact{Name: "event", Terms: []datalog.Constant{
		datalog.String("a"), datalog.String("2024-01-01T00:00:00Z"),
	}})
	b.AddFact(datalog.Fact{Name: "event", Terms: []datalog.Constant{
		datalog.String("b"), datalog.String("2024-01-01T01:00:00Z"),
	}})
	input := b.Build()

	rs, err := syntax.ParseAll(`
		diff(X, Y, D) :- event(X, T1), event(Y, T2), @time_diff(T2, T1, D), X != Y.
	`)
	if err != nil {
		t.Fatal(err)
	}
	eng := seminaive.New(seminaive.WithBuiltin("@time_diff", seminaive.TimeDiff))
	tr, err := eng.Compile(rs)
	if err != nil {
		t.Fatal(err)
	}

	output, err := tr.Transform(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}

	for row := range output.Query("diff", datalog.String("a"), datalog.String("b"), datalog.Variable("D")) {
		d := int64(row[2].(datalog.Integer))
		if d != 3600 {
			t.Errorf("expected diff(a, b) = 3600, got %d", d)
		}
	}
	for row := range output.Query("diff", datalog.String("b"), datalog.String("a"), datalog.Variable("D")) {
		d := int64(row[2].(datalog.Integer))
		if d != -3600 {
			t.Errorf("expected diff(b, a) = -3600, got %d", d)
		}
	}
}

func TestTimeDiffEpoch(t *testing.T) {
	b := memory.NewBuilder()
	b.AddFact(datalog.Fact{Name: "ts", Terms: []datalog.Constant{
		datalog.String("a"), datalog.Integer(1000),
	}})
	b.AddFact(datalog.Fact{Name: "ts", Terms: []datalog.Constant{
		datalog.String("b"), datalog.Integer(1060),
	}})
	input := b.Build()

	rs, err := syntax.ParseAll(`
		delta(X, Y, D) :- ts(X, T1), ts(Y, T2), @time_diff(T2, T1, D), X != Y.
	`)
	if err != nil {
		t.Fatal(err)
	}
	eng := seminaive.New(seminaive.WithBuiltin("@time_diff", seminaive.TimeDiff))
	tr, err := eng.Compile(rs)
	if err != nil {
		t.Fatal(err)
	}

	output, err := tr.Transform(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}

	for row := range output.Query("delta", datalog.String("a"), datalog.String("b"), datalog.Variable("D")) {
		d := int64(row[2].(datalog.Integer))
		if d != 60 {
			t.Errorf("expected delta(a, b) = 60, got %d", d)
		}
	}
}

func TestWithProfile(t *testing.T) {
	b := memory.NewBuilder()
	b.AddFact(datalog.Fact{Name: "edge", Terms: []datalog.Constant{datalog.String("a"), datalog.String("b")}})
	b.AddFact(datalog.Fact{Name: "edge", Terms: []datalog.Constant{datalog.String("b"), datalog.String("c")}})
	input := b.Build()

	var captured []seminaive.StratumStats

	rs, err := syntax.ParseAll(`
		reachable(X, Y) :- edge(X, Y).
		reachable(X, Y) :- edge(X, Z), reachable(Z, Y).
		out_degree(X, N) :- N = count : reachable(X, ?).
	`)
	if err != nil {
		t.Fatal(err)
	}
	eng := seminaive.New(seminaive.WithProfile(func(stats []seminaive.StratumStats) {
		captured = stats
	}))
	tr, err := eng.Compile(rs)
	if err != nil {
		t.Fatal(err)
	}

	_, err = tr.Transform(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}

	if len(captured) == 0 {
		t.Fatal("expected stratum stats, got none")
	}

	// Should have at least one stratum with rules that derived facts.
	totalFacts := 0
	for _, s := range captured {
		totalFacts += s.FactCount
		if s.Duration < 0 {
			t.Errorf("stratum duration should not be negative: %v", s.Duration)
		}
	}
	if totalFacts == 0 {
		t.Error("expected some derived facts across strata")
	}
}
