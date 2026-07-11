package seminaive_test

import (
	"context"
	"sort"
	"strings"
	"testing"

	"swdunlop.dev/pkg/datalog"
	"swdunlop.dev/pkg/datalog/memory"
	"swdunlop.dev/pkg/datalog/seminaive"
	"swdunlop.dev/pkg/datalog/syntax"
)

// rangeBuiltin yields 0..n-1 for input n.
func rangeBuiltin(inputs []any, yield func(outputs []any) bool) {
	n, ok := inputs[0].(int64)
	if !ok {
		return
	}
	for i := range n {
		if !yield([]any{i}) {
			return
		}
	}
}

// splitBuiltin yields (index, part) pairs for a comma-separated string.
func splitBuiltin(inputs []any, yield func(outputs []any) bool) {
	s, ok := inputs[0].(string)
	if !ok {
		return
	}
	for i, part := range strings.Split(s, ",") {
		if !yield([]any{int64(i), part}) {
			return
		}
	}
}

func transformFacts(t *testing.T, engine *seminaive.Engine, rules string, facts ...datalog.Fact) datalog.Database {
	t.Helper()
	b := memory.NewBuilder()
	for _, f := range facts {
		if err := b.AddFact(f); err != nil {
			t.Fatal(err)
		}
	}
	rs, err := syntax.ParseAll(rules)
	if err != nil {
		t.Fatal(err)
	}
	tr, err := engine.Compile(rs)
	if err != nil {
		t.Fatal(err)
	}
	output, err := tr.Transform(context.Background(), b.Build())
	if err != nil {
		t.Fatal(err)
	}
	return output
}

func TestMultiBuiltinFanOut(t *testing.T) {
	engine := seminaive.New(seminaive.WithMultiBuiltin("@range", 1, rangeBuiltin))
	output := transformFacts(t, engine,
		`slot(Name, I) :- task(Name, N), @range(N, I).`,
		datalog.Fact{Name: "task", Terms: []datalog.Constant{datalog.String("a"), datalog.Integer(3)}},
		datalog.Fact{Name: "task", Terms: []datalog.Constant{datalog.String("b"), datalog.Integer(1)}},
	)
	var got []string
	for row := range output.Facts("slot", 2) {
		got = append(got, row[0].String()+":"+row[1].String())
	}
	sort.Strings(got)
	want := []string{`"a":0`, `"a":1`, `"a":2`, `"b":0`}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

func TestMultiBuiltinTwoOutputs(t *testing.T) {
	engine := seminaive.New(seminaive.WithMultiBuiltin("@split", 2, splitBuiltin))
	output := transformFacts(t, engine,
		`part(I, P) :- csv(S), @split(S, I, P).`,
		datalog.Fact{Name: "csv", Terms: []datalog.Constant{datalog.String("x,y")}},
	)
	var got []string
	for row := range output.Facts("part", 2) {
		got = append(got, row[0].String()+"="+row[1].String())
	}
	sort.Strings(got)
	if len(got) != 2 || got[0] != `0="x"` || got[1] != `1="y"` {
		t.Errorf("got %v", got)
	}
}

func TestMultiBuiltinConstantOutputFilters(t *testing.T) {
	engine := seminaive.New(seminaive.WithMultiBuiltin("@range", 1, rangeBuiltin))
	output := transformFacts(t, engine,
		`has_two(Name) :- task(Name, N), @range(N, 2).`,
		datalog.Fact{Name: "task", Terms: []datalog.Constant{datalog.String("a"), datalog.Integer(3)}},
		datalog.Fact{Name: "task", Terms: []datalog.Constant{datalog.String("b"), datalog.Integer(1)}},
	)
	var got []string
	for row := range output.Facts("has_two", 1) {
		got = append(got, row[0].String())
	}
	if len(got) != 1 || got[0] != `"a"` {
		t.Errorf("got %v, want [\"a\"]", got)
	}
}

func TestMultiBuiltinBoundOutputEquality(t *testing.T) {
	// The output variable is already bound by an earlier join; the multi
	// builtin then acts as a membership check.
	engine := seminaive.New(seminaive.WithMultiBuiltin("@range", 1, rangeBuiltin))
	output := transformFacts(t, engine,
		`ok(Name, K) :- task(Name, N), key(K), @range(N, K).`,
		datalog.Fact{Name: "task", Terms: []datalog.Constant{datalog.String("a"), datalog.Integer(2)}},
		datalog.Fact{Name: "key", Terms: []datalog.Constant{datalog.Integer(1)}},
		datalog.Fact{Name: "key", Terms: []datalog.Constant{datalog.Integer(5)}},
	)
	var got []string
	for row := range output.Facts("ok", 2) {
		got = append(got, row[0].String()+":"+row[1].String())
	}
	if len(got) != 1 || got[0] != `"a":1` {
		t.Errorf("got %v, want [\"a\":1]", got)
	}
}

func TestMultiBuiltinUnsafeInput(t *testing.T) {
	engine := seminaive.New(seminaive.WithMultiBuiltin("@range", 1, rangeBuiltin))
	rs, err := syntax.ParseAll(`bad(I) :- @range(N, I).`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Compile(rs); err == nil {
		t.Error("expected unsafe-rule error for unbound multi builtin input")
	}
}

func TestMultiBuiltinInQueryPath(t *testing.T) {
	// Aggregate rules use the query evaluator, exercising queryRecursiveV.
	engine := seminaive.New(seminaive.WithMultiBuiltin("@range", 1, rangeBuiltin))
	output := transformFacts(t, engine,
		`total(Name, C) :- C = count : task(Name, N), @range(N, I).`,
		datalog.Fact{Name: "task", Terms: []datalog.Constant{datalog.String("a"), datalog.Integer(3)}},
	)
	var got []string
	for row := range output.Facts("total", 2) {
		got = append(got, row[0].String()+":"+row[1].String())
	}
	if len(got) != 1 || got[0] != `"a":3` {
		t.Errorf("got %v, want [\"a\":3]", got)
	}
}

func TestMultiBuiltinRecursiveDelta(t *testing.T) {
	// Multi builtin output feeding a recursive predicate must terminate and
	// produce the transitive results (delta iterations after the first).
	engine := seminaive.New(seminaive.WithMultiBuiltin("@range", 1, rangeBuiltin))
	output := transformFacts(t, engine,
		`down(N, I) :- seed(N), @range(N, I).
		 down(I, J) :- down(_, I), I > 0, @range(I, J).`,
		datalog.Fact{Name: "seed", Terms: []datalog.Constant{datalog.Integer(3)}},
	)
	count := 0
	for range output.Facts("down", 2) {
		count++
	}
	// down(3,{0,1,2}), down(2,{0,1}), down(1,{0}) = 6 facts.
	if count != 6 {
		t.Errorf("got %d down facts, want 6", count)
	}
}
