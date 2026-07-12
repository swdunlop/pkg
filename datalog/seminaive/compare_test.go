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

// TestMixedIntFloatComparisonTransferRepro is the end-to-end regression case
// for the compareValues bug: comparing an int64 fact field against a float64
// fact field (or vice versa) silently failed to match, dropping rows instead
// of comparing them numerically. transfer("a","b",10000.5) has a float
// amount while transfer("c","d",20000) has an int amount; both must satisfy
// Amt > 10000.
func TestMixedIntFloatComparisonTransferRepro(t *testing.T) {
	b := memory.NewBuilder()
	b.AddFact(datalog.Fact{Name: "transfer", Terms: []datalog.Constant{
		datalog.String("a"), datalog.String("b"), datalog.Float(10000.5),
	}})
	b.AddFact(datalog.Fact{Name: "transfer", Terms: []datalog.Constant{
		datalog.String("c"), datalog.String("d"), datalog.Integer(20000),
	}})
	input := b.Build()

	rs, err := syntax.ParseAll(`large_transfer(F, T, Amt) :- transfer(F, T, Amt), Amt > 10000.`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	tr, err := seminaive.New().Compile(rs)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	output, err := tr.Transform(context.Background(), input)
	if err != nil {
		t.Fatalf("transform: %v", err)
	}

	count := 0
	for range output.Facts("large_transfer", 3) {
		count++
	}
	if count != 2 {
		t.Errorf("expected 2 large_transfer facts, got %d", count)
	}
}

// TestMixedIntFloatComparisonOperators exercises <, >, <=, >=, ==, != between
// an int64 literal and a float64 fact value in both operand orders.
func TestMixedIntFloatComparisonOperators(t *testing.T) {
	cases := []struct {
		name      string
		factValue datalog.Constant
		rule      string
		wantMatch bool
	}{
		// fact(Val), Val < 10  -- fact side is float, literal side is int
		{"lt-float-lt-int-true", datalog.Float(9.5), `q(X) :- fact(X), X < 10.`, true},
		{"lt-float-lt-int-false", datalog.Float(10.5), `q(X) :- fact(X), X < 10.`, false},
		// fact(Val), 10 < Val -- literal side is int, fact side is float
		{"lt-int-lt-float-true", datalog.Float(10.5), `q(X) :- fact(X), 10 < X.`, true},
		{"lt-int-lt-float-false", datalog.Float(9.5), `q(X) :- fact(X), 10 < X.`, false},

		{"gt-float-gt-int-true", datalog.Float(10.5), `q(X) :- fact(X), X > 10.`, true},
		{"gt-float-gt-int-false", datalog.Float(9.5), `q(X) :- fact(X), X > 10.`, false},
		{"gt-int-gt-float-true", datalog.Float(9.5), `q(X) :- fact(X), 10 > X.`, true},
		{"gt-int-gt-float-false", datalog.Float(10.5), `q(X) :- fact(X), 10 > X.`, false},

		{"ge-float-ge-int-true-eq", datalog.Float(10), `q(X) :- fact(X), X >= 10.`, true},
		{"ge-float-ge-int-true-gt", datalog.Float(10.5), `q(X) :- fact(X), X >= 10.`, true},
		{"ge-float-ge-int-false", datalog.Float(9.5), `q(X) :- fact(X), X >= 10.`, false},
		{"ge-int-ge-float-true-eq", datalog.Float(10), `q(X) :- fact(X), 10 >= X.`, true},
		{"ge-int-ge-float-false", datalog.Float(10.5), `q(X) :- fact(X), 10 >= X.`, false},

		{"le-float-le-int-true-eq", datalog.Float(10), `q(X) :- fact(X), X <= 10.`, true},
		{"le-float-le-int-false", datalog.Float(10.5), `q(X) :- fact(X), X <= 10.`, false},
		{"le-int-le-float-true", datalog.Float(9.5), `q(X) :- fact(X), 10 <= X.`, false}, // 10 <= 9.5 is false
		{"le-int-le-float-true-eq", datalog.Float(10), `q(X) :- fact(X), 10 <= X.`, true},

		{"eq-float-eq-int-true", datalog.Float(10), `q(X) :- fact(X), X = 10.`, true},
		{"eq-float-eq-int-false", datalog.Float(10.5), `q(X) :- fact(X), X = 10.`, false},
		{"eq-int-eq-float-true", datalog.Float(10), `q(X) :- fact(X), 10 = X.`, true},

		{"ne-float-ne-int-true", datalog.Float(10.5), `q(X) :- fact(X), X != 10.`, true},
		{"ne-float-ne-int-false", datalog.Float(10), `q(X) :- fact(X), X != 10.`, false},
		{"ne-int-ne-float-true", datalog.Float(10.5), `q(X) :- fact(X), 10 != X.`, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := memory.NewBuilder()
			b.AddFact(datalog.Fact{Name: "fact", Terms: []datalog.Constant{tc.factValue}})
			input := b.Build()

			rs, err := syntax.ParseAll(tc.rule)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			tr, err := seminaive.New().Compile(rs)
			if err != nil {
				t.Fatalf("compile: %v", err)
			}
			output, err := tr.Transform(context.Background(), input)
			if err != nil {
				t.Fatalf("transform: %v", err)
			}

			count := 0
			for range output.Facts("q", 1) {
				count++
			}
			got := count == 1
			if got != tc.wantMatch {
				t.Errorf("rule %q with fact(%v): got match=%v, want %v", tc.rule, tc.factValue, got, tc.wantMatch)
			}
		})
	}
}

// TestLargeInt64FloatComparisonExact verifies that comparing an int64 beyond
// 2^53 against a float64 does not lose precision to float64 rounding.
// 9007199254740993 (2^53 + 1) is not exactly representable as float64; naive
// float conversion would round it to 9007199254740992.0 and incorrectly
// claim equality with that value. The engine must keep them distinct.
func TestLargeInt64FloatComparisonExact(t *testing.T) {
	const bigInt = 9007199254740993 // 2^53 + 1, not exactly representable as float64
	const bigFloatBelow = 9007199254740992.0

	b := memory.NewBuilder()
	b.AddFact(datalog.Fact{Name: "fact", Terms: []datalog.Constant{datalog.Integer(bigInt)}})
	input := b.Build()

	t.Run("not-equal", func(t *testing.T) {
		rule := fmt.Sprintf(`q(X) :- fact(X), X = %v.`, bigFloatBelow)
		rs, err := syntax.ParseAll(rule)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		tr, err := seminaive.New().Compile(rs)
		if err != nil {
			t.Fatalf("compile: %v", err)
		}
		output, err := tr.Transform(context.Background(), input)
		if err != nil {
			t.Fatalf("transform: %v", err)
		}
		count := 0
		for range output.Facts("q", 1) {
			count++
		}
		if count != 0 {
			t.Errorf("expected %d != %v (exact comparison), but they matched", bigInt, bigFloatBelow)
		}
	})

	t.Run("greater-than", func(t *testing.T) {
		rule := fmt.Sprintf(`q(X) :- fact(X), X > %v.`, bigFloatBelow)
		rs, err := syntax.ParseAll(rule)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		tr, err := seminaive.New().Compile(rs)
		if err != nil {
			t.Fatalf("compile: %v", err)
		}
		output, err := tr.Transform(context.Background(), input)
		if err != nil {
			t.Fatalf("transform: %v", err)
		}
		count := 0
		for range output.Facts("q", 1) {
			count++
		}
		if count != 1 {
			t.Errorf("expected %d > %v to hold exactly, got count=%d", bigInt, bigFloatBelow, count)
		}
	})
}
