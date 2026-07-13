package seminaive_test

import (
	"context"
	"math"
	"strings"
	"testing"

	"swdunlop.dev/pkg/datalog"
	"swdunlop.dev/pkg/datalog/memory"
	"swdunlop.dev/pkg/datalog/seminaive"
	"swdunlop.dev/pkg/datalog/syntax"
)

// TestAggregateMinExactInt64AboveFloat53 confirms that min over a single
// large int64 (above 2^53, where float64 can no longer represent every
// integer exactly) returns the exact value rather than a float64-rounded
// neighbor.
func TestAggregateMinExactInt64AboveFloat53(t *testing.T) {
	const want = int64(9007199254740993) // 2^53 + 1, not representable in float64

	b := memory.NewBuilder()
	b.AddFact(datalog.Fact{Name: "v", Terms: []datalog.Constant{datalog.Integer(want)}})
	input := b.Build()

	rs, err := syntax.ParseAll(`
		lowest(M) :- M = min(X) : v(X).
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

	found := false
	for row := range output.Query("lowest", datalog.Variable("M")) {
		found = true
		m := int64(row[0].(datalog.Integer))
		if m != want {
			t.Errorf("expected lowest = %d, got %d", want, m)
		}
	}
	if !found {
		t.Fatal("expected a lowest(M) fact")
	}
}

// TestAggregateSumMinExactNanosecondTimestamp confirms sum and min over a
// single nanosecond-resolution Unix timestamp (well above 2^53) are exact,
// not off by tens of nanoseconds from a float64 round-trip.
func TestAggregateSumMinExactNanosecondTimestamp(t *testing.T) {
	const want = int64(1752300000123456789)

	b := memory.NewBuilder()
	b.AddFact(datalog.Fact{Name: "ts", Terms: []datalog.Constant{datalog.Integer(want)}})
	input := b.Build()

	rs, err := syntax.ParseAll(`
		total(T) :- T = sum(X) : ts(X).
		earliest(M) :- M = min(X) : ts(X).
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

	for row := range output.Query("total", datalog.Variable("T")) {
		got := int64(row[0].(datalog.Integer))
		if got != want {
			t.Errorf("expected total = %d, got %d", want, got)
		}
	}
	for row := range output.Query("earliest", datalog.Variable("M")) {
		got := int64(row[0].(datalog.Integer))
		if got != want {
			t.Errorf("expected earliest = %d, got %d", want, got)
		}
	}
}

// TestAggregateSumMixedIntFloatPromotes confirms a mixed int+float sum still
// works and produces a float64 result once any float is involved.
func TestAggregateSumMixedIntFloatPromotes(t *testing.T) {
	b := memory.NewBuilder()
	b.AddFact(datalog.Fact{Name: "score", Terms: []datalog.Constant{datalog.Integer(10)}})
	b.AddFact(datalog.Fact{Name: "score", Terms: []datalog.Constant{datalog.Float(2.5)}})
	input := b.Build()

	rs, err := syntax.ParseAll(`
		total(T) :- T = sum(X) : score(X).
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

	found := false
	for row := range output.Query("total", datalog.Variable("T")) {
		found = true
		f, ok := row[0].(datalog.Float)
		if !ok {
			t.Fatalf("expected float result, got %T (%v)", row[0], row[0])
		}
		if float64(f) != 12.5 {
			t.Errorf("expected total = 12.5, got %v", f)
		}
	}
	if !found {
		t.Fatal("expected a total(T) fact")
	}
}

// TestAggregateSumInt64OverflowErrors confirms that summing two int64 values
// whose total overflows int64 returns an error from Transform instead of
// silently wrapping through a float64 accumulator.
func TestAggregateSumInt64OverflowErrors(t *testing.T) {
	b := memory.NewBuilder()
	b.AddFact(datalog.Fact{Name: "v", Terms: []datalog.Constant{datalog.String("a"), datalog.Integer(math.MaxInt64 - 1)}})
	b.AddFact(datalog.Fact{Name: "v", Terms: []datalog.Constant{datalog.String("b"), datalog.Integer(math.MaxInt64 - 1)}})
	input := b.Build()

	rs, err := syntax.ParseAll(`
		total(T) :- T = sum(X) : v(?, X).
	`)
	if err != nil {
		t.Fatal(err)
	}
	tr, err := seminaive.New().Compile(rs)
	if err != nil {
		t.Fatal(err)
	}

	_, err = tr.Transform(context.Background(), input)
	if err == nil {
		t.Fatal("expected an overflow error, got nil")
	}
}

// TestAggregatePlainFloatUnchanged confirms ordinary float-only aggregates
// still behave the same as before the int64 accumulator change.
func TestAggregatePlainFloatUnchanged(t *testing.T) {
	b := memory.NewBuilder()
	b.AddFact(datalog.Fact{Name: "score", Terms: []datalog.Constant{datalog.Float(1.5)}})
	b.AddFact(datalog.Fact{Name: "score", Terms: []datalog.Constant{datalog.Float(2.25)}})
	b.AddFact(datalog.Fact{Name: "score", Terms: []datalog.Constant{datalog.Float(0.75)}})
	input := b.Build()

	rs, err := syntax.ParseAll(`
		total(T) :- T = sum(X) : score(X).
		lowest(M) :- M = min(X) : score(X).
		highest(M) :- M = max(X) : score(X).
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

	for row := range output.Query("total", datalog.Variable("T")) {
		f := float64(row[0].(datalog.Float))
		if f != 4.5 {
			t.Errorf("expected total = 4.5, got %v", f)
		}
	}
	for row := range output.Query("lowest", datalog.Variable("M")) {
		f := float64(row[0].(datalog.Float))
		if f != 0.75 {
			t.Errorf("expected lowest = 0.75, got %v", f)
		}
	}
	for row := range output.Query("highest", datalog.Variable("M")) {
		f := float64(row[0].(datalog.Float))
		if f != 2.25 {
			t.Errorf("expected highest = 2.25, got %v", f)
		}
	}
}

// TestAggregateMinMaxExactMixedIntFloat covers min/max over groups mixing
// float64 and large int64 values. An earlier version compared int64
// candidates against a float running extremum via float64(v), so min over
// {1e19, int64max} returned float64(int64max) = 2^63 -- a value larger than
// the true minimum and not present in the input at all. The extremum must
// come back as the exact int64 that was in the data.
func TestAggregateMinMaxExactMixedIntFloat(t *testing.T) {
	const intMax = int64(math.MaxInt64)
	const intMin = int64(math.MinInt64)

	cases := []struct {
		name  string
		facts []datalog.Constant
		rule  string
		want  datalog.Constant
	}{
		// 1e19 > int64max, so the exact int64 is the minimum.
		{"min-float-first", []datalog.Constant{datalog.Float(1e19), datalog.Integer(intMax)},
			`m(M) :- M = min(X) : v(X).`, datalog.Integer(intMax)},
		{"min-int-first", []datalog.Constant{datalog.Integer(intMax), datalog.Float(1e19)},
			`m(M) :- M = min(X) : v(X).`, datalog.Integer(intMax)},
		// -1e19 < int64min, so the exact int64 is the maximum.
		{"max-float-first", []datalog.Constant{datalog.Float(-1e19), datalog.Integer(intMin)},
			`m(M) :- M = max(X) : v(X).`, datalog.Integer(intMin)},
		{"max-int-first", []datalog.Constant{datalog.Integer(intMin), datalog.Float(-1e19)},
			`m(M) :- M = max(X) : v(X).`, datalog.Integer(intMin)},
		// The float side still wins when it should, and stays a float.
		{"min-float-wins", []datalog.Constant{datalog.Float(-0.5), datalog.Integer(3)},
			`m(M) :- M = min(X) : v(X).`, datalog.Float(-0.5)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := memory.NewBuilder()
			for _, c := range tc.facts {
				b.AddFact(datalog.Fact{Name: "v", Terms: []datalog.Constant{c}})
			}
			input := b.Build()

			rs, err := syntax.ParseAll(tc.rule)
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
			var got []datalog.Constant
			for row := range output.Facts("m", 1) {
				got = append(got, row[0])
			}
			if len(got) != 1 || got[0] != tc.want {
				t.Fatalf("got %#v, want exactly [%#v]", got, tc.want)
			}
		})
	}
}

// TestAggregateMinMaxNaNErrors confirms min/max over a group containing NaN
// fails loudly instead of letting NaN's position in the group silently
// decide the result (NaN is unordered, so "keep the current best" would
// return whichever value happened to arrive first).
func TestAggregateMinMaxNaNErrors(t *testing.T) {
	b := memory.NewBuilder()
	b.AddFact(datalog.Fact{Name: "v", Terms: []datalog.Constant{datalog.Float(math.NaN())}})
	b.AddFact(datalog.Fact{Name: "v", Terms: []datalog.Constant{datalog.Integer(1)}})
	input := b.Build()

	rs, err := syntax.ParseAll(`m(M) :- M = min(X) : v(X).`)
	if err != nil {
		t.Fatal(err)
	}
	tr, err := seminaive.New().Compile(rs)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tr.Transform(context.Background(), input); err == nil {
		t.Fatal("expected an error computing min over a group containing NaN, got nil")
	} else if !strings.Contains(err.Error(), "NaN") {
		t.Fatalf("expected the error to mention NaN, got: %v", err)
	}
}
