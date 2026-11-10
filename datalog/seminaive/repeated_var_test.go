package seminaive_test

import (
	"slices"
	"testing"

	"swdunlop.dev/pkg/datalog"
	"swdunlop.dev/pkg/datalog/seminaive"
)

// TestRepeatedVariableInAtom verifies that a variable occurring more than
// once in a single body atom constrains those positions to be equal.
// Previously interned.UnifyV only compared a position against bindings made
// before the atom, so both occurrences of a fresh X were treated as new and
// the last one won: p(X, X) matched p(1, 2) and bound X = 2. Rule bodies,
// recursive (delta) joins and aggregate/query bodies all share UnifyV.
func TestRepeatedVariableInAtom(t *testing.T) {
	facts := []datalog.Fact{
		{Name: "p", Terms: []datalog.Constant{datalog.Integer(1), datalog.Integer(2)}},
		{Name: "p", Terms: []datalog.Constant{datalog.Integer(3), datalog.Integer(3)}},
		{Name: "r", Terms: []datalog.Constant{datalog.Integer(1)}},
		{Name: "r", Terms: []datalog.Constant{datalog.Integer(2)}},
		{Name: "r", Terms: []datalog.Constant{datalog.Integer(3)}},
	}
	ints := func(output datalog.Database, pred string) []int64 {
		var got []int64
		for row := range output.Facts(pred, 1) {
			got = append(got, int64(row[0].(datalog.Integer)))
		}
		slices.Sort(got)
		return got
	}
	cases := []struct {
		name, rules, pred string
		want              []int64
	}{
		{"join", `q(X) :- p(X, X).`, "q", []int64{3}},
		// e(X, X) in the recursive rule is matched against the delta of e
		// as well as the full relation. The bug would let e(1, 2) bind
		// X = 2 and derive e(2, 1), adding 2 to q.
		{"recursive", `
			e(X, Y) :- p(X, Y).
			e(X, Y) :- e(X, X), r(Y), Y < X.
			q(X) :- e(X, _), X != 3.
		`, "q", []int64{1}},
		{"aggregate", `q(N) :- N = count : p(X, X).`, "q", []int64{1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			output := transformFacts(t, seminaive.New(), tc.rules, facts...)
			if got := ints(output, tc.pred); !slices.Equal(got, tc.want) {
				t.Errorf("%s = %v, want %v", tc.pred, got, tc.want)
			}
		})
	}
}
