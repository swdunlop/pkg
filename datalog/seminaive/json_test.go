package seminaive_test

import (
	"sort"
	"testing"

	"swdunlop.dev/pkg/datalog"
	"swdunlop.dev/pkg/datalog/seminaive"
)

func mustComposite(t *testing.T, v any) *datalog.Composite {
	t.Helper()
	c, err := datalog.NewComposite(v)
	if err != nil {
		t.Fatalf("NewComposite: %v", err)
	}
	return c
}

// eventFact builds an event(Id, Record) fact with a composite record.
func eventFact(t *testing.T, id uint64, record any) datalog.Fact {
	t.Helper()
	return datalog.Fact{Name: "event", Terms: []datalog.Constant{
		datalog.ID(id), mustComposite(t, record),
	}}
}

func factStrings(t *testing.T, db datalog.Database, pred string, arity int) []string {
	t.Helper()
	var got []string
	for row := range db.Facts(pred, arity) {
		s := ""
		for i, c := range row {
			if i > 0 {
				s += ","
			}
			s += c.String()
		}
		got = append(got, s)
	}
	sort.Strings(got)
	return got
}

func TestJSONGetObject(t *testing.T) {
	output := transformFacts(t, seminaive.New(),
		`name(Id, N) :- event(Id, R), @json_get(R, "name", N).`,
		eventFact(t, 1, map[string]any{"name": "sh", "pid": 42}),
		eventFact(t, 2, map[string]any{"pid": 7}), // no name key: no match
	)
	got := factStrings(t, output, "name", 2)
	if len(got) != 1 || got[0] != `#1,"sh"` {
		t.Errorf("got %v", got)
	}
}

func TestJSONGetArrayIndex(t *testing.T) {
	output := transformFacts(t, seminaive.New(),
		`first(Id, V) :- event(Id, R), @json_get(R, 0, V).
		 oob(Id, V) :- event(Id, R), @json_get(R, 5, V).
		 neg(Id, V) :- event(Id, R), @json_get(R, -1, V).`,
		eventFact(t, 1, []any{"a", "b"}),
	)
	if got := factStrings(t, output, "first", 2); len(got) != 1 || got[0] != `#1,"a"` {
		t.Errorf("first: got %v", got)
	}
	if got := factStrings(t, output, "oob", 2); len(got) != 0 {
		t.Errorf("out of range should not match: %v", got)
	}
	if got := factStrings(t, output, "neg", 2); len(got) != 0 {
		t.Errorf("negative index should not match: %v", got)
	}
}

func TestJSONGetNestedComposite(t *testing.T) {
	output := transformFacts(t, seminaive.New(),
		`pname(Id, N) :- event(Id, R), @json_get(R, "proc", P), @json_get(P, "name", N).`,
		eventFact(t, 1, map[string]any{"proc": map[string]any{"name": "sh"}}),
	)
	got := factStrings(t, output, "pname", 2)
	if len(got) != 1 || got[0] != `#1,"sh"` {
		t.Errorf("got %v", got)
	}
}

func TestJSONGetScalarValues(t *testing.T) {
	output := transformFacts(t, seminaive.New(),
		`v(Id, K, V) :- event(Id, R), @json_items(R, K, V).`,
		eventFact(t, 1, map[string]any{"b": true, "n": nil, "i": 3, "f": 1.5, "s": "x"}),
	)
	got := factStrings(t, output, "v", 3)
	want := []string{`#1,"b",true`, `#1,"f",1.5`, `#1,"i",3`, `#1,"n",null`, `#1,"s","x"`}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

func TestJSONGetNonComposite(t *testing.T) {
	output := transformFacts(t, seminaive.New(),
		`bad(Id, V) :- plain(Id, S), @json_get(S, "k", V).`,
		datalog.Fact{Name: "plain", Terms: []datalog.Constant{datalog.ID(1), datalog.String("not json")}},
	)
	if got := factStrings(t, output, "bad", 2); len(got) != 0 {
		t.Errorf("non-composite input should fail: %v", got)
	}
}

func TestJSONGetWrongKeyType(t *testing.T) {
	output := transformFacts(t, seminaive.New(),
		`a(V) :- event(Id, R), @json_get(R, 0, V).`, // int index on an object
		eventFact(t, 1, map[string]any{"0": "x"}),
	)
	if got := factStrings(t, output, "a", 1); len(got) != 0 {
		t.Errorf("integer key on object should fail: %v", got)
	}
}

func TestJSONLen(t *testing.T) {
	output := transformFacts(t, seminaive.New(),
		`n(Id, N) :- event(Id, R), @json_len(R, N).`,
		eventFact(t, 1, []any{"a", "b", "c"}),
		eventFact(t, 2, map[string]any{"x": 1, "y": 2}),
	)
	got := factStrings(t, output, "n", 2)
	want := []string{"#1,3", "#2,2"}
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestJSONType(t *testing.T) {
	output := transformFacts(t, seminaive.New(),
		`ty(Id, T) :- event(Id, R), @json_type(R, T).
		 vty(K, T) :- scal(R), @json_items(R, K, V), @json_type(V, T).`,
		eventFact(t, 1, map[string]any{"o": map[string]any{}, "a": []any{}}),
		eventFact(t, 2, []any{1}),
		datalog.Fact{Name: "scal", Terms: []datalog.Constant{mustComposite(t,
			map[string]any{"b": false, "n": nil, "i": 3, "f": 2.5, "s": "x",
				"o": map[string]any{}, "a": []any{}})}},
	)
	got := factStrings(t, output, "ty", 2)
	if len(got) != 2 || got[0] != `#1,"object"` || got[1] != `#2,"array"` {
		t.Errorf("ty: got %v", got)
	}
	got = factStrings(t, output, "vty", 2)
	want := []string{`"a","array"`, `"b","bool"`, `"f","float"`, `"i","integer"`,
		`"n","null"`, `"o","object"`, `"s","string"`}
	if len(got) != len(want) {
		t.Fatalf("vty: got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("vty: got %v, want %v", got, want)
		}
	}
}

func TestJSONSlice(t *testing.T) {
	output := transformFacts(t, seminaive.New(),
		`tail(Id, T) :- event(Id, R), @json_slice(R, 1, T).
		 empty(Id, T) :- event(Id, R), @json_slice(R, 3, T).
		 oob(Id, T) :- event(Id, R), @json_slice(R, 4, T).`,
		eventFact(t, 1, []any{"a", "b", "c"}),
	)
	if got := factStrings(t, output, "tail", 2); len(got) != 1 || got[0] != `#1,["b","c"]` {
		t.Errorf("tail: got %v", got)
	}
	if got := factStrings(t, output, "empty", 2); len(got) != 1 || got[0] != `#1,[]` {
		t.Errorf("empty: got %v", got)
	}
	if got := factStrings(t, output, "oob", 2); len(got) != 0 {
		t.Errorf("out of range slice should fail: %v", got)
	}
}

func TestJSONSliceRecursion(t *testing.T) {
	// [H | T] style recursion over a list terminates because slices shrink.
	output := transformFacts(t, seminaive.New(),
		`elem(H) :- event(_, R), @json_get(R, 0, H).
		 rest(T) :- event(_, R), @json_slice(R, 1, T).
		 elem(H) :- rest(R), @json_get(R, 0, H).
		 rest(T) :- rest(R), @json_len(R, N), N > 0, @json_slice(R, 1, T).`,
		eventFact(t, 1, []any{"a", "b", "c"}),
	)
	got := factStrings(t, output, "elem", 1)
	want := []string{`"a"`, `"b"`, `"c"`}
	if len(got) != 3 || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestJSONEach(t *testing.T) {
	output := transformFacts(t, seminaive.New(),
		`to(Id, E) :- event(Id, R), @json_get(R, "rcpt", A), @json_each(A, E).`,
		eventFact(t, 1, map[string]any{"rcpt": []any{"x@a", "y@b"}}),
		eventFact(t, 2, map[string]any{"rcpt": []any{}}),
	)
	got := factStrings(t, output, "to", 2)
	if len(got) != 2 || got[0] != `#1,"x@a"` || got[1] != `#1,"y@b"` {
		t.Errorf("got %v", got)
	}
}

func TestJSONEachOnObjectFails(t *testing.T) {
	output := transformFacts(t, seminaive.New(),
		`e(V) :- event(_, R), @json_each(R, V).`,
		eventFact(t, 1, map[string]any{"k": "v"}),
	)
	if got := factStrings(t, output, "e", 1); len(got) != 0 {
		t.Errorf("@json_each on object should fail: %v", got)
	}
}

func TestJSONItemsOnArrayFails(t *testing.T) {
	output := transformFacts(t, seminaive.New(),
		`i(K, V) :- event(_, R), @json_items(R, K, V).`,
		eventFact(t, 1, []any{"a"}),
	)
	if got := factStrings(t, output, "i", 2); len(got) != 0 {
		t.Errorf("@json_items on array should fail: %v", got)
	}
}

func TestCompositeJoinAcrossFacts(t *testing.T) {
	// Structurally equal composites loaded separately join by ID equality.
	rec := map[string]any{"name": "sh", "pid": 1}
	output := transformFacts(t, seminaive.New(),
		`same(A, B) :- event(A, R), event(B, R), A != B.`,
		eventFact(t, 1, rec),
		eventFact(t, 2, map[string]any{"pid": 1.0, "name": "sh"}), // equal after normalization
		eventFact(t, 3, map[string]any{"pid": 2, "name": "sh"}),
	)
	got := factStrings(t, output, "same", 2)
	if len(got) != 2 || got[0] != "#1,#2" || got[1] != "#2,#1" {
		t.Errorf("got %v", got)
	}
}

func TestCompositeComparisonsFail(t *testing.T) {
	// < > on composites fail; = / != are ID comparisons and work.
	output := transformFacts(t, seminaive.New(),
		`lt(A, B) :- event(A, R1), event(B, R2), R1 < R2.
		 ne(A, B) :- event(A, R1), event(B, R2), R1 != R2.`,
		eventFact(t, 1, []any{1}),
		eventFact(t, 2, []any{2}),
	)
	if got := factStrings(t, output, "lt", 2); len(got) != 0 {
		t.Errorf("composite < should fail: %v", got)
	}
	got := factStrings(t, output, "ne", 2)
	if len(got) != 2 {
		t.Errorf("composite != should work by ID: %v", got)
	}
}
