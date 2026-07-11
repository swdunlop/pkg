package seminaive_test

import (
	"strings"
	"testing"

	"swdunlop.dev/pkg/datalog"
	"swdunlop.dev/pkg/datalog/seminaive"
	"swdunlop.dev/pkg/datalog/syntax"
)

func TestPatternEndToEnd(t *testing.T) {
	output := transformFacts(t, seminaive.New(),
		`suspicious(P) :- process(P, {name: Name, ppid: 4}), @ends_with(Name, ".tmp.exe").`,
		datalog.Fact{Name: "process", Terms: []datalog.Constant{
			datalog.ID(1), mustComposite(t, map[string]any{"name": "x.tmp.exe", "ppid": 4}),
		}},
		datalog.Fact{Name: "process", Terms: []datalog.Constant{
			datalog.ID(2), mustComposite(t, map[string]any{"name": "x.tmp.exe", "ppid": 7}),
		}},
		datalog.Fact{Name: "process", Terms: []datalog.Constant{
			datalog.ID(3), mustComposite(t, map[string]any{"name": "sh", "ppid": 4}),
		}},
	)
	got := factStrings(t, output, "suspicious", 1)
	if len(got) != 1 || got[0] != "#1" {
		t.Errorf("got %v, want [#1]", got)
	}
}

func TestNestedPatternEndToEnd(t *testing.T) {
	output := transformFacts(t, seminaive.New(),
		`pname(Id, N) :- event(Id, {proc: {name: N}}).`,
		eventFact(t, 1, map[string]any{"proc": map[string]any{"name": "sh"}}),
		eventFact(t, 2, map[string]any{"proc": "not an object"}),
		eventFact(t, 3, map[string]any{"other": 1}),
	)
	got := factStrings(t, output, "pname", 2)
	if len(got) != 1 || got[0] != `#1,"sh"` {
		t.Errorf("got %v", got)
	}
}

func TestArrayPatternEndToEnd(t *testing.T) {
	output := transformFacts(t, seminaive.New(),
		`pair(Id, A, B) :- event(Id, [A, B]).
		 walk(Id, H, T) :- event(Id, [H | T]).`,
		eventFact(t, 1, []any{"x", "y"}),
		eventFact(t, 2, []any{"only"}),
	)
	got := factStrings(t, output, "pair", 3)
	if len(got) != 1 || got[0] != `#1,"x","y"` {
		t.Errorf("pair: got %v", got)
	}
	got = factStrings(t, output, "walk", 3)
	want := []string{`#1,"x",["y"]`, `#2,"only",[]`}
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("walk: got %v, want %v", got, want)
	}
}

func TestRuleVarLimit(t *testing.T) {
	// 17 distinct variables exceeds the evaluator's fixed-size substitution.
	rules := `big(A) :- p(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P), q(Q).`
	rs, err := syntax.ParseAll(rules)
	if err != nil {
		t.Fatal(err)
	}
	_, err = seminaive.New().Compile(rs)
	if err == nil || !strings.Contains(err.Error(), "distinct variables") {
		t.Errorf("expected variable-limit error, got %v", err)
	}
}
