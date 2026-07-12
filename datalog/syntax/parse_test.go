package syntax_test

import (
	"strings"
	"testing"

	"swdunlop.dev/pkg/datalog"
	"swdunlop.dev/pkg/datalog/syntax"
)

func TestParseFact(t *testing.T) {
	result, err := syntax.ParseStatement(`parent("tom", "bob").`)
	if err != nil {
		t.Fatal(err)
	}
	r, ok := result.(*syntax.Rule)
	if !ok {
		t.Fatalf("expected *Rule, got %T", result)
	}
	if !r.IsFact() {
		t.Error("expected fact")
	}
	if r.Head.Pred != "parent" {
		t.Errorf("expected pred parent, got %s", r.Head.Pred)
	}
	if len(r.Head.Terms) != 2 {
		t.Fatalf("expected 2 terms, got %d", len(r.Head.Terms))
	}
	if s, ok := r.Head.Terms[0].(datalog.String); !ok || string(s) != "tom" {
		t.Errorf("expected String(tom), got %v", r.Head.Terms[0])
	}
}

func TestParseRule(t *testing.T) {
	result, err := syntax.ParseStatement(`grandparent(X, Z) :- parent(X, Y), parent(Y, Z).`)
	if err != nil {
		t.Fatal(err)
	}
	r, ok := result.(*syntax.Rule)
	if !ok {
		t.Fatalf("expected *Rule, got %T", result)
	}
	if r.IsFact() {
		t.Error("expected rule, not fact")
	}
	if len(r.Body) != 2 {
		t.Errorf("expected 2 body atoms, got %d", len(r.Body))
	}
}

func TestParseQuery(t *testing.T) {
	result, err := syntax.ParseStatement(`parent(X, "bob")?`)
	if err != nil {
		t.Fatal(err)
	}
	q, ok := result.(*syntax.Query)
	if !ok {
		t.Fatalf("expected *Query, got %T", result)
	}
	if len(q.Body) != 1 {
		t.Errorf("expected 1 body atom, got %d", len(q.Body))
	}
	if _, ok := q.Body[0].Terms[0].(datalog.Variable); !ok {
		t.Error("expected first term to be a Variable")
	}
}

func TestParseNegation(t *testing.T) {
	result, err := syntax.ParseStatement(`lonely(X) :- person(X), not friend(X, ?).`)
	if err != nil {
		t.Fatal(err)
	}
	r := result.(*syntax.Rule)
	if len(r.Body) != 2 {
		t.Fatalf("expected 2 body atoms, got %d", len(r.Body))
	}
	if !r.Body[1].Negated {
		t.Error("expected second body atom to be negated")
	}
}

func TestParseBareUnderscoreAnonymous(t *testing.T) {
	// Each bare "_" is a distinct anonymous variable, as in Prolog —
	// p(_, _) must not unify its two positions.
	result, err := syntax.ParseStatement(`p(_, _)?`)
	if err != nil {
		t.Fatal(err)
	}
	q := result.(*syntax.Query)
	a, aok := q.Body[0].Terms[0].(datalog.Variable)
	b, bok := q.Body[0].Terms[1].(datalog.Variable)
	if !aok || !bok {
		t.Fatalf("expected two variables, got %v", q.Body[0].Terms)
	}
	if a == b {
		t.Fatalf("bare underscores unified into one variable %q", a)
	}
	if !strings.HasPrefix(string(a), "?") || !strings.HasPrefix(string(b), "?") {
		t.Fatalf("expected parser-generated anonymous names, got %q, %q", a, b)
	}

	// A named underscore-prefixed variable is NOT anonymous: it joins.
	result, err = syntax.ParseStatement(`q(_Same, _Same)?`)
	if err != nil {
		t.Fatal(err)
	}
	q = result.(*syntax.Query)
	if q.Body[0].Terms[0] != q.Body[0].Terms[1] {
		t.Fatalf("_Same occurrences should be one variable: %v", q.Body[0].Terms)
	}

	// "_ is Expr" evaluates and discards.
	result, err = syntax.ParseStatement(`r(X) :- val(X), _ is X * 2.`)
	if err != nil {
		t.Fatal(err)
	}
	r := result.(*syntax.Rule)
	lhs := r.Body[1].Terms[0].(datalog.Variable)
	if !strings.HasPrefix(string(lhs), "?") {
		t.Fatalf("expected anonymous lhs for '_ is', got %q", lhs)
	}

	// "_" and "?" draw from the same fresh-name counter, so mixing them
	// in one statement still yields all-distinct variables.
	result, err = syntax.ParseStatement(`s(_, ?, _)?`)
	if err != nil {
		t.Fatal(err)
	}
	q = result.(*syntax.Query)
	seen := map[datalog.Term]bool{}
	for _, term := range q.Body[0].Terms {
		if seen[term] {
			t.Fatalf("mixed anonymous terms collided: %v", q.Body[0].Terms)
		}
		seen[term] = true
	}
}

func TestParseIs(t *testing.T) {
	result, err := syntax.ParseStatement(`double(X, Y) :- val(X), Y is X * 2.`)
	if err != nil {
		t.Fatal(err)
	}
	r := result.(*syntax.Rule)
	if len(r.Body) != 2 {
		t.Fatalf("expected 2 body atoms, got %d", len(r.Body))
	}
	isAtom := r.Body[1]
	if isAtom.Pred != "is" {
		t.Errorf("expected 'is' pred, got %s", isAtom.Pred)
	}
	if isAtom.Expr == nil {
		t.Error("expected non-nil Expr")
	}
}

func TestParseAggregate(t *testing.T) {
	result, err := syntax.ParseStatement(`total(P, T) :- T = sum(S) : score(P, S).`)
	if err != nil {
		t.Fatal(err)
	}
	ar, ok := result.(*syntax.AggregateRule)
	if !ok {
		t.Fatalf("expected *AggregateRule, got %T", result)
	}
	if ar.Kind != syntax.AggSum {
		t.Errorf("expected AggSum, got %v", ar.Kind)
	}
	if ar.ResultVar != "T" {
		t.Errorf("expected ResultVar T, got %s", ar.ResultVar)
	}
}

func TestParseAll(t *testing.T) {
	rs, err := syntax.ParseAll(`
		parent("tom", "bob").
		parent("bob", "ann").
		ancestor(X, Y) :- parent(X, Y).
		ancestor(X, Y) :- parent(X, Z), ancestor(Z, Y).
		ancestor("tom", X)?
	`)
	if err != nil {
		t.Fatal(err)
	}
	if len(rs.Rules) != 4 { // 2 facts + 2 rules
		t.Errorf("expected 4 rules, got %d", len(rs.Rules))
	}
	if len(rs.Queries) != 1 {
		t.Errorf("expected 1 query, got %d", len(rs.Queries))
	}
}

func TestParseIntegerLiterals(t *testing.T) {
	result, err := syntax.ParseStatement(`val(42, -5).`)
	if err != nil {
		t.Fatal(err)
	}
	r := result.(*syntax.Rule)
	if v, ok := r.Head.Terms[0].(datalog.Integer); !ok || int64(v) != 42 {
		t.Errorf("expected Integer(42), got %v", r.Head.Terms[0])
	}
	if v, ok := r.Head.Terms[1].(datalog.Integer); !ok || int64(v) != -5 {
		t.Errorf("expected Integer(-5), got %v", r.Head.Terms[1])
	}
}

func TestParseFloatLiterals(t *testing.T) {
	result, err := syntax.ParseStatement(`val(3.14).`)
	if err != nil {
		t.Fatal(err)
	}
	r := result.(*syntax.Rule)
	if v, ok := r.Head.Terms[0].(datalog.Float); !ok || float64(v) != 3.14 {
		t.Errorf("expected Float(3.14), got %v", r.Head.Terms[0])
	}
}

func TestParseUnquotedVariable(t *testing.T) {
	result, err := syntax.ParseStatement(`color(red).`)
	if err != nil {
		t.Fatal(err)
	}
	r := result.(*syntax.Rule)
	if v, ok := r.Head.Terms[0].(datalog.Variable); !ok || string(v) != "red" {
		t.Errorf("expected Variable(red), got %v", r.Head.Terms[0])
	}
}

func TestParseComparison(t *testing.T) {
	result, err := syntax.ParseStatement(`bigger(X, Y) :- val(X, A), val(Y, B), A > B.`)
	if err != nil {
		t.Fatal(err)
	}
	r := result.(*syntax.Rule)
	if len(r.Body) != 3 {
		t.Fatalf("expected 3 body atoms, got %d", len(r.Body))
	}
	cmp := r.Body[2]
	if cmp.Pred != ">" {
		t.Errorf("expected pred '>', got %s", cmp.Pred)
	}
}

func TestParseBuiltinPredicate(t *testing.T) {
	result, err := syntax.ParseStatement(`has(X) :- msg(X), @contains(X, "hello").`)
	if err != nil {
		t.Fatal(err)
	}
	r := result.(*syntax.Rule)
	if r.Body[1].Pred != "@contains" {
		t.Errorf("expected pred @contains, got %s", r.Body[1].Pred)
	}
}

func TestParseErrorLineColumn(t *testing.T) {
	// The stray ']' is on line 3, column 8.
	_, err := syntax.ParseAll("a(1).\nb(2).\nc(3,   ].\n")
	if err == nil {
		t.Fatal("expected a parse error, got nil")
	}
	msg := err.Error()
	if !strings.Contains(msg, "line 3, column 8") {
		t.Errorf("expected the error to report line 3, column 8, got: %v", msg)
	}
	if !strings.Contains(msg, "c(3,   ].") {
		t.Errorf("expected the error to include the offending source line, got: %v", msg)
	}
	if !strings.Contains(msg, "\n\t       ^") {
		t.Errorf("expected a caret under the offending column, got: %v", msg)
	}
}

// TestParseAllStopsOnUnrecognizedChar guards against the lexer silently
// treating a stray character as end of input: ParseAll must report an error
// rather than truncating the ruleset. See ParseAll("a(1). ; b(2).") which
// used to return one rule and a nil error, silently dropping b(2).
func TestParseAllStopsOnUnrecognizedChar(t *testing.T) {
	_, err := syntax.ParseAll("a(1). ; b(2).")
	if err == nil {
		t.Fatal("expected a parse error, got nil")
	}
	msg := err.Error()
	if !strings.Contains(msg, `";"`) {
		t.Errorf("expected the error to mention the offending ';', got: %v", msg)
	}
	if !strings.Contains(msg, "line 1, column 7") {
		t.Errorf("expected the error to report line 1, column 7, got: %v", msg)
	}
}

// TestParseAllUnrecognizedCharAtStart guards against ParseAll("#junk")
// returning an empty ruleset with a nil error.
func TestParseAllUnrecognizedCharAtStart(t *testing.T) {
	rs, err := syntax.ParseAll("#junk")
	if err == nil {
		t.Fatalf("expected a parse error, got nil (ruleset: %+v)", rs)
	}
	if !strings.Contains(err.Error(), `"#"`) {
		t.Errorf("expected the error to mention the offending '#', got: %v", err)
	}
}

func TestParseLoneBangErrors(t *testing.T) {
	_, err := syntax.ParseAll("a(1) ! b(2).")
	if err == nil {
		t.Fatal("expected a parse error for a lone '!', got nil")
	}
	if !strings.Contains(err.Error(), `"!"`) {
		t.Errorf("expected the error to mention the offending '!', got: %v", err)
	}
}

func TestParseLoneAtErrors(t *testing.T) {
	_, err := syntax.ParseAll("a(1) @ b(2).")
	if err == nil {
		t.Fatal("expected a parse error for a lone '@', got nil")
	}
	if !strings.Contains(err.Error(), `"@"`) {
		t.Errorf("expected the error to mention the offending '@', got: %v", err)
	}
}

// TestParseAllValidProgramsStillParse is a broader smoke test that the
// unrecognized-character fix did not disturb lexing of valid input.
func TestParseAllValidProgramsStillParse(t *testing.T) {
	rs, err := syntax.ParseAll(`
		parent("tom", "bob").
		ancestor(X, Y) :- parent(X, Y).
		has(X) :- msg(X), @contains(X, "hello").
		total(S, T) :- T = sum(V) : val(V).
		q(X) :- val(X), X != 0, not excluded(X).
		ancestor("tom", X)?
	`)
	if err != nil {
		t.Fatalf("expected valid program to parse cleanly, got: %v", err)
	}
	if len(rs.Rules) != 4 {
		t.Errorf("expected 4 rules, got %d", len(rs.Rules))
	}
	if len(rs.AggRules) != 1 {
		t.Errorf("expected 1 aggregate rule, got %d", len(rs.AggRules))
	}
	if len(rs.Queries) != 1 {
		t.Errorf("expected 1 query, got %d", len(rs.Queries))
	}
}
