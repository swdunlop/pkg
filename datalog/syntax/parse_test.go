package syntax_test

import (
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
