package syntax_test

import (
	"strings"
	"testing"

	"swdunlop.dev/pkg/datalog/syntax"
)

// parseRule parses a single statement expected to be a rule.
func parseRule(t *testing.T, input string) *syntax.Rule {
	t.Helper()
	result, err := syntax.ParseStatement(input)
	if err != nil {
		t.Fatalf("ParseStatement(%q): %v", input, err)
	}
	r, ok := result.(*syntax.Rule)
	if !ok {
		t.Fatalf("expected *Rule, got %T", result)
	}
	return r
}

// bodyString renders a rule body for structural assertions.
func bodyString(r *syntax.Rule) string {
	parts := make([]string, len(r.Body))
	for i, a := range r.Body {
		parts[i] = a.String()
	}
	return strings.Join(parts, ", ")
}

func TestObjectPatternDesugar(t *testing.T) {
	r := parseRule(t, `suspicious(P) :- process(P, {name: Name, pid: Pid}), Pid > 1000.`)
	want := `process(P, ?0), @json_get(?0, "name", Name), @json_get(?0, "pid", Pid), Pid > 1000`
	if got := bodyString(r); got != want {
		t.Errorf("got  %s\nwant %s", got, want)
	}
}

func TestObjectPatternQuotedKeyAndConstant(t *testing.T) {
	r := parseRule(t, `active(P) :- process(P, {"status": "active", name: N}).`)
	want := `process(P, ?0), @json_get(?0, "status", "active"), @json_get(?0, "name", N)`
	if got := bodyString(r); got != want {
		t.Errorf("got  %s\nwant %s", got, want)
	}
}

func TestNestedPatternDesugar(t *testing.T) {
	r := parseRule(t, `x(P) :- ev(P, {proc: {name: N}}).`)
	want := `ev(P, ?0), @json_get(?0, "proc", ?1), @json_get(?1, "name", N)`
	if got := bodyString(r); got != want {
		t.Errorf("got  %s\nwant %s", got, want)
	}
}

func TestNonLinearPattern(t *testing.T) {
	r := parseRule(t, `loop(P) :- conn(P, {src: X, dst: X}).`)
	want := `conn(P, ?0), @json_get(?0, "src", X), @json_get(?0, "dst", X)`
	if got := bodyString(r); got != want {
		t.Errorf("got  %s\nwant %s", got, want)
	}
}

func TestArrayPatternDesugar(t *testing.T) {
	r := parseRule(t, `pair(P) :- ev(P, [A, B]).`)
	want := `ev(P, ?0), @json_get(?0, 0, A), @json_get(?0, 1, B), @json_len(?0, 2)`
	if got := bodyString(r); got != want {
		t.Errorf("got  %s\nwant %s", got, want)
	}
}

func TestEmptyArrayPattern(t *testing.T) {
	r := parseRule(t, `none(P) :- ev(P, []).`)
	want := `ev(P, ?0), @json_len(?0, 0)`
	if got := bodyString(r); got != want {
		t.Errorf("got  %s\nwant %s", got, want)
	}
}

func TestHeadTailPattern(t *testing.T) {
	r := parseRule(t, `walk(H, T) :- list(L), l(L, [H | T]).`)
	want := `list(L), l(L, ?0), @json_get(?0, 0, H), @json_slice(?0, 1, T)`
	if got := bodyString(r); got != want {
		t.Errorf("got  %s\nwant %s", got, want)
	}
}

func TestMultiElementHeadTailPattern(t *testing.T) {
	r := parseRule(t, `x(A, B, T) :- l(L, [A, B | T]).`)
	want := `l(L, ?0), @json_get(?0, 0, A), @json_get(?0, 1, B), @json_slice(?0, 2, T)`
	if got := bodyString(r); got != want {
		t.Errorf("got  %s\nwant %s", got, want)
	}
}

func TestNestedArrayObjectPattern(t *testing.T) {
	r := parseRule(t, `x(N) :- ev([{name: N}]).`)
	want := `ev(?0), @json_get(?0, 0, ?1), @json_get(?1, "name", N), @json_len(?0, 1)`
	if got := bodyString(r); got != want {
		t.Errorf("got  %s\nwant %s", got, want)
	}
}

func TestPatternInQuery(t *testing.T) {
	result, err := syntax.ParseStatement(`process(P, {name: N})?`)
	if err != nil {
		t.Fatal(err)
	}
	q, ok := result.(*syntax.Query)
	if !ok {
		t.Fatalf("expected *Query, got %T", result)
	}
	if len(q.Body) != 2 {
		t.Fatalf("expected 2 body atoms after desugar, got %d: %v", len(q.Body), q)
	}
	if q.Body[1].Pred != "@json_get" {
		t.Errorf("expected @json_get, got %s", q.Body[1].Pred)
	}
}

func TestPatternInMultiAtomQuery(t *testing.T) {
	result, err := syntax.ParseStatement(`process(P, {name: N}), alert(P)?`)
	if err != nil {
		t.Fatal(err)
	}
	q, ok := result.(*syntax.Query)
	if !ok {
		t.Fatalf("expected *Query, got %T", result)
	}
	if len(q.Body) != 3 {
		t.Fatalf("expected 3 body atoms after desugar, got %d", len(q.Body))
	}
}

func TestPatternRejectedInRuleHead(t *testing.T) {
	_, err := syntax.ParseStatement(`bad({name: N}) :- p(N).`)
	if err == nil || !strings.Contains(err.Error(), "patterns are not allowed in rule heads") {
		t.Errorf("expected rule-head pattern error, got %v", err)
	}
}

func TestPatternRejectedInFact(t *testing.T) {
	_, err := syntax.ParseStatement(`bad({name: "x"}).`)
	if err == nil || !strings.Contains(err.Error(), "patterns are not allowed in rule heads") {
		t.Errorf("expected rule-head pattern error, got %v", err)
	}
}

func TestPatternRejectedUnderNegation(t *testing.T) {
	_, err := syntax.ParseStatement(`ok(P) :- p(P), not process(P, {name: "evil"}).`)
	if err == nil || !strings.Contains(err.Error(), "patterns are not allowed under negation") {
		t.Errorf("expected negation pattern error, got %v", err)
	}
}

func TestPatternRejectsVariableKey(t *testing.T) {
	_, err := syntax.ParseStatement(`bad(P) :- p(P, {Key: V}).`)
	if err == nil || !strings.Contains(err.Error(), "variable keys") {
		t.Errorf("expected variable-key error, got %v", err)
	}
	// Quoting makes the same key legal.
	if _, err := syntax.ParseStatement(`ok(P, V) :- p(P, {"Key": V}).`); err != nil {
		t.Errorf("quoted uppercase key should parse: %v", err)
	}
}

func TestPatternInAggregateBody(t *testing.T) {
	result, err := syntax.ParseStatement(`total(N, C) :- C = count : ev(E, {name: N}).`)
	if err != nil {
		t.Fatal(err)
	}
	ar, ok := result.(*syntax.AggregateRule)
	if !ok {
		t.Fatalf("expected *AggregateRule, got %T", result)
	}
	if len(ar.Body) != 2 || ar.Body[1].Pred != "@json_get" {
		t.Errorf("aggregate body missing desugared getter: %v", ar.Body)
	}
}

func TestDesugaredRuleStringReparses(t *testing.T) {
	r := parseRule(t, `suspicious(P) :- process(P, {name: Name}), @ends_with(Name, ".tmp.exe").`)
	printed := r.String()
	if strings.Contains(printed, "{") {
		t.Errorf("Rule.String should print the desugared form, got %s", printed)
	}
	if _, err := syntax.ParseStatement(printed); err != nil {
		t.Errorf("desugared form %q does not reparse: %v", printed, err)
	}
}

// --- Bug: {} desugared to no constraint atoms at all, so it matched any
// value (not just objects), unlike [] which correctly emits @json_len. ---

func TestEmptyObjectPatternConstrainsType(t *testing.T) {
	r := parseRule(t, `none(P) :- ev(P, {}).`)
	want := `ev(P, ?0), @json_type(?0, "object")`
	if got := bodyString(r); got != want {
		t.Errorf("got  %s\nwant %s", got, want)
	}
}

func TestEmptyObjectPatternNested(t *testing.T) {
	// {} must still constrain when nested inside another pattern, not just
	// at the top level of an atom's argument.
	r := parseRule(t, `x(N) :- ev(P, {meta: {}}), meta(P, N).`)
	want := `ev(P, ?0), @json_get(?0, "meta", ?1), @json_type(?1, "object"), meta(P, N)`
	if got := bodyString(r); got != want {
		t.Errorf("got  %s\nwant %s", got, want)
	}
}

// --- Bug: array patterns silently accepted elements with no comma
// separator, e.g. [X Y] parsed the same as [X, Y]. ---

func TestArrayPatternRequiresComma(t *testing.T) {
	_, err := syntax.ParseStatement(`q(X) :- p([X Y]).`)
	if err == nil {
		t.Fatal("expected a parse error for a missing comma in an array pattern, got nil")
	}
	if !strings.Contains(err.Error(), "','") {
		t.Errorf("expected the error to mention the missing comma, got: %v", err)
	}
}

func TestArrayPatternWithCommaStillParses(t *testing.T) {
	// Control: the comma-separated form must still work.
	r := parseRule(t, `q(X, Y) :- p([X, Y]).`)
	want := `p(?0), @json_get(?0, 0, X), @json_get(?0, 1, Y), @json_len(?0, 2)`
	if got := bodyString(r); got != want {
		t.Errorf("got  %s\nwant %s", got, want)
	}
}

// Object patterns require commas between fields too; confirm that already
// works (a missing comma there falls through to "expected '}'", which is a
// correct rejection even though the message differs from the array case).
func TestObjectPatternMissingCommaRejected(t *testing.T) {
	_, err := syntax.ParseStatement(`q(X, Y) :- p({a: X b: Y}).`)
	if err == nil {
		t.Fatal("expected a parse error for a missing comma in an object pattern, got nil")
	}
}

func TestFreshVarsDoNotCollideWithAnon(t *testing.T) {
	r := parseRule(t, `x(A) :- p(?, {k: A}), q(?).`)
	// All parser-generated variables must be distinct.
	seen := map[string]bool{}
	for _, a := range r.Body {
		for _, term := range a.Terms {
			s := term.String()
			if strings.HasPrefix(s, "?") {
				seen[s] = true
			}
		}
	}
	if len(seen) != 3 {
		t.Errorf("expected 3 distinct anonymous variables, got %v", seen)
	}
}
