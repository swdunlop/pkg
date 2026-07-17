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

func TestParseConstantLiterals(t *testing.T) {
	result, err := syntax.ParseStatement(`flag(true, false, null).`)
	if err != nil {
		t.Fatal(err)
	}
	r := result.(*syntax.Rule)
	if v, ok := r.Head.Terms[0].(datalog.Bool); !ok || bool(v) != true {
		t.Errorf("expected Bool(true), got %#v", r.Head.Terms[0])
	}
	if v, ok := r.Head.Terms[1].(datalog.Bool); !ok || bool(v) != false {
		t.Errorf("expected Bool(false), got %#v", r.Head.Terms[1])
	}
	if _, ok := r.Head.Terms[2].(datalog.Null); !ok {
		t.Errorf("expected Null{}, got %#v", r.Head.Terms[2])
	}
}

func TestParseConstantLiteralsInExpressions(t *testing.T) {
	// true/false/null must resolve to constants everywhere a bare
	// identifier reaches parseTerm, not just in atom argument position:
	// comparison/is constraints, aggregate terms, and query bodies.
	cases := []struct {
		src  string
		want datalog.Term
	}{
		{`ok(X) :- flag(X), X = true.`, datalog.Bool(true)},
		{`ok(X) :- flag(X), true = X.`, datalog.Bool(true)},
		{`bad(X) :- flag(X), X != false.`, datalog.Bool(false)},
		{`unset(X) :- missing(X), X = null.`, datalog.Null{}},
	}
	for _, c := range cases {
		result, err := syntax.ParseStatement(c.src)
		if err != nil {
			t.Fatalf("%q: %v", c.src, err)
		}
		r := result.(*syntax.Rule)
		cmp := r.Body[1]
		var got datalog.Term
		if cmp.Terms[0] == datalog.Variable("X") {
			got = cmp.Terms[1]
		} else {
			got = cmp.Terms[0]
		}
		if got != c.want {
			t.Errorf("%q: expected %#v, got %#v", c.src, c.want, got)
		}
	}
}

func TestParseConstantLiteralRoundTrip(t *testing.T) {
	// print -> reparse must be an identity for the new constant literals,
	// in head and body position alike.
	srcs := []string{
		`flag(true).`,
		`flag(false).`,
		`missing(null).`,
		`ok(X) :- flag(X), X = true.`,
	}
	for _, src := range srcs {
		result, err := syntax.ParseStatement(src)
		if err != nil {
			t.Fatalf("%q: %v", src, err)
		}
		r := result.(*syntax.Rule)
		printed := r.String()
		reresult, err := syntax.ParseStatement(printed)
		if err != nil {
			t.Fatalf("reparse of printed form %q (from %q) failed: %v", printed, src, err)
		}
		r2 := reresult.(*syntax.Rule)
		if r.String() != r2.String() {
			t.Errorf("round trip not stable: %q -> %q -> %q", src, printed, r2.String())
		}
	}
}

func TestParseIsRejectsReservedLiteralLHS(t *testing.T) {
	for _, src := range []string{
		`foo(R) :- true is 1 + 1.`,
		`foo(R) :- false is 1 + 1.`,
		`foo(R) :- null is 1 + 1.`,
	} {
		_, err := syntax.ParseStatement(src)
		if err == nil {
			t.Errorf("%q: expected error, got none", src)
		}
	}
}

func TestParseAggregateRejectsReservedLiteralResultVar(t *testing.T) {
	// The aggregate result variable is a binding position, like the LHS of
	// 'is': `true = count : ...` must be rejected the same way `true is ...`
	// is, so a bare reserved literal from source never becomes a (silently
	// discarded) result-variable name.
	for _, src := range []string{
		`h(R) :- true = count : b(X).`,
		`h(R) :- false = sum(X) : b(X).`,
		`h(R) :- null = max(X) : b(X).`,
		`h(true) :- true = count : b(X).`,
	} {
		if _, err := syntax.ParseStatement(src); err == nil {
			t.Errorf("%q: expected error, got none", src)
		}
	}
	// A real result variable and a `true = X` comparison body are unaffected.
	if _, err := syntax.ParseStatement(`h(R) :- R = count : b(X).`); err != nil {
		t.Errorf("normal aggregate rejected: %v", err)
	}
	stmt, err := syntax.ParseStatement(`h(X) :- b(X), true = X.`)
	if err != nil {
		t.Fatalf("`true = X` comparison rejected: %v", err)
	}
	r := stmt.(*syntax.Rule)
	last := r.Body[len(r.Body)-1]
	if last.Pred != "=" {
		t.Fatalf("expected '=' comparison, got %q", last.Pred)
	}
	if _, ok := last.Terms[0].(datalog.Bool); !ok {
		t.Errorf("expected Bool LHS in `true = X`, got %#v", last.Terms[0])
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

// TestParseStrayTokenAfterHeadNamesLegalContinuations guards the
// parseStatement fallthrough that used to report a bare "expected ':-',
// got ..." whenever the token after a rule/fact head atom was neither '.',
// '?', ',', nor ':-' -- misleadingly implying ':-' was the only legal
// continuation of a statement. A stray '?' landing mid-statement (as
// opposed to immediately after the head, where it is a valid single-atom
// query) hits this exact fallthrough via the unexpected 'b' that follows
// it, since parseAtomList/parseStatement report the token that actually
// broke the match. The message must name the full set of legal
// continuations at this point, not just ':-'.
func TestParseStrayTokenAfterHeadNamesLegalContinuations(t *testing.T) {
	_, err := syntax.ParseAll("a(1) b(2) ? c(3).")
	if err == nil {
		t.Fatal("expected a parse error for the stray '?' statement, got nil")
	}
	msg := err.Error()
	if strings.Contains(msg, `expected ':-', got`) {
		t.Errorf("error message still uses the misleading bare ':-' expectation: %v", msg)
	}
	for _, want := range []string{`'.'`, `'?'`, `','`, `':-'`} {
		if !strings.Contains(msg, want) {
			t.Errorf("expected the error to name %s as a legal continuation, got: %v", want, msg)
		}
	}
	if !strings.Contains(msg, `"b"`) {
		t.Errorf("expected the error to mention the offending token 'b', got: %v", msg)
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

// --- Bug 1: negated atoms must be rejected in head/fact position. ---

func TestParseRejectsNegatedFactHead(t *testing.T) {
	_, err := syntax.ParseStatement("not p(1).")
	if err == nil {
		t.Fatal("expected an error for a negated fact head, got nil")
	}
	if !strings.Contains(err.Error(), "negated atoms are not allowed") {
		t.Errorf("expected a 'negated atoms' error, got: %v", err)
	}
}

func TestParseRejectsNegatedRuleHead(t *testing.T) {
	_, err := syntax.ParseStatement("h(X) :- not p(X).")
	if err != nil {
		t.Fatalf("negation in the body should still be legal, got: %v", err)
	}

	_, err = syntax.ParseStatement("not h(X) :- p(X).")
	if err == nil {
		t.Fatal("expected an error for a negated rule head, got nil")
	}
	if !strings.Contains(err.Error(), "negated atoms are not allowed") {
		t.Errorf("expected a 'negated atoms' error, got: %v", err)
	}
}

// --- Bug 2: comparisons, is-expressions, and @builtins must be rejected
// as heads, tightening head position to a plain predicate atom. ---

func TestParseRejectsComparisonHead(t *testing.T) {
	for _, src := range []string{"1 < 2.", "A > B :- val(A), val(B)."} {
		_, err := syntax.ParseStatement(src)
		if err == nil {
			t.Fatalf("expected an error for comparison head %q, got nil", src)
		}
		if !strings.Contains(err.Error(), "comparisons are not allowed") {
			t.Errorf("%q: expected a 'comparisons are not allowed' error, got: %v", src, err)
		}
	}
	// Comparisons remain legal in body position.
	_, err := syntax.ParseStatement("bigger(X, Y) :- val(X, A), val(Y, B), A > B.")
	if err != nil {
		t.Fatalf("comparison in the body should still be legal, got: %v", err)
	}
}

func TestParseRejectsIsHead(t *testing.T) {
	_, err := syntax.ParseStatement("X is 1 + 2.")
	if err == nil {
		t.Fatal("expected an error for an 'is' fact head, got nil")
	}
	if !strings.Contains(err.Error(), "'is' expressions are not allowed") {
		t.Errorf("expected an \"'is' expressions\" error, got: %v", err)
	}

	_, err = syntax.ParseStatement("X is Y + 1 :- val(Y).")
	if err == nil {
		t.Fatal("expected an error for an 'is' rule head, got nil")
	}
	if !strings.Contains(err.Error(), "'is' expressions are not allowed") {
		t.Errorf("expected an \"'is' expressions\" error, got: %v", err)
	}

	// is remains legal in body position.
	_, err = syntax.ParseStatement("double(X, Y) :- val(X), Y is X * 2.")
	if err != nil {
		t.Fatalf("'is' in the body should still be legal, got: %v", err)
	}
}

func TestParseRejectsBuiltinHead(t *testing.T) {
	_, err := syntax.ParseStatement(`@contains(X, "hello").`)
	if err == nil {
		t.Fatal("expected an error for a @builtin fact head, got nil")
	}
	if !strings.Contains(err.Error(), "not allowed as a rule or fact head") {
		t.Errorf("expected a builtin-head error, got: %v", err)
	}

	// @builtins remain legal in body position.
	_, err = syntax.ParseStatement(`has(X) :- msg(X), @contains(X, "hello").`)
	if err != nil {
		t.Fatalf("@builtin in the body should still be legal, got: %v", err)
	}
}

// --- Bug 3: unterminated strings must error at the opening quote instead
// of silently pairing with a later, unrelated quote. ---

func TestParseUnterminatedStringErrors(t *testing.T) {
	_, err := syntax.ParseStatement(`p("unterminated).`)
	if err == nil {
		t.Fatal("expected an error for an unterminated string, got nil")
	}
	if !strings.Contains(err.Error(), "unterminated string") {
		t.Errorf("expected an 'unterminated string' error, got: %v", err)
	}
}

func TestParseUnterminatedStringDoesNotPairAcrossStatements(t *testing.T) {
	// A stray, unclosed quote must not consume the rest of the input
	// (including a later statement's own closing quote) as one giant
	// string constant.
	_, err := syntax.ParseAll("p(\").\np(\").\n")
	if err == nil {
		t.Fatal("expected an error for a stray unterminated quote, got nil")
	}
	if !strings.Contains(err.Error(), "unterminated string") {
		t.Errorf("expected an 'unterminated string' error, got: %v", err)
	}
	if !strings.Contains(err.Error(), "line 1") {
		t.Errorf("expected the error to be reported at the opening quote on line 1, got: %v", err)
	}

	// A well-formed string still parses normally.
	result, err := syntax.ParseStatement(`p("ok").`)
	if err != nil {
		t.Fatalf("expected a well-formed string to parse cleanly, got: %v", err)
	}
	r := result.(*syntax.Rule)
	if s, ok := r.Head.Terms[0].(datalog.String); !ok || string(s) != "ok" {
		t.Errorf("expected String(ok), got %v", r.Head.Terms[0])
	}
}

// --- Bug 4: unspaced '-' must lex as subtraction when it follows a token
// that can end an expression, while still allowing negative literals. ---

func TestParseUnspacedSubtraction(t *testing.T) {
	result, err := syntax.ParseStatement("diff(Y) :- val(X), Y is 1-2.")
	if err != nil {
		t.Fatalf("expected unspaced subtraction to parse, got: %v", err)
	}
	r := result.(*syntax.Rule)
	isAtom := r.Body[1]
	bin, ok := isAtom.Expr.(syntax.BinExpr)
	if !ok || bin.Op != "-" {
		t.Fatalf("expected a '-' BinExpr, got %#v", isAtom.Expr)
	}

	// Spaced subtraction must keep working too.
	result, err = syntax.ParseStatement("diff(Y) :- val(X), Y is 1 - 2.")
	if err != nil {
		t.Fatalf("expected spaced subtraction to parse, got: %v", err)
	}
	r = result.(*syntax.Rule)
	if _, ok := r.Body[1].Expr.(syntax.BinExpr); !ok {
		t.Fatalf("expected a BinExpr, got %#v", r.Body[1].Expr)
	}

	// Subtracting a variable, unspaced, must also work (not misread as a
	// negative-number token).
	result, err = syntax.ParseStatement("diff(Y) :- val(X), Y is X-1.")
	if err != nil {
		t.Fatalf("expected unspaced variable subtraction to parse, got: %v", err)
	}
}

func TestParseNegativeLiteralStillParses(t *testing.T) {
	result, err := syntax.ParseStatement("p(-5).")
	if err != nil {
		t.Fatalf("expected a negative literal fact to parse, got: %v", err)
	}
	r := result.(*syntax.Rule)
	if v, ok := r.Head.Terms[0].(datalog.Integer); !ok || int64(v) != -5 {
		t.Errorf("expected Integer(-5), got %v", r.Head.Terms[0])
	}

	// A negative literal on the right of 'is' must also still parse (unary
	// minus in an expression's primary position).
	result, err = syntax.ParseStatement("val(X) :- X is -5.")
	if err != nil {
		t.Fatalf("expected 'is -5' to parse, got: %v", err)
	}
	r = result.(*syntax.Rule)
	term, ok := r.Body[0].Expr.(syntax.TermExpr)
	if !ok {
		t.Fatalf("expected a TermExpr, got %#v", r.Body[0].Expr)
	}
	if v, ok := term.Term.(datalog.Integer); !ok || int64(v) != -5 {
		t.Errorf("expected Integer(-5), got %v", term.Term)
	}
}

// --- Bug 5: freshVar must never collide with an explicit ?N written
// anywhere in the same statement, regardless of lexical order. ---

func TestParseFreshVarDoesNotCollideWithExplicitAnonName(t *testing.T) {
	result, err := syntax.ParseStatement("p(_, ?0)?")
	if err != nil {
		t.Fatal(err)
	}
	q := result.(*syntax.Query)
	fresh := q.Body[0].Terms[0].(datalog.Variable)
	explicit := q.Body[0].Terms[1].(datalog.Variable)
	if fresh == explicit {
		t.Fatalf("fresh variable for '_' collided with explicit %q", explicit)
	}
	if string(explicit) != "?0" {
		t.Fatalf("expected the explicit variable to remain ?0, got %q", explicit)
	}

	// The explicit name can also appear before the anonymous position.
	result, err = syntax.ParseStatement("p(?0, _)?")
	if err != nil {
		t.Fatal(err)
	}
	q = result.(*syntax.Query)
	explicit = q.Body[0].Terms[0].(datalog.Variable)
	fresh = q.Body[0].Terms[1].(datalog.Variable)
	if fresh == explicit {
		t.Fatalf("fresh variable for '_' collided with explicit %q", explicit)
	}

	// ?N-looking text inside a string literal must not be mistaken for an
	// explicit variable name reservation.
	result, err = syntax.ParseStatement(`p("has ?0 in it", _)?`)
	if err != nil {
		t.Fatal(err)
	}
	q = result.(*syntax.Query)
	if _, ok := q.Body[0].Terms[1].(datalog.Variable); !ok {
		t.Fatalf("expected second term to be a Variable, got %v", q.Body[0].Terms[1])
	}
}

// --- Bug 6: aggregate parse errors must surface from parseAggregateBody,
// not be swallowed and re-reported as misleading comparison-body errors. ---

func TestParseAggregateErrorsSurface(t *testing.T) {
	// Each of these genuinely matches the aggregate shape (Var = aggkind)
	// but is malformed. The real defect must surface, not a re-parse of
	// Var = aggkind(...) as a comparison body that dies at a different spot.
	cases := []struct {
		src        string
		wantSubstr string
	}{
		// count takes no term; the '(' after count is where ':' was expected.
		{`p(R) :- R = count(X) : q(X).`, `expected ':'`},
		// count with parens is the same defect.
		{`p(R) :- R = count() : q(X).`, `expected ':'`},
		// empty aggregate body: the ':' was consumed, then the '.' is where
		// a predicate name was expected.
		{`p(R) :- R = sum(X) : .`, `expected predicate name`},
	}
	for _, c := range cases {
		_, err := syntax.ParseStatement(c.src)
		if err == nil {
			t.Errorf("%q: expected a parse error, got nil", c.src)
			continue
		}
		if !strings.Contains(err.Error(), c.wantSubstr) {
			t.Errorf("%q: expected error containing %q, got: %v", c.src, c.wantSubstr, err)
		}
		// The error must point at the real defect, not at the '(' of the
		// aggkind as the old "expected '.', got '('" misdirection did for
		// the count cases. The swallowed-error bug reported column 18
		// ("expected '.', got '('") for the count cases; the real error
		// also lands at column 18 but says "expected ':'" — so we only
		// assert the wording changed, not the column, since both are at
		// the aggkind. The empty-body case must land at the '.', not the
		// aggkind.
		if strings.Contains(err.Error(), `expected '.', got "("`) {
			t.Errorf("%q: error regressed to the swallowed comparison-body misdirection: %v", c.src, err)
		}
	}
}

func TestParseAggregateValidStillParses(t *testing.T) {
	// The fix must not regress the happy path or the non-aggregate
	// comparison fallback (Var = non-aggkind).
	valid := `total(P, T) :- T = sum(S) : score(P, S).`
	result, err := syntax.ParseStatement(valid)
	if err != nil {
		t.Fatalf("valid aggregate should parse, got: %v", err)
	}
	if _, ok := result.(*syntax.AggregateRule); !ok {
		t.Fatalf("expected *AggregateRule, got %T", result)
	}

	// A non-aggregate Var = ident comparison in the body must still fall
	// through to the comparison-body parse, not be claimed by the
	// aggregate path.
	rule, err := syntax.ParseStatement(`p(X) :- q(X), X = X.`)
	if err != nil {
		t.Fatalf("non-aggregate comparison should parse, got: %v", err)
	}
	if _, ok := rule.(*syntax.Rule); !ok {
		t.Fatalf("expected *Rule, got %T", rule)
	}
}

// --- Bug: a statement terminator ('.' or '?') must reset the lexer's
// sign-disambiguation state, so a following '-' at the start of the next
// statement is always read as a numeric sign, never leaked-in subtraction. ---

func TestParseMinusAfterQueryTerminatorIsSign(t *testing.T) {
	// This must parse exactly like the standalone form below: '-2' is a
	// negative literal, not "(garbage) - 2".
	if _, err := syntax.ParseAll(`-2 < X?`); err != nil {
		t.Fatalf("standalone '-2 < X?' should parse, got: %v", err)
	}
	rs, err := syntax.ParseAll(`p(1)? -2 < X?`)
	if err != nil {
		t.Fatalf("'-2 < X?' after another query's '?' terminator should parse, got: %v", err)
	}
	if len(rs.Queries) != 2 {
		t.Fatalf("expected 2 queries, got %d: %+v", len(rs.Queries), rs)
	}
	second := rs.Queries[1]
	if len(second.Body) != 1 || second.Body[0].Pred != "<" {
		t.Fatalf("expected second query body to be a single '<' comparison, got %+v", second.Body)
	}
	lhs, ok := second.Body[0].Terms[0].(datalog.Integer)
	if !ok || int64(lhs) != -2 {
		t.Errorf("expected Integer(-2) on the left of '<', got %#v", second.Body[0].Terms[0])
	}
}

func TestParseMinusAfterFactTerminatorIsSign(t *testing.T) {
	// Same bug, but for a fact's '.' terminator instead of a query's '?'.
	rs, err := syntax.ParseAll(`p(1). q(X) :- X is -2 + 1.`)
	if err != nil {
		t.Fatalf("negative literal after a fact's '.' terminator should parse, got: %v", err)
	}
	if len(rs.Rules) != 2 {
		t.Fatalf("expected 2 rules, got %d: %+v", len(rs.Rules), rs)
	}
}

// --- Bug: String.String() prints the full %q escape set (\r, \x00, \uNNNN,
// \a, \b, \f, \v, ...), but the parser only decoded a small subset and
// silently kept unrecognized escapes as literal backslash-sequences,
// corrupting any print -> re-parse round trip of a value containing one of
// the escapes it didn't know. ---

func TestStringEscapeRoundTrip(t *testing.T) {
	cases := []string{
		"\r",
		"\x00",
		"\a\b\f\v",
		"line1\nline2\ttabbed",
		"\x7f",       // DEL, not printable -> \x7f
		" ",          // line separator, not printable -> \u escape
		"\U0001F600", // astral-plane rune -> \U escape
		"mixed \r\n \x01 é end",
		"",
		"plain ascii, no escapes needed",
	}
	for _, orig := range cases {
		rule := syntax.Rule{Head: syntax.Atom{Pred: "p", Terms: []datalog.Term{datalog.String(orig)}}}
		text := rule.String()
		res, err := syntax.ParseStatement(text)
		if err != nil {
			t.Errorf("%q: printed as %s, reparse failed: %v", orig, text, err)
			continue
		}
		r, ok := res.(*syntax.Rule)
		if !ok {
			t.Errorf("%q: expected *Rule, got %T", orig, res)
			continue
		}
		got, ok := r.Head.Terms[0].(datalog.String)
		if !ok {
			t.Errorf("%q: expected datalog.String, got %T", orig, r.Head.Terms[0])
			continue
		}
		if string(got) != orig {
			t.Errorf("round trip changed value: printed %s, got %q, want %q", text, string(got), orig)
		}
	}
}

func TestStringEscapeDecoding(t *testing.T) {
	cases := []struct {
		src  string
		want string
	}{
		{`"\a"`, "\a"},
		{`"\b"`, "\b"},
		{`"\f"`, "\f"},
		{`"\v"`, "\v"},
		{`"\r"`, "\r"},
		{`"\x00"`, "\x00"},
		{`"\x41"`, "A"},
		{`"A"`, "A"},
		{`"\U00000041"`, "A"},
		{`" "`, " "},
	}
	for _, c := range cases {
		res, err := syntax.ParseStatement(`p(` + c.src + `).`)
		if err != nil {
			t.Errorf("%s: unexpected parse error: %v", c.src, err)
			continue
		}
		r := res.(*syntax.Rule)
		got, ok := r.Head.Terms[0].(datalog.String)
		if !ok || string(got) != c.want {
			t.Errorf("%s: got %#v, want String(%q)", c.src, r.Head.Terms[0], c.want)
		}
	}
}

func TestStringEscapeRejectsMalformed(t *testing.T) {
	cases := []string{
		`"\x1"`,        // too few hex digits
		`"\xzz"`,       // not hex digits
		`"\u12"`,       // too few hex digits
		`"\q"`,         // unknown escape
		`"\U00110000"`, // eight hex digits, but exceeds the maximum unicode code point
	}
	for _, src := range cases {
		_, err := syntax.ParseStatement(`p(` + src + `).`)
		if err == nil {
			t.Errorf("%s: expected a parse error for a malformed escape, got nil", src)
		}
	}
}
