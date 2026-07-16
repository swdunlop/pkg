package syntax_test

import (
	"reflect"
	"testing"

	"swdunlop.dev/pkg/datalog"
	"swdunlop.dev/pkg/datalog/syntax"
)

// renderRuleset re-renders every statement in a parsed Ruleset back to its
// canonical textual form, in the same relative order the parser encountered
// them (rules and aggregate rules interleave in rs.Rules/rs.AggRules by
// parse order isn't preserved separately -- we simply render each bucket in
// its own slice order, which is enough to check parse->print->parse
// round-tripping without depending on cross-bucket interleaving).
func renderRuleset(rs syntax.Ruleset) []string {
	var out []string
	for _, r := range rs.Rules {
		out = append(out, r.String())
	}
	for _, ar := range rs.AggRules {
		out = append(out, ar.String())
	}
	for _, q := range rs.Queries {
		out = append(out, q.String())
	}
	return out
}

// printedStmt pairs a statement's re-printed textual form with the ordered
// list of string-literal term values ("value fidelity") it carries, so a
// round trip can check not just that the printed form re-parses (as
// renderRuleset's callers already do), but that the values themselves
// survived the print->reparse trip unchanged. Without this, a bug in
// String()'s escaping (or the lexer's decoding of it) that corrupts a
// string's contents but still produces syntactically valid, re-parseable
// output would go undetected by parse->print->parse alone -- the fuzzer only
// ever asserted re-parseability, not that the re-parsed value matched.
type printedStmt struct {
	src  string
	strs []string
}

// collectAtomStrings gathers every datalog.String term appearing directly in
// atoms, in order. Composite/pattern terms are not walked: the parser
// desugars destructuring patterns into fresh ?N variables and getter atoms
// before Terms is populated (see renderRuleset's doc comment), so top-level
// atom terms are the only place a literal string can appear here.
func collectAtomStrings(atoms []syntax.Atom) []string {
	var out []string
	for _, a := range atoms {
		for _, t := range a.Terms {
			if s, ok := t.(datalog.String); ok {
				out = append(out, string(s))
			}
		}
	}
	return out
}

func renderRulesetWithStrings(rs syntax.Ruleset) []printedStmt {
	var out []printedStmt
	for _, r := range rs.Rules {
		strs := collectAtomStrings([]syntax.Atom{r.Head})
		strs = append(strs, collectAtomStrings(r.Body)...)
		out = append(out, printedStmt{src: r.String(), strs: strs})
	}
	for _, ar := range rs.AggRules {
		strs := collectAtomStrings([]syntax.Atom{ar.Head})
		if s, ok := ar.AggTerm.(datalog.String); ok {
			strs = append(strs, string(s))
		}
		strs = append(strs, collectAtomStrings(ar.Body)...)
		out = append(out, printedStmt{src: ar.String(), strs: strs})
	}
	for _, q := range rs.Queries {
		out = append(out, printedStmt{src: q.String(), strs: collectAtomStrings(q.Body)})
	}
	return out
}

// FuzzParseAll checks that syntax.ParseAll never panics on arbitrary input,
// and that whenever it succeeds, every re-printed statement (Rule.String,
// AggregateRule.String, Query.String) is itself valid Datalog that
// re-parses without error -- a parse -> print -> parse round trip. We only
// assert re-parseability (not byte-for-byte equality of the two parses'
// re-prints) because printing normalizes whitespace and desugars patterns
// into fresh ?N variables/getter atoms, so two syntactically different but
// semantically equivalent inputs need not print identically.
func FuzzParseAll(f *testing.F) {
	seeds := []string{
		// facts
		`parent("tom", "bob").`,
		`val(42, -5).`,
		`val(3.14).`,
		`color(red).`,
		`p(-5).`,
		`p(_, _).`,

		// rules
		`ancestor(X, Y) :- parent(X, Y).`,
		`ancestor(X, Y) :- parent(X, Z), ancestor(Z, Y).`,
		`grandparent(X, Z) :- parent(X, Y), parent(Y, Z).`,

		// negation
		`lonely(X) :- person(X), not friend(X, ?).`,
		`h(X) :- not p(X).`,

		// is / arithmetic
		`double(X, Y) :- val(X), Y is X * 2.`,
		`diff(Y) :- val(X), Y is 1-2.`,
		`diff(Y) :- val(X), Y is 1 - 2.`,
		`diff(Y) :- val(X), Y is X-1.`,
		`val(X) :- X is -5.`,
		`r(X) :- val(X), _ is X * 2.`,
		`m(X, Y) :- val(X, Y), Y is (X + 1) * 2 / 3.`,
		`m(X, Y) :- val(X, Y), Y is X mod 2.`,

		// aggregates
		`total(P, T) :- T = sum(S) : score(P, S).`,
		`total(S, T) :- T = sum(V) : val(V).`,
		`c(X, N) :- N = count : d(X).`,
		`mx(X, N) :- N = max(V) : d(X, V).`,
		`mn(X, N) :- N = min(V) : d(X, V).`,

		// builtins
		`has(X) :- msg(X), @contains(X, "hello").`,

		// comparisons
		`bigger(X, Y) :- val(X, A), val(Y, B), A > B.`,
		`q(X) :- val(X), X != 0, not excluded(X).`,

		// strings with escapes
		`s("a\nb\tc\\d\"e").`,

		// queries
		`parent(X, "bob")?`,
		`p(_, ?0)?`,
		`p(_, _)?`,
		`s(_, ?, _)?`,
		`ancestor("tom", X)?`,
		`a(X), b(X)?`,

		// patterns / destructuring
		`suspicious(P) :- process(P, {name: Name, pid: Pid}), Pid > 1000.`,
		`active(P) :- process(P, {"status": "active", name: N}).`,
		`x(P) :- ev(P, {proc: {name: N}}).`,
		`loop(P) :- conn(P, {src: X, dst: X}).`,
		`arr(P) :- xs(P, [A, B, C]).`,
		`arr(P) :- xs(P, [A, B | Rest]).`,
		`g(X) :- obj(O), X is O.`,

		// combined program
		`
			parent("tom", "bob").
			parent("bob", "ann").
			ancestor(X, Y) :- parent(X, Y).
			ancestor(X, Y) :- parent(X, Z), ancestor(Z, Y).
			ancestor("tom", X)?
		`,
		`
			parent("tom", "bob").
			ancestor(X, Y) :- parent(X, Y).
			has(X) :- msg(X), @contains(X, "hello").
			total(S, T) :- T = sum(V) : val(V).
			q(X) :- val(X), X != 0, not excluded(X).
			ancestor("tom", X)?
		`,

		// known-nasty seeds from recent parser fixes
		`a(1). ; b(2).`,
		`#junk`,
		`not p(1).`,
		`1 < 2.`,
		`p(").`,
		"p(\"\n",
		`X is 1-2`,
		`p(-5)`,
		`p(_, ?0)?`,
		`a(1) ! b(2).`,
		`a(1) @ b(2).`,
		"a(1).\nb(2).\nc(3,   ].\n",
		"p(\").\np(\").\n",
		`X is 1 + 2.`,
		`X is Y + 1 :- val(Y).`,
		`@contains(X, "hello").`,
		`not h(X) :- p(X).`,
		`A > B :- val(A), val(B).`,

		// misc edge cases
		``,
		` `,
		`.`,
		`?`,
		`(`,
		`)`,
		`"`,
		`p(`,
		`p()`,
		`p(,)`,
		`p(1,).`,
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, input string) {
		rs, err := syntax.ParseAll(input)
		if err != nil {
			return
		}

		// Every re-printed statement must itself be valid, re-parseable
		// Datalog: parse -> print -> parse must not fail, and every
		// string-literal term it carries must survive the trip unchanged
		// (value fidelity, not just re-parseability).
		for _, ps := range renderRulesetWithStrings(rs) {
			rs2, err := syntax.ParseAll(ps.src)
			if err != nil {
				t.Fatalf("re-parse of printed statement failed: %q: %v\noriginal input: %q", ps.src, err, input)
			}
			ps2s := renderRulesetWithStrings(rs2)
			// ps.src is the printed form of exactly one statement, so
			// re-parsing it should yield exactly one statement back; if it
			// doesn't, the re-parseability check above already caught
			// anything fatal, so just skip the value comparison rather than
			// guessing which of several statements to compare against.
			if len(ps2s) == 1 && !reflect.DeepEqual(ps.strs, ps2s[0].strs) {
				t.Fatalf("string-literal values changed across print->reparse: original %q, reprinted %q, want strings %#v, got %#v\noriginal input: %q",
					input, ps.src, ps.strs, ps2s[0].strs, input)
			}
			// Re-printing again should not itself fail to reprint or panic,
			// and should produce a re-parseable form (with unchanged string
			// values) as well (checks stability of the desugared/normalized
			// form under repeated print/parse cycles).
			for _, ps2 := range ps2s {
				rs3, err := syntax.ParseAll(ps2.src)
				if err != nil {
					t.Fatalf("second re-parse failed: %q: %v\nfirst reprint: %q\noriginal input: %q", ps2.src, err, ps.src, input)
				}
				ps3s := renderRulesetWithStrings(rs3)
				if len(ps3s) == 1 && !reflect.DeepEqual(ps2.strs, ps3s[0].strs) {
					t.Fatalf("string-literal values changed across second print->reparse: first reprint %q, second reprint %q, want strings %#v, got %#v\noriginal input: %q",
						ps2.src, ps3s[0].src, ps2.strs, ps3s[0].strs, input)
				}
			}
		}
	})
}
