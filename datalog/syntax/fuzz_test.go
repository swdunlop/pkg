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
//
// doc pins the same fidelity for '%%' doc-comment attachment: a doc that
// silently vanished, migrated to a different statement, or was mangled by
// String()'s "%% "-prefix rendering across a print->reparse trip would
// otherwise go undetected the same way a corrupted string literal would.
type printedStmt struct {
	src  string
	strs []string
	doc  string
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
		out = append(out, printedStmt{src: r.String(), strs: strs, doc: r.Doc})
	}
	for _, ar := range rs.AggRules {
		strs := collectAtomStrings([]syntax.Atom{ar.Head})
		if s, ok := ar.AggTerm.(datalog.String); ok {
			strs = append(strs, string(s))
		}
		strs = append(strs, collectAtomStrings(ar.Body)...)
		out = append(out, printedStmt{src: ar.String(), strs: strs, doc: ar.Doc})
	}
	for _, q := range rs.Queries {
		out = append(out, printedStmt{src: q.String(), strs: collectAtomStrings(q.Body), doc: q.Doc})
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

		// true/false/null constant literals
		`flag(true).`,
		`flag(false).`,
		`missing(null).`,
		`tuple(true, false, null).`,
		`ok(X) :- flag(X), X = true.`,
		`ok(X) :- flag(X), true = X.`,
		`bad(X) :- flag(X), X != false.`,
		`unset(X) :- missing(X), X = null.`,
		`g(X) :- val(X), X is true.`,
		`total(P, T) :- T = sum(S) : flag(true), score(P, S).`,
		`flag(true)?`,
		`missing(null)?`,

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

		// '%%' doc comments: single-line, on facts/rules/aggregate
		// rules/queries.
		"%% a single-line doc\nparent(\"tom\", \"bob\").",
		"%% derives ancestor from parent\nancestor(X, Y) :- parent(X, Y).",
		"%% counts distinct ports per source\nc(X, N) :- N = count : d(X).",
		"%% who is tom's ancestor\nancestor(\"tom\", X)?",

		// multi-line doc blocks.
		"%% line one\n%% line two\n%% line three\nport_scan(Src, Dst, PortCount) :- conn(Src, Dst, PortCount).",

		// '%%' immediately adjacent to a plain '%' comment: detaches.
		"% not a doc\n%% looks like a doc\nfact(1).",
		"%% looks like a doc\n% not a doc\nfact(1).",

		// blank-line detachment.
		"%% detached by blank line\n\nfact(1).",
		"%% attached\nfact(1).\n\n%% also attached, separate statement\nfact(2).",

		// docs containing literal '%' characters (including a second
		// '%%' inside the text).
		"%% 50%% discount, or use a %-sign\nfact(1).",

		// unicode content and combining/astral characters.
		"%% café naïve résumé \U0001F600\nfact(1).",

		// a bare (empty) '%%' line, and mixed blank+empty-doc-line blocks.
		"%%\nfact(1).",
		"%% a\n%%\n%% b\nfact(1).",

		// trailing/orphaned doc block with no statement after it.
		"%% orphaned, nothing follows\n",

		// doc immediately preceding an aggregate rule with min/max/sum.
		"%% total transferred per owner\ntotal(P, T) :- T = sum(S) : score(P, S).",

		// combined multi-statement program mixing documented and
		// undocumented statements, so attachment can't leak across
		// statement boundaries.
		"parent(\"tom\", \"bob\").\n%% ancestor via direct parent\nancestor(X, Y) :- parent(X, Y).\nancestor(X, Y) :- parent(X, Z), ancestor(Z, Y).\n%% who are tom's descendants\nancestor(\"tom\", X)?",
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
			// A '%%' doc comment must survive the same trip unchanged and
			// attached to the same (now sole) statement -- a doc that
			// vanishes, mutates, or silently migrates to a different
			// statement on reparse is exactly the class of silent-wrong-
			// result bug this fuzzer exists to catch (see
			// doc/features/predicate-docs.md, "Attachment ambiguity").
			if len(ps2s) == 1 && ps.doc != ps2s[0].doc {
				t.Fatalf("doc comment changed across print->reparse: original %q, reprinted %q, want doc %q, got %q\noriginal input: %q",
					input, ps.src, ps.doc, ps2s[0].doc, input)
			}
			// Re-printing again should not itself fail to reprint or panic,
			// and should produce a re-parseable form (with unchanged string
			// values and doc) as well (checks stability of the desugared/
			// normalized form under repeated print/parse cycles).
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
				if len(ps3s) == 1 && ps2.doc != ps3s[0].doc {
					t.Fatalf("doc comment changed across second print->reparse: first reprint %q, second reprint %q, want doc %q, got %q\noriginal input: %q",
						ps2.src, ps3s[0].src, ps2.doc, ps3s[0].doc, input)
				}
			}
		}
	})
}

// docStmt is the subset of a parsed Ruleset's statement info a doc-
// attachment test cares about: how many statements landed in each bucket,
// and each rule's Doc (the only bucket exercised below -- AggregateRule and
// Query attachment share the exact same lexer/parser mechanism as Rule, so
// they are covered by the "docs on facts/rules/aggregate rules/queries"
// fuzz seeds above rather than duplicated here statement-type by
// statement-type).
func docOf(t *testing.T, input string) (doc string, warnings []string) {
	t.Helper()
	rs, err := syntax.ParseAll(input)
	if err != nil {
		t.Fatalf("parse %q: %v", input, err)
	}
	if len(rs.Rules) != 1 {
		t.Fatalf("parse %q: want exactly one rule, got %d rules, %d agg rules, %d queries",
			input, len(rs.Rules), len(rs.AggRules), len(rs.Queries))
	}
	return rs.Rules[0].Doc, rs.Warnings
}

// TestDocAttachment pins the attachment-stability property called out in
// doc/features/predicate-docs.md's "Attachment ambiguity" risk: a '%%'
// block directly above a statement attaches; a blank line or a plain '%'
// comment between the block and the statement detaches it (with a warning,
// not an error); and the result is stable under print->reparse. The fuzzer
// above checks the print->reparse half of this property on arbitrary input;
// this test pins the specific attach/detach boundary cases by hand so a
// regression here fails with a small, readable diff instead of only a fuzz
// counterexample.
func TestDocAttachment(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		wantDoc     string
		wantWarning bool
	}{
		{
			name:    "directly above attaches",
			input:   "%% doc\nfact(1).",
			wantDoc: "doc",
		},
		{
			name:    "multiple contiguous lines attach as one block",
			input:   "%% line one\n%% line two\nfact(1).",
			wantDoc: "line one\nline two",
		},
		{
			name:        "blank line between block and statement detaches",
			input:       "%% doc\n\nfact(1).",
			wantDoc:     "",
			wantWarning: true,
		},
		{
			name:        "plain % comment between block and statement detaches",
			input:       "%% doc\n% just a comment\nfact(1).",
			wantDoc:     "",
			wantWarning: true,
		},
		{
			name:        "plain % comment then %% block: only the %% block (contiguous with the statement) attaches",
			input:       "% just a comment\n%% doc\nfact(1).",
			wantDoc:     "doc",
			wantWarning: false,
		},
		{
			name:    "trailing spaces on the doc line before the newline do not create a blank-line break",
			input:   "%% doc   \nfact(1).",
			wantDoc: "doc   ",
		},
		{
			name:        "two blocks separated by a blank line: only the nearer block attaches, the far one warns",
			input:       "%% far, detached\n\n%% near, attached\nfact(1).",
			wantDoc:     "near, attached",
			wantWarning: true,
		},
		{
			name:    "plain % comment alone (no %%) is not a doc and not a warning",
			input:   "% just a comment\nfact(1).",
			wantDoc: "",
		},
		{
			name:    "ordinary %% empty comment with no content is not distinguished from no doc",
			input:   "%%\nfact(1).",
			wantDoc: "",
		},
		{
			name:    "doc containing a literal % character",
			input:   "%% 100% done\nfact(1).",
			wantDoc: "100% done",
		},
		{
			name:    "doc containing unicode",
			input:   "%% café \U0001F600\nfact(1).",
			wantDoc: "café \U0001F600",
		},
		{
			// CRLF-terminated input must not bake a stray carriage return
			// into the doc content, or it would persist through every
			// render/reparse cycle and leak into describe/provenance output.
			name:    "CRLF line endings do not leave a stray carriage return in the doc",
			input:   "%% doc\r\nfact(1).\r\n",
			wantDoc: "doc",
		},
		{
			name:    "CRLF multi-line doc strips each carriage return",
			input:   "%% line one\r\n%% line two\r\nfact(1).\r\n",
			wantDoc: "line one\nline two",
		},
		{
			// A run of trailing '\r' (e.g. a bare '\r' before a CRLF: "\r\r\n")
			// must be stripped WHOLE, not one at a time -- a doc line ending in
			// '\r' cannot survive print->reparse (writeDoc emits "...\r\n",
			// which the next parse reads as a CRLF terminator), so the canonical
			// form must never contain a trailing '\r'. Found by FuzzParseAll on
			// "%%\r\r\nA()."; here the same shape with content ("doc") pins it
			// readably.
			name:    "run of trailing carriage returns stripped whole",
			input:   "%% doc\r\r\nfact(1).",
			wantDoc: "doc",
		},
		{
			name:    "doc that is only trailing carriage returns collapses to empty",
			input:   "%%\r\r\nfact(1).",
			wantDoc: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			doc, warnings := docOf(t, tt.input)
			if doc != tt.wantDoc {
				t.Errorf("Doc = %q, want %q", doc, tt.wantDoc)
			}
			gotWarning := len(warnings) > 0
			if gotWarning != tt.wantWarning {
				t.Errorf("warnings = %v (non-empty=%v), want non-empty=%v", warnings, gotWarning, tt.wantWarning)
			}
		})
	}
}

// TestDocAttachmentStableUnderReparse pins the print->reparse half of the
// attachment-stability property directly (in addition to the fuzzer's
// broader, randomized coverage of the same property): parsing, printing,
// and reparsing a documented statement must reproduce the identical Doc and
// an identical second printing, for both the attached and the (blank-line)
// detached cases.
func TestDocAttachmentStableUnderReparse(t *testing.T) {
	inputs := []string{
		"%% doc\nfact(1).",
		"%% doc\n\nfact(1).", // detaches on first parse; Doc="" must stay stable
		"%% line one\n%% line two\nfact(1).",
		"%% café \U0001F600\nfact(1).",
		"%% 100% done\nfact(1).",
	}
	for _, in := range inputs {
		t.Run(in, func(t *testing.T) {
			rs1, err := syntax.ParseAll(in)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			printed1 := rs1.Rules[0].String()

			rs2, err := syntax.ParseAll(printed1)
			if err != nil {
				t.Fatalf("reparse of %q: %v", printed1, err)
			}
			if rs2.Rules[0].Doc != rs1.Rules[0].Doc {
				t.Fatalf("Doc not stable across reparse: %q -> %q", rs1.Rules[0].Doc, rs2.Rules[0].Doc)
			}
			printed2 := rs2.Rules[0].String()
			if printed1 != printed2 {
				t.Fatalf("printed form not stable across reparse: %q -> %q", printed1, printed2)
			}
		})
	}
}
