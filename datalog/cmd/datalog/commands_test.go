package main

import (
	"bytes"
	"regexp"
	"strings"
	"testing"

	"swdunlop.dev/pkg/datalog/syntax"
)

// datalogExampleLine matches one of cmdHelp's "Datalog statements:" example
// lines: two-or-more leading spaces, the Datalog statement itself, then
// two-or-more spaces followed by a plain-English description. This mirrors
// the fixed-width layout cmdHelp prints (fmt.Fprintln with hardcoded
// padding), so it can pull just the statement portion back out for parsing.
var datalogExampleLine = regexp.MustCompile(`^  (\S.*?)\s{2,}\S.*$`)

// helpExamplesNotFullStatements lists example lines that are intentionally
// NOT complete, independently-parseable Datalog statements — e.g. "not
// parent(?, X)" illustrates negation as it appears inside a rule body, not
// a standalone statement (the parser correctly rejects a negated atom as a
// rule/fact head). These are excluded from the parseability check below;
// every other line under "Datalog statements:" is a complete statement and
// must parse.
var helpExamplesNotFullStatements = map[string]bool{
	"not parent(?, X)": true,
}

// TestHelpDatalogExamplesParse is the regression for BUG #12: cmdHelp
// (commands.go) previously advertised `C = count : person(?, ?).` as a
// valid aggregate-rule example, but the parser rejects a bare comparison
// as a rule/fact head ("comparisons are not allowed as a rule or fact
// head") — every aggregate rule needs a real predicate head, e.g.
// `pop(C) :- C = count : person(?, ?).`. This test runs the actual .help
// output through the real parser (syntax.ParseStatement) so the help text
// can never again silently drift out of sync with what the parser accepts.
func TestHelpDatalogExamplesParse(t *testing.T) {
	r := &repl{session: &session{}, out: new(bytes.Buffer)}
	buf := r.out.(*bytes.Buffer)

	if err := cmdHelp(r, ""); err != nil {
		t.Fatalf("cmdHelp: unexpected error: %v", err)
	}

	lines := strings.Split(buf.String(), "\n")
	inExamples := false
	tested := 0
	for _, line := range lines {
		if strings.HasPrefix(line, "Datalog statements:") {
			inExamples = true
			continue
		}
		if !inExamples || strings.TrimSpace(line) == "" {
			continue
		}
		m := datalogExampleLine.FindStringSubmatch(line)
		if m == nil {
			t.Fatalf("help example line did not match the expected two-column layout: %q", line)
		}
		stmt := strings.TrimSpace(m[1])
		if helpExamplesNotFullStatements[stmt] {
			continue
		}
		tested++
		if _, err := syntax.ParseStatement(stmt); err != nil {
			t.Errorf(".help advertises an example the parser rejects: %q: %v", stmt, err)
		}
	}
	if tested == 0 {
		t.Fatal("no Datalog example lines found in .help output — test is not exercising anything")
	}
}

// TestHelpAggregateExampleExactString pins the exact fixed string from the
// bug report's repro: the OLD text `C = count : person(?, ?).` must no
// longer appear as a bare-statement example in .help, and the NEW example
// must actually parse as an aggregate rule.
func TestHelpAggregateExampleExactString(t *testing.T) {
	r := &repl{session: &session{}, out: new(bytes.Buffer)}
	buf := r.out.(*bytes.Buffer)
	if err := cmdHelp(r, ""); err != nil {
		t.Fatalf("cmdHelp: unexpected error: %v", err)
	}
	out := buf.String()
	if strings.Contains(out, "  C = count : person(?, ?).") {
		t.Fatal(".help still advertises the invalid `C = count : person(?, ?).` example")
	}
	stmt, err := syntax.ParseStatement(`pop(C) :- C = count : person(?, ?).`)
	if err != nil {
		t.Fatalf("replacement aggregate example does not parse: %v", err)
	}
	if _, ok := stmt.(*syntax.AggregateRule); !ok {
		t.Fatalf("replacement example parsed as %T, want *syntax.AggregateRule", stmt)
	}
}

// TestCmdDescribe_PrintsDocAndRuleRefs is the REPL frontend test for
// .describe (session.describe, describe.go): it must print the
// declaration's assembled Use (from the rule's %% doc comment, per
// doc/features/predicate-docs.md), the fact count, and the rule's own doc
// comment under "derived by".
func TestCmdDescribe_PrintsDocAndRuleRefs(t *testing.T) {
	r := &repl{session: &session{}, out: new(bytes.Buffer)}
	buf := r.out.(*bytes.Buffer)

	src := `
%% A host observed doing something interesting.
event(Host, Kind) :- raw(Host, Kind, ?).
`
	if _, err := r.session.setRules(src); err != nil {
		t.Fatalf("setRules: %v", err)
	}

	if err := cmdDescribe(r, "event"); err != nil {
		t.Fatalf("cmdDescribe: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "event/2") {
		t.Errorf(".describe output missing the arity header: %q", out)
	}
	if !strings.Contains(out, "derived by:") {
		t.Errorf(".describe output missing the derived-by section: %q", out)
	}
	if !strings.Contains(out, "observed doing something interesting") {
		t.Errorf(".describe output missing the rule's doc comment: %q", out)
	}
}

// TestCmdDescribe_UnknownPredicateErrors asserts .describe with no argument
// or an unknown predicate returns an error rather than printing nothing —
// matching .why/.facts' usage-error style for a missing argument.
func TestCmdDescribe_UnknownPredicateErrors(t *testing.T) {
	r := &repl{session: &session{}, out: new(bytes.Buffer)}
	if err := cmdDescribe(r, ""); err == nil {
		t.Fatal("cmdDescribe: expected a usage error for no argument, got none")
	}
	if _, err := r.session.setRules(`event("h1").`); err != nil {
		t.Fatalf("setRules: %v", err)
	}
	if err := cmdDescribe(r, "nope"); err == nil {
		t.Fatal("cmdDescribe: expected an error for an unknown predicate, got none")
	}
}
