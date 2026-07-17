package main

import (
	"context"
	"strings"
	"testing"
)

// TestDescribe_MultiArityAndFactCounts asserts describe returns one entry
// per arity a name is overloaded under, each with its own fact count — the
// session.go "describe" schema from doc/features/predicate-docs.md, section
// "describe: the mechanical access surface".
func TestDescribe_MultiArityAndFactCounts(t *testing.T) {
	s := &session{}
	if _, err := s.setRules(`
tag("x").
tag("a", "b").
tag("c", "d").
`); err != nil {
		t.Fatalf("setRules: %v", err)
	}
	db, _, err := s.evaluate(context.Background())
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}
	s.derivedDB = db

	out, err := s.describe("tag")
	if err != nil {
		t.Fatalf("describe: %v", err)
	}
	if out.Name != "tag" {
		t.Fatalf("describe: name = %q, want %q", out.Name, "tag")
	}
	if len(out.Arities) != 2 {
		t.Fatalf("describe: got %d arities, want 2", len(out.Arities))
	}
	byArity := map[int]describeArity{}
	for _, a := range out.Arities {
		byArity[a.Arity] = a
	}
	if byArity[1].FactCount != 1 {
		t.Errorf("describe: arity 1 factCount = %d, want 1", byArity[1].FactCount)
	}
	if byArity[2].FactCount != 2 {
		t.Errorf("describe: arity 2 factCount = %d, want 2", byArity[2].FactCount)
	}
}

// TestDescribe_DerivedByAndConsumedBy asserts derivedBy collects every rule
// whose HEAD is the described predicate/arity, and consumedBy collects
// every rule whose BODY references it — including a negated atom and an
// aggregate rule's body, per the spec's explicit "including negated atoms
// and aggregate bodies" requirement.
func TestDescribe_DerivedByAndConsumedBy(t *testing.T) {
	s := &session{}
	src := `
%% A host observed doing something.
event(Host, Kind) :- raw(Host, Kind, ?).

%% Hosts with no observed events at all.
quiet(Host) :- known_host(Host), not event(Host, ?).

%% Count of events per host.
event_count(Host, N) :- N = count : event(Host, ?).
`
	if _, err := s.setRules(src); err != nil {
		t.Fatalf("setRules: %v", err)
	}

	out, err := s.describe("event")
	if err != nil {
		t.Fatalf("describe: %v", err)
	}
	if len(out.Arities) != 1 {
		t.Fatalf("describe: got %d arities, want 1", len(out.Arities))
	}
	a := out.Arities[0]

	if len(a.DerivedBy) != 1 {
		t.Fatalf("describe: derivedBy has %d entries, want 1", len(a.DerivedBy))
	}
	if a.DerivedBy[0].Doc == "" || !strings.Contains(a.DerivedBy[0].Doc, "observed doing something") {
		t.Errorf("describe: derivedBy[0].Doc = %q, missing the rule's %%%% doc comment", a.DerivedBy[0].Doc)
	}
	if !strings.Contains(a.DerivedBy[0].RuleText, "event(Host, Kind) :- raw") {
		t.Errorf("describe: derivedBy[0].RuleText = %q, missing the rule source", a.DerivedBy[0].RuleText)
	}

	if len(a.ConsumedBy) != 2 {
		t.Fatalf("describe: consumedBy has %d entries, want 2 (quiet's negated atom + event_count's aggregate body)", len(a.ConsumedBy))
	}
	var sawNegation, sawAggregate bool
	for _, ref := range a.ConsumedBy {
		if strings.Contains(ref.RuleText, "quiet(") {
			sawNegation = true
			if !strings.Contains(ref.RuleText, "not event") {
				t.Errorf("describe: negation consumer's ruleText = %q, missing the negated atom", ref.RuleText)
			}
		}
		if strings.Contains(ref.RuleText, "event_count(") {
			sawAggregate = true
		}
	}
	if !sawNegation {
		t.Error("describe: consumedBy missing the rule referencing event via a negated atom")
	}
	if !sawAggregate {
		t.Error("describe: consumedBy missing the aggregate rule referencing event in its body")
	}
}

// TestDescribe_UnknownPredicate asserts an error, not an empty/ambiguous
// result, for a name that is not known under any arity at all — no facts,
// no declaration, no rule reference — matching explain/.why's not-found
// error style (session.go/repl.go).
func TestDescribe_UnknownPredicate(t *testing.T) {
	s := &session{}
	if _, err := s.setRules(`event("h1").`); err != nil {
		t.Fatalf("setRules: %v", err)
	}
	_, err := s.describe("nope")
	if err == nil {
		t.Fatal("describe: expected an error for an unknown predicate, got none")
	}
	if !strings.Contains(err.Error(), "nope") {
		t.Errorf("describe: error %q does not name the unknown predicate", err.Error())
	}
}

// TestDescribe_BareRuleBodyReference asserts a predicate that is only ever
// referenced in a rule body (never loaded, never a rule head) still
// describes successfully with factCount 0 and no declaration, rather than
// being treated as unknown — per describe's doc comment on arity
// collection ("union of ... every arity referenced as a rule head or body
// atom").
func TestDescribe_BareRuleBodyReference(t *testing.T) {
	s := &session{}
	if _, err := s.setRules(`derived(X) :- never_loaded(X).`); err != nil {
		t.Fatalf("setRules: %v", err)
	}
	out, err := s.describe("never_loaded")
	if err != nil {
		t.Fatalf("describe: %v", err)
	}
	if len(out.Arities) != 1 {
		t.Fatalf("describe: got %d arities, want 1", len(out.Arities))
	}
	a := out.Arities[0]
	if a.FactCount != 0 {
		t.Errorf("describe: factCount = %d, want 0", a.FactCount)
	}
	if a.Declaration != nil {
		t.Errorf("describe: declaration = %+v, want nil (no jsonfacts declaration, no assembled one for a body-only reference)", a.Declaration)
	}
	if len(a.DerivedBy) != 0 {
		t.Errorf("describe: derivedBy = %+v, want empty (never_loaded has no rule head)", a.DerivedBy)
	}
	if len(a.ConsumedBy) != 1 {
		t.Errorf("describe: consumedBy has %d entries, want 1", len(a.ConsumedBy))
	}
}
