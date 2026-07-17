package main

import (
	"fmt"
	"sort"

	"swdunlop.dev/pkg/datalog"
	"swdunlop.dev/pkg/datalog/memory"
)

// describeRuleRef is one rule's contribution to a describeArity's derivedBy
// or consumedBy list: the rule rendered as source text (syntax.Rule.String/
// syntax.AggregateRule.String — the SAME rendering the .rules command and
// set_rules round-trip use, so a model reading this back sees exactly what
// it would find in the rules document) plus that rule's own %% doc comment,
// if any (syntax.Rule.Doc/syntax.AggregateRule.Doc — see
// doc/features/predicate-docs.md).
type describeRuleRef struct {
	RuleText string `json:"ruleText"`
	Doc      string `json:"doc,omitempty"`
}

// describeArity is one arity's worth of information about a described
// predicate: its declaration (docs/terms/types, possibly assembled by the
// transformer per doc/features/predicate-docs.md), how many facts currently
// exist for it, and which rules derive it (head) or consume it (any body
// position, including negated atoms and aggregate-rule bodies).
type describeArity struct {
	Arity       int                  `json:"arity"`
	FactCount   int                  `json:"factCount"`
	Declaration *datalog.Declaration `json:"declaration,omitempty"`
	DerivedBy   []describeRuleRef    `json:"derivedBy,omitempty"`
	ConsumedBy  []describeRuleRef    `json:"consumedBy,omitempty"`
}

// describeResult is describe(name)'s whole answer: one entry per arity the
// predicate is known under, since the same name may be overloaded across
// different arities (matching sample_facts/list_predicates' name+arity
// keying throughout this package).
type describeResult struct {
	Name    string          `json:"name"`
	Arities []describeArity `json:"arities"`
}

// describe is the ONE session-level implementation of the describe(name)
// operation (doc/features/predicate-docs.md's "describe: the mechanical
// access surface") — the MCP tool, the REPL's .describe, and the Fact
// Browser's header rendering all call this (or its constituent parts)
// rather than re-deriving any of it, per this repo's "fix at the mechanism"
// doctrine: one pipeline, N frontends.
//
// It walks two independent sources, neither requiring an engine change:
//   - the session's current syntax.Ruleset (s.rules/s.aggRules) for
//     derivedBy/consumedBy, since those are purely syntactic facts about the
//     rules document;
//   - the evaluated database's Declarations() (s.evaluatedDB()) for
//     per-arity declarations and fact counts, which already carries
//     whatever the transformer assembled from rule-head variable names and
//     rule docs (step 3 of predicate-docs.md), on top of any explicit
//     jsonfacts declaration.
//
// Arities are collected as the union of: every arity with at least one
// fact, every arity with a declaration, and every arity referenced as a
// rule head or body atom anywhere in the ruleset — so a predicate that is
// only ever referenced in rule bodies (never loaded, never derived) still
// shows up with factCount 0 and an empty declaration, rather than being
// silently omitted. Returns an error only when name is not known under ANY
// arity at all (no facts, no declaration, no rule reference) — see
// describeUnknownError.
func (s *session) describe(name string) (describeResult, error) {
	db, err := s.evaluatedDB()
	if err != nil {
		return describeResult{}, err
	}
	mdb, ok := db.(*memory.Database)
	if !ok {
		return describeResult{}, fmt.Errorf("describe: internal error: unexpected database type %T", db)
	}

	arities := map[int]bool{}
	factCounts := map[int]int{}
	for pa, n := range mdb.PredicateCounts() {
		if pa.Name != name {
			continue
		}
		arities[pa.Arity] = true
		factCounts[pa.Arity] = n
	}

	decls := map[int]datalog.Declaration{}
	for d := range mdb.Declarations() {
		if d.Name != name {
			continue
		}
		arity := len(d.Terms)
		decls[arity] = d
		arities[arity] = true
	}

	derivedBy := map[int][]describeRuleRef{}
	consumedBy := map[int][]describeRuleRef{}

	for _, r := range s.rules {
		if r.Head.Pred == name {
			arity := len(r.Head.Terms)
			arities[arity] = true
			derivedBy[arity] = append(derivedBy[arity], describeRuleRef{RuleText: r.String(), Doc: r.Doc})
		}
		for _, atom := range r.Body {
			if atom.Pred != name {
				continue
			}
			arity := len(atom.Terms)
			arities[arity] = true
			consumedBy[arity] = append(consumedBy[arity], describeRuleRef{RuleText: r.String(), Doc: r.Doc})
		}
	}
	for _, ar := range s.aggRules {
		if ar.Head.Pred == name {
			arity := len(ar.Head.Terms)
			arities[arity] = true
			derivedBy[arity] = append(derivedBy[arity], describeRuleRef{RuleText: ar.String(), Doc: ar.Doc})
		}
		for _, atom := range ar.Body {
			if atom.Pred != name {
				continue
			}
			arity := len(atom.Terms)
			arities[arity] = true
			consumedBy[arity] = append(consumedBy[arity], describeRuleRef{RuleText: ar.String(), Doc: ar.Doc})
		}
	}
	if len(arities) == 0 {
		return describeResult{}, describeUnknownError(name)
	}

	sorted := make([]int, 0, len(arities))
	for a := range arities {
		sorted = append(sorted, a)
	}
	sort.Ints(sorted)

	out := describeResult{Name: name, Arities: make([]describeArity, 0, len(sorted))}
	for _, a := range sorted {
		entry := describeArity{
			Arity:      a,
			FactCount:  factCounts[a],
			DerivedBy:  derivedBy[a],
			ConsumedBy: consumedBy[a],
		}
		if d, ok := decls[a]; ok {
			d := d // copy for the pointer below
			entry.Declaration = &d
		}
		out.Arities = append(out.Arities, entry)
	}
	return out, nil
}

// describeUnknownError reports that name is not known under any arity at
// all — no facts, no declaration, and no rule (head or body) references
// it — matching the wording style of explain/.why's not-found errors
// (session.go's explain, repl.go's cmdWhy): name the check the caller
// should run instead of returning an empty, ambiguous result.
func describeUnknownError(name string) error {
	return fmt.Errorf("describe: %q: no such predicate in the current session "+
		"(check the name with list_predicates — it lists every predicate loaded from data "+
		"or defined by a rule)", name)
}
