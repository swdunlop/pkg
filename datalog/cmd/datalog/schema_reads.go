package main

import (
	"context"
	"fmt"
	"slices"
	"sort"

	"swdunlop.dev/pkg/datalog"
	"swdunlop.dev/pkg/datalog/jsonfacts"
	"swdunlop.dev/pkg/datalog/seminaive"
)

// This file implements the three structured read tools phase 1c adds
// alongside the schema CRUD writes (schema_crud.go): get_config (the whole
// schema as structured JSON, design decision 7), predicate_deps (static
// dependency analysis in both directions, design decision 8), and
// explain_fact (post-hoc one-step derivation with an always-present
// editable address, design decision 9). All three join the read/query tool
// set in registerToolsForMode (mcp.go) — every mode gets them, per design
// decision 10.

// -- get_config -----------------------------------------------------------

type getConfigInput struct{}

// configSource is one source entry plus its current revision — the shape
// get_config returns, mirroring listRuleGroupsOutput's per-item revision
// field so a caller can go straight from get_config to a put_source/
// put_matcher/put_declaration edit without a second round trip.
type configSource struct {
	jsonfacts.Source
	Revision int `json:"revision"`
}

type configMatcher struct {
	jsonfacts.Matcher
	Revision int `json:"revision"`
}

type configDeclaration struct {
	datalog.Declaration
	Revision int `json:"revision"`
}

type getConfigOutput struct {
	Sources      []configSource      `json:"sources"`
	Matchers     []configMatcher     `json:"matchers"`
	Declarations []configDeclaration `json:"declarations"`
}

// getConfig returns the whole schema as structured JSON (design decision 7:
// "the whole schema as structured JSON... No YAML text in the output"):
// every source/matcher/declaration with its full item content and current
// revision. Items are returned in the SAME deterministic order
// serializeConfigYAML would write them in (schema_serialize.go's sort
// functions), so get_config's output order always matches what a caller
// would see re-reading the file from disk after any write.
func (h *mcpHandlers) getConfig(_ getConfigInput) (getConfigOutput, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	sorted := sortConfigForSerialization(h.sess.authoringCfg)

	out := getConfigOutput{
		Sources:      make([]configSource, len(sorted.Sources)),
		Matchers:     make([]configMatcher, len(sorted.Matchers)),
		Declarations: make([]configDeclaration, len(sorted.Declarations)),
	}
	for i, s := range sorted.Sources {
		out.Sources[i] = configSource{Source: s, Revision: h.schemaRev.sources[s.File]}
	}
	for i, m := range sorted.Matchers {
		out.Matchers[i] = configMatcher{Matcher: m, Revision: h.schemaRev.matchers[matcherKeyOf(m)]}
	}
	for i, d := range sorted.Declarations {
		out.Declarations[i] = configDeclaration{Declaration: d, Revision: h.schemaRev.declarations[declarationKeyOf(d)]}
	}
	return out, nil
}

// -- predicate_deps ---------------------------------------------------------

type predicateDepsInput struct {
	Predicate string `json:"predicate" jsonschema:"the predicate name to analyze"`
	Arity     int    `json:"arity" jsonschema:"the predicate's arity; required because a predicate name may be overloaded across arities (predicates may be overloaded by arity), and dependencies differ per arity"`
}

// ruleGroupAddress names an editable rule-group location: the group's key
// (head/arity) plus its on-disk file, when this session was started with a
// rules/ directory store. File is empty for a legacy (non---rules) session
// — h.rules == nil — per design decision 8's "if the session has no store,
// dependent rules get no file address but the tool still works (empty
// address field), don't error."
type ruleGroupAddress struct {
	Head  string `json:"head"`
	Arity int    `json:"arity"`
	File  string `json:"file,omitempty"`
}

// matcherRef names one schema matcher: its full key tuple plus current
// revision, the same shape put_matcher/delete_matcher key on. The key's
// Predicate/Term is the (predicate, term) the matcher READS — so when a
// matcherRef appears as a producer of some match-kind predicate (e.g.
// contains/2), Predicate/Term here name the upstream source it scanned,
// which is exactly the annotation a why-walk needs to keep going.
type matcherRef struct {
	Predicate       string `json:"predicate"`
	Term            int    `json:"term"`
	CaseInsensitive bool   `json:"case_insensitive,omitempty"`
	Windash         bool   `json:"windash,omitempty"`
	Revision        int    `json:"revision"`

	// Produces lists the arity-2 match-kind predicates this matcher emits
	// (jsonfacts.Matcher.ProducedPredicates), so an agent holding either
	// end of the edge can navigate to the other without knowing the
	// matcher naming rules.
	Produces []string `json:"produces,omitempty"`
}

// declarationRef names the schema declaration for a base predicate, if any.
type declarationRef struct {
	Name     string `json:"name"`
	Arity    int    `json:"arity"`
	Revision int    `json:"revision"`
}

type predicateDepsOutput struct {
	// DependsOn: what this predicate's OWN definition needs to be true —
	// the rule groups whose HEAD is this predicate/arity (each such rule's
	// body predicates are what it depends on, but the address returned here
	// is the group producing THIS predicate, exactly as list_rule_groups/
	// get_rule_group key a group), plus the schema matchers that PRODUCE
	// it: a matcher consumes its configured (predicate, term) and emits
	// arity-2 match-kind facts (contains/2, regex_match/2, ... — see
	// jsonfacts.Matcher.ProducedPredicates), so it appears here only for
	// those match-kind predicates at arity 2, never for the predicate it
	// reads. Each matcherRef's own Predicate/Term name the source it read.
	// A predicate produced by several of these at once (rules + matchers +
	// a source mapping sharing the name) reports all of them.
	DependsOnGroups   []ruleGroupAddress `json:"depends_on_groups,omitempty"`
	DependsOnMatchers []matcherRef       `json:"depends_on_matchers,omitempty"`
	Declaration       *declarationRef    `json:"declaration,omitempty"`

	// DependedOnBy: every rule group whose BODY references this predicate
	// (in any position, including a negated atom or an aggregate rule's
	// body), plus every matcher that READS it — a matcher with Predicate ==
	// this name consumes term Term of its facts, so it is a downstream
	// consumer at any arity where that term exists (arity > Term).
	DependedOnBy         []ruleGroupAddress `json:"depended_on_by,omitempty"`
	DependedOnByMatchers []matcherRef       `json:"depended_on_by_matchers,omitempty"`
}

// predicateDeps performs static dependency analysis over the loaded rules
// and schema — no evaluation, no facts inspected — in both directions
// (design decision 8): what predicate/arity depends on (its own deriving
// rule groups, or its base-loading matchers/declaration), and what depends
// on it (every rule group referencing it in a body position). It works on
// a legacy (non---rules) session too, for whatever rules the session holds
// in memory: h.rules == nil only means addresses come back with an empty
// File field, never an error (see ruleGroupAddress's doc comment).
func (h *mcpHandlers) predicateDeps(in predicateDepsInput) (predicateDepsOutput, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	name, arity := in.Predicate, in.Arity

	out := predicateDepsOutput{}

	// Rule groups whose HEAD is (name, arity): what produces this predicate
	// via rules. addressFor resolves the file from h.rules when present.
	seenHeadGroups := map[groupKey]bool{}
	for _, r := range h.sess.rules {
		if r.Head.Pred == name && len(r.Head.Terms) == arity {
			k := groupKey{Head: name, Arity: arity}
			if !seenHeadGroups[k] {
				seenHeadGroups[k] = true
				out.DependsOnGroups = append(out.DependsOnGroups, h.ruleGroupAddressFor(k))
			}
		}
	}
	for _, ar := range h.sess.aggRules {
		if ar.Head.Pred == name && len(ar.Head.Terms) == arity {
			k := groupKey{Head: name, Arity: arity}
			if !seenHeadGroups[k] {
				seenHeadGroups[k] = true
				out.DependsOnGroups = append(out.DependsOnGroups, h.ruleGroupAddressFor(k))
			}
		}
	}

	// Matcher edges, both directions. A matcher is a PRODUCER of its
	// arity-2 match-kind predicates (ProducedPredicates) and a CONSUMER of
	// its configured (Predicate, Term) — never a producer of the predicate
	// it reads. See doc/features/predicate-deps-matcher-direction.md for
	// the why-walk this orientation serves.
	for _, m := range h.sess.authoringCfg.Matchers {
		produced := m.ProducedPredicates()
		ref := matcherRef{
			Predicate: m.Predicate, Term: m.Term, CaseInsensitive: m.CaseInsensitive, Windash: m.Windash,
			Revision: h.schemaRev.matchers[matcherKeyOf(m)],
			Produces: produced,
		}
		if arity == 2 && slices.Contains(produced, name) {
			out.DependsOnMatchers = append(out.DependsOnMatchers, ref)
		}
		if m.Predicate == name && arity > m.Term {
			out.DependedOnByMatchers = append(out.DependedOnByMatchers, ref)
		}
	}
	if d, ok := findDeclaration(h.sess.authoringCfg.Declarations, declarationKey{Name: name, Arity: arity}); ok {
		key := declarationKeyOf(*d)
		out.Declaration = &declarationRef{Name: d.Name, Arity: arity, Revision: h.schemaRev.declarations[key]}
	}

	// What depends on this predicate: every rule/aggregate-rule group whose
	// BODY references (name, arity) in any position.
	seenBodyGroups := map[groupKey]bool{}
	for _, r := range h.sess.rules {
		for _, atom := range r.Body {
			if atom.Pred == name && len(atom.Terms) == arity {
				k := groupKey{Head: r.Head.Pred, Arity: len(r.Head.Terms)}
				if !seenBodyGroups[k] {
					seenBodyGroups[k] = true
					out.DependedOnBy = append(out.DependedOnBy, h.ruleGroupAddressFor(k))
				}
				break
			}
		}
	}
	for _, ar := range h.sess.aggRules {
		for _, atom := range ar.Body {
			if atom.Pred == name && len(atom.Terms) == arity {
				k := groupKey{Head: ar.Head.Pred, Arity: len(ar.Head.Terms)}
				if !seenBodyGroups[k] {
					seenBodyGroups[k] = true
					out.DependedOnBy = append(out.DependedOnBy, h.ruleGroupAddressFor(k))
				}
				break
			}
		}
	}

	if len(out.DependsOnGroups) == 0 && len(out.DependsOnMatchers) == 0 && out.Declaration == nil &&
		len(out.DependedOnBy) == 0 && len(out.DependedOnByMatchers) == 0 {
		// Mirror describe's "no such predicate" gate (describe.go) rather
		// than silently returning an all-empty struct: an unknown predicate
		// name/arity is far more likely a typo than a genuinely
		// dependency-free predicate an operator wants confirmed as such,
		// and list_predicates/describe already exist for that legitimate
		// case with a friendlier answer shape than four empty lists.
		if !predicateKnown(h.sess, name, arity) {
			return predicateDepsOutput{}, fmt.Errorf("predicate_deps: %s/%d: no such predicate in the current session "+
				"(check the name/arity with list_predicates or describe)", name, arity)
		}
	}

	sortRuleGroupAddresses(out.DependsOnGroups)
	sortRuleGroupAddresses(out.DependedOnBy)
	sortMatcherRefs(out.DependsOnMatchers)
	sortMatcherRefs(out.DependedOnByMatchers)

	return out, nil
}

// sortMatcherRefs sorts in place by the matcher key tuple for
// deterministic output — the same ordering serializeConfigYAML writes.
func sortMatcherRefs(refs []matcherRef) {
	sort.Slice(refs, func(i, j int) bool {
		return matcherKeyLess(jsonfacts.Matcher{
			Predicate: refs[i].Predicate, Term: refs[i].Term,
			CaseInsensitive: refs[i].CaseInsensitive, Windash: refs[i].Windash,
		}, jsonfacts.Matcher{
			Predicate: refs[j].Predicate, Term: refs[j].Term,
			CaseInsensitive: refs[j].CaseInsensitive, Windash: refs[j].Windash,
		})
	})
}

// predicateKnown mirrors describe.go's describe unknown-predicate gate: is
// name/arity referenced ANYWHERE (facts, declaration, rule head or body,
// aggregate head or body)?
func predicateKnown(s *session, name string, arity int) bool {
	if s.dataDB != nil {
		for pa := range s.dataDB.PredicateCounts() {
			if pa.Name == name && pa.Arity == arity {
				return true
			}
		}
	}
	for _, f := range s.facts {
		if f.Name == name && len(f.Terms) == arity {
			return true
		}
	}
	for _, d := range s.authoringCfg.Declarations {
		if d.Name == name && len(d.Terms) == arity {
			return true
		}
	}
	for _, m := range s.authoringCfg.Matchers {
		// Known via a matcher in either direction, arity-consistent with
		// predicateDeps: as the predicate a matcher reads (only at arities
		// where its term exists), or as an arity-2 match-kind predicate it
		// produces.
		if m.Predicate == name && arity > m.Term {
			return true
		}
		if arity == 2 && slices.Contains(m.ProducedPredicates(), name) {
			return true
		}
	}
	for _, r := range s.rules {
		if r.Head.Pred == name && len(r.Head.Terms) == arity {
			return true
		}
		for _, atom := range r.Body {
			if atom.Pred == name && len(atom.Terms) == arity {
				return true
			}
		}
	}
	for _, ar := range s.aggRules {
		if ar.Head.Pred == name && len(ar.Head.Terms) == arity {
			return true
		}
		for _, atom := range ar.Body {
			if atom.Pred == name && len(atom.Terms) == arity {
				return true
			}
		}
	}
	return false
}

// ruleGroupAddressFor resolves k's on-disk file from h.rules when present,
// leaving File empty for a legacy (non---rules) session — see
// ruleGroupAddress's doc comment. Callers must hold h.mu.
func (h *mcpHandlers) ruleGroupAddressFor(k groupKey) ruleGroupAddress {
	addr := ruleGroupAddress{Head: k.Head, Arity: k.Arity}
	if h.rules != nil {
		if g, ok := h.rules.Groups[k]; ok {
			addr.File = g.File
		}
	}
	return addr
}

// sortRuleGroupAddresses sorts in place by (head, arity) for deterministic
// output.
func sortRuleGroupAddresses(addrs []ruleGroupAddress) {
	sort.Slice(addrs, func(i, j int) bool {
		if addrs[i].Head != addrs[j].Head {
			return addrs[i].Head < addrs[j].Head
		}
		return addrs[i].Arity < addrs[j].Arity
	})
}

// -- explain_fact -----------------------------------------------------------

type explainFactInput struct {
	Fact string `json:"fact" jsonschema:"one ground fact to explain, e.g. concern(\"ws01\", 87) — same syntax as the explain tool's fact argument"`
}

// explainFactPremise is one premise fact one level down from the explained
// fact — deliberately NOT itself recursively explained (design decision 9:
// "post-hoc one-step derivation, model recurses"): a caller that wants the
// premise's own derivation calls explain_fact again with this literal.
type explainFactPremise struct {
	Fact string `json:"fact" jsonschema:"the premise fact, formatted exactly as explain_fact's own \"fact\" input expects"`
	Base bool   `json:"base"` // true when this premise itself has no further witness (loaded/asserted, not derived)
}

type explainFactOutput struct {
	Exists bool   `json:"exists"`         // false when fact was never produced by the current evaluation at all
	Kind   string `json:"kind,omitempty"` // "derived" or "base"; omitted when Exists is false

	// -- Kind == "derived" --
	Rule        string               `json:"rule,omitempty"`         // the deriving rule's source text
	Doc         string               `json:"doc,omitempty"`          // the rule's %% doc text, if any
	RuleAddress *ruleGroupAddress    `json:"rule_address,omitempty"` // the editable address of the group that defines Rule
	Premises    []explainFactPremise `json:"premises,omitempty"`     // body facts one level down (not recursively explained)

	// -- Kind == "base" --
	Declaration       *declarationRef `json:"declaration,omitempty"`        // the schema declaration for this predicate, if any
	CandidateMatchers []matcherRef    `json:"candidate_matchers,omitempty"` // every matcher that PRODUCES this fact's predicate, each keyed by the (predicate, term) it reads (static: not narrowed to the exact source record — see doc comment)
}

// explainFact resolves fact's post-hoc ONE-step derivation (design decision
// 9): whether it exists in the current evaluation; if derived, the rule
// that produced it (text + doc), the EDITABLE ADDRESS of the rule group
// that defines it (a ruleGroupAddress, empty File on a legacy session —
// same convention as predicate_deps), and the premise facts one level down
// as plain literals (the caller/model recurses by calling explain_fact
// again on a premise it wants to dig into — this tool never recurses
// itself, unlike the `explain` tool's full ExplainTree); if base, "kind":
// "base" plus the schema-side editable address: the declaration (if any)
// and every matcher that produces this fact's predicate (a static match
// on produced names — provenance to the EXACT source record that produced
// this specific base fact is out of scope here, per the task brief: "do
// not build it" — a base fact may have come from a plain source mapping
// with no matcher involvement at all, in which case CandidateMatchers is
// simply empty).
//
// Reuses the existing session.explainProvenance/Provenance.Explain
// machinery (session.go, seminaive/provenance.go) — the SAME one-step
// resolution the `explain` tool's REPL sibling (.why) would use if it only
// wanted one level, and the same timeout (h.timeout) query/explain already
// use.
func (h *mcpHandlers) explainFact(ctx context.Context, in explainFactInput) (explainFactOutput, error) {
	fact, err := parseFactStatement(in.Fact)
	if err != nil {
		return explainFactOutput{}, err
	}

	ctx, cancel := h.evalContext(ctx)
	defer cancel()

	h.mu.Lock()
	provEnabled := h.sess.provenanceEnabled
	ruleset, engineOpts, db, snapGen, buildErr := h.sess.snapshotForEvaluate()
	cachedProv := h.sess.derivedProv
	cachedDB := h.sess.derivedDB
	h.mu.Unlock()
	if !provEnabled {
		return explainFactOutput{}, fmt.Errorf("explain_fact: this session was not started with provenance enabled")
	}
	if buildErr != nil {
		return explainFactOutput{}, buildErr
	}

	prov := cachedProv
	if cachedDB == nil || cachedProv == nil {
		// Cold path, mirroring explain's (mcp.go): no cached base fixpoint yet,
		// so compute one and cache it back under the same gen guard.
		fresh := seminaive.NewProvenance()
		out, err := evaluateSnapshot(ctx, ruleset, engineOpts, db, fresh)
		if err != nil {
			return explainFactOutput{}, err
		}
		prov = fresh
		if err := h.checkFactCap(out); err == nil {
			h.mu.Lock()
			if h.sess.gen == snapGen {
				h.sess.derivedDB = out
				h.sess.derivedProv = fresh
			}
			h.mu.Unlock()
		}
	}

	d, found := prov.Explain(fact)
	if !found {
		return explainFactOutput{Exists: false}, nil
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	if d.Base {
		return h.explainBaseFactLocked(fact), nil
	}
	return h.explainDerivedFactLocked(d), nil
}

// explainDerivedFactLocked builds explain_fact's "derived" branch from a
// one-step Derivation. Callers must hold h.mu.
func (h *mcpHandlers) explainDerivedFactLocked(d seminaive.Derivation) explainFactOutput {
	addr := h.ruleGroupAddressFor(groupKey{Head: d.Fact.Name, Arity: len(d.Fact.Terms)})
	premises := make([]explainFactPremise, len(d.Body))
	for i, b := range d.Body {
		premises[i] = explainFactPremise{Fact: formatFactLiteral(b.Fact), Base: b.Base}
	}
	return explainFactOutput{
		Exists:      true,
		Kind:        "derived",
		Rule:        d.Rule,
		Doc:         d.Doc,
		RuleAddress: &addr,
		Premises:    premises,
	}
}

// explainBaseFactLocked builds explain_fact's "base" branch: the schema
// declaration for this predicate (if any) and every matcher that PRODUCES
// this fact's predicate — i.e. whose arity-2 match-kind output set
// (jsonfacts.Matcher.ProducedPredicates) contains it, each reported with
// the (predicate, term) it reads so the why-walk can continue upstream. A
// plain source-mapped fact (e.g. event/3) lists no matchers: matchers
// consume such predicates, they never produce them. This is a STATIC
// match over the schema's configured matchers, not a trace back to which
// matcher (if any) actually produced THIS fact from THIS source record;
// see explainFact's doc comment for why that finer-grained provenance is
// explicitly out of scope. Callers must hold h.mu.
func (h *mcpHandlers) explainBaseFactLocked(fact datalog.Fact) explainFactOutput {
	out := explainFactOutput{Exists: true, Kind: "base"}

	if d, ok := findDeclaration(h.sess.authoringCfg.Declarations, declarationKey{Name: fact.Name, Arity: len(fact.Terms)}); ok {
		key := declarationKeyOf(*d)
		out.Declaration = &declarationRef{Name: d.Name, Arity: len(fact.Terms), Revision: h.schemaRev.declarations[key]}
	}
	if len(fact.Terms) == 2 { // every matcher-produced fact is (value, pattern)
		for _, m := range h.sess.authoringCfg.Matchers {
			produced := m.ProducedPredicates()
			if !slices.Contains(produced, fact.Name) {
				continue
			}
			out.CandidateMatchers = append(out.CandidateMatchers, matcherRef{
				Predicate: m.Predicate, Term: m.Term, CaseInsensitive: m.CaseInsensitive, Windash: m.Windash,
				Revision: h.schemaRev.matchers[matcherKeyOf(m)],
				Produces: produced,
			})
		}
	}
	sortMatcherRefs(out.CandidateMatchers)
	return out
}

// formatFactLiteral renders fact exactly as parseFactStatement's input
// syntax expects it back (predicate name plus parenthesized constant
// terms, no trailing "."), so explain_fact's Premises can be fed straight
// back into another explain_fact (or the explain tool's "fact" input)
// call.
func formatFactLiteral(fact datalog.Fact) string {
	terms := make([]string, len(fact.Terms))
	for i, t := range fact.Terms {
		terms[i] = t.String()
	}
	s := fact.Name + "("
	for i, t := range terms {
		if i > 0 {
			s += ", "
		}
		s += t
	}
	return s + ")"
}
