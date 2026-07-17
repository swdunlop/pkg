package seminaive

import (
	"context"
	"fmt"
	"iter"
	"sort"
	"strings"
	"time"

	"swdunlop.dev/pkg/datalog"
	"swdunlop.dev/pkg/datalog/internal/interned"
	_ "swdunlop.dev/pkg/datalog/memory" // register interned.Memory hook
	"swdunlop.dev/pkg/datalog/syntax"
)

// declArityKey builds a map key combining a predicate name and its arity, for
// deduping declarations the same way datalog.DeclarationSet is keyed (see
// datalog.go's declKey): by (name, arity), not name alone, so a predicate
// declared at two distinct arities (p/1 and p/2) is never collapsed to just
// one of them.
func declArityKey(name string, arity int) string {
	return fmt.Sprintf("%s\x00%d", name, arity)
}

// transformer implements datalog.Transformer using semi-naive evaluation.
type transformer struct {
	rules         []syntax.Rule
	aggRules      []syntax.AggregateRule
	strata        []stratum // computed at compile time from rules and aggRules
	facts         []datalog.Fact
	maxIter       int
	factLimit     int
	builtins      map[string]BuiltinFunc
	multiBuiltins map[string]multiBuiltin
	externals     map[string]externalPredicate
	profile       func([]StratumStats)
}

var _ datalog.Transformer = (*transformer)(nil)

// Declarations returns the merged set of input and derived predicate declarations.
func (t *transformer) Declarations(ctx context.Context, input datalog.Database) iter.Seq[datalog.Declaration] {
	// Collect input declarations, deduped by (Name, arity) so a predicate
	// legitimately declared at two arities (p/1 and p/2) is not collapsed
	// to just the first one seen -- that would silently drop a real
	// declaration, matching how datalog.DeclarationSet itself is keyed.
	seen := map[string]bool{}      // any declaration exists for this name (used to gate the DocOnly bookkeeping below)
	seenArity := map[string]bool{} // (name, arity) pair already emitted, encoded as name+"\x00"+arity
	var decls []datalog.Declaration
	for d := range input.Declarations() {
		seen[d.Name] = true
		k := declArityKey(d.Name, len(d.Terms))
		if !seenArity[k] {
			seenArity[k] = true
			decls = append(decls, d)
		}
	}

	// Add DocOnly declarations for rule-head predicates not already declared
	// at any arity, purely so the predicate appears in Declarations() for
	// schema display. DocOnly ensures these never constrain arity/type
	// checking (see datalog.NewDeclarationSet); one bare marker per name is
	// enough regardless of how many arities the rule set derives.
	for _, r := range t.rules {
		if !seen[r.Head.Pred] {
			seen[r.Head.Pred] = true
			decls = append(decls, datalog.Declaration{Name: r.Head.Pred, DocOnly: true})
		}
	}
	for _, ar := range t.aggRules {
		if !seen[ar.Head.Pred] {
			seen[ar.Head.Pred] = true
			decls = append(decls, datalog.Declaration{Name: ar.Head.Pred, DocOnly: true})
		}
	}

	return func(yield func(datalog.Declaration) bool) {
		for _, d := range decls {
			if !yield(d) {
				return
			}
		}
	}
}

// Transform reads facts from the input database, applies rules via semi-naive
// evaluation, and returns a new database containing all input and derived facts.
func (t *transformer) Transform(ctx context.Context, input datalog.Database) (datalog.Database, error) {
	dict, existing, decls, err := t.loadInput(input)
	if err != nil {
		return nil, err
	}

	// Run evaluation if we have rules. Strata were computed at compile time.
	if len(t.strata) > 0 {
		strata := t.strata

		if len(t.externals) > 0 {
			if err := t.fetchExternals(ctx, dict, existing); err != nil {
				return nil, fmt.Errorf("external predicates: %w", err)
			}
		}

		ev := &evaluator{ctx: ctx, dict: dict, maxIter: t.maxIter, factLimit: t.factLimit, builtins: t.builtins, multiBuiltins: t.multiBuiltins}

		var stats []StratumStats
		if t.profile != nil {
			stats = make([]StratumStats, 0, len(strata))
		}

		for _, s := range strata {
			if err := ctx.Err(); err != nil {
				return nil, err
			}

			var ss StratumStats
			if stats != nil {
				preds := make([]string, 0, len(s.predicates))
				for p := range s.predicates {
					preds = append(preds, p)
				}
				sort.Strings(preds)
				ss.Predicates = preds
				ss.RuleCount = len(s.rules)
				ss.AggCount = len(s.aggRules)
			}

			var stratumStart time.Time
			if stats != nil {
				stratumStart = time.Now()
			}

			// Name the stratum so an error from either half (the plain-rule
			// fixpoint or the aggregate pass) identifies the offending
			// predicates the same way; %w keeps errors.Is working for
			// context cancellation in both cases.
			stratumErr := func(err error) error {
				preds := make([]string, 0, len(s.predicates))
				for p := range s.predicates {
					preds = append(preds, p)
				}
				sort.Strings(preds)
				return fmt.Errorf("stratum [%s]: %w", strings.Join(preds, " "), err)
			}

			if len(s.rules) > 0 {
				factCount, iterations, err := ev.evalRules(ctx, s.rules, existing, t.maxIter)
				if err != nil {
					return nil, stratumErr(err)
				}
				if stats != nil {
					ss.FactCount += factCount
					ss.Iterations = iterations
				}
			}

			if len(s.aggRules) > 0 {
				aggDerived, err := ev.evalAggregates(ctx, s.aggRules, existing)
				if err != nil {
					return nil, stratumErr(err)
				}
				if stats != nil {
					ss.FactCount += len(aggDerived.Index)
				}
				existing.Merge(aggDerived)
			}

			if stats != nil {
				ss.Duration = time.Since(stratumStart)
				stats = append(stats, ss)
			}
		}

		if t.profile != nil {
			t.profile(stats)
		}
	}

	// Merge declarations for derived predicates. seenDecl tracks "any
	// declaration exists for this name" -- sufficient to skip adding the
	// DocOnly rule-head marker below, since one bare marker per name is
	// enough regardless of how many arities are otherwise declared. decls
	// itself (from loadInput) already preserves distinct arities for the
	// same name; this loop must not re-collapse it by name.
	seenDecl := map[string]bool{}
	for _, d := range decls {
		seenDecl[d.Name] = true
	}
	for _, r := range t.rules {
		if !seenDecl[r.Head.Pred] {
			seenDecl[r.Head.Pred] = true
			decls = append(decls, datalog.Declaration{Name: r.Head.Pred, DocOnly: true})
		}
	}
	for _, ar := range t.aggRules {
		if !seenDecl[ar.Head.Pred] {
			seenDecl[ar.Head.Pred] = true
			decls = append(decls, datalog.Declaration{Name: ar.Head.Pred, DocOnly: true})
		}
	}
	sort.Slice(decls, func(i, j int) bool { return decls[i].Name < decls[j].Name })

	return interned.Memory.Wrap(dict, existing, decls), nil
}

// loadInput reads facts from the input database into a dict and interned fact set.
// When the input is already a memory.Database, it reuses the dict and clones the
// facts directly via the interned.Memory hook, avoiding re-interning overhead.
func (t *transformer) loadInput(input datalog.Database) (*interned.Dict, interned.InternedFactSet, []datalog.Declaration, error) {
	// Fast path: reuse internals from a memory database.
	if srcDict, srcFacts, _, ok := interned.Memory.Unwrap(input); ok {
		return t.loadFromInterned(srcDict, srcFacts, input)
	}
	return t.loadFromGeneric(input)
}

// loadFromInterned clones the dict and facts from an interned database, then adds
// ruleset facts and constants. Avoids re-interning existing facts entirely.
func (t *transformer) loadFromInterned(srcDict *interned.Dict, srcFacts interned.InternedFactSet, input datalog.Database) (*interned.Dict, interned.InternedFactSet, []datalog.Declaration, error) {
	dict := srcDict.Clone()
	existing := srcFacts.Clone()

	// Collect declarations.
	var decls []datalog.Declaration
	for d := range input.Declarations() {
		decls = append(decls, d)
	}

	// Intern ground facts from the ruleset.
	for _, f := range t.facts {
		ifact, err := dict.InternFact(f)
		if err != nil {
			return nil, interned.InternedFactSet{}, nil, fmt.Errorf("intern fact: %w", err)
		}
		existing.Add(ifact)
	}

	// Intern constants from rules so they're in the dict before evaluation.
	// No freeze needed -- we inherit the source dict's frozen ordering and
	// append new values (from ruleset facts/constants) at the end.
	t.internRuleConstants(dict)

	return dict, existing, decls, nil
}

// loadFromGeneric reads facts from an arbitrary Database implementation via iterators.
func (t *transformer) loadFromGeneric(input datalog.Database) (*interned.Dict, interned.InternedFactSet, []datalog.Declaration, error) {
	dict := interned.NewDict()
	existing := interned.NewInternedFactSet()

	// Collect declarations, deduped by (Name, arity) -- not Name alone -- so
	// a predicate legitimately declared at two arities (p/1 and p/2) is not
	// silently collapsed to just the first one seen. See declArityKey.
	var decls []datalog.Declaration
	seenArity := map[string]bool{}
	for d := range input.Declarations() {
		k := declArityKey(d.Name, len(d.Terms))
		if !seenArity[k] {
			seenArity[k] = true
			decls = append(decls, d)
		}
	}

	// internFact is a helper to intern and add a fact, returning any error.
	internFact := func(fact datalog.Fact) error {
		ifact, err := dict.InternFact(fact)
		if err != nil {
			return err
		}
		existing.Add(ifact)
		return nil
	}

	// Load every predicate the input reports, so the output carries all
	// input facts regardless of whether rules reference them — matching
	// the memory.Database fast path, which clones the whole fact set.
	predArities := map[string]map[int]bool{}
	for pred, arity := range input.Predicates() {
		if predArities[pred] == nil {
			predArities[pred] = map[int]bool{}
		}
		predArities[pred][arity] = true
	}
	for pred, arities := range predArities {
		for arity := range arities {
			for row := range input.Facts(pred, arity) {
				if err := internFact(datalog.Fact{Name: pred, Terms: row}); err != nil {
					return nil, interned.InternedFactSet{}, nil, err
				}
			}
		}
	}

	// Load facts for rule-referenced predicates the input did not report,
	// in case a Database implementation under-reports Predicates.
	loadUndeclaredPred := func(a syntax.Atom) error {
		if isConstraint(a) || isBindBuiltin(a, t.builtins) || isMultiBindBuiltin(a, t.multiBuiltins) || a.Pred == "is" || isExternalPred(a, t.externals) {
			return nil
		}
		arity := len(a.Terms)
		if predArities[a.Pred] != nil && predArities[a.Pred][arity] {
			return nil
		}
		for row := range input.Facts(a.Pred, arity) {
			if err := internFact(datalog.Fact{Name: a.Pred, Terms: row}); err != nil {
				return err
			}
		}
		if predArities[a.Pred] == nil {
			predArities[a.Pred] = map[int]bool{}
		}
		predArities[a.Pred][arity] = true
		return nil
	}
	for _, r := range t.rules {
		for _, a := range r.Body {
			if err := loadUndeclaredPred(a); err != nil {
				return nil, interned.InternedFactSet{}, nil, err
			}
		}
	}
	for _, ar := range t.aggRules {
		for _, a := range ar.Body {
			if err := loadUndeclaredPred(a); err != nil {
				return nil, interned.InternedFactSet{}, nil, err
			}
		}
	}

	// Intern ground facts from the ruleset.
	for _, f := range t.facts {
		if err := internFact(f); err != nil {
			return nil, interned.InternedFactSet{}, nil, err
		}
	}

	// Freeze dict for deterministic ordering, remap facts.
	remap := dict.Freeze()
	if remap != nil {
		existing = existing.Remap(remap)
	}

	return dict, existing, decls, nil
}

// fetchExternals performs semi-join reduction to materialize external predicate facts
// into existing before rule evaluation begins. For each external predicate referenced
// in rules, it collects all possible pushdown values from input facts and constants,
// calls the external function once with the complete set, and adds results to existing.
func (t *transformer) fetchExternals(ctx context.Context, dict *interned.Dict, existing interned.InternedFactSet) error {
	// derivedHeads collects every predicate that appears as the head of a
	// rule or aggregate rule. fetchExternals runs before any stratum is
	// evaluated, so facts for these predicates are not known yet — even if
	// the predicate also has base (EDB) facts loaded into existing. Using
	// such a predicate as a pushdown anchor would collect only the
	// pre-evaluation facts and, worse, an empty BoundTerm.Values (meaning
	// "no candidates") when it has no EDB facts at all — which the external
	// function reads as "nothing matches" rather than "unknown, don't
	// restrict". So anchors on derived predicates must be left unbound.
	derivedHeads := map[string]bool{}
	for _, r := range t.rules {
		derivedHeads[r.Head.Pred] = true
	}
	for _, ar := range t.aggRules {
		derivedHeads[ar.Head.Pred] = true
	}

	// For each external predicate, collect pushdown values per position.
	//
	// unbounded marks a position that some *occurrence* of the external
	// predicate leaves without any anchor to bound it (a free variable no
	// other atom in that occurrence's body constrains, e.g. `user(N, R)`
	// with R appearing nowhere else). One external call serves every rule
	// that references the predicate, so positions.get(pos) is the union of
	// candidate values across all occurrences -- but a position an
	// unconstrained occurrence needs the *entire* domain for, and pushing
	// down any restriction (even one a different occurrence's anchor
	// legitimately narrows to) would silently starve that unconstrained
	// occurrence of every non-matching value. So unbounded is a veto that
	// dominates: if set for a position, that position is never pushed down,
	// no matter what candidate values other occurrences collected for it.
	type pushdownInfo struct {
		ep        externalPredicate
		positions map[int]map[uint64]bool // position → set of interned value IDs
		unbounded map[int]bool            // position → some occurrence has no anchor here
	}
	pushdowns := map[string]*pushdownInfo{}

	collectFromBody := func(body []syntax.Atom) {
		for _, a := range body {
			ep, ok := t.externals[a.Pred]
			if !ok {
				continue
			}

			pd, exists := pushdowns[a.Pred]
			if !exists {
				pd = &pushdownInfo{ep: ep, positions: map[int]map[uint64]bool{}, unbounded: map[int]bool{}}
				pushdowns[a.Pred] = pd
			}

			// Collect constants in this external atom.
			for i, term := range a.Terms {
				if c, ok := term.(datalog.Constant); ok {
					if pd.positions[i] == nil {
						pd.positions[i] = map[uint64]bool{}
					}
					pd.positions[i][dict.InternConstant(c)] = true
				}
			}

			// Collect values from anchor atoms via shared variables. A
			// position whose term is a variable is bounded by this
			// occurrence only if some other atom in the SAME rule body
			// anchors that variable; otherwise this occurrence needs the
			// position left unbound (see unbounded's doc comment above).
			for i, term := range a.Terms {
				v, ok := term.(datalog.Variable)
				if !ok {
					continue
				}
				varName := string(v)
				anchored := false
				for _, other := range body {
					if isConstraint(other) || other.Pred == "is" || other.Negated {
						continue
					}
					if isBindBuiltin(other, t.builtins) || isMultiBindBuiltin(other, t.multiBuiltins) {
						continue
					}
					if _, isExt := t.externals[other.Pred]; isExt {
						continue
					}
					if derivedHeads[other.Pred] {
						// Anchor predicate is (at least partially) derived by a
						// rule; its facts aren't materialized yet at this point
						// in evaluation, so we cannot safely collect a bounded
						// candidate set. Leave this position unbound rather than
						// pushing down a partial (or empty) set.
						continue
					}
					for j, ot := range other.Terms {
						if ov, ok := ot.(datalog.Variable); ok && string(ov) == varName {
							anchored = true
							predID := dict.Intern(other.Pred)
							facts := existing.Get(predID, len(other.Terms))
							if pd.positions[i] == nil {
								pd.positions[i] = map[uint64]bool{}
							}
							for k := range facts {
								pd.positions[i][facts[k].Values[j]] = true
							}
							// facts may legitimately be empty (the anchor
							// predicate currently has zero rows): that's a
							// real, sound restriction for THIS occurrence
							// alone (its join can never match anything, so it
							// needs no candidate values), not a veto. Only
							// the absence of any anchor at all is a veto.
						}
					}
				}
				if !anchored {
					pd.unbounded[i] = true
				}
			}
		}
	}

	for _, r := range t.rules {
		collectFromBody(r.Body)
	}
	for _, ar := range t.aggRules {
		collectFromBody(ar.Body)
	}

	// Call each external function and materialize results.
	for predName, pd := range pushdowns {
		b := Bindings{Arity: pd.ep.arity}
		for pos, ids := range pd.positions {
			if pd.unbounded[pos] {
				// Some occurrence of this external needs every value at pos;
				// the veto dominates regardless of what other occurrences
				// collected here (see unbounded's doc comment above).
				continue
			}
			bt := BoundTerm{Position: pos}
			for id := range ids {
				bt.Values = append(bt.Values, dict.Resolve(id))
			}
			b.Bound = append(b.Bound, bt)
		}

		predID := dict.Intern(predName)
		for tuple := range pd.ep.fn(ctx, b) {
			if len(tuple) != pd.ep.arity {
				return fmt.Errorf("external predicate %s: expected tuples of arity %d, got %d", predName, pd.ep.arity, len(tuple))
			}
			var fact interned.InternedFact
			fact.Pred = predID
			fact.Arity = pd.ep.arity
			for j, v := range tuple {
				id, err := dict.InternUser(v)
				if err != nil {
					return fmt.Errorf("external predicate %s: %w", predName, err)
				}
				fact.Values[j] = id
			}
			existing.Add(fact)
		}
	}

	return nil
}

// internRuleConstants ensures all constants appearing in rules are interned
// in the dict before evaluation begins.
func (t *transformer) internRuleConstants(dict *interned.Dict) {
	internTerms := func(terms []datalog.Term) {
		for _, term := range terms {
			if c, ok := term.(datalog.Constant); ok {
				dict.InternConstant(c)
			}
		}
	}
	for _, r := range t.rules {
		internTerms(r.Head.Terms)
		for _, a := range r.Body {
			internTerms(a.Terms)
		}
	}
	for _, ar := range t.aggRules {
		internTerms(ar.Head.Terms)
		for _, a := range ar.Body {
			internTerms(a.Terms)
		}
		if ar.AggTerm != nil {
			if c, ok := ar.AggTerm.(datalog.Constant); ok {
				dict.InternConstant(c)
			}
		}
	}
}
