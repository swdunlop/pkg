package seminaive

import (
	"context"
	"fmt"
	"iter"
	"sort"
	"time"

	"swdunlop.dev/pkg/datalog"
	"swdunlop.dev/pkg/datalog/internal/interned"
	_ "swdunlop.dev/pkg/datalog/memory" // register interned.Memory hook
	"swdunlop.dev/pkg/datalog/syntax"
)

// transformer implements datalog.Transformer using semi-naive evaluation.
type transformer struct {
	rules     []syntax.Rule
	aggRules  []syntax.AggregateRule
	facts     []datalog.Fact
	maxIter   int
	builtins  map[string]BuiltinFunc
	externals map[string]externalPredicate
	profile   func([]StratumStats)
}

var _ datalog.Transformer = (*transformer)(nil)

// Declarations returns the merged set of input and derived predicate declarations.
func (t *transformer) Declarations(ctx context.Context, input datalog.Database) iter.Seq[datalog.Declaration] {
	// Collect input declarations.
	seen := map[string]bool{}
	var decls []datalog.Declaration
	for d := range input.Declarations() {
		if !seen[d.Name] {
			seen[d.Name] = true
			decls = append(decls, d)
		}
	}

	// Add declarations for rule-head predicates not already declared.
	for _, r := range t.rules {
		if !seen[r.Head.Pred] {
			seen[r.Head.Pred] = true
			decls = append(decls, datalog.Declaration{Name: r.Head.Pred})
		}
	}
	for _, ar := range t.aggRules {
		if !seen[ar.Head.Pred] {
			seen[ar.Head.Pred] = true
			decls = append(decls, datalog.Declaration{Name: ar.Head.Pred})
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

	// Run evaluation if we have rules.
	if len(t.rules) > 0 || len(t.aggRules) > 0 {
		strata, err := stratify(t.rules, t.aggRules, t.builtins)
		if err != nil {
			return nil, fmt.Errorf("stratification: %w", err)
		}

		if len(t.externals) > 0 {
			if err := t.fetchExternals(ctx, dict, existing); err != nil {
				return nil, fmt.Errorf("external predicates: %w", err)
			}
		}

		ev := &evaluator{dict: dict, maxIter: t.maxIter, builtins: t.builtins}

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

			if len(s.rules) > 0 {
				factCount, iterations, err := ev.evalRules(s.rules, existing, t.maxIter)
				if err != nil {
					return nil, err
				}
				if stats != nil {
					ss.FactCount += factCount
					ss.Iterations = iterations
				}
			}

			if len(s.aggRules) > 0 {
				aggDerived, err := ev.evalAggregates(s.aggRules, existing)
				if err != nil {
					return nil, err
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

	// Merge declarations for derived predicates.
	seenDecl := map[string]bool{}
	for _, d := range decls {
		seenDecl[d.Name] = true
	}
	for _, r := range t.rules {
		if !seenDecl[r.Head.Pred] {
			seenDecl[r.Head.Pred] = true
			decls = append(decls, datalog.Declaration{Name: r.Head.Pred})
		}
	}
	for _, ar := range t.aggRules {
		if !seenDecl[ar.Head.Pred] {
			seenDecl[ar.Head.Pred] = true
			decls = append(decls, datalog.Declaration{Name: ar.Head.Pred})
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

	// Collect declarations.
	var decls []datalog.Declaration
	seenDecl := map[string]bool{}
	for d := range input.Declarations() {
		if !seenDecl[d.Name] {
			seenDecl[d.Name] = true
			decls = append(decls, d)
		}
	}

	// Discover predicates from declarations.
	predArities := map[string]map[int]bool{}
	for d := range input.Declarations() {
		if predArities[d.Name] == nil {
			predArities[d.Name] = map[int]bool{}
		}
		if len(d.Terms) > 0 {
			predArities[d.Name][len(d.Terms)] = true
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

	// Load facts for declared predicates.
	for pred, arities := range predArities {
		for arity := range arities {
			for row := range input.Facts(pred, arity) {
				if err := internFact(datalog.Fact{Name: pred, Terms: row}); err != nil {
					return nil, interned.InternedFactSet{}, nil, err
				}
			}
		}
	}

	// Load facts for predicates referenced in rules but not declared.
	loadUndeclaredPred := func(a syntax.Atom) error {
		if isConstraint(a) || isBindBuiltin(a, t.builtins) || a.Pred == "is" || isExternalPred(a, t.externals) {
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
	// For each external predicate, collect pushdown values per position.
	type pushdownInfo struct {
		ep        externalPredicate
		positions map[int]map[uint64]bool // position → set of interned value IDs
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
				pd = &pushdownInfo{ep: ep, positions: map[int]map[uint64]bool{}}
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

			// Collect values from anchor atoms via shared variables.
			for i, term := range a.Terms {
				v, ok := term.(datalog.Variable)
				if !ok {
					continue
				}
				varName := string(v)
				for _, other := range body {
					if isConstraint(other) || other.Pred == "is" || other.Negated {
						continue
					}
					if isBindBuiltin(other, t.builtins) {
						continue
					}
					if _, isExt := t.externals[other.Pred]; isExt {
						continue
					}
					for j, ot := range other.Terms {
						if ov, ok := ot.(datalog.Variable); ok && string(ov) == varName {
							predID := dict.Intern(other.Pred)
							facts := existing.Get(predID, len(other.Terms))
							if pd.positions[i] == nil {
								pd.positions[i] = map[uint64]bool{}
							}
							for k := range facts {
								pd.positions[i][facts[k].Values[j]] = true
							}
						}
					}
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
			bt := BoundTerm{Position: pos}
			for id := range ids {
				bt.Values = append(bt.Values, dict.Resolve(id))
			}
			b.Bound = append(b.Bound, bt)
		}

		predID := dict.Intern(predName)
		for tuple := range pd.ep.fn(ctx, b) {
			var fact interned.InternedFact
			fact.Pred = predID
			fact.Arity = pd.ep.arity
			for j, v := range tuple {
				if j >= interned.MaxFactArity {
					break
				}
				fact.Values[j] = dict.Intern(v)
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
