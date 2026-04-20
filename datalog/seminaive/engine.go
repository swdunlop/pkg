package seminaive

import (
	"context"
	"fmt"
	"iter"
	"strings"
	"time"

	"swdunlop.dev/pkg/datalog"
	"swdunlop.dev/pkg/datalog/syntax"
)

const defaultMaxIter = 10000

// Bindings describes which term positions have known values during external predicate evaluation.
type Bindings struct {
	Arity int
	Bound []BoundTerm
}

// BoundTerm describes a term position with a set of known possible values (semi-join reduction).
// The external function receives the full set of values that could appear at this position,
// enabling efficient batch lookups rather than per-value API calls.
type BoundTerm struct {
	Position int
	Values   []any // all distinct possible values (string, int64, float64)
}

// ExternalFunc is called once per Transform with the complete set of pushed-down bindings.
// The Bindings describe the semi-join reduction — all possible values for bound positions.
// Returns an iterator of complete tuples (all positions filled with string, int64, or float64 values).
type ExternalFunc func(ctx context.Context, b Bindings) iter.Seq[[]any]

// externalPredicate holds the registered metadata for an external predicate.
type externalPredicate struct {
	arity int
	fn    ExternalFunc
}

// BuiltinFunc computes a derived value from resolved input arguments.
// Convention: all args except the last in the atom are inputs; the last is the output.
// Returns the result value to intern, or (nil, false) if the builtin cannot produce a result.
type BuiltinFunc func(inputs []any) (any, bool)

// StratumStats records evaluation metrics for a single stratum.
type StratumStats struct {
	Predicates []string
	RuleCount  int
	AggCount   int
	FactCount  int
	Iterations int
	Duration   time.Duration
}

// Engine implements syntax.Engine using semi-naive evaluation.
type Engine struct {
	maxIter   int
	builtins  map[string]BuiltinFunc
	externals map[string]externalPredicate
	decls     datalog.DeclarationSet
	profile   func([]StratumStats)
}

var _ syntax.Engine = (*Engine)(nil)

// Option configures an Engine.
type Option func(*Engine)

// WithMaxIterations sets the maximum number of fixpoint iterations.
func WithMaxIterations(n int) Option {
	return func(e *Engine) { e.maxIter = n }
}

// WithBuiltin registers a named binding builtin that can be used in rule bodies.
// The builtin predicate should start with "@" by convention.
func WithBuiltin(name string, fn BuiltinFunc) Option {
	return func(e *Engine) {
		if e.builtins == nil {
			e.builtins = make(map[string]BuiltinFunc)
		}
		e.builtins[name] = fn
	}
}

// WithExternal registers an external predicate that provides facts by calling back into Go.
// The engine passes known bindings (pushdown) so the function can query efficiently.
// Arity is required for compile-time validation of rules that reference this predicate.
func WithExternal(name string, arity int, fn ExternalFunc) Option {
	return func(e *Engine) {
		if e.externals == nil {
			e.externals = make(map[string]externalPredicate)
		}
		e.externals[name] = externalPredicate{arity: arity, fn: fn}
	}
}

// WithDeclarations registers predicate declarations for compile-time type checking.
// Constants in rule atoms are validated against declared types and arities.
func WithDeclarations(decls []datalog.Declaration) Option {
	return func(e *Engine) {
		e.decls = datalog.NewDeclarationSet(func(yield func(datalog.Declaration) bool) {
			for _, d := range decls {
				if !yield(d) {
					return
				}
			}
		})
	}
}

// WithProfile registers a callback that receives per-stratum evaluation statistics
// after each call to Transform.
func WithProfile(fn func([]StratumStats)) Option {
	return func(e *Engine) { e.profile = fn }
}

// New creates a new semi-naive Engine.
func New(options ...Option) *Engine {
	e := &Engine{maxIter: defaultMaxIter}
	for _, o := range options {
		o(e)
	}
	return e
}

// Compile validates the ruleset and returns a Transformer that applies the rules.
func (e *Engine) Compile(ruleset syntax.Ruleset) (datalog.Transformer, error) {
	// Separate facts from rules.
	var facts []datalog.Fact
	var rules []syntax.Rule
	for _, r := range ruleset.Rules {
		if r.IsFact() {
			facts = append(facts, r.ToFact())
		} else {
			if err := checkRuleSafety(r, e.builtins, e.externals); err != nil {
				return nil, err
			}
			if len(e.decls) > 0 {
				if err := checkRuleTypes(r, e.decls); err != nil {
					return nil, err
				}
			}
			rules = append(rules, r)
		}
	}
	if len(e.decls) > 0 {
		for _, ar := range ruleset.AggRules {
			if err := checkAggRuleTypes(ar, e.decls); err != nil {
				return nil, err
			}
		}
	}

	return &transformer{
		rules:     rules,
		aggRules:  ruleset.AggRules,
		facts:     facts,
		maxIter:   e.maxIter,
		builtins:  e.builtins,
		externals: e.externals,
		profile:   e.profile,
	}, nil
}

// checkRuleSafety ensures:
//   - Variables in is-expressions are bound by preceding positive atoms or is-atoms.
//   - The is-bound variable is then available for subsequent atoms and the head.
//   - Variables in negated atoms and comparison constraints are bound.
//   - All head variables are bound.
func checkRuleSafety(r syntax.Rule, builtins map[string]BuiltinFunc, externals map[string]externalPredicate) error {
	bound := map[string]bool{}
	for _, a := range r.Body {
		switch {
		case a.Pred == "is":
			if a.Expr != nil {
				if err := checkExprSafety(a.Expr, bound); err != nil {
					return err
				}
			}
			if v, ok := a.Terms[0].(datalog.Variable); ok {
				bound[string(v)] = true
			}
		case isBindBuiltin(a, builtins):
			// Input args (all except last) must be bound.
			for _, t := range a.Terms[:len(a.Terms)-1] {
				if v, ok := t.(datalog.Variable); ok {
					if !bound[string(v)] {
						return fmt.Errorf("unsafe rule: variable %s in %s not bound", string(v), a.Pred)
					}
				}
			}
			// Output arg (last) becomes bound.
			if v, ok := a.Terms[len(a.Terms)-1].(datalog.Variable); ok {
				bound[string(v)] = true
			}
		case a.Negated, isConstraint(a):
			// Checked in second pass below.
		default:
			// Regular joins and external predicates bind all variables.
			if ep, ok := externals[a.Pred]; ok {
				if ep.arity != len(a.Terms) {
					return fmt.Errorf("external predicate %s: expected arity %d, got %d", a.Pred, ep.arity, len(a.Terms))
				}
			}
			for _, t := range a.Terms {
				if v, ok := t.(datalog.Variable); ok {
					bound[string(v)] = true
				}
			}
		}
	}

	// Second pass: check negated atoms and comparisons.
	for _, a := range r.Body {
		if !a.Negated && !isConstraint(a) {
			continue
		}
		for _, t := range a.Terms {
			if v, ok := t.(datalog.Variable); ok {
				name := string(v)
				if isAnonymousVar(name) {
					continue
				}
				if !bound[name] {
					label := "negated atom " + a.Pred
					if isConstraint(a) {
						label = "comparison " + a.Pred
					}
					return fmt.Errorf("unsafe rule: variable %s in %s not bound", name, label)
				}
			}
		}
	}

	// Check head variables are bound.
	for _, t := range r.Head.Terms {
		if v, ok := t.(datalog.Variable); ok {
			if !bound[string(v)] {
				return fmt.Errorf("unsafe rule: head variable %s not bound", string(v))
			}
		}
	}
	return nil
}

// isAnonymousVar returns true for parser-generated anonymous variables (?0, ?1, ...).
func isAnonymousVar(name string) bool {
	return strings.HasPrefix(name, "?") && len(name) > 1 && name[1] >= '0' && name[1] <= '9'
}

// checkExprSafety verifies all variables in an arithmetic expression are bound.
func checkExprSafety(expr syntax.Expr, bound map[string]bool) error {
	switch e := expr.(type) {
	case syntax.TermExpr:
		if v, ok := e.Term.(datalog.Variable); ok {
			if !bound[string(v)] {
				return fmt.Errorf("unsafe rule: variable %s in is-expression not bound by positive atom", string(v))
			}
		}
	case syntax.BinExpr:
		if err := checkExprSafety(e.Left, bound); err != nil {
			return err
		}
		return checkExprSafety(e.Right, bound)
	}
	return nil
}

// checkRuleTypes validates constant types and arities in a rule against declarations.
func checkRuleTypes(r syntax.Rule, ds datalog.DeclarationSet) error {
	if err := ds.CheckAtom(r.Head.Pred, r.Head.Terms); err != nil {
		return fmt.Errorf("rule head: %w", err)
	}
	for _, a := range r.Body {
		if isConstraint(a) || a.Pred == "is" {
			continue
		}
		if err := ds.CheckAtom(a.Pred, a.Terms); err != nil {
			return fmt.Errorf("rule body: %w", err)
		}
	}
	return nil
}

// checkAggRuleTypes validates constant types and arities in an aggregate rule against declarations.
func checkAggRuleTypes(ar syntax.AggregateRule, ds datalog.DeclarationSet) error {
	if err := ds.CheckAtom(ar.Head.Pred, ar.Head.Terms); err != nil {
		return fmt.Errorf("aggregate rule head: %w", err)
	}
	for _, a := range ar.Body {
		if isConstraint(a) || a.Pred == "is" {
			continue
		}
		if err := ds.CheckAtom(a.Pred, a.Terms); err != nil {
			return fmt.Errorf("aggregate rule body: %w", err)
		}
	}
	return nil
}
