package seminaive

import (
	"context"
	"fmt"
	"iter"
	"strings"
	"time"

	"swdunlop.dev/pkg/datalog"
	"swdunlop.dev/pkg/datalog/internal/interned"
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

// MultiBuiltinFunc yields zero or more output tuples for the given inputs.
// Convention: the last N args of the atom (N declared at registration) are
// outputs; the rest are inputs. Yield returns false to stop enumeration.
type MultiBuiltinFunc func(inputs []any, yield func(outputs []any) bool)

// multiBuiltin holds a registered multi-result builtin and its output count.
type multiBuiltin struct {
	outputs int
	fn      MultiBuiltinFunc
}

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
	maxIter       int
	builtins      map[string]BuiltinFunc
	multiBuiltins map[string]multiBuiltin
	externals     map[string]externalPredicate
	decls         datalog.DeclarationSet
	profile       func([]StratumStats)
}

var _ syntax.Engine = (*Engine)(nil)

// Option configures an Engine.
type Option func(*Engine)

// WithMaxIterations sets the maximum number of fixpoint iterations.
// Transform returns an error if a stratum has not converged when the
// limit is reached, rather than returning incomplete results.
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

// WithMultiBuiltin registers a named multi-result builtin that can be used in
// rule bodies. The builtin predicate should start with "@" by convention.
// The last `outputs` args of the atom are output positions; the rest are inputs.
func WithMultiBuiltin(name string, outputs int, fn MultiBuiltinFunc) Option {
	return func(e *Engine) {
		if e.multiBuiltins == nil {
			e.multiBuiltins = make(map[string]multiBuiltin)
		}
		e.multiBuiltins[name] = multiBuiltin{outputs: outputs, fn: fn}
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

// New creates a new semi-naive Engine. The JSON destructuring builtins
// (@json_get, @json_len, @json_type, @json_slice, @json_each, @json_items)
// are always registered; options may override them by name.
func New(options ...Option) *Engine {
	e := &Engine{maxIter: defaultMaxIter}
	registerJSONBuiltins(e)
	for _, o := range options {
		o(e)
	}
	return e
}

// Compile validates the ruleset and returns a Transformer that applies the rules.
func (e *Engine) Compile(ruleset syntax.Ruleset) (datalog.Transformer, error) {
	for name, ep := range e.externals {
		if ep.arity < 1 || ep.arity > interned.MaxFactArity {
			return nil, fmt.Errorf("external predicate %s: arity %d out of range [1, %d]",
				name, ep.arity, interned.MaxFactArity)
		}
	}

	// Separate facts from rules.
	var facts []datalog.Fact
	var rules []syntax.Rule
	for _, r := range ruleset.Rules {
		if r.IsFact() {
			facts = append(facts, r.ToFact())
		} else {
			if err := checkRuleSafety(r, e.builtins, e.multiBuiltins, e.externals); err != nil {
				return nil, err
			}
			if err := checkRuleVarLimit(r.Head, r.Body); err != nil {
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
	for _, ar := range ruleset.AggRules {
		if err := checkAggRuleSafety(ar, e.builtins, e.multiBuiltins, e.externals); err != nil {
			return nil, err
		}
		if err := checkRuleVarLimit(ar.Head, ar.Body); err != nil {
			return nil, err
		}
		if len(e.decls) > 0 {
			if err := checkAggRuleTypes(ar, e.decls); err != nil {
				return nil, err
			}
		}
	}

	// Strata depend only on the ruleset, so compute them once here; this
	// also makes unstratifiable programs fail at compile time like every
	// other rule error.
	var strata []stratum
	if len(rules) > 0 || len(ruleset.AggRules) > 0 {
		s, err := stratify(rules, ruleset.AggRules, e.builtins, e.multiBuiltins)
		if err != nil {
			return nil, fmt.Errorf("stratification: %w", err)
		}
		strata = s
	}

	return &transformer{
		rules:         rules,
		aggRules:      ruleset.AggRules,
		strata:        strata,
		facts:         facts,
		maxIter:       e.maxIter,
		builtins:      e.builtins,
		multiBuiltins: e.multiBuiltins,
		externals:     e.externals,
		profile:       e.profile,
	}, nil
}

// checkRuleSafety ensures:
//   - Variables in is-expressions are bound by preceding positive atoms or is-atoms.
//   - The is-bound variable is then available for subsequent atoms and the head.
//   - Variables in negated atoms and comparison constraints are bound.
//   - All head variables are bound.
func checkRuleSafety(r syntax.Rule, builtins map[string]BuiltinFunc, multiBuiltins map[string]multiBuiltin, externals map[string]externalPredicate) error {
	bound, err := checkBodySafety(r.Body, builtins, multiBuiltins, externals)
	if err != nil {
		return err
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

// checkAggRuleSafety applies the same body-safety analysis as checkRuleSafety
// to an aggregate rule's body, then checks that every group-by variable in
// the head (all head variables except ResultVar) and the aggregated variable
// (AggTerm, when it's a variable rather than a constant) are bound by the
// body. Without this, a group-by or aggregated variable that the body never
// binds would silently compute over an empty/nonsensical grouping and then
// be dropped when GroundCompiled fails to ground the head, rather than
// failing at compile time like every other unsafe rule.
func checkAggRuleSafety(ar syntax.AggregateRule, builtins map[string]BuiltinFunc, multiBuiltins map[string]multiBuiltin, externals map[string]externalPredicate) error {
	bound, err := checkBodySafety(ar.Body, builtins, multiBuiltins, externals)
	if err != nil {
		return err
	}

	for _, t := range ar.Head.Terms {
		v, ok := t.(datalog.Variable)
		if !ok || string(v) == ar.ResultVar {
			continue
		}
		if !bound[string(v)] {
			return fmt.Errorf("unsafe aggregate rule: group-by variable %s not bound", string(v))
		}
	}

	if v, ok := ar.AggTerm.(datalog.Variable); ok {
		if !bound[string(v)] {
			return fmt.Errorf("unsafe aggregate rule: aggregated variable %s not bound", string(v))
		}
	}
	return nil
}

// checkBodySafety walks a rule/aggregate-rule body and returns the set of
// variables that end up bound by its positive atoms (joins, is-atoms, and
// binding builtins), after verifying that every negated atom, comparison
// constraint, and binding-builtin/external input is itself bound by some
// positive atom in the body. It does not check the head; callers apply their
// own head-shape-specific checks (see checkRuleSafety and
// checkAggRuleSafety) using the returned bound set.
func checkBodySafety(body []syntax.Atom, builtins map[string]BuiltinFunc, multiBuiltins map[string]multiBuiltin, externals map[string]externalPredicate) (map[string]bool, error) {
	bound := map[string]bool{}
	for _, a := range body {
		switch {
		case a.Pred == "is":
			if a.Expr != nil {
				if err := checkExprSafety(a.Expr, bound); err != nil {
					return nil, err
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
						return nil, fmt.Errorf("unsafe rule: variable %s in %s not bound", string(v), a.Pred)
					}
				}
			}
			// Output arg (last) becomes bound.
			if v, ok := a.Terms[len(a.Terms)-1].(datalog.Variable); ok {
				bound[string(v)] = true
			}
		case isMultiBindBuiltin(a, multiBuiltins):
			// Input args (all except the declared outputs) must be bound.
			nOut := multiBuiltins[a.Pred].outputs
			if len(a.Terms) < nOut {
				return nil, fmt.Errorf("builtin %s: expected at least %d args, got %d", a.Pred, nOut, len(a.Terms))
			}
			for _, t := range a.Terms[:len(a.Terms)-nOut] {
				if v, ok := t.(datalog.Variable); ok {
					if !bound[string(v)] {
						return nil, fmt.Errorf("unsafe rule: variable %s in %s not bound", string(v), a.Pred)
					}
				}
			}
			// Output args become bound.
			for _, t := range a.Terms[len(a.Terms)-nOut:] {
				if v, ok := t.(datalog.Variable); ok {
					bound[string(v)] = true
				}
			}
		case a.Negated, isConstraint(a):
			// Checked in second pass below.
		default:
			// Regular joins and external predicates bind all variables.
			if ep, ok := externals[a.Pred]; ok {
				if ep.arity != len(a.Terms) {
					return nil, fmt.Errorf("external predicate %s: expected arity %d, got %d", a.Pred, ep.arity, len(a.Terms))
				}
			}
			for _, t := range a.Terms {
				if v, ok := t.(datalog.Variable); ok {
					bound[string(v)] = true
				}
			}
		}
	}

	// Second pass: check negated atoms and comparisons. Negated atoms'
	// variables need only be bound by *some* positive atom in the body, not
	// one preceding them lexically -- evaluation defers all negation checks
	// to the end of the body (see compileBody/evalBodyRecursiveV), so
	// safety analysis must accept that ordering too.
	for _, a := range body {
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
					return nil, fmt.Errorf("unsafe rule: variable %s in %s not bound", name, label)
				}
			}
		}
	}

	return bound, nil
}

// checkRuleVarLimit errors when a rule uses more distinct variables than the
// evaluator's fixed-size substitution supports. Destructuring patterns consume
// fresh variables for intermediates, so pattern-heavy rules can hit this.
func checkRuleVarLimit(head syntax.Atom, body []syntax.Atom) error {
	vars := map[string]bool{}
	addTerms := func(terms []datalog.Term) {
		for _, t := range terms {
			if v, ok := t.(datalog.Variable); ok {
				vars[string(v)] = true
			}
		}
	}
	var addExpr func(expr syntax.Expr)
	addExpr = func(expr syntax.Expr) {
		switch e := expr.(type) {
		case syntax.TermExpr:
			addTerms([]datalog.Term{e.Term})
		case syntax.BinExpr:
			addExpr(e.Left)
			addExpr(e.Right)
		}
	}
	addTerms(head.Terms)
	for _, a := range body {
		addTerms(a.Terms)
		if a.Expr != nil {
			addExpr(a.Expr)
		}
	}
	if len(vars) > interned.MaxRuleVars {
		return fmt.Errorf("rule %s uses %d distinct variables (including pattern intermediates); the maximum is %d",
			head.Pred, len(vars), interned.MaxRuleVars)
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
