package seminaive

import (
	"fmt"
	"strings"
	"time"

	"swdunlop.dev/pkg/datalog"
	"swdunlop.dev/pkg/datalog/syntax"
)

const defaultMaxIter = 10000

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
	maxIter  int
	builtins map[string]BuiltinFunc
	profile  func([]StratumStats)
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
			if err := checkRuleSafety(r, e.builtins); err != nil {
				return nil, err
			}
			rules = append(rules, r)
		}
	}
	for _, ar := range ruleset.AggRules {
		// Aggregate rules are validated during evaluation.
		_ = ar
	}

	return &transformer{
		rules:    rules,
		aggRules: ruleset.AggRules,
		facts:    facts,
		maxIter:  e.maxIter,
		builtins: e.builtins,
		profile:  e.profile,
	}, nil
}

// checkRuleSafety ensures:
//   - Variables in is-expressions are bound by preceding positive atoms or is-atoms.
//   - The is-bound variable is then available for subsequent atoms and the head.
//   - Variables in negated atoms and comparison constraints are bound.
//   - All head variables are bound.
func checkRuleSafety(r syntax.Rule, builtins map[string]BuiltinFunc) error {
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
