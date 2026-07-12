package seminaive

import (
	"cmp"
	"context"
	"fmt"
	"math"
	"regexp"
	"strings"
	"sync"

	"swdunlop.dev/pkg/datalog"
	"swdunlop.dev/pkg/datalog/internal/interned"
	"swdunlop.dev/pkg/datalog/syntax"
)

// regexCache caches compiled regular expressions for regex_match.
var regexCache sync.Map // map[string]*regexp.Regexp

// isConstraint reports whether an atom is an inline constraint (comparison or string builtin).
func isConstraint(a syntax.Atom) bool {
	switch a.Pred {
	case "=", "!=", "<", ">", "<=", ">=",
		"@contains", "@starts_with", "@ends_with", "@regex_match":
		return true
	}
	return false
}

// isBindBuiltin reports whether an atom is a binding builtin (produces a new variable binding).
// Convention: all args except the last are inputs; the last is the output.
func isBindBuiltin(a syntax.Atom, builtins map[string]BuiltinFunc) bool {
	if builtins == nil {
		return false
	}
	_, ok := builtins[a.Pred]
	return ok
}

// isMultiBindBuiltin reports whether an atom is a multi-result binding builtin.
// Convention: the last N args (N declared at registration) are outputs; the rest are inputs.
func isMultiBindBuiltin(a syntax.Atom, multiBuiltins map[string]multiBuiltin) bool {
	if multiBuiltins == nil {
		return false
	}
	_, ok := multiBuiltins[a.Pred]
	return ok
}

// isExternalPred reports whether an atom references a registered external predicate.
func isExternalPred(a syntax.Atom, externals map[string]externalPredicate) bool {
	if externals == nil {
		return false
	}
	_, ok := externals[a.Pred]
	return ok
}

func cachedRegexp(pattern string) (*regexp.Regexp, error) {
	if v, ok := regexCache.Load(pattern); ok {
		return v.(*regexp.Regexp), nil
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}
	regexCache.Store(pattern, re)
	return re, nil
}

// resolveCompiledTermID returns the interned ID for a compiled term under a VarSub.
func resolveCompiledTermID(t interned.CompiledTerm, sub *interned.VarSub) (uint64, bool) {
	if t.VarIdx >= 0 {
		if sub.Mask>>uint(t.VarIdx)&1 != 0 {
			return sub.Vals[t.VarIdx], true
		}
		return 0, false
	}
	return t.ConstID, true
}

// resolveCompiledTermValue returns the actual value for a compiled term under a VarSub.
func resolveCompiledTermValue(t interned.CompiledTerm, sub *interned.VarSub, dict *interned.Dict) (any, bool) {
	if t.VarIdx >= 0 {
		if sub.Mask>>uint(t.VarIdx)&1 != 0 {
			return dict.Resolve(sub.Vals[t.VarIdx]), true
		}
		return nil, false
	}
	return dict.Resolve(t.ConstID), true
}

// checkConstraintV evaluates a constraint using pre-compiled terms and a VarSub.
func checkConstraintV(pred string, terms []interned.CompiledTerm, sub *interned.VarSub, dict *interned.Dict) bool {
	if len(terms) < 2 {
		return false
	}
	ct0, ct1 := terms[0], terms[1]
	switch pred {
	case "=":
		lid, lok := resolveCompiledTermID(ct0, sub)
		rid, rok := resolveCompiledTermID(ct1, sub)
		if !lok || !rok {
			return false
		}
		if lid == rid {
			return true
		}
		// Identical interned IDs cover exact matches (including equal
		// strings/atoms); mixed int64/float64 constants intern to distinct
		// IDs even when numerically equal, so fall back to a numeric
		// comparison in that case.
		lhs, rhs := dict.Resolve(lid), dict.Resolve(rid)
		if c, ok := compareValues(lhs, rhs); ok {
			return c == 0
		}
		return false
	case "!=":
		lid, lok := resolveCompiledTermID(ct0, sub)
		rid, rok := resolveCompiledTermID(ct1, sub)
		if !lok || !rok {
			return false
		}
		if lid == rid {
			return false
		}
		lhs, rhs := dict.Resolve(lid), dict.Resolve(rid)
		if c, ok := compareValues(lhs, rhs); ok {
			return c != 0
		}
		return true
	case "@contains":
		lhs, lok := resolveCompiledTermValue(ct0, sub, dict)
		rhs, rok := resolveCompiledTermValue(ct1, sub, dict)
		if !lok || !rok {
			return false
		}
		ls, lOk := lhs.(string)
		rs, rOk := rhs.(string)
		return lOk && rOk && strings.Contains(ls, rs)
	case "@starts_with":
		lhs, lok := resolveCompiledTermValue(ct0, sub, dict)
		rhs, rok := resolveCompiledTermValue(ct1, sub, dict)
		if !lok || !rok {
			return false
		}
		ls, lOk := lhs.(string)
		rs, rOk := rhs.(string)
		return lOk && rOk && strings.HasPrefix(ls, rs)
	case "@ends_with":
		lhs, lok := resolveCompiledTermValue(ct0, sub, dict)
		rhs, rok := resolveCompiledTermValue(ct1, sub, dict)
		if !lok || !rok {
			return false
		}
		ls, lOk := lhs.(string)
		rs, rOk := rhs.(string)
		return lOk && rOk && strings.HasSuffix(ls, rs)
	case "@regex_match":
		lhs, lok := resolveCompiledTermValue(ct0, sub, dict)
		rhs, rok := resolveCompiledTermValue(ct1, sub, dict)
		if !lok || !rok {
			return false
		}
		s, sOk := lhs.(string)
		pattern, pOk := rhs.(string)
		if !sOk || !pOk {
			return false
		}
		re, err := cachedRegexp(pattern)
		if err != nil {
			return false
		}
		return re.MatchString(s)
	default:
		lhs, lok := resolveCompiledTermValue(ct0, sub, dict)
		rhs, rok := resolveCompiledTermValue(ct1, sub, dict)
		if !lok || !rok {
			return false
		}
		c, ok := compareValues(lhs, rhs)
		if !ok {
			return false
		}
		switch pred {
		case "<":
			return c < 0
		case ">":
			return c > 0
		case "<=":
			return c <= 0
		case ">=":
			return c >= 0
		}
	}
	return false
}

// evalBindBuiltin evaluates a binding builtin and returns the interned result ID.
func evalBindBuiltin(a syntax.Atom, sub interned.InternedSub, dict *interned.Dict, builtins map[string]BuiltinFunc) (uint64, bool) {
	fn, ok := builtins[a.Pred]
	if !ok {
		return 0, false
	}
	inputs := make([]any, len(a.Terms)-1)
	for i, t := range a.Terms[:len(a.Terms)-1] {
		id, ok := resolveTermID(t, sub, dict)
		if !ok {
			return 0, false
		}
		inputs[i] = dict.Resolve(id)
	}
	result, ok := fn(inputs)
	if !ok {
		return 0, false
	}
	return dict.Intern(result), true
}

// compareValues compares two values, mirroring the int/float mixing that
// applyBinOp supports for arithmetic. Mixed int64/float64 operands are
// compared numerically rather than rejected outright, so that (for example)
// an integer literal threshold matches a fact field that happens to decode
// as a float (or vice versa).
func compareValues(lhs, rhs any) (int, bool) {
	switch l := lhs.(type) {
	case int64:
		switch r := rhs.(type) {
		case int64:
			return cmp.Compare(l, r), true
		case float64:
			return compareInt64Float64(l, r), true
		}
	case float64:
		switch r := rhs.(type) {
		case float64:
			return cmp.Compare(l, r), true
		case int64:
			return -compareInt64Float64(r, l), true
		}
	case string:
		if r, ok := rhs.(string); ok {
			return cmp.Compare(l, r), true
		}
	}
	return 0, false
}

// compareInt64Float64 compares an int64 and a float64 exactly, without
// losing precision for int64 magnitudes beyond 2^53 (where float64 can no
// longer represent every integer exactly). NaN never compares equal, less,
// or greater in the usual sense; we treat it as unordered by placing it
// consistently below every int64 so that == is never (incorrectly) claimed
// and < / > remain false for it, matching float64 NaN comparison semantics
// as closely as a total order allows.
func compareInt64Float64(l int64, r float64) int {
	if math.IsNaN(r) {
		return -1
	}
	rFloor := math.Floor(r)
	// Compare the integral part of r against l using integer arithmetic when
	// rFloor is within int64 range; otherwise r's magnitude alone decides.
	if rFloor >= -9223372036854775808.0 && rFloor < 9223372036854775808.0 {
		ri := int64(rFloor)
		switch {
		case l < ri:
			return -1
		case l > ri:
			return 1
		default:
			// l == floor(r); any positive fractional part makes r larger.
			if r > rFloor {
				return -1
			}
			return 0
		}
	}
	if r < 0 {
		return 1
	}
	return -1
}

// resolveTermID returns the interned ID for a term under a substitution.
func resolveTermID(t datalog.Term, sub interned.InternedSub, dict *interned.Dict) (uint64, bool) {
	switch v := t.(type) {
	case datalog.Variable:
		return sub.Get(string(v))
	case datalog.Constant:
		return dict.InternConstant(v), true
	}
	return 0, false
}

// resolveTermValue returns the actual value for a term under a substitution.
func resolveTermValue(t datalog.Term, sub interned.InternedSub, dict *interned.Dict) (any, bool) {
	switch v := t.(type) {
	case datalog.Variable:
		id, ok := sub.Get(string(v))
		if !ok {
			return nil, false
		}
		return dict.Resolve(id), true
	case datalog.Constant:
		return constantToAny(v), true
	}
	return nil, false
}

// constantToAny extracts the Go primitive from a typed datalog.Constant.
func constantToAny(c datalog.Constant) any {
	switch v := c.(type) {
	case datalog.Float:
		return float64(v)
	case datalog.Integer:
		return int64(v)
	case datalog.String:
		return string(v)
	case datalog.ID:
		return v
	case datalog.Bool:
		return v
	case datalog.Null:
		return v
	case *datalog.Composite:
		return v
	}
	panic("unknown constant type")
}

// evalExpr evaluates an arithmetic expression under an interned substitution.
func evalExpr(expr syntax.Expr, sub interned.InternedSub, dict *interned.Dict) (uint64, bool) {
	switch e := expr.(type) {
	case syntax.TermExpr:
		return resolveTermID(e.Term, sub, dict)
	case syntax.BinExpr:
		lval, lok := evalExprValue(e.Left, sub, dict)
		rval, rok := evalExprValue(e.Right, sub, dict)
		if !lok || !rok {
			return 0, false
		}
		result, ok := applyBinOp(e.Op, lval, rval)
		if !ok {
			return 0, false
		}
		return dict.Intern(result), true
	}
	return 0, false
}

// evalExprValue evaluates an expression and returns the raw value (for arithmetic).
func evalExprValue(expr syntax.Expr, sub interned.InternedSub, dict *interned.Dict) (any, bool) {
	switch e := expr.(type) {
	case syntax.TermExpr:
		return resolveTermValue(e.Term, sub, dict)
	case syntax.BinExpr:
		lval, lok := evalExprValue(e.Left, sub, dict)
		rval, rok := evalExprValue(e.Right, sub, dict)
		if !lok || !rok {
			return nil, false
		}
		return applyBinOp(e.Op, lval, rval)
	}
	return nil, false
}

func applyBinOp(op string, lhs, rhs any) (any, bool) {
	switch l := lhs.(type) {
	case int64:
		switch r := rhs.(type) {
		case int64:
			switch op {
			case "+":
				return l + r, true
			case "-":
				return l - r, true
			case "*":
				return l * r, true
			case "/":
				if r == 0 {
					return nil, false
				}
				return l / r, true
			case "mod":
				if r == 0 {
					return nil, false
				}
				return l % r, true
			}
		case float64:
			return applyBinOpFloat(op, float64(l), r)
		}
	case float64:
		switch r := rhs.(type) {
		case float64:
			return applyBinOpFloat(op, l, r)
		case int64:
			return applyBinOpFloat(op, l, float64(r))
		}
	}
	return nil, false
}

func applyBinOpFloat(op string, l, r float64) (any, bool) {
	switch op {
	case "+":
		return l + r, true
	case "-":
		return l - r, true
	case "*":
		return l * r, true
	case "/":
		if r == 0 {
			return nil, false
		}
		return l / r, true
	}
	return nil, false
}

// bodyItemKind classifies a step in the ordered rule body evaluation sequence.
type bodyItemKind int

const (
	bodyItemJoin      bodyItemKind = iota // match atom against facts
	bodyItemIs                           // evaluate arithmetic expression, bind variable
	bodyItemCompare                      // check comparison/string constraint
	bodyItemBind                         // evaluate binding builtin
	bodyItemBindMulti                    // evaluate multi-result binding builtin
)

// compiledExpr is a pre-compiled arithmetic expression using VarSub indices.
type compiledExpr interface {
	compiledExpr()
}

type compiledTermExpr struct {
	term interned.CompiledTerm
}

func (compiledTermExpr) compiledExpr() {}

type compiledBinExpr struct {
	op    string
	left  compiledExpr
	right compiledExpr
}

func (compiledBinExpr) compiledExpr() {}

// compileExpr pre-compiles an expression using the variable index map.
func compileExpr(expr syntax.Expr, dict *interned.Dict, varMap map[string]int8) compiledExpr {
	if expr == nil {
		return nil
	}
	switch e := expr.(type) {
	case syntax.TermExpr:
		return compiledTermExpr{term: compileTermV(e.Term, dict, varMap)}
	case syntax.BinExpr:
		return compiledBinExpr{
			op:    e.Op,
			left:  compileExpr(e.Left, dict, varMap),
			right: compileExpr(e.Right, dict, varMap),
		}
	}
	return nil
}

// compileTermV compiles a single term using the variable index map.
func compileTermV(t datalog.Term, dict *interned.Dict, varMap map[string]int8) interned.CompiledTerm {
	switch v := t.(type) {
	case datalog.Variable:
		name := string(v)
		idx := int8(-1)
		if varMap != nil {
			if existing, ok := varMap[name]; ok {
				idx = existing
			}
		}
		return interned.CompiledTerm{VarName: name, VarIdx: idx}
	case datalog.Constant:
		return interned.CompiledTerm{VarIdx: -1, ConstID: dict.InternConstant(v)}
	}
	return interned.CompiledTerm{VarIdx: -1}
}

// evalExprIDV evaluates a compiled expression under a VarSub, returning an interned ID.
func evalExprIDV(expr compiledExpr, sub *interned.VarSub, dict *interned.Dict) (uint64, bool) {
	switch e := expr.(type) {
	case compiledTermExpr:
		return resolveCompiledTermID(e.term, sub)
	case compiledBinExpr:
		lval, lok := evalExprValueV(e.left, sub, dict)
		rval, rok := evalExprValueV(e.right, sub, dict)
		if !lok || !rok {
			return 0, false
		}
		result, ok := applyBinOp(e.op, lval, rval)
		if !ok {
			return 0, false
		}
		return dict.Intern(result), true
	}
	return 0, false
}

// evalExprValueV evaluates a compiled expression under a VarSub, returning the raw value.
func evalExprValueV(expr compiledExpr, sub *interned.VarSub, dict *interned.Dict) (any, bool) {
	switch e := expr.(type) {
	case compiledTermExpr:
		return resolveCompiledTermValue(e.term, sub, dict)
	case compiledBinExpr:
		lval, lok := evalExprValueV(e.left, sub, dict)
		rval, rok := evalExprValueV(e.right, sub, dict)
		if !lok || !rok {
			return nil, false
		}
		return applyBinOp(e.op, lval, rval)
	}
	return nil, false
}

// bodyItem is a single step in the rule body evaluation sequence.
type bodyItem struct {
	kind      bodyItemKind
	atom      syntax.Atom          // original atom (for is/comparison -- needs Terms/Expr)
	ca        interned.CompiledAtom // pre-compiled atom (for joins and negation)
	cExpr     compiledExpr         // pre-compiled expression (for bodyItemIs)
	joinIdx   int                  // for bodyItemJoin: index among join items (delta tracking)
	outVarIdx int8                 // for bodyItemIs/bodyItemBind: VarSub index of output variable (-1 if unused)
	multiOut  int                  // for bodyItemBindMulti: number of output positions (the atom's last multiOut terms)
}

// compileBody classifies each atom in a rule/query body into a bodyItem,
// assigning per-body variable indices as it goes. It is the single atom
// classification path shared by evalRules (plain rules) and evalAggregates
// (aggregate rule bodies), so both pipelines agree on how "is", constraints,
// binding builtins, and joins are compiled.
//
// Negated atoms are collected into a separate negativeBody slice rather than
// left inline: negation is stratified (its predicates are always fully
// decided in a strictly lower stratum, or -- for aggregate bodies -- fully
// decided before the aggregate body scan begins), so deferring every
// negation check to the end of the body is both sound and order-independent.
// This matters because rule bodies are free to write negated atoms before
// the positive atoms that bind their variables (see checkRuleSafety, which
// allows negated atoms' variables to be bound by *any* positive atom in the
// body, not just ones preceding it lexically); checking negation at its
// lexical position instead would spuriously fail on unbound variables or
// (worse) silently match everything via matchesAnyV's "no bound args"
// fallback.
//
// joinCount counts the positive join atoms (bodyItemJoin), which the caller
// uses both for delta-based semi-naive iteration and to detect join-free
// bodies.
func compileBody(body []syntax.Atom, dict *interned.Dict, builtins map[string]BuiltinFunc, multiBuiltins map[string]multiBuiltin) (items []bodyItem, negativeBody []interned.CompiledAtom, joinCount int, varMap map[string]int8) {
	varMap = make(map[string]int8)
	items = make([]bodyItem, 0, len(body))

	for _, a := range body {
		switch {
		case a.Negated:
			negativeBody = append(negativeBody, interned.CompileAtomV(a.Pred, a.Terms, dict, varMap))
		case a.Pred == "is":
			registerExprVars(a.Expr, varMap)
			outVarIdx := int8(-1)
			if v, ok := a.Terms[0].(datalog.Variable); ok {
				name := string(v)
				if idx, exists := varMap[name]; exists {
					outVarIdx = idx
				} else {
					outVarIdx = int8(len(varMap))
					varMap[name] = outVarIdx
				}
			}
			ce := compileExpr(a.Expr, dict, varMap)
			items = append(items, bodyItem{kind: bodyItemIs, atom: a, cExpr: ce, outVarIdx: outVarIdx})
		case isConstraint(a):
			items = append(items, bodyItem{kind: bodyItemCompare, atom: a, ca: interned.CompileAtomV(a.Pred, a.Terms, dict, varMap)})
		case isBindBuiltin(a, builtins):
			registerAtomVars(a, varMap)
			outVarIdx := int8(-1)
			if v, ok := a.Terms[len(a.Terms)-1].(datalog.Variable); ok {
				name := string(v)
				if idx, exists := varMap[name]; exists {
					outVarIdx = idx
				} else {
					outVarIdx = int8(len(varMap))
					varMap[name] = outVarIdx
				}
			}
			items = append(items, bodyItem{kind: bodyItemBind, atom: a, ca: interned.CompileAtomV(a.Pred, a.Terms, dict, varMap), outVarIdx: outVarIdx})
		case isMultiBindBuiltin(a, multiBuiltins):
			registerAtomVars(a, varMap)
			items = append(items, bodyItem{kind: bodyItemBindMulti, atom: a, ca: interned.CompileAtomV(a.Pred, a.Terms, dict, varMap), multiOut: multiBuiltins[a.Pred].outputs})
		default:
			items = append(items, bodyItem{kind: bodyItemJoin, ca: interned.CompileAtomV(a.Pred, a.Terms, dict, varMap), joinIdx: joinCount})
			joinCount++
		}
	}
	return items, negativeBody, joinCount, varMap
}

// evaluator holds per-evaluation state.
type evaluator struct {
	dict          *interned.Dict
	maxIter       int
	builtins      map[string]BuiltinFunc
	multiBuiltins map[string]multiBuiltin
}

// evalRules runs semi-naive evaluation for a set of rules to fixpoint.
// It returns an error if the fixpoint is not reached within maxIter
// iterations or the context is cancelled mid-evaluation; the partial
// results are discarded in either case.
func (ev *evaluator) evalRules(ctx context.Context, rules []syntax.Rule, existing interned.InternedFactSet, maxIter int) (factCount int, iterations int, err error) {
	emitted := interned.NewLightInternedFactSet()
	delta := interned.NewLightInternedFactSet()

	// Pre-compile all rules once, assigning per-rule variable indices.
	type compiledRule struct {
		head         interned.CompiledAtom
		body         []bodyItem
		negativeBody []interned.CompiledAtom
		joinCount    int
		varNames     []string
	}
	compiled := make([]compiledRule, 0, len(rules))
	for _, rule := range rules {
		var cr compiledRule
		items, negativeBody, joinCount, varMap := compileBody(rule.Body, ev.dict, ev.builtins, ev.multiBuiltins)
		cr.head = interned.CompileAtomV(rule.Head.Pred, rule.Head.Terms, ev.dict, varMap)
		cr.body = items
		cr.negativeBody = negativeBody
		cr.joinCount = joinCount
		// A rule with no positive join atom (body is only is-atoms, bind
		// builtins, constraints, and/or negation) can't grow across
		// iterations: negation is already fully decided (stratified), and
		// is/builtins/constraints are deterministic given their inputs, so
		// one pass with the empty substitution on iteration 0 is sound and
		// complete. It is still compiled and reordered like any other rule;
		// only the per-iteration dispatch below (see "iterations == 0"
		// below) treats it specially, running it once and never again.
		cr.body = reorderBody(cr.body)
		cr.varNames = make([]string, len(varMap))
		for name, idx := range varMap {
			cr.varNames[idx] = name
		}
		compiled = append(compiled, cr)
	}

	for iterations = range maxIter {
		if err := ctx.Err(); err != nil {
			return factCount, iterations, err
		}
		if iterations > 0 {
			existing.Merge(emitted)
			delta = emitted
			emitted = interned.NewLightInternedFactSet()
		}

		for _, cr := range compiled {
			emit := func(sub *interned.VarSub) {
				fact, fk, ok := interned.HashAndGroundV(cr.head, sub)
				if !ok {
					return
				}
				if _, exists := existing.Index[fk]; exists {
					return
				}
				if _, exists := emitted.Index[fk]; exists {
					return
				}
				emitted.AddUnchecked(fact, fk)
				factCount++
			}

			var sub interned.VarSub
			if iterations == 0 {
				ev.evalBodyRecursiveV(cr.body, cr.negativeBody, cr.varNames,
					-1, delta, existing, emitted, &sub, 0, emit)
			} else if cr.joinCount > 0 {
				// Join-free rules (joinCount == 0) were already fully
				// evaluated on iteration 0 above -- their derivations don't
				// depend on the delta, so re-running them here would either
				// no-op (facts already in existing/emitted) or, worse,
				// re-derive nothing new while wasting a full body scan every
				// iteration. Skip them entirely once iteration 0 has run.
				for deltaJoinIdx := range cr.joinCount {
					var deltaCA interned.CompiledAtom
					for _, item := range cr.body {
						if item.kind == bodyItemJoin && item.joinIdx == deltaJoinIdx {
							deltaCA = item.ca
							break
						}
					}
					if len(delta.Get(deltaCA.Pred, deltaCA.Arity)) == 0 {
						continue
					}
					sub = interned.VarSub{}
					ev.evalBodyRecursiveV(cr.body, cr.negativeBody, cr.varNames,
						deltaJoinIdx, delta, existing, emitted,
						&sub, 0, emit)
				}
			}
		}

		if len(emitted.Index) == 0 {
			break
		}
	}

	// The loop only leaves emitted non-empty when it ran out of iterations,
	// which means the results are incomplete, not converged.
	if len(emitted.Index) != 0 {
		return factCount, iterations + 1, fmt.Errorf(
			"fixpoint not reached after %d iterations; results would be incomplete (raise the limit with seminaive.WithMaxIterations)",
			maxIter)
	}

	existing.Merge(emitted)
	iterations++
	return factCount, iterations, nil
}

// registerAtomVars ensures all variables in an atom have indices in varMap.
func registerAtomVars(a syntax.Atom, varMap map[string]int8) {
	for _, t := range a.Terms {
		if v, ok := t.(datalog.Variable); ok {
			name := string(v)
			if _, exists := varMap[name]; !exists {
				varMap[name] = int8(len(varMap))
			}
		}
	}
}

// registerExprVars ensures all variables in an expression have indices in varMap.
func registerExprVars(expr syntax.Expr, varMap map[string]int8) {
	if expr == nil {
		return
	}
	switch e := expr.(type) {
	case syntax.TermExpr:
		if v, ok := e.Term.(datalog.Variable); ok {
			name := string(v)
			if _, exists := varMap[name]; !exists {
				varMap[name] = int8(len(varMap))
			}
		}
	case syntax.BinExpr:
		registerExprVars(e.Left, varMap)
		registerExprVars(e.Right, varMap)
	}
}

// varSubToInternedSub converts a VarSub to an InternedSub for use at
// constraint/builtin boundaries.
func varSubToInternedSub(vs *interned.VarSub, names []string) interned.InternedSub {
	s := make(interned.InternedSub, 0, len(names))
	for i, name := range names {
		if vs.Mask>>uint(i)&1 != 0 {
			s = append(s, interned.InternedSubEntry{Name: name, Value: vs.Vals[i]})
		}
	}
	return s
}

// evalBindMulti evaluates a multi-result builtin item. For each yielded
// output tuple it binds (or compare-checks) the output positions and calls
// next; the substitution mask is restored between tuples and on return.
func (ev *evaluator) evalBindMulti(item bodyItem, sub *interned.VarSub, next func()) {
	mb, ok := ev.multiBuiltins[item.atom.Pred]
	if !ok {
		return
	}
	nOut := item.multiOut
	nIn := item.ca.Arity - nOut
	if nIn < 0 {
		return
	}
	inputs := make([]any, nIn)
	for i := range nIn {
		v, ok := resolveCompiledTermValue(item.ca.Terms[i], sub, ev.dict)
		if !ok {
			return
		}
		inputs[i] = v
	}
	outTerms := item.ca.Terms[nIn:]
	savedMask := sub.Mask
	mb.fn(inputs, func(outputs []any) bool {
		if len(outputs) != nOut {
			return true
		}
		for j, t := range outTerms {
			valID := ev.dict.Intern(outputs[j])
			if t.VarIdx < 0 {
				// Constant output position acts as a filter.
				if t.ConstID != valID {
					sub.Mask = savedMask
					return true
				}
				continue
			}
			if sub.Mask>>uint(t.VarIdx)&1 != 0 {
				// Already-bound output gets equality semantics.
				if sub.Vals[t.VarIdx] != valID {
					sub.Mask = savedMask
					return true
				}
			} else {
				sub.Set(int(t.VarIdx), valID)
			}
		}
		next()
		sub.Mask = savedMask
		return true
	})
	sub.Mask = savedMask
}

// evalBodyRecursiveV evaluates a rule body using nested-loop join.
func (ev *evaluator) evalBodyRecursiveV(
	body []bodyItem,
	negativeBody []interned.CompiledAtom,
	varNames []string,
	deltaJoinIdx int,
	delta interned.InternedFactSet,
	existing interned.InternedFactSet,
	emitted interned.InternedFactSet,
	sub *interned.VarSub,
	bodyIdx int,
	emit func(*interned.VarSub),
) {
	if bodyIdx == len(body) {
		for _, neg := range negativeBody {
			if ev.matchesAnyV(neg, sub, existing, emitted) {
				return
			}
		}
		emit(sub)
		return
	}

	item := body[bodyIdx]
	switch item.kind {
	case bodyItemCompare:
		if checkConstraintV(item.atom.Pred, item.ca.Terms, sub, ev.dict) {
			ev.evalBodyRecursiveV(body, negativeBody, varNames, deltaJoinIdx, delta, existing, emitted, sub, bodyIdx+1, emit)
		}

	case bodyItemIs:
		var valID uint64
		var ok bool
		if item.cExpr != nil {
			valID, ok = evalExprIDV(item.cExpr, sub, ev.dict)
		} else {
			isub := varSubToInternedSub(sub, varNames)
			valID, ok = evalExpr(item.atom.Expr, isub, ev.dict)
		}
		if !ok {
			return
		}
		idx := item.outVarIdx
		if idx < 0 {
			return
		}
		if sub.Mask>>uint(idx)&1 != 0 {
			if sub.Vals[idx] == valID {
				ev.evalBodyRecursiveV(body, negativeBody, varNames, deltaJoinIdx, delta, existing, emitted, sub, bodyIdx+1, emit)
			}
		} else {
			sub.Set(int(idx), valID)
			ev.evalBodyRecursiveV(body, negativeBody, varNames, deltaJoinIdx, delta, existing, emitted, sub, bodyIdx+1, emit)
			sub.Clear(int(idx))
		}

	case bodyItemBind:
		isub := varSubToInternedSub(sub, varNames)
		valID, ok := evalBindBuiltin(item.atom, isub, ev.dict, ev.builtins)
		if !ok {
			return
		}
		idx := item.outVarIdx
		if idx < 0 {
			// Constant output position acts as a filter.
			if last := item.ca.Terms[item.ca.Arity-1]; last.VarIdx < 0 && last.ConstID == valID {
				ev.evalBodyRecursiveV(body, negativeBody, varNames, deltaJoinIdx, delta, existing, emitted, sub, bodyIdx+1, emit)
			}
			return
		}
		if sub.Mask>>uint(idx)&1 != 0 {
			if sub.Vals[idx] == valID {
				ev.evalBodyRecursiveV(body, negativeBody, varNames, deltaJoinIdx, delta, existing, emitted, sub, bodyIdx+1, emit)
			}
		} else {
			sub.Set(int(idx), valID)
			ev.evalBodyRecursiveV(body, negativeBody, varNames, deltaJoinIdx, delta, existing, emitted, sub, bodyIdx+1, emit)
			sub.Clear(int(idx))
		}

	case bodyItemBindMulti:
		ev.evalBindMulti(item, sub, func() {
			ev.evalBodyRecursiveV(body, negativeBody, varNames, deltaJoinIdx, delta, existing, emitted, sub, bodyIdx+1, emit)
		})

	case bodyItemJoin:
		ca := item.ca
		if item.joinIdx == deltaJoinIdx {
			if interned.AllTermsBoundV(ca, sub) {
				fact, ok := interned.GroundV(ca, sub)
				if ok {
					fk := interned.InternedFactHash(fact)
					if _, exists := delta.Index[fk]; exists {
						ev.evalBodyRecursiveV(body, negativeBody, varNames, deltaJoinIdx, delta, existing, emitted, sub, bodyIdx+1, emit)
					}
				}
				return
			}
			dbs := interned.BoundArgsV(ca, sub)
			savedMask := sub.Mask
			deltaFacts := delta.Get(ca.Pred, ca.Arity)
			for i := range deltaFacts {
				fact := &deltaFacts[i]
				if !interned.MatchesBound(&dbs, fact) {
					continue
				}
				if interned.UnifyV(ca, fact, sub) {
					ev.evalBodyRecursiveV(body, negativeBody, varNames, deltaJoinIdx, delta, existing, emitted, sub, bodyIdx+1, emit)
					sub.Mask = savedMask
				}
			}
		} else {
			if interned.AllTermsBoundV(ca, sub) {
				fact, ok := interned.GroundV(ca, sub)
				if ok {
					fk := interned.InternedFactHash(fact)
					if _, exists := existing.Index[fk]; exists {
						ev.evalBodyRecursiveV(body, negativeBody, varNames, deltaJoinIdx, delta, existing, emitted, sub, bodyIdx+1, emit)
					} else if _, exists := emitted.Index[fk]; exists {
						ev.evalBodyRecursiveV(body, negativeBody, varNames, deltaJoinIdx, delta, existing, emitted, sub, bodyIdx+1, emit)
					}
				}
				return
			}
			bs := interned.BoundArgsV(ca, sub)
			savedMask := sub.Mask
			existScan := existing.Scan(ca.Pred, ca.Arity, &bs)
			for i := range existScan.Len() {
				fact := existScan.Fact(i)
				if !interned.MatchesBound(&bs, fact) {
					continue
				}
				if interned.UnifyV(ca, fact, sub) {
					ev.evalBodyRecursiveV(body, negativeBody, varNames, deltaJoinIdx, delta, existing, emitted, sub, bodyIdx+1, emit)
					sub.Mask = savedMask
				}
			}
			emitScan := emitted.Scan(ca.Pred, ca.Arity, &bs)
			for i := range emitScan.Len() {
				fact := emitScan.Fact(i)
				if !interned.MatchesBound(&bs, fact) {
					continue
				}
				if interned.UnifyV(ca, fact, sub) {
					ev.evalBodyRecursiveV(body, negativeBody, varNames, deltaJoinIdx, delta, existing, emitted, sub, bodyIdx+1, emit)
					sub.Mask = savedMask
				}
			}
		}

	}
}

// matchesAnyV checks if a compiled (negated) atom matches any fact using VarSub.
// The VarSub is not modified on return (mask is saved/restored).
func (ev *evaluator) matchesAnyV(ca interned.CompiledAtom, sub *interned.VarSub, existing interned.InternedFactSet, emitted interned.InternedFactSet) bool {
	if interned.AllTermsBoundV(ca, sub) {
		fact, ok := interned.GroundV(ca, sub)
		if !ok {
			return false
		}
		fk := interned.InternedFactHash(fact)
		if _, exists := existing.Index[fk]; exists {
			return true
		}
		if _, exists := emitted.Index[fk]; exists {
			return true
		}
		return false
	}
	bs := interned.BoundArgsV(ca, sub)
	savedMask := sub.Mask
	existScan := existing.Scan(ca.Pred, ca.Arity, &bs)
	for i := range existScan.Len() {
		fact := existScan.Fact(i)
		if !interned.MatchesBound(&bs, fact) {
			continue
		}
		if interned.UnifyV(ca, fact, sub) {
			sub.Mask = savedMask
			return true
		}
	}
	emitScan := emitted.Scan(ca.Pred, ca.Arity, &bs)
	for i := range emitScan.Len() {
		fact := emitScan.Fact(i)
		if !interned.MatchesBound(&bs, fact) {
			continue
		}
		if interned.UnifyV(ca, fact, sub) {
			sub.Mask = savedMask
			return true
		}
	}
	return false
}

// reorderBody reorders rule body items for better join selectivity.
// Joins with more bound arguments (constants + already-bound variables)
// are placed first. Constraints and is-atoms are placed as soon as their
// input variables become bound.
func reorderBody(body []bodyItem) []bodyItem {
	if len(body) <= 1 {
		return body
	}

	n := len(body)
	placed := make([]bool, n)
	result := make([]bodyItem, 0, n)
	boundVars := uint16(0)

	for len(result) < n {
		// Phase 1: place any ready constraints/is/bind atoms.
		progress := true
		for progress {
			progress = false
			for i := range n {
				if placed[i] {
					continue
				}
				item := body[i]
				switch item.kind {
				case bodyItemCompare:
					if bodyItemVarsBound(item, boundVars) {
						placed[i] = true
						result = append(result, item)
						progress = true
					}
				case bodyItemIs:
					if exprVarsBound(item.cExpr, boundVars) {
						placed[i] = true
						result = append(result, item)
						if item.outVarIdx >= 0 {
							boundVars |= 1 << uint(item.outVarIdx)
						}
						progress = true
					}
				case bodyItemBind:
					if bodyItemInputVarsBound(item, boundVars) {
						placed[i] = true
						result = append(result, item)
						if item.outVarIdx >= 0 {
							boundVars |= 1 << uint(item.outVarIdx)
						}
						progress = true
					}
				case bodyItemBindMulti:
					if multiInputVarsBound(item, boundVars) {
						placed[i] = true
						result = append(result, item)
						for _, t := range item.ca.Terms[item.ca.Arity-item.multiOut:] {
							if t.VarIdx >= 0 {
								boundVars |= 1 << uint(t.VarIdx)
							}
						}
						progress = true
					}
				}
			}
		}

		// Phase 2: pick the best-scoring unplaced join.
		bestIdx := -1
		bestScore := -1.0
		for i := range n {
			if placed[i] || body[i].kind != bodyItemJoin {
				continue
			}
			score := joinSelectivityScore(body[i].ca, boundVars)
			if score > bestScore {
				bestScore = score
				bestIdx = i
			}
		}
		if bestIdx >= 0 {
			placed[bestIdx] = true
			item := body[bestIdx]
			result = append(result, item)
			for _, t := range item.ca.Terms {
				if t.VarIdx >= 0 {
					boundVars |= 1 << uint(t.VarIdx)
				}
			}
		} else {
			// Place any remaining items (shouldn't happen for safe rules).
			for i := range n {
				if !placed[i] {
					placed[i] = true
					result = append(result, body[i])
				}
			}
		}
	}

	return result
}

// joinSelectivityScore estimates how selective a join is given currently bound variables.
// Higher score = more selective = should be evaluated earlier.
func joinSelectivityScore(ca interned.CompiledAtom, boundVars uint16) float64 {
	if ca.Arity == 0 {
		return 1.0
	}
	boundCount := 0
	for i := range ca.Arity {
		t := ca.Terms[i]
		if t.VarIdx < 0 {
			boundCount++ // constant
		} else if boundVars>>uint(t.VarIdx)&1 != 0 {
			boundCount++ // already-bound variable
		}
	}
	return float64(boundCount) / float64(ca.Arity)
}

// bodyItemVarsBound checks if all variable terms in the item's compiled atom are bound.
func bodyItemVarsBound(item bodyItem, boundVars uint16) bool {
	for i := range item.ca.Arity {
		t := item.ca.Terms[i]
		if t.VarIdx >= 0 && boundVars>>uint(t.VarIdx)&1 == 0 {
			return false
		}
	}
	return true
}

// multiInputVarsBound checks if all input variables (all except the last
// multiOut output positions) of a multi-result builtin are bound.
func multiInputVarsBound(item bodyItem, boundVars uint16) bool {
	nIn := item.ca.Arity - item.multiOut
	for i := range nIn {
		t := item.ca.Terms[i]
		if t.VarIdx >= 0 && boundVars>>uint(t.VarIdx)&1 == 0 {
			return false
		}
	}
	return true
}

// bodyItemInputVarsBound checks if all input variables (all except the last/output) are bound.
func bodyItemInputVarsBound(item bodyItem, boundVars uint16) bool {
	if item.ca.Arity <= 1 {
		return true
	}
	for i := range item.ca.Arity - 1 {
		t := item.ca.Terms[i]
		if t.VarIdx >= 0 && boundVars>>uint(t.VarIdx)&1 == 0 {
			return false
		}
	}
	return true
}

// exprVarsBound checks if all variables in a compiled expression are bound.
func exprVarsBound(expr compiledExpr, boundVars uint16) bool {
	if expr == nil {
		return true
	}
	switch e := expr.(type) {
	case compiledTermExpr:
		if e.term.VarIdx >= 0 {
			return boundVars>>uint(e.term.VarIdx)&1 != 0
		}
		return true
	case compiledBinExpr:
		return exprVarsBound(e.left, boundVars) && exprVarsBound(e.right, boundVars)
	}
	return true
}

// queryInternedFacts evaluates a query/aggregate-rule body against a single
// static fact set, returning results as InternedSub for compatibility with
// aggregate grouping. It shares its atom classification (compileBody), body
// reordering (reorderBody), and join/negation evaluation (evalBodyRecursiveV)
// with the plain-rule pipeline in evalRules, so aggregate bodies get the same
// selectivity-driven ordering and the same deferred (order-independent)
// negation semantics as plain rules -- negation checked at the end of the
// body against fully-known facts, not at its lexical position.
//
// memFacts is treated as a single static "existing" fact set with an empty
// delta: aggregate bodies are evaluated once per stratum against the
// fully-converged lower strata (stratify.go requires every predicate an
// aggregate body touches, including transitively via the aggregated
// predicate, to sit in a strictly lower stratum), so there is no
// semi-naive delta to track here -- deltaJoinIdx is always -1 and every join
// scans memFacts directly, mirroring how evalRules itself falls back to a
// full existing/emitted scan on iteration 0.
func (ev *evaluator) queryInternedFacts(body []syntax.Atom, memFacts interned.InternedFactSet) []interned.InternedSub {
	items, negativeBody, _, varMap := compileBody(body, ev.dict, ev.builtins, ev.multiBuiltins)
	items = reorderBody(items)

	varNames := make([]string, len(varMap))
	for name, idx := range varMap {
		varNames[idx] = name
	}

	noDelta := interned.InternedFactSet{}
	noEmitted := interned.InternedFactSet{}

	var results []interned.InternedSub
	var sub interned.VarSub
	ev.evalBodyRecursiveV(items, negativeBody, varNames, -1, noDelta, memFacts, noEmitted, &sub, 0, func(vs *interned.VarSub) {
		results = append(results, varSubToInternedSub(vs, varNames))
	})
	return results
}
