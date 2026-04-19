package seminaive

import (
	"cmp"
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
// Currently no builtins are registered; this is an extensibility hook.
func isBindBuiltin(a syntax.Atom) bool {
	return false
}

// checkConstraint evaluates a constraint under an interned substitution.
func checkConstraint(a syntax.Atom, sub interned.InternedSub, dict *interned.Dict) bool {
	switch a.Pred {
	case "=":
		lid, lok := resolveTermID(a.Terms[0], sub, dict)
		rid, rok := resolveTermID(a.Terms[1], sub, dict)
		return lok && rok && lid == rid
	case "!=":
		lid, lok := resolveTermID(a.Terms[0], sub, dict)
		rid, rok := resolveTermID(a.Terms[1], sub, dict)
		return lok && rok && lid != rid
	case "@contains":
		lhs, lok := resolveTermValue(a.Terms[0], sub, dict)
		rhs, rok := resolveTermValue(a.Terms[1], sub, dict)
		if !lok || !rok {
			return false
		}
		ls, lOk := lhs.(string)
		rs, rOk := rhs.(string)
		return lOk && rOk && strings.Contains(ls, rs)
	case "@starts_with":
		lhs, lok := resolveTermValue(a.Terms[0], sub, dict)
		rhs, rok := resolveTermValue(a.Terms[1], sub, dict)
		if !lok || !rok {
			return false
		}
		ls, lOk := lhs.(string)
		rs, rOk := rhs.(string)
		return lOk && rOk && strings.HasPrefix(ls, rs)
	case "@ends_with":
		lhs, lok := resolveTermValue(a.Terms[0], sub, dict)
		rhs, rok := resolveTermValue(a.Terms[1], sub, dict)
		if !lok || !rok {
			return false
		}
		ls, lOk := lhs.(string)
		rs, rOk := rhs.(string)
		return lOk && rOk && strings.HasSuffix(ls, rs)
	case "@regex_match":
		lhs, lok := resolveTermValue(a.Terms[0], sub, dict)
		rhs, rok := resolveTermValue(a.Terms[1], sub, dict)
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
		lhs, lok := resolveTermValue(a.Terms[0], sub, dict)
		rhs, rok := resolveTermValue(a.Terms[1], sub, dict)
		if !lok || !rok {
			return false
		}
		c, ok := compareValues(lhs, rhs)
		if !ok {
			return false
		}
		switch a.Pred {
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
		return lok && rok && lid == rid
	case "!=":
		lid, lok := resolveCompiledTermID(ct0, sub)
		rid, rok := resolveCompiledTermID(ct1, sub)
		return lok && rok && lid != rid
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
// Currently no builtins are registered; this is an extensibility hook.
func evalBindBuiltin(a syntax.Atom, sub interned.InternedSub, dict *interned.Dict) (uint64, bool) {
	return 0, false
}

// compareValues compares two values of the same type.
func compareValues(lhs, rhs any) (int, bool) {
	switch l := lhs.(type) {
	case int64:
		if r, ok := rhs.(int64); ok {
			return cmp.Compare(l, r), true
		}
	case float64:
		if r, ok := rhs.(float64); ok {
			return cmp.Compare(l, r), true
		}
	case string:
		if r, ok := rhs.(string); ok {
			return cmp.Compare(l, r), true
		}
	}
	return 0, false
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
	bodyItemJoin    bodyItemKind = iota // match atom against facts
	bodyItemIs                         // evaluate arithmetic expression, bind variable
	bodyItemCompare                    // check comparison/string constraint
	bodyItemBind                       // evaluate binding builtin
)

// bodyItem is a single step in the rule body evaluation sequence.
type bodyItem struct {
	kind      bodyItemKind
	atom      syntax.Atom          // original atom (for is/comparison -- needs Terms/Expr)
	ca        interned.CompiledAtom // pre-compiled atom (for joins and negation)
	joinIdx   int                  // for bodyItemJoin: index among join items (delta tracking)
	negated   bool                 // for negated atoms in query body
	outVarIdx int8                 // for bodyItemIs/bodyItemBind: VarSub index of output variable (-1 if unused)
}

// evaluator holds per-evaluation state.
type evaluator struct {
	dict    *interned.Dict
	maxIter int
}

// evalRules runs semi-naive evaluation for a set of rules to fixpoint.
func (ev *evaluator) evalRules(rules []syntax.Rule, existing interned.InternedFactSet, maxIter int) (factCount int, iterations int, err error) {
	emitted := interned.NewInternedFactSetCap(4096)
	delta := interned.NewLightInternedFactSet()

	type hashedFact struct {
		fact interned.InternedFact
		hash uint64
	}

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
		varMap := make(map[string]int8)
		cr.head = interned.CompileAtomV(rule.Head.Pred, rule.Head.Terms, ev.dict, varMap)
		for _, a := range rule.Body {
			switch {
			case a.Negated:
				cr.negativeBody = append(cr.negativeBody, interned.CompileAtomV(a.Pred, a.Terms, ev.dict, varMap))
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
				cr.body = append(cr.body, bodyItem{kind: bodyItemIs, atom: a, outVarIdx: outVarIdx})
			case isConstraint(a):
				cr.body = append(cr.body, bodyItem{kind: bodyItemCompare, atom: a, ca: interned.CompileAtomV(a.Pred, a.Terms, ev.dict, varMap)})
			case isBindBuiltin(a):
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
				cr.body = append(cr.body, bodyItem{kind: bodyItemBind, atom: a, outVarIdx: outVarIdx})
			default:
				cr.body = append(cr.body, bodyItem{kind: bodyItemJoin, ca: interned.CompileAtomV(a.Pred, a.Terms, ev.dict, varMap), joinIdx: cr.joinCount})
				cr.joinCount++
			}
		}
		if cr.joinCount == 0 {
			continue
		}
		cr.varNames = make([]string, len(varMap))
		for name, idx := range varMap {
			cr.varNames[idx] = name
		}
		compiled = append(compiled, cr)
	}

	for iterations = range maxIter {
		var newDeltaFacts []hashedFact

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
				emitted.AddWithKey(fact, fk)
				factCount++
				newDeltaFacts = append(newDeltaFacts, hashedFact{fact, fk})
			}

			var sub interned.VarSub
			if iterations == 0 {
				ev.evalBodyRecursiveV(cr.body, cr.negativeBody, cr.varNames,
					-1, delta, existing, emitted, &sub, 0, emit)
			} else {
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

		if len(newDeltaFacts) == 0 {
			break
		}

		delta = interned.NewLightInternedFactSet()
		for _, hf := range newDeltaFacts {
			delta.AddWithKey(hf.fact, hf.hash)
		}
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
		if len(negativeBody) > 0 {
			isub := varSubToInternedSub(sub, varNames)
			for _, neg := range negativeBody {
				if ev.matchesAnyCompiled(neg, isub, existing, emitted) {
					return
				}
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
		isub := varSubToInternedSub(sub, varNames)
		valID, ok := evalExpr(item.atom.Expr, isub, ev.dict)
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
		valID, ok := evalBindBuiltin(item.atom, isub, ev.dict)
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
			savedMask := sub.Mask
			for _, fact := range delta.Get(ca.Pred, ca.Arity) {
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
			for _, fact := range existing.Scan(ca.Pred, ca.Arity, &bs) {
				if interned.UnifyV(ca, fact, sub) {
					ev.evalBodyRecursiveV(body, negativeBody, varNames, deltaJoinIdx, delta, existing, emitted, sub, bodyIdx+1, emit)
					sub.Mask = savedMask
				}
			}
			for _, fact := range emitted.Scan(ca.Pred, ca.Arity, &bs) {
				if interned.UnifyV(ca, fact, sub) {
					ev.evalBodyRecursiveV(body, negativeBody, varNames, deltaJoinIdx, delta, existing, emitted, sub, bodyIdx+1, emit)
					sub.Mask = savedMask
				}
			}
		}
	}
}

// matchesAnyCompiled checks if a compiled (negated) atom matches any fact.
func (ev *evaluator) matchesAnyCompiled(ca interned.CompiledAtom, sub interned.InternedSub, existing interned.InternedFactSet, emitted interned.InternedFactSet) bool {
	if interned.AllTermsBound(ca, sub) {
		fact, ok := interned.GroundCompiled(ca, sub)
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
	bs := interned.BoundArgsCompiled(ca, sub)
	for _, fact := range existing.Scan(ca.Pred, ca.Arity, &bs) {
		if _, ok := interned.UnifyCompiled(ca, fact, sub); ok {
			return true
		}
	}
	for _, fact := range emitted.Scan(ca.Pred, ca.Arity, &bs) {
		if _, ok := interned.UnifyCompiled(ca, fact, sub); ok {
			return true
		}
	}
	return false
}

// compileQueryBody pre-compiles a query body into bodyItems.
func compileQueryBody(body []syntax.Atom, dict *interned.Dict) []bodyItem {
	items := make([]bodyItem, 0, len(body))
	for _, a := range body {
		switch {
		case a.Negated:
			items = append(items, bodyItem{kind: bodyItemJoin, ca: interned.CompileAtom(a.Pred, a.Terms, dict), negated: true})
		case a.Pred == "is":
			items = append(items, bodyItem{kind: bodyItemIs, atom: a})
		case isConstraint(a):
			items = append(items, bodyItem{kind: bodyItemCompare, atom: a})
		case isBindBuiltin(a):
			items = append(items, bodyItem{kind: bodyItemBind, atom: a})
		default:
			items = append(items, bodyItem{kind: bodyItemJoin, ca: interned.CompileAtom(a.Pred, a.Terms, dict)})
		}
	}
	return items
}

// queryInternedFacts evaluates a query body and returns interned substitutions.
// Used by aggregates to avoid de-intern/re-intern round-trips.
func (ev *evaluator) queryInternedFacts(body []syntax.Atom, memFacts interned.InternedFactSet) []interned.InternedSub {
	items := compileQueryBody(body, ev.dict)
	var results []interned.InternedSub
	ev.queryRecursive(items, memFacts, make(interned.InternedSub, 0, 8), 0, func(sub interned.InternedSub) {
		results = append(results, sub.Clone())
	})
	return results
}

func (ev *evaluator) queryRecursive(
	body []bodyItem,
	memFacts interned.InternedFactSet,
	sub interned.InternedSub,
	idx int,
	emit func(interned.InternedSub),
) {
	if idx == len(body) {
		emit(sub)
		return
	}

	item := body[idx]

	if item.negated {
		if !ev.matchesAnyCompiled(item.ca, sub, memFacts, interned.InternedFactSet{}) {
			ev.queryRecursive(body, memFacts, sub, idx+1, emit)
		}
		return
	}

	switch item.kind {
	case bodyItemCompare:
		if checkConstraint(item.atom, sub, ev.dict) {
			ev.queryRecursive(body, memFacts, sub, idx+1, emit)
		}

	case bodyItemIs:
		valID, ok := evalExpr(item.atom.Expr, sub, ev.dict)
		if !ok {
			return
		}
		lhsVar, isVar := item.atom.Terms[0].(datalog.Variable)
		if !isVar {
			return
		}
		name := string(lhsVar)
		if boundID, isBound := sub.Get(name); isBound {
			if boundID == valID {
				ev.queryRecursive(body, memFacts, sub, idx+1, emit)
			}
		} else {
			extSub := append(sub, interned.InternedSubEntry{Name: name, Value: valID})
			ev.queryRecursive(body, memFacts, extSub, idx+1, emit)
		}

	case bodyItemBind:
		valID, ok := evalBindBuiltin(item.atom, sub, ev.dict)
		if !ok {
			return
		}
		outTerm := item.atom.Terms[len(item.atom.Terms)-1]
		outVar, isVar := outTerm.(datalog.Variable)
		if !isVar {
			return
		}
		name := string(outVar)
		if boundID, isBound := sub.Get(name); isBound {
			if boundID == valID {
				ev.queryRecursive(body, memFacts, sub, idx+1, emit)
			}
		} else {
			extSub := append(sub, interned.InternedSubEntry{Name: name, Value: valID})
			ev.queryRecursive(body, memFacts, extSub, idx+1, emit)
		}

	case bodyItemJoin:
		ca := item.ca
		if interned.AllTermsBound(ca, sub) {
			fact, ok := interned.GroundCompiled(ca, sub)
			if ok {
				fk := interned.InternedFactHash(fact)
				if _, exists := memFacts.Index[fk]; exists {
					ev.queryRecursive(body, memFacts, sub, idx+1, emit)
				}
			}
			return
		}
		bs := interned.BoundArgsCompiled(ca, sub)
		for _, fact := range memFacts.Scan(ca.Pred, ca.Arity, &bs) {
			if extSub, ok := interned.UnifyCompiled(ca, fact, sub); ok {
				ev.queryRecursive(body, memFacts, extSub, idx+1, emit)
			}
		}
	}
}
