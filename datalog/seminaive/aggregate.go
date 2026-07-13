package seminaive

import (
	"context"
	"fmt"
	"math"

	"swdunlop.dev/pkg/datalog"
	"swdunlop.dev/pkg/datalog/internal/interned"
	"swdunlop.dev/pkg/datalog/syntax"
)

// evalAggregates evaluates aggregate rules against the current facts,
// returning new derived facts. All computation stays in interned space.
//
// ctx is checked once per aggregate rule (before running its body join, the
// expensive part) and, via the evaluator's shared countStep sampling, once
// every evalStepsPerCheck groups while computing aggregates -- the same
// sampling queryInternedFacts uses during a single rule's body evaluation, so
// one large aggregate body (e.g. a multi-way self cross-product) or a group
// loop over many groups can't itself run uncancellably.
func (ev *evaluator) evalAggregates(ctx context.Context, aggRules []syntax.AggregateRule, memFacts interned.InternedFactSet) (_ interned.InternedFactSet, err error) {
	defer recoverEvalError(ctx, &err)
	result := interned.NewLightInternedFactSet()

	for _, ar := range aggRules {
		if err := ctx.Err(); err != nil {
			return result, err
		}

		bindings, err := ev.queryInternedFacts(ctx, ar.Body, memFacts)
		if err != nil {
			return result, err
		}

		// Determine group-by variables: all variables in the head except ResultVar.
		var groupByVars []string
		for _, t := range ar.Head.Terms {
			if v, ok := t.(datalog.Variable); ok && string(v) != ar.ResultVar {
				groupByVars = append(groupByVars, string(v))
			}
		}

		// Resolve the aggregate term name (if it's a variable).
		var aggVarName string
		var aggConstID uint64
		var aggIsConst bool
		if v, ok := ar.AggTerm.(datalog.Variable); ok {
			aggVarName = string(v)
		}
		if c, ok := ar.AggTerm.(datalog.Constant); ok {
			aggConstID = ev.dict.InternConstant(c)
			aggIsConst = true
		}

		// Group bindings by group-by variable IDs using FNV-1a hash.
		type group struct {
			representative interned.InternedSub
			subs           []interned.InternedSub
		}
		groups := map[uint64][]group{}

		for _, sub := range bindings {
			gk := internedGroupKey(sub, groupByVars)
			bucket := groups[gk]
			matched := false
			for i := range bucket {
				if internedGroupEqual(sub, bucket[i].representative, groupByVars) {
					bucket[i].subs = append(bucket[i].subs, sub)
					matched = true
					break
				}
			}
			if !matched {
				bucket = append(bucket, group{representative: sub, subs: []interned.InternedSub{sub}})
			}
			groups[gk] = bucket
		}

		// Pre-compile head atom for grounding.
		head := interned.CompileAtom(ar.Head.Pred, ar.Head.Terms, ev.dict)

		// Compute aggregate for each group, sampling ctx via the evaluator's
		// shared countStep -- the same mechanism the join scans in
		// evalBodyRecursiveV and matchesAnyV use -- rather than a second,
		// independent per-group counter.
		for _, bucket := range groups {
			for _, g := range bucket {
				ev.countStep()

				aggResultID, err := computeInternedAggregate(
					ar.Kind, aggVarName, aggConstID, aggIsConst,
					g.subs, ev.dict,
				)
				if err != nil {
					return result, fmt.Errorf("aggregate %s: %w", ar.Kind, err)
				}

				resultSub := append(g.representative.Clone(),
					interned.InternedSubEntry{Name: ar.ResultVar, Value: aggResultID})

				if fact, ok := interned.GroundCompiled(head, resultSub); ok {
					result.Add(fact)
				}
			}
		}
	}

	return result, nil
}

func internedGroupKey(sub interned.InternedSub, vars []string) uint64 {
	h := uint64(interned.FNVOffset64)
	for _, v := range vars {
		val, _ := sub.Get(v)
		h ^= val
		h *= interned.FNVPrime64
	}
	return h
}

func internedGroupEqual(a, b interned.InternedSub, vars []string) bool {
	for _, v := range vars {
		av, _ := a.Get(v)
		bv, _ := b.Get(v)
		if av != bv {
			return false
		}
	}
	return true
}

func computeInternedAggregate(
	kind syntax.AggregateKind,
	aggVarName string, aggConstID uint64, aggIsConst bool,
	subs []interned.InternedSub, dict *interned.Dict,
) (uint64, error) {
	switch kind {
	case syntax.AggCount:
		return dict.Intern(int64(len(subs))), nil

	case syntax.AggSum:
		var sumInt int64
		var sumFloat float64
		isInt := true
		for _, sub := range subs {
			val, err := resolveInternedAggValue(aggVarName, aggConstID, aggIsConst, sub, dict, "sum")
			if err != nil {
				return 0, err
			}
			switch v := val.(type) {
			case int64:
				if isInt {
					next, overflow := addInt64Checked(sumInt, v)
					if overflow {
						return 0, fmt.Errorf("sum overflows int64: %d + %d", sumInt, v)
					}
					sumInt = next
				} else {
					sumFloat += float64(v)
				}
			case float64:
				if isInt {
					// Promote to float64, converting the running int64 total.
					// This conversion may itself lose precision for very
					// large accumulated sums; that's acceptable and matches
					// ordinary float semantics once a float is involved.
					sumFloat = float64(sumInt)
					isInt = false
				}
				sumFloat += v
			default:
				return 0, fmt.Errorf("cannot sum non-numeric value: %v", val)
			}
		}
		if isInt {
			return dict.Intern(sumInt), nil
		}
		return dict.Intern(sumFloat), nil

	case syntax.AggMin:
		return computeExtremum("min", -1, aggVarName, aggConstID, aggIsConst, subs, dict)

	case syntax.AggMax:
		return computeExtremum("max", 1, aggVarName, aggConstID, aggIsConst, subs, dict)

	default:
		return 0, fmt.Errorf("unknown aggregate kind: %v", kind)
	}
}

// computeExtremum computes min (sign = -1) or max (sign = +1) over a group.
// The running best is kept as its original int64 or float64 value and every
// candidate is compared with compareValues, whose mixed int64/float64 path
// is exact beyond 2^53 -- routing an int64 candidate through float64(v)
// (as an earlier version did) could return a "minimum" larger than the true
// minimum and not present in the input at all. Keeping the winner's original
// value also means an int64 extremum stays an int64 even when floats appear
// elsewhere in the group. NaN inputs are rejected by resolveInternedAggValue
// before they ever reach here, so the only remaining !ok from compareValues
// is a genuine non-numeric mismatch, not NaN.
func computeExtremum(
	name string, sign int,
	aggVarName string, aggConstID uint64, aggIsConst bool,
	subs []interned.InternedSub, dict *interned.Dict,
) (uint64, error) {
	var best any
	for _, sub := range subs {
		val, err := resolveInternedAggValue(aggVarName, aggConstID, aggIsConst, sub, dict, name)
		if err != nil {
			return 0, err
		}
		switch val.(type) {
		case int64, float64:
		default:
			return 0, fmt.Errorf("cannot compute %s of non-numeric value: %v", name, val)
		}
		if best == nil {
			best = val
			continue
		}
		c, ok := compareValues(val, best)
		if !ok {
			return 0, fmt.Errorf("cannot compute %s of non-numeric value: %v", name, val)
		}
		if sign*c > 0 {
			best = val
		}
	}
	if best == nil {
		// Groups are built from at least one body solution each, so this
		// only fires on a caller bug; fail loudly rather than fabricate a 0.
		return 0, fmt.Errorf("cannot compute %s of an empty group", name)
	}
	return dict.Intern(best), nil
}

// resolveInternedAggValue is the single chokepoint every numeric aggregate
// (sum, min, max, and avg if/when it exists) resolves its per-row value
// through. NaN is an admitted value at interning (dict.go's nanKey -- that
// trade-off is settled and not revisited here), but letting it flow into an
// aggregate silently produces bad results downstream: computeExtremum can't
// order it (a NaN could win or lose a min/max depending on its arbitrary
// position in the group) and AggSum would silently intern a NaN total that
// then compares unordered-false against everything else. So this is the one
// place that checks for it, uniformly, for every aggregate kind, and the
// error names both the aggregate (via name) and the offending predicate/rule
// context the caller already carries in its own wrapping (evalAggregates
// wraps this error with "aggregate %s: %w").
func resolveInternedAggValue(
	varName string, constID uint64, isConst bool,
	sub interned.InternedSub, dict *interned.Dict, name string,
) (any, error) {
	var val any
	if isConst {
		val = dict.Resolve(constID)
	} else {
		id, ok := sub.Get(varName)
		if !ok {
			return nil, fmt.Errorf("unbound variable %s in aggregate", varName)
		}
		val = dict.Resolve(id)
	}
	if f, ok := val.(float64); ok && math.IsNaN(f) {
		return nil, fmt.Errorf("cannot compute %s: NaN value in %s", name, describeAggSource(varName, isConst))
	}
	return val, nil
}

// describeAggSource names the aggregate's input for a NaN error message: the
// bound variable whose value is NaN, or "constant" when the aggregated term
// is a literal rather than a variable.
func describeAggSource(varName string, isConst bool) string {
	if isConst {
		return "constant"
	}
	return fmt.Sprintf("variable %s", varName)
}

// addInt64Checked adds a and b, reporting overflow instead of wrapping. This
// keeps int64 sums exact until the caller decides how to surface overflow,
// rather than silently corrupting values by routing through float64.
func addInt64Checked(a, b int64) (sum int64, overflow bool) {
	sum = a + b
	// Overflow occurred iff a and b have the same sign but the result's sign
	// differs from theirs.
	if (a >= 0) == (b >= 0) && (sum >= 0) != (a >= 0) {
		return 0, true
	}
	return sum, false
}
