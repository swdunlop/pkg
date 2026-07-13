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
// expensive part) and once per group while computing aggregates, matching
// evalRules' once-per-iteration granularity; queryInternedFacts additionally
// samples ctx during a single rule's body evaluation so one large aggregate
// body (e.g. a multi-way self cross-product) can't itself run uncancellably.
func (ev *evaluator) evalAggregates(ctx context.Context, aggRules []syntax.AggregateRule, memFacts interned.InternedFactSet) (interned.InternedFactSet, error) {
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

		// Compute aggregate for each group. groupsChecked counts groups
		// processed across all buckets so ctx is sampled every N groups
		// (evalStepsPerCheck) rather than on every single one, mirroring
		// the evaluator's countStep sampling during the join above.
		groupsChecked := 0
		for _, bucket := range groups {
			for _, g := range bucket {
				groupsChecked++
				if groupsChecked%evalStepsPerCheck == 0 {
					if err := ctx.Err(); err != nil {
						return result, err
					}
				}

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
			val, err := resolveInternedAggValue(aggVarName, aggConstID, aggIsConst, sub, dict)
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
// elsewhere in the group. NaN is unordered (compareValues reports !ok), so
// its position in the group could otherwise silently decide the result;
// error loudly instead, matching the non-numeric case.
func computeExtremum(
	name string, sign int,
	aggVarName string, aggConstID uint64, aggIsConst bool,
	subs []interned.InternedSub, dict *interned.Dict,
) (uint64, error) {
	var best any
	for _, sub := range subs {
		val, err := resolveInternedAggValue(aggVarName, aggConstID, aggIsConst, sub, dict)
		if err != nil {
			return 0, err
		}
		switch val.(type) {
		case int64, float64:
		default:
			return 0, fmt.Errorf("cannot compute %s of non-numeric value: %v", name, val)
		}
		if best == nil {
			if f, ok := val.(float64); ok && math.IsNaN(f) {
				return 0, fmt.Errorf("cannot compute %s of NaN", name)
			}
			best = val
			continue
		}
		c, ok := compareValues(val, best)
		if !ok {
			// int64/float64 mixes always compare; the only unorderable
			// numeric operand is NaN.
			return 0, fmt.Errorf("cannot compute %s of NaN", name)
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

func resolveInternedAggValue(
	varName string, constID uint64, isConst bool,
	sub interned.InternedSub, dict *interned.Dict,
) (any, error) {
	if isConst {
		return dict.Resolve(constID), nil
	}
	id, ok := sub.Get(varName)
	if !ok {
		return nil, fmt.Errorf("unbound variable %s in aggregate", varName)
	}
	return dict.Resolve(id), nil
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
