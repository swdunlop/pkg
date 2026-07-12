package seminaive

import (
	"context"
	"fmt"

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
		// (queryTuplesPerCheck) rather than on every single one, mirroring
		// queryInternedFacts' sampling of solutions during the join above.
		groupsChecked := 0
		for _, bucket := range groups {
			for _, g := range bucket {
				groupsChecked++
				if groupsChecked%queryTuplesPerCheck == 0 {
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
		var minInt int64
		var minFloat float64
		isInt := true
		haveVal := false
		for _, sub := range subs {
			val, err := resolveInternedAggValue(aggVarName, aggConstID, aggIsConst, sub, dict)
			if err != nil {
				return 0, err
			}
			switch v := val.(type) {
			case int64:
				if !haveVal {
					minInt = v
				} else if isInt {
					if v < minInt {
						minInt = v
					}
				} else {
					if float64(v) < minFloat {
						minFloat = float64(v)
					}
				}
			case float64:
				if !haveVal {
					minFloat = v
					isInt = false
				} else if isInt {
					// Promote to float64, converting the running int64 min exactly
					// (the min so far is a single value, so this conversion is
					// exact for any int64 that fits a float64 range comparison).
					if float64(minInt) < v {
						minFloat = float64(minInt)
					} else {
						minFloat = v
					}
					isInt = false
				} else {
					if v < minFloat {
						minFloat = v
					}
				}
			default:
				return 0, fmt.Errorf("cannot compute min of non-numeric value: %v", val)
			}
			haveVal = true
		}
		if isInt {
			return dict.Intern(minInt), nil
		}
		return dict.Intern(minFloat), nil

	case syntax.AggMax:
		var maxInt int64
		var maxFloat float64
		isInt := true
		haveVal := false
		for _, sub := range subs {
			val, err := resolveInternedAggValue(aggVarName, aggConstID, aggIsConst, sub, dict)
			if err != nil {
				return 0, err
			}
			switch v := val.(type) {
			case int64:
				if !haveVal {
					maxInt = v
				} else if isInt {
					if v > maxInt {
						maxInt = v
					}
				} else {
					if float64(v) > maxFloat {
						maxFloat = float64(v)
					}
				}
			case float64:
				if !haveVal {
					maxFloat = v
					isInt = false
				} else if isInt {
					if float64(maxInt) > v {
						maxFloat = float64(maxInt)
					} else {
						maxFloat = v
					}
					isInt = false
				} else {
					if v > maxFloat {
						maxFloat = v
					}
				}
			default:
				return 0, fmt.Errorf("cannot compute max of non-numeric value: %v", val)
			}
			haveVal = true
		}
		if isInt {
			return dict.Intern(maxInt), nil
		}
		return dict.Intern(maxFloat), nil

	default:
		return 0, fmt.Errorf("unknown aggregate kind: %v", kind)
	}
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
