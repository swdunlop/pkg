package seminaive

import (
	"fmt"
	"math"

	"swdunlop.dev/pkg/datalog"
	"swdunlop.dev/pkg/datalog/internal/interned"
	"swdunlop.dev/pkg/datalog/syntax"
)

// evalAggregates evaluates aggregate rules against the current facts,
// returning new derived facts. All computation stays in interned space.
func (ev *evaluator) evalAggregates(aggRules []syntax.AggregateRule, memFacts interned.InternedFactSet) (interned.InternedFactSet, error) {
	result := interned.NewInternedFactSet()

	for _, ar := range aggRules {
		bindings := ev.queryInternedFacts(ar.Body, memFacts)

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

		// Compute aggregate for each group.
		for _, bucket := range groups {
			for _, g := range bucket {
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
		sum := 0.0
		isInt := true
		for _, sub := range subs {
			val, err := resolveInternedAggValue(aggVarName, aggConstID, aggIsConst, sub, dict)
			if err != nil {
				return 0, err
			}
			switch v := val.(type) {
			case int64:
				sum += float64(v)
			case float64:
				sum += v
				isInt = false
			default:
				return 0, fmt.Errorf("cannot sum non-numeric value: %v", val)
			}
		}
		if isInt {
			return dict.Intern(int64(sum)), nil
		}
		return dict.Intern(sum), nil

	case syntax.AggMin:
		minVal := math.Inf(1)
		isInt := true
		for _, sub := range subs {
			val, err := resolveInternedAggValue(aggVarName, aggConstID, aggIsConst, sub, dict)
			if err != nil {
				return 0, err
			}
			switch v := val.(type) {
			case int64:
				if float64(v) < minVal {
					minVal = float64(v)
				}
			case float64:
				isInt = false
				if v < minVal {
					minVal = v
				}
			default:
				return 0, fmt.Errorf("cannot compute min of non-numeric value: %v", val)
			}
		}
		if isInt {
			return dict.Intern(int64(minVal)), nil
		}
		return dict.Intern(minVal), nil

	case syntax.AggMax:
		maxVal := math.Inf(-1)
		isInt := true
		for _, sub := range subs {
			val, err := resolveInternedAggValue(aggVarName, aggConstID, aggIsConst, sub, dict)
			if err != nil {
				return 0, err
			}
			switch v := val.(type) {
			case int64:
				if float64(v) > maxVal {
					maxVal = float64(v)
				}
			case float64:
				isInt = false
				if v > maxVal {
					maxVal = v
				}
			default:
				return 0, fmt.Errorf("cannot compute max of non-numeric value: %v", val)
			}
		}
		if isInt {
			return dict.Intern(int64(maxVal)), nil
		}
		return dict.Intern(maxVal), nil

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
