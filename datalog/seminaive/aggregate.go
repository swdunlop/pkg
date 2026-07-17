package seminaive

import (
	"context"
	"fmt"
	"math"

	"swdunlop.dev/pkg/datalog"
	"swdunlop.dev/pkg/datalog/internal/interned"
	"swdunlop.dev/pkg/datalog/syntax"
)

// aggGroup accumulates one aggregate group as it streams past: keyVals is
// the group-by tuple that identifies it (extracted once, when the group is
// first seen), values holds the aggregated variable's interned id per row
// (streamed straight from the join callback), and count is the row count
// (kept separately so AggCount doesn't need values populated at all).
// Nothing about a row beyond its group-by key and aggregated value is ever
// needed, so a group never holds a full VarSub or a name-keyed InternedSub
// per row -- see evalAggregates.
//
// samples is provenance-only (nil for the whole run when ev.rec == nil,
// see evalAggregates): the first witnessSampleCap solutions' own ground
// body/detail, captured as the group streams past and never grown beyond
// the cap -- so a count over 500k solutions costs exactly cap retained
// samples per group, not 500k. count still tracks the true cardinality
// independent of how many samples were kept, which is what lets the
// rendered explanation say "aggregated over 500000 solutions (first 10
// shown)" honestly.
type aggGroup struct {
	keyVals []uint64
	values  []uint64
	count   int
	samples []witnessSample
}

// evalAggregates evaluates aggregate rules against the current facts,
// returning new derived facts. All computation stays in interned space.
//
// Grouping streams straight out of the body's join evaluation
// (evalBodyRecursiveV) instead of first collecting every body solution into
// a name-keyed InternedSub (an allocation per solution) or even a raw
// VarSub (a fixed 16-slot array, most of it wasted for the handful of
// variables a typical body actually uses) and only then grouping: each
// solution's group-by key is read directly off the VarSub at pre-resolved
// indices (groupByIdx/aggVarIdx, resolved once per rule below) and folded
// into its aggGroup's compact per-row value slice, so per-solution cost is
// a map lookup plus appending one or two uint64s, not materializing and
// then re-scanning a whole substitution.
//
// ctx is checked once per aggregate rule (before running its body join, the
// expensive part) and, via the evaluator's shared countStep sampling, once
// every evalStepsPerCheck groups while computing aggregates -- the same
// sampling the join scans inside evalBodyRecursiveV use during a single
// rule's body evaluation, so one large aggregate body (e.g. a multi-way self
// cross-product) or a group loop over many groups can't itself run
// uncancellably.
//
// aggRuleIdx[i] is aggRules[i]'s index into the compiled Transformer's flat
// aggRules list (see stratify.go's stratum.aggRuleIdx and
// Provenance.aggRules) -- the aggregate mirror of evalRules' ruleIdx
// parameter, carried so a recorded aggregate witness can name which
// aggregate rule fired in a way that survives stratification's regrouping.
// Unused when ev.rec is nil.
func (ev *evaluator) evalAggregates(ctx context.Context, aggRules []syntax.AggregateRule, aggRuleIdx []int, memFacts interned.InternedFactSet) (_ interned.InternedFactSet, err error) {
	defer recoverEvalError(ctx, &err)
	result := interned.NewLightInternedFactSet()

	for ari, ar := range aggRules {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		var flatIdx int
		if aggRuleIdx != nil {
			flatIdx = aggRuleIdx[ari]
		}

		items, negativeBody, varNames, err := ev.compileQueryBody(ar.Body)
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
		groupByIdx := make([]int8, len(groupByVars))
		for i, name := range groupByVars {
			groupByIdx[i] = varNameIndex(varNames, name)
		}

		// Resolve the aggregate term name (if it's a variable).
		var aggVarName string
		var aggVarIdx int8 = -1
		var aggConstID uint64
		var aggIsConst bool
		if v, ok := ar.AggTerm.(datalog.Variable); ok {
			aggVarName = string(v)
			aggVarIdx = varNameIndex(varNames, aggVarName)
		}
		if c, ok := ar.AggTerm.(datalog.Constant); ok {
			aggConstID = ev.dict.InternConstant(c)
			aggIsConst = true
		}
		// checkAggRuleSafety (engine.go) rejects an aggregated variable the
		// body never binds at compile time, so aggVarIdx < 0 here can only
		// mean this evaluator's compiled varMap disagrees with that check --
		// fail loudly with the same message the old per-row check used,
		// checked once per rule rather than once per row. AggCount ignores
		// AggTerm entirely (per syntax.AggregateRule's doc), so it never has
		// a meaningful aggVarName/aggVarIdx to validate here.
		if ar.Kind != syntax.AggCount && !aggIsConst && aggVarIdx < 0 {
			return result, fmt.Errorf("aggregate %s: unbound variable %s in aggregate", ar.Kind, aggVarName)
		}

		groups := map[uint64][]*aggGroup{}
		noDelta := interned.InternedFactSet{}
		noEmitted := interned.InternedFactSet{}
		var sub interned.VarSub
		ev.evalBodyRecursiveV(items, negativeBody, varNames, -1, noDelta, memFacts, noEmitted, &sub, 0, func(vs *interned.VarSub) {
			gk := groupKeyHash(vs, groupByIdx)
			bucket := groups[gk]
			var g *aggGroup
			for _, cand := range bucket {
				if groupKeyMatches(vs, groupByIdx, cand.keyVals) {
					g = cand
					break
				}
			}
			if g == nil {
				g = &aggGroup{keyVals: extractGroupKey(vs, groupByIdx)}
				groups[gk] = append(bucket, g)
			}
			g.count++
			if ar.Kind != syntax.AggCount {
				// AggCount only needs the row count; every other kind needs
				// each row's aggregated value. aggVarIdx is guaranteed valid
				// and bound here: it was resolved from varNames (the same
				// compiled varMap this join is running against), and every
				// variable compileBody registers via a positive atom is
				// bound by the time evalBodyRecursiveV reaches this terminal
				// callback -- a join can't succeed to this point otherwise.
				var id uint64
				if aggIsConst {
					id = aggConstID
				} else {
					id = vs.Vals[aggVarIdx]
				}
				g.values = append(g.values, id)
			}
			// Sample capture: only when provenance is enabled, and only
			// while the group's sample slice is still under the cap -- once
			// witnessSampleCap solutions have been captured, every later
			// solution in this group is grounded and discarded (never
			// appended), so a group of 500k solutions retains exactly cap
			// samples' worth of memory regardless of how far past the cap
			// count grows. This is the same "first N in iteration order"
			// determinism buildWitness relies on for plain rules: fixed
			// rule/join order makes the retained sample stable across runs.
			if ev.rec != nil && len(g.samples) < witnessSampleCap {
				g.samples = append(g.samples, buildAggSample(items, negativeBody, vs, ev.dict))
			}
		})

		// Pre-compile head atom for grounding.
		head, err := interned.CompileAtom(ar.Head.Pred, ar.Head.Terms, ev.dict)
		if err != nil {
			return result, err
		}

		// Compute aggregate for each group, sampling ctx via the evaluator's
		// shared countStep -- the same mechanism the join scans in
		// evalBodyRecursiveV and matchesAnyV use -- rather than a second,
		// independent per-group counter.
		for _, bucket := range groups {
			for _, g := range bucket {
				ev.countStep()

				aggResultID, err := computeGroupAggregate(
					ar.Kind, aggVarName, aggIsConst, g.values, g.count, ev.dict,
				)
				if err != nil {
					return result, fmt.Errorf("aggregate %s: %w", ar.Kind, err)
				}

				// The group's InternedSub is materialized exactly once per
				// output group here, not once per input body solution.
				// checkAggRuleSafety (engine.go) already rejects a
				// body-bound ResultVar at compile time, so appending the
				// result binding last can never be shadowed by a same-named
				// body binding.
				resultSub := make(interned.InternedSub, 0, len(groupByVars)+1)
				for i, name := range groupByVars {
					resultSub = append(resultSub, interned.InternedSubEntry{Name: name, Value: g.keyVals[i]})
				}
				resultSub = append(resultSub, interned.InternedSubEntry{Name: ar.ResultVar, Value: aggResultID})

				if fact, ok := interned.GroundCompiled(head, resultSub); ok {
					result.Add(fact)
					// Counted against the same shared limit evalRules' emit
					// closure uses (see checkFactLimit): one aggregate-produced
					// fact per output group, so WithFactLimit applies to
					// aggregate strata too, not just plain-rule fixpoints.
					ev.checkFactLimit()
					// Record the aggregate witness at the same point evalRules'
					// emit closure records a plain witness: right after the
					// fact is known to be genuinely produced (GroundCompiled
					// succeeded). One aggregate fact per group (this loop
					// never revisits a group), so there is no first-witness
					// race to guard against the way recorder.record's map
					// check guards evalRules' iterative re-derivation.
					if ev.rec != nil {
						fk := interned.InternedFactHash(fact)
						ev.rec.record(fk, witness{
							rule:       flatIdx,
							isAgg:      true,
							groupCount: g.count,
							sample:     g.samples,
						})
					}
				}
			}
		}
	}

	return result, nil
}

// varNameIndex finds name's VarSub index in names, or -1 if absent.
// Resolved once per aggregate rule (for groupByIdx/aggVarIdx), not per
// binding -- checkAggRuleSafety guarantees group-by and aggregated
// variables are bound by the body, so -1 is not expected in practice, but
// callers still handle it (as an always-unbound value) rather than assume it
// can't happen.
func varNameIndex(names []string, name string) int8 {
	for i, n := range names {
		if n == name {
			return int8(i)
		}
	}
	return -1
}

// groupKeyHash, groupKeyMatches, and extractGroupKey read a body solution's
// group-by values straight off its VarSub at pre-resolved indices
// (groupByIdx), instead of the retired name-keyed grouping's linear
// InternedSub.Get scan per group-by variable per solution. A negative index
// or an unbound slot contributes 0, matching InternedSub.Get's (0, false)
// -> value 0 fallback the retired functions relied on (silently, by
// discarding the ok bool) for the same "can't happen per
// checkAggRuleSafety, but degrade gracefully" case.
func groupKeyHash(sub *interned.VarSub, idx []int8) uint64 {
	h := uint64(interned.FNVOffset64)
	for _, i := range idx {
		h ^= groupKeyVal(sub, i)
		h *= interned.FNVPrime64
	}
	return h
}

func groupKeyMatches(sub *interned.VarSub, idx []int8, keyVals []uint64) bool {
	for i, vi := range idx {
		if groupKeyVal(sub, vi) != keyVals[i] {
			return false
		}
	}
	return true
}

func extractGroupKey(sub *interned.VarSub, idx []int8) []uint64 {
	vals := make([]uint64, len(idx))
	for i, vi := range idx {
		vals[i] = groupKeyVal(sub, vi)
	}
	return vals
}

func groupKeyVal(sub *interned.VarSub, idx int8) uint64 {
	if idx >= 0 && sub.Mask>>uint(idx)&1 != 0 {
		return sub.Vals[idx]
	}
	return 0
}

// computeGroupAggregate computes one group's aggregate result from its
// already-extracted per-row values (see aggGroup) rather than a slice of
// substitutions -- extraction happened once, while streaming the body's
// join solutions into groups (evalAggregates), not once per aggregate kind
// dispatch.
func computeGroupAggregate(
	kind syntax.AggregateKind,
	aggVarName string, aggIsConst bool,
	values []uint64, count int, dict *interned.Dict,
) (uint64, error) {
	switch kind {
	case syntax.AggCount:
		return dict.Intern(int64(count)), nil

	case syntax.AggSum:
		var sumInt int64
		var sumFloat float64
		isInt := true
		for _, id := range values {
			val, err := resolveInternedAggValue(id, dict, "sum", aggVarName, aggIsConst)
			if err != nil {
				return 0, err
			}
			switch v := val.(type) {
			case int64:
				if isInt {
					next, overflow := addInt64Checked(sumInt, v)
					if overflow {
						// Route through the shared errInt64Overflow reason so
						// both arithmetic paths (is-expressions via applyBinOp
						// and AggSum here) report one overflow contract and
						// errors.Is(err, errInt64Overflow) holds for either.
						return 0, fmt.Errorf("sum: %w: %d + %d", errInt64Overflow, sumInt, v)
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
		return computeExtremum("min", -1, aggVarName, aggIsConst, values, dict)

	case syntax.AggMax:
		return computeExtremum("max", 1, aggVarName, aggIsConst, values, dict)

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
	aggVarName string, aggIsConst bool,
	values []uint64, dict *interned.Dict,
) (uint64, error) {
	var best any
	for _, id := range values {
		val, err := resolveInternedAggValue(id, dict, name, aggVarName, aggIsConst)
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
// through. id is already known-valid: evalAggregates extracts it while
// streaming (either the constant AggTerm's id or Vals[aggVarIdx], both
// guaranteed resolvable at that point -- see its "unbound variable" pre-check
// and streaming callback), so there is no per-row unbound case left to
// detect here, unlike the retired per-substitution version. NaN is an
// admitted value at interning (dict.go's nanKey -- that trade-off is settled
// and not revisited here), but letting it flow into an aggregate silently
// produces bad results downstream: computeExtremum can't order it (a NaN
// could win or lose a min/max depending on its arbitrary position in the
// group) and AggSum would silently intern a NaN total that then compares
// unordered-false against everything else. So this is the one place that
// checks for it, uniformly, for every aggregate kind, and the error names
// both the aggregate (via name) and the offending predicate/rule context the
// caller already carries in its own wrapping (evalAggregates wraps this
// error with "aggregate %s: %w").
func resolveInternedAggValue(id uint64, dict *interned.Dict, name, varName string, isConst bool) (any, error) {
	val := dict.Resolve(id)
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

// subInt64Checked subtracts b from a, reporting overflow instead of wrapping.
// Overflow occurs iff a and b differ in sign and the result's sign differs
// from a's — the mirror of addInt64Checked applied to a + (-b).
func subInt64Checked(a, b int64) (diff int64, overflow bool) {
	diff = a - b
	if (a >= 0) != (b >= 0) && (diff >= 0) != (a >= 0) {
		return 0, true
	}
	return diff, false
}

// mulInt64Checked multiplies a and b, reporting overflow instead of wrapping.
// Uses the canonical round-trip check: a non-overflowing product divides back
// by each factor exactly -- except for the MinInt64*-1 (and -1*MinInt64)
// case, which the round-trip check alone misses: Go defines MinInt64 * -1 as
// MinInt64 * -1 == MinInt64 (silent wrap, since +2^63 is not representable),
// and MinInt64/-1 then reproduces -1 exactly (prod/a == b holds even though
// prod is wrong), so the round-trip check alone would pass through a wrapped
// value. Guarded explicitly up front, matching the documented policy that
// int64 overflow panics arithmeticOverflowError rather than wrapping.
func mulInt64Checked(a, b int64) (prod int64, overflow bool) {
	if a == 0 || b == 0 {
		return 0, false
	}
	if (a == -1 && b == math.MinInt64) || (b == -1 && a == math.MinInt64) {
		return 0, true
	}
	prod = a * b
	if prod/a != b {
		return 0, true
	}
	return prod, false
}

// divInt64Checked divides a by b, reporting overflow instead of wrapping.
// Go defines MinInt64 / -1 == MinInt64 (a silent wrap: the mathematical
// result, +2^63, is not representable as int64), the only int64/int64
// division that can overflow. Division by zero is handled by the caller
// (applyBinOp), which already treats it as "no result" rather than an error;
// divInt64Checked only guards the overflow case.
func divInt64Checked(a, b int64) (quot int64, overflow bool) {
	if a == math.MinInt64 && b == -1 {
		return 0, true
	}
	return a / b, false
}
