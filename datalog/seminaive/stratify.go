package seminaive

import (
	"fmt"
	"slices"
	"strings"

	"swdunlop.dev/pkg/datalog/syntax"
)

// stratum groups predicates and their rules for evaluation.
type stratum struct {
	predicates map[string]bool
	rules      []syntax.Rule
	aggRules   []syntax.AggregateRule
}

// depEdge represents a dependency from one predicate to another.
//
// negative marks edges that must not participate in a cycle: ordinary
// negation edges (b.Negated) and aggregate-rule body edges, since
// evaluation runs aggregates non-monotonically (once per stratum, after
// the plain-rule fixpoint) rather than folding their output back into the
// fixpoint. Both require "to" to be in a strictly lower stratum than
// "from", and both are rejected if they land in the same SCC.
type depEdge struct {
	from, to string
	negative bool
	agg      bool // true if this edge comes from an aggregate rule's body
}

// stratify partitions rules into strata based on predicate dependencies.
func stratify(rules []syntax.Rule, aggRules []syntax.AggregateRule, builtins map[string]BuiltinFunc, multiBuiltins map[string]multiBuiltin) ([]stratum, error) {
	edges := []depEdge{}
	allPreds := map[string]bool{}

	for _, r := range rules {
		allPreds[r.Head.Pred] = true
		for _, b := range r.Body {
			if isConstraint(b) || isBindBuiltin(b, builtins) || isMultiBindBuiltin(b, multiBuiltins) || b.Pred == "is" {
				continue
			}
			allPreds[b.Pred] = true
			edges = append(edges, depEdge{from: r.Head.Pred, to: b.Pred, negative: b.Negated})
		}
	}
	for _, ar := range aggRules {
		allPreds[ar.Head.Pred] = true
		for _, b := range ar.Body {
			if isConstraint(b) || isBindBuiltin(b, builtins) || isMultiBindBuiltin(b, multiBuiltins) || b.Pred == "is" {
				continue
			}
			allPreds[b.Pred] = true
			// Aggregates are non-monotonic like negation: evaluation runs
			// plain rules to fixpoint and then evalAggregates exactly once
			// per stratum, never feeding aggregate output back into the
			// fixpoint. So every body predicate of an aggregate rule
			// (including the aggregated predicate itself) must land in a
			// strictly lower stratum than the aggregate's head, on pain of
			// silently wrong (non-fixpoint) results. Mark these edges the
			// same way negated edges are marked so a cycle through an
			// aggregate is rejected at stratification time.
			edges = append(edges, depEdge{from: ar.Head.Pred, to: b.Pred, negative: true, agg: true})
		}
	}

	sccs := tarjanSCC(allPreds, edges)

	predToSCC := map[string]int{}
	for i, scc := range sccs {
		for p := range scc {
			predToSCC[p] = i
		}
	}
	for _, e := range edges {
		if e.negative && predToSCC[e.from] == predToSCC[e.to] {
			members := make([]string, 0, len(sccs[predToSCC[e.from]]))
			for p := range sccs[predToSCC[e.from]] {
				members = append(members, p)
			}
			slices.Sort(members)
			if e.agg {
				return nil, fmt.Errorf("unstratifiable: cycle through an aggregate involving %s", strings.Join(members, ", "))
			}
			return nil, fmt.Errorf("unstratifiable: negation cycle involving %s", strings.Join(members, ", "))
		}
	}

	sccStratum := make([]int, len(sccs))
	for i := range sccs {
		sccStratum[i] = 0
	}

	for i := range len(sccs) {
		for _, e := range edges {
			fromSCC := predToSCC[e.from]
			toSCC := predToSCC[e.to]
			if fromSCC != i {
				continue
			}
			if toSCC == fromSCC {
				continue
			}
			minStratum := sccStratum[toSCC] + 1
			if minStratum > sccStratum[i] {
				sccStratum[i] = minStratum
			}
		}
	}

	maxStratum := 0
	for _, s := range sccStratum {
		if s > maxStratum {
			maxStratum = s
		}
	}

	strata := make([]stratum, maxStratum+1)
	for i := range strata {
		strata[i].predicates = map[string]bool{}
	}
	for i, scc := range sccs {
		s := sccStratum[i]
		for p := range scc {
			strata[s].predicates[p] = true
		}
	}

	for _, r := range rules {
		s := sccStratum[predToSCC[r.Head.Pred]]
		strata[s].rules = append(strata[s].rules, r)
	}
	for _, ar := range aggRules {
		s := sccStratum[predToSCC[ar.Head.Pred]]
		strata[s].aggRules = append(strata[s].aggRules, ar)
	}

	return strata, nil
}

// tarjanSCC computes strongly connected components.
// Returns SCCs in reverse topological order.
func tarjanSCC(nodes map[string]bool, edges []depEdge) []map[string]bool {
	adj := map[string][]string{}
	for _, e := range edges {
		adj[e.from] = append(adj[e.from], e.to)
	}

	index := 0
	stack := []string{}
	onStack := map[string]bool{}
	indices := map[string]int{}
	lowlinks := map[string]int{}
	var sccs []map[string]bool

	var strongconnect func(v string)
	strongconnect = func(v string) {
		indices[v] = index
		lowlinks[v] = index
		index++
		stack = append(stack, v)
		onStack[v] = true

		for _, w := range adj[v] {
			if _, visited := indices[w]; !visited {
				strongconnect(w)
				lowlinks[v] = min(lowlinks[v], lowlinks[w])
			} else if onStack[w] {
				lowlinks[v] = min(lowlinks[v], indices[w])
			}
		}

		if lowlinks[v] == indices[v] {
			scc := map[string]bool{}
			for {
				w := stack[len(stack)-1]
				stack = stack[:len(stack)-1]
				onStack[w] = false
				scc[w] = true
				if w == v {
					break
				}
			}
			sccs = append(sccs, scc)
		}
	}

	sorted := make([]string, 0, len(nodes))
	for n := range nodes {
		sorted = append(sorted, n)
	}
	slices.Sort(sorted)

	for _, n := range sorted {
		if _, visited := indices[n]; !visited {
			strongconnect(n)
		}
	}

	return sccs
}
