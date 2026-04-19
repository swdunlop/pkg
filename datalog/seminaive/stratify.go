package seminaive

import (
	"fmt"
	"slices"

	"swdunlop.dev/pkg/datalog/syntax"
)

// stratum groups predicates and their rules for evaluation.
type stratum struct {
	predicates map[string]bool
	rules      []syntax.Rule
	aggRules   []syntax.AggregateRule
}

// depEdge represents a dependency from one predicate to another.
type depEdge struct {
	from, to string
	negative bool
}

// stratify partitions rules into strata based on predicate dependencies.
func stratify(rules []syntax.Rule, aggRules []syntax.AggregateRule, builtins map[string]BuiltinFunc) ([]stratum, error) {
	edges := []depEdge{}
	allPreds := map[string]bool{}

	for _, r := range rules {
		allPreds[r.Head.Pred] = true
		for _, b := range r.Body {
			if isConstraint(b) || isBindBuiltin(b, builtins) || b.Pred == "is" {
				continue
			}
			allPreds[b.Pred] = true
			edges = append(edges, depEdge{from: r.Head.Pred, to: b.Pred, negative: b.Negated})
		}
	}
	for _, ar := range aggRules {
		allPreds[ar.Head.Pred] = true
		for _, b := range ar.Body {
			if isConstraint(b) || isBindBuiltin(b, builtins) || b.Pred == "is" {
				continue
			}
			allPreds[b.Pred] = true
			edges = append(edges, depEdge{from: ar.Head.Pred, to: b.Pred, negative: b.Negated})
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
			return nil, fmt.Errorf("unstratifiable: negation cycle involving %q and %q", e.from, e.to)
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
