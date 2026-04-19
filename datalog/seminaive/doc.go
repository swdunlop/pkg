// Package seminaive implements a Datalog evaluation engine using the semi-naive algorithm.
//
// Semi-naive evaluation is an optimization over naive bottom-up evaluation. Instead of
// re-joining all facts on every iteration, it tracks newly derived facts ("deltas") and
// only joins deltas against existing facts. This avoids redundant derivations and
// converges to a fixpoint much faster for recursive rules.
//
// # Compiling Rules
//
// Create an [Engine] and compile a parsed [syntax.Ruleset] into a [datalog.Transformer]:
//
//	engine := seminaive.New()
//	rs, _ := syntax.ParseAll(`
//	    reachable(X, Y) :- edge(X, Y).
//	    reachable(X, Y) :- reachable(X, Z), edge(Z, Y).
//	`)
//	tr, err := engine.Compile(rs)
//
// The transformer can then be applied to any input database:
//
//	output, err := tr.Transform(ctx, input)
//
// The output database contains all input facts plus all derived facts.
//
// # Safety Checking
//
// The compiler rejects unsafe rules where:
//   - A head variable is not bound by any positive body atom
//   - A variable in a negated atom is not bound by a positive atom
//   - A variable in a comparison or arithmetic expression is not bound
//
// # Stratification
//
// Programs with negation are stratified using Tarjan's algorithm to compute
// strongly connected components. Rules are partitioned into strata so that
// negated predicates are always fully computed before they are referenced.
// Negation cycles (where predicate A depends negatively on B and B depends
// on A) are rejected at compile time.
//
// # Supported Features
//
// The engine supports:
//   - Recursive rules with fixpoint iteration
//   - Negation (stratified)
//   - Comparison constraints: =, !=, <, >, <=, >=
//   - Arithmetic via 'is' atoms: +, -, *, /, mod
//   - String builtins: @contains, @starts_with, @ends_with, @regex_match
//   - Aggregates: count, sum, min, max with group-by
//   - Custom binding builtins via [WithBuiltin]
//
// # Custom Builtins
//
// Register custom binding builtins that compute a derived value from resolved
// input arguments. In rule bodies, all arguments except the last are inputs;
// the last is the output variable that receives the result:
//
//	double := func(inputs []any) (any, bool) {
//	    v, ok := inputs[0].(int64)
//	    if !ok { return nil, false }
//	    return v * 2, true
//	}
//	engine := seminaive.New(seminaive.WithBuiltin("@double", double))
//
// Then use it in rules:
//
//	doubled(X, D) :- val(X, V), @double(V, D).
//
// Builtin predicate names should start with "@" by convention to distinguish them
// from regular predicates. Inputs are resolved Go values (int64, float64, string,
// or [datalog.ID]); the result is interned into the dictionary automatically.
//
// The package provides [TimeDiff] as a ready-to-use builtin for computing the
// difference between two timestamps (RFC3339 strings or numeric epoch values)
// in seconds:
//
//	engine := seminaive.New(seminaive.WithBuiltin("@time_diff", seminaive.TimeDiff))
//
// # Profiling
//
// Use [WithProfile] to receive per-stratum evaluation statistics after each
// call to Transform:
//
//	engine := seminaive.New(seminaive.WithProfile(func(stats []seminaive.StratumStats) {
//	    for _, s := range stats {
//	        fmt.Printf("%v: %d facts in %d iterations (%v)\n",
//	            s.Predicates, s.FactCount, s.Iterations, s.Duration)
//	    }
//	}))
//
// Stats collection is zero-cost when no profile callback is registered.
//
// # Options
//
// Use [WithMaxIterations] to limit the number of fixpoint iterations (default 10000):
//
//	engine := seminaive.New(seminaive.WithMaxIterations(500))
package seminaive
