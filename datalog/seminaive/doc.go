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
//
// # Options
//
// Use [WithMaxIterations] to limit the number of fixpoint iterations (default 10000):
//
//	engine := seminaive.New(seminaive.WithMaxIterations(500))
package seminaive
