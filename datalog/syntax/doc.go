// Package syntax defines the abstract syntax tree for Datalog programs and provides
// a recursive-descent parser.
//
// # Parsing
//
// Use [ParseAll] to parse a complete Datalog program (multiple rules, facts, and queries):
//
//	rs, err := syntax.ParseAll(`
//	    parent("tom", "bob").
//	    ancestor(X, Y) :- parent(X, Y).
//	    ancestor(X, Y) :- parent(X, Z), ancestor(Z, Y).
//	`)
//
// Use [ParseStatement] to parse a single statement interactively (one rule, fact, or query).
//
// Use [Parse] as a convenience that parses and compiles in one step:
//
//	tr, err := syntax.Parse(seminaive.New(), rules)
//
// # Grammar
//
// Statements are terminated by '.' (rules and facts) or '?' (queries).
// Line comments begin with '%'.
//
// Facts are rules with no body whose head terms are all constants:
//
//	parent("tom", "bob").
//	port(80).
//	severity("alert-1", "high").
//
// Rules derive new facts from existing ones:
//
//	ancestor(X, Y) :- parent(X, Y).
//	ancestor(X, Y) :- parent(X, Z), ancestor(Z, Y).
//
// Queries match facts in the derived database:
//
//	ancestor("tom", X)?
//
// # Negation
//
// Body atoms may be negated with 'not'. Negated atoms must only use variables
// that are bound by positive atoms in the same rule body:
//
//	no_parent(X) :- person(X), not parent(X, ?).
//
// The '?' character introduces an anonymous variable (the parser generates unique
// names like ?0, ?1, etc.).
//
// # Constraints
//
// Inline comparison constraints are supported in rule bodies:
//
//	large_transfer(From, To, Amt) :-
//	    transfer(From, To, Amt), Amt > 10000.
//
// Supported operators: =, !=, <, >, <=, >=
//
// # Arithmetic
//
// The 'is' atom assigns the result of an arithmetic expression to a variable:
//
//	doubled(X, Y) :- value(X), Y is X * 2.
//
// Supported operators: +, -, *, /, mod
// Expressions may be parenthesized for grouping.
//
// # String Builtins
//
// Engine builtins use the '@' prefix as constraint checks in rule bodies:
//
//	@contains(Haystack, Needle)
//	@starts_with(Str, Prefix)
//	@ends_with(Str, Suffix)
//	@regex_match(Str, Pattern)
//
// These succeed when the string relationship holds and fail otherwise.
// Both arguments must be bound by preceding positive atoms.
//
// # Aggregates
//
// Aggregate rules compute summary values over matching tuples:
//
//	total_amount(Owner, Total) :- Total = sum(Amt) : owns(Owner, Acct), transfer(Acct, _, Amt).
//	num_alerts(N) :- N = count : alert(?, ?, ?).
//	max_severity(S) :- S = max(Sev) : alert(?, Sev, ?).
//	min_time(T) :- T = min(Time) : event(?, Time).
//
// The syntax is: Head :- ResultVar = Kind(AggTerm) : Body.
// Supported kinds: count, sum, min, max.
// For count, the AggTerm is ignored. Variables in the head that are not the
// result variable define the group-by columns.
//
// # Literals
//
// String literals are double-quoted with standard escape sequences (\n, \t, \\, \").
// Integer literals include negative numbers (-42). Float literals support
// scientific notation (1.5e3). The parser normalizes floats that represent
// exact integers to integer type.
package syntax
