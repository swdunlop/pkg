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
// names like ?0, ?1, etc.). A bare '_' is anonymous the same way, as in Prolog:
// each occurrence is a distinct variable, so parent(_, _) does not unify its
// two positions. Longer '_'-prefixed names (_Ignored) are ordinary named
// variables.
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
//
// The bare identifiers 'true', 'false', and 'null' are reserved constant
// literals ([datalog.Bool], [datalog.Null]), not variable names, in every
// position a term may appear (fact/rule arguments, comparison and 'is'
// constraints, aggregate terms, queries):
//
//	flagged(Host, true) :- suspicious(Host).
//	missing(Field, null) :- record(Field), not present(Field).
//
// A variable literally named True, False, or Null (capitalized, as Datalog
// variables conventionally are) is unaffected — the reserved forms are
// exact, case-sensitive matches on the lowercase spelling.
//
// # Destructuring Patterns
//
// Object and array patterns in body-atom argument positions match inside
// composite (JSON) constants:
//
//	suspicious(P) :- process(P, {name: Name, ppid: 4}), @ends_with(Name, ".tmp.exe").
//	pair(A, B)    :- ev(Id, [A, B]).
//	walk(H, T)    :- list(L), l(L, [H | T]).
//
// The grammar, allowed only in argument positions of positive body atoms:
//
//	pattern     := object_pat | array_pat
//	object_pat  := "{" [ field ("," field)* ] "}"
//	field       := (ident | string) ":" (term | pattern)
//	array_pat   := "[" [ (term | pattern) ("," (term | pattern))* [ "|" var ] ] "]"
//
// Patterns are pure syntax sugar: the parser rewrites each pattern into a
// fresh anonymous variable plus @json_get/@json_len/@json_slice getter atoms
// appended to the body. The engine never sees a pattern. Consequently
// [Rule.String] prints the desugared form, not the original pattern; the
// printed form reparses to the same rule (explicit ?N variables are accepted
// by the lexer).
//
// Object matching is open: {name: N} matches any object that has a "name"
// key, regardless of other keys. A missing key is not an error; the
// candidate simply fails to match. Constants in value positions act as
// filters ({status: "active"}), repeated variables get equality semantics
// ({src: X, dst: X}), and nested patterns recurse through fresh
// intermediates. Unquoted field keys are literal key names and must start
// with a lowercase letter; quote keys that begin with an uppercase letter.
// Enumerating unknown keys is @json_items' job, not a pattern's.
//
// Patterns are rejected in rule heads (term construction could grow the
// term universe and break termination) and under negation (negating a
// desugared conjunction is not the conjunction of negations).
package syntax
