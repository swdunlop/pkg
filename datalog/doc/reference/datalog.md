# Datalog Language Reference

This manual describes the Datalog dialect implemented by this package: its
lexical syntax, grammar, data types, evaluation semantics, and built-in
functionality. It covers the language surface a rule author sees, not the Go
API (see the package documentation for `syntax`, `seminaive`, and `memory`
for that).

Facts usually enter the database from JSONL files rather than from Datalog
source text. That layer — schema files, mappings, matchers, and the JSONL
encoder — is documented separately in the
[JSON Fact Extraction Reference](jsonfacts.md). This manual only assumes
that a database of facts exists.

## 1. Programs and statements

A program is a sequence of statements. There are three statement forms,
distinguished by their terminator:

| Form | Syntax | Terminator |
|------|--------|------------|
| Fact | `parent("tom", "bob").` | `.` |
| Rule | `ancestor(X, Y) :- parent(X, Y).` | `.` |
| Query | `ancestor("tom", X)?` | `?` |

A fact is a rule with no body whose head terms are all constants. A rule
derives new facts: whenever every atom in the body (right of `:-`) is
satisfied, the head (left of `:-`) is asserted. A query matches against the
database after all rules have been evaluated to fixpoint.

Statements may span multiple lines; the terminator ends the statement, not
the newline.

## 2. Lexical structure

### 2.1 Character set

The lexer is byte-oriented and ASCII-only for identifiers and operators.
Bytes ≥ 0x80 outside string literals are errors. Non-ASCII text is fully
supported *inside* string literals.

### 2.2 Comments and whitespace

Line comments begin with `%` and run to the end of the line. There are no
block comments. Space, tab, carriage return, and newline separate tokens
and are otherwise insignificant.

### 2.3 Identifiers and variables

An identifier is `[A-Za-z_][A-Za-z0-9_]*`.

**Every bare identifier in a term position is a variable.** There is no
case-based distinction between variables and symbolic constants: `X`,
`Host`, `red`, and `true` are all variables. Ground data must be written as
quoted strings or numeric literals (or arrive via the JSON loader). A
"fact" like `color(red).` parses, but is rejected at compile time because
its head variable `red` is unbound.

Identifiers appearing in *predicate position* (before a parenthesized
argument list) are predicate names, not variables.

Two identifiers are reserved words: `not` (negation) and `is` (arithmetic
assignment). `mod` is an ordinary identifier that acts as an operator only
inside arithmetic expressions.

### 2.4 Anonymous variables

- `?` introduces a fresh anonymous variable. Each occurrence is distinct:
  `parent(?, ?)` places no constraint between the two positions.
- A bare `_` behaves identically (Prolog convention): each `_` is a
  distinct fresh variable.
- Longer `_`-prefixed names (`_Ignored`) are ordinary named variables — all
  occurrences within a rule co-refer. (In queries they are treated as
  don't-cares for output purposes; see §10.)
- The explicit forms `?0`, `?1`, … are accepted by the lexer as named
  variables. The parser generates such names when desugaring patterns and
  anonymous variables, and reserves any that appear in the input so
  generated names never collide. Printed rules therefore reparse to the
  same rule.

`?` also terminates queries; the parser disambiguates by context.

### 2.5 Numeric literals

- Integer: `[0-9]+` with optional leading `-`, a 64-bit signed integer.
  Out-of-range literals are a parse error.
- Float: triggered by a fractional part (`1.5`, `.5`) or an exponent
  (`1e10`, `1.5e3`, `2E-5`), an IEEE 754 double.
- A `.` not followed by a digit is the statement terminator, never a
  decimal point. `1.` is the integer `1` followed by end-of-statement.
- A `-` immediately before a digit is a numeric sign only where an
  expression cannot continue (start of statement, after `(`, `,`, `:-`, or
  an operator); otherwise it is the subtraction operator. `p(-5)` contains
  the literal `-5`; `1-2` is a subtraction.
- The parser preserves the written type: `val(3.0)` is the float `3.0` in
  the AST. Floats that represent exact integers are normalized to integers
  when values are interned into the database, so `3.0` and `3` denote the
  same stored constant (see §4.3).

### 2.6 String literals

Strings are double-quoted. A raw newline inside a string is an error
(preventing a missing close quote from swallowing later statements), as is
an unterminated string at end of input.

Escape sequences (any other escape is an error, not a literal
pass-through):

| Escape | Meaning |
|--------|---------|
| `\n` `\t` `\r` `\a` `\b` `\f` `\v` | newline, tab, CR, bell, backspace, form feed, vertical tab |
| `\\` `\"` | backslash, double quote |
| `\xHH` | one raw byte, exactly 2 hex digits (round-trips non-UTF-8 bytes) |
| `\uNNNN` | Unicode code point, exactly 4 hex digits |
| `\UNNNNNNNN` | Unicode code point, exactly 8 hex digits |

Invalid code points (`\uD800`, values above U+10FFFF) are errors.

### 2.7 Builtin names

`@` followed by an identifier forms a builtin name (`@contains`,
`@json_get`). A lone `@` is an error. Builtins may only appear in rule and
query bodies, never as a rule head.

### 2.8 Punctuation

`( ) { } [ ] | , : :- = != < <= > >= + - * / . ?`

`!` alone (not part of `!=`) is an error.

## 3. Terms and data types

A term is either a variable or a constant. The constant types:

| Type | Description | Written form |
|------|-------------|--------------|
| String | UTF-8 (or arbitrary-byte) string | `"cmd.exe"` |
| Integer | 64-bit signed integer | `42`, `-7` |
| Float | IEEE 754 double | `3.14`, `1e10` |
| Bool | JSON boolean | *(not writable — see below)* |
| Null | JSON null | *(not writable)* |
| ID | synthetic identifier | *(not writable; prints as `#N`)* |
| Composite | JSON object or array, atomic | *(not writable; enters via the loader)* |

### 3.1 Non-writable constants

`true`, `false`, and `null` are **not** literals — as bare identifiers they
parse as variables. Bool and Null constants exist so that JSON `true`,
`false`, and `null` extracted from composites remain distinct from the
strings `"true"`, `"false"`, `"null"`. They arise only from JSON
extraction (§8.2) and the Go API. To test them in a rule, compare two
extracted values with `=`, or use `@json_type(V, "bool")` / `@json_type(V,
"null")`.

IDs (`#0`, `#1`, …) are synthetic join keys generated by the JSONL loader's
`fresh_id()` (see the [JSON Fact Extraction Reference](jsonfacts.md)). The
`#N` form appears in printed output but is not accepted by the parser; IDs
enter the database only through the loader or Go API.

### 3.2 Composites

A Composite is a JSON object or array treated as a single **atomic**
constant. The engine never unifies inside a composite; equality is
identity of the canonical encoding. Rules reach inside composites with
destructuring patterns (§6) or the `@json_*` builtins (§8.2).

Canonicalization guarantees structural equality is term equality:

- Object keys are sorted lexicographically; duplicate keys are an error.
- Floats representing exact integers become integers (`{"pid": 1.0}` and
  `{"pid": 1}` are the same constant).
- NaN and ±Inf are rejected; invalid UTF-8 in strings or keys is rejected.

Composites are hash-consed: each distinct value is stored once regardless
of how many facts reference it.

### 3.3 Equality and ordering across types

`=` and `!=` first compare stored identity (which covers equal strings,
composites, bools, nulls, and IDs). Mixed integer/float operands that are
numerically equal compare equal (comparison is exact, even beyond 2^53).

`<`, `>`, `<=`, `>=` are defined only for:

- number vs number (integer and float mix numerically, exactly), and
- string vs string (lexicographic byte order).

Every other pairing — string vs number, and anything involving Bool, Null,
ID, or Composite — is unordered: the constraint is simply **false** (the
candidate solution is dropped; there is no error). NaN is unordered
against everything, including itself, though `X = Y` holds between two NaN
values because all NaNs intern to one constant.

## 4. Rules

```
head(T1, ..., Tn) :- body_atom, body_atom, ... .
```

The head must be a plain predicate atom. Negated atoms, comparisons, `is`
expressions, builtins, and destructuring patterns are all rejected in head
position with a compile-time error.

A predicate is identified by its name **and arity**: `p/2` and `p/3` are
unrelated relations.

### 4.1 Body atoms

A body is a comma-separated conjunction of:

1. **Positive atoms** — `pred(T, ...)`. Zero-arity `pred()` is allowed.
2. **Negated atoms** — `not pred(T, ...)`. See §4.2.
3. **Comparisons** — `T1 op T2` with `op` ∈ `= != < > <= >=` (§3.3).
4. **Arithmetic** — `Var is Expr` (§4.3).
5. **Builtin calls** — `@name(T, ...)` (§8).

Atom order in the body does not affect meaning. The compiler reorders
evaluation so builtins and constraints run as soon as their inputs are
bound; a negated atom may lexically precede the positive atoms that bind
its variables.

### 4.2 Negation

`not p(T, ...)` succeeds when no fact matches the atom. Restrictions:

- Every named variable in a negated atom must be bound by a positive atom
  in the same body. Anonymous variables (`?`, `_`) are permitted and act
  as wildcards: `not parent(?, X)` means "no fact `parent(_, X)` exists".
- Constraints and builtins cannot be negated. Use the inverse operator
  instead (`!=` rather than `not` around `=`).
- Destructuring patterns cannot appear under negation.
- Programs must be stratifiable (§9.2): recursion through negation is a
  compile-time error.

### 4.3 Arithmetic: `is`

```
cost(Item, Total) :- price(Item, P), tax_rate(R), Total is P * (1 + R).
```

`Var is Expr` evaluates `Expr` and binds the result to `Var`. If `Var` is
already bound, the atom acts as an equality test. `_ is Expr` evaluates
and discards the result.

Expressions use `+ - * /` and `mod`, with conventional precedence (`* /
mod` bind tighter than `+ -`), parentheses for grouping, and unary minus.
Leaves are numeric literals and bound variables; every variable in the
expression must be bound by a positive atom.

Numeric behavior:

- Two integer operands produce an integer. Integer `+`, `-`, `*` are
  **checked**: overflow is a transform-time error (`integer overflow:
  result exceeds int64 range`), never a silent wrap.
- Integer `/` and `mod` are integer division and modulo. Division or
  modulo by zero produces no result — the atom fails and that candidate
  solution is dropped, without error.
- If either operand is a float, the operation is performed in floating
  point. Float arithmetic follows IEEE semantics (overflow yields ±Inf);
  float `mod` is `math.Mod`; float division by zero fails the atom.

## 5. Facts

A fact is a bodiless rule whose head terms are all constants:

```
allowlisted("svchost.exe").
port(80).
threshold(0.75).
```

Facts written in a program are added to the database before evaluation.
Because bare identifiers are variables (§2.3), symbolic values must be
quoted: `color("red").`, not `color(red).`

## 6. Destructuring patterns

Object and array patterns match inside composite constants. They are
allowed **only in argument positions of positive body atoms**:

```
suspicious(P) :- process(P, {name: Name, ppid: 4}), @ends_with(Name, ".tmp.exe").
pair(A, B)    :- ev(Id, [A, B]).
walk(H, T)    :- list(L), l(L, [H | T]).
```

Grammar:

```
pattern     := object_pat | array_pat
object_pat  := "{" [ field ("," field)* ] "}"
field       := (ident | string) ":" (term | pattern)
array_pat   := "[" [ (term | pattern) ("," (term | pattern))* [ "|" var ] ] "]"
```

Semantics:

- **Open object matching.** `{name: N}` matches any object that has a
  `name` key, regardless of other keys. A missing key is not an error; the
  candidate simply fails to match. There is no closed-match form;
  enumerating unknown keys is `@json_items`' job.
- **Constants filter.** `{status: "active"}` matches only objects whose
  `status` equals `"active"`.
- **Repeated variables** get equality semantics: `{src: X, dst: X}`
  requires both fields equal.
- **Nested patterns** recurse: `{proc: {name: N}}`.
- **Fixed-length arrays.** `[A, B]` requires length exactly 2.
- **Head/tail.** `[H | T]` binds `H` to element 0 and `T` to the rest of
  the array (as a composite). Slices strictly shrink, so list-walking
  recursion terminates.
- **Empty object `{}`** matches any object (a type constraint only).
- Unquoted field keys are literal key names and must start with a
  lowercase letter; quote keys beginning with an uppercase letter.
  Variable keys are rejected (`variable keys are not allowed in patterns`).

Patterns are pure syntax sugar. The parser rewrites each pattern into a
fresh `?N` variable plus `@json_get` / `@json_len` / `@json_slice` /
`@json_type` atoms appended to the body; the engine never sees a pattern,
and printed rules show the desugared form (which reparses identically).

Patterns are rejected, with compile-time errors, in rule heads (term
construction would break termination) and under negation.

Patterns do not use column indexes — each candidate fact is destructured.
For hot fields, project into a derived predicate once and join on that,
rather than destructuring in every rule.

## 7. Aggregates

An aggregate rule computes a summary value over the solutions of a body:

```
head(GroupVars..., Result) :- Result = kind(AggTerm) : body.

alert_count(Sev, N)  :- N = count : alert(?, Sev, ?).
total_bytes(Host, T) :- T = sum(Bytes) : traffic(Host, Bytes).
max_severity(S)      :- S = max(Sev) : alert(?, Sev, ?).
```

- Kinds: `count`, `sum`, `min`, `max`. `count` takes no parenthesized
  term; the others require one (a variable bound by the body, or a
  constant).
- **Grouping**: head variables other than the result variable are the
  group-by columns. Each distinct combination of their values forms a
  group, yielding one fact.
- The form is recognized by lookahead on `Var = kind`: a body beginning
  `R = X` where `X` is not an aggregate keyword is an ordinary equality
  comparison, not an aggregate.

Result types and edge cases:

- `count` yields an integer.
- `sum` stays integer while all inputs are integers, with **checked**
  overflow (an error, not a wrap); if any input is a float the total
  promotes to float. Non-numeric inputs are an error.
- `min`/`max` compare mixed integer/float inputs exactly and return the
  winning element with its **original type** — no float rounding is
  introduced. Non-numeric inputs are an error.
- A body with no solutions produces **no fact** — `count` yields nothing
  rather than `0`. (To detect emptiness, use negation.)
- NaN among aggregated values is an error for every kind.

Aggregates evaluate non-monotonically: once per stratum, after the plain
rules in that stratum reach fixpoint. Consequently a program in which an
aggregate's result feeds back into its own body — directly, through
negation, or through a plain rule in the same recursive component — is
rejected as unstratifiable (§9.2).

## 8. Built-in predicates

Builtins are named `@name` and come in two behavioral kinds:

- **Constraints** check a relationship between bound values; they bind
  nothing and succeed or fail.
- **Binding builtins** compute outputs from inputs: the trailing
  argument(s) are outputs, the rest are inputs, and every input variable
  must be bound by a positive atom. If an output position holds a constant
  or an already-bound variable, it acts as an equality filter instead.

A builtin that fails (wrong input type, missing key, no result) silently
drops the candidate solution; it never raises an error.

### 8.1 String constraints

Both arguments must be bound strings; any other type fails the atom.

| Builtin | Succeeds when |
|---------|---------------|
| `@contains(Str, Sub)` | `Str` contains substring `Sub` |
| `@starts_with(Str, Prefix)` | `Str` begins with `Prefix` |
| `@ends_with(Str, Suffix)` | `Str` ends with `Suffix` |
| `@regex_match(Str, Pattern)` | `Str` matches Go/RE2 regexp `Pattern` |

Matching is case-sensitive. `@regex_match` uses Go's RE2 syntax (no
backreferences); an invalid pattern fails the atom rather than erroring.
Compiled patterns are cached. These four names are reserved and cannot be
overridden by custom builtins.

Note that the JSONL loader's *matchers* provide a bulk, pre-computed
alternative (`contains(Value, Pattern)` facts and friends) that is usually
preferable for large pattern lists; see the
[JSON Fact Extraction Reference](jsonfacts.md).

### 8.2 JSON builtins

Registered in every engine; these are the target of pattern desugaring.
`C` denotes a composite input.

| Builtin | Outputs | Behavior |
|---------|---------|----------|
| `@json_get(C, Key, V)` | `V` | Value at string key (object) or int64 index (array). Fails on missing key, negative/out-of-range index, wrong key type, or non-composite `C`. |
| `@json_len(C, N)` | `N` | Number of object keys or array elements. |
| `@json_type(V, T)` | `T` | Type name of any value: `"object"`, `"array"`, `"string"`, `"integer"`, `"float"`, `"bool"`, `"null"`, `"id"`. |
| `@json_slice(Arr, From, T)` | `T` | The sub-array `Arr[From:]` as a composite; `From` must be an int64 in `[0, len]`. |
| `@json_each(Arr, Elem)` | `Elem` (multi) | Yields each array element in order — one solution per element. |
| `@json_items(Obj, K, V)` | `K`, `V` (multi) | Yields each key/value pair, in sorted key order. |

Extracted scalars become ordinary constants: JSON strings, integers, and
floats map to String/Integer/Float; `true`/`false` to Bool; `null` to
Null; nested objects and arrays to new composites. All these builtins
produce only sub-values of their inputs, which preserves Datalog's
termination guarantee.

### 8.3 Custom builtins (Go API)

Embedders may register additional binding builtins
(`seminaive.WithBuiltin`, `WithMultiBuiltin`) and external fact sources
(`WithExternal`). The package ships one ready-made builtin, **not**
registered by default:

- `@time_diff(T2, T1, D)` (`seminaive.TimeDiff`) — binds `D` to `T2 − T1`
  in seconds. Inputs may be numeric epoch seconds or RFC3339/RFC3339Nano
  strings; `D` is an integer when the difference is whole seconds,
  otherwise a float. Unparseable inputs fail the atom.

The `datalog` CLI registers no custom builtins; `@time_diff` is available
only in Go embeddings that opt in.

## 9. Program semantics

### 9.1 Safety

The compiler rejects unsafe rules with descriptive errors:

- every head variable must be bound by a positive body atom;
- every variable in a negated atom, comparison, or `is` expression must be
  bound by a positive atom (the `is` result variable then counts as bound
  for later atoms and the head);
- every input variable of a builtin must be bound;
- for aggregate rules: group-by variables and the aggregated variable must
  be bound by the aggregate body, and the result variable must **not** be
  bound by the body.

Structural limits, enforced at compile time: maximum predicate arity
**16**; maximum **16** distinct variables per rule (pattern-generated
intermediates count toward the limit).

### 9.2 Stratification

Programs with negation or aggregates are partitioned into strata such that
whenever a rule uses `not p(...)` or aggregates over `p`, predicate `p` is
fully computed in an earlier stratum. Strata are evaluated in order, each
to fixpoint. Programs where this is impossible — a cycle through negation,
or through an aggregate — are rejected at compile time
(`unstratifiable: ...`).

### 9.3 Evaluation

Evaluation is bottom-up and semi-naive: each iteration joins only newly
derived facts against the existing database, repeating until no new facts
appear. The output database contains all input facts plus all derived
facts. Duplicate derivations are collapsed — a fact exists or it does not.

Operational limits (Go API options, listed here because their effects are
user-visible):

- **Iteration limit** (`WithMaxIterations`, default 10000 per stratum): if
  a stratum has not converged at the limit, the transform fails with an
  error rather than returning incomplete results.
- **Fact limit** (`WithFactLimit`, unlimited by default): caps the total
  number of derived facts; exceeding it aborts the transform.
- **Cancellation**: evaluation checks the context regularly, so runaway
  joins can be aborted promptly.

Evaluation-time errors (integer overflow in `is` or `sum`, aggregate over
non-numeric values, iteration/fact limits) abort the transform with an
error naming the offending stratum; there are no partial results.

### 9.4 Optional type checking

Declarations (normally supplied by the JSONL schema; see the
[JSON Fact Extraction Reference](jsonfacts.md)) may assign types to
predicate terms: `string`, `integer`, `float`, `id`, `json`, or
unspecified (any). When the engine is given declarations, constants in
rule heads and bodies are checked against them at compile time, and arity
mismatches are reported.

## 10. Queries

```
ancestor("tom", X)?
suspicious(Host, Pid, Cmd)?
lateral(Src, Dst, ?), high_volume(Src, ?)?
N = count : conn(Src, ?, ?, ?, ?)?
```

A query is a body — one or more atoms, including comparisons, builtins,
and aggregate forms — terminated by `?`. Rules are first evaluated to
fixpoint; the query then runs against the resulting database.

The result is the set of distinct tuples of bindings for the query's
**named** variables, in order of first occurrence. Anonymous variables
(`?`, `_`) and `_`-prefixed names are don't-cares: they constrain matching
but are omitted from the output columns.

Queries are subject to the same safety rules as rule bodies (e.g. a purely
negative query is rejected).

## Appendix A. Grammar summary

```
program     := statement*
statement   := rule "." | fact "." | query "?"
fact        := atom                          ; all terms constant
rule        := atom ":-" body
             | atom ":-" var "=" aggkind [ "(" term ")" ] ":" body
query       := body
body        := literal ("," literal)*
literal     := atom | "not" atom | comparison | var "is" expr
atom        := pred "(" [ arg ("," arg)* ] ")"
arg         := term | pattern
comparison  := term op term                  ; op: = != < > <= >=
expr        := mulexpr (("+"|"-") mulexpr)*
mulexpr     := primary (("*"|"/"|"mod") primary)*
primary     := "(" expr ")" | "-" primary | term
term        := var | string | integer | float
pattern     := object_pat | array_pat        ; positive body args only
object_pat  := "{" [ field ("," field)* ] "}"
field       := (ident | string) ":" (term | pattern)
array_pat   := "[" [ (term|pattern) ("," (term|pattern))* [ "|" var ] ] "]"
aggkind     := "count" | "sum" | "min" | "max"
var         := ident | "?" | "_" | "?" digits
pred        := ident | "@" ident
```

## Appendix B. Reserved and special names

- Reserved words: `not`, `is`.
- Operator identifier: `mod` (in expression context only).
- Aggregate keywords (contextual): `count`, `sum`, `min`, `max`.
- Reserved builtins: `@contains`, `@starts_with`, `@ends_with`,
  `@regex_match` (cannot be overridden).
- Always-present builtins: `@json_get`, `@json_len`, `@json_type`,
  `@json_slice`, `@json_each`, `@json_items`.
