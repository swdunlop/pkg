# Feature: Composite terms (JSON objects and arrays as constants)

## Problem

The only way to get structured JSON into the database today is to flatten
it at load time: jsonfacts mappings pick fields out of each record and
emit flat facts, with synthetic `datalog.ID` values gluing one-to-many
relationships back together. That works, but it forces every interesting
field to be identified *before* loading, and it explodes documents into
ID-joined fact soup:

```json
{"expr": "let id = fresh_id(); assert(\"email\", [id, value.sender, value.time]); map(value.recipients, assert(\"email_to\", [id, #]))"}
```

Security telemetry makes this worse: input schemas are open-ended, records
carry nested objects whose shape varies by event type, and the analyst
often doesn't know which fields matter until they're writing rules. We
want to load a record (or sub-object) as a *single term* and let rules
reach into it lazily:

```prolog
suspicious(P) :- process(P, {name: Name, ppid: PPid}),
                 Name @ends_with ".tmp.exe",
                 PPid = 4.
```

## The load-bearing design decision

**Composites are atomic, hash-consed constants. The unifier never sees
inside them.**

A JSON value is canonicalized, interned once in the `Dict`, and from then
on it is a uint64 like every other constant. Equality is ID equality.
This single decision preserves everything the engine's performance rests
on: the fixed `[16]uint64` `InternedFact` layout, zero-alloc `UnifyV`,
`MatchesBound`, FNV fact hashing, and column indexing (see
FEATURE-jit-column-indexing.md) all work on composites unchanged, because
they only ever compare IDs.

Two facts loaded from different files that carry structurally equal JSON
join on it naturally — canonicalization collapses them to one ID.

The alternative — Prolog-style compound terms with variables inside
structures and recursive structural unification — would invalidate the
flat fact representation and the zero-alloc join path for a capability
that compile-time desugaring (below) delivers at the language level
anyway. Rejected.

## Destructuring patterns: a compile-time desugar

Patterns in body-atom argument positions are pure syntax sugar. The
parser rewrites them into a fresh anonymous variable plus getter atoms;
the engine sees only plain atoms and binding builtins:

```prolog
suspicious(P) :- process(P, {name: Name, pid: Pid}), Pid > 1000.

% parses as:
suspicious(P) :- process(P, ?0),
                 @json_get(?0, "name", Name),
                 @json_get(?0, "pid", Pid),
                 Pid > 1000.
```

Existing machinery does almost all the work:

- The parser already generates fresh anonymous variables (`?0`, `?1`,
  ... — `anonID` counter in parse.go); `isAnonymousVar` in the safety
  checker already exempts them.
- `reorderBody` already schedules binding builtins as soon as their
  inputs are bound, so getters land right after the join that binds the
  object variable no matter where the desugar emits them.
- Non-linear patterns work for free: `{src: X, dst: X}` desugars to two
  getters with the same output variable, and the evaluator's bind case
  already handles "output already bound" by ID comparison — repeated
  variables get equality semantics, as a logic programmer expects.

Pattern semantics:

- **Open matching.** `{name: N}` matches any object that *has* a `name`
  key, regardless of other keys. This is the right default for security
  data, where the input schema's full domain is unknown. Closed matching
  is not offered (see Out of scope).
- **Missing key = no match.** `@json_get` fails, the substitution dies.
  No nulls, no errors.
- **Constants in patterns**: `{status: "active", name: N}` desugars to a
  getter whose output term is the constant `"active"` — the evaluator's
  existing bound-output check turns that into a filter.
- **Nested patterns** recurse through fresh intermediates:
  `{proc: {name: N}}` → `@json_get(?0, "proc", ?1), @json_get(?1,
  "name", N)`.
- **Array patterns**: `[A, B]` desugars to `@json_len(?0, 2)` plus
  indexed gets. `[H | T]` desugars to `@json_get(?0, 0, H),
  @json_slice(?0, 1, T)` — permitted, see the termination rule below.

Where patterns are **rejected**, with compile-time errors:

1. **Rule heads.** A pattern in a head is term construction; head
   construction can nest one level deeper per fixpoint iteration and
   never terminate. Error: `patterns are not allowed in rule heads`.
2. **Negated atoms** (phase 1). Negating a desugared conjunction is not
   the conjunction of negations; correct treatment needs an auxiliary
   predicate rewrite that interacts with stratification. Deferred — see
   Phase 2. Error: `patterns are not allowed under negation (yet)`.
3. **Key positions.** `{Key: V}` with a variable key is enumeration, not
   matching — that's what `@json_items` is for.

Because the desugar happens *in the parser* (during atom parsing, with
getters appended to the body being built), no pattern representation
ever exists in `syntax.Atom` and the sealed `datalog.Term` interface is
untouched. The parser knows whether it is parsing a head, a negated
atom, or a query body, so the restrictions are enforced where the
pattern is read. Programmatic rule construction (building `syntax.Rule`
in Go) writes the getter atoms directly — no API change.

## The termination boundary

Datalog's termination guarantee depends on a finite universe of terms.
The rule that preserves it here:

> **Builtin outputs must be subterms, or size-bounded derivatives, of
> their inputs.**

Destructuring (`@json_get`, `@json_slice`, `@json_each`) only produces
values reachable inside EDB values, so the universe stays finite even
under recursion — list-tail recursion terminates because slices strictly
shrink. What breaks the guarantee is *growing* constructors
(`@json_merge`, `@json_set`, patterns in heads). Phase 1 ships no
constructors. (`is` arithmetic already violates this in principle, so
this is a documented line, not a new enforcement mechanism.)

## Proposed solution

### 1. `datalog.Composite` constant

```go
// Composite is a JSON object or array value, treated as an atomic
// constant. Equality is structural, established at interning time via
// canonical encoding. The sealed Constant interface gains this one
// implementation; it is always used by pointer.
type Composite struct {
    canon   string // canonical JSON encoding (sorted keys, normalized numbers)
    decoded any    // map[string]any | []any, normalized
}

func NewComposite(v any) (*Composite, error) // canonicalizes, validates
func (c *Composite) Value() any              // decoded form (callers must not mutate)
func (c *Composite) Canonical() string
func (c *Composite) String() string          // canon, for term printing
func (c *Composite) isaConstant()
func (c *Composite) isaTerm()
```

Pointer receiver because the struct is not comparable and must never be
compared with `==` — cross-instance equality is `a.Canonical() ==
b.Canonical()`, and inside the engine it is ID equality. Document this
on the type.

`TermType` gains `TermJSON TermType = "json"`, checked in
`CheckConstant` by a `*Composite` type assertion.

### 2. Canonicalization

The correctness of every composite join hangs on this. Rules:

- Object keys sorted lexicographically; no duplicate keys (error).
- Numbers normalized exactly as `interned.NormalizeNumeric` does for
  scalars: a float64 that is an exact int64 canonicalizes as the
  integer. `{"pid": 1.0}` and `{"pid": 1}` must produce identical
  canonical forms, or values loaded from JSON silently fail to join
  with values built in Go. Non-integral floats format with
  `strconv.FormatFloat(f, 'g', -1, 64)` — same as `datalog.Float.String`.
- No NaN/Inf (error at construction; JSON can't express them anyway,
  this guards the Go API path).
- Nested scalars inside `decoded` are normalized (int64, not float64)
  during canonicalization so `Value()` accessors and getter builtins
  observe the same types the dict uses.

This is the highest-test-weight component: a canonicalization bug
produces silent join misses, not errors.

### 3. Dict interning

`Dict.index` is `map[any]uint64` and Go maps/slices are not comparable,
so composites intern under a distinct comparable key:

```go
type compositeKey string // canonical encoding; distinct type so a
                         // composite can never collide with a String term
```

- `Intern`/`InternConstant` on `*Composite`: look up
  `compositeKey(c.canon)`; on miss, append the `*Composite` itself to
  `values` and index it.
- `ResolveConstant`: a `*Composite` in `values` returns as-is — no
  reconstruction, and repeated resolution yields pointer-identical
  results within one dict.
- `Has`/`constantToAny` paths (memory/database.go, eval.go) gain
  `*Composite` cases mapping to `compositeKey`.
- **`Freeze`/`Remap` need no structural changes**: the key is
  self-contained (no child-ID references), so nothing rewrites.
  `typeOrder` gains a slot after `datalog.ID`; `dictCompare` compares
  canonical strings. This self-containment is why the canonical-string
  key was chosen over a node-with-child-IDs encoding.

### 4. Destructuring builtins

Registered as standard engine builtins (`WithBuiltin`), last-arg-output
convention, plus one new engine capability:

| builtin | signature | notes |
| --- | --- | --- |
| `@json_get` | `(Obj, Key, V)` / `(Arr, Idx, V)` | fails on missing key / out of range / non-composite |
| `@json_len` | `(ArrOrObj, N)` | |
| `@json_type` | `(V, T)` | `"object"`, `"array"`, `"string"`, `"integer"`, `"float"`, `"bool"`, `"null"` |
| `@json_slice` | `(Arr, From, T)` | subterm-bounded; enables `[H \| T]` |
| `@json_each` | `(Arr, Elem)` | **multi-result** |
| `@json_items` | `(Obj, K, V)` | **multi-result, two outputs** |

`@json_get` returning a nested object/array interns the sub-value as a
composite in its own right (a subterm, so termination-safe).

JSON `true`/`false`/`null` inside composites need scalar
representations when extracted. Simplest: `true`/`false` → strings
`"true"`/`"false"` is *wrong* (collides with real strings); use
dedicated sealed singleton constants (`datalog.Bool`, `datalog.Null`)
— small, mechanical additions to the same switches `Composite` already
touches. Decide before implementation; the doc assumes dedicated
constants.

**Multi-result builtins** are the one real engine change. Today
`BuiltinFunc` returns one value and the evaluator does Set-once /
compare-if-bound. Add:

```go
// MultiBuiltinFunc yields zero or more output tuples for the given inputs.
type MultiBuiltinFunc func(inputs []any, yield func(outputs []any) bool)
```

with a `bodyItemBindMulti` kind in eval.go whose case in
`evalBodyRecursiveV` / `queryRecursiveV` loops over yielded tuples,
binding (or compare-checking) each output position, recursing, and
restoring the mask — structurally identical to the join case's loop.
Safety checking treats all but the declared output positions as inputs;
`reorderBody` treats it like `bodyItemBind` with multiple out-vars.

### 5. Parser: pattern syntax and desugar

Grammar, in atom argument positions of positive body atoms only:

```
pattern     := object_pat | array_pat
object_pat  := "{" [ field ("," field)* ] "}"
field       := (ident | string) ":" (term | pattern)
array_pat   := "[" [ (term | pattern) ("," (term | pattern))* [ "|" var ] ] "]"
```

During `parseAtom` for body atoms: on `{` or `[` in an argument
position, allocate a fresh `?N` variable as the argument, then append
getter atoms for each field/element to the enclosing body (recursing
for nested patterns). Head and negated-atom parse paths reject the
opening token with the errors named above.

JSON *literal* terms (ground composites written in rule text, e.g.
`config(X, {"mode": "strict"})` as a value rather than a pattern) are
**not** in phase 1 — composites enter via jsonfacts and the Go API
only. A ground pattern in a body atom position is still legal and
means matching, not construction (open matching: `{mode: "strict"}`
desugars to a getter + constant check).

### 6. jsonfacts integration

- Imperative and simple mappings may assert `value` (or any sub-object
  expr result) as a term; the loader wraps `map[string]any`/`[]any`
  results in `NewComposite` instead of erroring.
- Encoder: a `*Composite` term encodes as its decoded JSON value —
  round-trips naturally.
- Matchers: composite terms are skipped by string matchers (they gate
  on string values today; unchanged).

The recommended idiom for security workloads is **flatten and retain**:
flatten the hot fields for indexed joins, and assert the raw record once
under the same synthetic ID for provenance:

```json
{"expr": "let id = fresh_id(); assert(\"event\", [id, value]); assert(\"process\", [id, value.pid, value.name, value.cmdline])"}
```

Detection rules run entirely on the flat predicates; only findings reach
back for evidence:

```prolog
alert(Id, Record) :- suspicious(Id), event(Id, Record).
```

The encoder then emits the complete original record in the alert output
— evidence survives the pipeline without reconstructing it from
flattened predicates. This costs one composite per record (hash-consed,
stored once no matter how many flat facts share its ID) and destructures
nothing except the facts that survive detection. Document this pattern
prominently in jsonfacts/doc.go.

### 7. Aggregates and constraints

- `count` / `count distinct` work on composites (ID-based) unchanged.
- `sum`/`min`/`max` reject composites the same way they reject
  cross-type inputs today.
- `<` `>` `<=` `>=` fail on composites (`compareValues` returns
  `false`); `=` / `!=` are ID comparisons and just work.
- String builtins (`@contains` etc.) fail on composites (existing
  type-assertion behavior).

## Work

1. **`datalog.Composite`** (+ `Bool`/`Null` if adopted) in term.go;
   `TermJSON` in datalog.go. Canonicalization in a new file
   (canon.go) with exhaustive tests: key sorting, number normalization
   (`1.0` ≡ `1`, `-0.0`, large int64 edge), nesting, duplicate-key and
   NaN errors, decoded-form normalization.
2. **Dict support**: `compositeKey`, Intern/Resolve/Has/typeOrder/
   dictCompare cases in internal/interned/dict.go; `constantToAny`
   switches in memory/database.go and seminaive/eval.go.
3. **Multi-result builtin kind**: `MultiBuiltinFunc`, `WithMultiBuiltin`
   engine option, `bodyItemBindMulti` in eval.go (both recursive
   evaluators), safety-checker and reorderBody handling.
4. **Builtins**: the six-table above, registered by default by the
   seminaive engine (not opt-in — patterns desugar to them, so they
   must always exist). Unit tests per builtin including failure modes.
5. **Parser desugar** in syntax/parse.go: pattern grammar, fresh-var
   emission, getter appending, head/negation/var-key rejection with
   position-carrying errors. Round-trip note: `Rule.String()` prints
   the desugared form; document that in syntax/doc.go.
6. **jsonfacts**: loader wraps composite mapping results; encoder
   handles `*Composite`; doc.go gains a section showing
   "assert the whole record, destructure in rules" as the alternative
   to flattening.
7. **Docs**: datalog/doc.go — composite semantics, open matching, the
   termination rule (subterm-bounded builtins), pattern restrictions.
8. **Benchmarks**: pattern-destructure vs. pre-flattened equivalents on
   a jsonfacts-shaped workload, to quantify the flatten-vs-lazy
   tradeoff and give users guidance; interning throughput for large
   records (canonicalization cost per MB).

## Risks / open questions

- **Memory: the dict holds every distinct composite forever** — both
  canonical string and decoded form, so "assert the whole record" for
  high-cardinality telemetry approaches holding the input twice in
  RAM. Phase 1 accepts this and documents it: the flatten-and-retain
  idiom (one `event(Id, Record)` composite per record, flat predicates
  for everything rules join on) is the intended shape, and it stores
  each record exactly once. If it bites, the
  fallback design is keeping only a 128-bit structural hash as the
  index key (with the decoded form retained for getters) and treating
  collisions as unreachable — a contained change to compositeKey.
  Decide only with real workload numbers.
- **Canonicalization cost at load time.** Sorting keys and re-encoding
  every record is O(size log keys) per record. jsonfacts loading is
  already the batch phase, so this is the right place to pay it, but
  the benchmark in Work 8 should confirm it doesn't dominate.
- **Patterns don't index.** `process(P, {status: "active"})` scans the
  predicate and destructures per fact. The idiom is to project hot
  fields into a derived predicate (one rule), which the JIT column
  indexes then serve. Document the idiom; do not attempt
  expression indexes here.
- **`Fact` JSON serialization.** `datalog.Fact` has json tags and
  `[]Constant` marshals as the concrete values; `*Composite` needs a
  `MarshalJSON` (emit decoded value) and the jsonfacts encoder path
  needs a matching case. Unmarshal of facts (if anything does it)
  cannot reconstruct typed constants today either — verify nothing
  depends on it before worrying.
- **Mutation of `Value()` results.** Getters and encoders share the
  decoded form; a caller mutating the returned map corrupts every
  holder of that composite. Documented "must not mutate" in phase 1;
  a deep-copy accessor can be added if it ever bites.
- **`MaxRuleVars = 16`.** Desugaring consumes variable indices for
  fresh intermediates; a rule with many patterns can now exceed 16
  distinct variables more easily. Detect at compile time and error
  clearly (today's behavior on overflow should be checked — if it's a
  silent mask wrap, fix that regardless).

## Phase 2 (explicitly deferred)

- **Patterns under negation** via auxiliary-predicate rewrite
  (`!process(P, {name: "evil"})` → `!aux0(P)` + generated rule),
  integrated with stratify.go.
- **Growing constructors** (`@json_set`, `@json_merge`, object literals
  in heads) behind a termination story (e.g. forbidden in recursive
  strata, mirroring stratified negation).
- **JSON literal terms** in rule text as ground constants.
- **Closed / exact patterns** — only if a real use case appears; open
  matching is the contract.

## Out of scope

- Structural unification / variables inside stored terms.
- Schema validation inside composites (TermJSON is shape-blind).
- Indexing into composite fields (project-then-index is the idiom).
- JSONPath or expression-based access languages — getters and patterns
  compose to cover access; a path language is a different feature.
