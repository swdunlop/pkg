# predicate_deps / explain_fact model matcher dependency direction backwards

Status: **analysis, not yet fixed.** Discovered 2026-07-18 while validating
workbench-v2 phase 1c (commit 69560a8). Scott confirmed the feature's intent
(why-trees). This note is self-contained so a fresh context can implement the
fix without re-deriving anything.

## TL;DR

`predicate_deps` and `explain_fact` exist to walk "why is this fact true?"
back to its premises (provenance / why-trees). For **rules** they do this
correctly. For **matchers** they attach the matcher to the wrong predicate,
which breaks the premise-walk exactly where it should cross a matcher:

- A matcher `{predicate: event, term: 2, contains: [...]}` **reads** `event`
  facts and **emits** `contains(value, pattern)` facts (arity 2). It is a
  *consumer* of `event` and a *producer* of `contains`.
- The code registers the matcher under the predicate it **reads** (`event`),
  as if `event` were *produced by* / *depends on* the matcher.
- Result: `predicate_deps(contains, 2)` reports **no producer** (walk
  dead-ends, never learns `contains` came from a matcher on `event`), while
  `predicate_deps(event, 3)` **points at the matcher** (a wrong branch — an
  `event` fact comes from the source mapping, and the matcher is downstream).
- `explain_fact`'s base-fact branch has the **identical inversion**.

The fix keeps the tools' direction; it moves the matcher to the predicate it
actually produces and adds the consumer edge on the one it reads.

## What these tools are for (confirmed intent)

Design decision 8 (`doc/features/workbench-v2.md`): `predicate_deps` is static
dependency analysis in both directions, and `explain_fact` is a post-hoc
one-step derivation. Both serve the same goal — an agent/human asking "why is
`at_risk(host)` true?" builds an explanation tree by walking a fact back to
its premises. Scott's framing: *"help users and agents reason about 'why is
this true?' … build trees that explain the decision that culminated in a new
fact … when we want to know why X is an ancestor of Y, we need to know that
it is because X is a parent of Z, and Z is an ancestor of Y."*

So the intended orientation is: **report what a fact/predicate depends on
(its producers/premises).** The bug is not the orientation; it is that the
matcher's producer edge is recorded on the wrong node.

## Matcher semantics (the ground truth)

Source of truth: `jsonfacts/matcher.go` (`compileMatchers`, `applyMatchers`,
`matchPred`) and `jsonfacts/loader.go:45-51` (matchers run after source
mappings; their output facts are appended to the loaded fact set, so the
engine sees them as **base/loaded facts**, not rule-derived).

A `Matcher{Predicate: P, Term: T, CaseInsensitive: ci, Windash: wd, <kinds>}`:

- **reads** facts named `P` whose term `T` is a `datalog.String`
  (`applyMatchers`, `matcher.go:327-341`), and
- for each populated pattern kind, **emits** a NEW fact
  `<kindPred>(String(value), String(pattern))` — always **arity 2**
  (`matcher.go:315-325`).

The emitted predicate name is the **match kind**, not `P`. Full table
(`matchPred(base, ci, wd)` prefixes `ci_` when ci and `wd_` when wd):

| Kind (Matcher field)      | Emitted predicate name                         | ci prefix? | windash prefix? | Notes |
|---------------------------|------------------------------------------------|-----------|-----------------|-------|
| `Contains`                | `matchPred("contains", ci, wd)`                | yes       | yes             | `contains`, `ci_contains`, `wd_contains`, `ci_wd_contains` (matcher.go:123) |
| `StartsWith`              | `matchPred("starts_with", ci, wd)`             | yes       | yes             | matcher.go:124 |
| `EndsWith`                | `matchPred("ends_with", ci, false)`            | yes       | **no**          | windash never applied (matcher.go:125) |
| `RegexMatch`              | `matchPred("regex_match", ci, false)`          | yes       | **no**          | windash never applied (matcher.go:126) |
| `Base64`                  | `"base64_contains"` (base of the variants)     | no        | no              | via `compileBase64Patterns(mc.Base64, false, "base64_contains")` (matcher.go:229); emitted as `bv.pred` (matcher.go:446) |
| `Base64UTF16`             | `"base64_utf16le_contains"`                    | no        | no              | matcher.go:232 |
| `CIDR`                    | literal `"cidr_match"`                          | no        | no              | no prefixes, no per-pattern variation (matcher.go:461) |

Two consequences that matter for the fix:

1. **Every matcher-produced predicate is arity 2.** This moots the original
   "arity 99" symptom (the finding that started this): the produced predicate
   is always arity 2, and the *read* predicate `P` is depended-on at whatever
   arity `P` exists (any arity with a term at index `T`).
2. **The naming has per-kind quirks** (ends_with/regex drop windash; base64
   and cidr use fixed names with no prefixes; a matcher with several kinds
   produces several predicates). Hardcoding this table in `cmd/datalog` would
   silently drift from `jsonfacts` if the naming ever changes. **Fix at the
   mechanism:** expose the produced-predicate names from `jsonfacts` as a
   single source of truth (see Recommended fix) rather than re-deriving them
   in the workbench.

Also note: many matchers can emit the **same** predicate (e.g. two matchers
with `contains` and identical flags reading different source predicates both
emit `contains`). So a produced predicate's producer list is naturally
many-to-one, and each entry should name the source predicate + term it read.

## The two broken walks (concrete, mordor)

Data flow in `examples/mordor` (`rules.dl` uses `contains(DstPort, "445")`):

```
source mapping ──> conn(..., DstPort, ...) ──[matcher reads term]──> contains(value, pattern) ──[rule body]──> at_risk / suspicious ...
```

Reproduced 2026-07-18 (probe, since deleted) on a minimal
`event`/`contains` config:

- `predicate_deps(event, 3)` → `DependsOnMatchers = [event/term-2 matcher]`,
  `DependedOnBy = []`. **Wrong:** claims `event` is produced by the matcher.
- `predicate_deps(contains, 2)` → `DependsOnMatchers = []`,
  `DependedOnBy = []`. **Wrong:** the matcher that genuinely produces
  `contains` is invisible; the why-walk dead-ends here.

`explain_fact` mirrors this (`explainBaseFactLocked`, `schema_reads.go:453`,
matches `m.Predicate == fact.Name`):

- `explain_fact(contains("x","445"))` → base fact, `CandidateMatchers = []`
  (no matcher has `Predicate == "contains"`). Dead-end.
- `explain_fact(event(...))` → base fact, `CandidateMatchers = [the matcher]`
  — wrongly attributes the source-mapped `event` fact to the matcher.

So the why-tree feature is broken at both the schema level (`predicate_deps`)
and the fact level (`explain_fact`).

## Correct model

For a matcher `M{Predicate: P, Term: T, ...}` producing predicates
`Q1, Q2, ... (each arity 2)`:

- **`M` is a producer of each `Qi`** → `Qi` **DependsOn** `M`
  (`predicate_deps(Qi, 2)` lists `M`, annotated with `P` and `T` it read).
- **`M` is a consumer of `P`** → `P` **DependedOnBy** `M`
  (`predicate_deps(P, arity)` lists `M` under DependedOnBy, for arities where
  term `T` exists, i.e. `arity > T`).
- **`M` is NOT a producer of `P`** → remove it from `P`'s DependsOn entirely.

`explain_fact(fact)` base branch: given a base fact named `N`/2, the candidate
producers are the matchers whose produced set contains `N` (each reported with
its read predicate `P` + term `T`), NOT the matchers with `Predicate == N`.
For a base fact that is a plain source-mapped predicate (e.g. `event`), no
matcher should be listed.

Edge cases to honor:
- A predicate can be produced by both a source mapping AND matchers (if a user
  names a mapping predicate `contains`) — list both; both genuinely produce it.
- `predicateKnown` (`schema_reads.go:252`, the unknown-predicate gate) must be
  corrected in BOTH directions too: a predicate is "known" if it is a read
  predicate of some matcher (at an arity > that matcher's term) OR a produced
  predicate of some matcher (arity 2). The current matcher branch
  (`schema_reads.go:270`) checks `m.Predicate == name` ignoring arity — keep it
  consistent with whatever the two predicate_deps directions decide.

## Recommended fix (fix at the mechanism)

1. **`jsonfacts`: one source of truth for produced-predicate names.** Add an
   exported helper, e.g. `func (m Matcher) ProducedPredicates() []string` (or a
   package func) that returns the arity-2 predicate names `m` emits, computed
   by the SAME `matchPred`/base64/cidr logic `compileMatchers` uses. Ideally
   refactor `compileMatchers` to consume the helper so the two cannot drift.
   This is the mechanism: the workbench must not hardcode a parallel copy of
   the emit-naming table above.
2. **`cmd/datalog/schema_reads.go`:**
   - `predicateDeps`: replace the `DependsOnMatchers` loop
     (`schema_reads.go:179`) — matchers whose `ProducedPredicates()` contains
     `(name)` at arity 2 go under **DependsOn**; matchers whose `Predicate ==
     name` (and `name`'s arity `> Term`) go under **DependedOnBy**. Annotate
     each matcher entry with the source predicate/term as appropriate. The
     `matcherRef` output shape may need a field for the read predicate/term so
     an agent can navigate.
   - `explainBaseFactLocked` (`schema_reads.go:453`): candidate matchers are
     those PRODUCING `fact.Name`, reported with their read predicate + term.
   - `predicateKnown` (`schema_reads.go:270`): both-direction matcher check.
   - **Revert/replace `matcherProducesArity`** — the FIX-3 arity gate added on
     branch `fix/phase1cd-validation`. It made the *arity* less wrong inside
     the backwards model; it is superseded by this correct-direction model
     (all matcher products are arity 2, so it is unnecessary).
3. **`doc/features/workbench-v2.md` decision 8:** correct the wording. Two
   statements are factually wrong about matcher semantics:
   - *"for a base predicate, the schema matchers/declarations that produce it"*
     — matchers do not produce their `Predicate`; they produce the match-kind
     predicate.
   - *"matchers never depend on predicates the way rules do (a matcher only
     ever reads ITS OWN configured predicate/term, never another predicate's
     output)"* — a matcher DOES depend on (consume) its `Predicate`; that is
     precisely the DependedOnBy edge. Reframe: a matcher consumes its
     configured `(predicate, term)` and produces the match-kind predicate.

## Blast radius / files

- `jsonfacts/matcher.go` (add exported produced-predicates helper; optional
  refactor of `compileMatchers`).
- `cmd/datalog/schema_reads.go` (`predicateDeps`, `explainBaseFactLocked`,
  `predicateKnown`; remove `matcherProducesArity`; possibly extend
  `matcherRef`/`declarationRef` output shapes).
- `doc/features/workbench-v2.md` (decision 8 wording).
- Tests: rework `TestPredicateDeps_MatcherDoesNotInventArity`
  (`cmd/datalog/schema_reads_...` test) which currently encodes the arity
  behavior inside the backwards model; add why-walk regressions for both
  `predicate_deps` (event vs contains) and `explain_fact` (contains vs event),
  reproducing the two broken walks above and asserting the corrected edges.

## Current branch state (context for the fix)

Branch `fix/phase1cd-validation` (uncommitted at time of writing) carries the
other phase-1c/1d validation fixes, all Opus-validated sound — keep them:

- **FIX 1 (HIGH):** duplicate CRUD-key rejection in
  `validateConfigKeyUniqueness` (`schema_serialize.go`), wired into
  `parseConfigFormat`. Done, validated.
- **FIX 2 (HIGH):** whole-file staleness guard in `commitSchemaWrite`
  (`schema_crud.go`) — a concurrent cross-item vim/agent edit is no longer
  clobbered. Done, validated.
- **FIX 4:** `applySchemaLocked` nulls `derivedProv`; lock-contract docs on
  `getRuleGroup`/`listRuleGroups`. Done, validated.
- **FIX 3 (this note):** the matcher-direction rework — NOT done. The interim
  `matcherProducesArity` arity gate on this branch should be reverted/replaced
  by the correct-direction model described here.

Related known limitation (separate, already tracked): `explain_fact` premises
carrying `datalog.ID`/`*Composite` terms print unparseable literals — see the
"Facts with ID/Composite head terms are unexplainable" item in `TODO.md`.
