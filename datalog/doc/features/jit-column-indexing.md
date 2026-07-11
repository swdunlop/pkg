# Feature: Just-in-time column indexing for fact scans

## Problem

`InternedFactSet` indexes facts on exactly one access path: the first
argument. `Scan` (internal/interned/interned.go) consults `ByArg0` when
position 0 is bound; every other binding pattern falls back to a full
predicate scan filtered by `MatchesBound`:

```go
// interned.go, Scan:
if val, ok := bound.Get(0); ok && fs.ByArg0 != nil { ... O(1) lookup ... }
if fc := fs.ByPred[k]; fc != nil { return ScanResult{facts: fc.facts} } // O(n)
```

So a rule body like:

```prolog
alert(P) :- process(P, Name, Cmd), parent(Q, P), suspicious(Q).
```

pays O(|parent|) for the `parent(Q, P)` join on every substitution, even
though `P` is bound — the bound column is position 1, not 0. Semi-naive
evaluation re-runs the same compiled body every fixpoint iteration, so a
bad access path is paid once per delta tuple per iteration, not once.

The pain scales with fact count: for security-telemetry workloads
(jsonfacts loads hundreds of thousands of rows), a single second-column
join turns a rule from linear to quadratic.

## Design constraints and observations

**1. Binding patterns are statically known.** `reorderBody` (eval.go)
already computes `boundVars` at each join position during rule
compilation. At the moment `Scan` is called for a given `bodyItem`, the
set of bound columns is a function of the rule, not the data. We could
compute demanded indexes entirely at compile time. However —

**2. Fact sets grow between iterations.** `existing` absorbs `emitted`
via `Merge` at each fixpoint iteration, and `factChunks` is append-only.
Any index must tolerate facts appended after it was built. `Merge`
currently contains ~60 lines of offset-fixup logic to keep `ByArg0`
consistent across merges (full-to-full index stealing, light-to-full
rebuilds).

**3. Facts are append-only, never mutated or deleted.** This is the key
simplification: an index over positions `[0, w)` of the fact slice stays
valid forever; only the tail `[w, len)` is unindexed. A single watermark
per index gives incremental maintenance for free.

**4. Three fact sets participate in evaluation.** `existing` is the big
one and the main scan target. `emitted` and `delta` are created with
`NewLightInternedFactSet` (no ByArg0 at all) and are usually small —
they hold one iteration's new facts. Indexing them is out of scope
until measured.

**5. The evaluator, not the caller, knows selectivity.** When several
columns are bound, the best index is the one with the most distinct
values. `len(index map)` is a free cardinality estimate — no statistics
infrastructure needed.

**Design decision: build indexes lazily inside `Scan`, keyed by
(pred, arity, column), each with a catch-up watermark.** Lazy beats
compile-time demand registration because it needs no new plumbing
between the evaluator and the fact set, it naturally skips access paths
that stratification or delta emptiness make unreachable, and semi-naive
iteration guarantees the build cost amortizes: the same compiled body
runs every iteration, so an index built on first use is hit on every
subsequent use. The watermark mechanism also lets `Merge` drop its
ByArg0 fixup logic entirely — indexes self-heal on next scan.

## Proposed solution

### Replace `ByArg0` with per-column indexes

```go
// colIndex indexes one argument position of one (pred, arity) fact slice.
// Facts at positions >= indexedUpTo are not yet indexed; catchUp extends
// the index to cover them. Facts are append-only, so entries never go stale.
type colIndex struct {
    m           map[uint64][]int32 // value -> positions in factChunks.facts
    indexedUpTo int32
}

type InternedFactSet struct {
    ByPred map[PredArityI]*factChunks
    ByCol  map[PredArityI]map[int]*colIndex // nil for light sets
    Index  map[uint64]struct{}
}
```

`ByArg0` disappears. Column 0 is just the first entry in `ByCol[k]`,
created on first scan like any other column.

### Scan: choose a column, catch up, look up

```go
func (fs InternedFactSet) Scan(pred uint64, arity int, bound *BoundSet) ScanResult {
    k := PredArityI{pred, arity}
    fc := fs.ByPred[k]
    if fc == nil { return ScanResult{} }
    if fs.ByCol == nil || bound.Mask == 0 {
        return ScanResult{facts: fc.facts}
    }
    col := fs.chooseColumn(k, bound)     // see below
    if col < 0 {
        return ScanResult{facts: fc.facts}
    }
    ci := fs.colIndexFor(k, col)         // create if absent
    ci.catchUp(fc.facts, col)            // index [indexedUpTo, len)
    val, _ := bound.Get(col)
    indices := ci.m[val]
    if indices == nil { return ScanResult{indices: emptyIndices} }
    return ScanResult{facts: fc.facts, indices: indices}
}
```

`catchUp` is the entire maintenance story:

```go
func (ci *colIndex) catchUp(facts []InternedFact, col int) {
    for i := ci.indexedUpTo; i < int32(len(facts)); i++ {
        v := facts[i].Values[col]
        ci.m[v] = append(ci.m[v], i)
    }
    ci.indexedUpTo = int32(len(facts))
}
```

`ScanResult` is unchanged — it already abstracts indexed vs. unindexed
iteration behind `Len`/`Fact`.

### Column choice

`chooseColumn` picks among bound positions:

1. If exactly one column is bound, use it.
2. Among bound columns that already have an index, pick the one with
   the largest `len(ci.m)` (highest cardinality = most selective).
3. If no bound column has an index yet, build on the **lowest bound
   position**. Deterministic, cheap, and matches the current ByArg0
   behavior when position 0 is bound.

Rule 3 is deliberately dumb. A build-on-first-use policy means the
second iteration's scan hits rule 2 with real cardinality data if
another column's index appears later. Don't add cost-based logic here
until benchmarks demand it.

### Threshold: don't index tiny relations

Building a map for a 12-fact predicate costs more than scanning it.
Gate index creation (not use) on `len(fc.facts) >= minIndexSize`, with
`minIndexSize = 64` as a starting point (tune against benchmarks).
Below the threshold, fall through to the full scan — `MatchesBound`
already filters correctly.

### Merge simplification

`Merge` currently maintains `ByArg0` across four cases (existing/new
key × light/full source). All of it goes away:

```go
func (fs InternedFactSet) Merge(other InternedFactSet) {
    maps.Copy(fs.Index, other.Index)
    for k, ofc := range other.ByPred {
        if dfc := fs.ByPred[k]; dfc != nil {
            dfc.appendFrom(ofc)
        } else {
            fs.ByPred[k] = ofc
            // NOTE: do not steal other.ByCol — indices reference other's
            // slice which we just adopted, so they'd actually be valid,
            // but keeping the rule "indexes are only created by Scan"
            // is worth the one rebuild.
        }
    }
}
```

Existing indexes stay valid (append-only) and catch up on next scan.
`Add`/`AddUnchecked` similarly drop their inline ByArg0 maintenance.

Optional refinement: when adopting a wholesale `factChunks` from a full
set, stealing `other.ByCol[k]` is correct (indices are positions into
the same slice) and saves a rebuild. Implementer's call; correctness
does not depend on it.

### Clone

`Clone` (used by `memory.Database.Extend`) can copy indexes or drop
them; dropping is correct (they rebuild on demand) and cheaper. Drop
them, and document that a cloned set starts index-cold.

## Concurrency

`InternedFactSet` has no synchronization today and evaluation is
single-goroutine per Transform. Lazy building means `Scan` now
**mutates** the set, so any future parallel-stratum work must either
lock around `colIndexFor`/`catchUp` or pre-build. Put one comment on
`Scan` stating it is not safe for concurrent use — same contract as
today (`Add` was never safe either), but now non-obvious because scans
look read-only.

The same applies to `memory.Database.Query`, which is a public API and
plausibly called concurrently. Two options:

- **A (recommended):** give `Database` a small mutex around `Scan`.
  Query volume is human/tool scale, not hot-loop scale.
- **B:** have Query pass a flag that disables lazy building (use an
  index if present, never create one).

## Work

1. **`colIndex` type + `ByCol` field** in internal/interned/interned.go;
   delete `ByArg0`. Update `NewInternedFactSet`, `NewInternedFactSetCap`,
   `NewLightInternedFactSet` (light sets keep `ByCol: nil`, which
   disables indexing exactly as `ByArg0: nil` does today).
2. **Rewrite `Scan`** with column choice, threshold, and catch-up.
3. **Strip index maintenance from `Add`, `AddUnchecked`, `Merge`,
   `Clone`.** This deletes more code than step 2 adds.
4. **memory.Database:** `Query`'s doc comment says the boundSet
   "filters via the byArg0 index" — update comment in `matchFact`
   (memory/database.go); add the Query-path concurrency decision from
   above.
5. **memhook.go / jsonfacts loader:** grep for `ByArg0` uses outside
   interned (loader.go builds fact sets); switch constructors as needed.
6. **Tests** in internal/interned (new file, interned_test.go):
   - Scan with column 1..k bound returns exactly the matching facts
     (compare against brute-force filter).
   - Catch-up: build index, Add more facts, Scan again, verify tail
     facts are found.
   - Merge then Scan finds facts from both sides on a non-zero column.
   - Below-threshold predicate never allocates a colIndex (assert via
     `ByCol` inspection).
   - Empty-match returns `emptyIndices` sentinel, not nil (pins the
     indexed-but-no-match vs. unindexed distinction).
7. **Benchmarks.** Existing seminaive benchmarks
   (`BenchmarkJoinRule`, `BenchmarkJoin1_1000/5000`,
   `BenchmarkTransitiveClosure*`, `BenchmarkSameGeneration*`) guard
   against regression on arg0-friendly workloads. Add one benchmark
   that is specifically arg0-hostile: a two-atom join where the second
   atom binds only its **second** argument, e.g.
   `path(X, Y) :- edge(X, Y). reach(Y) :- start(X), edge2(_, X, Y).`
   shaped so today's code full-scans. Record before/after in the PR.

## Risks / open questions

- **Memory.** Worst case one `[]int32` map per (pred, arity, column)
  actually scanned. Bounded by access paths in the ruleset, not by
  arity — a column no rule binds never gets an index. The threshold
  keeps small predicates free. If memory becomes a concern, an LRU or
  a per-set index budget can come later; don't build it now.
- **First-scan latency spike.** The first bound-column scan of a large
  predicate pays an O(n) build inline. Acceptable: it replaces an O(n)
  scan that would have happened anyway, and every later scan is O(1).
- **Multi-column binding patterns.** A join binding columns {1, 3}
  uses only one index and post-filters with `MatchesBound`. If profiles
  later show this dominating, a mask-keyed composite index
  (`map[maskedValueHash][]int32` per demanded column mask) slots into
  the same colIndex/watermark scheme. Explicitly out of scope here.
- **`emitted`/`delta` stay light.** Delta scans remain linear. Deltas
  are small in typical fixpoints, but a workload with huge per-iteration
  deltas (wide fan-out rules) would not benefit. Measure before acting;
  the fix (use full sets for emitted) is a one-line constructor change
  guarded by a size heuristic.
- **Cardinality heuristic can mislead.** `len(ci.m)` measures distinct
  values at last catch-up, which is fine, but a high-cardinality column
  with one hot value (e.g. 90% of rows share `status = "ok"`) still
  yields a long posting list for that value. This matches what every
  hash-index database lives with; not worth per-value statistics.

## Out of scope

- Composite (multi-column) indexes — see risk note above.
- Indexing `emitted`/`delta` sets.
- Hash-join restructuring of the evaluator (replacing indexed
  nested-loop join entirely).
- Range scans / ordered indexes. `Dict.Freeze` sorts IDs by value, so
  a sorted-posting-list design could someday serve `<`/`>` constraints;
  that is a different feature.
- Persistence of indexes across `Clone` (dropped deliberately).
