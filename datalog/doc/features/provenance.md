# Feature: Witness provenance for derived facts

## Problem

The engine can conclude but cannot explain. When `Transform` derives
`concern("ws01", 87)`, nothing records which rule fired over which
facts; the derivation is discarded the moment the head is interned. Two
consumers need exactly that record:

- **The LLM.** The pipeline's output is "situations deserving LLM
  attention with justification." Asking a model to *invent* the
  justification for an alert is the hard, unreliable task; asking it to
  *narrate* a concrete derivation — "this fired because these four
  evidence facts satisfied these two rules" — is a translation task
  models are reliably good at.
- **The reviewer.** Security researchers who cannot author Datalog can
  absolutely judge "should this rule have fired on these facts?" — but
  only if the workbench can show the derivation. Without provenance,
  reviewing a detection means reading rule text, which reinstates the
  logic-literacy bar this project is designed to avoid.

Efficiency is explicitly not a requirement: this is a batch pipeline
and an interactive workbench, not a hot loop. Correctness of the
explanation and simplicity of the mechanism dominate.

## Design constraints and observations

**1. The emit closure already holds everything a witness needs.** In
`evalRules` (`seminaive/eval.go`), at the moment
`emitted.AddUnchecked(fact, fk)` runs, the closure has the compiled
rule `cr` and the complete substitution `sub`. Crucially, the join
machinery does *not* need to track which facts matched: grounding each
positive body atom under the final `sub` (the same `HashAndGroundV`
used for the head) reproduces the exact fact key that satisfied it.
Witness capture is therefore a pure add-on at the emit seam — zero
changes to join, reorder, or delta bookkeeping.

**2. One witness per fact is enough.** A fact may have many
derivations; semi-naive dedup keeps only the first
(`existing.Index`/`emitted.Index` check ahead of `AddUnchecked`).
Recording the witness at first emission gives exactly one derivation
per fact, deterministic per run (rule order and iteration order are
fixed), and sidesteps the exponential blowup of full why-provenance.
"Here is *a* proof" serves both consumers; "here are *all* proofs"
serves neither better.

**3. Non-join body items ground to self-evident strings.** Constraints,
`is`-expressions, and builtins are deterministic given their inputs, so
their ground rendering under `sub` (`@time_diff(1700000060, 1700000000,
60)`, `PortCount > 20` with PortCount = 34) is its own justification.
Negated atoms ground to the atom that had no matching fact — sound to
assert because stratification guarantees the negated predicate was
complete when checked.

**4. The dict is immutable after publish.** Witnesses are cheapest
stored as interned fact keys, resolved to `datalog.Fact` lazily. The
clone-before-publish discipline on `Dict` means a recorder that
retains the evaluation's frozen dict can resolve keys forever without
copying at record time.

**5. Aggregates cannot retain full support sets.** `aggGroup`
accumulators deliberately stream (the 07-15 efficiency work); a
`count` over 500k solutions must not materialize them for provenance.
A capped sample plus the true group cardinality explains an aggregate
honestly: "PortCount = 34 aggregated over 34 solutions; first 10: …".

## Proposed solution

### Recording

`seminaive.WithProvenance()` enables a per-Transform recorder. When
enabled, the emit path records:

```go
type witness struct {
    rule     int      // index into the stratum's rule list (rule text + Doc recoverable)
    body     []uint64 // fact keys of positive join atoms, ground under sub
    detail   []string // ground renderings: constraints, is, builtins, negated atoms
}
// recorder: map[uint64]witness — fact key -> first derivation
```

Aggregate heads record a variant: aggregate rule index, group solution
count, and up to `witnessSampleCap` (default 10) sampled contributor
tuples captured as the group streams. Facts asserted directly in the
ruleset and facts present in the input database record no witness —
absence from the map *is* the "base fact / asserted fact" leaf marker,
distinguished by which database contains them.

The recorder lives and dies with one Transform: `WithFactLimit` aborts,
context cancellation, and eval errors discard it along with the partial
results (existing behavior — partial provenance for discarded facts
must not survive).

### API

```go
prov := seminaive.NewProvenance()
tr, _ := syntax.Parse(seminaive.New(seminaive.WithProvenance(prov)), rules)
out, _ := tr.Transform(ctx, db)

d, ok := prov.Explain(fact)       // one step: rule + ground body facts + detail
tree := prov.ExplainTree(fact,    // recursive walk with caps
    seminaive.MaxDepth(8), seminaive.MaxNodes(200))
```

`Derivation` nodes carry the resolved `datalog.Fact`, the rule's
rendered text and `Doc` (from the predicate-docs feature — an
explanation that can cite "%% A source address probed many distinct
ports…" is the payoff of landing that feature first), the detail
strings, and child derivations. Shared subtrees (the same fact
supporting many parents) render once and are referenced thereafter;
cycles are impossible (a fact's witness precedes it) but the walker
caps depth and node count anyway — caps are output-size hygiene, not
safety.

A text renderer produces the LLM/console form:

```
concern("ws01", 87)
└─ rule: concern(H, S) :- S = sum(W) : indicator(H, ?, W).
   S = 87 aggregated over 3 solutions (all shown):
   ├─ indicator("ws01", "port_scan", 40)
   │  └─ rule: %% A source address probed many distinct ports…
   │     port_scan("10.0.0.5", "ws01", 34); PortCount > 20 [34]
   │     └─ … 34 conn facts, first 10 shown …
   ├─ indicator("ws01", "new_admin", 35)   [base fact]
   └─ indicator("ws01", "off_hours", 12)   [base fact]
```

### Surfaces

- **MCP:** an `explain` tool: predicate + constant terms (+ optional
  depth) → the rendered tree, one fact per call. Enabled whenever the
  session evaluates with provenance on.
- **Workbench:** a "why?" affordance per row in the Fact Browser for
  derived predicates, opening the derivation tree in the drawer. This
  is the reviewer surface — thumbs-up/down on derivations is how
  non-logicians contribute signal.
- **REPL:** `.why concern("ws01", 87)`.
- **Session policy:** `cmd/datalog` sessions enable provenance by
  default (interactive scale; the memory cost is a map entry per
  derived fact). The library default stays off.

### Session cache interaction

The derived-query cache (`cacheDerivedQuery`) stores the base fixpoint
per generation. The recorder must be cached *beside* the database it
explains and swapped atomically with it — an `explain` after a cache
hit must resolve against the recorder from the Transform that produced
the cached DB, not a later one. Same generation guard, same locked
swap; a cache-admission refusal drops both. The synthetic `_q_` stage
runs its own Transform over the cached base; its recorder is
query-scoped and discarded with the query result (explaining `_q_`
heads is not a goal; explaining the user predicates under them uses
the cached base recorder).

## Work

1. **Recorder + emit hook** (`seminaive/eval.go`, `engine.go`):
   `WithProvenance`, witness capture at the emit closure (positive-atom
   grounding via `HashAndGroundV`, detail rendering), frozen-dict
   retention. Gated so the disabled path costs one nil check.
2. **Aggregate witnesses** (`seminaive/aggregate.go`): group
   cardinality + capped contributor sample captured during streaming;
   verify the accumulator's memory profile is unchanged when disabled.
3. **`Explain`/`ExplainTree` + text renderer**, with rule text and
   `Doc` threading from the compiled stratum back to `syntax.Rule`.
4. **Correctness tests:** for every emitted witness, assert each body
   fact key is genuinely present in the database (a witness citing a
   nonexistent fact is a lie — this invariant is cheap to check
   exhaustively in tests); negation/constraint/builtin/aggregate
   renderings; determinism across repeated Transforms; discard on
   abort. Property test candidate for the fuzz plan: witnesses of all
   derived facts resolve, and replaying each witness's rule on its body
   facts re-derives its head.
5. **Session wiring** (`cmd/datalog/session.go`): recorder beside the
   cached DB under the generation guard; MCP `explain` tool
   (`mcp.go`, `mcp_docs.go`); Fact Browser "why?" drawer; REPL `.why`.

Order: 1 → 3 → 4 prove the mechanism in the library; 2 and 5 follow.
Land after (or with) predicate-docs so explanations cite rule docs from
day one.

## Risks / open questions

- **Memory.** ~(1 map entry + one `[]uint64` of join arity) per derived
  fact. For a workload within `WithFactLimit` this is bounded and
  small relative to the facts themselves. If a pathological ruleset
  hurts, the option is per-session off, not a cleverer structure.
- **First-witness bias.** The recorded derivation depends on rule and
  iteration order; a reviewer might see a less intuitive proof than the
  one they'd pick. Acceptable: any true proof justifies the fact.
  Deterministic order at least makes it stable across runs.
- **Double-occurrence grounding.** A body atom appearing twice with
  different variables grounds to two distinct facts under `sub` —
  correct by construction, but the tests in step 4 must cover it (it
  is the same trap the delta-reorder work had to prove out).
- **Externals.** Facts fetched by external predicates are base facts
  from the engine's view; the witness cites them like any other. If an
  external's own upstream justification ever matters, that is a
  different feature.
- **`set_rules` invalidates explanations.** A derivation tree open in
  the drawer refers to a generation that may be swapped out beneath
  it. The existing SSE repaint discipline (everything repaints on
  session change) covers this — the drawer must repaint or close on
  generation change like every other pane.

## Out of scope

- Full why-provenance (all derivations per fact) and why-not
  provenance ("why did this rule *not* fire?" — enormously valuable
  for rule debugging, enormously harder; it requires tracking failed
  joins, not successful ones).
- Provenance semirings, weighted/probabilistic provenance.
- Persisting provenance to disk or shipping it in JSONL output
  (`jsonfacts.Encoder` unchanged; the pipeline can call `ExplainTree`
  and emit renderings itself).
- Incremental maintenance of witnesses (moot — evaluation is
  full-recompute by design).
