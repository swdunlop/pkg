# Feature: Workbench scale — real datasets, not toy sets

Amends the **Execution sandbox** section of web-ui.md. That section's
mechanisms (goroutine isolation, panic recovery, stale suppression,
Global Cancel, per-iteration `ctx.Err()` checks) stay in force; what
changes is the two numeric limits it fixes as constants, plus the
startup and paging assumptions that only held for `examples/mordor`.

## Problem

The workbench exists to develop rules against a dataset. But its
evaluation sandbox was sized for `examples/mordor` (506 events) and
hard-codes two constants in `cmd/datalog/sandbox.go` that make any real
dataset unusable:

- `factCap = 1000` — evaluation halts with "rule too broad" the moment
  a Transform derives more than 1,000 output facts.
- `evalTimeout = 5 * time.Second` — Run, Apply, agent `query`, and the
  Fact Browser's derived-query all run under a 5-second context.

On the OpTC starter slice (`examples/optc`, one host bucket, one day)
a *correct* ruleset derives ~300k facts and its base load is ~8.7 GB /
~6 min. Both limits trip immediately, and they are coupled: raising
one alone is useless, because a legitimate Run on real data blows past
both at once. The net effect is that the one workload the workbench was
built for — iterate on rules while watching what they derive — is
exactly the workload it refuses to run.

Two further assumptions were fine at 506 events and are not at 300k:
startup loads the whole dataset synchronously before the listener
opens, and the Fact/Data browsers page in O(offset).

## Design decisions

### 1. The two caps become operator-set budgets, not demo guards

Both limits stay — a runaway cross-product must still be stoppable —
but they stop being compile-time constants tuned for a toy and become
`serve` flags with defaults matched to a real machine, not a demo.

- **`--max-facts` (was `factCap = 1000`).** Default raised to a
  memory-shaped ceiling (proposed **10,000,000**), settable per run,
  `0` meaning "no cap, rely on Stop + OOM". This stays a genuine
  runaway backstop: `seminaive.WithFactLimit` still halts *during*
  Transform the moment the derived count crosses it, so a Cartesian
  explosion is caught before it materializes — the mechanism the
  current const already documents. Only the number and its source
  change.

  The user-facing message changes with it. "rule too broad: output
  exceeds 1000 facts" reads as *you wrote a bad rule*; at 10M it reads
  as *you hit the resource ceiling* — reword to name the limit and how
  to raise it (`--max-facts`), so a developer legitimately deriving
  millions isn't told their correct rule is malformed.

- **`--eval-timeout` (was `evalTimeout = 5s`).** Default raised to a
  ceiling that admits real evaluation (proposed **5m**), settable,
  `0` meaning "no deadline — Stop is the only brake". 5s was a
  keep-the-UI-snappy guillotine; it predates the BusyActionButton/Stop
  idiom. Now that every long action rides `$busy` and morphs into a
  Stop that posts `/cancel` (Global Cancel), *interactive* interruption
  is already handled by the operator, and `ctx.Err()` per fixpoint
  iteration already makes Transform cancellable. The timeout's job
  shrinks to a backstop against a genuinely wedged evaluation, so its
  default belongs in minutes, not seconds.

Keystroke evaluation is untouched: it is parse/compile only, carries no
timeout and no cap, and must stay that way — the editors have to feel
instant regardless of dataset size.

### 2. Fix at the one mechanism, not the five call sites

`--max-facts` is already single-sourced: it is wired once as
`session.engineOpts = []seminaive.Option{seminaive.WithFactLimit(...)}`
in `newMCPHandlers` (`mcp.go`), and every Compile path reads
`engineOpts`. Threading a flag to that one line covers the whole
surface. **Reject** any change that passes a limit into an individual
Compile call.

`--eval-timeout` is *not* single-sourced today: the `evalTimeout` const
is referenced directly at five sites (`watch.go`, `commands_composer.go`,
`fact_browser.go`, `serve.go`, and via the `newMCPHandlers` param). The
fix is to resolve the value **once** at startup and store it on the
handlers/session (as `newMCPHandlers` already half-does by taking it as
a param), then have every `context.WithTimeout(...)` call read that
field. **Reject** a patch that leaves the package const in place and
only overrides it at the site that surfaced the problem — the next call
site would silently keep the 5s guillotine. One resolved value, every
site reads it.

### 3. Startup must not block the listener on a 6-minute load

`datalog serve` currently runs the full `LoadFS` inside `newWorkbench`
→ `newMCPHandlers` → `setSchema` before `runServe` starts listening, so
the operator stares at a dead terminal for ~6 min with no way to tell
load from hang. Bring the HTTP listener up first and load the dataset
in a background job that publishes progress on the existing bus (the
same `#status` channel Run uses), with the browser panes showing a
"loading N records…" state until the base DB is ready. This reuses the
job/stale-suppression plumbing already in the sandbox; it is not new
machinery, just a reordering so the workbench is reachable while it
loads. (Deferrable if we accept a blocking start with at least a
stderr progress line — but the point of the feature is real datasets,
and real datasets take minutes.)

### 4. Provenance default-on needs a scale escape hatch

`cmd/datalog` sessions enable provenance by default (per
provenance.md), costing ~1 map entry + a `[]uint64` per derived fact.
That was justified "for a workload within `WithFactLimit`" — i.e.
within 1,000. Once `--max-facts` is 10M, default-on provenance is an
unbounded memory multiplier on top of an already-8.7 GB base. Add
`--provenance=off` (and consider auto-disabling above a derived-fact
threshold, announced in the console) so an operator developing rules
on a large slice can trade the "why?" drawer for headroom. The library
default (off) is unchanged.

### 5. Browser paging is O(offset) — name it, bound it

The Fact Browser computes `total` and locates each 50-row window by
scanning the whole predicate from the top on every Load More
(`fact_browser.go`), and the Data Browser has the same O(offset) JSONL
walk (web-ui.md already flags this). At 300k facts the first page is
fine and page 500 is a full re-scan. `total` is the cheap half: swap
the full-scan count for the O(1) `PredicateCounts()` already used by
the counts fan-out. The window walk is the harder half and can stay
O(offset) initially (Load More is sequential, so amortized it is one
scan), but it should be measured, not assumed — and jit-column-indexing
is the lever if it drags. This is a follow-on, not a blocker for
raising the caps.

## Work

1. **`sandbox.go` + `serve.go` flags:** replace the two consts with
   `--max-facts` (default 10_000_000, 0 = off) and `--eval-timeout`
   (default 5m, 0 = off); parse in `runServe`, thread to
   `newWorkbench`/`newMCPHandlers`.
2. **Single-source the timeout:** store the resolved deadline on the
   handlers/session; convert all five `context.WithTimeout(evalTimeout)`
   sites to read it. Delete the package const.
3. **Reword the cap error:** "evaluation exceeded the --max-facts
   limit (N); raise it with --max-facts or stop the run" in place of
   "rule too broad".
4. **Background startup load:** listener up first, `LoadFS` in a bus-
   reporting job, panes render a loading state until ready.
5. **`--provenance` flag** (+ optional auto-off threshold with a
   console notice).
6. **Fact Browser `total` via `PredicateCounts()`**; measure the
   window walk at 300k before deciding whether the O(offset) window
   needs the index.
7. **Tests:** a large-derivation ruleset completes above the old 1,000
   cap and below the new one; a run past `--eval-timeout` reports the
   timeout wording, not the cap wording; Stop cancels a long run mid-
   fixpoint; the reworded cap error fires at the configured limit.
   Keep the mordor handler tests green (defaults must not regress the
   small-dataset behavior they assert).

## Risks / open questions

- **Pipeline contention gets teeth.** Run, Apply, and agent `query`
  share the serialized mutation pipeline (web-ui.md risk). At a 5s
  ceiling a blocked queue self-clears; at 5m a single Run can wall off
  Apply and the agent for minutes. The `$busy` mutex already serializes
  and the Stop button already exists, so the operator *can* clear it —
  but the agent-vs-human contention is now a real wait, and the
  memoization valve from mcp-server.md may need to land alongside this
  rather than after it.
- **Memory is the true ceiling, and it is unbounded by these flags.**
  `--max-facts` caps derived output, not the base load; the 8.7 GB is
  mostly interned base facts. Raising the caps lets a developer ask for
  an evaluation their RAM can't hold, and the only backstops are Stop
  and the OS OOM killer. A memory-watermark guard (halt at X% RSS with
  a clear message) is the principled fix and is out of scope here;
  until then, document the RAM reality in the OpTC README so the
  expectation is set, not discovered.
- **Default numbers are proposals.** 10M / 5m are placeholders chosen
  to clear the OpTC slice with headroom, not measured optima. Pin them
  against one real evaluation before committing.
