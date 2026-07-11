# Feature: MCP server (`datalog mcp`)

## Problem

The target application transforms an evidence snapshot into higher-level
observations worth LLM attention. Today the LLM sits only at the *end*
of that pipeline: a human authors the jsonfacts schema and the Datalog
rules in the REPL, runs the transform, and ships the output JSONL to a
model. But schema and rule authoring is itself the kind of iterative,
feedback-driven work models are good at — look at raw data, propose
mappings, check what matched, write rules, inspect results, refine.

The REPL is exactly that loop, built for humans. There is no equivalent
surface for a model. An MCP server (`datalog mcp`) gives a model the
same session: build a jsonfacts configuration, load data, define rules,
and query the resulting observations — with the engine's diagnostics
(parse errors with line:col, compile errors, iteration-cap warnings,
per-stratum profile stats) serving as the model's feedback signal.

## Design constraints and observations

**1. Models write documents better than they drive builders.** The
tempting tool surface is fine-grained CRUD: `add_source`,
`add_matcher`, `add_rule`, `delete_rule`. That multiplies tool calls,
forces the model to track server-side state it can't see, and invents
an editing protocol for documents (YAML configs, `.dl` programs) that
models already author fluently in one shot. The recent investment in
error quality — parser errors with line:col and the offending source
line, compile-time stratification failures — pays off precisely when
the model submits a whole document and revises against precise errors.

**2. The model must see raw data to write mappings.** A jsonfacts
mapping is a function of the input JSONL shape (`value.pid`,
`value.recipients`, ...). Without a way to sample raw lines from the
data directory, the model is guessing field names. Sampling raw input
is as essential as submitting the schema.

**3. Declarations are ready-made model documentation.** The
`declarations` section already carries `use` strings for predicates and
terms, designed for structured output. Surfacing them through predicate
listing turns existing schema metadata into tool-facing docs for free.

**4. The config is now untrusted input.** In the REPL, the operator
writes the config; in MCP, the model does. The config references files
(`sources[].file`, `*_from` pattern files) and embeds expr-lang
programs that execute server-side. File references must be confined to
an operator-chosen data directory, and the expr environment must stay
limited to the loader's own functions (`fresh_id`, `assert`,
`match_*`) — no filesystem or process access.

**5. The batch posture makes state management trivial.** Per the
incremental-evaluation deferral (TODO.md), the output is a pure
function of (config, data, rules). An MCP session therefore holds only
those three inputs plus the loaded base database; every query
recompiles and re-Transforms from the snapshot, exactly as `execQuery`
does today. No invalidation, no retraction, no cross-call caching to
get wrong.

**6. The REPL already contains the engine-facing half.** `repl.go`
mixes two concerns: session state + evaluation (`facts`, `rules`,
`aggRules`, `loadProgram`, `execQuery`'s synthetic `_q_` rule,
`loadData`, `buildDB`) and terminal interaction (readline, tab
completion, history, printing). The first half is what MCP needs;
duplicating it guarantees drift.

**Design decision: a small, coarse, document-oriented tool surface
over a `session` type factored out of the REPL.** Whole-document
submission with high-quality errors as the feedback loop; one session
per server process; full recompute per query; filesystem scope fixed
by operator flags at startup, never by the model.

## Proposed solution

### Extract `session` from `repl`

New file `cmd/datalog/session.go` (staying in `package main` — nothing
else needs it yet):

```go
// session holds one evaluation context: a data source, loaded base
// facts, and accumulated rules. It is the engine-facing core shared by
// the REPL and the MCP server; it never touches a terminal.
type session struct {
    facts    []datalog.Fact
    rules    []syntax.Rule
    aggRules []syntax.AggregateRule

    engineOpts []seminaive.Option

    configPath string
    dataDir    string
    cfg        jsonfacts.Config
    dataDB     *memory.Database

    // Canonical document texts. The session owns the documents, not
    // just their compiled artifacts: the web workbench (web-ui.md)
    // renders and patches these, and set_schema/set_rules update
    // them, so a human typing in an editor and an agent submitting a
    // document are the same operation.
    schemaText string
    rulesText  string
}
```

Moves from `repl` to `session` (mechanical, behavior-preserving):
`loadProgram` (minus per-query printing), `setDataSource`, `loadData`,
`buildDB`, `newEngine`, plus a new `runQuery(ctx, q) ([][]datalog.
Constant, []string, error)` extracted from `execQuery` — everything up
to and including collecting `_q_` rows, returning rows + variable
names and leaving sorting/printing to the caller. `repl` embeds
`*session` and keeps readline, tab completion, history, `.commands`,
and all formatting. `runQuery` takes a `context.Context` (the REPL
passes `context.Background()`; MCP passes a per-call timeout context —
the fixpoint loop already honors cancellation).

The one behavioral change: `session` methods return errors and data,
never write to an `io.Writer`. Profile stats are returned as
`[]seminaive.StratumStats` from `runQuery` so the REPL can print them
and MCP can serialize them.

### The MCP subcommand

`datalog mcp` runs an MCP server on stdio using
`github.com/mark3labs/mcp-go` (chosen over the official SDK because it
is the mature community implementation, supports stdio, streamable
HTTP, and in-process transports from one server value, and is the
framework underlying `mark3labs/kit`, the embeddable agent in
acp-integration.md). Startup mirrors the REPL's flags:

```
datalog mcp -d ./data [-c schema.yaml] [rules.dl ...]
```

- `-d` pins the data directory. **Required** for `mcp` (unlike the
  REPL): it is the security boundary. All file references — schema
  sources, `*_from` pattern files, `sample_input` arguments — resolve
  relative to it and must not escape it (reject absolute paths and
  any path whose `filepath.Clean` result starts with `..`; resolve
  symlinks before the containment check).
- `-c` optionally preloads an operator-provided schema; the model may
  replace it via `set_schema`.
- Positional `.dl` files preload rules (becoming the initial
  `rulesText`), same as the REPL.

Since the current CLI has no subcommands (flags + positional files),
this introduces one: bare invocation keeps today's REPL behavior;
`datalog mcp ...` dispatches to the server. `main.go` grows a
two-armed switch, not a framework.

Standalone stdio is one of three mountings of the same tool handlers.
The web workbench (`datalog serve`, web-ui.md) mounts them over
streamable HTTP at `/mcp` behind a bearer token, sharing its live
session with external agents; its embedded agent (acp-integration.md)
connects via mcp-go's in-process transport. A `datalog mcp --proxy
<url>` mode bridges stdio to a serve instance's `/mcp` for MCP clients
that only speak stdio.

### Tool surface

Six tools. All results are structured JSON (the consumer is a model,
not a terminal): counts, truncation markers, and errors with line:col
positions verbatim from the parser/compiler.

**`set_schema`** — replace the session's jsonfacts configuration.
Input: `{schema: string, format?: "yaml"|"json"}`. Parses the config,
validates file references against the data dir, loads the data, and
returns per-predicate fact counts:

```json
{"predicates": [{"name": "process", "arity": 4, "facts": 15234}],
 "warnings": ["source connections.jsonl: 12 lines skipped by filter"]}
```

The fact counts are the model's primary feedback that its mappings
actually matched anything — a schema that parses but produces zero
`process` facts is a wrong schema, and counts say so immediately.

**`set_rules`** — replace the session's Datalog program. Input:
`{source: string}`. The source is the whole document — the same text
the workbench's rules editor holds (`rulesText`); submitting replaces
it, mirroring `set_schema`. Returns the parse error (with line:col
and source line) or the list of head predicates defined. Queries
(`?` statements) embedded in the source are rejected here — use
`query`. Compilation happens per-query, so compile-time errors
(unstratifiable negation, arity conflicts) surface from `query`;
`set_rules` additionally runs a trial `Compile` so those errors
attach to the submission that caused them rather than to a later
innocent query. There is no append and no deletion: whole-document
replacement is the editing model (constraint 1), for humans and
models alike.

**`query`** — evaluate one Datalog query against the current session.
Input: `{query: string, limit?: int}` (default limit 100, hard cap
1000). Runs `session.runQuery` under a timeout (flag-configurable,
default 60s). Returns:

```json
{"vars": ["Host", "Pid", "Cmd"],
 "rows": [["ws01", 1234, "certutil -urlcache ..."]],
 "total": 2, "truncated": false,
 "stats": [{"predicates": ["ancestor/3"], "facts": 8123,
            "iterations": 14, "duration_ms": 92, "capped": false}]}
```

`stats` carries the `StratumStats` the REPL's `.profile` shows,
always — "stratum hit the iteration cap" and per-stratum timings are
exactly the diagnostics a model needs to repair a runaway recursive
rule, and they cost nothing to include.

**`list_predicates`** — names, arities, fact counts, and declaration
`use` strings for predicates and terms, covering both loaded and
rule-defined predicates (the REPL's `.list` plus declaration docs).

**`sample_facts`** — up to N facts for one predicate. Input:
`{predicate: string, arity: int, limit?: int}`. The REPL's `.facts`
with a JSON result.

**`sample_input`** — raw lines from a JSONL file in the data dir.
Input: `{file: string, limit?: int, offset?: int}` (default limit 10;
truncate individual lines beyond ~4 KiB with a marker). Also lists
available files when called with no `file`. This is what makes schema
authoring possible: peek at raw records → write mappings → check
counts.

The intended loop, which tool descriptions should spell out:
`sample_input` → `set_schema` (iterate on counts) → `set_rules`
(iterate on errors) → `query` (iterate on results/stats).

### What is deliberately absent

- **No filesystem-writing tools.** The session is ephemeral; in
  standalone stdio use the operator persists artifacts by asking
  their agent for the final schema/rules text. In the workbench trio,
  the Save button is the persistence path (web-ui.md) — the human
  reviews what the agent authored and commits it.
- **No `load_data`/path arguments beyond the data dir.** Data location
  is the operator's decision at startup.
- **No incremental fact assertion tool.** `Extend` + re-Transform is
  already the interactive what-if mechanism; if a use case appears, an
  `assert_facts` tool slots in later without disturbing the design.
- **No MCP resources/prompts.** Tools only, until a client-driven need
  shows up. (A `resource` exposing the current schema and rules text
  is the obvious first candidate.)

## Work

1. **Extract `session`** (`cmd/datalog/session.go`): move
   `loadProgram`, `setDataSource`, `loadData`, `buildDB`, `newEngine`;
   extract `runQuery` from `execQuery`; thread `context.Context`
   through. `repl` embeds `*session`. Pure refactor — REPL behavior
   unchanged; verify by running the existing pipe-mode examples.
2. **Path confinement helper** in jsonfacts or the cmd:
   `confine(dataDir, ref string) (string, error)` used for schema
   source files, `*_from` pattern files, and `sample_input`. Includes
   symlink resolution. Unit-test the escapes (absolute, `..`,
   symlink-out).
3. **Audit the expr environment** (`jsonfacts/config.go` /
   `loader.go`): confirm the compiled expr programs expose only the
   loader's function set and the `value` input; pin with a test that a
   config using e.g. `os`-ish identifiers fails to compile.
4. **`datalog mcp` subcommand** (`cmd/datalog/mcp.go`): one
   `*server.MCPServer` (mark3labs/mcp-go) holding the seven tools,
   served over stdio here and reused by serve's HTTP and in-process
   mountings; per-call timeout flag; result types as plain structs
   with `json` tags.
5. **Tool descriptions**: write them as the model-facing manual —
   the workflow loop, the Datalog syntax summary (condensed from
   README), and the matcher/modifier tables. This text is load-bearing;
   review it like API docs.
6. **Tests**: session-level tests for each tool handler (they're just
   methods once the SDK plumbing is separated); a golden test driving
   the full loop (sample → schema → rules → query) against
   `examples/mordor` data; truncation and timeout behavior.
7. **README**: new section alongside "CLI REPL" documenting `datalog
   mcp`, its flags, and an example client configuration.

## Risks / open questions

- **Result-set blowups.** A careless `query` over a large join can
  produce millions of rows before the limit truncates output — the
  limit caps serialization, not evaluation. The Transform timeout is
  the real guard; the limit is presentation. Acceptable for a
  local, operator-launched tool; revisit if sessions move server-side.
- **Transform cost per query.** Full recompute per query is the REPL's
  existing behavior and the batch posture's explicit choice, but a
  model may issue queries far faster than a human. If mordor-scale
  data makes this noticeable, memoize the (ruleset-hash → transformer,
  db-generation → output) pair inside `session` — an optimization both
  frontends inherit. Don't build it until it's felt.
- **Concurrent tool calls.** MCP clients may issue calls concurrently;
  `session` is single-threaded by design. Serialize with one mutex
  around every handler — queries are seconds-scale, and correctness
  beats parallelism here.
- **Schema warnings surface.** The loader currently has no
  warning channel (filtered lines, unmatched sources are silent).
  The `warnings` field starts as best-effort; extending the loader to
  report per-source line/emit counts is a small, separately mergeable
  improvement that benefits the REPL's `.reload` too.
- **Framework dependency.** mcp-go is the established community
  implementation and doubles as kit's substrate, but it is still a
  third-party dependency tracking a moving protocol; pin the version.
  Fallback if it disappoints: the protocol's stdio framing is small
  enough to hand-roll, but don't start there.

## Out of scope

- Multi-session servers and authentication beyond serve's loopback
  bearer token. Standalone `datalog mcp` stays stdio, one session per
  process, launched by the operator's agent runtime; the HTTP and
  in-process mountings exist only inside `datalog serve` (web-ui.md,
  acp-integration.md).
- Fine-grained schema/rule editing tools (see constraint 1).
- Writing schemas/rules to disk from the server.
- Incremental evaluation (still deferred per TODO.md).
- Exposing `Extend`-style fact assertion (noted above as a clean
  later addition).
