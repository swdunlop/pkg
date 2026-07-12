# Feature: Web workbench (`datalog serve`)

## Problem

The authoring loop — map JSONL into facts with jsonfacts, write rules,
query, refine — has one human surface today: the REPL. Line-oriented
interaction is wrong for the artifacts, which are documents (a YAML
config, a `.dl` program) and tables (raw records, extracted facts,
query results). A human correcting a schema needs to see a raw record
beside the mapping beside the facts it produced. None of that fits a
readline prompt.

The workbench is a local IDE for a single project: browse JSONL
datasets, edit the jsonfacts config against live extraction, author
rules with live error feedback, run queries, inspect the fact
database. It is also the human observation post for the trio
(mcp-server.md, acp-integration.md): when an agent does the
authoring, these same panes are how the human watches and corrects.

## Design constraints and observations

**1. The session is the shared core; the UI must be a view of it, not
a peer.** mcp-server.md factors a `session` type out of the REPL. The
trio — REPL, workbench, MCP tools, embedded agent — only coheres if
they all read and write that one in-process session. Two consequences.
First, the session must own the *canonical documents*, not just
compiled artifacts: `schemaText` and `rulesText` live on the session,
the editors render them, and the editor content **is** the document —
an agent rewriting the rules and a human typing in the textarea are
the same operation. Second, the UI gets no private mutation path:
"Apply" calls the same `set_schema`/`set_rules`/`query` handlers the
MCP tools expose. One pipeline, N frontends, zero drift.

**2. The batch posture makes reads snapshot-cheap.** `Transform`
returns a fresh `*memory.Database`; the server holds an atomic pointer
to the current snapshot. All reads (fact browsing, query results,
trace) run lock-free against the snapshot; mutations run serialized
through the session, producing a new snapshot and swapping the
pointer. No invalidation logic, matching the incremental-evaluation
deferral in TODO.md.

**3. Single-user, single-tenant, single-project.** One in-memory
database shared across all browser tabs; no sessions, no per-tab
isolation — the REPL's mental model with a rendered face. Between the
human and their embedded agent, concurrent edits resolve as
last-writer-wins with SSE patch-back and a visible "updated by agent"
marker: they are one human and their delegate, and the human knows not
to fix rules while the agent is working. Conflict management can layer
in later if needed.

**4. Hypermedia keeps state where the engine lives.** Go stdlib
(`net/http`) plus Datastar for SSE-driven DOM patching and debounced
inputs, Oat CSS for zero-JS styling, Mermaid.js for client-rendered
proof trees. No Node build step, so the binary stays self-contained and
Nix packaging stays trivial. HTML is built with
[`html-go`](https://github.com/swdunlop/html-go) rather than
`html/template`: the workbench is exactly the "sustained project,
Datastar in production, reusable UI primitives across domain types"
case doc/notes/datastar.md §10 calls out for it — four panes share
table/pagination/error-list shapes, and compile-time checking on
`session` field access matters more here than designer-editable
markup. `html-go/datastar` builds SSE events (`Elements`, `Signal`,
`Batch`) directly from `html.Content`, so no fragment ever passes
through `fmt.Sprintf`. See doc/notes/datastar.md for the Go+Datastar
idioms this feature follows throughout, and its Appendix A for the
html-go cheat sheet.

**5. Keystrokes buy errors; Transforms are explicit.** The main value
of live feedback while typing rules is the error list, and parse +
compile (including stratification, now computed at compile time) costs
microseconds with no timeout machinery at all. Full Transforms are
reserved for explicit actions — Run, Apply, and `query` calls — under
a 5s timeout; the fixpoint loop checks `ctx.Err()` per iteration and
between strata, so even those are cleanly cancellable. This keeps
doomed keystroke Transforms from contending on the serialized pipeline
with agent tool calls. (The jsonfacts editor's live single-row
extraction stays keystroke-live: extracting one record is cheap.)

**Design decision: a hypermedia workbench over the shared session —
every pane an SSE-patched view of session state, every mutation
flowing through the same handlers the MCP tools use, and disk writes
only on explicit Save.**

## Proposed solution

### Project directory

`datalog serve` takes the same shape as the REPL and MCP subcommands:

```
datalog serve -d ./data [-c schema.yaml] [rules.dl ...] [--listen 127.0.0.1:8080]
```

The project directory holds the jsonfacts config (convention:
`jsonfacts.yaml`), rules (convention: `rules.dl`), and the JSONL/zip
data files referenced by `sources[].file`, relative paths only. The
server loads config, rules, and data at startup. Disk-change detection
is deliberately omitted; a "Reload from Disk" button covers the git-
pull case.

### Session state and persistence

The session (per mcp-server.md, extended with `schemaText` and
`rulesText`) is the single source of truth. Editors are views: typing
updates the document server-side via a debounced `data-on:keydown__debounce.500ms="@post(...)"`
action (doc/notes/datastar.md §2, §4) that decodes signals with
`datastar.Decode` and re-renders just the error-list fragment;
agent-side `set_schema`/`set_rules` calls update the same fields and
patch the editors over the Fact Browser's subscription SSE connection
(doc/notes/datastar.md §8) — one write path, two triggers. Per §6's
signal-hygiene rule, the editors bind `data-bind:schema-text` /
`data-bind:rules-text` to the textarea because the content genuinely
needs to be observed keystroke-by-keystroke; every other pane sticks to
`value`-attribute rendering and re-render-on-change, since the server
owns that state.

Nothing touches disk until the user clicks **Save**, which writes the
file and, if the project directory is a git repo, runs `git add` +
`git commit -m "ui: save <filename>"`. One click, one commit;
squashing and rewording stay in the terminal. The UI performs no other
git operations. The embedded agent has no disk path at all — the Save
button is the persistence step for agent-authored documents too
(acp-integration.md).

### The panes

Four workspace panes plus a console drawer shared by both views (the
drawer's Agent tab is defined in acp-integration.md), split across two
three-column views rather than one page — four disjoint panes on a
single screen proved unworkable, and each pair naturally shares a Fact
Browser as its "what did that just produce" column:

- **Facts view** (`/facts`): Data Browser | jsonfacts Editor | Fact Browser
  (base facts only) — authoring how base facts are extracted from JSONL.
- **Rules view** (`/rules`): Fact Browser (base facts only) | Datalog
  Editor | Fact Browser (derived facts only) — authoring how rules derive
  facts from base facts, base on the left as the rules' input, derived on
  the right as their output.

A nav switcher in the header (`/facts`, `/rules`) moves between them via
plain full-page navigation (doc/notes/datastar.md §7) — a real URL change,
bookmarkable and reloadable, not a Datastar action. `GET /` redirects to
`/facts`, the authoring loop's usual starting point. The Fact Browser's
subscription connection (below) moved from pane-scoped to page-scoped so
the Rules view's two panes share one connection instead of opening two.

**Data Browser.** Raw JSONL records in a semantic table, one file at a
time (file list from `sources[].file`). Server-side pagination, 50
rows per chunk; "Load More" is a plain `data-on:click="@get('/data/{{file}}?offset=N')"`
action SSE call (doc/notes/datastar.md §1–§2) whose handler
`MergeFragments`-appends the next chunk with `Mode("append")` rather
than replacing the table body — files re-read per request, zip members
decompressed to a temp file on first access. Each row has a "Test"
button (`data-on:click="@get('/jsonfacts/test/{{row}}')"`) that selects
it as the jsonfacts editor's evaluation target and patches the
jsonfacts Editor's row pane and live-output pane in the same response.

**jsonfacts Editor.** Three-pane grid: the selected source row
pretty-printed; the config as a raw YAML textarea (no structured
forms — the config is a serializable struct and raw YAML is the
simplest faithful mapping); live output. Input debounced at 500ms via
`data-on:keydown__debounce.500ms="@post('/jsonfacts/preview')"`; each
event parses the YAML, compiles the expr mappings, and extracts from
the **single selected row only** — fast feedback against a
representative sample, cheap enough to skip the long-running-action
machinery below. **Apply** follows the streaming-progress shape from
doc/notes/datastar.md §9: `data-indicator:_applying` +
`data-attr:disabled="$_applying"` on the button, `@post('/jsonfacts/apply')`
re-extracts everything and runs a full Transform via the `set_schema`
handler (in-memory only), gated through the sandbox's per-resource
`Jobs.Begin` so a second Apply click while one is in flight is a no-op
rather than a race. Errors render as a list below the editor with
`line:col` prefixes, verbatim from the parsers — an in-form error
surface per doc/notes/datastar.md §4, not a toast, since the fix is
the user's to make.

**Datalog Editor.** One textarea using the REPL's `.`/`?` terminator
convention, so `.dl` files paste directly. Debounced 500ms
(`data-on:keydown__debounce.500ms="@post('/rules/check')"`); each event
runs parse + compile only (observation 5), refreshing the error list —
`line:col` prefixes, verbatim from the parser/compiler — with a
cursor-position indicator to aid navigation; no inline highlighting,
no rich editor. A "Run" button (`data-indicator:_running`,
`data-attr:disabled="$_running"`) applies the document via `set_rules`
and executes its queries through the `query` handler under the 5s
timeout, streaming a `#status` fragment per doc/notes/datastar.md §9 so
the button doesn't just freeze; a timeout reports "evaluation timed
out, results may be incomplete" in that same `#status` div. The
sandbox's Global Cancel button (below) targets the same job key.

**Fact Browser.** Predicates with fact counts (the REPL's `.list`), each
labeled **base** (EDB) or **derived** (IDB) from the ruleset — but each
Fact Browser *instance* only lists one kind, since the Facts and Rules
views each show base and/or derived in separate columns rather than one
mixed list (`view.FactBrowser(kind, heading)`, `kind` = `"base"` or
`"derived"`, rendering into `#predicates-base` / `#predicates-derived`).
Expanding a predicate pages its facts 50 at a time via the
`iter.Seq[[]Constant]` from `Database.Facts`. Composite terms render
as a one-level `<details>`: the summary shows ~80 chars of canonical
JSON, expansion shows the full `json.MarshalIndent` output. The page
shell (not this pane) owns the page's one long-lived subscription
connection (doc/notes/datastar.md §8): a `<div data-init="@get('/events', {openWhenHidden: true, requestCancellation: 'disabled'})">`
opens on page load, the handler subscribes to the session's
Transform-completed bus *before* sending the initial base+derived fact
listings as one `Batch` (the subscribe-before-render ordering in §8, to
avoid losing a Transform that lands mid-render), then forwards each
subsequent completion — including ones triggered by agent tool calls —
as a `Batch` of both fragments; Datastar morphs whichever id the current
view actually has on screen and no-ops on the other.

**Console drawer.** A full-width strip beneath the three columns,
present on both views and collapsed by default to a one-line status
bar (so the no-agent, no-query-yet case costs no screen). Two tabs:
**Query** — an ad-hoc query input running through the same `query`
handler as Run (same 5s timeout, same sandbox, same `Jobs` key
discipline), with a scrollback of results — the REPL's `?` line with a
rendered face, so one-off probes stop requiring edits to the rules
document; and **Agent** — the chat pane defined in acp-integration.md.
Both tabs' scrollbacks are session state like `schemaText`: the server
owns the transcript, the drawer re-renders it on page load, and new
entries patch in over SSE (the same page-scoped subscription pattern
as the Fact Browser). Switching views mid-turn or mid-query therefore
loses nothing — the drawer on the next page picks the stream back up.
A **Clear** button on the bar wipes the visible tab's scrollback on
every open page; clearing the Agent tab also drops the embedded
driver, so it is a conversation reset (the model forgets too). A turn
still running is cancelled first — Clear means "start over", so it
implies Stop rather than demanding it as a separate click.
Agent tool calls render for the operator, not the wire, and the
drawer's vertical real estate is the constraint: each call is one
collapsed `<details>` whose summary line carries the tool name, its
elided arguments (a `query` call shows the query text itself, not its
JSON envelope), and a status that turns red on failure — one line per
call until the operator opens it. The body holds the rest: query
results as the editor's variable-named table, errors as its error
list, other results as pretty-printed JSON, and — whenever the
summary line truncated the arguments — the full document, so elision
never loses anything.
A drawer rather than a fourth column because the three-column rhythm
was hard-won; the ultrawide case is an open question below.

### Execution sandbox

Malformed rules can loop or explode combinatorially; handlers must
assume it.

- **Goroutine isolation**: evaluation runs in a spawned goroutine; the
  handler stays free to respond with errors or partial output.
- **Context timeouts**: 5s on Run, Apply, and agent `query` calls,
  honored per fixpoint iteration. Keystroke evaluation is parse/
  compile only and needs no timeout.
- **Panic recovery**: `defer recover()` in the evaluation goroutine,
  translated to a formatted error patch.
- **Combinatorial caps**: hard per-evaluation fact limit (1,000);
  hitting it halts and tells the user the rule is too broad.
- **Stale suppression**: a newer evaluation supersedes an in-flight
  one; only the result for the latest editor content is patched.
- **Global Cancel**: the server keeps a set of `context.CancelFunc`s,
  one per running operation, structured as the `Jobs` map from
  doc/notes/datastar.md §9 (`(userID, resourceID, jobName)` keys
  collapse here to just `jobName`, since it's single-user); Cancel is
  `@post('/cancel')` and fires them all — the emergency brake, not
  surgical — and also cancels any active agent turn
  (acp-integration.md). Single-user makes the blunt instrument
  acceptable.

### Deployment

Self-contained Go binary, no frontend build: packaged as a Nix flake
output. Serves on a loopback port; remote access via Tailscale rather
than public exposure. The MCP tool surface is mounted at `/mcp`
(streamable HTTP, bearer token) for external ACP agents and MCP
clients; the embedded kit agent reaches the same server in-process —
see mcp-server.md and acp-integration.md.

## Work

1. **`serve` subcommand** (`cmd/datalog/serve.go`): flags, startup
   loading, the third arm of the subcommand switch.
2. **`view` package scaffolding**: per the html-go bootstrapping
   checklist (doc/notes/datastar.md Appendix A.12) —
   `view/tags.go` for cached prototypes (form fields, action buttons,
   error lists shared across panes), `view/page.go` for the page shell
   struct implementing `AppendHTML`, one `view/<pane>.go` per pane
   below pairing with a `handler/<pane>.go`.
3. **Session document fields**: add `schemaText`/`rulesText` to the
   session (mcp-server.md work item 1 grows two fields); handlers
   update them.
4. **SSE hub + layout**: `html-go/datastar` `Stream`/`Elements`
   plumbing (doc/notes/datastar.md Appendix A.11), Oat CSS shell, pane
   scaffolding.
5. **Data Browser**: pagination, zip temp-file handling, row
   selection.
6. **jsonfacts Editor**: debounced single-row extraction endpoint;
   Apply via the `set_schema` handler, gated by `Jobs.Begin`.
7. **Datalog Editor**: debounced parse/compile endpoint for the error
   list; Run via the `set_rules` + `query` handlers, streaming
   `#status` progress.
8. **Fact Browser**: predicate listing with EDB/IDB labels, fact
   paging, composite rendering, subscription SSE endpoint
   (subscribe-before-render).
9. **Save/git**: write + add + commit; skip git outside a repo.
10. **Sandbox plumbing**: `Jobs` cancel-set, panic recovery, stale
    suppression, fact cap.
11. **Tests**: handler tests against `examples/mordor`; timeout,
    cancel, and stale-suppression tests.
12. **Console drawer**: the collapsible shell shared by both views,
    session-owned scrollback, and the Query tab (input → `query`
    handler → appended result fragment). The Agent tab is
    acp-integration.md's work, not this feature's.

## Risks / open questions

- **Pipeline contention.** Run, Apply, and agent `query` calls share
  the serialized mutation pipeline, so a human's Run queues behind an
  agent's queries and vice versa. Bounded by the 5s timeout and much
  reduced by keeping keystrokes off the pipeline (observation 5); the
  memoization option in mcp-server.md is the shared relief valve if
  it still drags.
- **JSONL pagination is O(offset).** Skipping to page N re-reads N×50
  lines. Fine at the anticipated 10k-row scale; a line-offset index
  per file is a small later fix.
- **Git autocommit noise.** One commit per Save is deliberate, but a
  heavy session produces dozens of `ui: save rules.dl` commits. The
  user squashes in the terminal; revisit only if it annoys in
  practice.
- **Snapshot memory.** Pointer-swap semantics briefly hold two full
  databases during a mutation. Inherent to the snapshot model;
  acceptable at target scale.
- **Ultrawide users may want the console as a fourth column.** The
  drawer trades vertical space to preserve the three-column layout; on
  an ultrawide monitor a right-rail column may be strictly better. If
  asked for, a layout toggle (drawer vs. column) is chrome-only — the
  console's content, session state, and SSE wiring are unchanged —
  but it is deliberately not built until someone asks.

## Out of scope

- The chat pane and everything agent-facing — defined in
  acp-integration.md.
- Provenance tracing (replaying a derived fact's proof tree, rendered
  via Mermaid). Severable, and the largest bespoke engineering item in
  the pane set; deferred to its own feature doc.
- Multi-user, authentication, non-loopback exposure beyond Tailscale.
- File watching (`fsnotify`) and disk-change warnings.
- Structured (form-based) jsonfacts editing.
- Search/filter in the Data Browser beyond pagination.
- Surgical per-operation cancellation.
- Git operations beyond add+commit on Save.
