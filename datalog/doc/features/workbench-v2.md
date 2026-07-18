# Feature: Workbench v2 — conversations left, browser right

Supersedes the pane/layout design in web-ui.md (the two three-column
views, the console drawer, the textarea editors, Save/git), the chat
pane sections of acp-integration.md, and — together with the
structured-CRUD surface below — constraint 1 of mcp-server.md
(`set_schema`/`set_rules` whole-document submission). The parts of
those docs that describe the session pipeline, sandbox, bus, driver
split, and MCP mount remain in force.

## Problem

The v1 workbench grew pane-by-pane and it shows. The agent pane is a
full-width drawer on the bottom, so a conversation — a tall, narrow
artifact — renders comically wide. The editors are one column of
three, comically narrow for documents. The fact browsers are clunky
and the JSONL browser is worse. And the textarea editors compete with
the operator's real editor: vim wins that fight every time, but the
server has no way to notice a vim save.

Meanwhile the actual workload has clarified: the workbench is where a
human *converses with an agent* about the rules and facts, and
*browses* what the system knows. Authoring text is vim's job.

## Design decisions

Settled in the 2026-07-18 design session; the 2026-07-12 structured-
editing decisions are incorporated below with three amendments (disk
canonical, undo dropped, per-conversation modes replace the UI mode
toggle).

**1. Keep the core, rebuild the view layer.** The session/MCP pipeline
("one pipeline, N frontends"), sandbox (timeouts, fact cap, panic
recovery, Global Cancel), jobs, and SSE bus survive. The `view`
package, page layout, console drawer, and pane handlers are rewritten.
The `$busy` mutex, BusyActionButton Stop-morph, one-spinner rule, and
`evalHaltStatus` cancel-vs-timeout classification carry over as
conventions.

**2. Layout: conversations left, browser right.** The left half is
agent conversations: a narrow rail (New Conversation with an inline
mode picker; conversations newest-first, each with a mode badge and a
title auto-derived from the first user message; delete with confirm)
beside the active conversation's transcript and composer. The right
half is a read-only browser with four tabs: **Data | Schema | Rules |
Facts**, each master-detail. Transcript content cross-links into the
browser: a predicate in a query result, a rule group in an approved
edit, a fact in an answer are links that navigate the right half —
the conversation is the index into the browser.

**3. No textareas, ever. Disk is canonical; fsnotify makes it
reactive.** The operator authors rules and schema in vim, and vim
stays a first-class authoring path beside the agent. The server
watches the project files (the schema and the `rules/` directory,
see decision 4) with fsnotify: a save triggers reload →
validate → **full automatic re-evaluation** (the existing 5s-capped
Transform) → SSE repaint of every open view. Parse/compile errors land
in a persistent status surface and are visible to the next agent turn.
Approved agent writes go straight to the same files, so the operator
sees them in vim; the watcher treats self-writes and vim writes
identically (reload is idempotent; a debounce absorbs the echo). The
Save button and save-time git commits are gone — git is the human's
job in the terminal. This reverses web-ui.md's "no file watching"
out-of-scope item.

**4. Structured CRUD replaces whole-document tools.** Per the
2026-07-12 design: `set_schema`/`set_rules` are removed from the MCP
surface entirely. Writes are keyed CRUD — source=file,
declaration=name, matcher=(predicate, term, case_insensitive,
windash); rules grouped by head predicate/arity, `put_rule_group`
carrying `.dl` text that must all define that head. Every write is
full validate + reload, all-or-nothing, with fact-count feedback;
per-part revision counters reject stale writes with current content.
Reads: `get_config` whole, `list_rule_groups` + `get_rule_group`,
`predicate_deps` (both directions), `explain_fact` (post-hoc one-step
derivation, model recurses, always names an editable address,
query-style timeout). The per-layer undo stacks from 2026-07-12 are
dropped — consent gating prevents the bad write, git reverts the
regretted one.

**The monolithic ruleset file is retired.** Datalog is
order-independent, so a single file's ordering carries no semantics —
the monolith was a bad serialization of what the CRUD surface already
treats as a keyed collection. The canonical rule store is a **`rules/`
directory, one `.dl` file per rule group** (`<head>_<arity>.dl`): the
filesystem is the structure. Keys are filenames, `%%` docs and
comments live in the one file they describe (no free-floating
file-level comments can exist), revision tracking is per-file, an
agent write touches exactly one group's file, and vim, fsnotify, git
diffs, and consent-diff cards all work unchanged. Within a group's
file the agent's text lands verbatim — no cross-group serialization
order exists to normalize. Monolithic `.dl` files demote to
import/export interchange: the human-only Import (split into group
files, with confirm) and Export (concatenate) from the 2026-07-12
design, which also covers migrating existing rulesets and feeding the
REPL, whose `.dl` loading is unchanged. The jsonfacts schema stays a
single YAML file — it is already structured, and deterministic
serialization (matchers/declarations sorted by key) applies to agent
writes of it.

**5. Three conversation modes, chosen at start.** The mode picks the
agent: its system prompt and its tool registration. The 2026-07-12
"hard modal gating, human-held" survives as the mode choice itself —
the human picks the mode when starting the conversation, and the
agent's tool surface is scoped server-side, not by instruction.

- **Query Mode** — read-only: `query`, `list_predicates`,
  `sample_facts`, `explain`/`explain_fact`, `describe`,
  `predicate_deps`, `sample_input`. "Tell me why pgustavo is at risk
  in Mordor?" No write tools registered at all.
- **Rules Mode** — Query Mode's surface plus rule-group CRUD.
  **Adding** a new rule group applies immediately (additive, visible,
  revertable). **Editing or deleting** an existing group renders a
  diff card in the transcript with Approve/Deny (the existing
  pendingPermission machinery); Deny returns a "denied by user" tool
  result and the turn continues. No tool can replace the whole
  ruleset.
- **Facts Mode** — instructions oriented entirely around producing
  facts from JSONL; the finer points of the datalog implementation are
  deliberately absent. Tools: `sample_input`, expr evaluation against
  the selected record, schema CRUD (sources/matchers/declarations,
  same consent shape: adds free, edits/deletes gated), plus
  `list_predicates` and `sample_facts` to verify what its mappings
  produced across the corpus. No `query`/`explain` — it verifies
  extraction output without touching datalog semantics.

There is **no assert-fact tool** in any mode. The agent contributes
facts only by authoring extraction mappings; the output stays a pure
function of (config, data, rules) and every fact remains explainable
back to a source record.

**6. Conversations persist as kit sessions.** kit's tree-session
support does the heavy lifting: one JSONL session file per
conversation under `<project>/.datalog/sessions` (`SessionDir`;
gitignorable, travels with the project), `ListSessions` backs the
rail, `SetSessionName` holds the auto-title, and the conversation's
mode rides in session extension data so resume reconstructs the right
agent (SessionPath + mode-appropriate system prompt and tool
registration). Resume is full-fidelity: kit rebuilds the model context
from the session tree. **One turn at a time, globally** — sending in
any conversation while another turn runs is blocked, the composer
disabled with a "turn running in <conversation>" note; this matches
the single `$busy` mutex and the one-human posture.

**7. Kit primary, ACP follows.** Modes are designed for the embedded
kit driver. ACP conversations (--agent) get the same mode choice:
mode instructions ride the first prompt as preamble, and the `/mcp`
mount filters tools per-conversation server-side. ACP stays supported
but is the second-class citizen; its sessions are not resumable
across restarts unless the agent itself supports it.

**8. Commands in the composer: `?` and `!`.** A leading `?` runs a
datalog query (all modes); a leading `!` evaluates an expr against the
record currently selected in the Data tab (Facts Mode; no selection →
inline error). The command and its result render in the transcript
and persist with the conversation (extension-data entries). The agent
sees everything the user ran: command/result pairs since its last
turn are prepended to the next prompt, clearly framed as "the user
ran these" — but **a command never grants the agent a turn**; only a
chat message does. Agents likewise see their own tool queries in
history the normal way. An unsent conversation is therefore a free
scratchpad — no fourth "console" mode is needed, and the v1 Query tab
dies with the drawer.

The same preamble carries **disk-change notices**: when fsnotify
reloads a file between the agent's turns, the next prompt notes which
parts changed and their new revisions ("rules.dl changed on disk:
groups at_risk/2, admin_share/1 modified") — the reload already
computes per-part revision bumps, so the summary is free. This keeps
a Rules-mode agent from reasoning against a stale picture and turns a
stale-write rejection from a surprise into an expectation. Mid-turn,
no notice is pushed: tool reads always return current state, and a
stale write's rejection (which hands back current content) is itself
the warning, delivered at the moment it matters. No `InjectSteer` on
file events — an agent reacting to file saves is the same
blindly-reacting failure mode the no-turn-on-commands rule exists to
prevent.

**9. The browser is rich; JSON appears only in the Data tab.**

- **Data** — master-detail: file picker (from `sources[].file`), a
  compact record list (line number + first ~100 chars), and a detail
  pane pretty-printing the selected record with collapsible nesting.
  Selection doubles as the `!` eval target (replacing v1's Test
  button). Server-side pagination stays; add a substring filter over
  raw lines. This is the only JSON in the UI.
- **Schema** — sources, matchers, and declarations rendered
  structurally (not YAML), with `use` docs shown and links to the
  predicates they produce.
- **Rules** — rule-group list → group detail: syntax-highlighted rule
  text, rendered `%%` docs, head fact count, dependency links both
  directions (`predicate_deps` given a face).
- **Facts** — predicate list (base/derived, counts) → fact table with
  column names from declarations, composite terms rendered
  structurally, and a per-derived-fact "why?" expansion backed by
  provenance/`explain_fact`.

## Work (phased)

Old UI keeps working until phase 2 replaces it.

1. **CRUD core + disk-canonical + fsnotify.** The `rules/` directory
   store (group-file naming, Import to split an existing monolithic
   `.dl`, Export to concatenate; serve grows a rules-directory
   argument beside the legacy positional files); the structured CRUD
   tool surface (registration mode-scoped but modes not yet
   user-visible); agent writes to disk; fsnotify watcher (schema file
   + rules directory) with debounce → reload → validate → auto
   re-evaluate → bus publish; removal of `set_schema`/`set_rules`,
   Save, and save-time git.
2. **Conversation UI.** kit SessionManager wiring
   (`.datalog/sessions`, mode in extension data, auto-titles), the
   rail, mode picker, transcript rendering (reusing the tool-entry/
   permission/plan rendering from agent.go), consent diff cards,
   `?`/`!` commands with persistence and next-turn prepend, global
   one-turn gating, per-mode system prompts. The drawer, Query tab,
   and old views die here.
3. **Browser tabs.** Data (master-detail + filter), Schema, Rules,
   Facts as specified above.
4. **Cross-links + provenance polish.** Transcript→browser links,
   why? expansion, dependency navigation.

## Risks / open questions

- **fsnotify echo and partial writes.** Editors write files
  non-atomically (vim writes then renames, or truncates then writes);
  debounce plus validate-before-swap keeps a half-written file from
  poisoning the session — a failed reload keeps the last good state
  and reports the error.
- **Concurrent vim and agent edits.** The race has two directions,
  guarded differently. Agent-racing-human is mechanical: a vim save
  bumps the per-part revision counters on reload, so an agent write
  staged against pre-save content is rejected with the current
  content handed back. Human-racing-agent is the social contract:
  the user can see a turn running and knows not to race the train to
  the crossing — vim's "file changed on disk" warning is the
  backstop. Per-group files shrink the blast radius further: a
  collision requires both hands on the *same rule group*. Accepted
  as a decision, not a hazard: one human, one delegate.
- **Group-file naming collisions.** `<head>_<arity>.dl` must survive
  predicates whose names collide after filename sanitization; the
  serializer owns a reversible, deterministic naming scheme, and
  Import rejects a split that would collide rather than guessing.
- **Auto re-evaluate cost.** Every save burns a Transform (≤5s). At
  target scale this is the feature, not a problem; if it drags, the
  memoization relief valve from mcp-server.md applies.
- **ACP tool filtering.** Per-conversation tool scoping on the shared
  `/mcp` mount needs a conversation-identity mechanism (per-
  conversation bearer token is the likely shape). Deferred to the ACP
  leg of phase 2; kit needs none of it.
- **Title generation.** First-message truncation is the v1 of
  auto-titling; model-generated titles can layer in later.

## Out of scope

- Multi-user, authentication, non-loopback exposure beyond Tailscale.
- Editing affordances in the browser (read-only by design; the CRUD
  endpoints are shaped so per-item editing could bolt on later).
- Undo machinery (git covers it).
- Assert-fact tools or scratch fact layers.
- Concurrent agent turns.
- Incremental evaluation (still deferred per TODO.md).
