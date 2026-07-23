# Design: `datastar-acp` — an embeddable ACP chat component (package `chat`)

## Problem

The datalog workbench proved a pattern worth keeping: a web application
spawns an agent (Claude Code via the `claude-agent-acp` adapter, or any
other Agent Client Protocol agent) as a subprocess, hands it a set of
specialized MCP tools and instructions, and nests the conversation in a
chat pane beside the application's own panes. The human steers, the
agent types, the application referees.

Every future prototype shaped like datalog — an app built *around* an
agent with domain tools the agent wouldn't normally have — needs the
same plumbing: subprocess lifecycle, stdio JSON-RPC, streaming events
into the DOM over SSE, permission prompts, cancellation, conversation
persistence. That plumbing is ~40KB of Go inside `cmd/datalog` today,
tangled with datalog's tool vocabulary and mode system. This package
extracts the generic half so the next prototype embeds a chat component
instead of rebuilding one.

Source basis in datalog: `cmd/datalog/acp.go` (subprocess driver),
`agent.go` (driver interface + event vocabulary), `conversation.go`,
`conversations_http.go`, `console.go` (server-owned scrollback),
`bus.go` (SSE pub/sub), `view/` (html-go builders), and the designs in
`doc/features/acp-integration.md` and `workbench-v2.md`.

## Decisions

**1. Full chat component, not just a driver.** The package ships the
ACP subprocess driver *and* the conversation manager, rail, transcript,
composer, console morphing, permission flow, and SSE plumbing. The
driver alone would save little; the UI is where the reusable work
lives. The host supplies MCP tools, instructions, and the page shell.

**2. ACP-subprocess only; no kit dependency.** Datalog's in-process
`kitDriver` stays in datalog (it can satisfy the same driver seam from
outside if datalog wants to keep it after migrating). Dropping kit also
drops kit session files: this package owns its conversation
persistence (decision 8).

**3. Copy now, migrate datalog later.** The extraction copies and
adapts datalog's code; datalog keeps its inline version until the API
stabilizes against the example app. This deliberately violates the
one-mechanism rule *temporarily* — porting datalog's ACP path onto this
package is the milestone immediately after the E2E demo (see Work), not
an indefinite "later". Until then, fixes found in either copy must be
mirrored by hand.

**4. Package `chat`.** The directory is `datastar-acp` (the vanity
import), the Go package is `chat`: `acp` collides with
`coder/acp-go-sdk` at most import sites, and `chat.New`, `chat.Option`
read correctly at call sites.

**5. Embedding: one value that is handler, content, and lifetime.**
`chat.New(options...) (Interface, error)` returns a component that is
simultaneously:

- an `http.Handler` the host mounts at a prefix (default `/agent`):
  conversation CRUD, send, cancel, answer, and the SSE/MCP mounts
  below;
- an `html.Content` (`AppendHTML`) that renders the hydrated chat pane
  — rail, transcript, composer — for the host to place in its page
  shell;
- a `Shutdown()` that reaps the subprocess and internal goroutines.

Datastar patches publish through a small bus interface. A datastar page
has exactly one SSE feed, and hosts like datalog also push their own
panel updates through it, so the host may supply its bus; the package
ships a default (extracted from datalog's `bus.go`) and serves it under
the base path for hosts that have none.

**6. MCP: reference mount, host escape hatch.** The package lifts
datalog's MCP mount as a reference implementation — bearer token
generation, constant-time comparison, loopback enforcement, and the
`session/new` handshake that hands the URL + token to the agent via
`mcpServers`. The host registers a configured `mcp-go` server with it.
A profile may instead name any externally hosted MCP URL + token, which
is passed through the same handshake. The handshake is the chokepoint:
no host can forget the token or the mount wiring.

**7. Named agent profiles carry the specialization.** A profile is the
unit of "agent with specialized tools and instructions":

- command, args, env (e.g. `CLAUDE_CODE_EXECUTABLE`);
- working directory (also the hook for agents that read a
  `CLAUDE.md`/`AGENTS.md`);
- MCP configuration (reference mount or external URL + token);
- an instructions preamble, injected by framing the first prompt —
  ACP has no system-prompt field, and the preamble keeps the
  specialization visible in the transcript store.

The new-conversation UI offers one button per registered profile,
generalizing datalog's query/rules/facts mode picker. Each conversation
records its profile name.

**8. Persistence: `ConversationStore` interface, JSONL default.** The
default store writes one `{uuid}.jsonl` per conversation under a
host-supplied directory: line 1 is a metadata header (title, profile,
created-at, a host extension map), then one line per user prompt and
per *finalized* agent event. ACP `tool_call_update` notifications are
partial patches; the driver accumulates them (datalog's `toolState`
mechanism) and the store records only the settled entry. Replay is
therefore the live rendering path run over stored events — one
rendering mechanism, unlike datalog's separate `renderSessionHistory`
reconstruction. Raw wire logging is out of scope until something needs
it. The store also carries `Rename` — the component titles a
conversation from its first prompt (truncated; no LLM auto-title), so
the interface must be able to update the header after `Create`.

**9. Signals: configurable names, datalog defaults, scoped at the
root.** Defaults are `$busy`, `$busyConv`, `$busyConvName`, `$prompt`;
names live in the options so a host with prior conventions can adapt.
Signals initialize via `data-signals` on the component's root tag. The
package exports the BusyActionButton and `aria-busy` helpers so hosts
adopt one activity convention. `$busy` remains a page-wide mutex the
host's own long-running jobs may share.

**10. One turn at a time, globally.** Single-operator posture: `$busy`
names the one running conversation, composers elsewhere disable, the
action button morphs to Stop and posts cancel. Serializing turns keeps
MCP tool mutations from racing each other.

**11. Permissions: generic card, host renderer hook.** ACP
`RequestPermission` renders as a card with the request title/tool info
and one button per option, resolved through the answer route (pending
map keyed by request id, unblocked by channel — datalog's plumbing).
An optional `PermissionRenderer` lets a host substitute richer cards
(e.g. diffs from the tool's raw input) over the same plumbing.

**12. Styling: semantic, oat-compatible, overridable.** Views emit
semantic, class-light HTML following the same `aria-busy`/structure
conventions as the datalog workbench. The package ships optional CSS a
host can take or override; it never fights a host stylesheet.

**13. Resume is cold.** After a server restart the transcript replays
from the store, but the next prompt spawns a fresh agent session with
the profile preamble re-injected; the agent does not recall the old
context. Acceptable for prototypes, and the event store loses nothing
a smarter resume (ACP `session/load`) would need later.

**14. Subprocess lifetime: lazy, single live driver.** Spawn on a
conversation's first prompt; keep the driver across turns; when a
*different* conversation prompts, close the idle driver first —
effectively one live subprocess, bounded resources. Fatal subprocess
errors drop the driver (single monitor goroutine owns `cmd.Wait`, 5s
kill timer on crash — datalog's mechanism); the next prompt respawns.

**15. Layout: flat.** `chat` (driver, store, handlers, views — they are
as intertwined as they were in datalog), `chat/acptest` (the scripted
fake agent, kept out of production imports), `internal/` for helpers.
Further splitting waits for a second host to demand it.

## API sketch

Matches the scaffold in `chat.go`; names indicative, not final.

```go
component, err := chat.New(
    chat.BasePath("/agent"),                 // default shown
    chat.Store(store),                       // default: chat.DirStore(dir)
    chat.Bus(bus),                           // default: internal bus + SSE mount
    chat.Profile(chat.AgentProfile{
        Name:         "triage",
        Command:      "claude-agent-acp",
        Env:          []string{"CLAUDE_CODE_EXECUTABLE=..."},
        Dir:          workDir,
        Instructions: preamble,              // framed into the first prompt
        MCP:          chat.MCPServer(srv),   // or chat.MCPEndpoint(url, token)
    }),
    chat.PermissionRenderer(renderDiffCard), // optional
    chat.Signals(chat.SignalNames{...}),     // optional, datalog defaults
)
// host wiring:
mux.Handle("/agent/", component)
page := view.Page(..., component /* html.Content: the pane */)
defer component.Shutdown()
```

Event vocabulary (public, shared by driver, store, and views —
datalog's `agentEvent` shapes): `Kind ∈ message | thought | tool-call |
tool-result | error | permission | plan`, plus options and plan
entries. Stored and rendered identically live and on replay.

HTTP surface under the base path: create/list/delete conversations,
send, cancel, answer, the default SSE feed (when the package owns the
bus), and the reference MCP mount (when a profile uses it).

## Work

**Milestone 1 — example app E2E against real Claude.** `examples/`
gets a minimal `main.go`: one page embedding the pane, one toy MCP tool
(scratch key-value store), one profile wrapping `claude-agent-acp`
(honoring `CLAUDE_CODE_EXECUTABLE`). Done means: create a conversation,
prompt, watch streaming tool calls hit the toy tool, approve a
permission, stop a turn mid-flight, restart the server, and see the
transcript replay. Alongside it, `chat/acptest` — a scripted agent
binary speaking real stdio JSON-RPC, extracted from datalog's
`acp_e2e_test` harness — covers the same loop deterministically in CI.

**Milestone 2 — port datalog.** Replace datalog's inline ACP path with
this package. This is when copy-and-diverge ends; the kit path may stay
inline behind datalog's own driver seam or be retired.

## Risks / open questions

- **Divergence during the copy window.** Any ACP fix in datalog before
  milestone 2 must be mirrored here by hand. Mitigation: keep the
  window short — milestone 2 follows the demo directly.
- **Bus interface shape.** The host-supplied bus must carry datastar
  Elements/Signals batches without importing this package's view
  internals; the extraction from `bus.go` will settle the seam.
- **Preamble injection is per-agent-agnostic but visible.** Framing the
  first prompt puts instructions in the transcript. That's a feature
  (auditable) but hosts with long instructions may prefer the cwd
  file route; profiles support both.
- **`AppendHTML` and per-conversation state.** *Resolved:* `AppendHTML`
  is the fresh-page hydration view — the rail of choosable
  conversations, the empty transcript state, the composer, signal
  initialization, and (when the component owns the bus) the SSE
  connect. It never restores a selected conversation; selection and
  transcript replay happen only via bus patches from the select route.
  The component still tracks the active conversation server-side for
  driver lifetime and the turn gate, but that state never leaks into
  the initial HTML.

## Out of scope

- In-process (kit) agents, and any LLM API access of our own.
- ACP `session/load` resume; digest-into-prompt resume.
- Parallel turns, multi-user, non-loopback deployment.
- Raw ACP wire logging.
- Datalog's tool vocabulary, mode semantics, consent diff-card
  internals (a host may rebuild them via `PermissionRenderer`).
