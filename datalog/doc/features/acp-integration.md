# Feature: Embedded agent (`datalog serve` chat pane, ACP + kit)

## Problem

The MCP server (mcp-server.md) gives a model the authoring loop, but
headless: the operator launches an agent runtime elsewhere and watches
nothing while it works. The web workbench (web-ui.md) gives the human
observation and correction, but authoring stays manual. The third leg
closes the triangle: nest an agent *inside* the workbench. The user
converses with it in a chat pane; the agent manipulates the same
session through the same MCP tools; every mutation appears live in the
panes the user is already watching. The human steers, the agent types,
the engine referees.

Two audiences: operators who already run an agent (Claude Code) should
plug it in; operators who run nothing should get an agent out of the
box, so `datalog serve` is an all-in-one solution — binary plus an API
key equals a working workbench with an agent in it.

## Design constraints and observations

**1. The plug-in case is the IDE-embedding problem, and ACP is its
protocol.** The Agent Client Protocol (agentclientprotocol.com)
standardizes exactly this shape: a *client* (an editor — here, the
workbench server) spawns an *agent* (Claude Code via the
`@agentclientprotocol/claude-agent-acp` adapter, Gemini CLI natively,
others) as a subprocess and speaks JSON-RPC 2.0 over its stdio. We
take the client role. Targeting the protocol instead of a vendor keeps
the choice of agent with the operator — one `--agent` flag, no
per-vendor code.

**2. The all-in-one case is `mark3labs/kit`, embedded as a library.**
kit (`pkg/kit`) is an off-the-shelf coding agent embeddable in-process:
`kit.New(ctx, opts)`, multi-provider model selection
(`anthropic/...`, `openai/...`, Ollama, Bedrock, ...) configured via
`~/.kit.yml` and provider API-key environment variables, streamed
turns, and — decisively — in-process registration of mcp-go servers
(`InProcessMCPServers`), so the embedded agent reaches the session's
tools with no subprocess, no socket, no token. kit exposes no ACP
server as a library, which motivates the driver split in observation
4. kit is built on mcp-go, which is one reason mcp-server.md adopts
mcp-go as the MCP framework.

**3. `session/new` carries `mcpServers` — but a stdio MCP subprocess
would be a *different* session.** For external ACP agents, ACP lets
the client declare MCP servers at session setup, so ACP and MCP wire
up in one handshake. But the obvious configuration —
`command: datalog mcp` — would have the agent spawn a fresh process
with fresh state, sharing nothing with the workbench. Shared state
therefore forces the MCP server *into* the serve process: mount the
same mcp-go server over streamable HTTP at `/mcp` and pass the agent
its URL. ACP agents are only required to support stdio MCP (HTTP is an
optional capability), so a shim — `datalog mcp --proxy <url>`, a dumb
stdio↔HTTP pipe — covers agents without it. Claude Code's adapter
speaks HTTP; the shim is insurance. The embedded kit path skips all of
this via in-process registration.

**4. One chat pane, two drivers, ACP's event vocabulary.** The pane
should not know which agent is behind it. Define a small internal
`agentDriver` interface whose event stream *is* ACP's update shape —
message chunk, thought chunk, tool call/update, plan, permission
request, turn end — with two implementations: `acpDriver` (subprocess
over stdio, events passed through) and `kitDriver` (pkg/kit in-process,
its stream events mapped into the same shapes). ACP's vocabulary is
the standard and the superset; kit's events project into it. The pane,
cancellation, and turn-state logic are written once.

**5. The agent's only lever on the workspace is the MCP tools.** For
ACP agents, we decline the client-side `fs` and `terminal`
capabilities at `initialize`. For embedded kit, we register *only* the
session's MCP server and disable kit's built-in coding tools (file
edit, shell) — the workbench is not a coding workspace, and everything
the session tools permit was already engineered for an untrusted
caller (path confinement, expr-environment audit, limits, timeouts).
Be precise about what this is *not*: an external agent process runs
with the operator's privileges and — for Claude Code — carries its own
built-in tools unless the operator configures otherwise. Capability
refusal constrains the *workspace*, not the process; process-level
containment belongs to the operator's agent configuration. The
embedded kit path is tighter: with built-ins off, its tool surface is
exactly ours.

**6. Two hands, one document: last-writer-wins.** The user and the
agent both edit the session's canonical documents (`schemaText`,
`rulesText` — see web-ui.md). They are one human and their delegate,
not two users: when the agent writes, the editors patch over SSE with
a visible "updated by agent" marker; when the human writes, the next
agent tool call sees the new text. The human is smart enough to know
not to fix rules while their agent is working — the same social
contract every ACP-hosting IDE relies on. Version control and conflict
management can layer in later if the triangle proves out.

**7. The emergency brake extends naturally.** The workbench's global
Cancel already cancels every running engine operation; it additionally
cancels the active turn (`session/cancel` for ACP; context
cancellation for kit). The MCP per-call timeout still bounds each tool
call underneath, so a cancelled turn cannot leave a runaway Transform
behind.

**Design decision: one chat pane over an `agentDriver` interface
speaking ACP's event vocabulary, with embedded kit (in-process, MCP
via in-process transport, built-in tools disabled) as the all-in-one
default and any ACP agent (subprocess, MCP via in-process HTTP mount)
as the operator's alternative — either way, the agent's only workspace
lever is the shared session's tools, supervised by the human.**

## Proposed solution

### CLI

```
datalog serve -d ./data [-c schema.yaml] [rules.dl ...] \
    [--model anthropic/claude-...]                       \
    [--agent 'npx @agentclientprotocol/claude-agent-acp']
```

- Default: embedded kit, model resolved kit's way (`--model` flag →
  `KIT_MODEL` → `~/.kit.yml`), provider keys from the environment
  (`ANTHROPIC_API_KEY`, ...). No resolvable model: serve runs with the
  chat pane showing how to configure one; everything else works.
- `--agent` replaces kit with an external ACP agent command, split
  shell-style (phase 2 — see Work).
- Either agent is created/spawned lazily on the first prompt and
  recreated on the next prompt after an exit or fatal error.

### The `agentDriver` interface

```go
// agentDriver abstracts one conversational agent behind the chat
// pane. Events use ACP's update vocabulary regardless of backend.
type agentDriver interface {
    // Prompt starts one turn; events stream to the sink until the
    // turn ends. One turn at a time; Prompt while a turn is active
    // is an error surfaced to the pane.
    Prompt(ctx context.Context, text string, sink func(agentEvent)) (stopReason string, err error)
    // Answer resolves a pending permission request by option ID.
    Answer(requestID, optionID string) error
    Close() error
}
```

`kitDriver` wraps `pkg/kit`: `kit.New` with the session's
`*server.MCPServer` in `InProcessMCPServers`, built-in tools disabled,
stream events mapped to `agentEvent`s. `acpDriver` wraps
`github.com/coder/acp-go-sdk`'s client side
(`NewClientSideConnection`, Initialize/NewSession/Prompt) around a
spawned subprocess, passing updates through.

### Handshake (ACP driver)

1. Spawn the agent; `initialize` with client capabilities
   `{fs: {readTextFile: false, writeTextFile: false}, terminal:
   false}`; record the agent's capabilities, notably
   `mcpCapabilities.http`.
2. Mint a per-launch bearer token and require it on `/mcp`. The
   listener is loopback, but other local processes should not get tool
   access for free. The token travels in the `mcpServers` config
   (headers for HTTP, environment for the proxy shim), never in argv.
3. `session/new` with `cwd` set to the project directory and
   `mcpServers` set to the HTTP endpoint — or, when the agent lacks
   the HTTP capability, the `datalog mcp --proxy <url>` stdio command.

The kit driver has no handshake: the mcp-go server value is handed to
`kit.New` directly. From phase 2 onward, `/mcp` (with its token) is
mounted regardless of driver, so external MCP clients can reach the
live session even when kit is the agent.

### Chat pane

The chat pane is the **Agent tab of the console drawer** (web-ui.md
"Console drawer"): a full-width collapsible strip beneath both views'
three columns, not a peer pane in the column grid. The placement is
forced by this feature's premise — the agent's mutations land in the
panes above, and the human must watch those panes *while* the agent
types, so the chat cannot live on a view of its own. The transcript
is session state: the server owns it, page loads re-render it, and
streamed events patch it over SSE, so navigating between the Facts
and Rules views mid-turn drops nothing. (An ultrawide fourth-column
layout is an acknowledged possible future toggle — see web-ui.md's
open questions — and would change only chrome, nothing here.)

A prompt box issues one turn at a time, with send disabled while a
turn runs. The pane renders `agentEvent`s as they stream:

- **Message chunks** — streamed text, minimal markdown (paragraphs,
  code fences).
- **Thought chunks** — collapsed `<details>` block.
- **Tool calls / updates** — one line per call: tool name, compact
  input summary, status, result summary (e.g. `query → 14 rows,
  92ms`), expandable to raw JSON.
- **Plan** (phase 2) — a checklist block updated in place.
- **Permission requests** (phase 2) — the request's options rendered
  as inline buttons; the reply carries the chosen option. A turn
  blocked on permission shows that state plainly.
- **Turn end** — the stop reason is shown when it is not `end_turn`;
  an agent crash renders an explicit "agent exited (code N)" terminal
  state, never silence.

Agent tool calls mutate the session, so each state-changing tool
update triggers the same pane patches an Apply does — fact counts,
editor text, query results. The chat pane and the workspace panes
cannot drift because they are all views of one session.

### Cancellation

The global Cancel cancels the active turn (`session/cancel` /
context) and fires the engine brake. An ACP agent is expected to end
the turn with stop reason `cancelled`; if the subprocess wedges, a
kill timer (5s) terminates it, and the next prompt respawns it.

### Persistence

Unchanged from web-ui.md: the agent never touches disk; the human
reviews the panes and clicks Save → git commit. This resolves the MCP
doc's "ask your agent for the final text" awkwardness — in the trio,
the Save button *is* the persistence path.

## Work

Two phases, split along the `agentDriver` seam: everything network-
and subprocess-shaped belongs to the external-agent path and none of
it blocks the all-in-one. Phase 1 ships a single binary with an
embedded agent, no listening surface beyond the UI port, and no
subprocess management.

### Phase 1 — embedded kit (the all-in-one)

1. **`agentDriver` + `kitDriver`** (`internal/agentpane` or
   `cmd/datalog/agent.go`): the interface, event types, and the kit
   wrapper — verify `pkg/kit`'s options for disabling built-in tools
   and mapping its stream events; this verification gates the design
   (see risks). Prior integrations of kit exist to crib from.
2. **Chat pane**: templates and Datastar wiring; message, tool-call,
   and turn-end rendering; turn-state indicator.
3. **Cancel integration** with the existing brake (context
   cancellation of the kit turn).
4. **Tests**: a kitDriver test against a stub provider if pkg/kit
   permits, else against the driver's event mapping in isolation; a
   cancel-mid-turn test.
5. **README**: the serve section documents the embedded default (kit,
   model/key configuration) and the capability posture from
   observation 5.

### Phase 2 — external agents over ACP

6. **MCP over HTTP in serve**: mount the existing mcp-go server via
   its streamable HTTP transport at `/mcp`, behind bearer-token
   middleware. The handlers do not change — they already serialize on
   the session mutex.
7. **`datalog mcp --proxy <url>`**: the stdio↔HTTP bridge, roughly a
   page of code. Also useful standalone, for any external MCP client
   that wants the live workbench session.
8. **`acpDriver`**: spawn/initialize/new-session/prompt/cancel
   lifecycle over `coder/acp-go-sdk`, update pass-through, permission
   plumbing, respawn on exit, the kill timer.
9. **Chat pane additions**: permission-request buttons and plan
   rendering — deferred here because kit may not emit either event;
   the pane's phase-1 vocabulary is message/thought/tool/turn-end.
10. **Tests**: a scripted fake ACP agent binary that reads
    `session/prompt` and replays a canned update sequence — including
    a real tool call against `/mcp` and a permission request — driven
    end-to-end; a token-auth negative test.
11. **README**: `--agent` with the two verified external agents
    (claude-agent-acp, Gemini CLI).

## Risks / open questions

- **kit's containment knobs need verification.** The design assumes
  pkg/kit can disable its built-in coding tools and restrict the tool
  surface to registered MCP servers, and that its permission/approval
  flow (if any) is surfaceable as events. If any of these fail,
  fallback: run kit as a subprocess in its ACP server mode through the
  acpDriver instead — all-in-one becomes "bundled command" rather than
  "embedded library", and the driver split still pays for itself.
- **Capability refusal is not a sandbox.** Restated from observation 5
  because it is the easiest thing to misread: an external agent
  process runs with the operator's privileges and may carry its own
  tools. This design constrains the workspace, not the process.
  Document prominently; do not pretend to mitigate. (Embedded kit with
  built-ins off does not have this gap.)
- **Dependency weight.** pkg/kit pulls multi-provider LLM SDKs into
  the datalog binary. Acceptable for an all-in-one tool; if binary
  size or dependency churn hurts, a build tag (`nokitagent`) that
  compiles out the kitDriver is cheap to add.
- **Protocol and adapters are young.** ACP is v0.13.x as of mid-2026
  and the Claude adapter has already been renamed once
  (`@zed-industries/...` → `@agentclientprotocol/...`). Pin versions
  of acp-go-sdk, mcp-go, and kit; keep the consumed subsets minimal
  behind the driver interface.
- **Turn-blocking UX.** One turn at a time plus the serialized
  mutation pipeline means an agent grinding through queries makes the
  human's live-eval laggy. Accepted for v1 (same note as web-ui.md);
  the memoization option in mcp-server.md is the first relief valve.
- **Tool results inflate agent context.** The MCP tools' limits (query
  row cap, sample truncation) already bound payloads; tool
  descriptions should steer agents toward counts and samples over
  dumps.

## Out of scope

- Multiple concurrent agents or agent sessions; one agent, one turn at
  a time.
- Conversation persistence across serve restarts (ACP `session/load`,
  kit session files).
- Granting the `fs` or `terminal` capabilities, or re-enabling kit's
  built-in coding tools, even partially.
- Exposing datalog itself as an ACP *agent*; we are only the client.
- Authentication beyond loopback + per-launch bearer token.
- Rich chat rendering: images, file attachments, full markdown.
- kit's wider feature set (skills, subagents, extensions, themes) —
  the embedding uses prompt/stream/MCP registration and nothing else.
