# datastar-acp demo

A minimal host embedding the `chat` component (`swdunlop.dev/pkg/datastar-acp`):
one page, two agent profiles, one toy MCP tool.

- **`plain`** — the ACP agent you name on the command line, no tools.
- **`kv`** — the same agent, plus a scratch key-value store exposed as an MCP
  tool (`kv_set` / `kv_get`) mounted through the component's reference MCP mount.
  The profile's `Instructions` tell the agent to use it.

This is Milestone 1 scaffolding: it demonstrates the wiring. A live run against
a real agent is done by the operator, not by CI.

## Run

The demo requires the ACP agent command as its arguments; it refuses to start
without one. The agent subprocess inherits the demo's environment, so
adapter-specific settings (like `CLAUDE_CODE_EXECUTABLE` for the Claude
adapter) just need to be set in the shell.

```sh
# Claude via the claude-agent-acp adapter:
CLAUDE_CODE_EXECUTABLE=$(which claude) \
  go run ./examples/demo -- npx -y @agentclientprotocol/claude-agent-acp

# Gemini CLI speaks ACP natively:
go run ./examples/demo -- gemini --experimental-acp
```

Then open <http://127.0.0.1:8765>. The component serves its own HTTP surface
under `/agent/` and opens its SSE feed automatically.

Transcripts persist to `./conversations` (override with `DEMO_CONV_DIR`).

## What to try

1. Start a `plain` conversation from the rail, send a prompt, watch the reply
   stream in.
2. Start a `kv` conversation and ask the agent to store and then retrieve a
   value — the tool call and result appear in the transcript, and a permission
   card may prompt you to approve the call.
3. Stop a turn mid-flight with the composer's Stop button.
4. Restart the server and reselect a conversation — the transcript replays from
   the store (the next prompt starts a fresh agent session; resume is cold).

## Deterministic testing

For CI or offline development, drive the same loop without real Claude using the
scripted agent in `swdunlop.dev/pkg/datastar-acp/acptest` — see that package's
`Example` and the end-to-end suite in `datastar-acp/e2e_test.go`.
