package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/mark3labs/kit/pkg/kit"
	"github.com/mark3labs/mcp-go/server"
	html "github.com/swdunlop/html-go"
	"github.com/swdunlop/html-go/datastar"
	"github.com/swdunlop/html-go/tag"
)

// agentEvent is one streamed update from an agent turn, using ACP's update
// vocabulary regardless of backend (doc/features/acp-integration.md
// observation 4): kit's richer event stream projects into these shapes, and
// a future acpDriver passes its updates through unchanged.
type agentEvent struct {
	Kind string // "message" | "thought" | "tool-call" | "tool-result" | "error"

	Text string // message/thought chunk, or error text

	// Tool-call correlation fields (tool-call and tool-result kinds).
	ToolCallID string
	ToolName   string
	ToolArgs   string // JSON-encoded arguments
	Result     string
	IsError    bool
}

// agentDriver abstracts one conversational agent behind the chat pane
// (doc/features/acp-integration.md). Phase 1 needs only Prompt and Close;
// Answer (permission requests) arrives with the acpDriver in phase 2.
type agentDriver interface {
	// Prompt starts one turn; events stream to sink until the turn ends.
	// One turn at a time — the workbench's jobs set enforces it, so a
	// driver may assume no concurrent Prompt calls.
	Prompt(ctx context.Context, text string, sink func(agentEvent)) (stopReason string, err error)
	Close() error
}

// agentConfig carries the operator's model choice from serve's flags into
// the lazily-constructed driver. Zero values defer to kit's own precedence
// chain (KIT_MODEL and friends, then ~/.kit.yml) — the flags exist so the
// operator can point one serve instance somewhere specific without
// touching their global kit config.
type agentConfig struct {
	Model          string
	ProviderURL    string
	ProviderAPIKey string
}

// kitDriver embeds mark3labs/kit in-process (doc/features/acp-integration.md
// phase 1): built-in coding tools disabled, no skills/extensions/context
// files/session persistence — the workbench is not a coding workspace, and
// the agent's only lever is the session's MCP tools, registered in-process
// so no subprocess, socket, or token is involved.
type kitDriver struct {
	k *kit.Kit
}

// agentSystemPrompt frames the workbench for the agent. The MCP server's
// own instructions (mcp_docs.go) ride along via the registered server; this
// prompt only sets the conversational posture.
const agentSystemPrompt = `You are the assistant embedded in a Datalog workbench.
The human is authoring a jsonfacts schema (mapping JSONL records to base facts)
and Datalog rules over those facts, and their working session is usually
already loaded. Orient with list_predicates, then answer questions with query
and sample_facts against what is there. set_schema and set_rules REPLACE the
human's working documents - call them only when the human explicitly asks you
to change the schema or rules, never to answer a question; sample_input is for
authoring too, not for lookups. Datalog is the reasoner: express joins and
filters in a single conjunctive query and let the engine unify, rather than
fetching predicates one at a time and correlating results yourself. Prefer
counts and samples over dumps. Every
change you make appears immediately in the panes the human is watching; keep
them informed of what you changed and why.`

// newKitDriver constructs the embedded agent. mcpSrv is the workbench's own
// mcp-go server value — the same one mounted at /mcp — registered through
// kit's InProcessMCPServers so tool calls hit the session directly.
func newKitDriver(ctx context.Context, cfg agentConfig, mcpSrv *server.MCPServer) (*kitDriver, error) {
	k, err := kit.New(ctx, &kit.Options{
		Model:            cfg.Model,
		ProviderURL:      cfg.ProviderURL,
		ProviderAPIKey:   cfg.ProviderAPIKey,
		SystemPrompt:     agentSystemPrompt,
		DisableCoreTools: true,
		NoSession:        true,
		NoSkills:         true,
		NoExtensions:     true,
		NoContextFiles:   true,
		Quiet:            true,
		InProcessMCPServers: map[string]*kit.MCPServer{
			"datalog": mcpSrv,
		},
	})
	if err != nil {
		return nil, err
	}
	return &kitDriver{k: k}, nil
}

// Prompt runs one turn, mapping kit's event stream into agentEvents
// (doc/notes/kit.md §4's subscribe-then-PromptResult idiom). Subscriptions
// are per-turn rather than per-driver so sink never outlives its turn.
func (d *kitDriver) Prompt(ctx context.Context, text string, sink func(agentEvent)) (string, error) {
	// MessageUpdateEvent fires per STREAMING chunk only — when the provider
	// (or kit's resolved streaming mode) doesn't stream the final text, the
	// reply exists solely in TurnResult.Response, so it is replayed below as
	// one message event rather than silently dropped.
	var sawMessage bool
	unsubMsg := d.k.OnMessageUpdate(func(e kit.MessageUpdateEvent) {
		sawMessage = true
		sink(agentEvent{Kind: "message", Text: e.Chunk})
	})
	defer unsubMsg()
	unsubThought := d.k.OnReasoningDelta(func(e kit.ReasoningDeltaEvent) {
		sink(agentEvent{Kind: "thought", Text: e.Delta})
	})
	defer unsubThought()
	unsubCall := d.k.OnToolCall(func(e kit.ToolCallEvent) {
		sink(agentEvent{Kind: "tool-call", ToolCallID: e.ToolCallID, ToolName: e.ToolName, ToolArgs: e.ToolArgs})
	})
	defer unsubCall()
	unsubResult := d.k.OnToolResult(func(e kit.ToolResultEvent) {
		sink(agentEvent{Kind: "tool-result", ToolCallID: e.ToolCallID, ToolName: e.ToolName,
			ToolArgs: e.ToolArgs, Result: e.Result, IsError: e.IsError})
	})
	defer unsubResult()
	unsubErr := d.k.OnError(func(e kit.ErrorEvent) {
		sink(agentEvent{Kind: "error", Text: e.Error.Error()})
	})
	defer unsubErr()

	res, err := d.k.PromptResult(ctx, text)
	if err != nil {
		return "", err
	}
	if !sawMessage && res.Response != "" {
		sink(agentEvent{Kind: "message", Text: res.Response})
	}
	return res.StopReason, nil
}

func (d *kitDriver) Close() error { return d.k.Close() }

// agentTurnJobKey gates the Agent tab on the jobs set: one turn at a time,
// and Global Cancel (Stop) cancels the active turn's context — the
// emergency-brake extension acp-integration.md observation 7 specifies.
const agentTurnJobKey = "agent"

// handleConsolePrompt is the Agent tab's send action (POST /console/prompt).
// The turn runs in a background goroutine detached from the request context
// — a turn takes as long as the model takes, must survive the POST
// returning, and every page watches it over /events anyway. The POST's own
// stream just clears the prompt input; busy-state, transcript entries, and
// errors all travel the bus.
func (wb *workbench) handleConsolePrompt(w http.ResponseWriter, r *http.Request) {
	var sig consoleSignals
	decodeErr := datastar.Decode(&sig, r)

	stream, err := datastar.RequestStream(w, r)
	if err != nil {
		return
	}
	if decodeErr != nil {
		wb.consoleAppend("agent", "error", html.Text(decodeErr.Error()))
		return
	}
	text := strings.TrimSpace(sig.ConsolePrompt)
	if text == "" {
		return
	}

	driver, err := wb.agentDriver()
	if err != nil {
		wb.consoleAppend("agent", "error", html.Text(fmt.Sprintf(
			"agent unavailable: %v — configure a model with --model (or KIT_MODEL / ~/.kit.yml) and restart serve", err)))
		return
	}

	// context.Background, not r.Context(): the turn outlives the POST. The
	// jobs entry is the turn's only cancellation path (Stop / CancelAll).
	jobCtx, done := wb.jobs.Begin(context.Background(), agentTurnJobKey)
	if jobCtx == nil {
		wb.consoleAppend("agent", "error", html.Text("a turn is already running"))
		return
	}

	_ = stream.Emit(datastar.Signal(map[string]any{"consolePrompt": ""}))
	wb.consoleAppend("agent", "user", html.Text(text))
	wb.publishBusy("agent")

	go func() {
		defer done()
		defer wb.publishBusy("")
		wb.runAgentTurn(jobCtx, driver, text)
	}()
}

// agentDriver returns the lazily-constructed embedded agent, building it on
// first use (acp-integration.md: "created/spawned lazily on the first
// prompt and recreated on the next prompt after an exit or fatal error").
// Construction failure is not cached — a fixed environment (say, an API key
// exported and serve restarted... or kit config corrected) should be
// retried, and until then each prompt reports the same error.
func (wb *workbench) agentDriver() (agentDriver, error) {
	wb.agentMu.Lock()
	defer wb.agentMu.Unlock()
	if wb.agent != nil {
		return wb.agent, nil
	}
	d, err := newKitDriver(context.Background(), wb.agentCfg, wb.mcpSrv)
	if err != nil {
		return nil, err
	}
	wb.agent = d
	return d, nil
}

// dropAgentDriver discards the current driver after a fatal turn error so
// the next prompt reconstructs it fresh.
func (wb *workbench) dropAgentDriver(d agentDriver) {
	wb.agentMu.Lock()
	if wb.agent == d {
		wb.agent = nil
	}
	wb.agentMu.Unlock()
	_ = d.Close()
}

// runAgentTurn drives one turn's event stream into console entries:
//
//   - message chunks accumulate into one streaming "agent" entry, morphed
//     in place per chunk (medea's per-token MorphEvent idiom);
//   - thought chunks accumulate the same way into a collapsed <details>;
//   - each tool call gets its own entry, updated from "running" to its
//     result summary when the tool returns;
//   - a non-clean stop reason or turn error appends a terminal entry —
//     never silence (acp-integration.md "Chat pane": an agent crash renders
//     an explicit terminal state).
func (wb *workbench) runAgentTurn(ctx context.Context, driver agentDriver, text string) {
	var (
		mu         sync.Mutex // orders entry updates; kit events fire from its goroutines
		msgID      uint64
		msgText    strings.Builder
		thoughtID  uint64
		thoughtBuf strings.Builder
		toolIDs    = map[string]uint64{} // ToolCallID → console entry id
	)

	sink := func(ev agentEvent) {
		mu.Lock()
		defer mu.Unlock()
		switch ev.Kind {
		case "message":
			msgText.WriteString(ev.Text)
			if msgID == 0 {
				msgID = wb.consoleAppend("agent", "agent", html.Text(msgText.String()))
			} else {
				wb.consoleUpdate(msgID, "agent", html.Text(msgText.String()))
			}
		case "thought":
			thoughtBuf.WriteString(ev.Text)
			body := thoughtEntry(thoughtBuf.String())
			if thoughtID == 0 {
				thoughtID = wb.consoleAppend("agent", "thought", body)
			} else {
				wb.consoleUpdate(thoughtID, "thought", body)
			}
		case "tool-call":
			id := wb.consoleAppend("agent", "tool", toolEntry(ev, false))
			toolIDs[ev.ToolCallID] = id
		case "tool-result":
			if id, ok := toolIDs[ev.ToolCallID]; ok {
				wb.consoleUpdate(id, "tool", toolEntry(ev, true))
			} else {
				wb.consoleAppend("agent", "tool", toolEntry(ev, true))
			}
		case "error":
			wb.consoleAppend("agent", "error", html.Text(ev.Text))
		}
	}

	stopReason, err := driver.Prompt(ctx, text, sink)
	mu.Lock()
	gotReply := msgID != 0
	mu.Unlock()
	switch {
	case ctx.Err() != nil:
		wb.consoleAppend("agent", "note", html.Text("turn cancelled"))
	case err != nil:
		wb.consoleAppend("agent", "error", html.Text(fmt.Sprintf("turn failed: %v", err)))
		wb.dropAgentDriver(driver)
	case !gotReply:
		// A model may end its turn on a tool round without composing any
		// reply (small models do this routinely). Rendering nothing would
		// read as a hang — say so instead (acp-integration.md: an ended
		// turn always has explicit terminal state).
		if stopReason == "" {
			stopReason = "unknown"
		}
		wb.consoleAppend("agent", "note",
			html.Text("the model ended the turn without a reply (stop reason: "+stopReason+")"))
	case stopReason != "" && stopReason != "stop" && stopReason != "end_turn" && stopReason != "tool_calls":
		wb.consoleAppend("agent", "note", html.Text("turn ended: "+stopReason))
	}
}

// thoughtEntry renders accumulated reasoning as a collapsed disclosure —
// present but out of the way, per acp-integration.md's chat pane spec.
func thoughtEntry(text string) html.Content {
	return tag.New("details",
		tag.New("summary", html.Text("thinking…")),
		tag.New("pre", html.Text(text)),
	)
}

// toolEntry renders one tool call as a single collapsed <details>: the
// summary line is the tool name, elided arguments, and status — one line of
// drawer real estate per call — and expanding it reveals the full
// arguments, the result, or the error. The status span turns red on error,
// so a failed call is visible without expanding. Result text is already
// bounded by the MCP tools' own limits (row caps, sample truncation), and
// nothing elided on the summary line is lost — the body always carries the
// full documents. The query tool's body renders as a result table instead
// of raw JSON.
func toolEntry(ev agentEvent, done bool) html.Content {
	name := strings.TrimPrefix(ev.ToolName, "datalog__")
	headText := name + " " + argsSummary(name, ev.ToolArgs)
	compact, elided := compactArgs(headText)
	body := html.Group{}
	if elided {
		body = append(body, tag.New("pre", html.Text(formatToolResult(ev.ToolArgs))))
	}
	if done && ev.Result != "" {
		body = append(body, toolResultBody(name, ev))
	}
	// The floated status precedes the code in source order so it pins to
	// the first line's right edge even when a long summary wraps.
	return tag.New("details",
		tag.New("summary.tool-line",
			toolStatus(ev, done),
			tag.New("code", html.Text(compact)),
		),
		body,
	)
}

// argsSummary reduces a tool call's JSON arguments to their essence for the
// summary line: the query tool shows the query itself (the JSON envelope is
// transport, not something the operator should read); everything else shows
// its compacted JSON.
func argsSummary(name, args string) string {
	if name == "query" {
		var in queryInput
		if json.Unmarshal([]byte(args), &in) == nil && in.Query != "" {
			return in.Query
		}
	}
	return strings.TrimSpace(args)
}

// toolResultBody renders a finished call's outcome inside the disclosure:
// errors as the editor's error-list styling, query results as the editor's
// variable-named table, and anything else as pretty-printed JSON.
func toolResultBody(name string, ev agentEvent) html.Content {
	if ev.IsError {
		return toolError(ev.Result)
	}
	if name == "query" {
		var out queryOutput
		if json.Unmarshal([]byte(ev.Result), &out) == nil {
			rows := make([][]string, len(out.Rows))
			for i, row := range out.Rows {
				cells := make([]string, len(row))
				for j, c := range row {
					cells[j] = cellString(c)
				}
				rows[i] = cells
			}
			return resultTable(queryResultBlock{
				Vars:      out.Vars,
				Rows:      rows,
				Total:     out.Total,
				Truncated: out.Truncated,
			})
		}
		// Not the tool's structured shape (a transport quirk); fall through
		// to the raw rendering rather than show an empty table.
	}
	return tag.New("pre", html.Text(formatToolResult(ev.Result)))
}

// cellString displays one decoded result cell: strings as-is, everything
// else (numbers, composite JSON terms) re-marshalled so it reads as it was
// written rather than through fmt's float formatting.
func cellString(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprint(v)
	}
	return string(b)
}

// toolError renders a failed tool call's message, matching the Datalog
// Editor's error-list styling.
func toolError(text string) html.Content {
	return tag.New("ul.errors", tag.New("li", html.Text(text)))
}

// formatToolResult pretty-prints a JSON tool result for the disclosure —
// the MCP tools emit single-line JSON, unreadable in a <pre>. Anything that
// isn't a JSON document (error strings, plain text) passes through as-is.
func formatToolResult(s string) string {
	trimmed := strings.TrimSpace(s)
	if trimmed == "" || (trimmed[0] != '{' && trimmed[0] != '[') {
		return s
	}
	var buf bytes.Buffer
	if err := json.Indent(&buf, []byte(trimmed), "", "  "); err != nil {
		return s
	}
	return buf.String()
}

// toolStatus marks the summary line only when there is something to say:
// "running…" while the call is in flight, a red "error" on failure, and
// nothing at all on success — a finished call with no flag is the ok state.
func toolStatus(ev agentEvent, done bool) html.Content {
	switch {
	case !done:
		return tag.New("span.tool-status", html.Text("running…"))
	case ev.IsError:
		return tag.New("span.tool-status.error", html.Text("error"))
	default:
		return html.Group{}
	}
}

// compactArgs bounds a tool call's summary line to one readable length,
// reporting whether anything was cut so the caller can put the full
// document in the disclosure body (elided JSON must always stay reachable).
func compactArgs(args string) (string, bool) {
	args = strings.Join(strings.Fields(args), " ")
	const max = 120
	if len(args) > max {
		return args[:max] + "…", true
	}
	return args, false
}
