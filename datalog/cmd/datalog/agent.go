package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/mark3labs/kit/pkg/kit"
	"github.com/mark3labs/mcp-go/server"
	html "github.com/swdunlop/html-go"
	"github.com/swdunlop/html-go/datastar"
	"github.com/swdunlop/html-go/tag"
)

// agentEvent is one streamed update from an agent turn, using ACP's update
// vocabulary regardless of backend (doc/features/acp-integration.md
// observation 4): kit's richer event stream projects into these shapes, and
// acpDriver passes its updates through nearly unchanged. Phase 2 adds
// "permission" and "plan" — kit never emits either (observation: kit has no
// approval flow to surface), so kitDriver simply never produces them; the
// vocabulary is still ACP's superset, per observation 4's mandate.
type agentEvent struct {
	Kind string // "message" | "thought" | "tool-call" | "tool-result" | "error" | "permission" | "plan"

	Text string // message/thought chunk, or error text

	// Tool-call correlation fields (tool-call and tool-result kinds).
	ToolCallID string
	ToolName   string
	ToolArgs   string // JSON-encoded arguments
	Result     string
	IsError    bool

	// Permission-request fields ("permission" kind). RequestID is the
	// driver-generated key Answer(requestID, ...) resolves; it is distinct
	// from ToolCallID because a single tool call could in principle be
	// re-requested (ACP does not forbid it), and because kitDriver — which
	// never emits "permission" — has no ToolCallID/RequestID pairing to
	// keep consistent. The tool name/args ride along so the pane can render
	// "agent wants to run X with {...}" without a second round-trip.
	RequestID string
	Options   []agentOption

	// Plan fields ("plan" kind): the agent's full task list as of this
	// update. ACP's plan/update replaces the whole list each time (no
	// incremental diffing), so PlanEntries is always the complete plan.
	PlanEntries []agentPlanEntry
}

// agentOption is one selectable response to a permission request — ACP's
// PermissionOption trimmed to what the pane needs to render a button and
// echo a choice back through Answer.
type agentOption struct {
	ID   string // Answer's optionID
	Name string // button label
	Kind string // "allow_once" | "allow_always" | "reject_once" | "reject_always" (ACP's PermissionOptionKind, passed through as a plain string so this package stays acp-agnostic)
}

// agentPlanEntry is one task in the agent's plan — ACP's PlanEntry trimmed
// to content and status, which is all the phase-2 checklist rendering
// (agent.go's runAgentTurn) needs; priority is dropped as unused for now.
type agentPlanEntry struct {
	Content string
	Status  string // "pending" | "in_progress" | "completed" (ACP's PlanEntryStatus)
}

// pendingPermission is one entry in the workbench's RequestID→state map
// (serve.go's wb.pendingPerm): the console entry id to morph when the
// request resolves, and the original event so the resolved rendering can
// still name the tool it gated (handleConsoleAnswer and runAgentTurn's
// post-turn cleanup both need this — the map, not a turn-local closure, is
// what makes the event reachable from the HTTP handler).
type pendingPermission struct {
	entryID uint64
	event   agentEvent
}

// agentDriver abstracts one conversational agent behind the chat pane
// (doc/features/acp-integration.md). kitDriver's Answer always errors — kit
// issues no permission requests to answer.
type agentDriver interface {
	// Prompt starts one turn; events stream to sink until the turn ends.
	// One turn at a time — the workbench's jobs set enforces it, so a
	// driver may assume no concurrent Prompt calls.
	Prompt(ctx context.Context, text string, sink func(agentEvent)) (stopReason string, err error)
	// Answer resolves a pending permission request (a "permission"
	// agentEvent's RequestID) by option ID, unblocking the driver's
	// in-flight session/request_permission RPC. Unknown requestID is an
	// error.
	Answer(requestID, optionID string) error
	Close() error
}

// agentConfig carries the operator's model/agent choice from serve's flags
// into the lazily-constructed driver. Zero values for Model/ProviderURL/
// ProviderAPIKey defer to kit's own precedence chain (KIT_MODEL and
// friends, then ~/.kit.yml) — the flags exist so the operator can point one
// serve instance somewhere specific without touching their global kit
// config.
//
// AgentCommand switches the driver: empty selects the default embedded
// kitDriver; non-empty is a shell-style command line for an external ACP
// agent (acp-integration.md's --agent flag), and MCPURL/MCPToken are the
// workbench's own /mcp endpoint and bearer token that acpDriver hands the
// agent at session/new so it reaches the live session's tools.
type agentConfig struct {
	Model          string
	ProviderURL    string
	ProviderAPIKey string

	AgentCommand string
	MCPURL       string
	MCPToken     string
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
and sample_facts against what is there. set_schema REPLACES the human's whole
data-loading document; put_rule_group/delete_rule_group edit ONE rule group at
a time - call any of these only when the human explicitly asks you to change
the schema or rules, never to answer a question; sample_input is for
authoring too, not for lookups. Datalog is the reasoner: express joins and
filters in a single conjunctive query and let the engine unify, rather than
fetching predicates one at a time and correlating results yourself. Prefer
counts and samples over dumps. Every
change you make appears immediately in the panes the human is watching; keep
them informed of what you changed and why.`

// queryModeSystemPrompt frames Query Mode's conversational posture (doc/
// features/workbench-v2.md design decision 5): an investigation stance,
// read-only by construction (no write tool is even registered — see
// mcp.go's registerToolsForMode), so the prompt itself only needs to set
// tone and technique, not enumerate what the agent must not do.
const queryModeSystemPrompt = `You are the assistant embedded in a Datalog workbench, in QUERY MODE: a
read-only investigation session. The human is asking questions about what
the system currently knows — orient with list_predicates, then answer with
query, sample_facts, describe, predicate_deps, and explain/explain_fact.
Datalog is the reasoner: express joins and filters as a single conjunctive
query and let the engine unify, rather than fetching predicates one at a
time and correlating results yourself. When asked "why" a fact holds, use
explain_fact and recurse into its body facts rather than guessing. Prefer
counts and samples over dumps. You have no write tools in this mode —
if the human asks you to change rules or schema, say so and suggest they
start a Rules or Facts conversation instead.`

// rulesModeSystemPrompt is Query Mode's prompt plus rule-authoring guidance
// (design decision 5's "Rules Mode" bullet): the rule-group CRUD surface
// (put_rule_group/delete_rule_group) this mode registers on top of Query
// Mode's reads.
const rulesModeSystemPrompt = queryModeSystemPrompt + `

You are ALSO in RULES MODE: you may author Datalog rules, grouped by head
predicate/arity ("<head>/<arity>" rule groups, one per file on disk).
put_rule_group replaces one WHOLE group's text — every statement you submit
must share that one head predicate/arity, and the text you send lands
verbatim, so include any %% doc comments and existing statements you want
to keep. Each group carries a "revision" counter: get_rule_group or
list_rule_groups tells you the current revision, and put_rule_group/
delete_rule_group reject a stale revision (e.g. the human edited the file
in vim since you last read it) by handing back the current content instead
of overwriting it — re-read and retry rather than forcing the write.
Creating a NEW group applies immediately; editing or deleting an EXISTING
group is consent-gated (the human approves or denies before it lands).
There is no whole-ruleset replace tool and no assert-fact tool — every
fact must remain explainable back through the rules and the source data.`

// factsModeSystemPrompt is deliberately NOT layered on Query Mode's prompt
// (design decision 5: "the finer points of the datalog implementation are
// deliberately absent") — Facts Mode never sees query/explain/rule
// vocabulary at all, in the prompt or the tool surface, because it verifies
// JSONL extraction output without touching datalog semantics.
const factsModeSystemPrompt = `You are the assistant embedded in a Datalog workbench, in FACTS MODE: you
turn raw JSONL records into base facts by authoring extraction mappings —
sources (which file, which mappings apply to it), matchers (scan a
predicate's string term for a pattern and emit new facts), and
declarations (name a predicate's terms for readability; purely
documentation, no behavior). Use sample_input to see what the raw records
actually look like before you propose a mapping — never guess field names.
put_source/put_matcher/put_declaration create or replace ONE item each;
delete_source/delete_matcher/delete_declaration remove one. Creating a NEW
item applies immediately; editing or deleting an EXISTING item is
consent-gated (the human approves or denies before it lands). Each item
carries a "revision" counter the same way rule groups do — a stale write is
rejected with current content handed back, not silently overwritten.
After a change, use list_predicates and sample_facts to verify what your
mapping actually produced across the corpus. You have no query, explain, or
rule-authoring tools in this mode — that is deliberate: your job is
correct extraction, not reasoning over the results.`

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
	// sawMessage is written from kit's callback goroutine and read below
	// after PromptResult returns, on no guaranteed happens-before edge
	// between the two — atomic.Bool, not a plain bool, avoids the race.
	var sawMessage atomic.Bool
	unsubMsg := d.k.OnMessageUpdate(func(e kit.MessageUpdateEvent) {
		sawMessage.Store(true)
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
	if !sawMessage.Load() && res.Response != "" {
		sink(agentEvent{Kind: "message", Text: res.Response})
	}
	return res.StopReason, nil
}

// Answer always fails: kit has no permission/approval flow to surface (see
// the agentDriver interface doc and acp-integration.md's kit risk note), so
// it never emits a "permission" agentEvent for a pane Answer click to
// target.
func (d *kitDriver) Answer(requestID, optionID string) error {
	return fmt.Errorf("the embedded agent does not issue permission requests")
}

func (d *kitDriver) Close() error { return d.k.Close() }

// readOnlyTools is the workbench's own read-only MCP tool set (mcp.go): the
// tools that only ever read the session's schema/rules/facts back to the
// caller, never mutate them. set_schema and sample_input are deliberately
// excluded — they REPLACE the human's working documents (see mcp_docs.go's
// tool descriptions) and must keep prompting; put_rule_group/
// delete_rule_group are excluded for the same reason (design decision 5:
// editing or deleting an existing group is consent-gated). list_rule_groups
// and get_rule_group are pure reads (phase 1b, doc/features/workbench-v2.md
// work item 1) and join this set alongside query/sample_facts/
// list_predicates.
var readOnlyTools = map[string]bool{
	"query":            true,
	"sample_facts":     true,
	"list_predicates":  true,
	"list_rule_groups": true,
	"get_rule_group":   true,
}

// readOnlyToolName recognizes a permission request's tool-call title as one
// of the workbench's own read-only session tools (query, sample_facts,
// list_predicates — see readOnlyTools), returning the bare tool name and
// true when it does. This is the auto-allow policy's sole gate
// (doc/features/acp-integration.md's "Permission requests" bullet): the
// workbench's own tools were engineered for an untrusted caller (path
// confinement, expr-environment audit, limits, timeouts — observation 5),
// so a request recognizably gating one of them can be answered without the
// human. A generic ACP tool-kind of "read" is deliberately NOT consulted
// here — an external agent like Claude Code carries its own built-in tools,
// and its own "read" means reading the OPERATOR'S FILES, which must keep
// prompting; only titles that resolve to one of THIS package's own MCP tool
// names match.
//
// Adapters name MCP tools in a permission request's title in several
// observed shapes: the bare name ("query"), MCP-namespace-prefixed
// ("mcp__datalog__query", "datalog__query"), or colon/space/parenthesis-
// decorated ("datalog:query", "datalog - query (MCP)"). The extraction
// below normalizes case, strips a trailing parenthetical annotation (e.g.
// "(MCP)"), then takes the LAST colon- or whitespace-separated token and the
// part of THAT token following the last "__" separator (if any) or the last
// "-"-separated word — never a substring match, since a false positive here
// silently grants permission. A title that only happens to CONTAIN a
// read-only name (e.g. "Bash: psql query") does not match: after taking the
// last token ("query"), that token itself must equal a read-only name, but
// "psql query" splits into two tokens ("psql", "query") on whitespace, and
// only the trailing one ("query") is considered — so this case actually
// looks like it matches. To guard against that specific shape (a bare tool
// invocation embedded at the end of an unrelated title), and against a
// single-token dash-joined title like "db-query" or "web-query" that could
// just as easily be some OTHER agent's own built-in tool, the function
// requires that everything BEFORE the matched trailing token, once trimmed
// of separators, is one of a small explicit allowlist of namespace shapes
// this package's own tools actually appear under — see the switch below —
// rather than merely "no internal whitespace". "Bash: psql" and "db"/"web"
// are not on that list, so "Bash: psql query", "db-query", and "web-query"
// are all rejected; "datalog - query (MCP)" and "mcp__datalog__query" both
// pass because their folded prefixes ("datalog" and "mcp__datalog") are.
func readOnlyToolName(title string) (string, bool) {
	s := strings.TrimSpace(title)
	if s == "" {
		return "", false
	}
	s = strings.ToLower(s)

	// Strip one trailing parenthetical annotation, e.g. "(mcp)".
	if i := strings.LastIndex(s, "("); i >= 0 && strings.HasSuffix(s, ")") {
		s = strings.TrimSpace(s[:i])
	}
	if s == "" {
		return "", false
	}

	// Split on the last colon, if any: "datalog:query" -> prefix "datalog",
	// tail "query". A title with no colon has an empty prefix here.
	prefix := ""
	tail := s
	if i := strings.LastIndex(s, ":"); i >= 0 {
		prefix, tail = strings.TrimSpace(s[:i]), strings.TrimSpace(s[i+1:])
	}

	// Split the tail on whitespace and take the last field: "datalog -
	// query" (after stripping "(mcp)" and finding no colon) tails to
	// "datalog - query", whose last field is "query"; anything before that
	// field folds into prefix too.
	fields := strings.Fields(tail)
	if len(fields) == 0 {
		return "", false
	}
	last := fields[len(fields)-1]
	if len(fields) > 1 {
		if prefix != "" {
			prefix += " "
		}
		prefix += strings.Join(fields[:len(fields)-1], " ")
	}

	// Within the final field, take the part after the last "__" or "-"
	// separator (MCP-namespace and dash-joined prefixes), folding anything
	// before that separator into prefix as well.
	name := last
	if i := strings.LastIndex(name, "__"); i >= 0 {
		if prefix != "" {
			prefix += " "
		}
		prefix += name[:i]
		name = name[i+2:]
	} else if i := strings.LastIndex(name, "-"); i >= 0 {
		if prefix != "" {
			prefix += " "
		}
		prefix += name[:i]
		name = name[i+1:]
	}

	if !readOnlyTools[name] {
		return "", false
	}

	// The accumulated prefix must be one of the known namespace shapes this
	// package's own MCP tools actually appear under, not merely "one token
	// with no whitespace" — that looser rule let a single-token dash-joined
	// title like "db-query" or "web-query" through, which reads as some
	// OTHER agent's own tool (a plausible name for an external agent's
	// built-in), not this package's. A false positive here auto-grants
	// permission with no human in the loop, so the accepted set is an
	// explicit allowlist derived from how the extraction above actually
	// folds each legitimate shape: bare ("query" -> ""), MCP-namespaced
	// double-underscore ("datalog__query" -> "datalog",
	// "mcp__datalog__query" -> "mcp__datalog"), colon-prefixed
	// ("datalog:query" -> "datalog"), and dash/space-decorated
	// ("datalog - query (MCP)" -> "datalog", after the trailing "(MCP)" is
	// stripped and the "-" trimmed). "mcp datalog" is included too since an
	// adapter could plausibly render the mcp/datalog namespace with a space
	// instead of "__" even though none of the currently-observed shapes
	// produce it. Everything else — "db", "psql", "foo", "web", "bash psql",
	// "some other" — is rejected.
	prefix = strings.TrimSpace(strings.Trim(prefix, "-"))
	switch prefix {
	case "", "datalog", "mcp__datalog", "mcp datalog":
		return name, true
	default:
		return "", false
	}
}

// autoAllowOption picks the permission option the auto-allow policy answers
// with, per acp-integration.md's "Permission requests" bullet: prefer an
// option whose Kind is exactly allow_once; failing that, any Kind with the
// "allow" prefix (covers allow_always, and any future allow_* ACP adds).
// Returns ok = false when the request offers no allow option at all, so the
// caller falls through to normal button rendering rather than guessing.
func autoAllowOption(opts []agentOption) (agentOption, bool) {
	for _, o := range opts {
		if o.Kind == "allow_once" {
			return o, true
		}
	}
	for _, o := range opts {
		if strings.HasPrefix(o.Kind, "allow") {
			return o, true
		}
	}
	return agentOption{}, false
}

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
	if wb.agentCfg.AgentCommand != "" {
		d, err := newACPDriver(wb.agentCfg)
		if err != nil {
			return nil, err
		}
		wb.agent = d
		return d, nil
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
		planID     uint64                // "plan" entry, updated in place per acp-integration.md phase 2

		// gotReply records whether any message chunk arrived this turn,
		// independent of msgID: msgID resets to 0 whenever a tool call,
		// permission, or other appended entry interrupts the streaming
		// message (breakStreaming), so "msgID != 0" can no longer stand in
		// for "the model composed a reply" the way it once could when a
		// turn's whole message text accumulated into a single entry that
		// never reset.
		gotReply bool
	)

	// breakStreaming ends whatever message/thought entry is currently
	// accumulating so the NEXT chunk of either kind starts a fresh entry at
	// the bottom of the transcript, rather than morphing something that is
	// no longer adjacent to it. This is the fix for the interleaving bug
	// observed live against a real Claude Code ACP agent
	// (doc/features/acp-integration.md): without it, every "message" chunk
	// for the whole turn morphs the SAME entry created by the first chunk,
	// so an agent that emits text, then a tool call, then more text renders
	// with all the text pooled at the top and the tool call stuck
	// underneath — reading out of chronological order. The rule: any event
	// that APPENDS a new entry (tool-call, permission — including the
	// auto-allowed note, the plan's first append, error) ends the current
	// streaming message/thought; events that morph an existing entry in
	// place (tool-result, a plan UPDATE) do not, since nothing new was
	// inserted between the streaming entry and wherever the next chunk
	// would land. A thought interrupting a message ends the message entry,
	// and vice versa — each kind's accumulator resets when a DIFFERENT kind
	// appends, not just any non-matching kind.
	breakStreaming := func() {
		msgID = 0
		msgText.Reset()
		thoughtID = 0
		thoughtBuf.Reset()
	}

	sink := func(ev agentEvent) {
		mu.Lock()
		defer mu.Unlock()
		switch ev.Kind {
		case "message":
			thoughtID = 0
			thoughtBuf.Reset()
			msgText.WriteString(ev.Text)
			if msgID == 0 {
				gotReply = true
				msgID = wb.consoleAppend("agent", "agent", html.Text(msgText.String()))
			} else {
				wb.consoleUpdate(msgID, "agent", html.Text(msgText.String()))
			}
		case "thought":
			msgID = 0
			msgText.Reset()
			thoughtBuf.WriteString(ev.Text)
			body := thoughtEntry(thoughtBuf.String())
			if thoughtID == 0 {
				thoughtID = wb.consoleAppend("agent", "thought", body)
			} else {
				wb.consoleUpdate(thoughtID, "thought", body)
			}
		case "tool-call":
			breakStreaming()
			id := wb.consoleAppend("agent", "tool", toolEntry(ev, false))
			toolIDs[ev.ToolCallID] = id
		case "tool-result":
			// A morph in place, never an append — does not break the
			// streaming accumulators (see breakStreaming's doc comment). The
			// "unknown tool call" fallback below DOES append, so it breaks
			// them like any other append.
			if id, ok := toolIDs[ev.ToolCallID]; ok {
				wb.consoleUpdate(id, "tool", toolEntry(ev, true))
			} else {
				breakStreaming()
				wb.consoleAppend("agent", "tool", toolEntry(ev, true))
			}
		case "error":
			breakStreaming()
			wb.consoleAppend("agent", "error", html.Text(ev.Text))
		case "permission":
			// Auto-allow policy (doc/features/acp-integration.md's
			// "Permission requests" bullet): a request recognizably gating
			// one of the workbench's own read-only session tools (query,
			// sample_facts, list_predicates — readOnlyToolName) is answered
			// immediately, without a human click, because those tools were
			// already engineered for an untrusted caller (observation 5).
			// Everything else — mutating workbench tools, and any tool
			// belonging to an external agent's OWN built-ins (e.g. Claude
			// Code's file reads, which this package cannot and must not
			// distinguish from a legitimate ACP "read" kind) — still gets
			// the interactive rendering below. Answer is called
			// synchronously right here in the sink: the agentDriver
			// interface's doc comment on Answer notes it sends on a
			// buffered channel, so this cannot deadlock against the very
			// Prompt call whose goroutine is running this sink. A driver
			// error (a raced turn teardown — e.g. the turn ended between
			// the event firing and this call) falls through to the normal
			// buttons path so the request is never silently dropped.
			//
			// Both branches below append a new entry — the auto-allowed note,
			// or the interactive buttons — never a morph, so the streaming
			// accumulators break once here regardless of which branch is taken.
			breakStreaming()
			if _, ok := readOnlyToolName(ev.ToolName); ok {
				if opt, ok := autoAllowOption(ev.Options); ok {
					if err := driver.Answer(ev.RequestID, opt.ID); err == nil {
						wb.consoleAppend("agent", "note",
							html.Text("auto-allowed: "+permissionSummary(ev)))
						return
					}
				}
			}
			// Interactive rendering (acp-integration.md work item 9): one
			// entry with a live button per option. The entry id is tracked
			// under wb.permMu — NOT the local toolIDs map above — because
			// the click that resolves this request arrives at
			// handleConsoleAnswer on its own HTTP goroutine, outside this
			// sink closure entirely; toolIDs would simply be unreachable
			// from there. "A turn blocked on permission shows that state
			// plainly": the entry's own phrasing carries that, so no
			// second spinner is added alongside agentActivity.
			id := wb.consoleAppend("agent", "permission", permissionEntry(ev))
			wb.permMu.Lock()
			wb.pendingPerm[ev.RequestID] = pendingPermission{entryID: id, event: ev}
			wb.permMu.Unlock()
		case "plan":
			body := planEntry(ev.PlanEntries)
			if planID == 0 {
				// The plan's FIRST update is an append (a new entry lands in
				// the transcript), so — like any other append — it breaks
				// the streaming accumulators. Every later plan update morphs
				// that same entry in place (ACP's plan/update always
				// carries the complete list, per agentPlanEntry's doc
				// comment) and must NOT break them.
				breakStreaming()
				planID = wb.consoleAppend("agent", "plan", body)
			} else {
				wb.consoleUpdate(planID, "plan", body)
			}
		}
	}

	stopReason, err := driver.Prompt(ctx, text, sink)
	mu.Lock()
	reply := gotReply
	mu.Unlock()
	switch {
	case ctx.Err() != nil:
		wb.consoleAppend("agent", "note", html.Text("turn cancelled"))
	case err != nil:
		wb.consoleAppend("agent", "error", html.Text(fmt.Sprintf("turn failed: %v", err)))
		wb.dropAgentDriver(driver)
	case !reply:
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

	// A cancelled or crashed turn can leave permission requests the driver
	// never resolves an answer for — the ACP request itself is abandoned
	// with the subprocess/turn, but the pane's buttons would otherwise sit
	// there forever, dead (clicking Answer on a requestID the driver no
	// longer recognizes). Morph every entry still in wb.pendingPerm to a
	// cancelled state and clear the map so no stale requestID survives into
	// the next turn (acp-integration.md work item 9: "cancelled turn
	// resolves pending permissions driver-side; morph any unresolved
	// permission entries to a cancelled state").
	wb.permMu.Lock()
	stale := wb.pendingPerm
	wb.pendingPerm = map[string]pendingPermission{}
	wb.permMu.Unlock()
	for _, p := range stale {
		ev := p.event
		wb.consoleUpdate(p.entryID, "permission",
			permissionResolvedEntry(&ev, "cancelled: turn ended before the agent received an answer"))
	}
}

// permissionSummary renders the tool call a permission request gates —
// "agent is waiting for permission: <tool> <args>" — shared by the pending
// (with buttons) and resolved (without) renderings so the phrasing that
// makes a blocked turn "show that state plainly" (acp-integration.md "Chat
// pane") survives the morph to resolved/cancelled.
func permissionSummary(ev agentEvent) string {
	var b strings.Builder
	b.WriteString(strings.TrimPrefix(ev.ToolName, "datalog__"))
	if ev.ToolArgs != "" {
		compact, _ := compactArgs(strings.TrimSpace(ev.ToolArgs))
		b.WriteString(" ")
		b.WriteString(compact)
	}
	return b.String()
}

// permissionEntry renders one pending permission request: the phrasing
// "agent is waiting for permission: ..." makes the blocked state plain
// (acp-integration.md "Chat pane"), and one inline button per option —
// posting RequestID/OptionID to POST /console/answer (handleConsoleAnswer,
// console.go) — lets the operator resolve it without a second round-trip
// through the tool-call entry. Resolving morphs the entry to
// permissionResolvedEntry instead, which drops the buttons.
func permissionEntry(ev agentEvent) html.Content {
	summary := permissionSummary(ev)
	buttons := make(html.Group, 0, len(ev.Options))
	for _, opt := range ev.Options {
		class := "permission-option"
		if strings.HasPrefix(opt.Kind, "reject") {
			class += " reject"
		} else {
			class += " allow"
		}
		buttons = append(buttons, tag.New("button."+class).
			Set("data-on:click", "@post('/console/answer?requestID="+urlQueryEscape(ev.RequestID)+
				"&optionID="+urlQueryEscape(opt.ID)+"')").
			Add(html.Text(opt.Name)))
	}
	return html.Group{
		tag.New("p.permission-line",
			html.Text("agent is waiting for permission: "+summary)),
		tag.New("div.permission-options", buttons),
	}
}

// permissionResolvedEntry renders a request after it stops being pending —
// either Answer succeeded (note names the chosen option) or the turn ended
// with it still unanswered (runAgentTurn's cleanup, driver-side
// cancellation per acp-integration.md work item 9). Both callers hold the
// original event via wb.pendingPerm, so the resolved line still names the
// tool it gated; ev is a pointer only to tolerate a future caller without
// one, falling back to a generic subject.
func permissionResolvedEntry(ev *agentEvent, note string) html.Content {
	subject := "a pending permission request"
	if ev != nil {
		subject = "permission for " + permissionSummary(*ev)
	}
	return tag.New("p.permission-line", html.Text(subject+" — "+note))
}

// urlQueryEscape escapes a value for inclusion in a query string built by
// string concatenation, matching the idiom view/console.go's clearButton
// already uses for ?tab=. RequestID/OptionID are driver-generated tokens,
// not free text, but escaping costs nothing and removes any assumption
// about their alphabet.
func urlQueryEscape(s string) string {
	return url.QueryEscape(s)
}

// planMark renders one plan entry's status as a checklist glyph — a real
// widget (acp-integration.md work item 9) in place of the phase-1 [ ]/[x]/[~]
// text markers.
func planMark(status string) string {
	switch status {
	case "completed":
		return "☑"
	case "in_progress":
		return "◐"
	default:
		return "☐"
	}
}

// planEntry renders the agent's plan as a checklist block: one line per
// entry with its status glyph, replaced wholesale and morphed in place each
// time the agent sends a new plan (ACP's plan/update always carries the
// complete list — see agentPlanEntry's doc comment).
func planEntry(entries []agentPlanEntry) html.Content {
	lines := make(html.Group, 0, len(entries))
	for _, e := range entries {
		class := "plan-line"
		if e.Status != "" {
			class += " " + strings.ReplaceAll(e.Status, "_", "-")
		}
		lines = append(lines, tag.New("li."+class,
			tag.New("span.plan-mark", html.Text(planMark(e.Status))),
			html.Text(" "+e.Content),
		))
	}
	return tag.New("ul.plan-checklist", lines)
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
// arguments, the result, or the error. The status span shows a red ✕ badge
// on error, so a failed call is visible without expanding. Result text is already
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
	// oat's summary is a flex row (space-between, trailing chevron ::after),
	// so source order is layout order: code takes the flexible middle and
	// the status icon rides the right edge beside the chevron.
	return tag.New("details",
		tag.New("summary.tool-line",
			tag.New("code", html.Text(compact)),
			toolStatus(ev, done),
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
// Editor's error-list styling. The message is unwrapped from whatever JSON
// envelope the transport put around it first — the operator reads prose,
// not payloads.
func toolError(text string) html.Content {
	return tag.New("ul.errors", tag.New("li", html.Text(toolErrorText(text))))
}

// toolErrorText digs the human-readable message out of a failed call's
// result. Providers and transports wrap tool errors in JSON envelopes (MCP
// content blocks, {"error": ...} objects, bare JSON strings); showing the
// envelope buries the one line the operator needs. Anything unrecognized
// falls back to formatToolResult so no information is ever dropped.
func toolErrorText(s string) string {
	trimmed := strings.TrimSpace(s)
	if trimmed == "" || (trimmed[0] != '{' && trimmed[0] != '[' && trimmed[0] != '"') {
		return s // already prose
	}
	if msg := jsonErrorMessage([]byte(trimmed)); msg != "" {
		return msg
	}
	return formatToolResult(s)
}

// jsonErrorMessage extracts a message from the JSON error shapes seen in
// practice, returning "" when the document matches none of them.
func jsonErrorMessage(b []byte) string {
	var str string
	if json.Unmarshal(b, &str) == nil {
		return str
	}

	type textBlock struct {
		Text string `json:"text"`
	}
	joinBlocks := func(blocks []textBlock) string {
		parts := make([]string, 0, len(blocks))
		for _, blk := range blocks {
			if t := strings.TrimSpace(blk.Text); t != "" {
				parts = append(parts, t)
			}
		}
		return strings.Join(parts, "\n")
	}

	var blocks []textBlock
	if json.Unmarshal(b, &blocks) == nil {
		return joinBlocks(blocks)
	}

	var env struct {
		Content []textBlock     `json:"content"`
		Error   json.RawMessage `json:"error"`
		Message string          `json:"message"`
	}
	if json.Unmarshal(b, &env) != nil {
		return ""
	}
	if msg := joinBlocks(env.Content); msg != "" {
		return msg
	}
	if len(env.Error) > 0 {
		if msg := jsonErrorMessage(env.Error); msg != "" {
			return msg
		}
	}
	return env.Message
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

// toolStatus marks the summary line only when there is something to say —
// with an icon, not a word: a red ✕ badge (drawn by .tool-status.error's
// ::before) on failure, nothing otherwise. No per-call spinner: an
// in-flight call is the newest entry, sitting directly above the chat
// pane's single running indicator (view.Console's agentActivity), and a
// second ring there just multiplied the tells. The title keeps the meaning
// reachable by hover and assistive tech.
func toolStatus(ev agentEvent, done bool) html.Content {
	if done && ev.IsError {
		return tag.New("span.tool-status.error").
			Set("title", "tool call failed")
	}
	return html.Group{}
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
