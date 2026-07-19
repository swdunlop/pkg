package main

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	stdflag "flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"sync"

	"github.com/mark3labs/mcp-go/server"
	"github.com/swdunlop/html-go/datastar"
	"swdunlop.dev/pkg/datalog/cmd/datalog/view"
)

// runServe implements the `datalog serve` subcommand: a hypermedia
// workbench over the same session/mcpHandlers pipeline runMCP exposes over
// stdio (doc/features/web-ui.md design constraint 1 — one pipeline, N
// frontends). args excludes the "serve" argument itself (main.go strips it
// before dispatching here).
//
// serve owns its own flag.FlagSet for the same reason runMCP does:
// registering flags globally would make bare mode and serve fight over
// -c/-d parsing depending on dispatch order.
func runServe(args []string) {
	flags := stdflag.NewFlagSet("serve", stdflag.ExitOnError)
	dataDir := flags.String("d", "", "data directory or .zip file (required; the security boundary for all file access)")
	configPath := flags.String("c", "", "path to a JSON or YAML jsonfacts config file to preload")
	rulesDir := flags.String("rules", "", "path to a rules/ directory store (one <head>_<arity>.dl file per rule group); mutually exclusive with positional rule files")
	listen := flags.String("listen", "127.0.0.1:8080", "address to listen on")
	mcpToken := flags.String("mcp-token", "", "bearer token required on /mcp (default: generate one and print it to stderr)")
	model := flags.String("model", "", "embedded agent model, kit-style (e.g. anthropic/claude-sonnet-5, openai/<alias>); empty defers to KIT_MODEL / ~/.kit.yml")
	providerURL := flags.String("provider-url", "", "override the agent model provider's base URL (e.g. an OpenAI-compatible llama-swap endpoint)")
	providerKey := flags.String("provider-api-key", "", "override the agent model provider's API key; empty defers to provider env vars")
	agentCmd := flags.String("agent", "", "external ACP agent command, split shell-style (e.g. 'npx @agentclientprotocol/claude-agent-acp'); empty uses the embedded kit agent")
	if err := flags.Parse(args); err != nil {
		// flag.ExitOnError already printed usage and exited on real errors;
		// this only returns for -h/-help.
		os.Exit(0)
	}

	if *dataDir == "" {
		fmt.Fprintln(os.Stderr, "datalog serve: -d <data directory or .zip> is required")
		os.Exit(1)
	}

	// Flags are parsed before positionals (stdlib flag.FlagSet stops at the
	// first non-flag argument), so flags.Args() here is exactly the rules
	// files given on the command line, in order. --rules and
	// positional rule files are mutually exclusive (doc/features/
	// workbench-v2.md work item 1): the directory store and the legacy
	// monolithic-file(s) path both set session.rulesText, and there is no
	// sensible way to merge "load this directory" with "also load these
	// specific files" without inventing an ordering the store doesn't have.
	ruleFiles := flags.Args()

	if err := rulesSourceConflict(*rulesDir, ruleFiles); err != nil {
		fmt.Fprintf(os.Stderr, "datalog serve: %v\n", err)
		os.Exit(1)
	}

	wb, closeFn, err := newWorkbench(*dataDir, *configPath, ruleFiles, *rulesDir, *mcpToken,
		agentConfig{
			Model:          *model,
			ProviderURL:    *providerURL,
			ProviderAPIKey: *providerKey,
			AgentCommand:   *agentCmd,
			MCPURL:         mcpURL(*listen),
		})
	if err != nil {
		fmt.Fprintf(os.Stderr, "datalog serve: %v\n", err)
		os.Exit(1)
	}
	defer closeFn()

	// Disk is canonical: watch the schema file and rules/ directory so a
	// vim save (or an agent write's echo) reloads, re-evaluates, and
	// repaints (watch.go; doc/features/workbench-v2.md design decision 3).
	// Fatal on error: an operator who started serve with -c/--rules is
	// relying on saves being seen — a silently dead watcher would bring
	// back exactly the stale-workbench bug this feature exists to fix.
	stopWatcher, err := wb.startWatcher()
	if err != nil {
		fmt.Fprintf(os.Stderr, "datalog serve: %v\n", err)
		os.Exit(1)
	}
	defer stopWatcher()

	fmt.Fprintf(os.Stderr, "datalog serve: /mcp bearer token: %s\n", wb.mcpToken)

	mux := http.NewServeMux()
	wb.routes(mux)
	wb.mountMCP(mux)

	srv := &http.Server{Addr: *listen, Handler: mux}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	fmt.Fprintf(os.Stderr, "datalog serve: listening on http://%s\n", *listen)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		fmt.Fprintf(os.Stderr, "datalog serve: %v\n", err)
		os.Exit(1)
	}
}

// newWorkbench builds a *workbench wired exactly like runServe does: it
// opens the data source, preloads schema/rules, generates (or accepts) the
// /mcp bearer token, builds the shared MCP server value, and wires the
// onChange patch-back seam — everything runServe needs except the flag
// parsing and the HTTP listener itself. Factored out so tests can build a
// workbench against a temp directory or the mordor example without going
// through flag parsing or os.Exit calls. tokenFlag is the --mcp-token
// flag's value; empty means "generate one". rulesDir is the --rules
// directory-store path (empty means "use ruleFiles instead", the legacy
// monolithic path) — runServe has already rejected the case where both are
// given, mirroring runMCP's identical check, so newWorkbench itself does
// not re-validate that here (newMCPHandlers just prefers rulesDir when set).
func newWorkbench(dataDir, configPath string, ruleFiles []string, rulesDir string, tokenFlag string, agentCfg agentConfig) (*workbench, func() error, error) {
	h, closeFn, err := newMCPHandlers(dataDir, configPath, ruleFiles, rulesDir, evalTimeout)
	if err != nil {
		return nil, nil, err
	}

	token := tokenFlag
	if token == "" {
		token, err = generateToken()
		if err != nil {
			closeFn()
			return nil, nil, fmt.Errorf("generating /mcp bearer token: %w", err)
		}
	}
	// The token is only known once resolved above, but acpDriver needs it
	// alongside MCPURL (set by runServe from --listen) to hand the agent at
	// session/new — agentCfg is otherwise fully populated by the caller.
	agentCfg.MCPToken = token

	// One MCP server value serves both consumers: mounted at /mcp for
	// external agents (mountMCP) and registered in-process with the
	// embedded kit agent (newKitDriver) — the "one pipeline, N frontends"
	// rule extended to the tool surface itself.
	srv := server.NewMCPServer("datalog", "0.1.0",
		server.WithInstructions(mcpServerInstructions),
		// WithRecovery: this one server value backs both the /mcp mount and
		// the in-process kit agent, so a tool-handler panic must surface as
		// a tool error on either path rather than a dropped request (the
		// stdio server in mcp.go carries the same option for the same
		// reason).
		server.WithRecovery(),
	)
	h.registerTools(srv)

	wb := &workbench{
		h:           h,
		bus:         newBus(),
		jobs:        newJobs(),
		console:     &consoleLog{},
		mcpSrv:      srv,
		agentCfg:    agentCfg,
		mcpToken:    token,
		pendingPerm: map[string]pendingPermission{},
		pendingCmds: map[string][]commandRecord{},
		reloadSeen:  map[string]int{},
	}
	wb.turnGate = newConversationTurnGate(wb.jobs)

	// Conversation manager (doc/features/workbench-v2.md design decision 6):
	// rooted at .datalog/sessions beside the schema file (or the rules
	// directory, or cwd — see conversationSessionsDir). Phase 2a wires the
	// manager onto the workbench so it is reachable and testable end to
	// end; no route yet calls it (this task's brief: "do NOT rebuild any
	// UI" — the v1 Agent tab is untouched). A failure here is non-fatal and
	// logged rather than aborting startup: it would only block the
	// not-yet-built conversation UI, and the v1 tab must keep working
	// regardless.
	if convDir, err := conversationSessionsDir(configPath, rulesDir); err != nil {
		fmt.Fprintf(os.Stderr, "datalog serve: conversation directory: %v\n", err)
	} else if cm, err := newConversationManager(convDir); err != nil {
		fmt.Fprintf(os.Stderr, "datalog serve: conversation directory: %v\n", err)
	} else {
		wb.conversations = cm
	}

	// The patch-back seam (doc/features/web-ui.md Deployment section): an
	// agent mutating via /mcp must repaint the human's browser. onChange
	// fires from inside setSchema/setRules while h.mu is still held, matching
	// publishSessionChanged's documented contract (fact_browser.go).
	// runMCP (stdio `datalog mcp`) never sets this field, so stdio behavior
	// is unchanged.
	h.onChange = wb.publishSessionChanged

	// Evaluate the preloaded ruleset once at startup, mirroring what Run
	// does (rules_editor.go handleRulesRun): the -c schema is already
	// applied by newMCPHandlers (setSchema loads the data), but nothing
	// populates session.derivedDB until someone presses Run — and the
	// agent has no Run button, so without this it would see every derived
	// predicate as 0 facts until the human remembered to click. Errors
	// are non-fatal: parse errors were already fatal in newMCPHandlers,
	// and a compile/timeout problem here is fixable in the running
	// editor, so warn and serve rather than refuse to start.
	if len(h.sess.rules)+len(h.sess.aggRules) > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), evalTimeout)
		evalErr := <-runRecovered(func() error {
			h.mu.Lock()
			defer h.mu.Unlock()
			db, prov, err := h.sess.evaluate(ctx)
			if err != nil {
				return err
			}
			if err := checkFactCap(db); err != nil {
				return err
			}
			// prov rides beside db under the same generation guard every
			// other derivedDB writer uses (doc/features/provenance.md
			// "Session cache interaction") — here there is no concurrent
			// mutation possible (h.mu has been held since before evaluate
			// started), so the cache is unconditional, unlike Run/query's
			// snapGen-checked writes.
			h.sess.derivedDB = db
			h.sess.derivedProv = prov
			return nil
		})
		cancel()
		if evalErr != nil {
			fmt.Fprintf(os.Stderr, "datalog serve: initial rule evaluation: %v\n", evalErr)
		}
	}

	return wb, closeFn, nil
}

// mcpURL derives the /mcp URL an acpDriver hands its agent from the
// --listen flag's value. listen is frequently host-less (the default
// "127.0.0.1:8080", or an operator-given ":8080") — net.SplitHostPort
// handles both, and a host-less OR wildcard-bind host (0.0.0.0, ::) becomes
// 127.0.0.1: the ACP agent is a local subprocess of this same host, per
// acp-integration.md's loopback-only posture, and a wildcard bind address
// means "accept on every interface," not an address that can itself be
// dialed (some platforms refuse to dial 0.0.0.0 at all).
func mcpURL(listen string) string {
	host, port, err := net.SplitHostPort(listen)
	if err != nil {
		// Not a valid host:port at all (shouldn't happen — ListenAndServe
		// would fail the same way); still recover the real port rather than
		// silently pointing the agent at the flag's default 8080 — e.g. a
		// bare port number ("9090") fails SplitHostPort but names the
		// intended port plainly.
		port = defaultListenPort
		if _, perr := strconv.Atoi(listen); perr == nil {
			port = listen
		}
		return fmt.Sprintf("http://127.0.0.1:%s/mcp", port)
	}
	switch host {
	case "", "0.0.0.0", "::":
		host = "127.0.0.1"
	}
	// JoinHostPort rather than Sprintf: IPv6 literal hosts (e.g. a --listen
	// of "[::1]:8080") must be re-bracketed or the URL fails to parse.
	return fmt.Sprintf("http://%s/mcp", net.JoinHostPort(host, port))
}

// defaultListenPort mirrors --listen's flag default ("127.0.0.1:8080")
// port, used only as mcpURL's last-resort fallback when listen doesn't
// parse as host:port at all.
const defaultListenPort = "8080"

// generateToken returns 32 hex characters (16 random bytes) from
// crypto/rand, used as the /mcp bearer token when --mcp-token is not given.
func generateToken() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	const hexDigits = "0123456789abcdef"
	out := make([]byte, len(buf)*2)
	for i, b := range buf {
		out[i*2] = hexDigits[b>>4]
		out[i*2+1] = hexDigits[b&0xf]
	}
	return string(out), nil
}

// mountMCP mounts the same MCP tool surface the stdio `datalog mcp`
// subcommand exposes at /mcp, using mcp-go's streamable HTTP server
// (doc/features/web-ui.md Deployment section). It shares the exact same
// *mcpHandlers (and thus mutex + session) the panes use, via
// h.registerTools — an agent calling put_rule_group over /mcp and a human
// clicking Run in the browser are the same operation on the same session.
//
// mcp-go's server.NewStreamableHTTPServer implements http.Handler directly
// (ServeHTTP), so mounting it is a plain mux.Handle call; no adapter shim was
// needed. Bearer-token enforcement wraps that handler: Authorization: Bearer
// <token> or 401, checked with crypto/subtle.ConstantTimeCompare so token
// comparison isn't timing-observable.
func (wb *workbench) mountMCP(mux *http.ServeMux) {
	// WithStateLess: the workbench is single-user/single-session (design
	// constraint 3), so there is no benefit to the streamable transport's
	// stateful session bookkeeping (an Mcp-Session-Id the client must carry
	// across calls) — every request already shares the one *mcpHandlers/
	// session this server was built around, regardless of transport-level
	// session identity.
	streamable := server.NewStreamableHTTPServer(wb.mcpSrv, server.WithStateLess(true))
	mux.Handle("/mcp", wb.requireBearerToken(streamable))
}

// requireBearerToken wraps next, rejecting any request whose
// Authorization header isn't exactly "Bearer <wb.mcpToken>" with 401.
// Comparison is constant-time (crypto/subtle) to avoid leaking the token
// through response-time side channels.
func (wb *workbench) requireBearerToken(next http.Handler) http.Handler {
	const prefix = "Bearer "
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		ok := len(auth) == len(prefix)+len(wb.mcpToken) &&
			subtle.ConstantTimeCompare([]byte(auth[:len(prefix)]), []byte(prefix)) == 1 &&
			subtle.ConstantTimeCompare([]byte(auth[len(prefix):]), []byte(wb.mcpToken)) == 1
		if !ok {
			w.Header().Set("WWW-Authenticate", `Bearer realm="datalog serve /mcp"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// workbench holds the shared state behind every HTTP handler: the same
// mcpHandlers the MCP tool surface calls (so a human's Apply/Run and an
// agent's set_schema/put_rule_group/query are the same operation), the SSE bus
// for Transform-completed fan-out, and the job set backing Global Cancel.
// Mutating handlers go through h's typed methods under h.mu (the same mutex
// the MCP tools use); reads may use session state under the same mutex for
// now — the design's snapshot-pointer optimization (design constraint 2) can
// layer in later without changing this struct's shape.
type workbench struct {
	h    *mcpHandlers
	bus  *bus
	jobs *jobs

	// busyMu guards busyKey (the current $busy value as of the last
	// publishBusy call) and busyConvID/busyConvName (WHICH conversation
	// owns a running "agent" turn, riding the same signal batch) —
	// handleEvents replays them to a freshly connecting SSE subscription
	// (see publishBusy's doc comment) so a tab opened mid-job can still
	// see and stop the job, instead of showing idle (and offering no Stop
	// control) until the job happens to end.
	busyMu       sync.Mutex
	busyKey      string
	busyConvID   string
	busyConvName string

	// console is the drawer's server-owned scrollback (console.go); mcpSrv
	// is the shared MCP server value built in newWorkbench, consumed by
	// both mountMCP (streamable HTTP at /mcp) and the embedded kit agent
	// (in-process registration).
	console *consoleLog
	mcpSrv  *server.MCPServer

	// agentMu guards the lazily-constructed agent driver (agent.go) and
	// agentConvID, the conversation it is currently bound to
	// (conversations_http.go's conversationDriver — one live agent at a
	// time, switched when the human sends in a different conversation).
	// agentCfg is operator-trusted flag input, immutable after startup.
	agentMu     sync.Mutex
	agent       agentDriver
	agentConvID string
	agentCfg    agentConfig

	// conversations is the phase-2 conversation manager (conversation.go,
	// doc/features/workbench-v2.md design decision 6), nil only if its
	// directory could not be created at startup (see newWorkbench). turnGate
	// is the global one-turn-at-a-time gate every conversation's prompt path
	// acquires before running, sharing jobs (the SAME *jobs the v1 Agent tab
	// and the fsnotify watcher's re-evaluation register under, so Global
	// Cancel reaches a conversation turn too) so its "busy" state rides the
	// existing $busy machinery rather than a parallel mechanism.
	conversations *conversationManager
	turnGate      *conversationTurnGate

	// permMu guards pendingPerm, the RequestID→pendingPermission map a
	// running turn's sink populates (agent.go's runAgentTurn) and
	// handleConsoleAnswer (console.go) consumes. It is a separate map from
	// agent.go's local toolIDs because Answer arrives over HTTP on its own
	// goroutine, outside the turn's sink closure — this is the one piece of
	// per-turn state that must be reachable from a handler, so it lives on
	// the workbench instead of a local in runAgentTurn. Cleared entry-by-
	// entry as each request resolves, and wholesale when the turn ends
	// (acp-integration.md work item 9: "cancelled turn resolves pending
	// permissions driver-side; morph any unresolved permission entries to a
	// cancelled state").
	permMu      sync.Mutex
	pendingPerm map[string]pendingPermission

	// lastReload is the persistent reload-status surface (doc/features/
	// workbench-v2.md design decision 3: "Parse/compile errors land in a
	// persistent status surface and are visible to the next agent turn"):
	// the fsnotify watcher's most recent reload outcome — what changed, or
	// why the reload was refused and the last good state kept. Recorded
	// under reloadMu, NOT h.mu — status reads must not contend with the
	// session mutex. reloadSeq counts recordReload calls so the agent-turn
	// preamble (commands_composer.go's consumePreamble) can tell "changed
	// since this conversation's last turn" from "ever changed."
	reloadMu   sync.Mutex
	lastReload reloadStatus
	reloadSeq  int

	// cmdMu guards the composer-command bookkeeping (commands_composer.go):
	// pendingCmds holds each conversation's command/result pairs run since
	// its last turn (consumed into the next prompt's preamble), and
	// reloadSeen holds each conversation's reload watermark (the reloadSeq
	// value as of its last turn).
	cmdMu       sync.Mutex
	pendingCmds map[string][]commandRecord
	reloadSeen  map[string]int

	// mcpToken is the bearer token required on /mcp (doc/features/web-ui.md
	// Deployment section). Generated at startup if --mcp-token was not
	// given; see generateToken.
	mcpToken string

	// selMu guards the jsonfacts Editor's evaluation-target selection below.
	// This is a separate mutex from h.mu (the session mutex): selecting a
	// row is not a session mutation, and holding h.mu across a Data Browser
	// request would serialize pure reads against the mutation pipeline for
	// no reason.
	selMu     sync.Mutex
	selFile   string // source file the selected row came from
	selRow    int    // 0-based row (line) index within selFile
	selRecord string // raw JSONL line of the selected record
	selValid  bool   // whether a selection has been made yet
}

// currentSelection reads the jsonfacts Editor's evaluation-target selection
// under selMu, for handlers (like the Data Browser's row rendering) that
// only need to know which row to highlight, not its raw content.
func (wb *workbench) currentSelection() (file string, row int, valid bool) {
	wb.selMu.Lock()
	defer wb.selMu.Unlock()
	return wb.selFile, wb.selRow, wb.selValid
}

// routes registers the full route table on mux using Go 1.22+ method+
// pattern syntax. Pane endpoints are stubs for now (later waves fill them
// in, one file per pane so parallel agents never touch this file); the
// full-page shell, static CSS, the /events subscription skeleton, and
// Global Cancel are implemented completely in this wave.
func (wb *workbench) routes(mux *http.ServeMux) {
	// Conversation UI (conversations_http.go; doc/features/workbench-v2.md
	// phase 2): GET / lands on the newest conversation, /c/{id} is one
	// conversation's page, create/delete are plain form POST+redirect.
	mux.HandleFunc("GET /{$}", wb.handleRoot)
	mux.HandleFunc("GET /c/{id}", wb.handleConversationPage)
	mux.HandleFunc("POST /conversations", wb.handleConversationCreate)
	mux.HandleFunc("POST /c/{id}/delete", wb.handleConversationDelete)
	mux.HandleFunc("POST /c/{id}/send", wb.handleConversationSend)
	mux.HandleFunc("POST /answer", wb.handleAnswer)

	mux.HandleFunc("GET /oat.css", wb.handleOatCSS)
	mux.HandleFunc("GET /workbench.css", wb.handleWorkbenchCSS)
	mux.HandleFunc("GET /events", wb.handleEvents)

	// Browser: Data tab (data_browser.go).
	mux.HandleFunc("GET /data", wb.handleDataList)
	mux.HandleFunc("GET /data/{file}", wb.handleDataFile)
	mux.HandleFunc("GET /data/select/{file}/{row}", wb.handleDataSelect)

	// Browser: Facts tab (fact_browser.go), including the why? affordance
	// (doc/features/provenance.md), which renders into the tab's
	// #why-output surface.
	mux.HandleFunc("GET /facts/{predicate}/{arity}", wb.handleFacts)
	mux.HandleFunc("POST /why/{predicate}/{arity}", wb.handleWhy)

	// Global Cancel (doc/features/web-ui.md "Execution sandbox").
	mux.HandleFunc("POST /cancel", wb.handleCancel)
}

// handleOatCSS serves the embedded, self-hosted oat.css base.
func (wb *workbench) handleOatCSS(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/css; charset=utf-8")
	_, _ = w.Write(view.OatCSS)
}

// handleWorkbenchCSS serves the workbench's own chrome layer, linked after
// oat.css in the page head (see view/page.go's workbenchTag).
func (wb *workbench) handleWorkbenchCSS(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/css; charset=utf-8")
	_, _ = w.Write(view.WorkbenchCSS)
}

// handleCancel is the Global Cancel emergency brake: fires every in-flight
// job's CancelFunc. Single-user makes the blunt instrument acceptable —
// see jobs.CancelAll's doc. There is no dedicated Stop button anymore:
// whichever action button started the work (Run/Apply/Send) morphs into
// Stop while its job holds the $busy mutex and posts here.
func (wb *workbench) handleCancel(w http.ResponseWriter, r *http.Request) {
	wb.jobs.CancelAll()
	w.WriteHeader(http.StatusNoContent)
}

// publishBusy fans the page-wide $busy mutex signal out to every open page:
// key is "run", "apply", "query" or "agent" while that job runs, "" when it
// ends. The UI greys out the other action buttons and morphs the holder's
// own button into Stop (view.BusyActionButton, view/console.go's send
// button). Job keys are distinct, so this is a UI-level mutex, not a
// server-side one — direct POSTs can still overlap jobs; the signal just
// reflects the most recent transition (last-writer-wins across distinct job
// keys — a second job ending can publish "" while the first is still
// running), which is fine for the single-user workbench's UI gating, though
// a genuinely careful "still busy with something else" signal would need a
// reference count instead of a single key. wb.busyKey caches the same value
// this call publishes so handleEvents can replay it to a page that connects
// mid-job (see busyKey's doc comment on workbench).
func (wb *workbench) publishBusy(key string) {
	wb.publishBusySignals(key, "", "")
}

// publishBusyConv is publishBusy for a conversation turn: the "agent" key
// plus WHICH conversation owns it (id and display name), so every
// composer can tell "my turn is running" from "a turn is running
// elsewhere" (view/conversation.go's signal vocabulary).
func (wb *workbench) publishBusyConv(convID, convName string) {
	wb.publishBusySignals("agent", convID, convName)
}

func (wb *workbench) publishBusySignals(key, convID, convName string) {
	wb.busyMu.Lock()
	wb.busyKey, wb.busyConvID, wb.busyConvName = key, convID, convName
	wb.busyMu.Unlock()
	wb.bus.Publish(datastar.Signal(map[string]any{
		"busy": key, "busyConv": convID, "busyConvName": convName,
	}))
}

// currentBusy returns the $busy signal values as of the last publish.
func (wb *workbench) currentBusy() (key, convID, convName string) {
	wb.busyMu.Lock()
	defer wb.busyMu.Unlock()
	return wb.busyKey, wb.busyConvID, wb.busyConvName
}

// handleEvents is the page's one long-lived subscription connection
// (doc/notes/datastar.md §8), opened once per page load by the
// data-init="@get('/events', ...)" div in view.Page's body (both the
// Facts and Rules views carry it, page-scoped rather than pane-scoped, so
// a view with two Fact Browser panes still shares a single connection).
// Ordering matters: subscribe to the bus BEFORE sending the initial
// fragment, so a Transform-completed notification that lands mid-render is
// queued rather than lost (the subscribe-before-render trap the design doc
// calls out).
func (wb *workbench) handleEvents(w http.ResponseWriter, r *http.Request) {
	stream, err := datastar.RequestStream(w, r)
	if err != nil {
		return
	}

	sub := wb.bus.Subscribe() // 1. attach to the bus FIRST
	defer sub.Close()

	// 2. then send current state — just the #predicates-{base,derived}
	// fragments, not the whole pane: re-emitting the subscribe div itself
	// here would re-trigger Datastar's subscribe-on-mount behavior. Both
	// fragments go out regardless of which Fact Browser pane(s) the current
	// view actually has on screen; Datastar morphs whichever id is present
	// and no-ops on the other (view/page.go's doc comment).
	wb.h.mu.Lock()
	base, derived := renderPredicates(wb.h.sess)
	wb.h.mu.Unlock()
	_ = stream.Emit(datastar.Batch(datastar.Elements(base), datastar.Elements(derived)))

	// $busy defaults to '' client-side and this connection has seen no
	// publishBusy calls yet, so a tab opened while a job is already running
	// would otherwise show idle — no Stop control, and no visible feedback —
	// until that job happens to end and publish "". Replaying the current
	// key here (captured AFTER subscribing, so a transition landing in the
	// gap is still caught by the subscription rather than missed entirely)
	// fixes that for every fresh connection, not just ones that happen to be
	// open when a job starts.
	if key, convID, convName := wb.currentBusy(); key != "" {
		_ = stream.Emit(datastar.Signal(map[string]any{
			"busy": key, "busyConv": convID, "busyConvName": convName,
		}))
	}

	for { // 3. drain anything that arrived between 1 and 2, plus all future events
		select {
		case <-r.Context().Done():
			return
		case ev := <-sub.Events():
			if err := stream.Emit(ev); err != nil {
				return
			}
		}
	}
}
