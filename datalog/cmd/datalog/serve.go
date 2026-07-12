package main

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	stdflag "flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"sync"

	"github.com/mark3labs/mcp-go/server"
	html "github.com/swdunlop/html-go"
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
	listen := flags.String("listen", "127.0.0.1:8080", "address to listen on")
	mcpToken := flags.String("mcp-token", "", "bearer token required on /mcp (default: generate one and print it to stderr)")
	model := flags.String("model", "", "embedded agent model, kit-style (e.g. anthropic/claude-sonnet-5, openai/<alias>); empty defers to KIT_MODEL / ~/.kit.yml")
	providerURL := flags.String("provider-url", "", "override the agent model provider's base URL (e.g. an OpenAI-compatible llama-swap endpoint)")
	providerKey := flags.String("provider-api-key", "", "override the agent model provider's API key; empty defers to provider env vars")
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
	// files given on the command line, in order. The first one (if any) is
	// the Save target for the Datalog Editor's rules document, per the
	// design's "Session state and persistence" section — see handleSave's
	// doc comment for the full path-resolution policy.
	ruleFiles := flags.Args()

	wb, closeFn, err := newWorkbench(*dataDir, *configPath, ruleFiles, *mcpToken,
		agentConfig{Model: *model, ProviderURL: *providerURL, ProviderAPIKey: *providerKey})
	if err != nil {
		fmt.Fprintf(os.Stderr, "datalog serve: %v\n", err)
		os.Exit(1)
	}
	defer closeFn()

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
// flag's value; empty means "generate one".
func newWorkbench(dataDir, configPath string, ruleFiles []string, tokenFlag string, agentCfg agentConfig) (*workbench, func() error, error) {
	h, closeFn, err := newMCPHandlers(dataDir, configPath, ruleFiles, evalTimeout)
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

	// One MCP server value serves both consumers: mounted at /mcp for
	// external agents (mountMCP) and registered in-process with the
	// embedded kit agent (newKitDriver) — the "one pipeline, N frontends"
	// rule extended to the tool surface itself.
	srv := server.NewMCPServer("datalog", "0.1.0",
		server.WithInstructions(mcpServerInstructions),
	)
	h.registerTools(srv)

	wb := &workbench{
		h:          h,
		bus:        newBus(),
		jobs:       newJobs(),
		console:    &consoleLog{},
		mcpSrv:     srv,
		agentCfg:   agentCfg,
		schemaPath: configPath,
		rulesPath:  firstOrEmpty(ruleFiles),
		mcpToken:   token,
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
			db, err := h.sess.evaluate(ctx)
			if err != nil {
				return err
			}
			if err := checkFactCap(db); err != nil {
				return err
			}
			h.sess.derivedDB = db
			return nil
		})
		cancel()
		if evalErr != nil {
			fmt.Fprintf(os.Stderr, "datalog serve: initial rule evaluation: %v\n", evalErr)
		}
	}

	return wb, closeFn, nil
}

// firstOrEmpty returns files[0], or "" if files is empty — the rules Save
// target resolution needs "no rules file given at startup" to be
// distinguishable from "given but empty".
func firstOrEmpty(files []string) string {
	if len(files) == 0 {
		return ""
	}
	return files[0]
}

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

// mountMCP mounts the same six-tool MCP surface the stdio `datalog mcp`
// subcommand exposes at /mcp, using mcp-go's streamable HTTP server
// (doc/features/web-ui.md Deployment section). It shares the exact same
// *mcpHandlers (and thus mutex + session) the panes use, via
// h.registerTools — an agent calling set_rules over /mcp and a human
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
// agent's set_schema/set_rules/query are the same operation), the SSE bus
// for Transform-completed fan-out, the job set backing Global Cancel, and
// the stale-suppression generation counter. Mutating handlers go through
// h's typed methods under h.mu (the same mutex the MCP tools use); reads
// may use session state under the same mutex for now — the design's
// snapshot-pointer optimization (design constraint 2) can layer in later
// without changing this struct's shape.
type workbench struct {
	h    *mcpHandlers
	bus  *bus
	jobs *jobs
	gen  generation

	// console is the drawer's server-owned scrollback (console.go); mcpSrv
	// is the shared MCP server value built in newWorkbench, consumed by
	// both mountMCP (streamable HTTP at /mcp) and the embedded kit agent
	// (in-process registration).
	console *consoleLog
	mcpSrv  *server.MCPServer

	// agentMu guards the lazily-constructed embedded agent (agent.go).
	// agentCfg is operator-trusted flag input, immutable after startup.
	agentMu  sync.Mutex
	agent    agentDriver
	agentCfg agentConfig

	// schemaPath and rulesPath are the operator-given startup paths for the
	// schema (-c) and rules (first positional .dl file) documents,
	// respectively — the Save targets (doc/features/web-ui.md "Session state
	// and persistence"). Empty means "not given at startup"; see
	// handleSave's doc comment for the resulting no-path policy. These are
	// operator-trusted (flag/argv values), never model or browser input.
	schemaPath string
	rulesPath  string

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
	// GET / has no natural pane composition of its own — it redirects to
	// the Facts view, the authoring loop's usual starting point (raw data
	// in, base facts out).
	mux.HandleFunc("GET /{$}", wb.handleRoot)
	mux.HandleFunc("GET /facts", wb.handleFactsView)
	mux.HandleFunc("GET /rules", wb.handleRulesView)
	mux.HandleFunc("GET /oat.css", wb.handleOatCSS)
	mux.HandleFunc("GET /workbench.css", wb.handleWorkbenchCSS)
	mux.HandleFunc("GET /events", wb.handleEvents)

	// Data Browser (view/data_browser.go stubs; wave 5 fills in).
	mux.HandleFunc("GET /data", wb.handleDataList)
	mux.HandleFunc("GET /data/{file}", wb.handleDataFile)
	mux.HandleFunc("GET /jsonfacts/test/{file}/{row}", wb.handleJSONFactsTest)

	// jsonfacts Editor (view/jsonfacts_editor.go stubs; wave 6 fills in).
	mux.HandleFunc("POST /jsonfacts/preview", wb.handleJSONFactsPreview)
	mux.HandleFunc("POST /jsonfacts/apply", wb.handleJSONFactsApply)

	// Datalog Editor (view/rules_editor.go stubs; wave 7 fills in).
	mux.HandleFunc("POST /rules/check", wb.handleRulesCheck)
	mux.HandleFunc("POST /rules/run", wb.handleRulesRun)

	// Fact Browser (view/fact_browser.go stub; wave 8 fills in).
	mux.HandleFunc("GET /facts/{predicate}/{arity}", wb.handleFacts)

	// Console drawer (console.go, agent.go): the Query tab's ad-hoc probe
	// and the Agent tab's prompt (doc/features/web-ui.md "Console drawer").
	mux.HandleFunc("POST /console/query", wb.handleConsoleQuery)
	mux.HandleFunc("POST /console/prompt", wb.handleConsolePrompt)
	mux.HandleFunc("POST /console/clear", wb.handleConsoleClear)

	// Global Cancel — implemented fully now (doc/features/web-ui.md
	// "Execution sandbox").
	mux.HandleFunc("POST /cancel", wb.handleCancel)

	// Save/git (save.go).
	mux.HandleFunc("POST /save/{doc}", wb.handleSave)
}

// handleRoot redirects GET / to the Facts view, the authoring loop's usual
// starting point (raw data in, base facts out).
func (wb *workbench) handleRoot(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/facts", http.StatusFound)
}

// handleFactsView renders the Facts view (view/page.go's doc comment):
// Data Browser | jsonfacts Editor | Fact Browser (base) — authoring how
// base facts are extracted from JSONL. This and handleRulesView are the
// only handlers that render a full <html> document (doc/notes/datastar.md
// §1: full renders happen only on browser navigation).
func (wb *workbench) handleFactsView(w http.ResponseWriter, r *http.Request) {
	wb.h.mu.Lock()
	schemaText := wb.h.sess.schemaText
	wb.h.mu.Unlock()

	output := wb.renderJSONFactsSelection()

	page := view.Page{
		Title:  "Datalog Workbench — facts",
		Active: "facts",
		Columns: []html.Content{
			view.DataBrowser(),
			view.JSONFactsEditor(schemaText, output),
			view.FactBrowser("base", "Base Facts"),
		},
		Console: view.Console(wb.console.Render("query"), wb.console.Render("agent")),
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	buf := html.Append(nil, page)
	_, _ = w.Write(buf)
}

// handleRulesView renders the Rules view: Fact Browser (base) | Datalog
// Editor | Fact Browser (derived) — authoring how rules derive facts from
// base facts.
func (wb *workbench) handleRulesView(w http.ResponseWriter, r *http.Request) {
	wb.h.mu.Lock()
	rulesText := wb.h.sess.rulesText
	wb.h.mu.Unlock()

	page := view.Page{
		Title:  "Datalog Workbench — Rules",
		Active: "rules",
		Columns: []html.Content{
			view.FactBrowser("base", "Base Facts"),
			view.RulesEditor(rulesText),
			view.FactBrowser("derived", "Derived Facts"),
		},
		Console: view.Console(wb.console.Render("query"), wb.console.Render("agent")),
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	buf := html.Append(nil, page)
	_, _ = w.Write(buf)
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
// key is "run", "apply" or "agent" while that job runs, "" when it ends.
// The UI greys out the other action buttons and morphs the holder's own
// button into Stop (view.BusyActionButton, view/console.go's send button).
// Job keys are distinct, so this is a UI-level mutex, not a server-side
// one — direct POSTs can still overlap jobs; the signal just reflects the
// most recent transition, which is fine for the single-user workbench.
func (wb *workbench) publishBusy(key string) {
	wb.bus.Publish(datastar.Signal(map[string]any{"busy": key}))
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
