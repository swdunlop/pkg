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
	html "github.com/swdunlop/html-go"
	"github.com/swdunlop/html-go/datastar"
	"swdunlop.dev/pkg/datalog"
	"swdunlop.dev/pkg/datalog/cmd/datalog/view"
	"swdunlop.dev/pkg/datalog/memory"
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
	maxFacts := flags.Int("max-facts", defaultMaxFacts, "cap on total facts (base + derived) an evaluation may hold before it is refused as too large; 0 = no cap, rely on Stop + OOM (doc/features/workbench-scale.md)")
	evalTimeout := flags.Duration("eval-timeout", defaultEvalTimeout, "deadline for Run/Apply/agent query/Fact Browser evaluations; 0 = no deadline, Stop is the only brake (doc/features/workbench-scale.md)")
	provenance := flags.Bool("provenance", true, "record derivation provenance for every evaluation (the why? drawer / explain tool); costs roughly one map entry plus a []uint64 per derived fact, so turn it off for memory headroom on large datasets (doc/features/workbench-scale.md design decision 4)")
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

	// newWorkbenchAsync, not newWorkbench: serve's whole reason for this
	// split (doc/features/workbench-scale.md design decision 3) is bringing
	// the listener up WITHOUT waiting for loadDone — a real dataset's load
	// takes minutes, and an operator staring at a dead terminal can't tell
	// load from hang. loadDone is only consumed below, to sequence the
	// watcher's start (see the comment there) — runServe itself never blocks
	// on it before opening the listener.
	wb, closeFn, loadDone, err := newWorkbenchAsync(*dataDir, *configPath, ruleFiles, *rulesDir, *mcpToken,
		agentConfig{
			Model:          *model,
			ProviderURL:    *providerURL,
			ProviderAPIKey: *providerKey,
			AgentCommand:   *agentCmd,
			MCPURL:         mcpURL(*listen),
		}, *maxFacts, *evalTimeout, *provenance)
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
	//
	// Started only AFTER the background load job finishes (loadDone, in its
	// own goroutine below), not before: the watcher's reload path
	// (watch.go's reloadFromDisk/reloadSchema) reads/writes the same session
	// fields the load job is still populating, through the same h.mu — the
	// load job holds h.mu only for the swap itself (loadDeferredSchema), not
	// across the whole multi-minute prepareSchema read, so a vim save
	// landing mid-load COULD interleave a reload's applySchemaLocked with
	// the load's own loadDeferredSchema call. Rather than reason about that
	// interleaving (both take h.mu, but "atomic under the lock" doesn't make
	// "which one's result wins" meaningful when the load's own file read
	// races the save), the watcher simply isn't listening yet: a save that
	// happens to land during the minutes-long initial load is picked up the
	// FIRST time fsnotify's watch registers, same as if the operator saved
	// again after noticing "dataset loaded" in the console. This can only
	// ever delay noticing an edit made during the load window, never lose
	// or corrupt one, since disk itself is unaffected either way.
	var stopWatcher func() = func() {}
	watcherReady := make(chan struct{})
	go func() {
		<-loadDone
		sw, err := wb.startWatcher()
		if err != nil {
			fmt.Fprintf(os.Stderr, "datalog serve: %v\n", err)
			os.Exit(1)
		}
		stopWatcher = sw
		close(watcherReady)
	}()
	defer func() {
		select {
		case <-watcherReady:
			stopWatcher()
		default:
			// Shutdown raced the load itself; nothing to stop yet.
		}
	}()

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

// newWorkbench builds a *workbench wired exactly like runServe does, blocking
// until the base dataset is fully loaded and the initial rule evaluation has
// run — i.e. the synchronous contract every existing test in this package
// relies on (newWorkbench "keeps a synchronous variant for tests" per
// doc/features/workbench-scale.md work item 4's test brief). It is a thin
// wrapper over newWorkbenchAsync that waits on the returned done channel;
// runServe itself calls newWorkbenchAsync directly so the HTTP listener does
// NOT wait for a multi-minute load. See newWorkbenchAsync's doc comment for
// the full picture of what "opens the data source, preloads schema/rules...”
// covers and what got deferred to the background load job.
func newWorkbench(dataDir, configPath string, ruleFiles []string, rulesDir string, tokenFlag string, agentCfg agentConfig, maxFacts int, evalTimeout time.Duration, provenance bool) (*workbench, func() error, error) {
	wb, closeFn, done, err := newWorkbenchAsync(dataDir, configPath, ruleFiles, rulesDir, tokenFlag, agentCfg, maxFacts, evalTimeout, provenance)
	if err != nil {
		return nil, nil, err
	}
	<-done
	return wb, closeFn, nil
}

// loadJobKey is the Jobs key for the background dataset-load job (doc/
// features/workbench-scale.md design decision 3): registered so Global
// Cancel can stop a load exactly like a Run's, and doubling as the $busy
// vocabulary for the one-spinner rule while the load runs. A cancelled load
// leaves the workbench up with an empty dataset and the cancellation
// recorded as a console/status error — the same "stay up, report the
// failure" posture a failed load gets (see runLoadJob's doc comment) rather
// than a special case, since from the session's point of view a cancelled
// load and a failed one both mean "the base DB never got applied."
const loadJobKey = "load"

// newWorkbenchAsync is newWorkbench's real implementation: it constructs the
// workbench and returns it USABLE IMMEDIATELY — routes wired, /mcp mounted,
// rules loaded and (if the ruleset is non-empty) ready to evaluate — with
// the configPath dataset load and the initial rule evaluation running in a
// background job instead of inline. The returned done channel closes when
// that job finishes (success OR failure); runServe never waits on it (the
// whole point is bringing the listener up before a 6-minute load completes,
// doc/features/workbench-scale.md design decision 3), but newWorkbench
// (the test-facing synchronous wrapper) does, and a caller that legitimately
// wants "wait until ready" (a future CLI flag, a health-check endpoint) has
// somewhere to hook in without re-deriving this ordering.
//
// When configPath == "", there is nothing to defer: the load job still runs
// (for the initial rule evaluation, if any rules were given) but finishes
// near-instantly, and wb.isLoading() is false from construction.
func newWorkbenchAsync(dataDir, configPath string, ruleFiles []string, rulesDir string, tokenFlag string, agentCfg agentConfig, maxFacts int, evalTimeout time.Duration, provenance bool) (*workbench, func() error, <-chan struct{}, error) {
	// deferConfigLoad is unconditional (even for configPath == ""): the one
	// load-job code path below (runLoadJob) is shared regardless of whether
	// there is an actual config load to perform, so there is no second
	// "startup eval only, no deferred load" variant to keep in sync with the
	// first (see runLoadJob's doc comment).
	h, closeFn, err := newMCPHandlers(dataDir, configPath, ruleFiles, rulesDir, evalTimeout, maxFacts, true, provenance)
	if err != nil {
		return nil, nil, nil, err
	}

	token := tokenFlag
	if token == "" {
		token, err = generateToken()
		if err != nil {
			closeFn()
			return nil, nil, nil, fmt.Errorf("generating /mcp bearer token: %w", err)
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
		h:             h,
		bus:           newBus(),
		jobs:          newJobs(),
		console:       &consoleLog{},
		mcpSrv:        srv,
		agentCfg:      agentCfg,
		mcpToken:      token,
		pendingPerm:   map[string]pendingPermission{},
		pendingCmds:   map[string][]commandRecord{},
		reloadSeen:    map[string]int{},
		modePreambled: map[string]bool{},
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

	// The background load job (doc/features/workbench-scale.md design
	// decision 3): loads configPath's dataset (if any, via
	// h.loadDeferredSchema — the deferred half of newMCPHandlers'
	// construction above) and then runs the initial rule evaluation,
	// mirroring what Run does (rules_editor.go handleRulesRun) — nothing
	// populates session.derivedDB until someone presses Run, and the agent
	// has no Run button, so without an initial evaluation it would see every
	// derived predicate as 0 facts until the human remembered to click.
	// wb.loading is set BEFORE the goroutine starts (not inside it) so a
	// caller that checks it immediately after newWorkbenchAsync returns
	// never sees a false "not loading" window before the goroutine gets
	// scheduled.
	wb.setLoading(true)
	done := make(chan struct{})
	go wb.runLoadJob(done)

	return wb, closeFn, done, nil
}

// setLoading records whether the background load job is in flight, read by
// isLoading (render paths) and toggled exactly twice per job: true right
// before runLoadJob's goroutine is scheduled, false at its completion path.
func (wb *workbench) setLoading(v bool) {
	wb.loadMu.Lock()
	wb.loading = v
	wb.loadMu.Unlock()
}

// isLoading reports whether the background dataset load (and initial rule
// evaluation) is still running — the signal render paths use to show
// "loading dataset…" instead of a convincingly empty session (doc/features/
// workbench-scale.md design decision 3).
func (wb *workbench) isLoading() bool {
	wb.loadMu.Lock()
	defer wb.loadMu.Unlock()
	return wb.loading
}

// runLoadJob is the background startup-load job newWorkbenchAsync launches:
// it loads configPath's dataset (if any — h.configPath is empty when serve
// was started with no -c) via h.loadDeferredSchema — prepareSchema plus the
// SAME commitSchemaLocked swap every other schema write goes through, with
// the load itself lock-free (loadDeferredSchema's doc comment) — then runs
// the initial rule evaluation exactly as newWorkbench used to do inline
// before this feature. It rides the established job/
// busy/console plumbing (wb.jobs.Begin + wb.publishBusy + runRecovered is
// "the established pattern," per this task's brief, matching
// autoReevaluate's shape in watch.go) rather than inventing new machinery:
//   - wb.jobs.Begin(loadJobKey) registers the job so Global Cancel
//     (handleCancel) can stop a load exactly like a Run's.
//   - wb.publishBusy(loadJobKey) drives the $busy spinner/Stop-button UI.
//   - A start console entry ("loading dataset…") and a completion entry
//     (predicate/fact counts and elapsed time, or the error) bracket the
//     job, so the operator can tell load-in-progress from a hung server
//     (the console drawer is "the natural place for... notifications," per
//     the brief) — this is deliberately the ONE progress checkpoint on each
//     end, not a granular per-file/per-record stream: jsonfacts's loader
//     exposes no cheap incremental hook today, and the brief explicitly
//     accepts "do not invent deep machinery for it."
//
// done is closed exactly once, after wb.setLoading(false) and the console/
// busy bookkeeping below — so a caller waiting on it (newWorkbench's
// synchronous test wrapper) never observes isLoading() still true or a
// stale busy key once done has fired.
//
// A cancelled load (Global Cancel firing mid-load) and a failed load both
// leave the workbench's dataset empty and are reported identically: an
// error console entry plus a non-nil error recorded on the job (there is no
// separate "load was cancelled" status surface — see loadJobKey's doc
// comment). The workbench itself stays up and reachable either way; only
// the session's data/derived predicates are empty until an operator fixes
// the config and restarts (there is no retry-in-place for a failed initial
// load in this task's scope).
func (wb *workbench) runLoadJob(done chan<- struct{}) {
	defer close(done)

	jobCtx, jobDone := wb.jobs.Begin(context.Background(), loadJobKey)
	if jobCtx == nil {
		// Should not happen (nothing else registers loadJobKey this early),
		// but a busy key must not silently strand isLoading() at true.
		wb.setLoading(false)
		return
	}
	defer jobDone()
	wb.publishBusy(loadJobKey)
	defer wb.publishBusy("")

	start := time.Now()
	const loadTab = "" // startup load has no owning conversation; the empty tab is the shared/global scrollback (consoleLog.Render("") — see handleRoot's empty-state page, which reads the same tab).
	wb.consoleAppend(loadTab, "status", html.Text("loading dataset…"))

	var loadErr error
	if wb.h.configPath != "" {
		loadErr = wb.h.loadDeferredSchema()
	}

	// The rules-present check takes h.mu: an agent's put_rule_group over
	// /mcp can land while the load job runs (the listener is up — that is
	// the point of this job), and session.rules is only stable under the
	// session mutex.
	haveRules := false
	if loadErr == nil {
		wb.h.mu.Lock()
		haveRules = len(wb.h.sess.rules)+len(wb.h.sess.aggRules) > 0
		wb.h.mu.Unlock()
	}
	if haveRules {
		// Mirrors autoReevaluate (watch.go): h.mu is held only to snapshot
		// and to commit — the Transform itself runs lock-free. Holding h.mu
		// across a multi-minute evaluation would block every pane render and
		// tool call for the duration, exactly the dead-listener symptom the
		// background load exists to prevent. The write-back is gen-guarded
		// like Run's: if anything mutated the session while the Transform
		// ran (the listener is up — an agent write can land), the result is
		// discarded and the mutator's own invalidation stands.
		ctx, cancel := wb.h.evalContext(jobCtx)
		wb.h.mu.Lock()
		ruleset, engineOpts, db, snapGen, buildErr := wb.h.sess.snapshotForEvaluate()
		prov := wb.h.sess.newEvalProvenance()
		wb.h.mu.Unlock()

		var evaluated datalog.Database
		evalErr := buildErr
		if buildErr == nil {
			evalErr = <-runRecovered(func() error {
				var err error
				evaluated, err = evaluateSnapshot(ctx, ruleset, engineOpts, db, prov)
				return err
			})
		}
		if evalErr == nil {
			evalErr = wb.h.checkFactCap(evaluated)
		}
		if evalErr == nil {
			wb.h.mu.Lock()
			if ctx.Err() == nil && wb.h.sess.gen == snapGen {
				wb.h.sess.derivedDB = evaluated
				wb.h.sess.derivedProv = prov
			}
			wb.h.mu.Unlock()
		}
		cancel()
		loadErr = evalErr
	}

	wb.setLoading(false)

	if loadErr != nil {
		fmt.Fprintf(os.Stderr, "datalog serve: background dataset load: %v\n", loadErr)
		wb.consoleAppend(loadTab, "error", html.Text("dataset load failed: "+loadErr.Error()))
		return
	}

	wb.h.mu.Lock()
	n := 0
	predicates := 0
	if db, err := wb.h.sess.evaluatedDB(); err == nil {
		// PredicateCounts is the O(1)-per-predicate tally (the same one
		// renderPredicates uses) — a full db.Facts walk here would re-scan
		// every base fact under h.mu right after a multi-GB load, exactly
		// the O(offset)-style full scan workbench-scale.md's design
		// decision 5 exists to retire. The non-*memory.Database fallback
		// walks, but no session path produces one (evaluatedDB's contract).
		if mdb, ok := db.(*memory.Database); ok {
			for _, c := range mdb.PredicateCounts() {
				predicates++
				n += c
			}
		} else {
			for name, arity := range db.Predicates() {
				predicates++
				for range db.Facts(name, arity) {
					n++
				}
			}
		}
	}
	// publishSessionChanged repaints every open page's predicate lists and
	// Schema/Rules panels now that the base DB (and, if there were rules, the
	// derived DB) is populated — the panes were rendering wb.isLoading()'s
	// "loading dataset…" tell against an empty session until this call
	// (fact_browser.go's publishSessionChanged, called under h.mu per its
	// documented contract).
	wb.publishSessionChanged()
	wb.h.mu.Unlock()

	wb.consoleAppend(loadTab, "status", html.Text(fmt.Sprintf(
		"dataset loaded: %d facts in %d predicates (took %s)", n, predicates, time.Since(start).Round(time.Millisecond))))
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

	// loadMu guards loading, which is true from the moment newWorkbenchAsync
	// launches the background load job until runLoadJob's completion path
	// clears it (success or failure — a failed load is still "done loading,"
	// not "still loading"). Render paths that would otherwise show a
	// convincingly-empty dataset (renderPredicates and friends, fact_browser.go)
	// read wb.isLoading() to render a "loading dataset…" tell instead (doc/
	// features/workbench-scale.md design decision 3's pane-loading-state
	// requirement). A separate mutex from busyMu: loading is a render-time
	// fact about session readiness, not a $busy-signal transition, and the
	// two must not be conflated even though the load job ALSO rides the
	// $busy machinery (publishBusy(loadJobKey)) for the Stop button/spinner.
	loadMu  sync.Mutex
	loading bool

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
	// modePreambled marks conversations whose ACP driver already received
	// their mode instructions in-band (commands_composer.go's
	// frameModePreamble); kit conversations never enter it.
	modePreambled map[string]bool

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

	// Browser: Rules tab group detail (browser_panels.go).
	mux.HandleFunc("GET /rules/{head}/{arity}", wb.handleRuleGroup)

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
	base, derived := renderPredicates(wb.h.sess, wb.isLoading())
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
