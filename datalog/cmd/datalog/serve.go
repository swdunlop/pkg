package main

import (
	"context"
	stdflag "flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	html "github.com/swdunlop/html-go"
	"github.com/swdunlop/html-go/datastar"
	"github.com/swdunlop/html-go/tag"
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
	if err := flags.Parse(args); err != nil {
		// flag.ExitOnError already printed usage and exited on real errors;
		// this only returns for -h/-help.
		os.Exit(0)
	}

	if *dataDir == "" {
		fmt.Fprintln(os.Stderr, "datalog serve: -d <data directory or .zip> is required")
		os.Exit(1)
	}

	h, closeFn, err := newMCPHandlers(*dataDir, *configPath, flags.Args(), evalTimeout)
	if err != nil {
		fmt.Fprintf(os.Stderr, "datalog serve: %v\n", err)
		os.Exit(1)
	}
	defer closeFn()

	wb := &workbench{
		h:    h,
		bus:  newBus(),
		jobs: newJobs(),
	}

	mux := http.NewServeMux()
	wb.routes(mux)

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
}

// routes registers the full route table on mux using Go 1.22+ method+
// pattern syntax. Pane endpoints are stubs for now (later waves fill them
// in, one file per pane so parallel agents never touch this file); the
// full-page shell, static CSS, the /events subscription skeleton, and
// Global Cancel are implemented completely in this wave.
func (wb *workbench) routes(mux *http.ServeMux) {
	mux.HandleFunc("GET /{$}", wb.handleIndex)
	mux.HandleFunc("GET /oat.css", wb.handleOatCSS)
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

	// Global Cancel — implemented fully now (doc/features/web-ui.md
	// "Execution sandbox").
	mux.HandleFunc("POST /cancel", wb.handleCancel)

	// Save/git (wave 9 fills in).
	mux.HandleFunc("POST /save/{doc}", wb.handleSave)
}

// handleIndex renders the full page shell with the four panes as
// placeholder sections carrying stable element ids. Later waves patch
// pane content over SSE; this is the only handler that renders a full
// <html> document (doc/notes/datastar.md §1: full renders happen only on
// browser navigation).
func (wb *workbench) handleIndex(w http.ResponseWriter, r *http.Request) {
	page := view.Page{
		Title:           "datalog workbench",
		DataBrowser:     view.DataBrowser(),
		JSONFactsEditor: view.JSONFactsEditor(),
		RulesEditor:     view.RulesEditor(),
		FactBrowser:     view.FactBrowser(),
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

// handleCancel is the Global Cancel emergency brake: fires every in-flight
// job's CancelFunc. Single-user makes the blunt instrument acceptable —
// see jobs.CancelAll's doc.
func (wb *workbench) handleCancel(w http.ResponseWriter, r *http.Request) {
	wb.jobs.CancelAll()
	w.WriteHeader(http.StatusNoContent)
}

// handleEvents is the Fact Browser's long-lived subscription connection
// (doc/notes/datastar.md §8), opened once per page load by the
// data-init="@get('/events', ...)" div in view.FactBrowser. Ordering
// matters: subscribe to the bus BEFORE sending the initial fragment, so a
// Transform-completed notification that lands mid-render is queued rather
// than lost (the subscribe-before-render trap the design doc calls out).
// The initial predicate listing and per-event fragment rendering are wave
// 8's job; this wave wires the connection lifecycle correctly so that wave
// only has to fill in what gets rendered.
func (wb *workbench) handleEvents(w http.ResponseWriter, r *http.Request) {
	stream, err := datastar.RequestStream(w, r)
	if err != nil {
		return
	}

	sub := wb.bus.Subscribe() // 1. attach to the bus FIRST
	defer sub.Close()

	// 2. then send current state — just the #predicates fragment, not the
	// whole pane: re-emitting the pane's own data-init div here would
	// re-trigger Datastar's subscribe-on-mount behavior. Wave 8 replaces
	// this placeholder with the real predicate listing.
	_ = stream.Emit(datastar.Elements(stubFragment("predicates")))

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

// stubFragment renders a minimal Datastar patch target for a pane endpoint
// not yet implemented by a later wave: a div with the given id (matching a
// stable id declared in view/<pane>.go) containing a "not implemented yet"
// message. Every pane stub handler (data_browser.go, jsonfacts_editor.go,
// rules_editor.go, fact_browser.go) uses this so their responses are valid
// Datastar SSE patches from day one, before the real rendering exists.
func stubFragment(id string) html.Content {
	return tag.New("div").Set("id", id).Add(html.Text("not implemented yet"))
}

// handleSave is the Save stub (POST /save/{doc}, doc = schema|rules). Not
// pane-owned — wave 9 fills this in: writes the file and, if the project
// directory is a git repo, runs git add + git commit -m "ui: save
// <filename>".
func (wb *workbench) handleSave(w http.ResponseWriter, r *http.Request) {
	stream, err := datastar.RequestStream(w, r)
	if err != nil {
		return
	}
	_ = stream.Emit(datastar.Elements(stubFragment("toast")))
}
