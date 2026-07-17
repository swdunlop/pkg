package main

import (
	"archive/zip"
	"bufio"
	"context"
	stdflag "flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"swdunlop.dev/pkg/datalog"
	"swdunlop.dev/pkg/datalog/jsonfacts"
	"swdunlop.dev/pkg/datalog/memory"
	"swdunlop.dev/pkg/datalog/seminaive"
	"swdunlop.dev/pkg/datalog/syntax"
)

// runMCP implements the `datalog mcp` subcommand: an MCP server on stdio
// exposing the session as six structured-JSON tools. args excludes the
// "mcp" argument itself (main.go strips it before dispatching here).
//
// runMCP uses its own flag.FlagSet rather than the package-level pflag
// flags main.go's bare mode registers on flag.CommandLine — registering
// mcp's flags globally would make bare-mode's `-c`/`-d` parsing see mcp's
// flags (or vice versa) depending on dispatch order, so the two arms of
// main's subcommand switch must never share flag state.
func runMCP(args []string) {
	flags := stdflag.NewFlagSet("mcp", stdflag.ExitOnError)
	dataDir := flags.String("d", "", "data directory or .zip file (required unless --proxy is given; the security boundary for all file access)")
	configPath := flags.String("c", "", "path to a JSON or YAML jsonfacts config file to preload")
	timeout := flags.Duration("timeout", 60*time.Second, "per-query evaluation timeout")
	proxy := flags.String("proxy", "", "bridge stdio to a remote streamable-HTTP MCP endpoint (e.g. a running datalog serve's /mcp) instead of serving a local session; the bearer token comes from DATALOG_MCP_TOKEN, never a flag — argv is visible to every process listing")
	if err := flags.Parse(args); err != nil {
		// flag.ExitOnError already printed usage and exited on real errors;
		// this only returns for -h/-help.
		os.Exit(0)
	}

	if *proxy != "" {
		// The stdio<->HTTP bridge (doc/features/acp-integration.md work item
		// 7): -d/-c/rule-file args describe a LOCAL session and have no
		// meaning when proxying a REMOTE one, so runMCPProxy takes over
		// entirely rather than sharing any of the setup below.
		runMCPProxy(*proxy, os.Getenv("DATALOG_MCP_TOKEN"), os.Stdin, os.Stdout)
		return
	}

	if *dataDir == "" {
		fmt.Fprintln(os.Stderr, "datalog mcp: -d <data directory or .zip> is required")
		os.Exit(1)
	}

	h, closeFn, err := newMCPHandlers(*dataDir, *configPath, flags.Args(), *timeout)
	if err != nil {
		fmt.Fprintf(os.Stderr, "datalog mcp: %v\n", err)
		os.Exit(1)
	}
	defer closeFn()

	srv := server.NewMCPServer("datalog", "0.1.0",
		server.WithInstructions(mcpServerInstructions),
		// WithRecovery: a panic in any tool handler (network hiccup in an
		// external predicate, a nil-pointer bug, ...) must not take down the
		// stdio server mid-conversation — the process serves this one
		// long-lived session for the whole agent conversation, so a crash
		// here is unrecoverable for every OTHER tool call too, not just the
		// one that panicked. This option is the query path's ONLY panic
		// guard — h.query runs Transform inline with no runRecovered of its
		// own, unlike the web handlers.
		server.WithRecovery(),
	)
	h.registerTools(srv)

	if err := server.ServeStdio(srv); err != nil {
		fmt.Fprintf(os.Stderr, "datalog mcp: %v\n", err)
		os.Exit(1)
	}
}

// mcpHandlers holds the session, confinement, and mutex backing the MCP
// tool surface. Methods are plain Go (typed input struct + session in,
// typed output struct + error out) so they can be tested directly without
// the SDK. mcp.go's registerTools is the only place that knows about
// mcp-go's types; everything else here is ordinary Go.
type mcpHandlers struct {
	mu      sync.Mutex
	sess    *session
	fsys    fs.FS      // confined data filesystem: dataRoot.FS() or a *zip.Reader
	confine confineRef // validates a ref against the data root; nil-safe (treated as "no restriction") only in tests
	timeout time.Duration

	// onChange, if set, is invoked after a SUCCESSFUL setSchema or setRules
	// call, while h.mu is STILL HELD (matching publishSessionChanged's
	// documented contract in fact_browser.go: rendering the patch-back
	// fragment reads session state, so the caller must hold h.mu across the
	// read). runServe sets this to wb.publishSessionChanged so an agent
	// mutating over /mcp repaints the human's browser (doc/features/web-ui.md
	// deployment section, design constraint 3's "updated by agent" flow).
	// runMCP (stdio `datalog mcp`) leaves this nil, so stdio behavior is
	// byte-identical to before this field existed.
	onChange func()
}

// lockedSnapshot is the one sanctioned way to read session state for a
// query: it holds h.mu only long enough to capture a querySnapshot, so the
// caller can run the snapshot's Compile+Transform (up to the query timeout)
// lock-free without blocking every other tool call and workbench pane. The
// query sees the session as of this call; a set_rules/set_schema landing
// mid-evaluation applies to the next query. New query surfaces must go
// through this helper rather than touching h.mu/h.sess directly.
func (h *mcpHandlers) lockedSnapshot() (querySnapshot, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.sess.snapshotForQuery()
}

// cacheDerivedQuery writes a freshly computed base fixpoint (the query
// handler's cold-path Transform — see querySnapshot.runQuery's doc comment,
// and lockedSnapshot's caller in query()) back into session.derivedDB, so a
// later query against the same generation can reuse it instead of
// recomputing the whole base ruleset from scratch. This is query's half of
// the write-back rules_editor.go's Run path already performs after its own
// evaluateSnapshot call: same gen guard, same cap policy.
//
// base is never the synthetic _q_ stage's output — runQuery's cold path
// populates querySnapshot.derived with the base ruleset's fixpoint BEFORE
// running the _q_ stage, so what lands here has no _q_ facts in it by
// construction. snapGen is the gen captured when the snapshot was taken
// (querySnapshot.gen, itself session.gen as of lockedSnapshot); if the
// session's current gen no longer matches, a set_rules/set_schema/loadData
// landed while this query's Transform ran lock-free, and base reflects a
// ruleset/schema/data that no longer exists, so the write-back is silently
// dropped — mirroring runApplyRulesDocument's `wb.h.sess.gen == snapGen`
// guard (rules_editor.go). A base fixpoint that fails checkFactCap
// (sandbox.go) is refused rather than cached, exactly as the Run/startup
// paths already refuse to cache one: a query must not be able to cache what
// Run would refuse.
func (h *mcpHandlers) cacheDerivedQuery(base datalog.Database, snapGen uint64) {
	if base == nil {
		return
	}
	if err := checkFactCap(base); err != nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.sess.gen == snapGen {
		h.sess.derivedDB = base
	}
}

// newMCPHandlers opens the data source named by dataDir and constructs the
// handlers. dataDir is either a directory (confined via dataRoot/os.Root)
// or a .zip file (confined via fs.ValidPath, since zip.Reader already
// implements fs.FS and rejects escaping names on its own — no os.Root
// wrapper is needed or possible for an in-memory zip index). Both cases
// produce one fs.FS plus one confineRef, threaded uniformly through
// set_schema's data loading and sample_input's file listing/reading, so
// the two data-source kinds behave identically to callers.
func newMCPHandlers(dataDir, configPath string, ruleFiles []string, timeout time.Duration) (*mcpHandlers, func() error, error) {
	var (
		fsys    fs.FS
		confine confineRef
		closeFn = func() error { return nil }
	)

	if strings.HasSuffix(dataDir, ".zip") {
		r, err := zip.OpenReader(dataDir)
		if err != nil {
			return nil, nil, fmt.Errorf("opening data zip %s: %w", dataDir, err)
		}
		fsys = &r.Reader
		confine = func(ref string) (string, error) {
			if ref == "" {
				return "", fmt.Errorf("empty file reference")
			}
			if !fs.ValidPath(ref) {
				return "", fmt.Errorf("%s: escapes data zip", ref)
			}
			return ref, nil
		}
		closeFn = r.Close
	} else {
		root, err := openDataRoot(dataDir)
		if err != nil {
			return nil, nil, err
		}
		fsys = root.FS()
		confine = root.Confine
		closeFn = root.Close
	}

	// WithFactLimit(factCap) (sandbox.go) is the default engine option for
	// every seminaive.Engine this session's methods build — set here, once,
	// rather than at each of the several Compile call sites (session.go's
	// setRules/setRulesWithQueries/evaluateSnapshot/runQuery, rules_editor.go's
	// handleRulesCheck), so a query's two Transform stages (the base ruleset
	// and the synthetic _q_ rule) and every other evaluation path share one
	// mid-evaluation cap with no risk of a new call site forgetting it. Both
	// `datalog mcp` (runMCP) and `datalog serve` (newWorkbench) build their
	// session through this constructor, so both get the cap; the bare REPL
	// (repl.go's newREPL) is a separate, uncapped surface not in scope here.
	sess := &session{engineOpts: []seminaive.Option{seminaive.WithFactLimit(factCap)}}

	if configPath != "" {
		data, err := os.ReadFile(configPath)
		if err != nil {
			closeFn()
			return nil, nil, fmt.Errorf("reading config %s: %w", configPath, err)
		}
		format := "yaml"
		if ext := filepath.Ext(configPath); ext == ".json" {
			format = "json"
		}
		if err := sess.setSchema(string(data), format, fsys, confine); err != nil {
			closeFn()
			return nil, nil, fmt.Errorf("loading config %s: %w", configPath, err)
		}
	}

	var rulesText strings.Builder
	for _, rf := range ruleFiles {
		data, err := os.ReadFile(rf)
		if err != nil {
			closeFn()
			return nil, nil, fmt.Errorf("reading rules %s: %w", rf, err)
		}
		queries, err := sess.loadProgram(string(data))
		if err != nil {
			closeFn()
			return nil, nil, fmt.Errorf("loading rules %s: %w", rf, err)
		}
		// Unlike the REPL's loadProgram (repl.go), which runs embedded '?'
		// queries and prints their results, mcp/serve mode has nowhere to
		// print to: stdio's stdout is the JSON-RPC channel, and serve has no
		// console yet at this point in startup. Silently dropping them was
		// the bug — warn by name instead, so an operator who pasted a REPL
		// script wholesale learns why those queries never ran rather than
		// wondering why nothing happened.
		if len(queries) > 0 {
			fmt.Fprintf(os.Stderr, "datalog: %s: %d embedded query statement(s) ignored "+
				"(startup rule files load rules only; use the query tool to run queries)\n", rf, len(queries))
		}
		rulesText.Write(data)
	}
	sess.rulesText = rulesText.String()

	return &mcpHandlers{sess: sess, fsys: fsys, confine: confine, timeout: timeout}, closeFn, nil
}

// registerTools wires the six typed handler methods into srv using
// mcp-go's generic structured-tool-handler helper: mcp.WithInputSchema[T]
// derives the JSON input schema from each input struct's fields and
// "jsonschema" tags, and mcp.NewStructuredToolHandler binds incoming
// arguments to that struct and serializes the returned struct as the
// tool's structured result.
func (h *mcpHandlers) registerTools(srv *server.MCPServer) {
	srv.AddTool(
		mcp.NewTool("set_schema",
			mcp.WithDescription(mcpSetSchemaDescription),
			mcp.WithInputSchema[setSchemaInput](),
		),
		// No h.mu here: like query, setSchema manages its own locking,
		// holding it only around the cheap field swap — the expensive
		// parse/confine/load work (prepareSchema) runs lock-free first. See
		// setSchema's doc comment.
		mcp.NewStructuredToolHandler(func(_ context.Context, _ mcp.CallToolRequest, in setSchemaInput) (setSchemaOutput, error) {
			return h.setSchema(in)
		}),
	)
	srv.AddTool(
		mcp.NewTool("set_rules",
			mcp.WithDescription(mcpSetRulesDescription),
			mcp.WithInputSchema[setRulesInput](),
		),
		mcp.NewStructuredToolHandler(func(_ context.Context, _ mcp.CallToolRequest, in setRulesInput) (setRulesOutput, error) {
			h.mu.Lock()
			defer h.mu.Unlock()
			return h.setRules(in)
		}),
	)
	srv.AddTool(
		mcp.NewTool("query",
			mcp.WithDescription(mcpQueryDescription(h.timeout)),
			mcp.WithInputSchema[queryInput](),
		),
		// No h.mu here: query manages the lock itself, holding it only for
		// the state snapshot so a long-running Transform does not freeze
		// the other tools or the workbench panes that share this mutex.
		mcp.NewStructuredToolHandler(func(ctx context.Context, _ mcp.CallToolRequest, in queryInput) (queryOutput, error) {
			return h.query(ctx, in)
		}),
	)
	srv.AddTool(
		mcp.NewTool("list_predicates",
			mcp.WithDescription(mcpListPredicatesDescription),
			mcp.WithInputSchema[listPredicatesInput](),
		),
		mcp.NewStructuredToolHandler(func(_ context.Context, _ mcp.CallToolRequest, in listPredicatesInput) (listPredicatesOutput, error) {
			h.mu.Lock()
			defer h.mu.Unlock()
			return h.listPredicates(in)
		}),
	)
	srv.AddTool(
		mcp.NewTool("sample_facts",
			mcp.WithDescription(mcpSampleFactsDescription),
			mcp.WithInputSchema[sampleFactsInput](),
		),
		mcp.NewStructuredToolHandler(func(_ context.Context, _ mcp.CallToolRequest, in sampleFactsInput) (sampleFactsOutput, error) {
			h.mu.Lock()
			defer h.mu.Unlock()
			return h.sampleFacts(in)
		}),
	)
	srv.AddTool(
		mcp.NewTool("sample_input",
			mcp.WithDescription(mcpSampleInputDescription),
			mcp.WithInputSchema[sampleInputInput](),
		),
		// No h.mu here: sample_input only ever reads h.fsys/h.confine, both
		// immutable after construction (never reassigned), and touches no
		// session state at all — see sampleInput's doc comment.
		mcp.NewStructuredToolHandler(func(_ context.Context, _ mcp.CallToolRequest, in sampleInputInput) (sampleInputOutput, error) {
			return h.sampleInput(in)
		}),
	)
}

// -- set_schema ---------------------------------------------------------

type setSchemaInput struct {
	Schema string `json:"schema" jsonschema:"the jsonfacts config document (sources, matchers, declarations), whole-document replacement"`
	Format string `json:"format,omitempty" jsonschema:"\"yaml\" (default) or \"json\", matching how schema is written"`
}

type predicateCount struct {
	Name  string `json:"name"`
	Arity int    `json:"arity"`
	Facts int    `json:"facts"`
}

type setSchemaOutput struct {
	Predicates []predicateCount `json:"predicates"`
	Warnings   []string         `json:"warnings,omitempty"`
}

// setSchema runs the expensive parse/confine/*_from-resolve/load work
// (session.prepareSchema) BEFORE taking h.mu, then holds the lock only for
// the field swap, buildDB, and onChange — a query, another tool call, or a
// workbench pane render no longer blocks for the duration of a full data
// reload (the mechanism this fixes: set_schema previously held h.mu across
// the whole call). Two set_schema calls racing each other each run their
// own prepareSchema independently and then swap in whichever finishes last;
// every field that swap touches (cfg, schemaText, dataDB, derivedDB) comes
// from the SAME prepareSchema result and is assigned in one lock
// acquisition, so last-swap-wins still leaves the session internally
// coherent — never a schemaText from one submission paired with a dataDB
// from another.
func (h *mcpHandlers) setSchema(in setSchemaInput) (setSchemaOutput, error) {
	cfg, db, err := prepareSchema(in.Schema, in.Format, h.fsys, h.confine)
	if err != nil {
		return setSchemaOutput{}, err
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	return h.applySchemaLocked(in.Schema, cfg, db)
}

// applySchemaLocked swaps a prepareSchema result into the session and fires
// onChange. Callers must hold h.mu; the schema editor's Apply path takes the
// lock itself so it can check its context for a Stop between acquiring the
// lock and committing the swap.
func (h *mcpHandlers) applySchemaLocked(schemaText string, cfg jsonfacts.Config, db *memory.Database) (setSchemaOutput, error) {
	h.sess.cfg = cfg
	h.sess.schemaText = schemaText
	h.sess.dataDB = db
	h.sess.derivedDB = nil
	h.sess.gen++
	full, err := h.sess.buildDB()
	if err != nil {
		return setSchemaOutput{}, err
	}
	if h.onChange != nil {
		h.onChange()
	}
	return setSchemaOutput{Predicates: countPredicates(full)}, nil
}

// countPredicates enumerates every predicate/arity pair in db and counts
// its facts, mirroring commands.go's cmdList (.list in the REPL).
func countPredicates(db datalog.Database) []predicateCount {
	type key struct {
		name  string
		arity int
	}
	counts := map[key]int{}
	for name, arity := range db.Predicates() {
		k := key{name, arity}
		for range db.Facts(name, arity) {
			counts[k]++
		}
	}
	out := make([]predicateCount, 0, len(counts))
	for k, n := range counts {
		out = append(out, predicateCount{Name: k.name, Arity: k.arity, Facts: n})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Name != out[j].Name {
			return out[i].Name < out[j].Name
		}
		return out[i].Arity < out[j].Arity
	})
	return out
}

// -- set_rules ------------------------------------------------------------

type setRulesInput struct {
	Source string `json:"source" jsonschema:"the whole Datalog program (facts and rules); embedded '?' queries are rejected, use the query tool instead"`
}

type setRulesOutput struct {
	Predicates []string `json:"predicates"`
}

// setRules requires the caller to hold h.mu — unlike h.query, which manages
// its own locking via lockedSnapshot/cacheDerivedQuery, its safety lives
// entirely in the tool-handler wrapper that locks around it.
func (h *mcpHandlers) setRules(in setRulesInput) (setRulesOutput, error) {
	if err := h.sess.setRules(in.Source); err != nil {
		return setRulesOutput{}, err
	}
	seen := map[string]bool{}
	var preds []string
	for _, r := range h.sess.rules {
		if !seen[r.Head.Pred] {
			seen[r.Head.Pred] = true
			preds = append(preds, r.Head.Pred)
		}
	}
	for _, ar := range h.sess.aggRules {
		if !seen[ar.Head.Pred] {
			seen[ar.Head.Pred] = true
			preds = append(preds, ar.Head.Pred)
		}
	}
	sort.Strings(preds)
	if h.onChange != nil {
		h.onChange()
	}
	return setRulesOutput{Predicates: preds}, nil
}

// -- query ------------------------------------------------------------

const (
	defaultQueryLimit = 100
	maxQueryLimit     = 1000
)

type queryInput struct {
	Query string `json:"query" jsonschema:"a single Datalog query statement, e.g. suspicious(Host, Pid, Cmd)?"`
	Limit int    `json:"limit,omitempty" jsonschema:"max rows to serialize (default 100, hard cap 1000); evaluation always runs to completion regardless"`
}

type statOutput struct {
	Predicates []string `json:"predicates"`
	RuleCount  int      `json:"rule_count"`
	AggCount   int      `json:"agg_count"`
	FactCount  int      `json:"fact_count"`
	Iterations int      `json:"iterations"`
	DurationMS int64    `json:"duration_ms"`
}

type queryOutput struct {
	Vars      []string     `json:"vars"`
	Rows      [][]any      `json:"rows"`
	Total     int          `json:"total"`
	Truncated bool         `json:"truncated"`
	Stats     []statOutput `json:"stats"`
}

func (h *mcpHandlers) query(ctx context.Context, in queryInput) (queryOutput, error) {
	parsed, err := syntax.ParseStatement(in.Query)
	if err != nil {
		return queryOutput{}, err
	}
	if err := validateStatementNoReservedPred(parsed); err != nil {
		return queryOutput{}, err
	}
	q, ok := parsed.(*syntax.Query)
	if !ok {
		return queryOutput{}, fmt.Errorf("query: expected a query statement ending in '?', got %T", parsed)
	}
	if err := rejectAnonQueryVars(q); err != nil {
		return queryOutput{}, err
	}

	limit := in.Limit
	if limit <= 0 {
		limit = defaultQueryLimit
	}
	if limit > maxQueryLimit {
		limit = maxQueryLimit
	}

	ctx, cancel := context.WithTimeout(ctx, h.timeout)
	defer cancel()

	snap, err := h.lockedSnapshot()
	if err != nil {
		return queryOutput{}, err
	}
	// cold records whether this snapshot arrived with no cached fixpoint
	// (session.derivedDB was nil as of lockedSnapshot), i.e. whether
	// runQuery's cold path is about to compute one and leave it in
	// snap.derived (querySnapshot.runQuery's pointer-receiver doc comment).
	// Only in that case is there anything new worth writing back below —
	// a snapshot that already had snap.derived populated reused the
	// session's existing cache, and re-writing the same value back would
	// just be a redundant checkFactCap + lock round trip.
	cold := snap.derived == nil

	rows, vars, stats, err := snap.runQuery(ctx, q)
	if err != nil {
		return queryOutput{}, err
	}
	if cold {
		h.cacheDerivedQuery(snap.derived, snap.gen)
	}

	n := len(rows)
	serialize := min(n, limit)
	outRows := make([][]any, serialize)
	for i := range serialize {
		row := make([]any, len(rows[i]))
		for j, c := range rows[i] {
			row[j] = constantToJSON(c)
		}
		outRows[i] = row
	}

	outStats := make([]statOutput, len(stats))
	for i, s := range stats {
		outStats[i] = statOutput{
			Predicates: s.Predicates,
			RuleCount:  s.RuleCount,
			AggCount:   s.AggCount,
			FactCount:  s.FactCount,
			Iterations: s.Iterations,
			DurationMS: s.Duration.Milliseconds(),
		}
	}

	return queryOutput{
		Vars:      vars,
		Rows:      outRows,
		Total:     n,
		Truncated: n > serialize,
		Stats:     outStats,
	}, nil
}

// rejectAnonQueryVars refuses anonymous variables ('?' or a bare '_') in a
// query tool call's POSITIVE body atoms. Both are legal in the language,
// but in a positive query atom a weak model that writes pred(?, ?)? is
// usually pattern-matching SQL, not choosing anonymity — the error text
// teaches the two forms it should have used, since tool errors are the
// model's only corrective feedback (doc/features/mcp-server.md's
// atomic-feedback posture). Negated atoms are exempt: there, anonymity is
// the REQUIRED don't-care form — the engine's safety check skips anonymous
// variables in negated atoms but rejects unbound named ones, so
// `not remote_logon(H, ?, ?, ?)` is the only way to write not-exists. The
// parser renames both literals to ?N before we see them, so detection
// matches the generated prefix.
func rejectAnonQueryVars(q *syntax.Query) error {
	for _, atom := range q.Body {
		if atom.Negated {
			continue
		}
		for _, term := range atom.Terms {
			v, ok := term.(datalog.Variable)
			if !ok || !strings.HasPrefix(string(v), "?") {
				continue
			}
			return fmt.Errorf("query: anonymous variables ('?' or bare '_') are not allowed outside negated atoms: "+
				"name every column you want returned (e.g. %s), or use an underscore-prefixed "+
				"variable (e.g. _Ignored) for positions you don't care about", exampleNamedQuery(q))
		}
	}
	return nil
}

// exampleNamedQuery rewrites the offending query with A, B, C... in place
// of its anonymous variables, so the error shows the fix applied to the
// model's own query rather than an unrelated example.
func exampleNamedQuery(q *syntax.Query) string {
	names := []string{"A", "B", "C", "D", "E", "F", "G", "H"}
	next := 0
	fixed := syntax.Query{Body: make([]syntax.Atom, len(q.Body))}
	for i, atom := range q.Body {
		terms := make([]datalog.Term, len(atom.Terms))
		for j, term := range atom.Terms {
			if v, ok := term.(datalog.Variable); ok && strings.HasPrefix(string(v), "?") && next < len(names) {
				terms[j] = datalog.Variable(names[next])
				next++
				continue
			}
			terms[j] = term
		}
		fixed.Body[i] = syntax.Atom{Pred: atom.Pred, Terms: terms, Negated: atom.Negated, Expr: atom.Expr}
	}
	return strings.TrimSpace(fixed.String())
}

// constantToJSON converts a datalog.Constant to a value that serializes as
// natural JSON in query/sample_facts rows: strings as JSON strings,
// integers/floats as JSON numbers, composites as their decoded JSON value
// (map/array — the same representation the composite was built from), IDs
// as their "#<n>" display string (they are synthetic join keys, not
// meaningful to a model as a bare number, so the same string form the REPL
// prints is used here rather than round-tripping the raw index), booleans
// as JSON booleans, and null as JSON null.
func constantToJSON(c datalog.Constant) any {
	switch v := c.(type) {
	case datalog.String:
		return string(v)
	case datalog.Integer:
		return int64(v)
	case datalog.Float:
		return float64(v)
	case datalog.Bool:
		return bool(v)
	case datalog.ID:
		return v.String() // "#<n>"
	case datalog.Null:
		return nil
	case *datalog.Composite:
		return v.Value()
	default:
		return c.String()
	}
}

// -- list_predicates ------------------------------------------------------

type listPredicatesInput struct{}

type predicateInfo struct {
	Name  string `json:"name"`
	Arity int    `json:"arity"`
	Facts int    `json:"facts"`
	Use   string `json:"use,omitempty"`
}

type listPredicatesOutput struct {
	Predicates []predicateInfo `json:"predicates"`
}

func (h *mcpHandlers) listPredicates(_ listPredicatesInput) (listPredicatesOutput, error) {
	// evaluatedDB, not buildDB: the agent and the human must see the same
	// counts. The workbench's Fact Browser reads the last Run's evaluated
	// snapshot, so a rule-derived predicate that showed N facts there must
	// not report 0 here (the EDB alone never holds derived facts).
	db, err := h.sess.evaluatedDB()
	if err != nil {
		return listPredicatesOutput{}, err
	}
	// evaluatedDB always yields a *memory.Database under the hood (buildDB
	// returns one directly; evaluate's Transform wraps its result via
	// interned.Memory.Wrap, which also produces one) — asserting here lets
	// PredicateCounts replace a full scan-and-count with the fact-set's O(1)
	// per-predicate lengths (doc/features/mcp-server.md review item 7).
	mdb, ok := db.(*memory.Database)
	if !ok {
		return listPredicatesOutput{}, fmt.Errorf("list_predicates: internal error: unexpected database type %T", db)
	}

	type key struct {
		name  string
		arity int
	}
	counts := map[key]int{}
	for pa, n := range mdb.PredicateCounts() {
		counts[key{pa.Name, pa.Arity}] = n
	}
	// IDB predicates: rule and aggregate-rule heads not already covered by
	// loaded (EDB) facts.
	for _, r := range h.sess.rules {
		arity := len(r.Head.Terms)
		k := key{r.Head.Pred, arity}
		if _, ok := counts[k]; !ok {
			counts[k] = 0
		}
	}
	for _, ar := range h.sess.aggRules {
		arity := len(ar.Head.Terms)
		k := key{ar.Head.Pred, arity}
		if _, ok := counts[k]; !ok {
			counts[k] = 0
		}
	}

	uses := map[string]string{}
	for _, d := range h.sess.cfg.Declarations {
		uses[d.Name] = d.Use
	}

	out := make([]predicateInfo, 0, len(counts))
	for k, n := range counts {
		out = append(out, predicateInfo{Name: k.name, Arity: k.arity, Facts: n, Use: uses[k.name]})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Name != out[j].Name {
			return out[i].Name < out[j].Name
		}
		return out[i].Arity < out[j].Arity
	})
	return listPredicatesOutput{Predicates: out}, nil
}

// -- sample_facts ------------------------------------------------------

const defaultSampleFactsLimit = 20

type sampleFactsInput struct {
	Predicate string `json:"predicate" jsonschema:"predicate name to sample"`
	Arity     int    `json:"arity" jsonschema:"arity of the predicate (predicates may be overloaded by arity)"`
	Limit     int    `json:"limit,omitempty" jsonschema:"max facts to return (default 20)"`
}

type sampleFactsOutput struct {
	Facts     [][]any `json:"facts"`
	Total     int     `json:"total"`
	Truncated bool    `json:"truncated"`
}

func (h *mcpHandlers) sampleFacts(in sampleFactsInput) (sampleFactsOutput, error) {
	limit := in.Limit
	if limit <= 0 {
		limit = defaultSampleFactsLimit
	}

	// evaluatedDB, not buildDB — same reasoning as listPredicates: derived
	// facts the Fact Browser shows must be sampleable, not report as 0.
	db, err := h.sess.evaluatedDB()
	if err != nil {
		return sampleFactsOutput{}, err
	}
	mdb, ok := db.(*memory.Database)
	if !ok {
		return sampleFactsOutput{}, fmt.Errorf("sample_facts: internal error: unexpected database type %T", db)
	}
	// FactCount is O(1) (a fact-slice length), so Total no longer requires
	// scanning past limit just to keep counting; the Facts range below now
	// stops as soon as it has enough rows instead of exhausting the predicate.
	total := mdb.FactCount(in.Predicate, in.Arity)

	var out [][]any
	for row := range db.Facts(in.Predicate, in.Arity) {
		if len(out) >= limit {
			break
		}
		jsonRow := make([]any, len(row))
		for i, c := range row {
			jsonRow[i] = constantToJSON(c)
		}
		out = append(out, jsonRow)
	}
	return sampleFactsOutput{Facts: out, Total: total, Truncated: total > len(out)}, nil
}

// -- sample_input ------------------------------------------------------

const (
	defaultSampleInputLimit  = 10
	sampleInputLineTruncated = 4096 // bytes; individual lines beyond this are truncated with a marker
)

type sampleInputInput struct {
	File   string `json:"file,omitempty" jsonschema:"file to read, relative to the data directory; omit to list available files"`
	Limit  int    `json:"limit,omitempty" jsonschema:"max lines to return when file is set (default 10)"`
	Offset int    `json:"offset,omitempty" jsonschema:"0-based line offset to start reading from, when file is set"`
}

// sampleInputOutput covers both call shapes: when Files is non-nil (no
// "file" argument given), it lists available files and Lines is omitted;
// when Lines is non-nil, it holds the requested slice of (possibly
// truncated) lines from File and TotalLines is the file's true line count.
type sampleInputOutput struct {
	Files      []string `json:"files,omitempty"`
	Lines      []string `json:"lines,omitempty"`
	TotalLines int      `json:"total_lines,omitempty"`
	Truncated  bool     `json:"truncated,omitempty"`
}

// sampleInput reads h.fsys and h.confine only — both are immutable after
// newMCPHandlers/newWorkbench construction (never reassigned, per
// mcpHandlers' field docs) and it touches no session state, so unlike the
// other tool handlers it needs no h.mu at all (see registerTools). File
// contents are streamed with a bufio.Reader rather than slurped whole via
// io.ReadAll: a data file can be arbitrarily large, and the old
// implementation held the entire file in memory (while ALSO holding h.mu,
// blocking every other tool call) just to return a handful of lines.
// Streaming still reads to EOF to report the file's true TotalLines (see
// TestSampleInput_OffsetAndLimit), but only ever keeps "limit" lines'
// worth of bytes in memory at once, not the whole file.
func (h *mcpHandlers) sampleInput(in sampleInputInput) (sampleInputOutput, error) {
	if in.File == "" {
		var files []string
		err := fs.WalkDir(h.fsys, ".", func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !d.IsDir() {
				files = append(files, p)
			}
			return nil
		})
		if err != nil {
			return sampleInputOutput{}, err
		}
		sort.Strings(files)
		return sampleInputOutput{Files: files}, nil
	}

	if in.Offset < 0 {
		return sampleInputOutput{}, fmt.Errorf("sample_input: offset must be >= 0, got %d", in.Offset)
	}

	ref, err := h.confine(in.File)
	if err != nil {
		return sampleInputOutput{}, err
	}

	f, err := h.fsys.Open(path.Clean(ref))
	if err != nil {
		return sampleInputOutput{}, err
	}
	defer f.Close()

	limit := in.Limit
	if limit <= 0 {
		limit = defaultSampleInputLimit
	}

	var lines []string
	lineNum := 0
	r := bufio.NewReader(f)
	for {
		chunk, rerr := r.ReadString('\n')
		// ReadString only ever returns an empty chunk together with a
		// non-nil error (there is nothing left to count); any non-empty
		// chunk — including a final newline-less partial line at EOF — is
		// one more line, counted and (if in [offset, offset+limit)) kept.
		if chunk != "" {
			line := strings.TrimSuffix(strings.TrimSuffix(chunk, "\n"), "\r")
			if lineNum >= in.Offset && len(lines) < limit {
				lines = append(lines, truncateLine(line))
			}
			lineNum++
		}
		if rerr != nil {
			if rerr != io.EOF {
				return sampleInputOutput{}, fmt.Errorf("reading %s: %w", in.File, rerr)
			}
			break
		}
	}

	return sampleInputOutput{
		Lines:      lines,
		TotalLines: lineNum,
		Truncated:  lineNum > in.Offset+len(lines),
	}, nil
}

// truncateLine truncates a line beyond sampleInputLineTruncated bytes,
// appending a marker noting the original length so the model knows data
// was cut rather than mistaking the cut point for the real line ending.
func truncateLine(line string) string {
	if len(line) <= sampleInputLineTruncated {
		return line
	}
	return fmt.Sprintf("%s...[truncated, %d bytes total]", line[:sampleInputLineTruncated], len(line))
}
