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
// exposing the session as a set of structured-JSON tools (registerToolsForMode
// lists them by name). args excludes the "mcp" argument itself (main.go
// strips it before dispatching here).
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
	rulesDir := flags.String("rules", "", "path to a rules/ directory store (one <head>_<arity>.dl file per rule group); mutually exclusive with positional rule files")
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

	if err := rulesSourceConflict(*rulesDir, flags.Args()); err != nil {
		fmt.Fprintf(os.Stderr, "datalog mcp: %v\n", err)
		os.Exit(1)
	}

	h, closeFn, err := newMCPHandlers(*dataDir, *configPath, flags.Args(), *rulesDir, *timeout)
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

	// rules is the loaded rules/ directory store (rulestore.go), set only
	// when newMCPHandlers was given --rules instead of positional rule
	// files. nil means this session's rules came from the legacy monolithic
	// file(s) (or no rules at all) — h.sess.rulesText/rules/aggRules are the
	// only source of truth in that case. This is deliberately a thin seam:
	// a later task adds per-group revision counters and CRUD tool methods
	// on top of it (doc/features/workbench-v2.md work item 1's "leave a
	// small, obvious seam, not speculative machinery"); this task only
	// needs it to keep the loaded groups available for that follow-up and
	// to know, at load time, which file a parse error belongs to.
	rules *ruleStore

	// configPath is the -c flag's value (schema_crud.go's write path's ONE
	// disk target): empty means this session was started with no schema
	// file at all, in which case every schema CRUD write tool refuses with
	// errSchemaPathRequired, the same way h.rules == nil refuses every
	// rule-group CRUD call. Unlike rules (a directory of many files), the
	// schema is always ONE file — every put_source/put_matcher/
	// put_declaration write rewrites this same path in full, per design
	// decision 4's "deterministic serialization... rewrites the whole
	// file."
	configPath string

	// schemaRev is the in-memory, per-process revision bookkeeping for every
	// keyed schema item (schema_crud.go's schemaRevisions) — the schema
	// CRUD surface's counterpart to h.rules' per-group Revision field.
	// Populated at construction from whatever h.sess.authoringCfg loaded with
	// (newSchemaRevisions), and replaced wholesale after every successful
	// write (schema_crud.go's commitSchemaWrite). nil-safe is NOT relied
	// upon: newMCPHandlers always sets this to a non-nil value (even for a
	// configPath == "" session, since get_config's read path — and a
	// caller checking h.configPath first — never needs to consult it, but
	// leaving it nil would panic the moment anything did).
	schemaRev *schemaRevisions
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
// construction. prov is querySnapshot.derivedProv, the recorder for that
// SAME base-stage Transform (nil when provenance is disabled) — cached
// beside base in the same critical section as base itself, so an explain
// after this write-back resolves against the recorder that actually
// produced the cached database (doc/features/provenance.md "Session cache
// interaction"), never a later or unrelated run's recorder. snapGen is the
// gen captured when the snapshot was taken (querySnapshot.gen, itself
// session.gen as of lockedSnapshot); if the session's current gen no longer
// matches, a set_rules/set_schema/loadData landed while this query's
// Transform ran lock-free, and base (and prov) reflect a ruleset/schema/data
// that no longer exists, so the write-back is silently dropped — mirroring
// runApplyRulesDocument's `wb.h.sess.gen == snapGen` guard (rules_editor.go).
// A base fixpoint that fails checkFactCap (sandbox.go) is refused rather
// than cached, exactly as the Run/startup paths already refuse to cache one:
// a query must not be able to cache what Run would refuse — and refusing the
// database means refusing its recorder too, so a cache-admission refusal
// drops both together, never one without the other.
func (h *mcpHandlers) cacheDerivedQuery(base datalog.Database, prov *seminaive.Provenance, snapGen uint64) {
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
		h.sess.derivedProv = prov
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
//
// ruleFiles and rulesDir are mutually exclusive (both runMCP and
// newWorkbench refuse to call this with both non-empty — "use --rules or
// positional rule files, not both"): ruleFiles is the legacy monolithic-
// file(s) path (loaded via session.loadProgram, unchanged from before this
// function grew a rules-directory argument), and rulesDir is the
// doc/features/workbench-v2.md canonical rules/ directory store
// (rulestore.go's loadRuleStore), loaded per-file so a parse error names
// its file, with h.sess.rulesText set to the store's Export-style
// concatenation so every existing surface (REPL echo, workbench Rules
// pane, etc.) keeps seeing one document exactly as it did for a monolithic
// file. The loaded *ruleStore itself is kept on the returned handlers (see
// mcpHandlers.rules's doc comment) for a later CRUD task to build on.
func newMCPHandlers(dataDir, configPath string, ruleFiles []string, rulesDir string, timeout time.Duration) (*mcpHandlers, func() error, error) {
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
	// provenanceEnabled: true by default for every cmd/datalog session
	// (doc/features/provenance.md "Session policy" — interactive scale, the
	// memory cost is bounded per doc/features/provenance.md's Risks section).
	// Both `datalog mcp` (runMCP) and `datalog serve` (newWorkbench) build
	// their session through this constructor, so both get it; the library
	// default (seminaive.WithProvenance never used unless a caller opts in)
	// is unchanged for callers outside cmd/datalog.
	sess := &session{
		engineOpts:        []seminaive.Option{seminaive.WithFactLimit(factCap)},
		provenanceEnabled: true,
	}

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

	var store *ruleStore
	if rulesDir != "" {
		// loadRuleStore validates all-or-nothing and names the offending
		// file on any error (rulestore.go's doc comment) — a startup failure
		// here is exactly as fatal as a malformed monolithic rules file
		// always was, just attributed to one file in the directory instead
		// of the single positional argument.
		s, err := loadRuleStore(rulesDir)
		if err != nil {
			closeFn()
			return nil, nil, fmt.Errorf("loading rules directory %s: %w", rulesDir, err)
		}
		store = s
		// session.loadRuleStore is the one rebuild chokepoint (session.go,
		// doc/features/workbench-v2.md work item 1 design decision 3): it
		// loads every group through session.loadProgram — the same
		// fact-routing path the positional-file path below uses — rather
		// than writing sess.rules/aggRules/facts directly here. loadProgram
		// routes ground facts (body-less rules) into s.facts, where buildDB
		// folds them into the BASE database; appending them into s.rules
		// instead would evaluate them as derived rules, silently changing
		// base/derived classification (listPredicates) and base-DB contents
		// versus loading the identical statements from a monolith. Queries
		// cannot occur here (loadRuleStore already rejected them per file),
		// so the returned queries slice is always empty; errors are wrapped
		// with the group's filename so attribution survives the shared path.
		if err := sess.loadRuleStore(s); err != nil {
			closeFn()
			return nil, nil, fmt.Errorf("loading rules directory %s: %w", rulesDir, err)
		}
	} else {
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
	}

	// schemaRev is built from whatever sess.authoringCfg ended up holding above
	// (empty if configPath == "", exactly like a freshly loaded config with
	// no items). A configPath == "" session still gets a non-nil schemaRev
	// even though every write tool refuses it outright (errSchemaPathRequired)
	// — get_config's read path (schema_reads.go) has no such restriction and
	// must never nil-panic on a schema-less session.
	return &mcpHandlers{
		sess: sess, fsys: fsys, confine: confine, timeout: timeout, rules: store,
		configPath: configPath,
		schemaRev:  newSchemaRevisions(sess.authoringCfg),
	}, closeFn, nil
}

// toolMode scopes which tools registerToolsForMode wires into a server
// (doc/features/workbench-v2.md design decision 5: "Three conversation
// modes, chosen at start... the agent's tool surface is scoped server-side,
// not by instruction"). Phase 2 (conversation.go) makes the three modes
// user-visible at conversation-creation time; registerToolsForMode's switch
// below is the enforcement point every conversation's MCP registration goes
// through, so a tool can never leak into the wrong mode by a call site
// forgetting to gate it.
type toolMode string

const (
	// toolModeQuery is the read-only investigation surface (design decision
	// 5's "Query Mode" bullet): query, list_predicates, sample_facts,
	// explain, explain_fact, describe, predicate_deps, sample_input,
	// get_config, list_rule_groups, get_rule_group. No write tools at all.
	toolModeQuery toolMode = "query"

	// toolModeRules is Query Mode's surface plus rule-group CRUD
	// (put_rule_group, delete_rule_group) — design decision 5's "Rules
	// Mode" bullet. It does NOT get schema CRUD (that is Facts Mode's).
	toolModeRules toolMode = "rules"

	// toolModeFacts is oriented entirely around producing facts from JSONL
	// extraction mappings (design decision 5's "Facts Mode" bullet):
	// sample_input, schema CRUD (put/delete source/matcher/declaration),
	// get_config, list_predicates, sample_facts. No query/explain/
	// explain_fact/predicate_deps/rule tools — Facts Mode verifies
	// extraction output without touching datalog semantics.
	toolModeFacts toolMode = "facts"
)

// registerTools wires this handler's full tool surface — every tool this
// package exposes, spanning all three modes — into srv. This backs the two
// pre-conversation-manager call sites that predate per-conversation mode
// choice (mcp.go's runMCP stdio server and serve.go's newWorkbench, both of
// which serve a single implicit session with no mode picker): they get
// everything, matching their pre-phase-2 behavior byte for byte. The
// conversation manager (conversation.go) calls registerToolsForMode
// directly with the conversation's own mode instead of going through this
// alias.
func (h *mcpHandlers) registerTools(srv *server.MCPServer) {
	h.registerFactsReadSet(srv)
	h.registerQueryOnlySet(srv)
	// nil consent: these surfaces predate per-conversation consent (stdio's
	// caller IS the operator's tool; /mcp holds the operator-issued bearer
	// token) — the diff-card gate is the KIT conversation's (design decision
	// 5; consent.go's header).
	h.registerRulesWriteSet(srv, nil)
	h.registerSchemaWriteSet(srv, nil)
}

// registerToolsForMode wires the mode-appropriate subset of typed handler
// methods into srv using mcp-go's generic structured-tool-handler helper:
// mcp.WithInputSchema[T] derives the JSON input schema from each input
// struct's fields and "jsonschema" tags, and mcp.NewStructuredToolHandler
// binds incoming arguments to that struct and serializes the returned
// struct as the tool's structured result.
//
// The registration methods below are named and cut exactly to the task
// brief's per-mode golden lists (doc/features/workbench-v2.md design
// decision 5), so a future tool lands in exactly one mode's method body —
// there is no shared "everything" closure left to silently carry a new tool
// into a mode it does not belong in.
// consent, when non-nil, gates edits/deletes of existing items behind a
// transcript diff card (consent.go) — the write sets thread it into their
// handlers, so no mode or tool can register a gated write without deciding
// its consent story at this one seam.
func (h *mcpHandlers) registerToolsForMode(srv *server.MCPServer, mode toolMode, consent *consentGate) {
	switch mode {
	case toolModeQuery:
		h.registerFactsReadSet(srv)
		h.registerQueryOnlySet(srv)
	case toolModeRules:
		h.registerFactsReadSet(srv)
		h.registerQueryOnlySet(srv)
		h.registerRulesWriteSet(srv, consent)
	case toolModeFacts:
		h.registerFactsReadSet(srv)
		h.registerSchemaWriteSet(srv, consent)
	default:
		// An unknown mode value is a programming error in this package, not
		// a runtime condition a caller can trigger, so it registers nothing
		// rather than guessing at a fallback surface.
	}
}

// registerFactsReadSet wires the reads every mode gets, including Facts
// Mode (design decision 5's "Facts Mode" bullet: "list_predicates and
// sample_facts to verify what its mappings produced"): sample_input,
// list_predicates, sample_facts, get_config. Query and Rules Mode layer
// registerQueryOnlySet on top; Facts Mode does not.
func (h *mcpHandlers) registerFactsReadSet(srv *server.MCPServer) {
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
		mcp.NewTool("get_config",
			mcp.WithDescription(mcpGetConfigDescription),
			mcp.WithInputSchema[getConfigInput](),
		),
		// h.getConfig takes h.mu itself (read-only, cheap — no Transform).
		mcp.NewStructuredToolHandler(func(_ context.Context, _ mcp.CallToolRequest, in getConfigInput) (getConfigOutput, error) {
			return h.getConfig(in)
		}),
	)
}

// registerQueryOnlySet wires the rest of Query Mode's surface — the tools
// Facts Mode deliberately does NOT get (design decision 5: "No
// query/explain — it verifies extraction output without touching datalog
// semantics"): query, explain, describe, predicate_deps, explain_fact,
// list_rule_groups, get_rule_group. Query and Rules Mode both register this
// alongside registerFactsReadSet.
func (h *mcpHandlers) registerQueryOnlySet(srv *server.MCPServer) {
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
		mcp.NewTool("explain",
			mcp.WithDescription(mcpExplainDescription),
			mcp.WithInputSchema[explainInput](),
		),
		// No h.mu here: explain manages its own locking via
		// lockedSnapshot-style capture inside h.explain, for the same reason
		// query does — resolving/computing a fixpoint can take a while and
		// must not freeze every other tool call or workbench pane.
		mcp.NewStructuredToolHandler(func(ctx context.Context, _ mcp.CallToolRequest, in explainInput) (explainOutput, error) {
			return h.explain(ctx, in)
		}),
	)
	srv.AddTool(
		mcp.NewTool("describe",
			mcp.WithDescription(mcpDescribeDescription),
			mcp.WithInputSchema[describeInput](),
		),
		mcp.NewStructuredToolHandler(func(_ context.Context, _ mcp.CallToolRequest, in describeInput) (describeOutput, error) {
			h.mu.Lock()
			defer h.mu.Unlock()
			return h.describe(in)
		}),
	)
	srv.AddTool(
		mcp.NewTool("list_rule_groups",
			mcp.WithDescription(mcpListRuleGroupsDescription),
			mcp.WithInputSchema[listRuleGroupsInput](),
		),
		mcp.NewStructuredToolHandler(func(_ context.Context, _ mcp.CallToolRequest, in listRuleGroupsInput) (listRuleGroupsOutput, error) {
			h.mu.Lock()
			defer h.mu.Unlock()
			return h.listRuleGroups(in)
		}),
	)
	srv.AddTool(
		mcp.NewTool("get_rule_group",
			mcp.WithDescription(mcpGetRuleGroupDescription),
			mcp.WithInputSchema[getRuleGroupInput](),
		),
		mcp.NewStructuredToolHandler(func(_ context.Context, _ mcp.CallToolRequest, in getRuleGroupInput) (getRuleGroupOutput, error) {
			h.mu.Lock()
			defer h.mu.Unlock()
			return h.getRuleGroup(in)
		}),
	)
	srv.AddTool(
		mcp.NewTool("predicate_deps",
			mcp.WithDescription(mcpPredicateDepsDescription),
			mcp.WithInputSchema[predicateDepsInput](),
		),
		mcp.NewStructuredToolHandler(func(_ context.Context, _ mcp.CallToolRequest, in predicateDepsInput) (predicateDepsOutput, error) {
			return h.predicateDeps(in)
		}),
	)
	srv.AddTool(
		mcp.NewTool("explain_fact",
			mcp.WithDescription(mcpExplainFactDescription),
			mcp.WithInputSchema[explainFactInput](),
		),
		// No h.mu here: explainFact manages its own locking, mirroring
		// explain's lock-free-Transform-then-writeback split.
		mcp.NewStructuredToolHandler(func(ctx context.Context, _ mcp.CallToolRequest, in explainFactInput) (explainFactOutput, error) {
			return h.explainFact(ctx, in)
		}),
	)
}

// registerRulesWriteSet is the rule-group CRUD surface (put_rule_group,
// delete_rule_group) that only Rules Mode gets (design decision 5).
// consent, when non-nil, gates edits/deletes of EXISTING groups behind a
// transcript diff card (consent.go's consented* methods, where the gating
// logic and its tests live); creating a new group applies immediately.
func (h *mcpHandlers) registerRulesWriteSet(srv *server.MCPServer, consent *consentGate) {
	srv.AddTool(
		mcp.NewTool("put_rule_group",
			mcp.WithDescription(mcpPutRuleGroupDescription),
			mcp.WithInputSchema[putRuleGroupInput](),
		),
		// h.mu held for the whole write (design decision 4: "Writes happen
		// under h.mu like the old set_rules did — rule compiles are fast");
		// the consent gate runs before, outside the lock (consent.go's
		// consentedPutRuleGroup).
		mcp.NewStructuredToolHandler(func(ctx context.Context, _ mcp.CallToolRequest, in putRuleGroupInput) (putRuleGroupOutput, error) {
			return h.consentedPutRuleGroup(ctx, consent, in)
		}),
	)
	srv.AddTool(
		mcp.NewTool("delete_rule_group",
			mcp.WithDescription(mcpDeleteRuleGroupDescription),
			mcp.WithInputSchema[deleteRuleGroupInput](),
		),
		mcp.NewStructuredToolHandler(func(ctx context.Context, _ mcp.CallToolRequest, in deleteRuleGroupInput) (deleteRuleGroupOutput, error) {
			return h.consentedDeleteRuleGroup(ctx, consent, in)
		}),
	)
}

// registerSchemaWriteSet is the schema CRUD surface (design decision 4's
// schema half; schema_crud.go): put_source/delete_source,
// put_matcher/delete_matcher, put_declaration/delete_declaration. This is
// Facts Mode's surface (design decision 5's "Facts Mode" bullet) — Query
// and Rules Mode never register it. consent, when non-nil, gates
// edits/deletes of EXISTING items behind a transcript diff card
// (consent.go's consented* methods); adds apply immediately.
func (h *mcpHandlers) registerSchemaWriteSet(srv *server.MCPServer, consent *consentGate) {
	srv.AddTool(
		mcp.NewTool("put_source",
			mcp.WithDescription(mcpPutSourceDescription),
			mcp.WithInputSchema[putSourceInput](),
		),
		mcp.NewStructuredToolHandler(func(ctx context.Context, _ mcp.CallToolRequest, in putSourceInput) (putSourceOutput, error) {
			return h.consentedPutSource(ctx, consent, in)
		}),
	)
	srv.AddTool(
		mcp.NewTool("delete_source",
			mcp.WithDescription(mcpDeleteSourceDescription),
			mcp.WithInputSchema[deleteSourceInput](),
		),
		mcp.NewStructuredToolHandler(func(ctx context.Context, _ mcp.CallToolRequest, in deleteSourceInput) (deleteSourceOutput, error) {
			return h.consentedDeleteSource(ctx, consent, in)
		}),
	)
	srv.AddTool(
		mcp.NewTool("put_matcher",
			mcp.WithDescription(mcpPutMatcherDescription),
			mcp.WithInputSchema[putMatcherInput](),
		),
		mcp.NewStructuredToolHandler(func(ctx context.Context, _ mcp.CallToolRequest, in putMatcherInput) (putMatcherOutput, error) {
			return h.consentedPutMatcher(ctx, consent, in)
		}),
	)
	srv.AddTool(
		mcp.NewTool("delete_matcher",
			mcp.WithDescription(mcpDeleteMatcherDescription),
			mcp.WithInputSchema[deleteMatcherInput](),
		),
		mcp.NewStructuredToolHandler(func(ctx context.Context, _ mcp.CallToolRequest, in deleteMatcherInput) (deleteMatcherOutput, error) {
			return h.consentedDeleteMatcher(ctx, consent, in)
		}),
	)
	srv.AddTool(
		mcp.NewTool("put_declaration",
			mcp.WithDescription(mcpPutDeclarationDescription),
			mcp.WithInputSchema[putDeclarationInput](),
		),
		mcp.NewStructuredToolHandler(func(ctx context.Context, _ mcp.CallToolRequest, in putDeclarationInput) (putDeclarationOutput, error) {
			return h.consentedPutDeclaration(ctx, consent, in)
		}),
	)
	srv.AddTool(
		mcp.NewTool("delete_declaration",
			mcp.WithDescription(mcpDeleteDeclarationDescription),
			mcp.WithInputSchema[deleteDeclarationInput](),
		),
		mcp.NewStructuredToolHandler(func(ctx context.Context, _ mcp.CallToolRequest, in deleteDeclarationInput) (deleteDeclarationOutput, error) {
			return h.consentedDeleteDeclaration(ctx, consent, in)
		}),
	)
}

// -- schema output shapes -------------------------------------------------
//
// setSchemaOutput's name predates phase 1c (it originally backed the
// whole-document set_schema MCP tool, removed in favor of the schema CRUD
// tools in schema_crud.go — design decision 4). It survives, unrenamed, as
// applySchemaLocked's return shape: every schema CRUD write still ends by
// swapping into the session through applySchemaLocked and wants the exact
// same per-predicate fact-count feedback, so this type (and
// predicateCount/countPredicates below) are shared, not duplicated, by
// schema_crud.go's commitSchemaWrite.

type predicateCount struct {
	Name  string `json:"name"`
	Arity int    `json:"arity"`
	Facts int    `json:"facts"`
}

type setSchemaOutput struct {
	Predicates []predicateCount `json:"predicates"`
	Warnings   []string         `json:"warnings,omitempty"`
}

// applySchemaLocked swaps a prepareSchema result into the session and fires
// onChange. Callers must hold h.mu; the schema editor's Apply path takes the
// lock itself so it can check its context for a Stop between acquiring the
// lock and committing the swap. authoring and runtime are prepareSchema's
// two config forms (see session.authoringCfg's doc comment) and must come
// from the SAME prepareSchema call as db, so the session's schema-derived
// fields never mix generations.
func (h *mcpHandlers) applySchemaLocked(schemaText string, authoring, runtime jsonfacts.Config, db *memory.Database) (setSchemaOutput, error) {
	h.sess.cfg = runtime
	h.sess.authoringCfg = authoring
	h.sess.schemaText = schemaText
	h.sess.dataDB = db
	h.sess.derivedDB = nil
	h.sess.derivedProv = nil // keep paired with derivedDB, per session.derivedProv's doc comment (mirrors loadRuleStore/setSchema/loadProgram)
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
		h.cacheDerivedQuery(snap.derived, snap.derivedProv, snap.gen)
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

// -- explain ------------------------------------------------------------

type explainInput struct {
	Fact  string `json:"fact" jsonschema:"one ground fact to explain, e.g. concern(\"ws01\", 87) — the exact predicate and constant terms of a fact the current evaluation produced"`
	Depth int    `json:"depth,omitempty" jsonschema:"max derivation-tree depth to render (default 8); does not affect correctness, only how much of a deep tree prints"`
}

type explainOutput struct {
	Tree string `json:"tree"`
}

// explain resolves fact's full derivation tree and renders it as the same
// unicode box-drawing text seminaive.Derivation.String() produces for the
// REPL's .why — one rendering, shared by every explain surface (this tool,
// repl.go's .why, cmd/datalog/fact_browser.go's "why?" affordance's
// underlying call). Mirrors query's own lock-free-Transform-then-writeback
// split (h.lockedSnapshot/h.cacheDerivedQuery): the potentially-slow part
// (computing a base fixpoint when none is cached yet) runs with no lock
// held, and only the cheap write-back takes h.mu again.
func (h *mcpHandlers) explain(ctx context.Context, in explainInput) (explainOutput, error) {
	fact, err := parseFactStatement(in.Fact)
	if err != nil {
		return explainOutput{}, err
	}

	ctx, cancel := context.WithTimeout(ctx, h.timeout)
	defer cancel()

	h.mu.Lock()
	provEnabled := h.sess.provenanceEnabled
	ruleset, engineOpts, db, snapGen, buildErr := h.sess.snapshotForEvaluate()
	cachedProv := h.sess.derivedProv
	cachedDB := h.sess.derivedDB
	h.mu.Unlock()
	if !provEnabled {
		return explainOutput{}, fmt.Errorf("explain: this session was not started with provenance enabled")
	}
	if buildErr != nil {
		return explainOutput{}, buildErr
	}

	prov := cachedProv
	if cachedDB == nil || cachedProv == nil {
		// Cold path, mirroring query's: no cached base fixpoint (or session
		// provenance was only just enabled), so compute one — a fresh
		// Provenance for THIS Transform only (session.newEvalProvenance's
		// doc comment: a Provenance is most-recent-run-only, never shared
		// across Transforms whose result might get cached).
		fresh := seminaive.NewProvenance()
		out, err := evaluateSnapshot(ctx, ruleset, engineOpts, db, fresh)
		if err != nil {
			return explainOutput{}, err
		}
		prov = fresh
		if err := checkFactCap(out); err == nil {
			h.mu.Lock()
			if h.sess.gen == snapGen {
				h.sess.derivedDB = out
				h.sess.derivedProv = fresh
			}
			h.mu.Unlock()
		}
	}

	depth := in.Depth
	var opts []seminaive.TreeOption
	if depth > 0 {
		opts = append(opts, seminaive.MaxDepth(depth))
	}
	d, found := prov.ExplainTree(fact, opts...)
	if !found {
		return explainOutput{}, fmt.Errorf("explain: %s: no such derived fact in the current evaluation "+
			"(check the predicate name/arity and constant terms with list_predicates/sample_facts, or "+
			"re-run query — an explain must name a fact the current ruleset actually produced)", in.Fact)
	}
	return explainOutput{Tree: d.String()}, nil
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

// -- describe ------------------------------------------------------------

type describeInput struct {
	Predicate string `json:"predicate" jsonschema:"predicate name to describe"`
}

type describeOutput struct {
	Name    string          `json:"name"`
	Arities []describeArity `json:"arities"`
}

// describe is the MCP frontend for session.describe (describe.go) — one of
// three thin frontends over the single session-level implementation (the
// REPL's .describe, repl.go/commands.go; the Fact Browser's predicate
// headers, fact_browser.go/view/fact_browser.go), per this repo's "one
// pipeline, N frontends" doctrine. Takes h.mu because it reads
// h.sess.rules/aggRules directly (session.describe walks them), mirroring
// list_predicates/sample_facts rather than query/explain's lock-free split
// — describe never runs a Transform, so there is nothing expensive to keep
// outside the lock.
func (h *mcpHandlers) describe(in describeInput) (describeOutput, error) {
	result, err := h.sess.describe(in.Predicate)
	if err != nil {
		return describeOutput{}, err
	}
	return describeOutput{Name: result.Name, Arities: result.Arities}, nil
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
