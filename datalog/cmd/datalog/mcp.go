package main

import (
	"archive/zip"
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
	dataDir := flags.String("d", "", "data directory or .zip file (required; the security boundary for all file access)")
	configPath := flags.String("c", "", "path to a JSON or YAML jsonfacts config file to preload")
	timeout := flags.Duration("timeout", 60*time.Second, "per-query evaluation timeout")
	if err := flags.Parse(args); err != nil {
		// flag.ExitOnError already printed usage and exited on real errors;
		// this only returns for -h/-help.
		os.Exit(0)
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

	sess := &session{}

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
		if _, err := sess.loadProgram(string(data)); err != nil {
			closeFn()
			return nil, nil, fmt.Errorf("loading rules %s: %w", rf, err)
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
		mcp.NewStructuredToolHandler(func(_ context.Context, _ mcp.CallToolRequest, in setSchemaInput) (setSchemaOutput, error) {
			h.mu.Lock()
			defer h.mu.Unlock()
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
			mcp.WithDescription(mcpQueryDescription),
			mcp.WithInputSchema[queryInput](),
		),
		mcp.NewStructuredToolHandler(func(ctx context.Context, _ mcp.CallToolRequest, in queryInput) (queryOutput, error) {
			h.mu.Lock()
			defer h.mu.Unlock()
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
		mcp.NewStructuredToolHandler(func(_ context.Context, _ mcp.CallToolRequest, in sampleInputInput) (sampleInputOutput, error) {
			h.mu.Lock()
			defer h.mu.Unlock()
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

func (h *mcpHandlers) setSchema(in setSchemaInput) (setSchemaOutput, error) {
	if err := h.sess.setSchema(in.Schema, in.Format, h.fsys, h.confine); err != nil {
		return setSchemaOutput{}, err
	}
	db, err := h.sess.buildDB()
	if err != nil {
		return setSchemaOutput{}, err
	}
	if h.onChange != nil {
		h.onChange()
	}
	return setSchemaOutput{Predicates: countPredicates(db)}, nil
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

	rows, vars, stats, err := h.sess.runQuery(ctx, q)
	if err != nil {
		return queryOutput{}, err
	}

	n := len(rows)
	serialize := n
	if serialize > limit {
		serialize = limit
	}
	outRows := make([][]any, serialize)
	for i := 0; i < serialize; i++ {
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

	type key struct {
		name  string
		arity int
	}
	counts := map[key]int{}
	for name, arity := range db.Predicates() {
		counts[key{name, arity}] = 0
	}
	for k := range counts {
		for range db.Facts(k.name, k.arity) {
			counts[k]++
		}
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

	var out [][]any
	total := 0
	for row := range db.Facts(in.Predicate, in.Arity) {
		total++
		if len(out) < limit {
			jsonRow := make([]any, len(row))
			for i, c := range row {
				jsonRow[i] = constantToJSON(c)
			}
			out = append(out, jsonRow)
		}
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

	data, err := io.ReadAll(f)
	if err != nil {
		return sampleInputOutput{}, err
	}

	var lines []string
	lineNum := 0
	if len(data) > 0 {
		text := strings.TrimSuffix(string(data), "\n")
		for _, line := range strings.Split(text, "\n") {
			if lineNum >= in.Offset && len(lines) < limit {
				lines = append(lines, truncateLine(strings.TrimSuffix(line, "\r")))
			}
			lineNum++
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
