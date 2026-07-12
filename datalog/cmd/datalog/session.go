package main

import (
	"archive/zip"
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
	"swdunlop.dev/pkg/datalog"
	"swdunlop.dev/pkg/datalog/jsonfacts"
	"swdunlop.dev/pkg/datalog/memory"
	"swdunlop.dev/pkg/datalog/seminaive"
	"swdunlop.dev/pkg/datalog/syntax"
)

// session holds one evaluation context: a data source, loaded base facts,
// and accumulated rules. It is the engine-facing core shared by the REPL
// and the MCP server; it never touches a terminal. Methods return errors
// and data, never write to an io.Writer.
type session struct {
	facts    []datalog.Fact
	rules    []syntax.Rule
	aggRules []syntax.AggregateRule

	engineOpts []seminaive.Option

	// Data source configured via setDataSource.
	configPath string
	dataDir    string
	cfg        jsonfacts.Config
	dataDB     *memory.Database // facts loaded from data source (replaced on reload)

	// Canonical document texts. The session owns the documents, not just
	// their compiled artifacts: the web workbench renders and patches
	// these, and set_schema/set_rules update them, so a human typing in
	// an editor and an agent submitting a document are the same operation.
	schemaText string
	rulesText  string

	// derivedDB caches the last full Transform's output (doc/features/web-ui.md
	// design constraint 2's snapshot pointer): base facts plus every
	// rule-derived predicate, populated by evaluate() and consumed by
	// evaluatedDB(). Transform returns the datalog.Database interface (not
	// concretely *memory.Database), so this field is typed to match. nil
	// means "no successful evaluation since the last rules/schema/fact
	// change" — evaluatedDB() falls back to buildDB's EDB-only snapshot in
	// that case. Every mutator that can change what evaluate() would
	// produce (setSchema, setRules, setRulesWithQueries, loadProgram,
	// loadData) clears it, so a stale Run's derived facts never survive an
	// unapplied edit.
	derivedDB datalog.Database
}

// setDataSource configures the session to load facts from a jsonfacts config.
func (s *session) setDataSource(configPath, dataDir string) {
	s.configPath = configPath
	s.dataDir = dataDir
}

// loadData loads (or reloads) facts from the configured data source.
func (s *session) loadData() error {
	if s.configPath == "" {
		return fmt.Errorf("no data source configured (use -c flag)")
	}

	data, err := os.ReadFile(s.configPath)
	if err != nil {
		return fmt.Errorf("config %s: %w", s.configPath, err)
	}
	cfg, err := parseConfig(data, s.configPath)
	if err != nil {
		return fmt.Errorf("config %s: %w", s.configPath, err)
	}

	var db *memory.Database
	if strings.HasSuffix(s.dataDir, ".zip") {
		db, err = loadFromZip(cfg, s.dataDir)
	} else {
		db, err = cfg.LoadDir(s.dataDir)
	}
	if err != nil {
		return fmt.Errorf("loading data from %s: %w", s.dataDir, err)
	}

	s.cfg = cfg
	s.schemaText = string(data)
	s.dataDB = db
	s.derivedDB = nil
	return nil
}

// loadFromZip opens a zip file and loads JSONL data using it as an fs.FS.
func loadFromZip(cfg jsonfacts.Config, path string) (*memory.Database, error) {
	r, err := zip.OpenReader(path)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return cfg.LoadFS(&r.Reader)
}

// parseConfig decodes a jsonfacts.Config from JSON or YAML source; path is
// used only to detect YAML by extension.
func parseConfig(data []byte, path string) (jsonfacts.Config, error) {
	ext := filepath.Ext(path)
	format := "json"
	if ext == ".yaml" || ext == ".yml" {
		format = "yaml"
	}
	return parseConfigFormat(data, format)
}

// parseConfigFormat decodes a jsonfacts.Config from raw bytes given an
// explicit format hint ("json" or "yaml"), rather than sniffing a file
// extension. This is the variant set_schema uses: an MCP submission is a
// string in memory with no path to sniff. An empty format defaults to
// "yaml", matching the schema's typical authoring format.
func parseConfigFormat(data []byte, format string) (jsonfacts.Config, error) {
	switch format {
	case "", "yaml":
		var raw any
		if err := yaml.Unmarshal(data, &raw); err != nil {
			return jsonfacts.Config{}, fmt.Errorf("parsing YAML: %w", err)
		}
		var err error
		data, err = json.Marshal(raw)
		if err != nil {
			return jsonfacts.Config{}, fmt.Errorf("converting YAML to JSON: %w", err)
		}
	case "json":
		// no conversion needed
	default:
		return jsonfacts.Config{}, fmt.Errorf("unknown config format %q (want %q or %q)", format, "yaml", "json")
	}

	var cfg jsonfacts.Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return jsonfacts.Config{}, fmt.Errorf("parsing config: %w", err)
	}
	return cfg, nil
}

// confineRef validates a single file reference against a session's
// confinement boundary. MCP wires this to dataRoot.Confine (directory data
// source) or a zip-based fs.ValidPath check (zip data source); the REPL
// never sets it, so setSchema outside MCP behaves as before (no
// confinement, since the operator authors the config themselves).
type confineRef func(ref string) (string, error)

// setSchema replaces the session's jsonfacts configuration atomically: it
// parses the config from raw text plus a format hint ("yaml" or "json",
// empty defaults to yaml), validates every file reference the config makes
// (each source's file and every matcher's *_from pattern file) through
// confine, resolves *_from pattern files itself (LoadFS does not do this;
// only LoadSchemaFS does, and set_schema submissions are not schema-dir
// files), loads the data via fsys, and only then mutates session state. On
// any error, the session is left exactly as it was before the call — the
// model's replacement schema either fully replaces the old one or not at
// all, per the design's atomic-replacement requirement (mcp-server.md).
func (s *session) setSchema(text string, format string, fsys fs.FS, confine confineRef) error {
	cfg, err := parseConfigFormat([]byte(text), format)
	if err != nil {
		return err
	}

	if confine != nil {
		for i, src := range cfg.Sources {
			if _, err := confine(src.File); err != nil {
				return fmt.Errorf("source %d: file %q: %w", i, src.File, err)
			}
		}
		for i := range cfg.Matchers {
			m := &cfg.Matchers[i]
			for _, ref := range []string{
				m.ContainsFrom, m.StartsWithFrom, m.EndsWithFrom,
				m.RegexMatchFrom, m.Base64From, m.Base64UTF16From, m.CIDRFrom,
			} {
				if ref == "" {
					continue
				}
				if _, err := confine(ref); err != nil {
					return fmt.Errorf("matcher %d: file %q: %w", i, ref, err)
				}
			}
		}
	}

	// Resolve *_from pattern files ourselves: LoadFS (unlike LoadSchemaFS)
	// does not do this, and an MCP-submitted config is not a schema-dir
	// file, so nothing else will resolve them before matching runs.
	if err := cfg.ResolveFromFS(fsys); err != nil {
		return err
	}

	db, err := cfg.LoadFS(fsys)
	if err != nil {
		return fmt.Errorf("loading data: %w", err)
	}

	s.cfg = cfg
	s.schemaText = text
	s.dataDB = db
	s.derivedDB = nil
	return nil
}

// setRules replaces the session's Datalog program atomically: it parses
// source, rejects embedded `?` queries (the model should use the query
// tool instead of embedding one in the document), runs a trial Compile so
// stratification/arity errors attach to this submission rather than to a
// later, innocent query, and only then replaces s.rules/s.aggRules/
// s.rulesText wholesale. Unlike loadProgram (which appends, for the REPL's
// .load), setRules always replaces the whole document — the editing model
// mcp-server.md specifies for the MCP tool surface. On any error, session
// state is unchanged.
func (s *session) setRules(source string) error {
	ruleset, err := syntax.ParseAll(source)
	if err != nil {
		return err
	}
	if len(ruleset.Queries) > 0 {
		return fmt.Errorf("set_rules: source contains %d embedded query statement(s) ('?'); "+
			"remove them and use the query tool to run queries", len(ruleset.Queries))
	}

	if _, err := seminaive.New(s.engineOpts...).Compile(ruleset); err != nil {
		return err
	}

	s.rules = ruleset.Rules
	s.aggRules = ruleset.AggRules
	s.rulesText = source
	s.derivedDB = nil
	return nil
}

// setRulesWithQueries replaces the session's Datalog program atomically,
// exactly like setRules (trial Compile of the parsed rules; rules/aggRules/
// rulesText are replaced wholesale only if that Compile succeeds, and
// rulesText captures the full original source — including any embedded
// queries — since the workbench editor's content is the canonical document),
// but returns the embedded `?` queries to the caller instead of rejecting
// them. The MCP set_rules tool keeps the stricter no-embedded-queries rule
// (setRules above): a model is expected to use the query tool instead of
// embedding one in a set_rules document. The workbench's Datalog Editor Run
// action accepts embedded queries because the editor follows the REPL's
// `.`/`?` convention, so a pasted `.dl` file with trailing queries should
// just work. On any error, session state is unchanged.
func (s *session) setRulesWithQueries(source string) ([]syntax.Query, error) {
	ruleset, err := syntax.ParseAll(source)
	if err != nil {
		return nil, err
	}

	if _, err := seminaive.New(s.engineOpts...).Compile(ruleset); err != nil {
		return nil, err
	}

	s.rules = ruleset.Rules
	s.aggRules = ruleset.AggRules
	s.rulesText = source
	s.derivedDB = nil
	return ruleset.Queries, nil
}

// loadProgram parses a Datalog source string containing multiple statements,
// adding facts and rules to the session. Queries found in the source are
// returned for the caller to execute and present.
func (s *session) loadProgram(src string) ([]syntax.Query, error) {
	ruleset, err := syntax.ParseAll(src)
	if err != nil {
		return nil, err
	}
	for _, rule := range ruleset.Rules {
		if rule.IsFact() {
			s.facts = append(s.facts, rule.ToFact())
		} else {
			s.rules = append(s.rules, rule)
		}
	}
	s.aggRules = append(s.aggRules, ruleset.AggRules...)
	s.derivedDB = nil
	return ruleset.Queries, nil
}

// buildDB returns the database to evaluate against: the loaded data source
// extended with interactively asserted facts.
func (s *session) buildDB() (*memory.Database, error) {
	if s.dataDB != nil {
		if len(s.facts) == 0 {
			return s.dataDB, nil
		}
		return s.dataDB.Extend(s.facts...)
	}
	b := memory.NewBuilder()
	for _, f := range s.facts {
		if err := b.AddFact(f); err != nil {
			return nil, err
		}
	}
	return b.Build(), nil
}

// evaluate compiles the session's current ruleset and runs a full seminaive
// Transform against buildDB()'s EDB, returning the resulting database: base
// facts plus every rule-derived predicate. This is the same computation
// runQuery performs per query (with its extra synthetic _q_ rule mixed in)
// minus the synthetic head — callers that want a snapshot of "what does the
// current ruleset actually derive," not just one query's answer, call this
// instead and cache the result themselves (see derivedDB, evaluatedDB).
func (s *session) evaluate(ctx context.Context) (datalog.Database, error) {
	ruleset := syntax.Ruleset{Rules: s.rules, AggRules: s.aggRules}
	t, err := seminaive.New(s.engineOpts...).Compile(ruleset)
	if err != nil {
		return nil, err
	}
	db, err := s.buildDB()
	if err != nil {
		return nil, err
	}
	return t.Transform(ctx, db)
}

// evaluatedDB returns derivedDB if a successful evaluate() has populated it
// since the last rules/schema/fact change, otherwise falls back to
// buildDB's EDB-only snapshot. Fact-browsing callers (the workbench's Fact
// Browser) use this instead of buildDB directly so a predicate's "derived"
// facts reflect the last Run rather than always reading as empty
// (buildDB alone never runs rule evaluation — see doc/features/web-ui.md
// design constraint 2's snapshot-pointer intent). REPL and MCP fact-listing
// paths still call buildDB directly and are unaffected by this cache.
func (s *session) evaluatedDB() (datalog.Database, error) {
	if s.derivedDB != nil {
		return s.derivedDB, nil
	}
	return s.buildDB()
}

// runQuery compiles the session's rules plus a synthetic _q_ rule for q,
// evaluates against the current database, and returns the result rows, the
// query's variable names, and per-stratum evaluation stats. Sorting and
// presentation are left to the caller. Stats are non-nil only when the
// Transform ran to completion.
func (s *session) runQuery(ctx context.Context, q *syntax.Query) (rows [][]datalog.Constant, vars []string, stats []seminaive.StratumStats, err error) {
	vars = extractNamedVars(q.Body)

	// Build synthetic rule: _q_(Var1, ..., VarN) :- body.
	headTerms := make([]datalog.Term, len(vars))
	for i, v := range vars {
		headTerms[i] = datalog.Variable(v)
	}
	synth := syntax.Rule{
		Head: syntax.Atom{Pred: "_q_", Terms: headTerms},
		Body: q.Body,
	}

	allRules := make([]syntax.Rule, len(s.rules)+1)
	copy(allRules, s.rules)
	allRules[len(s.rules)] = synth

	ruleset := syntax.Ruleset{Rules: allRules, AggRules: s.aggRules}
	opts := append(s.engineOpts[:len(s.engineOpts):len(s.engineOpts)],
		seminaive.WithProfile(func(ss []seminaive.StratumStats) { stats = ss }))
	t, err := seminaive.New(opts...).Compile(ruleset)
	if err != nil {
		return nil, vars, nil, err
	}

	db, err := s.buildDB()
	if err != nil {
		return nil, vars, nil, err
	}
	output, err := t.Transform(ctx, db)
	if err != nil {
		return nil, vars, stats, err
	}

	for row := range output.Facts("_q_", len(vars)) {
		rows = append(rows, row)
	}
	return rows, vars, stats, nil
}

func (s *session) allPredicateNames() []string {
	seen := map[string]bool{}
	if s.dataDB != nil {
		for pred := range s.dataDB.Predicates() {
			seen[pred] = true
		}
	}
	for _, f := range s.facts {
		seen[f.Name] = true
	}
	for _, rule := range s.rules {
		seen[rule.Head.Pred] = true
		for _, atom := range rule.Body {
			seen[atom.Pred] = true
		}
	}
	for _, ar := range s.aggRules {
		seen[ar.Head.Pred] = true
	}

	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// extractNamedVars collects unique non-underscore variables from query body atoms,
// preserving order of first occurrence.
func extractNamedVars(body []syntax.Atom) []string {
	var vars []string
	seen := map[string]bool{}
	for _, atom := range body {
		for _, t := range atom.Terms {
			if v, ok := t.(datalog.Variable); ok {
				name := string(v)
				if !seen[name] && !strings.HasPrefix(name, "_") {
					vars = append(vars, name)
					seen[name] = true
				}
			}
		}
		if atom.Pred == "is" && atom.Expr != nil {
			extractExprVars(atom.Expr, &vars, seen)
		}
	}
	return vars
}

func extractExprVars(expr syntax.Expr, vars *[]string, seen map[string]bool) {
	switch e := expr.(type) {
	case syntax.TermExpr:
		if v, ok := e.Term.(datalog.Variable); ok {
			name := string(v)
			if !seen[name] && !strings.HasPrefix(name, "_") {
				*vars = append(*vars, name)
				seen[name] = true
			}
		}
	case syntax.BinExpr:
		extractExprVars(e.Left, vars, seen)
		extractExprVars(e.Right, vars, seen)
	}
}
