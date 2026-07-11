package main

import (
	"archive/zip"
	"context"
	"encoding/json"
	"fmt"
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
	if ext == ".yaml" || ext == ".yml" {
		var raw any
		if err := yaml.Unmarshal(data, &raw); err != nil {
			return jsonfacts.Config{}, fmt.Errorf("parsing YAML: %w", err)
		}
		var err error
		data, err = json.Marshal(raw)
		if err != nil {
			return jsonfacts.Config{}, fmt.Errorf("converting YAML to JSON: %w", err)
		}
	}

	var cfg jsonfacts.Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return jsonfacts.Config{}, fmt.Errorf("parsing config: %w", err)
	}
	return cfg, nil
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
