package main

import (
	"archive/zip"
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/lmorg/readline/v4"
	"golang.org/x/term"
	"swdunlop.dev/pkg/datalog"
	"swdunlop.dev/pkg/datalog/jsonfacts"
	"swdunlop.dev/pkg/datalog/memory"
	"swdunlop.dev/pkg/datalog/seminaive"
	"swdunlop.dev/pkg/datalog/syntax"
)

type repl struct {
	rl       *readline.Instance
	out      io.Writer
	facts    []datalog.Fact
	rules    []syntax.Rule
	aggRules []syntax.AggregateRule
	engine   *seminaive.Engine

	// Data source loaded via -c / -d flags.
	configPath string
	dataDir    string
	dataDB     *memory.Database // facts loaded from data source (replaced on .reload)
}

func newREPL(eng *seminaive.Engine) *repl {
	rl := readline.NewInstance()

	r := &repl{
		rl:     rl,
		out:    os.Stdout,
		engine: eng,
	}

	rl.TabCompleter = r.tabComplete

	// Persist history to disk when possible.
	if dir, err := os.UserCacheDir(); err == nil {
		histDir := filepath.Join(dir, "datalog")
		if err := os.MkdirAll(histDir, 0700); err == nil {
			rl.History = loadFileHistory(filepath.Join(histDir, "history"))
		}
	}

	return r
}

// isStatementComplete reports whether the accumulated input is a complete
// Datalog statement or dot-command — i.e. ready for evaluation.
func isStatementComplete(text string) bool {
	text = strings.TrimSpace(text)
	if text == "" || strings.HasPrefix(text, ".") {
		return true
	}
	return strings.HasSuffix(text, ".") || strings.HasSuffix(text, "?")
}

// run starts the REPL loop. Returns nil on clean exit.
// If stdin is not a terminal, it reads lines directly without readline.
func (r *repl) run() error {
	if !term.IsTerminal(int(os.Stdin.Fd())) {
		return r.runPipe()
	}

	fmt.Fprintln(r.out, "datalog — Datalog REPL")
	fmt.Fprintln(r.out, "Type .help for commands, .quit to exit.")
	fmt.Fprintln(r.out)

	var buf strings.Builder
	r.rl.SetPrompt("?> ")
	for {
		line, err := r.rl.Readline()
		if err != nil {
			if errors.Is(err, io.EOF) {
				if buf.Len() > 0 {
					buf.Reset()
					r.rl.SetPrompt("?> ")
					continue
				}
				fmt.Fprintln(r.out)
				return nil
			}
			if errors.Is(err, readline.ErrCtrlC) {
				buf.Reset()
				r.rl.SetPrompt("?> ")
				continue
			}
			return err
		}

		if buf.Len() > 0 {
			buf.WriteByte(' ')
		}
		buf.WriteString(line)

		full := strings.TrimSpace(buf.String())
		if full == "" {
			buf.Reset()
			r.rl.SetPrompt("?> ")
			continue
		}
		if !isStatementComplete(full) {
			r.rl.SetPrompt(".. ")
			continue
		}
		buf.Reset()
		r.rl.SetPrompt("?> ")

		if strings.HasPrefix(full, ".") {
			if err := r.dispatchCommand(full); err != nil {
				if err == io.EOF {
					return nil
				}
				fmt.Fprintf(r.out, "error: %v\n", err)
			}
			continue
		}

		if err := r.execStatement(full); err != nil {
			fmt.Fprintf(r.out, "error: %v\n", err)
		}
	}
}

// runPipe reads Datalog statements from stdin when not connected to a terminal.
// Multi-line statements are accumulated until a line ending with '.' is seen.
func (r *repl) runPipe() error {
	scanner := bufio.NewScanner(os.Stdin)
	var buf strings.Builder
	for scanner.Scan() {
		line := scanner.Text()
		text := strings.TrimSpace(line)
		if text == "" {
			continue
		}
		if buf.Len() > 0 {
			buf.WriteByte(' ')
		}
		buf.WriteString(text)
		full := strings.TrimSpace(buf.String())
		if strings.HasPrefix(full, ".") || strings.HasSuffix(full, ".") || strings.HasSuffix(full, "?") {
			buf.Reset()
			if strings.HasPrefix(full, ".") {
				if err := r.dispatchCommand(full); err != nil {
					if err == io.EOF {
						return nil
					}
					fmt.Fprintf(r.out, "error: %v\n", err)
				}
			} else {
				if err := r.execStatement(full); err != nil {
					fmt.Fprintf(r.out, "error: %v\n", err)
				}
			}
		}
	}
	return scanner.Err()
}

// loadProgram parses a Datalog source string containing multiple statements,
// adding facts and rules to the REPL state and executing any queries.
func (r *repl) loadProgram(src string) error {
	ruleset, err := syntax.ParseAll(src)
	if err != nil {
		return err
	}
	for _, rule := range ruleset.Rules {
		if rule.IsFact() {
			r.facts = append(r.facts, rule.ToFact())
		} else {
			r.rules = append(r.rules, rule)
		}
	}
	r.aggRules = append(r.aggRules, ruleset.AggRules...)
	for _, q := range ruleset.Queries {
		if err := r.execQuery(&q); err != nil {
			return err
		}
	}
	return nil
}

func (r *repl) execStatement(text string) error {
	result, err := syntax.ParseStatement(text)
	if err != nil {
		return err
	}

	switch v := result.(type) {
	case *syntax.Rule:
		if v.IsFact() {
			fact := v.ToFact()
			r.facts = append(r.facts, fact)
			fmt.Fprintf(r.out, "  fact: %s\n", v.String())
		} else {
			r.rules = append(r.rules, *v)
			fmt.Fprintf(r.out, "  rule: %s\n", v.String())
		}

	case *syntax.AggregateRule:
		r.aggRules = append(r.aggRules, *v)
		fmt.Fprintf(r.out, "  aggregate rule: %s\n", v.String())

	case *syntax.Query:
		return r.execQuery(v)

	default:
		return fmt.Errorf("unexpected parse result: %T", result)
	}

	return nil
}

func (r *repl) execQuery(q *syntax.Query) error {
	vars := extractNamedVars(q.Body)

	// Build synthetic rule: _q_(Var1, ..., VarN) :- body.
	headTerms := make([]datalog.Term, len(vars))
	for i, v := range vars {
		headTerms[i] = datalog.Variable(v)
	}
	synth := syntax.Rule{
		Head: syntax.Atom{Pred: "_q_", Terms: headTerms},
		Body: q.Body,
	}

	allRules := make([]syntax.Rule, len(r.rules)+1)
	copy(allRules, r.rules)
	allRules[len(r.rules)] = synth

	ruleset := syntax.Ruleset{Rules: allRules, AggRules: r.aggRules}
	t, err := r.engine.Compile(ruleset)
	if err != nil {
		return err
	}

	db, err := r.buildDB()
	if err != nil {
		return err
	}
	output, err := t.Transform(context.Background(), db)
	if err != nil {
		return err
	}

	var results [][]datalog.Constant
	for row := range output.Facts("_q_", len(vars)) {
		results = append(results, row)
	}

	if len(results) == 0 {
		fmt.Fprintln(r.out, "  no results.")
		return nil
	}

	if len(vars) == 0 {
		fmt.Fprintf(r.out, "  true (%d results)\n", len(results))
		return nil
	}

	// Sort results for deterministic output.
	sort.Slice(results, func(i, j int) bool {
		for k := range vars {
			a := results[i][k].String()
			b := results[j][k].String()
			if a != b {
				return a < b
			}
		}
		return false
	})

	for _, row := range results {
		var parts []string
		for i, v := range vars {
			parts = append(parts, fmt.Sprintf("%s = %s", v, row[i].String()))
		}
		fmt.Fprintf(r.out, "  %s\n", strings.Join(parts, ", "))
	}
	fmt.Fprintf(r.out, "  (%d results)\n", len(results))
	return nil
}

// setDataSource configures the REPL to load facts from a jsonfacts config.
func (r *repl) setDataSource(configPath, dataDir string) {
	r.configPath = configPath
	r.dataDir = dataDir
}

// loadData loads (or reloads) facts from the configured data source.
func (r *repl) loadData() error {
	if r.configPath == "" {
		return fmt.Errorf("no data source configured (use -c flag)")
	}

	cfg, err := loadConfig(r.configPath)
	if err != nil {
		return fmt.Errorf("config %s: %w", r.configPath, err)
	}

	var db *memory.Database
	if strings.HasSuffix(r.dataDir, ".zip") {
		db, err = loadFromZip(cfg, r.dataDir)
	} else {
		db, err = cfg.LoadDir(r.dataDir)
	}
	if err != nil {
		return fmt.Errorf("loading data from %s: %w", r.dataDir, err)
	}

	r.dataDB = db
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

func (r *repl) buildDB() (*memory.Database, error) {
	if r.dataDB != nil {
		if len(r.facts) == 0 {
			return r.dataDB, nil
		}
		return r.dataDB.Extend(r.facts...)
	}
	b := memory.NewBuilder()
	for _, f := range r.facts {
		if err := b.AddFact(f); err != nil {
			return nil, err
		}
	}
	return b.Build(), nil
}

// tabComplete provides tab completions for the readline instance.
// Suggestions are returned as the suffix to insert at the cursor position
// (lmorg's tab completer appends suggestions verbatim, so we crop the
// already-typed portion of the current word).
func (r *repl) tabComplete(line []rune, cursor int, _ readline.DelayedTabContext) *readline.TabCompleterReturnT {
	ret := &readline.TabCompleterReturnT{}
	before := string(line[:cursor])
	word := wordAtCursor(before)
	trimmed := strings.TrimSpace(before)

	// Dot-command completions when the line starts with '.'
	if strings.HasPrefix(trimmed, ".") {
		// File path completion for .load
		if strings.HasPrefix(trimmed, ".load ") {
			return completeFilePath(strings.TrimSpace(trimmed[len(".load"):]))
		}

		ret.Descriptions = map[string]string{}
		for _, cmd := range allCommands() {
			if !strings.HasPrefix(cmd.name, word) {
				continue
			}
			suffix := cmd.name[len(word):]
			ret.Suggestions = append(ret.Suggestions, suffix)
			ret.Descriptions[suffix] = cmd.help
		}
		ret.Prefix = word
		return ret
	}

	// Predicate name completions for Datalog statements.
	for _, name := range r.allPredicateNames() {
		if !strings.HasPrefix(name, word) {
			continue
		}
		ret.Suggestions = append(ret.Suggestions, name[len(word):])
	}
	ret.Prefix = word
	return ret
}

// completeFilePath returns completions for a partial file path.
func completeFilePath(partial string) *readline.TabCompleterReturnT {
	ret := &readline.TabCompleterReturnT{}
	dir := filepath.Dir(partial)
	if dir == "" {
		dir = "."
	}
	base := filepath.Base(partial)
	if partial == "" || strings.HasSuffix(partial, string(filepath.Separator)) {
		dir = partial
		if dir == "" {
			dir = "."
		}
		base = ""
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return ret
	}

	for _, e := range entries {
		name := e.Name()
		if strings.HasPrefix(name, ".") && !strings.HasPrefix(base, ".") {
			continue
		}
		if base != "" && !strings.HasPrefix(name, base) {
			continue
		}
		path := filepath.Join(dir, name)
		if dir == "." {
			path = name
		}
		if e.IsDir() {
			path += string(filepath.Separator)
		}
		ret.Suggestions = append(ret.Suggestions, path[len(partial):])
	}
	ret.Prefix = partial
	return ret
}

func (r *repl) allPredicateNames() []string {
	seen := map[string]bool{}
	if r.dataDB != nil {
		for pred := range r.dataDB.Predicates() {
			seen[pred] = true
		}
	}
	for _, f := range r.facts {
		seen[f.Name] = true
	}
	for _, rule := range r.rules {
		seen[rule.Head.Pred] = true
		for _, atom := range rule.Body {
			seen[atom.Pred] = true
		}
	}
	for _, ar := range r.aggRules {
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

func formatTerms(terms []datalog.Constant) string {
	parts := make([]string, len(terms))
	for i, t := range terms {
		parts[i] = t.String()
	}
	return strings.Join(parts, ", ")
}

func wordAtCursor(s string) string {
	i := strings.LastIndexAny(s, " \t(,")
	if i < 0 {
		return s
	}
	return s[i+1:]
}

func (r *repl) dispatchCommand(line string) error {
	parts := strings.SplitN(line, " ", 2)
	name := parts[0]
	args := ""
	if len(parts) > 1 {
		args = parts[1]
	}
	for _, cmd := range allCommands() {
		if cmd.name == name {
			return cmd.fn(r, args)
		}
	}
	return fmt.Errorf("unknown command: %s (type .help for commands)", name)
}

// fileHistory persists REPL line history to a file, appending each new entry.
type fileHistory struct {
	items []string
	path  string
}

func loadFileHistory(path string) *fileHistory {
	h := &fileHistory{path: path}
	if data, err := os.ReadFile(path); err == nil {
		for line := range strings.SplitSeq(string(data), "\n") {
			if line != "" {
				h.items = append(h.items, line)
			}
		}
	}
	return h
}

func (h *fileHistory) Write(s string) (int, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return len(h.items), nil
	}
	if n := len(h.items); n > 0 && h.items[n-1] == s {
		return n, nil
	}
	h.items = append(h.items, s)
	if h.path != "" {
		if f, err := os.OpenFile(h.path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0600); err == nil {
			fmt.Fprintln(f, s)
			f.Close()
		}
	}
	return len(h.items), nil
}

func (h *fileHistory) GetLine(i int) (string, error) {
	if i < 0 || i >= len(h.items) {
		return "", fmt.Errorf("history index %d out of range", i)
	}
	return h.items[i], nil
}

func (h *fileHistory) Len() int { return len(h.items) }

func (h *fileHistory) Dump() any { return h.items }
