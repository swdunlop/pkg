package main

import (
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
	"swdunlop.dev/pkg/datalog/seminaive"
	"swdunlop.dev/pkg/datalog/syntax"
)

// repl wraps a session with terminal interaction: readline, tab
// completion, history, dot-commands, and result formatting.
type repl struct {
	*session
	rl      *readline.Instance
	out     io.Writer
	profile bool // print per-stratum stats after each query (.profile)
}

func newREPL(opts ...seminaive.Option) *repl {
	rl := readline.NewInstance()

	r := &repl{
		session: &session{engineOpts: opts},
		rl:      rl,
		out:     os.Stdout,
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

// loadProgram loads a Datalog source string into the session and executes
// any queries it contains, printing their results.
func (r *repl) loadProgram(src string) error {
	queries, err := r.session.loadProgram(src)
	if err != nil {
		return err
	}
	for _, q := range queries {
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
	if err := validateStatementNoReservedPred(result); err != nil {
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

// execQuery evaluates a query through the session and prints the results.
func (r *repl) execQuery(q *syntax.Query) error {
	results, vars, stats, err := r.runQuery(context.Background(), q)
	if err != nil {
		return err
	}

	if r.profile {
		r.printProfile(stats)
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

// printProfile renders per-stratum evaluation statistics after a query.
func (r *repl) printProfile(stats []seminaive.StratumStats) {
	for i, s := range stats {
		fmt.Fprintf(r.out, "  stratum %d [%s]: %d rules, %d aggregates, %d facts, %d iterations, %s\n",
			i, strings.Join(s.Predicates, " "), s.RuleCount, s.AggCount, s.FactCount, s.Iterations, s.Duration)
	}
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

// completeFilePath returns completions for a partial file path. Suggestions
// are the suffix readline should APPEND to partial verbatim (see
// tabComplete's doc comment), so this must never reconstruct the directory
// portion via filepath.Dir/Base/Join: those normalize away exactly the
// prefix partial still carries (e.g. "./x" -> Dir "."), and slicing that
// normalized reconstruction by len(partial) either produces suggestions
// misaligned with what the user actually typed or, when the normalized
// path is shorter than partial (e.g. partial "./x" against a one-character
// file "x"), panics with a slice-bounds-out-of-range that readline has no
// recover for — taking the whole REPL process down. Splitting partial on
// its own last separator instead keeps dirPart exactly as typed (including
// any "./" or "../"), so a suggestion of name[len(base):] appended after
// partial always reconstructs dirPart+name correctly, with no
// reconstruction or re-slicing of the typed prefix at all.
func completeFilePath(partial string) *readline.TabCompleterReturnT {
	ret := &readline.TabCompleterReturnT{}

	dirPart, base := "", partial
	if i := strings.LastIndexByte(partial, filepath.Separator); i >= 0 {
		dirPart, base = partial[:i+1], partial[i+1:]
	}
	dir := dirPart
	if dir == "" {
		dir = "."
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
		suggestion := name[len(base):]
		if e.IsDir() {
			suggestion += string(filepath.Separator)
		}
		ret.Suggestions = append(ret.Suggestions, suggestion)
	}
	ret.Prefix = partial
	return ret
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

// replHistoryLimit bounds the in-memory REPL history; the history file is
// compacted back down to this many lines once appends grow it past twice
// the limit, so the file stays under ~2x the bound too.
const replHistoryLimit = 1000

// fileHistory persists REPL line history to a file, appending each new
// entry and compacting per replHistoryLimit. Write failures are reported
// once per session rather than per keystroke.
type fileHistory struct {
	items     []string
	path      string
	fileLines int
	warned    bool
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
	h.fileLines = len(h.items)
	if n := len(h.items); n > replHistoryLimit {
		h.items = append([]string(nil), h.items[n-replHistoryLimit:]...)
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
	if n := len(h.items); n > replHistoryLimit {
		h.items = h.items[n-replHistoryLimit:]
	}
	h.persist(s)
	return len(h.items), nil
}

// persist appends s to the history file, or rewrites the file with the
// retained tail when appends have grown it past 2x replHistoryLimit.
func (h *fileHistory) persist(s string) {
	if h.path == "" {
		return
	}
	if h.fileLines+1 > 2*replHistoryLimit {
		data := strings.Join(h.items, "\n") + "\n"
		if err := os.WriteFile(h.path, []byte(data), 0600); err != nil {
			h.warn(err)
			return
		}
		h.fileLines = len(h.items)
		return
	}
	f, err := os.OpenFile(h.path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0600)
	if err != nil {
		h.warn(err)
		return
	}
	if _, err := fmt.Fprintln(f, s); err != nil {
		h.warn(err)
	}
	if err := f.Close(); err != nil {
		h.warn(err)
	}
	h.fileLines++
}

func (h *fileHistory) warn(err error) {
	if h.warned {
		return
	}
	h.warned = true
	fmt.Fprintf(os.Stderr, "datalog: cannot save REPL history: %v\n", err)
}

func (h *fileHistory) GetLine(i int) (string, error) {
	if i < 0 || i >= len(h.items) {
		return "", fmt.Errorf("history index %d out of range", i)
	}
	return h.items[i], nil
}

func (h *fileHistory) Len() int { return len(h.items) }

func (h *fileHistory) Dump() any { return h.items }
