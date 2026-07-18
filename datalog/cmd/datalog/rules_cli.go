package main

import (
	"bufio"
	stdflag "flag"
	"fmt"
	"os"
	"sort"
	"strings"
)

// runRules implements the `datalog rules` subcommand namespace: Import
// (split a monolithic .dl into a rules/ directory store) and Export
// (concatenate a store back into one document), the human-only interchange
// path doc/features/workbench-v2.md design decision 4 assigns to
// monolithic .dl files once the directory store becomes canonical. args
// excludes the "rules" argument itself (main.go strips it before
// dispatching here); args[0] must be "import" or "export".
func runRules(args []string) {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "datalog rules: expected a subcommand: import or export")
		os.Exit(1)
	}
	switch args[0] {
	case "import":
		runRulesImport(args[1:])
	case "export":
		runRulesExport(args[1:])
	default:
		fmt.Fprintf(os.Stderr, "datalog rules: unknown subcommand %q: expected import or export\n", args[0])
		os.Exit(1)
	}
}

// runRulesImport implements `datalog rules import --rules <dir> [-y]
// <monolith.dl>...` (flags first: stdlib flag stops parsing at the first
// positional argument, so flags AFTER the input files would be silently
// treated as file names): parses and concatenates every input file (multiple inputs
// are treated as one document, in argument order — the same convention
// bare mode's positional rule files already use), splits it into group
// files (rulestore.go's importRuleset, via splitRuleset), refuses if the
// target directory exists and is non-empty, prints the planned file list,
// and — unless -y was given — asks for interactive confirmation on the
// terminal before writing anything.
func runRulesImport(args []string) {
	flags := stdflag.NewFlagSet("rules import", stdflag.ExitOnError)
	rulesDir := flags.String("rules", "", "target rules/ directory store to create (required)")
	yes := flags.Bool("y", false, "skip the confirmation prompt")
	if err := flags.Parse(args); err != nil {
		os.Exit(0)
	}

	inputs := flags.Args()
	if len(inputs) == 0 {
		fmt.Fprintln(os.Stderr, "datalog rules import: at least one monolithic .dl file is required")
		os.Exit(1)
	}
	if *rulesDir == "" {
		fmt.Fprintln(os.Stderr, "datalog rules import: --rules <dir> is required")
		os.Exit(1)
	}

	// Import is an explicit human action operating on trusted local files,
	// so the same reserved-predicate/detached-doc/query checks setRules
	// applies to an agent's document apply here too — parseUserProgram is
	// the one shared gate (session.go's doc comment on it), not
	// syntax.ParseAll directly, so a monolith that happens to use the
	// reserved query predicate is refused before it can be split at all.
	var concatenated strings.Builder
	for i, in := range inputs {
		data, err := os.ReadFile(in)
		if err != nil {
			fmt.Fprintf(os.Stderr, "datalog rules import: reading %s: %v\n", in, err)
			os.Exit(1)
		}
		if i > 0 {
			concatenated.WriteString("\n\n")
		}
		concatenated.Write(data)
	}

	ruleset, err := parseUserProgram(concatenated.String())
	if err != nil {
		fmt.Fprintf(os.Stderr, "datalog rules import: %v\n", err)
		os.Exit(1)
	}

	empty, err := dirIsEmpty(*rulesDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "datalog rules import: checking %s: %v\n", *rulesDir, err)
		os.Exit(1)
	}
	if !empty {
		fmt.Fprintf(os.Stderr, "datalog rules import: %s already exists and is not empty; "+
			"remove it or choose an empty target\n", *rulesDir)
		os.Exit(1)
	}

	// splitRuleset here is a dry run purely to print the file list and let
	// splitRuleset-level errors (embedded queries, detached docs, filename
	// collisions) surface before the confirmation prompt rather than after
	// the user says yes; importRuleset re-derives the same split internally
	// when it actually writes, so there is no state to thread between the
	// two calls.
	_, order, err := splitRuleset(ruleset)
	if err != nil {
		fmt.Fprintf(os.Stderr, "datalog rules import: %v\n", err)
		os.Exit(1)
	}
	planned := plannedFilenames(order)

	fmt.Fprintf(os.Stderr, "datalog rules import: will write %d file(s) to %s:\n", len(planned), *rulesDir)
	for _, name := range planned {
		fmt.Fprintf(os.Stderr, "  %s\n", name)
	}

	if !*yes {
		if !confirm(os.Stdin, os.Stderr, "proceed?") {
			fmt.Fprintln(os.Stderr, "datalog rules import: aborted")
			os.Exit(1)
		}
	}

	written, err := importRuleset(ruleset, *rulesDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "datalog rules import: %v\n", err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "datalog rules import: wrote %d file(s) to %s\n", len(written), *rulesDir)
}

// plannedFilenames renders order (as produced by splitRuleset) as sorted
// filenames, for the import preview printed before the confirmation prompt.
func plannedFilenames(order []groupKey) []string {
	names := make([]string, len(order))
	for i, k := range order {
		names[i] = k.filename()
	}
	sort.Strings(names)
	return names
}

// runRulesExport implements `datalog rules export --rules <dir> [-o
// <file>]`: loads the store the same way every other rulesDir consumer
// does (loadRuleStore — the load/validate path is shared, not
// reimplemented here) and writes its concatenation to -o, or stdout by
// default.
func runRulesExport(args []string) {
	flags := stdflag.NewFlagSet("rules export", stdflag.ExitOnError)
	rulesDir := flags.String("rules", "", "rules/ directory store to export (required)")
	out := flags.String("o", "", "output file (default: stdout)")
	if err := flags.Parse(args); err != nil {
		os.Exit(0)
	}

	if *rulesDir == "" {
		fmt.Fprintln(os.Stderr, "datalog rules export: --rules <dir> is required")
		os.Exit(1)
	}

	store, err := loadRuleStore(*rulesDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "datalog rules export: %v\n", err)
		os.Exit(1)
	}

	text := store.export()

	if *out == "" {
		fmt.Println(text)
		return
	}
	if err := os.WriteFile(*out, []byte(text+"\n"), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "datalog rules export: writing %s: %v\n", *out, err)
		os.Exit(1)
	}
}

// confirm prints prompt to w followed by " [y/N] " and reads one line from
// r, returning true only for an explicit y/yes (case-insensitive) —
// anything else, including a blank line or EOF, is a "no", matching the
// brief's "-y or interactive y/N" contract (an unattended pipe with no -y
// must refuse, not hang or default to yes).
func confirm(r *os.File, w *os.File, prompt string) bool {
	fmt.Fprintf(w, "%s [y/N] ", prompt)
	scanner := bufio.NewScanner(r)
	if !scanner.Scan() {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(scanner.Text())) {
	case "y", "yes":
		return true
	default:
		return false
	}
}
