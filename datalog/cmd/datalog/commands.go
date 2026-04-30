package main

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
)

// command represents a meta-command handler.
type command struct {
	name string
	help string
	fn   func(r *repl, args string) error
}

func allCommands() []command {
	return []command{
		{".help", "Show available commands", cmdHelp},
		{".load", "Load Datalog statements from a file: .load <file.dl>", cmdLoad},
		{".reload", "Reload data from the configured -c/-d source", cmdReload},
		{".list", "List all predicates with fact counts", cmdList},
		{".rules", "Show defined rules", cmdRules},
		{".facts", "Dump facts for a predicate: .facts <pred>/<arity>", cmdFacts},
		{".clear", "Clear rules and/or facts: .clear [rules|facts|all]", cmdClear},
		{".quit", "Exit the REPL", cmdQuit},
		{".exit", "Exit the REPL", cmdQuit},
	}
}

func cmdHelp(r *repl, _ string) error {
	fmt.Fprintln(r.out, "Commands:")
	for _, c := range allCommands() {
		fmt.Fprintf(r.out, "  %-20s %s\n", c.name, c.help)
	}
	fmt.Fprintln(r.out)
	fmt.Fprintln(r.out, "Datalog statements:")
	fmt.Fprintln(r.out, `  parent("tom", "bob").                 Assert a fact`)
	fmt.Fprintln(r.out, "  ancestor(X, Y) :- parent(X, Y).      Define a rule")
	fmt.Fprintln(r.out, `  ancestor("tom", X)?                   Query`)
	fmt.Fprintln(r.out, "  not parent(?, X)                     Negation in rule body")
	fmt.Fprintln(r.out, "  C = count : person(?, ?).            Aggregate in rule body")
	return nil
}

func cmdReload(r *repl, _ string) error {
	if r.configPath == "" {
		return fmt.Errorf("no data source configured (use -c flag)")
	}
	if err := r.loadData(); err != nil {
		return err
	}
	fmt.Fprintf(r.out, "Reloaded %s\n", r.configPath)
	return nil
}

func cmdLoad(r *repl, args string) error {
	args = strings.TrimSpace(args)
	if args == "" {
		return fmt.Errorf("usage: .load <file.dl>")
	}
	data, err := os.ReadFile(args)
	if err != nil {
		return err
	}
	if err := r.loadProgram(string(data)); err != nil {
		return err
	}
	fmt.Fprintf(r.out, "Loaded %s\n", args)
	return nil
}

func cmdList(r *repl, _ string) error {
	type predArity struct {
		pred  string
		arity int
	}
	counts := map[predArity]int{}
	db, err := r.buildDB()
	if err != nil {
		return err
	}
	for pred, arity := range db.Predicates() {
		key := predArity{pred, arity}
		for range db.Facts(pred, arity) {
			counts[key]++
		}
	}
	if len(counts) == 0 {
		fmt.Fprintln(r.out, "No facts in database.")
		return nil
	}
	keys := make([]predArity, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].pred != keys[j].pred {
			return keys[i].pred < keys[j].pred
		}
		return keys[i].arity < keys[j].arity
	})
	for _, k := range keys {
		fmt.Fprintf(r.out, "  %s/%d  (%d facts)\n", k.pred, k.arity, counts[k])
	}
	return nil
}

func cmdRules(r *repl, _ string) error {
	if len(r.rules) == 0 && len(r.aggRules) == 0 {
		fmt.Fprintln(r.out, "No rules defined.")
		return nil
	}
	for _, rule := range r.rules {
		fmt.Fprintf(r.out, "  %s\n", rule.String())
	}
	for _, ar := range r.aggRules {
		fmt.Fprintf(r.out, "  %s\n", ar.String())
	}
	return nil
}

func cmdFacts(r *repl, args string) error {
	args = strings.TrimSpace(args)
	slash := strings.LastIndex(args, "/")
	if slash < 1 {
		return fmt.Errorf("usage: .facts <pred>/<arity>  (e.g., .facts parent/2)")
	}
	pred := args[:slash]
	var arity int
	if _, err := fmt.Sscanf(args[slash+1:], "%d", &arity); err != nil {
		return fmt.Errorf("usage: .facts <pred>/<arity>  (e.g., .facts parent/2)")
	}
	db, err := r.buildDB()
	if err != nil {
		return err
	}
	count := 0
	for row := range db.Facts(pred, arity) {
		fmt.Fprintf(r.out, "  %s(%s)\n", pred, formatTerms(row))
		count++
	}
	if count == 0 {
		fmt.Fprintf(r.out, "No facts for %s/%d.\n", pred, arity)
	}
	return nil
}

func cmdClear(r *repl, args string) error {
	args = strings.TrimSpace(args)
	switch args {
	case "rules":
		r.rules = nil
		r.aggRules = nil
		fmt.Fprintln(r.out, "Rules cleared.")
	case "facts":
		r.facts = nil
		fmt.Fprintln(r.out, "Facts cleared.")
	case "", "all":
		r.facts = nil
		r.rules = nil
		r.aggRules = nil
		fmt.Fprintln(r.out, "All facts and rules cleared.")
	default:
		return fmt.Errorf("usage: .clear [rules|facts|all]")
	}
	return nil
}

func cmdQuit(_ *repl, _ string) error {
	return io.EOF
}
