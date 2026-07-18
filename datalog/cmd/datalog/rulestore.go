package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"swdunlop.dev/pkg/datalog/syntax"
)

// errRulesSourceConflict is returned by rulesSourceConflict when both a
// --rules directory and one or more positional rule files were given —
// the one error message shared verbatim by `datalog serve` and `datalog
// mcp` (mcp.go's runMCP, serve.go's runServe), so the wording and the
// condition it fires on can't drift between the two dispatch sites.
var errRulesSourceConflict = fmt.Errorf("use --rules or positional rule files, not both")

// rulesSourceConflict reports whether rulesDir and ruleFiles were BOTH
// given — the two ways of naming a session's rules are mutually exclusive
// (doc/features/workbench-v2.md work item 1), since there is no sensible
// way to merge "load this directory" with "also load these specific
// files" without inventing an ordering the directory store doesn't have.
func rulesSourceConflict(rulesDir string, ruleFiles []string) error {
	if rulesDir != "" && len(ruleFiles) > 0 {
		return errRulesSourceConflict
	}
	return nil
}

// groupKey identifies one rule group: all rules and aggregate rules sharing
// a head predicate name and arity (doc/features/workbench-v2.md design
// decision 4, "The monolithic ruleset file is retired" — the canonical
// store is one `.dl` file per (predicate, arity) pair).
type groupKey struct {
	Head  string
	Arity int
}

// filename returns k's canonical group-file name, "<head>_<arity>.dl".
//
// The scheme is injective — no two distinct groupKeys ever produce the same
// filename — because Head is always a datalog identifier ([A-Za-z0-9_]+,
// syntax/parse.go's isIdentChar) and Arity is always rendered as a decimal
// integer with no leading zeros, so the LAST underscore-delimited segment
// of the stem is always all-digits and always exactly Arity: an identifier
// byte is never confused with the delimiter we chose to split on, since we
// always split from the right at the LAST underscore. Concretely:
// k={"foo_2", 1} yields "foo_2_1.dl" (stem "foo_2_1"; splitting at the
// final "_" leaves head "foo_2", arity "1"), while k={"foo", 21} yields
// "foo_21.dl" (stem "foo_21"; splitting at the final "_" leaves head "foo",
// arity "21") — these are different filenames, and parseGroupFilename
// recovers the exact original key from either, because it also always
// splits at the LAST underscore: whichever underscore filename() inserted
// to join head and arity is necessarily the last one in the stem (Head
// itself may contain underscores, as in "foo_2", but never introduces a
// TRAILING all-digit run, since Head is validated to be a legal predicate
// identifier and Arity's decimal rendering has no leading zero, so the two
// halves can't be confused).
func (k groupKey) filename() string {
	return fmt.Sprintf("%s_%d.dl", k.Head, k.Arity)
}

// parseGroupFilename is filename's inverse: given a base name (with or
// without the ".dl" suffix), it strips ".dl", then splits the stem at its
// LAST underscore, requiring everything after that underscore to be a
// non-empty run of ASCII digits (the arity) and everything before it to be
// a non-empty, legal predicate identifier (the head). See filename's doc
// comment for why splitting at the last underscore is always correct.
func parseGroupFilename(name string) (groupKey, error) {
	stem := strings.TrimSuffix(name, ".dl")
	if stem == name {
		return groupKey{}, fmt.Errorf("%s: rule group filenames must end in .dl", name)
	}
	i := strings.LastIndexByte(stem, '_')
	if i < 0 {
		return groupKey{}, fmt.Errorf("%s: expected <head>_<arity>.dl", name)
	}
	head, arityStr := stem[:i], stem[i+1:]
	if head == "" {
		return groupKey{}, fmt.Errorf("%s: empty predicate name before the arity suffix", name)
	}
	if arityStr == "" {
		return groupKey{}, fmt.Errorf("%s: empty arity suffix", name)
	}
	for j := 0; j < len(arityStr); j++ {
		if arityStr[j] < '0' || arityStr[j] > '9' {
			return groupKey{}, fmt.Errorf("%s: expected <head>_<arity>.dl, %q is not a decimal arity", name, arityStr)
		}
	}
	arity, err := strconv.Atoi(arityStr)
	if err != nil {
		return groupKey{}, fmt.Errorf("%s: %w", name, err)
	}
	for j := 0; j < len(head); j++ {
		b := head[j]
		ok := (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || b == '_'
		if !ok {
			return groupKey{}, fmt.Errorf("%s: %q is not a valid predicate name", name, head)
		}
	}
	return groupKey{Head: head, Arity: arity}, nil
}

// statement is the ordering-preserving union splitRuleset walks: each
// element is either a rule or an aggregate rule from the parsed Ruleset,
// kept in original parse order so a group's Text preserves relative
// statement order exactly, per design decision 4 ("Preserve relative order
// within a group").
type statement interface {
	headKey() groupKey
	String() string
}

type ruleStmt struct{ syntax.Rule }

func (s ruleStmt) headKey() groupKey {
	return groupKey{Head: s.Head.Pred, Arity: len(s.Head.Terms)}
}

type aggStmt struct{ syntax.AggregateRule }

func (s aggStmt) headKey() groupKey {
	return groupKey{Head: s.Head.Pred, Arity: len(s.Head.Terms)}
}

// ruleGroup is one group file's parsed content: every Rule/AggregateRule
// statement whose head is (Head, Arity), in original source order (Stmts),
// plus Rules/AggRules split out by concrete type for callers that need
// typed access (session.setRules wants []syntax.Rule/[]syntax.AggregateRule
// directly), and Text, the exact canonical file content Export concatenates
// and Load reparses: each statement's String() form (round-trips '%%' docs
// exactly — see syntax.writeDoc and the package's fuzz tests), one per
// line in Stmts order, joined by a single '\n', no trailing blank line.
type ruleGroup struct {
	Key      groupKey
	Stmts    []statement
	Rules    []syntax.Rule
	AggRules []syntax.AggregateRule
	Text     string
}

// splitRuleset partitions rs into per-group ruleGroups, keyed by each
// statement's head (predicate, arity), preserving relative order within a
// group. It rejects (all-or-nothing, per design decision 4's "Rejections"):
//
//   - embedded '?' queries: a group file has no place for them (Import is
//     an explicit human action, so this is an error, not a warning);
//   - detached '%%' doc blocks (rs.Warnings): the store forbids free-floating
//     file-level comments, so a detached block — one the parser could not
//     attach to a following statement — must be fixed by the author
//     (attach it, or downgrade to a plain '%' comment) before Import;
//   - case-insensitive filename collisions between two DISTINCT group keys
//     (macOS/Windows case-insensitive filesystems would silently merge
//     them) — rejected rather than guessed at, per the spec's Risks
//     section on group-file naming collisions.
func splitRuleset(rs syntax.Ruleset) (map[groupKey]*ruleGroup, []groupKey, error) {
	if len(rs.Queries) > 0 {
		return nil, nil, fmt.Errorf("rules store: %d embedded query statement(s) ('?') cannot be imported into "+
			"a rule group file; remove them (use the REPL or the query tool to run queries) and re-import", len(rs.Queries))
	}
	if len(rs.Warnings) > 0 {
		return nil, nil, fmt.Errorf("rules store: %d detached '%%%%' doc comment block(s) found; the rule store "+
			"forbids free-floating file-level comments -- attach each block to the statement immediately "+
			"following it, or change it to a plain '%%' comment, then re-import: %s",
			len(rs.Warnings), strings.Join(rs.Warnings, "; "))
	}

	var stmts []statement
	for _, r := range rs.Rules {
		stmts = append(stmts, ruleStmt{r})
	}
	for _, ar := range rs.AggRules {
		stmts = append(stmts, aggStmt{ar})
	}

	groups := map[groupKey]*ruleGroup{}
	var order []groupKey
	for _, s := range stmts {
		k := s.headKey()
		g, ok := groups[k]
		if !ok {
			g = &ruleGroup{Key: k}
			groups[k] = g
			order = append(order, k)
		}
		g.Stmts = append(g.Stmts, s)
		switch v := s.(type) {
		case ruleStmt:
			g.Rules = append(g.Rules, v.Rule)
		case aggStmt:
			g.AggRules = append(g.AggRules, v.AggregateRule)
		}
	}

	if err := checkFilenameCollisions(order); err != nil {
		return nil, nil, err
	}

	for _, k := range order {
		groups[k].Text = renderGroupText(groups[k])
	}
	return groups, order, nil
}

// checkFilenameCollisions rejects the split/load if two distinct keys in
// keys would produce the same on-disk filename under strings.EqualFold
// (case-insensitive filesystems, macOS/Windows safety). Two EQUAL keys
// naturally share one filename by construction and are not a collision;
// this only fires when two keys differ but fold to the same name.
func checkFilenameCollisions(keys []groupKey) error {
	folded := map[string]groupKey{}
	for _, k := range keys {
		name := k.filename()
		fk := strings.ToLower(name)
		if prior, ok := folded[fk]; ok && prior != k {
			return fmt.Errorf("rules store: rule groups %s/%d (%s) and %s/%d (%s) collide under "+
				"case-insensitive filenames -- rename one of the predicates before importing",
				prior.Head, prior.Arity, prior.filename(), k.Head, k.Arity, k.filename())
		}
		folded[fk] = k
	}
	return nil
}

// renderGroupText renders g's canonical file content: each statement's
// String() form, in g.Stmts order (the original parse order), one per
// line, joined by a single '\n' with no trailing blank line.
func renderGroupText(g *ruleGroup) string {
	var buf strings.Builder
	for i, s := range g.Stmts {
		if i > 0 {
			buf.WriteByte('\n')
		}
		buf.WriteString(s.String())
	}
	return buf.String()
}

// ruleStoreGroup is one loaded group file: the parsed statements (used by
// loadRuleStore's head validation; the session itself is populated by
// re-loading Text through session.loadProgram, the one ingest chokepoint —
// see newMCPHandlers) plus the RAW on-disk Text (verbatim except trailing
// newlines, per loadRuleStore's doc) and the File name it came from (for
// error messages and for a later CRUD layer's per-file revision tracking —
// see the seam noted on ruleStore below).
type ruleStoreGroup struct {
	Key      groupKey
	File     string // base filename, e.g. "at_risk_2.dl"
	Text     string
	Rules    []syntax.Rule
	AggRules []syntax.AggregateRule
}

// ruleStore is a loaded rules/ directory: every group file, parsed and
// validated, keyed by groupKey and also available in filename order. This
// is the load-time result threaded into a session (session.rulesText is set
// to ruleStore.export()) and is the seam a later task's CRUD layer builds
// on: per-group Text is already isolated here, so adding revision counters
// and put/get/delete methods needs no change to how loading works — see
// doc/features/workbench-v2.md work item 1's note that this task leaves
// "a small, obvious seam, not speculative machinery."
type ruleStore struct {
	Dir    string
	Order  []groupKey // filename order, matching directory listing order
	Groups map[groupKey]*ruleStoreGroup
}

// export concatenates every group's Text in filename order, exactly as
// exportGroups does for an in-memory split — the store's own Export
// operation, and also how a loaded rules/ directory's content is rendered
// into session.rulesText for every existing surface (REPL history, the
// workbench's Rules pane, etc.) that expects one document.
func (rs *ruleStore) export() string {
	var buf strings.Builder
	for i, k := range rs.Order {
		if i > 0 {
			buf.WriteString("\n\n")
		}
		buf.WriteString(rs.Groups[k].Text)
	}
	return buf.String()
}

// loadRuleStore reads every "*.dl" file directly inside dir (non-recursive),
// sorted by filename, parses each, and validates it into a ruleStore.
// All-or-nothing: any single file's failure aborts the whole load with an
// error naming that file, so a later fsnotify-triggered reload (a follow-up
// task) can rely on "validate everything, then swap" to keep the last-good
// store in place when an in-progress edit is only half-written.
//
// Per file, in addition to splitRuleset's own rejections (embedded queries,
// detached '%%' doc blocks), loadRuleStore requires:
//
//   - the filename parses as a valid group key (parseGroupFilename);
//   - every statement in the file has EXACTLY that head (both predicate
//     name and arity) — a file is not allowed to contain a mix of heads,
//     or a head that doesn't match its own filename, since the filename
//     IS the group's identity in this store.
func loadRuleStore(dir string) (*ruleStore, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("rules store: reading %s: %w", dir, err)
	}

	var names []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if filepath.Ext(e.Name()) != ".dl" {
			continue
		}
		names = append(names, e.Name())
	}
	sort.Strings(names)

	rs := &ruleStore{Dir: dir, Groups: map[groupKey]*ruleStoreGroup{}}
	for _, name := range names {
		key, err := parseGroupFilename(name)
		if err != nil {
			return nil, fmt.Errorf("rules store: %w", err)
		}
		// filename() is injective over groupKeys, but parseGroupFilename is
		// deliberately lenient about spellings it would never emit (a
		// leading-zero arity like "foo_01.dl" parses to foo/1). Two spellings
		// of one key in the same directory would silently collapse into one
		// map entry — one file's statements duplicated, the other's dropped,
		// the worst class of defect here — so the load chokepoint requires
		// every file to carry its key's canonical name, which restores
		// filesystem-name→key injectivity for everything that loads.
		if canonical := key.filename(); canonical != name { // pinned by TestLoadRuleStoreRejectsNonCanonicalFilename
			return nil, fmt.Errorf("rules store: %s: not the canonical name for rule group %s/%d; rename it to %s",
				name, key.Head, key.Arity, canonical)
		}

		full := filepath.Join(dir, name)
		data, err := os.ReadFile(full)
		if err != nil {
			return nil, fmt.Errorf("rules store: reading %s: %w", name, err)
		}

		ruleset, err := parseUserProgram(string(data))
		if err != nil {
			return nil, fmt.Errorf("rules store: %s: %w", name, err)
		}
		groups, order, err := splitRuleset(ruleset)
		if err != nil {
			return nil, fmt.Errorf("rules store: %s: %w", name, err)
		}
		if len(order) == 0 {
			return nil, fmt.Errorf("rules store: %s: file has no rules or facts", name)
		}
		if len(order) > 1 || order[0] != key {
			return nil, fmt.Errorf("rules store: %s: every statement in a group file must have head "+
				"%s/%d (the file's own name); found %v", name, key.Head, key.Arity, headsOf(order))
		}

		g := groups[key]
		rs.Groups[key] = &ruleStoreGroup{
			Key:  key,
			File: name,
			// Text is the RAW on-disk content, not renderGroupText's
			// re-rendered canonical form: "within a group's file the
			// agent's text lands verbatim" (doc/features/workbench-v2.md
			// design decision 4), and the same verbatim posture applies to
			// a vim-authored file — an operator's plain '%' comments,
			// interior blank lines, and formatting must survive into
			// rulesText/export, and a later get_rule_group must return
			// what's actually on disk. Only trailing newlines are trimmed,
			// so export()'s "\n\n" joining stays clean; renderGroupText is
			// Import/split-only, where the source is a parsed monolith and
			// no original per-group file exists to preserve.
			Text:     strings.TrimRight(string(data), "\n"),
			Rules:    g.Rules,
			AggRules: g.AggRules,
		}
		rs.Order = append(rs.Order, key)
	}
	return rs, nil
}

// headsOf renders keys as "pred/arity" pairs for loadRuleStore's
// head-mismatch error.
func headsOf(keys []groupKey) []string {
	out := make([]string, len(keys))
	for i, k := range keys {
		out[i] = fmt.Sprintf("%s/%d", k.Head, k.Arity)
	}
	return out
}

// importRuleset splits a parsed monolithic ruleset into group files and
// writes them under dir, one file per group, per design decision 4's
// Import ("split into group files, with confirm"). Callers (the `datalog
// rules import` CLI) are responsible for the "refuse if dir exists and is
// non-empty" and confirmation-prompt policy; importRuleset itself always
// writes (MkdirAll's usual create-or-reuse semantics) and returns the list
// of files written, in filename order, for the caller to print.
func importRuleset(rs syntax.Ruleset, dir string) ([]string, error) {
	groups, order, err := splitRuleset(rs)
	if err != nil {
		return nil, err
	}
	sort.Slice(order, func(i, j int) bool {
		return order[i].filename() < order[j].filename()
	})

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("rules store: creating %s: %w", dir, err)
	}

	var written []string
	for _, k := range order {
		g := groups[k]
		full := filepath.Join(dir, k.filename())
		// Text has no trailing newline (renderGroupText's doc comment); every
		// other .dl file in this repo (see examples/*.dl) ends in one, so a
		// file this store writes matches that convention rather than lacking
		// a final newline.
		if err := os.WriteFile(full, []byte(g.Text+"\n"), 0o644); err != nil {
			return nil, fmt.Errorf("rules store: writing %s: %w", full, err)
		}
		written = append(written, k.filename())
	}
	return written, nil
}

// dirIsEmpty reports whether dir does not exist or exists but contains no
// entries — importRuleset's caller (runRulesImport) refuses a non-empty
// target outright rather than overwriting existing group files; -y only
// skips the confirmation prompt, it never overrides this check.
func dirIsEmpty(dir string) (bool, error) {
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	return len(entries) == 0, nil
}
