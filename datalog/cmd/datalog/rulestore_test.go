package main

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"swdunlop.dev/pkg/datalog/syntax"
)

// -- rulesSourceConflict ------------------------------------------------------

// TestRulesSourceConflict covers the three states runServe/runMCP's shared
// --rules-vs-positional-files check must distinguish: neither given (fine,
// no rules at all), exactly one given (fine), and both given (error) --
// "specifying both is an error" per the brief.
func TestRulesSourceConflict(t *testing.T) {
	if err := rulesSourceConflict("", nil); err != nil {
		t.Errorf("neither given: got error %v, want nil", err)
	}
	if err := rulesSourceConflict("rules", nil); err != nil {
		t.Errorf("only --rules given: got error %v, want nil", err)
	}
	if err := rulesSourceConflict("", []string{"a.dl"}); err != nil {
		t.Errorf("only positional files given: got error %v, want nil", err)
	}
	if err := rulesSourceConflict("rules", []string{"a.dl"}); err == nil {
		t.Errorf("both given: got nil error, want a conflict error")
	}
}

// -- groupKey filename round-trip -----------------------------------------

// TestGroupKeyFilenameRoundTrip pins the naming scheme's injectivity: the
// canonical example from rulestore.go's doc comment, foo_2/1 vs foo/21,
// must produce two DIFFERENT filenames, and each must parse back to
// exactly its own original key -- proving the scheme survives a predicate
// name that itself contains an underscore immediately followed by digits.
func TestGroupKeyFilenameRoundTrip(t *testing.T) {
	a := groupKey{Head: "foo_2", Arity: 1}
	b := groupKey{Head: "foo", Arity: 21}

	if a.filename() == b.filename() {
		t.Fatalf("collision: %s == %s", a.filename(), b.filename())
	}
	if got := a.filename(); got != "foo_2_1.dl" {
		t.Errorf("a.filename() = %q, want foo_2_1.dl", got)
	}
	if got := b.filename(); got != "foo_21.dl" {
		t.Errorf("b.filename() = %q, want foo_21.dl", got)
	}

	gotA, err := parseGroupFilename(a.filename())
	if err != nil {
		t.Fatalf("parseGroupFilename(%s): %v", a.filename(), err)
	}
	if gotA != a {
		t.Errorf("parseGroupFilename(%s) = %+v, want %+v", a.filename(), gotA, a)
	}

	gotB, err := parseGroupFilename(b.filename())
	if err != nil {
		t.Fatalf("parseGroupFilename(%s): %v", b.filename(), err)
	}
	if gotB != b {
		t.Errorf("parseGroupFilename(%s) = %+v, want %+v", b.filename(), gotB, b)
	}
}

// TestParseGroupFilenameRejectsMalformed covers parseGroupFilename's error
// paths: no ".dl" suffix, no underscore, empty head, empty arity, and a
// non-decimal arity segment.
func TestParseGroupFilenameRejectsMalformed(t *testing.T) {
	cases := []string{
		"foo_1.txt",  // wrong extension
		"foo.dl",     // no underscore at all
		"_1.dl",      // empty head
		"foo_.dl",    // empty arity
		"foo_bar.dl", // arity segment is not decimal
	}
	for _, name := range cases {
		if _, err := parseGroupFilename(name); err == nil {
			t.Errorf("parseGroupFilename(%q): expected error, got nil", name)
		}
	}
}

// -- splitRuleset -----------------------------------------------------------

func mustParseAll(t *testing.T, src string) syntax.Ruleset {
	t.Helper()
	rs, err := syntax.ParseAll(src)
	if err != nil {
		t.Fatalf("ParseAll: %v", err)
	}
	return rs
}

// TestSplitRulesetGroupsByHeadAndPreservesOrder: two predicates interleaved
// in source order must land in two groups, each preserving its own
// statements' relative order.
func TestSplitRulesetGroupsByHeadAndPreservesOrder(t *testing.T) {
	src := `foo(1).
bar(1).
foo(2).
bar(2).
foo(3).
`
	rs := mustParseAll(t, src)
	groups, order, err := splitRuleset(rs)
	if err != nil {
		t.Fatalf("splitRuleset: %v", err)
	}
	if len(order) != 2 {
		t.Fatalf("expected 2 groups, got %d (%v)", len(order), order)
	}

	foo := groups[groupKey{"foo", 1}]
	if foo == nil {
		t.Fatalf("no foo/1 group")
	}
	if len(foo.Rules) != 3 {
		t.Fatalf("foo/1: expected 3 rules, got %d", len(foo.Rules))
	}
	wantFoo := "foo(1).\nfoo(2).\nfoo(3)."
	if foo.Text != wantFoo {
		t.Errorf("foo/1 Text = %q, want %q", foo.Text, wantFoo)
	}

	bar := groups[groupKey{"bar", 1}]
	if bar == nil {
		t.Fatalf("no bar/1 group")
	}
	wantBar := "bar(1).\nbar(2)."
	if bar.Text != wantBar {
		t.Errorf("bar/1 Text = %q, want %q", bar.Text, wantBar)
	}
}

// TestSplitRulesetRejectsEmbeddedQueries: a group file has no place for a
// '?' query (Import is an explicit human action), so splitRuleset must
// error, not warn or silently drop it.
func TestSplitRulesetRejectsEmbeddedQueries(t *testing.T) {
	rs := mustParseAll(t, "foo(1).\nfoo(X)?\n")
	_, _, err := splitRuleset(rs)
	if err == nil {
		t.Fatalf("expected an error for an embedded query, got nil")
	}
	if !strings.Contains(err.Error(), "query") {
		t.Errorf("error does not mention queries: %v", err)
	}
}

// TestSplitRulesetRejectsDetachedDocs: a '%%' block that fails to attach to
// a following statement produces a Ruleset.Warnings entry, which the store
// treats as an error (no free-floating file-level comments allowed).
func TestSplitRulesetRejectsDetachedDocs(t *testing.T) {
	src := "%% a detached doc block\n\nfoo(1).\n"
	rs := mustParseAll(t, src)
	if len(rs.Warnings) == 0 {
		t.Fatalf("test setup: expected ParseAll to report a detached doc warning")
	}
	_, _, err := splitRuleset(rs)
	if err == nil {
		t.Fatalf("expected an error for a detached doc block, got nil")
	}
	if !strings.Contains(err.Error(), "detached") {
		t.Errorf("error does not mention detached docs: %v", err)
	}
}

// TestSplitRulesetRejectsCaseInsensitiveCollision: two distinct predicates
// whose filenames collide under strings.EqualFold must be rejected rather
// than guessed at (macOS/Windows case-insensitive filesystem safety).
func TestSplitRulesetRejectsCaseInsensitiveCollision(t *testing.T) {
	src := "Foo(1).\nfoo(2).\n"
	rs := mustParseAll(t, src)
	_, _, err := splitRuleset(rs)
	if err == nil {
		t.Fatalf("expected a case-collision error, got nil")
	}
	if !strings.Contains(err.Error(), "case-insensitive") {
		t.Errorf("error does not mention case-insensitivity: %v", err)
	}
}

// TestSplitExportRoundTrip: splitting a ruleset and exporting it back must
// reparse to the same statement multiset as the original (order-
// independent per datalog semantics, but every original statement's
// rendered form must survive), and doc comments must be preserved through
// the split.
func TestSplitExportRoundTrip(t *testing.T) {
	src := `%% first rule doc
foo(1).
%% second rule doc
foo(2).
bar(X) :- foo(X).
`
	rs := mustParseAll(t, src)
	groups, order, err := splitRuleset(rs)
	if err != nil {
		t.Fatalf("splitRuleset: %v", err)
	}
	sort.Slice(order, func(i, j int) bool { return order[i].filename() < order[j].filename() })

	store := &ruleStore{Groups: map[groupKey]*ruleStoreGroup{}}
	for _, k := range order {
		g := groups[k]
		store.Groups[k] = &ruleStoreGroup{Key: k, Text: g.Text, Rules: g.Rules, AggRules: g.AggRules}
		store.Order = append(store.Order, k)
	}
	exported := store.export()

	reRs, err := syntax.ParseAll(exported)
	if err != nil {
		t.Fatalf("reparsing exported text: %v\n---\n%s", err, exported)
	}
	if len(reRs.Rules) != len(rs.Rules) {
		t.Fatalf("reparsed rule count = %d, want %d", len(reRs.Rules), len(rs.Rules))
	}

	orig := map[string]bool{}
	for _, r := range rs.Rules {
		orig[r.String()] = true
	}
	for _, r := range reRs.Rules {
		if !orig[r.String()] {
			t.Errorf("reparsed rule not found in original set: %s", r.String())
		}
	}

	if !strings.Contains(exported, "first rule doc") || !strings.Contains(exported, "second rule doc") {
		t.Errorf("doc comments not preserved through split/export: %q", exported)
	}
}

// -- loadRuleStore -----------------------------------------------------------

// TestLoadRuleStoreHeadMismatchNamesFile: a group file whose statements
// don't all share the file's own (predicate, arity) must fail with an
// error naming that file.
func TestLoadRuleStoreHeadMismatchNamesFile(t *testing.T) {
	dir := t.TempDir()
	mustWriteFile(t, filepath.Join(dir, "foo_1.dl"), "foo(1).\nbar(2).\n")

	_, err := loadRuleStore(dir)
	if err == nil {
		t.Fatalf("expected a head-mismatch error, got nil")
	}
	if !strings.Contains(err.Error(), "foo_1.dl") {
		t.Errorf("error does not name the offending file: %v", err)
	}
}

// TestLoadRuleStoreInvalidFilename: a *.dl file whose name doesn't parse as
// a group key fails the whole load, naming that file.
func TestLoadRuleStoreInvalidFilename(t *testing.T) {
	dir := t.TempDir()
	mustWriteFile(t, filepath.Join(dir, "notagroupkey.dl"), "foo(1).\n")

	_, err := loadRuleStore(dir)
	if err == nil {
		t.Fatalf("expected an invalid-filename error, got nil")
	}
	if !strings.Contains(err.Error(), "notagroupkey.dl") {
		t.Errorf("error does not name the offending file: %v", err)
	}
}

// TestLoadRuleStoreRejectsNonCanonicalFilename: parseGroupFilename is lenient
// about spellings filename() never emits (leading-zero arity: "foo_01.dl" →
// foo/1), so two files can parse to the SAME key. Before the canonical-name
// check, "foo_1.dl" + "foo_01.dl" collapsed into one Groups entry: foo(1)
// loaded twice, foo(999) silently dropped — the silent-derivation-loss class.
// The load chokepoint now rejects any file not carrying its key's canonical
// name, restoring filesystem-name→key injectivity.
func TestLoadRuleStoreRejectsNonCanonicalFilename(t *testing.T) {
	dir := t.TempDir()
	mustWriteFile(t, filepath.Join(dir, "foo_1.dl"), "foo(1).\n")
	mustWriteFile(t, filepath.Join(dir, "foo_01.dl"), "foo(999).\n")

	_, err := loadRuleStore(dir)
	if err == nil {
		t.Fatalf("expected a non-canonical-filename error, got nil")
	}
	if !strings.Contains(err.Error(), "foo_01.dl") || !strings.Contains(err.Error(), "foo_1.dl") {
		t.Errorf("error should name the offending file and the canonical name: %v", err)
	}

	// A lone non-canonical file is rejected too — the invariant is the name,
	// not the presence of a conflicting sibling.
	lone := t.TempDir()
	mustWriteFile(t, filepath.Join(lone, "bar_007.dl"), "bar(1, 2, 3, 4, 5, 6, 7).\n")
	if _, err := loadRuleStore(lone); err == nil {
		t.Fatalf("expected a non-canonical-filename error for a lone leading-zero file, got nil")
	}
}

// TestLoadRuleStoreRejectsEmbeddedQueryNamingFile mirrors the head-mismatch
// test but for the embedded-query rejection, confirming the file name is
// still attached to a splitRuleset-level error surfaced through
// loadRuleStore, not just its own checks.
func TestLoadRuleStoreRejectsEmbeddedQueryNamingFile(t *testing.T) {
	dir := t.TempDir()
	mustWriteFile(t, filepath.Join(dir, "foo_1.dl"), "foo(1).\nfoo(X)?\n")

	_, err := loadRuleStore(dir)
	if err == nil {
		t.Fatalf("expected an error, got nil")
	}
	if !strings.Contains(err.Error(), "foo_1.dl") {
		t.Errorf("error does not name the offending file: %v", err)
	}
}

// TestLoadRuleStoreValidDirectory: a well-formed rules/ directory loads
// cleanly, Order reflects sorted filename order, and export() concatenates
// group files in that order separated by a blank line.
func TestLoadRuleStoreValidDirectory(t *testing.T) {
	dir := t.TempDir()
	mustWriteFile(t, filepath.Join(dir, "bar_1.dl"), "bar(1).")
	mustWriteFile(t, filepath.Join(dir, "foo_1.dl"), "foo(1).\nfoo(2).")

	store, err := loadRuleStore(dir)
	if err != nil {
		t.Fatalf("loadRuleStore: %v", err)
	}
	if len(store.Order) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(store.Order))
	}
	if store.Order[0] != (groupKey{"bar", 1}) || store.Order[1] != (groupKey{"foo", 1}) {
		t.Errorf("Order = %v, want [bar/1 foo/1] (sorted filename order)", store.Order)
	}

	want := "bar(1).\n\nfoo(1).\nfoo(2)."
	if got := store.export(); got != want {
		t.Errorf("export() = %q, want %q", got, want)
	}
}

// TestLoadRuleStorePreservesVerbatimText pins design decision 4's "within a
// group's file the text lands verbatim": a vim-authored group file's plain
// '%' comments, interior blank lines, and formatting must survive
// loadRuleStore -> export byte-identically (modulo the trailing-newline
// trim), NOT be replaced by renderGroupText's re-rendered canonical form.
// A later get_rule_group must return what's actually on disk.
func TestLoadRuleStorePreservesVerbatimText(t *testing.T) {
	raw := `% plain operator comment, not a doc block
foo(1).

% another comment after an interior blank line
foo(X) :-
    bar(X).
`
	dir := t.TempDir()
	mustWriteFile(t, filepath.Join(dir, "foo_1.dl"), raw)
	mustWriteFile(t, filepath.Join(dir, "bar_1.dl"), "bar(1).\n")

	store, err := loadRuleStore(dir)
	if err != nil {
		t.Fatalf("loadRuleStore: %v", err)
	}

	want := strings.TrimRight(raw, "\n")
	got := store.Groups[groupKey{"foo", 1}].Text
	if got != want {
		t.Errorf("Text not verbatim:\n got: %q\nwant: %q", got, want)
	}

	exported := store.export()
	if !strings.Contains(exported, want) {
		t.Errorf("export() lost the verbatim group text:\n%q", exported)
	}
	// The exported document must still be parseable (the '%' comments and
	// blank lines are ordinary datalog surface syntax).
	if _, err := syntax.ParseAll(exported); err != nil {
		t.Errorf("exported text no longer parses: %v\n---\n%s", err, exported)
	}
}

// TestRulesDirFactsLoadThroughChokepoint: a facts-only group file loaded
// via --rules must route its ground facts through session.loadProgram's
// fact routing (into sess.facts, folded into the BASE database by buildDB)
// exactly as the positional-monolith path does — never left in sess.rules,
// where they would evaluate as derived rules and flip base/derived
// classification. Compares sess.facts/sess.rules shape and the full
// listPredicates output between the two load paths.
func TestRulesDirFactsLoadThroughChokepoint(t *testing.T) {
	const monolith = `host("a").
host("b").
risky(H) :- host(H).
`
	dir := t.TempDir()
	monolithPath := filepath.Join(dir, "rules.dl")
	mustWriteFile(t, monolithPath, monolith)

	rs := mustParseAll(t, monolith)
	rulesDir := filepath.Join(dir, "rules")
	if _, err := importRuleset(rs, rulesDir); err != nil {
		t.Fatalf("importRuleset: %v", err)
	}

	dataDir := t.TempDir() // empty data root; rules provide all the facts
	hFile, closeFile, err := newMCPHandlers(dataDir, "", []string{monolithPath}, "", 5*time.Second, defaultMaxFacts)
	if err != nil {
		t.Fatalf("newMCPHandlers (positional): %v", err)
	}
	defer closeFile()
	hDir, closeDir, err := newMCPHandlers(dataDir, "", nil, rulesDir, 5*time.Second, defaultMaxFacts)
	if err != nil {
		t.Fatalf("newMCPHandlers (--rules): %v", err)
	}
	defer closeDir()

	// The ground facts must land in sess.facts (base), not sess.rules, on
	// BOTH paths — identical routing through the loadProgram chokepoint.
	if got, want := len(hDir.sess.facts), len(hFile.sess.facts); got != want || got != 2 {
		t.Errorf("sess.facts: --rules path has %d, positional has %d, want 2 in both", got, want)
	}
	if got, want := len(hDir.sess.rules), len(hFile.sess.rules); got != want || got != 1 {
		t.Errorf("sess.rules: --rules path has %d, positional has %d, want 1 in both", got, want)
	}

	// And listPredicates — which reads the base DB via evaluatedDB plus
	// rule heads — must report identical results, including host/1's facts
	// being visible in the (unevaluated) base database.
	lpFile, err := hFile.listPredicates(listPredicatesInput{})
	if err != nil {
		t.Fatalf("listPredicates (positional): %v", err)
	}
	lpDir, err := hDir.listPredicates(listPredicatesInput{})
	if err != nil {
		t.Fatalf("listPredicates (--rules): %v", err)
	}
	if len(lpFile.Predicates) != len(lpDir.Predicates) {
		t.Fatalf("listPredicates length mismatch: positional %v, --rules %v", lpFile.Predicates, lpDir.Predicates)
	}
	for i := range lpFile.Predicates {
		if lpFile.Predicates[i] != lpDir.Predicates[i] {
			t.Errorf("listPredicates[%d]: positional %+v, --rules %+v", i, lpFile.Predicates[i], lpDir.Predicates[i])
		}
	}
	// host/1's facts must appear as BASE facts (2) without any evaluation
	// having run — if the facts had been left in sess.rules, the base DB
	// would report 0 here.
	found := false
	for _, p := range lpDir.Predicates {
		if p.Name == "host" && p.Arity == 1 {
			found = true
			if p.Facts != 2 {
				t.Errorf("host/1 base facts = %d via --rules, want 2", p.Facts)
			}
		}
	}
	if !found {
		t.Errorf("host/1 missing from --rules listPredicates: %v", lpDir.Predicates)
	}
}

// -- importRuleset / dirIsEmpty ----------------------------------------------

// TestImportRulesetWritesGroupFiles: importing a small monolith writes one
// file per group and each file reparses to the same statements.
func TestImportRulesetWritesGroupFiles(t *testing.T) {
	dir := t.TempDir()
	targetDir := filepath.Join(dir, "rules")

	rs := mustParseAll(t, "foo(1).\nfoo(2).\nbar(X) :- foo(X).\n")
	written, err := importRuleset(rs, targetDir)
	if err != nil {
		t.Fatalf("importRuleset: %v", err)
	}
	sort.Strings(written)
	want := []string{"bar_1.dl", "foo_1.dl"}
	if len(written) != len(want) {
		t.Fatalf("written = %v, want %v", written, want)
	}
	for i := range want {
		if written[i] != want[i] {
			t.Errorf("written[%d] = %q, want %q", i, written[i], want[i])
		}
	}

	store, err := loadRuleStore(targetDir)
	if err != nil {
		t.Fatalf("loadRuleStore after import: %v", err)
	}
	if len(store.Order) != 2 {
		t.Fatalf("expected 2 groups after import, got %d", len(store.Order))
	}
}

// TestDirIsEmpty covers the three states dirIsEmpty must distinguish: does
// not exist, exists and empty, exists and non-empty.
func TestDirIsEmpty(t *testing.T) {
	base := t.TempDir()

	missing := filepath.Join(base, "missing")
	if empty, err := dirIsEmpty(missing); err != nil || !empty {
		t.Errorf("dirIsEmpty(missing) = %v, %v; want true, nil", empty, err)
	}

	emptyDir := filepath.Join(base, "empty")
	if err := os.MkdirAll(emptyDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if empty, err := dirIsEmpty(emptyDir); err != nil || !empty {
		t.Errorf("dirIsEmpty(emptyDir) = %v, %v; want true, nil", empty, err)
	}

	nonEmptyDir := filepath.Join(base, "nonempty")
	if err := os.MkdirAll(nonEmptyDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	mustWriteFile(t, filepath.Join(nonEmptyDir, "foo_1.dl"), "foo(1).")
	if empty, err := dirIsEmpty(nonEmptyDir); err != nil || empty {
		t.Errorf("dirIsEmpty(nonEmptyDir) = %v, %v; want false, nil", empty, err)
	}
}

// TestConfirm covers the y/N contract: an explicit y/yes/Y/YES answer is
// the only way to get true; a blank line, "n", garbage, or EOF (a closed
// pipe with nothing written) must all read as "no" -- an unattended
// pipeline with no -y must refuse rather than hang or default to yes.
func TestConfirm(t *testing.T) {
	cases := []struct {
		input string
		want  bool
	}{
		{"y\n", true},
		{"yes\n", true},
		{"Y\n", true},
		{"YES\n", true},
		{"n\n", false},
		{"\n", false},
		{"garbage\n", false},
		{"", false}, // EOF, no input at all
	}
	for _, c := range cases {
		r, w, err := os.Pipe()
		if err != nil {
			t.Fatalf("os.Pipe: %v", err)
		}
		if _, err := w.WriteString(c.input); err != nil {
			t.Fatalf("writing pipe input: %v", err)
		}
		w.Close()

		devnull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
		if err != nil {
			t.Fatalf("opening devnull: %v", err)
		}
		got := confirm(r, devnull, "proceed?")
		devnull.Close()
		r.Close()

		if got != c.want {
			t.Errorf("confirm(%q) = %v, want %v", c.input, got, c.want)
		}
	}
}

// -- end-to-end: mordor example import + --rules session --------------------

// TestImportMordorExampleAndLoadViaRulesDir imports the real
// examples/mordor/rules.dl monolith into a fresh rules/ directory, then
// builds a session two ways -- via the legacy positional rule file, and via
// --rules pointed at the imported directory -- and confirms both evaluate
// the same derived predicates over the same data. This is the end-to-end
// case the brief calls for: "verify the same predicates evaluate as
// loading the monolith."
func TestImportMordorExampleAndLoadViaRulesDir(t *testing.T) {
	monolith := filepath.Join("..", "..", "examples", "mordor", "rules.dl")
	if _, err := os.Stat(monolith); err != nil {
		t.Fatalf("mordor rules.dl not found: %v", err)
	}
	zipPath := filepath.Join("..", "..", "examples", "mordor", "covenant_copy_smb.zip")
	if _, err := os.Stat(zipPath); err != nil {
		t.Fatalf("mordor zip not found: %v", err)
	}
	schemaPath := filepath.Join("..", "..", "examples", "mordor", "mordor.yaml")

	data, err := os.ReadFile(monolith)
	if err != nil {
		t.Fatalf("reading monolith: %v", err)
	}
	rs, err := syntax.ParseAll(string(data))
	if err != nil {
		t.Fatalf("parsing monolith: %v", err)
	}

	rulesDir := filepath.Join(t.TempDir(), "rules")
	if _, err := importRuleset(rs, rulesDir); err != nil {
		t.Fatalf("importRuleset: %v", err)
	}

	// Path 1: legacy positional rule file.
	hFile, closeFile, err := newMCPHandlers(zipPath, schemaPath, []string{monolith}, "", 5*time.Second, defaultMaxFacts)
	if err != nil {
		t.Fatalf("newMCPHandlers (positional file): %v", err)
	}
	defer closeFile()

	// Path 2: --rules directory.
	hDir, closeDir, err := newMCPHandlers(zipPath, schemaPath, nil, rulesDir, 5*time.Second, defaultMaxFacts)
	if err != nil {
		t.Fatalf("newMCPHandlers (--rules dir): %v", err)
	}
	defer closeDir()

	if hDir.rules == nil {
		t.Fatalf("expected mcpHandlers.rules to be populated when --rules is used")
	}

	// evaluatedDB falls back to the EDB-only buildDB snapshot until
	// derivedDB is populated by an actual evaluate() run (newWorkbench's
	// startup eval does this for `datalog serve`; a bare newMCPHandlers
	// caller, like this test, must do it itself to see derived predicates
	// at all).
	ctx := context.Background()
	dbFile, _, err := hFile.sess.evaluate(ctx)
	if err != nil {
		t.Fatalf("evaluate (positional file): %v", err)
	}
	dbDir, _, err := hDir.sess.evaluate(ctx)
	if err != nil {
		t.Fatalf("evaluate (--rules dir): %v", err)
	}

	for _, pa := range []struct {
		name  string
		arity int
	}{
		{"smb_conn", 3},
		{"remote_logon", 4},
		{"admin_share", 4},
		{"lateral_movement", 4},
	} {
		nFile := 0
		for range dbFile.Facts(pa.name, pa.arity) {
			nFile++
		}
		nDir := 0
		for range dbDir.Facts(pa.name, pa.arity) {
			nDir++
		}
		if nFile == 0 {
			t.Fatalf("%s/%d: positional-file path produced 0 facts (test is meaningless)", pa.name, pa.arity)
		}
		if nFile != nDir {
			t.Errorf("%s/%d: positional-file path = %d facts, --rules dir path = %d facts", pa.name, pa.arity, nFile, nDir)
		}
	}

	// The full listPredicates output must also agree between the two load
	// paths — same predicates, same arities, same counts, same Use docs —
	// covering classification for every predicate, not just the derived
	// samples above. evaluate() alone does not cache into derivedDB, so
	// listPredicates here reads the EDB-only buildDB fallback on both
	// sides: exactly the comparison that would catch ground facts
	// mis-routed into sess.rules instead of sess.facts.
	lpFile, err := hFile.listPredicates(listPredicatesInput{})
	if err != nil {
		t.Fatalf("listPredicates (positional): %v", err)
	}
	lpDir, err := hDir.listPredicates(listPredicatesInput{})
	if err != nil {
		t.Fatalf("listPredicates (--rules): %v", err)
	}
	if len(lpFile.Predicates) != len(lpDir.Predicates) {
		t.Fatalf("listPredicates length mismatch: positional %v, --rules %v", lpFile.Predicates, lpDir.Predicates)
	}
	for i := range lpFile.Predicates {
		if lpFile.Predicates[i] != lpDir.Predicates[i] {
			t.Errorf("listPredicates[%d]: positional %+v, --rules %+v", i, lpFile.Predicates[i], lpDir.Predicates[i])
		}
	}
}
