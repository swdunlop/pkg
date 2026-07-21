package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// This file covers the rule-group CRUD tools (phase 1b,
// doc/features/workbench-v2.md design decision 4): put_rule_group,
// delete_rule_group, list_rule_groups, get_rule_group. It supersedes the
// coverage the removed whole-document set_rules MCP tool used to have in
// mcp_test.go (embedded-query rejection, trial-compile error, detached-doc
// warnings, whole-document replacement) — see that file's "set_rules
// (removed as an MCP tool)" section header.

// newCrudTestHandlers builds mcpHandlers rooted at a rules/ directory store
// under dataDir/"rules" (created empty unless seed is non-empty, in which
// case seed maps group filenames to their .dl content before the store
// loads) — the fixture every put/delete/list/get test in this file starts
// from. dataDir has no jsonfacts schema; these tests only exercise the
// rule-group surface, not fact loading.
func newCrudTestHandlers(t *testing.T, seed map[string]string) (*mcpHandlers, string) {
	t.Helper()
	dataDir := t.TempDir()
	rulesDir := filepath.Join(dataDir, "rules")
	if err := os.MkdirAll(rulesDir, 0o755); err != nil {
		t.Fatalf("mkdir rules dir: %v", err)
	}
	for name, text := range seed {
		mustWriteFile(t, filepath.Join(rulesDir, name), text)
	}
	h, closeFn, err := newMCPHandlers(dataDir, "", nil, rulesDir, 5*time.Second, defaultMaxFacts, false)
	if err != nil {
		t.Fatalf("newMCPHandlers: %v", err)
	}
	t.Cleanup(func() { closeFn() })
	return h, rulesDir
}

// dirEntryNames lists the base names of every entry directly inside dir,
// for asserting "no .tmp droppings survive a rejected write."
func dirEntryNames(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("reading %s: %v", dir, err)
	}
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name()
	}
	return names
}

// -- create ----------------------------------------------------------------

func TestPutRuleGroup_CreateWithZeroRevision(t *testing.T) {
	h, rulesDir := newCrudTestHandlers(t, nil)

	out, err := h.putRuleGroup(putRuleGroupInput{
		Head: "foo", Arity: 1, Text: `foo("a").`, Revision: 0,
	})
	if err != nil {
		t.Fatalf("put_rule_group: %v", err)
	}
	if out.IsStale {
		t.Fatalf("put_rule_group: unexpected stale rejection: %+v", out)
	}
	if out.Revision != 1 {
		t.Errorf("put_rule_group: revision = %d, want 1 (first-ever revision issued by this store)", out.Revision)
	}
	if out.File != "foo_1.dl" {
		t.Errorf("put_rule_group: file = %q, want foo_1.dl", out.File)
	}

	data, err := os.ReadFile(filepath.Join(rulesDir, "foo_1.dl"))
	if err != nil {
		t.Fatalf("reading foo_1.dl: %v", err)
	}
	if string(data) != "foo(\"a\").\n" {
		t.Errorf("foo_1.dl content = %q, want %q (verbatim plus trailing newline)", string(data), "foo(\"a\").\n")
	}

	// Fact-count feedback: foo/1 has one fact.
	found := false
	for _, p := range out.Predicates {
		if p.Name == "foo" && p.Arity == 1 {
			found = true
			if p.Facts != 1 {
				t.Errorf("put_rule_group: foo/1 facts = %d, want 1", p.Facts)
			}
		}
	}
	if !found {
		t.Errorf("put_rule_group: foo/1 missing from Predicates: %+v", out.Predicates)
	}
}

func TestPutRuleGroup_CreateRejectsNonZeroRevision(t *testing.T) {
	h, _ := newCrudTestHandlers(t, nil)

	out, err := h.putRuleGroup(putRuleGroupInput{Head: "foo", Arity: 1, Text: `foo("a").`, Revision: 5})
	if err != nil {
		t.Fatalf("put_rule_group: unexpected Go error (staleness is not an error): %v", err)
	}
	if !out.IsStale {
		t.Fatal("put_rule_group: expected a stale rejection for a create attempt with revision != 0")
	}
	if out.CurrentText != "" || out.CurrentRevision != 0 {
		t.Errorf("put_rule_group: a create-vs-nonexistent stale rejection must carry no current content, got %+v", out)
	}
}

// -- edit --------------------------------------------------------------------

func TestPutRuleGroup_EditWithCorrectRevision(t *testing.T) {
	h, rulesDir := newCrudTestHandlers(t, map[string]string{
		"foo_1.dl": "foo(\"a\").\n",
	})

	out, err := h.putRuleGroup(putRuleGroupInput{Head: "foo", Arity: 1, Text: `foo("b").`, Revision: 1})
	if err != nil {
		t.Fatalf("put_rule_group: %v", err)
	}
	if out.IsStale {
		t.Fatalf("put_rule_group: unexpected stale rejection: %+v", out)
	}
	if out.Revision != 2 {
		t.Errorf("put_rule_group: revision = %d, want 2 (bumped from 1)", out.Revision)
	}

	data, err := os.ReadFile(filepath.Join(rulesDir, "foo_1.dl"))
	if err != nil {
		t.Fatalf("reading foo_1.dl: %v", err)
	}
	if string(data) != "foo(\"b\").\n" {
		t.Errorf("foo_1.dl content = %q, want %q", string(data), "foo(\"b\").\n")
	}

	// The old fact must be gone (whole-group replacement, not append).
	sample, err := h.sampleFacts(sampleFactsInput{Predicate: "foo", Arity: 1})
	if err != nil {
		t.Fatalf("sample_facts: %v", err)
	}
	if sample.Total != 1 {
		t.Fatalf("sample_facts: total = %d, want 1 (edit replaces the group, not appends)", sample.Total)
	}
}

func TestPutRuleGroup_EditReplacesGroupContent(t *testing.T) {
	// Whole-document-replacement coverage, migrated from the removed
	// TestSetRules_WholeDocumentReplacement (mcp_test.go): editing a group
	// twice must leave only the SECOND edit's content, never a union of both.
	h, _ := newCrudTestHandlers(t, nil)

	out1, err := h.putRuleGroup(putRuleGroupInput{Head: "foo", Arity: 1, Text: `foo("a"). foo("b").`, Revision: 0})
	if err != nil {
		t.Fatalf("put_rule_group (create): %v", err)
	}
	out2, err := h.putRuleGroup(putRuleGroupInput{Head: "foo", Arity: 1, Text: `foo("c").`, Revision: out1.Revision})
	if err != nil {
		t.Fatalf("put_rule_group (edit): %v", err)
	}
	if out2.IsStale {
		t.Fatalf("put_rule_group (edit): unexpected stale rejection: %+v", out2)
	}

	sample, err := h.sampleFacts(sampleFactsInput{Predicate: "foo", Arity: 1, Limit: 10})
	if err != nil {
		t.Fatalf("sample_facts: %v", err)
	}
	if sample.Total != 1 {
		t.Fatalf("sample_facts: total = %d, want 1 (edit replaced, not appended)", sample.Total)
	}
}

func TestPutRuleGroup_EditRejectsStaleRevision(t *testing.T) {
	h, _ := newCrudTestHandlers(t, map[string]string{
		"foo_1.dl": "foo(\"a\").\n",
	})

	out, err := h.putRuleGroup(putRuleGroupInput{Head: "foo", Arity: 1, Text: `foo("b").`, Revision: 99})
	if err != nil {
		t.Fatalf("put_rule_group: unexpected Go error (staleness is not an error): %v", err)
	}
	if !out.IsStale {
		t.Fatal("put_rule_group: expected a stale rejection for a wrong revision")
	}
	if out.CurrentText != `foo("a").` {
		t.Errorf("put_rule_group: CurrentText = %q, want %q", out.CurrentText, `foo("a").`)
	}
	if out.CurrentRevision != 1 {
		t.Errorf("put_rule_group: CurrentRevision = %d, want 1", out.CurrentRevision)
	}

	// The rejected edit must not have touched the on-disk file or the fact.
	sample, err := h.sampleFacts(sampleFactsInput{Predicate: "foo", Arity: 1})
	if err != nil {
		t.Fatalf("sample_facts: %v", err)
	}
	if sample.Total != 1 {
		t.Fatalf("sample_facts: total = %d, want 1 (stale edit must not apply)", sample.Total)
	}
}

func TestPutRuleGroup_EditRejectsAbsentGroup(t *testing.T) {
	h, _ := newCrudTestHandlers(t, nil)

	out, err := h.putRuleGroup(putRuleGroupInput{Head: "nope", Arity: 1, Text: `nope("a").`, Revision: 3})
	if err != nil {
		t.Fatalf("put_rule_group: unexpected Go error: %v", err)
	}
	if !out.IsStale {
		t.Fatal("put_rule_group: expected a stale rejection for a non-zero revision against an absent group")
	}
	if out.CurrentText != "" || out.CurrentRevision != 0 {
		t.Errorf("put_rule_group: absent-group rejection must carry no current content, got %+v", out)
	}
}

// -- validation: wrong head / two heads / embedded query / _q_ / detached doc

func TestPutRuleGroup_WrongHeadRejected(t *testing.T) {
	h, _ := newCrudTestHandlers(t, nil)

	_, err := h.putRuleGroup(putRuleGroupInput{Head: "foo", Arity: 1, Text: `bar("a").`, Revision: 0})
	if err == nil {
		t.Fatal("put_rule_group: expected an error for text whose head does not match head/arity")
	}
}

func TestPutRuleGroup_TwoHeadsRejected(t *testing.T) {
	h, _ := newCrudTestHandlers(t, nil)

	_, err := h.putRuleGroup(putRuleGroupInput{Head: "foo", Arity: 1, Text: "foo(\"a\").\nbar(\"b\").", Revision: 0})
	if err == nil {
		t.Fatal("put_rule_group: expected an error for text defining two different heads")
	}
}

func TestPutRuleGroup_RejectsEmbeddedQuery(t *testing.T) {
	h, _ := newCrudTestHandlers(t, nil)

	_, err := h.putRuleGroup(putRuleGroupInput{Head: "foo", Arity: 1, Text: "foo(\"a\").\nfoo(X)?", Revision: 0})
	if err == nil {
		t.Fatal("put_rule_group: expected an error for an embedded query")
	}
}

func TestPutRuleGroup_RejectsReservedQueryPred(t *testing.T) {
	h, _ := newCrudTestHandlers(t, nil)

	_, err := h.putRuleGroup(putRuleGroupInput{Head: "_q_", Arity: 1, Text: `_q_("boom").`, Revision: 0})
	if err == nil {
		t.Fatal("put_rule_group: expected an error for the reserved _q_ predicate")
	}
	if !strings.Contains(err.Error(), "_q_") {
		t.Errorf("put_rule_group: error %q does not name the reserved predicate", err.Error())
	}
}

// TestPutRuleGroup_RejectsDetachedDoc pins a design difference from the
// removed whole-document set_rules tool: that tool treated a detached '%%'
// doc block as a soft warning (ruleset.Warnings) and still applied the
// document. The rule-group STORE (rulestore.go's splitRuleset, phase 1a)
// forbids free-floating file-level comments outright — "no free-floating
// file-level comments can exist" (doc/features/workbench-v2.md design
// decision 4) — so put_rule_group, which routes through splitRuleset, treats
// a detached doc as a hard error, not a warning: nothing is written, and the
// caller must attach the doc (or downgrade it to a plain '%' comment)
// before resubmitting.
func TestPutRuleGroup_RejectsDetachedDoc(t *testing.T) {
	h, _ := newCrudTestHandlers(t, nil)

	src := "%% this doc is detached\n\nfoo(\"a\").\n"
	_, err := h.putRuleGroup(putRuleGroupInput{Head: "foo", Arity: 1, Text: src, Revision: 0})
	if err == nil {
		t.Fatal("put_rule_group: expected an error for a detached '%%' doc block")
	}
	if !strings.Contains(err.Error(), "detached") {
		t.Errorf("put_rule_group: error %q does not mention the detached doc", err.Error())
	}
}

// TestPutRuleGroup_SurfacesAttachedDocWarnings_None confirms a clean
// program with properly attached docs produces no Warnings at all, the
// complement of TestPutRuleGroup_RejectsDetachedDoc.
func TestPutRuleGroup_CleanAttachedDocHasNoWarnings(t *testing.T) {
	h, _ := newCrudTestHandlers(t, nil)

	out, err := h.putRuleGroup(putRuleGroupInput{
		Head: "foo", Arity: 1, Text: "%% attached doc\nfoo(\"a\").\n", Revision: 0,
	})
	if err != nil {
		t.Fatalf("put_rule_group: %v", err)
	}
	if len(out.Warnings) != 0 {
		t.Fatalf("put_rule_group: unexpected warnings for a clean attached doc: %v", out.Warnings)
	}
}

func TestPutRuleGroup_BreaksFullRulesetRejected(t *testing.T) {
	// Migrated from the removed TestSetRules_TrialCompileError (mcp_test.go),
	// but exercising the "breaks the OTHER group" case specifically: a/1
	// compiles alone (base facts, trivially stratifiable), but introduces an
	// unstratifiable negation CYCLE with an EXISTING group b/1 once both are
	// considered together: b depends negatively on a, and a (as written
	// here) depends on b, so neither can be stratified before the other.
	h, rulesDir := newCrudTestHandlers(t, map[string]string{
		"b_1.dl": "b(X) :- c(X), not a(X).\n",
		"c_1.dl": "c(\"x\").\n",
	})

	before := dirEntryNames(t, rulesDir)

	out, err := h.putRuleGroup(putRuleGroupInput{Head: "a", Arity: 1, Text: `a(X) :- b(X).`, Revision: 0})
	if err == nil {
		t.Fatalf("put_rule_group: expected an error from the full-ruleset trial compile, got %+v", out)
	}
	if !strings.Contains(err.Error(), "unstratifiable") && !strings.Contains(err.Error(), "stratif") {
		t.Errorf("put_rule_group: error %q does not mention stratification", err.Error())
	}

	// Nothing should have been written: no new a_1.dl, no .tmp droppings.
	after := dirEntryNames(t, rulesDir)
	if len(after) != len(before) {
		t.Errorf("put_rule_group: directory listing changed after a rejected write: before=%v after=%v", before, after)
	}
	for _, name := range after {
		if strings.Contains(name, ".tmp") {
			t.Errorf("put_rule_group: stray temp file left behind: %s", name)
		}
	}
}

// -- disk write discipline ---------------------------------------------------

func TestPutRuleGroup_NoTempDroppingsOnRejectedWrite(t *testing.T) {
	h, rulesDir := newCrudTestHandlers(t, map[string]string{
		"foo_1.dl": "foo(\"a\").\n",
	})
	before := dirEntryNames(t, rulesDir)

	// A stale-revision edit never reaches writeGroupFile at all, but a
	// parse-error edit is the more interesting case: it must fail before any
	// temp file is created.
	_, err := h.putRuleGroup(putRuleGroupInput{Head: "foo", Arity: 1, Text: `this is not valid datalog !!!`, Revision: 1})
	if err == nil {
		t.Fatal("put_rule_group: expected a parse error for invalid text")
	}

	after := dirEntryNames(t, rulesDir)
	if len(after) != len(before) {
		t.Errorf("put_rule_group: directory listing changed after a rejected write: before=%v after=%v", before, after)
	}
	for _, name := range after {
		if strings.Contains(name, ".tmp") {
			t.Errorf("put_rule_group: stray temp file left behind: %s", name)
		}
	}
}

// TestPutRuleGroup_RejectsCaseInsensitiveFilenameCollision pins the create
// path's reuse of checkFilenameCollisions (rulestore.go): a NEW key whose
// filename folds onto an existing group's under case-insensitivity ({Foo,1}
// beside {foo,1}) must be refused BEFORE any disk write — on a
// case-insensitive filesystem (macOS/Windows) writeGroupFile's rename would
// otherwise silently clobber the other group's file while both stayed live
// in memory. Import/load already reject this (splitRuleset/loadRuleStore);
// this test keeps the CRUD surface from being the one path that forgets.
func TestPutRuleGroup_RejectsCaseInsensitiveFilenameCollision(t *testing.T) {
	h, rulesDir := newCrudTestHandlers(t, map[string]string{
		"foo_1.dl": "foo(\"a\").\n",
	})
	before := dirEntryNames(t, rulesDir)

	_, err := h.putRuleGroup(putRuleGroupInput{Head: "Foo", Arity: 1, Text: `Foo("b").`, Revision: 0})
	if err == nil {
		t.Fatal("put_rule_group: expected a case-insensitive filename-collision error creating Foo/1 beside foo/1")
	}
	if !strings.Contains(err.Error(), "case-insensitive") {
		t.Errorf("put_rule_group: error %q does not mention the case-insensitive collision", err.Error())
	}

	// Nothing on disk changed — foo_1.dl survives with its original content.
	after := dirEntryNames(t, rulesDir)
	if len(after) != len(before) {
		t.Errorf("put_rule_group: directory listing changed after a rejected collision: before=%v after=%v", before, after)
	}
	data, err := os.ReadFile(filepath.Join(rulesDir, "foo_1.dl"))
	if err != nil {
		t.Fatalf("reading foo_1.dl: %v", err)
	}
	if string(data) != "foo(\"a\").\n" {
		t.Errorf("foo_1.dl content = %q, want original %q", string(data), "foo(\"a\").\n")
	}

	// An EDIT of the existing key is naturally exempt (equal keys are not a
	// collision) — the guard must not lock the group against its own edits.
	out, err := h.putRuleGroup(putRuleGroupInput{Head: "foo", Arity: 1, Text: `foo("b").`, Revision: 1})
	if err != nil {
		t.Fatalf("put_rule_group (edit of existing key): %v", err)
	}
	if out.IsStale || out.Revision != 2 {
		t.Errorf("put_rule_group (edit): got %+v, want revision 2, not stale", out)
	}
}

// -- delete ------------------------------------------------------------------

func TestDeleteRuleGroup_CorrectRevision(t *testing.T) {
	h, rulesDir := newCrudTestHandlers(t, map[string]string{
		"foo_1.dl": "foo(\"a\").\n",
	})

	out, err := h.deleteRuleGroup(deleteRuleGroupInput{Head: "foo", Arity: 1, Revision: 1})
	if err != nil {
		t.Fatalf("delete_rule_group: %v", err)
	}
	if out.IsStale {
		t.Fatalf("delete_rule_group: unexpected stale rejection: %+v", out)
	}
	if out.File != "foo_1.dl" {
		t.Errorf("delete_rule_group: file = %q, want foo_1.dl", out.File)
	}

	if _, err := os.Stat(filepath.Join(rulesDir, "foo_1.dl")); !os.IsNotExist(err) {
		t.Errorf("delete_rule_group: foo_1.dl still exists on disk (err=%v)", err)
	}

	lg, err := h.listRuleGroups(listRuleGroupsInput{})
	if err != nil {
		t.Fatalf("list_rule_groups: %v", err)
	}
	if len(lg.Groups) != 0 {
		t.Errorf("list_rule_groups: expected no groups after delete, got %+v", lg.Groups)
	}
}

func TestDeleteRuleGroup_StaleRevision(t *testing.T) {
	h, rulesDir := newCrudTestHandlers(t, map[string]string{
		"foo_1.dl": "foo(\"a\").\n",
	})

	out, err := h.deleteRuleGroup(deleteRuleGroupInput{Head: "foo", Arity: 1, Revision: 42})
	if err != nil {
		t.Fatalf("delete_rule_group: unexpected Go error: %v", err)
	}
	if !out.IsStale {
		t.Fatal("delete_rule_group: expected a stale rejection for a wrong revision")
	}
	if out.CurrentText != `foo("a").` || out.CurrentRevision != 1 {
		t.Errorf("delete_rule_group: current content = %q rev %d, want %q rev 1", out.CurrentText, out.CurrentRevision, `foo("a").`)
	}

	// Must not have deleted the file.
	if _, err := os.Stat(filepath.Join(rulesDir, "foo_1.dl")); err != nil {
		t.Errorf("delete_rule_group: foo_1.dl was removed despite a stale rejection: %v", err)
	}
}

func TestDeleteRuleGroup_AbsentGroup(t *testing.T) {
	h, _ := newCrudTestHandlers(t, nil)

	out, err := h.deleteRuleGroup(deleteRuleGroupInput{Head: "nope", Arity: 1, Revision: 1})
	if err != nil {
		t.Fatalf("delete_rule_group: unexpected Go error: %v", err)
	}
	if !out.IsStale {
		t.Fatal("delete_rule_group: expected a stale rejection for a non-existent group")
	}
}

// -- legacy session (no --rules) ----------------------------------------------

func TestRuleGroupTools_ErrorWithoutRulesStore(t *testing.T) {
	// A session started without --rules (h.rules == nil) must reject all
	// four CRUD tools -- reads and writes alike -- with the SAME message
	// (design decision 6: "one consistent message").
	h, done := newTestHandlers(t, t.TempDir())
	defer done()

	_, err := h.listRuleGroups(listRuleGroupsInput{})
	if err == nil {
		t.Fatal("list_rule_groups: expected an error on a legacy (no --rules) session")
	}
	listErr := err.Error()

	_, err = h.getRuleGroup(getRuleGroupInput{Head: "foo", Arity: 1})
	if err == nil || err.Error() != listErr {
		t.Errorf("get_rule_group: error = %v, want the same message as list_rule_groups (%q)", err, listErr)
	}

	_, err = h.putRuleGroup(putRuleGroupInput{Head: "foo", Arity: 1, Text: `foo("a").`})
	if err == nil || err.Error() != listErr {
		t.Errorf("put_rule_group: error = %v, want the same message as list_rule_groups (%q)", err, listErr)
	}

	_, err = h.deleteRuleGroup(deleteRuleGroupInput{Head: "foo", Arity: 1, Revision: 1})
	if err == nil || err.Error() != listErr {
		t.Errorf("delete_rule_group: error = %v, want the same message as list_rule_groups (%q)", err, listErr)
	}
}

// -- list / get: order, revisions ---------------------------------------------

func TestListRuleGroups_FilenameOrderAndRevisions(t *testing.T) {
	h, _ := newCrudTestHandlers(t, map[string]string{
		"bbb_1.dl": "bbb(\"a\").\n",
		"aaa_1.dl": "aaa(\"a\").\n",
	})

	out, err := h.listRuleGroups(listRuleGroupsInput{})
	if err != nil {
		t.Fatalf("list_rule_groups: %v", err)
	}
	if len(out.Groups) != 2 {
		t.Fatalf("list_rule_groups: got %d groups, want 2", len(out.Groups))
	}
	// Filename order: "aaa_1.dl" sorts before "bbb_1.dl".
	if out.Groups[0].Head != "aaa" || out.Groups[1].Head != "bbb" {
		t.Errorf("list_rule_groups: order = [%s, %s], want [aaa, bbb] (filename order)",
			out.Groups[0].Head, out.Groups[1].Head)
	}
	for _, g := range out.Groups {
		if g.Revision != 1 {
			t.Errorf("list_rule_groups: %s/%d revision = %d, want 1 (fresh load)", g.Head, g.Arity, g.Revision)
		}
		if g.Statements != 1 {
			t.Errorf("list_rule_groups: %s/%d statements = %d, want 1", g.Head, g.Arity, g.Statements)
		}
	}

	// After an edit, list_rule_groups must reflect the bumped revision.
	if _, err := h.putRuleGroup(putRuleGroupInput{Head: "aaa", Arity: 1, Text: `aaa("b").`, Revision: 1}); err != nil {
		t.Fatalf("put_rule_group: %v", err)
	}
	out2, err := h.listRuleGroups(listRuleGroupsInput{})
	if err != nil {
		t.Fatalf("list_rule_groups (after edit): %v", err)
	}
	for _, g := range out2.Groups {
		if g.Head == "aaa" && g.Revision != 2 {
			t.Errorf("list_rule_groups: aaa/1 revision = %d after edit, want 2", g.Revision)
		}
	}
}

func TestGetRuleGroup_ReturnsVerbatimTextAndRevision(t *testing.T) {
	h, _ := newCrudTestHandlers(t, map[string]string{
		"foo_1.dl": "%% a doc comment\nfoo(\"a\").\n",
	})

	out, err := h.getRuleGroup(getRuleGroupInput{Head: "foo", Arity: 1})
	if err != nil {
		t.Fatalf("get_rule_group: %v", err)
	}
	if out.Revision != 1 {
		t.Errorf("get_rule_group: revision = %d, want 1", out.Revision)
	}
	// Text is the verbatim on-disk content with trailing newlines trimmed
	// (loadRuleStore's convention).
	if !strings.Contains(out.Text, "a doc comment") {
		t.Errorf("get_rule_group: text %q missing the doc comment", out.Text)
	}
	if out.File != "foo_1.dl" {
		t.Errorf("get_rule_group: file = %q, want foo_1.dl", out.File)
	}
}

func TestGetRuleGroup_UnknownGroupErrors(t *testing.T) {
	h, _ := newCrudTestHandlers(t, nil)

	_, err := h.getRuleGroup(getRuleGroupInput{Head: "nope", Arity: 1})
	if err == nil {
		t.Fatal("get_rule_group: expected an error for an unknown group")
	}
}

// -- end-to-end: put then reload from disk -----------------------------------

func TestPutRuleGroup_SurvivesReloadFromDisk(t *testing.T) {
	h, rulesDir := newCrudTestHandlers(t, nil)

	out, err := h.putRuleGroup(putRuleGroupInput{Head: "foo", Arity: 1, Text: `foo("a"). foo("b").`, Revision: 0})
	if err != nil {
		t.Fatalf("put_rule_group: %v", err)
	}
	if out.Revision != 1 {
		t.Fatalf("put_rule_group: revision = %d, want 1", out.Revision)
	}

	// Simulate a process restart: reload the store fresh from the same
	// directory (loadRuleStore), independent of h.
	fresh, err := loadRuleStore(rulesDir)
	if err != nil {
		t.Fatalf("loadRuleStore (fresh): %v", err)
	}
	g, ok := fresh.Groups[groupKey{Head: "foo", Arity: 1}]
	if !ok {
		t.Fatal("loadRuleStore (fresh): foo/1 group missing after reload")
	}
	if g.Revision != 1 {
		t.Errorf("loadRuleStore (fresh): revision = %d, want 1 (revisions reset on reload)", g.Revision)
	}
	if g.Text != `foo("a"). foo("b").` {
		t.Errorf("loadRuleStore (fresh): text = %q, want %q (content survives)", g.Text, `foo("a"). foo("b").`)
	}
}

func TestPutRuleGroup_RecreateAfterDeleteResumesPastHighWater(t *testing.T) {
	// ruleStore.deletedHighWater's doc comment: a re-created key must not
	// reset to revision 1, since a caller could still be holding a stale
	// "current_revision: 1" rejection for the OLD generation of this key.
	h, _ := newCrudTestHandlers(t, map[string]string{
		"foo_1.dl": "foo(\"a\").\n",
	})

	// Bump foo/1 to revision 3 via two edits.
	out, err := h.putRuleGroup(putRuleGroupInput{Head: "foo", Arity: 1, Text: `foo("b").`, Revision: 1})
	if err != nil {
		t.Fatalf("put_rule_group (edit 1): %v", err)
	}
	out, err = h.putRuleGroup(putRuleGroupInput{Head: "foo", Arity: 1, Text: `foo("c").`, Revision: out.Revision})
	if err != nil {
		t.Fatalf("put_rule_group (edit 2): %v", err)
	}
	if out.Revision != 3 {
		t.Fatalf("put_rule_group: revision = %d, want 3 before delete", out.Revision)
	}

	if _, err := h.deleteRuleGroup(deleteRuleGroupInput{Head: "foo", Arity: 1, Revision: 3}); err != nil {
		t.Fatalf("delete_rule_group: %v", err)
	}

	// Re-create: must resume from 4, not reset to 1.
	recreated, err := h.putRuleGroup(putRuleGroupInput{Head: "foo", Arity: 1, Text: `foo("d").`, Revision: 0})
	if err != nil {
		t.Fatalf("put_rule_group (re-create): %v", err)
	}
	if recreated.IsStale {
		t.Fatalf("put_rule_group (re-create): unexpected stale rejection: %+v", recreated)
	}
	if recreated.Revision != 4 {
		t.Errorf("put_rule_group (re-create): revision = %d, want 4 (resumed past the deleted key's high water mark)", recreated.Revision)
	}
}

// -- fact-count feedback matches a post-write query --------------------------

func TestPutRuleGroup_FactCountFeedbackMatchesQuery(t *testing.T) {
	h, _ := newCrudTestHandlers(t, nil)

	out, err := h.putRuleGroup(putRuleGroupInput{
		Head: "foo", Arity: 1, Text: "foo(\"a\").\nfoo(\"b\").\nfoo(\"c\").",
	})
	if err != nil {
		t.Fatalf("put_rule_group: %v", err)
	}
	var feedbackCount int
	for _, p := range out.Predicates {
		if p.Name == "foo" && p.Arity == 1 {
			feedbackCount = p.Facts
		}
	}

	sample, err := h.sampleFacts(sampleFactsInput{Predicate: "foo", Arity: 1, Limit: 10})
	if err != nil {
		t.Fatalf("sample_facts: %v", err)
	}
	if feedbackCount != sample.Total {
		t.Errorf("put_rule_group feedback count = %d, sample_facts total = %d, want equal", feedbackCount, sample.Total)
	}
	if sample.Total != 3 {
		t.Errorf("sample_facts: total = %d, want 3", sample.Total)
	}
}

// -- delete cannot break stratification (no trial compile needed) -----------

func TestDeleteRuleGroup_DanglingReferenceIsNotAnError(t *testing.T) {
	// Deleting a rule group another group's body references must succeed:
	// an undefined predicate is 0 rows, not a compile error, per this
	// dialect's convention.
	h, _ := newCrudTestHandlers(t, map[string]string{
		"a_1.dl": "a(\"x\").\n",
		"b_1.dl": "b(X) :- a(X).\n",
	})

	out, err := h.deleteRuleGroup(deleteRuleGroupInput{Head: "a", Arity: 1, Revision: 1})
	if err != nil {
		t.Fatalf("delete_rule_group: %v", err)
	}
	if out.IsStale {
		t.Fatalf("delete_rule_group: unexpected stale rejection: %+v", out)
	}

	// b/1 should still exist as a known predicate, now with 0 facts.
	sample, err := h.sampleFacts(sampleFactsInput{Predicate: "b", Arity: 1})
	if err != nil {
		t.Fatalf("sample_facts: %v", err)
	}
	if sample.Total != 0 {
		t.Errorf("sample_facts: b/1 total = %d, want 0 (a/1 deleted, b depends on it)", sample.Total)
	}
}
