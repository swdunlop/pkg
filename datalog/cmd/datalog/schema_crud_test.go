package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"swdunlop.dev/pkg/datalog"
	"swdunlop.dev/pkg/datalog/jsonfacts"
)

// This file covers the schema CRUD tools (phase 1c,
// doc/features/workbench-v2.md design decision 4): put_source/
// delete_source, put_matcher/delete_matcher, put_declaration/
// delete_declaration, plus the three reads (get_config, predicate_deps,
// explain_fact). It mirrors rules_crud_test.go's coverage shape: create/edit
// happy paths, stale rejections with exact current-item assertions,
// validation failures leave disk/memory/revisions untouched, deterministic
// serialization stability, round-trip through a simulated restart, and
// no-configPath rejection.

// newSchemaCrudTestHandlers builds mcpHandlers rooted at dataDir with a
// schema file at dataDir/"schema.yaml" (seeded with seedYAML, or empty if
// ""), the fixture every put/delete/get_config/predicate_deps/explain_fact
// test in this file starts from. writeSyntheticData(t, dataDir, n) is
// called by the caller when the fixture needs a matching events.jsonl.
func newSchemaCrudTestHandlers(t *testing.T, dataDir string, seedYAML string) (*mcpHandlers, string) {
	t.Helper()
	schemaPath := filepath.Join(dataDir, "schema.yaml")
	if seedYAML == "" {
		seedYAML = "{}\n"
	}
	mustWriteFile(t, schemaPath, seedYAML)

	h, closeFn, err := newMCPHandlers(dataDir, schemaPath, nil, "", 5_000_000_000)
	if err != nil {
		t.Fatalf("newMCPHandlers: %v", err)
	}
	t.Cleanup(func() { closeFn() })
	return h, schemaPath
}

// -- put_source: create/edit/stale ------------------------------------------

func TestPutSource_CreateWithZeroRevision(t *testing.T) {
	dir := t.TempDir()
	writeSyntheticData(t, dir, 3)
	h, schemaPath := newSchemaCrudTestHandlers(t, dir, "")

	out, err := h.putSource(putSourceInput{
		File:     "events.jsonl",
		Mappings: jsonMapping("event", "value.host", "value.pid", "value.cmd"),
		Revision: 0,
	})
	if err != nil {
		t.Fatalf("put_source: %v", err)
	}
	if out.IsStale {
		t.Fatalf("put_source: unexpected stale rejection: %+v", out)
	}
	if out.Revision != 1 {
		t.Errorf("put_source: revision = %d, want 1", out.Revision)
	}
	found := false
	for _, p := range out.Predicates {
		if p.Name == "event" && p.Arity == 3 {
			found = true
			if p.Facts != 3 {
				t.Errorf("put_source: event/3 facts = %d, want 3", p.Facts)
			}
		}
	}
	if !found {
		t.Errorf("put_source: event/3 missing from Predicates: %+v", out.Predicates)
	}

	data, err := os.ReadFile(schemaPath)
	if err != nil {
		t.Fatalf("reading schema file: %v", err)
	}
	if !strings.Contains(string(data), "events.jsonl") {
		t.Errorf("schema file %q missing the new source", string(data))
	}
}

func TestPutSource_CreateRejectsNonZeroRevision(t *testing.T) {
	dir := t.TempDir()
	writeSyntheticData(t, dir, 1)
	h, _ := newSchemaCrudTestHandlers(t, dir, "")

	out, err := h.putSource(putSourceInput{File: "events.jsonl", Mappings: jsonMapping("event", "value.host"), Revision: 5})
	if err != nil {
		t.Fatalf("put_source: unexpected Go error (staleness is not an error): %v", err)
	}
	if !out.IsStale {
		t.Fatal("put_source: expected a stale rejection for a create attempt with revision != 0")
	}
	if out.CurrentSource != nil || out.CurrentRevision != 0 {
		t.Errorf("put_source: a create-vs-nonexistent stale rejection must carry no current content, got %+v", out)
	}
}

func TestPutSource_EditWithCorrectRevision(t *testing.T) {
	dir := t.TempDir()
	writeSyntheticData(t, dir, 3)
	h, _ := newSchemaCrudTestHandlers(t, dir, "")

	created, err := h.putSource(putSourceInput{File: "events.jsonl", Mappings: jsonMapping("event", "value.host", "value.pid", "value.cmd"), Revision: 0})
	if err != nil {
		t.Fatalf("put_source (create): %v", err)
	}

	edited, err := h.putSource(putSourceInput{
		File: "events.jsonl", Mappings: jsonMapping("ev2", "value.host"), Revision: created.Revision,
	})
	if err != nil {
		t.Fatalf("put_source (edit): %v", err)
	}
	if edited.IsStale {
		t.Fatalf("put_source (edit): unexpected stale rejection: %+v", edited)
	}
	if edited.Revision != 2 {
		t.Errorf("put_source (edit): revision = %d, want 2", edited.Revision)
	}

	// Old predicate must be gone (whole-source replacement, not merge).
	list, err := h.listPredicates(listPredicatesInput{})
	if err != nil {
		t.Fatalf("list_predicates: %v", err)
	}
	if containsPredicate(list.Predicates, "event") {
		t.Error("put_source (edit): old 'event' predicate survived a source replacement")
	}
}

func TestPutSource_EditRejectsStaleRevision(t *testing.T) {
	dir := t.TempDir()
	writeSyntheticData(t, dir, 1)
	h, _ := newSchemaCrudTestHandlers(t, dir, "")

	if _, err := h.putSource(putSourceInput{File: "events.jsonl", Mappings: jsonMapping("event", "value.host"), Revision: 0}); err != nil {
		t.Fatalf("put_source (create): %v", err)
	}

	out, err := h.putSource(putSourceInput{File: "events.jsonl", Mappings: jsonMapping("event2", "value.host"), Revision: 99})
	if err != nil {
		t.Fatalf("put_source: unexpected Go error: %v", err)
	}
	if !out.IsStale {
		t.Fatal("put_source: expected a stale rejection for a wrong revision")
	}
	if out.CurrentSource == nil || out.CurrentSource.File != "events.jsonl" {
		t.Errorf("put_source: CurrentSource = %+v, want the events.jsonl source", out.CurrentSource)
	}
	if out.CurrentRevision != 1 {
		t.Errorf("put_source: CurrentRevision = %d, want 1", out.CurrentRevision)
	}

	list, err := h.listPredicates(listPredicatesInput{})
	if err != nil {
		t.Fatalf("list_predicates: %v", err)
	}
	if !containsPredicate(list.Predicates, "event") {
		t.Error("put_source: stale edit must not have applied (original 'event' predicate should still exist)")
	}
}

// -- put_source: validation leaves disk/memory untouched ---------------------

func TestPutSource_EscapingFileRejectedLeavesStateUntouched(t *testing.T) {
	dir := t.TempDir()
	writeSyntheticData(t, dir, 1)
	outsideDir := t.TempDir()
	mustWriteFile(t, filepath.Join(outsideDir, "secret.jsonl"), `{"host":"x"}`+"\n")

	h, schemaPath := newSchemaCrudTestHandlers(t, dir, "")
	before, err := os.ReadFile(schemaPath)
	if err != nil {
		t.Fatalf("reading schema file: %v", err)
	}

	badFile := "../" + filepath.Base(outsideDir) + "/secret.jsonl"
	out, err := h.putSource(putSourceInput{File: badFile, Mappings: jsonMapping("event", "value.host"), Revision: 0})
	if err == nil {
		t.Fatalf("put_source: expected an error for an escaping file reference, got %+v", out)
	}

	after, err := os.ReadFile(schemaPath)
	if err != nil {
		t.Fatalf("reading schema file: %v", err)
	}
	if string(before) != string(after) {
		t.Errorf("put_source: schema file changed after a rejected write: before=%q after=%q", before, after)
	}
	if h.schemaRev.sources[badFile] != 0 {
		t.Errorf("put_source: revision map polluted by a rejected write: %+v", h.schemaRev.sources)
	}
}

// -- delete_source ------------------------------------------------------------

func TestDeleteSource_CorrectRevision(t *testing.T) {
	dir := t.TempDir()
	writeSyntheticData(t, dir, 1)
	h, _ := newSchemaCrudTestHandlers(t, dir, "")

	created, err := h.putSource(putSourceInput{File: "events.jsonl", Mappings: jsonMapping("event", "value.host"), Revision: 0})
	if err != nil {
		t.Fatalf("put_source: %v", err)
	}

	out, err := h.deleteSource(deleteSourceInput{File: "events.jsonl", Revision: created.Revision})
	if err != nil {
		t.Fatalf("delete_source: %v", err)
	}
	if out.IsStale {
		t.Fatalf("delete_source: unexpected stale rejection: %+v", out)
	}

	list, err := h.listPredicates(listPredicatesInput{})
	if err != nil {
		t.Fatalf("list_predicates: %v", err)
	}
	if containsPredicate(list.Predicates, "event") {
		t.Error("delete_source: 'event' predicate survived the delete")
	}
}

func TestDeleteSource_StaleRevision(t *testing.T) {
	dir := t.TempDir()
	writeSyntheticData(t, dir, 1)
	h, _ := newSchemaCrudTestHandlers(t, dir, "")

	if _, err := h.putSource(putSourceInput{File: "events.jsonl", Mappings: jsonMapping("event", "value.host"), Revision: 0}); err != nil {
		t.Fatalf("put_source: %v", err)
	}

	out, err := h.deleteSource(deleteSourceInput{File: "events.jsonl", Revision: 42})
	if err != nil {
		t.Fatalf("delete_source: unexpected Go error: %v", err)
	}
	if !out.IsStale {
		t.Fatal("delete_source: expected a stale rejection for a wrong revision")
	}
	if out.CurrentRevision != 1 {
		t.Errorf("delete_source: CurrentRevision = %d, want 1", out.CurrentRevision)
	}
}

func TestDeleteSource_AbsentSource(t *testing.T) {
	dir := t.TempDir()
	h, _ := newSchemaCrudTestHandlers(t, dir, "")

	out, err := h.deleteSource(deleteSourceInput{File: "nope.jsonl", Revision: 1})
	if err != nil {
		t.Fatalf("delete_source: unexpected Go error: %v", err)
	}
	if !out.IsStale {
		t.Fatal("delete_source: expected a stale rejection for a non-existent source")
	}
}

// -- put_matcher / delete_matcher ---------------------------------------------

func TestPutMatcher_CreateAndEdit(t *testing.T) {
	dir := t.TempDir()
	writeSyntheticData(t, dir, 2)
	h, _ := newSchemaCrudTestHandlers(t, dir, "")

	if _, err := h.putSource(putSourceInput{File: "events.jsonl", Mappings: jsonMapping("event", "value.host", "value.pid", "value.cmd"), Revision: 0}); err != nil {
		t.Fatalf("put_source: %v", err)
	}

	out, err := h.putMatcher(putMatcherInput{
		Matcher: matcherWithContains("event", 2, "cmd0"),
	})
	if err != nil {
		t.Fatalf("put_matcher: %v", err)
	}
	if out.IsStale {
		t.Fatalf("put_matcher: unexpected stale rejection: %+v", out)
	}
	if out.Revision != 1 {
		t.Errorf("put_matcher: revision = %d, want 1", out.Revision)
	}

	// Edit: same key, different pattern list.
	edited, err := h.putMatcher(putMatcherInput{
		Matcher:  matcherWithContains("event", 2, "cmd1"),
		Revision: out.Revision,
	})
	if err != nil {
		t.Fatalf("put_matcher (edit): %v", err)
	}
	if edited.IsStale || edited.Revision != 2 {
		t.Errorf("put_matcher (edit): got %+v, want revision 2, not stale", edited)
	}
}

func TestPutMatcher_EditRejectsStaleRevision(t *testing.T) {
	dir := t.TempDir()
	writeSyntheticData(t, dir, 2)
	h, _ := newSchemaCrudTestHandlers(t, dir, "")
	if _, err := h.putSource(putSourceInput{File: "events.jsonl", Mappings: jsonMapping("event", "value.host", "value.pid", "value.cmd"), Revision: 0}); err != nil {
		t.Fatalf("put_source: %v", err)
	}
	if _, err := h.putMatcher(putMatcherInput{Matcher: matcherWithContains("event", 2, "cmd0")}); err != nil {
		t.Fatalf("put_matcher: %v", err)
	}

	out, err := h.putMatcher(putMatcherInput{Matcher: matcherWithContains("event", 2, "cmd1"), Revision: 99})
	if err != nil {
		t.Fatalf("put_matcher: unexpected Go error: %v", err)
	}
	if !out.IsStale {
		t.Fatal("put_matcher: expected a stale rejection for a wrong revision")
	}
	if out.CurrentMatcher == nil || out.CurrentRevision != 1 {
		t.Errorf("put_matcher: current = %+v rev %d, want rev 1", out.CurrentMatcher, out.CurrentRevision)
	}
}

func TestPutMatcher_InvalidRegexRejected(t *testing.T) {
	dir := t.TempDir()
	writeSyntheticData(t, dir, 1)
	h, schemaPath := newSchemaCrudTestHandlers(t, dir, "")
	if _, err := h.putSource(putSourceInput{File: "events.jsonl", Mappings: jsonMapping("event", "value.host"), Revision: 0}); err != nil {
		t.Fatalf("put_source: %v", err)
	}
	before, err := os.ReadFile(schemaPath)
	if err != nil {
		t.Fatalf("reading schema: %v", err)
	}

	_, err = h.putMatcher(putMatcherInput{Matcher: jsonfactsMatcherRegex("event", 0, "[unterminated")})
	if err == nil {
		t.Fatal("put_matcher: expected an error for an invalid regex")
	}

	after, err := os.ReadFile(schemaPath)
	if err != nil {
		t.Fatalf("reading schema: %v", err)
	}
	if string(before) != string(after) {
		t.Errorf("put_matcher: schema file changed after a rejected write")
	}
}

func TestDeleteMatcher_CorrectRevision(t *testing.T) {
	dir := t.TempDir()
	writeSyntheticData(t, dir, 2)
	h, _ := newSchemaCrudTestHandlers(t, dir, "")
	if _, err := h.putSource(putSourceInput{File: "events.jsonl", Mappings: jsonMapping("event", "value.host", "value.pid", "value.cmd"), Revision: 0}); err != nil {
		t.Fatalf("put_source: %v", err)
	}
	created, err := h.putMatcher(putMatcherInput{Matcher: matcherWithContains("event", 2, "cmd0")})
	if err != nil {
		t.Fatalf("put_matcher: %v", err)
	}

	out, err := h.deleteMatcher(deleteMatcherInput{Predicate: "event", Term: 2, Revision: created.Revision})
	if err != nil {
		t.Fatalf("delete_matcher: %v", err)
	}
	if out.IsStale {
		t.Fatalf("delete_matcher: unexpected stale rejection: %+v", out)
	}
}

// -- put_declaration / delete_declaration -------------------------------------

func TestPutDeclaration_CreateAndEdit(t *testing.T) {
	dir := t.TempDir()
	h, _ := newSchemaCrudTestHandlers(t, dir, "")

	out, err := h.putDeclaration(putDeclarationInput{Declaration: declWithUse("process", "a process", "pid", "name")})
	if err != nil {
		t.Fatalf("put_declaration: %v", err)
	}
	if out.IsStale || out.Revision != 1 {
		t.Fatalf("put_declaration: got %+v, want revision 1, not stale", out)
	}

	edited, err := h.putDeclaration(putDeclarationInput{
		Declaration: declWithUse("process", "an updated process", "pid", "name"),
		Revision:    out.Revision,
	})
	if err != nil {
		t.Fatalf("put_declaration (edit): %v", err)
	}
	if edited.IsStale || edited.Revision != 2 {
		t.Errorf("put_declaration (edit): got %+v, want revision 2, not stale", edited)
	}
}

func TestPutDeclaration_DifferentArityIsADifferentKey(t *testing.T) {
	// Item 1's resolved ambiguity: declarations are keyed by (name, arity),
	// mirroring datalog.NewDeclarationSet's own declKey{Name, len(Terms)} —
	// two declarations sharing a Name at different arities are legal,
	// distinct entries, not a collision.
	dir := t.TempDir()
	h, _ := newSchemaCrudTestHandlers(t, dir, "")

	if _, err := h.putDeclaration(putDeclarationInput{Declaration: declWithUse("process", "arity 2", "pid", "name")}); err != nil {
		t.Fatalf("put_declaration (arity 2): %v", err)
	}
	// Same name, three terms: a DIFFERENT key (arity 3), so Revision: 0 must
	// be accepted as a fresh create, not rejected as stale against the
	// arity-2 entry.
	out, err := h.putDeclaration(putDeclarationInput{Declaration: declWithUse("process", "arity 3", "pid", "name", "cmdline")})
	if err != nil {
		t.Fatalf("put_declaration (arity 3): %v", err)
	}
	if out.IsStale {
		t.Fatalf("put_declaration: arity 3 create wrongly rejected as stale against the arity 2 entry: %+v", out)
	}

	cfg, err := h.getConfig(getConfigInput{})
	if err != nil {
		t.Fatalf("get_config: %v", err)
	}
	if len(cfg.Declarations) != 2 {
		t.Fatalf("get_config: got %d declarations, want 2 (process/2 and process/3)", len(cfg.Declarations))
	}
}

func TestDeleteDeclaration_CorrectRevision(t *testing.T) {
	dir := t.TempDir()
	h, _ := newSchemaCrudTestHandlers(t, dir, "")

	created, err := h.putDeclaration(putDeclarationInput{Declaration: declWithUse("process", "a process", "pid")})
	if err != nil {
		t.Fatalf("put_declaration: %v", err)
	}

	out, err := h.deleteDeclaration(deleteDeclarationInput{Name: "process", Arity: 1, Revision: created.Revision})
	if err != nil {
		t.Fatalf("delete_declaration: %v", err)
	}
	if out.IsStale {
		t.Fatalf("delete_declaration: unexpected stale rejection: %+v", out)
	}

	cfg, err := h.getConfig(getConfigInput{})
	if err != nil {
		t.Fatalf("get_config: %v", err)
	}
	if len(cfg.Declarations) != 0 {
		t.Errorf("get_config: got %d declarations after delete, want 0", len(cfg.Declarations))
	}
}

// -- no configPath session ----------------------------------------------------

func TestSchemaCrudTools_ErrorWithoutConfigPath(t *testing.T) {
	h, done := newTestHandlers(t, t.TempDir())
	defer done()

	_, err := h.putSource(putSourceInput{File: "x.jsonl", Mappings: jsonMapping("x", "value.a")})
	if err == nil {
		t.Fatal("put_source: expected an error with no configPath")
	}
	putSourceErr := err.Error()

	_, err = h.deleteSource(deleteSourceInput{File: "x.jsonl", Revision: 1})
	if err == nil || err.Error() != putSourceErr {
		t.Errorf("delete_source: error = %v, want %q", err, putSourceErr)
	}
	_, err = h.putMatcher(putMatcherInput{Matcher: matcherWithContains("x", 0, "y")})
	if err == nil || err.Error() != putSourceErr {
		t.Errorf("put_matcher: error = %v, want %q", err, putSourceErr)
	}
	_, err = h.deleteMatcher(deleteMatcherInput{Predicate: "x", Term: 0, Revision: 1})
	if err == nil || err.Error() != putSourceErr {
		t.Errorf("delete_matcher: error = %v, want %q", err, putSourceErr)
	}
	_, err = h.putDeclaration(putDeclarationInput{Declaration: declWithUse("x", "doc", "a")})
	if err == nil || err.Error() != putSourceErr {
		t.Errorf("put_declaration: error = %v, want %q", err, putSourceErr)
	}
	_, err = h.deleteDeclaration(deleteDeclarationInput{Name: "x", Arity: 1, Revision: 1})
	if err == nil || err.Error() != putSourceErr {
		t.Errorf("delete_declaration: error = %v, want %q", err, putSourceErr)
	}

	// get_config has NO such restriction (design decision 7's read path).
	if _, err := h.getConfig(getConfigInput{}); err != nil {
		t.Errorf("get_config: unexpected error on a configPath-less session: %v", err)
	}
}

// -- round trip: put -> get_config -> restart from disk ----------------------

func TestPutMatcher_SurvivesReloadFromDisk(t *testing.T) {
	dir := t.TempDir()
	writeSyntheticData(t, dir, 2)
	h, schemaPath := newSchemaCrudTestHandlers(t, dir, "")
	if _, err := h.putSource(putSourceInput{File: "events.jsonl", Mappings: jsonMapping("event", "value.host", "value.pid", "value.cmd"), Revision: 0}); err != nil {
		t.Fatalf("put_source: %v", err)
	}
	if _, err := h.putMatcher(putMatcherInput{Matcher: matcherWithContains("event", 2, "cmd0")}); err != nil {
		t.Fatalf("put_matcher: %v", err)
	}

	cfg, err := h.getConfig(getConfigInput{})
	if err != nil {
		t.Fatalf("get_config: %v", err)
	}
	if len(cfg.Matchers) != 1 {
		t.Fatalf("get_config: got %d matchers, want 1", len(cfg.Matchers))
	}

	// Simulate a process restart: build a fresh handlers value from the same
	// schema file/data dir.
	fresh, closeFn, err := newMCPHandlers(dir, schemaPath, nil, "", 5_000_000_000)
	if err != nil {
		t.Fatalf("newMCPHandlers (fresh): %v", err)
	}
	defer closeFn()

	freshCfg, err := fresh.getConfig(getConfigInput{})
	if err != nil {
		t.Fatalf("get_config (fresh): %v", err)
	}
	if len(freshCfg.Matchers) != 1 {
		t.Fatalf("get_config (fresh): got %d matchers after reload, want 1 (content must survive)", len(freshCfg.Matchers))
	}
	if freshCfg.Matchers[0].Revision != 1 {
		t.Errorf("get_config (fresh): matcher revision = %d, want 1 (revisions reset on reload)", freshCfg.Matchers[0].Revision)
	}
}

// -- concurrent write during the lock-free prepare turns into stale ----------

// TestPutSource_ConcurrentWriteDuringPrepareTurnsStale simulates the race
// design decision 3 calls out: prepareSchemaWrite (the expensive
// parse/confine/load half) runs with NO lock held, so another write can land
// in between it finishing and commitSchemaWrite's recheck running. This
// drives that ordering directly through the two exposed halves (rather than
// a genuine goroutine race, which the brief allows: "you can simulate by
// mutating revision between prepare and apply if the seam allows") to prove
// the recheck inside commitSchemaWrite — not just the cheap up-front check —
// is what actually guards the window: without it, the second write's result
// would be silently lost the moment the first write's commit ran.
func TestPutSource_ConcurrentWriteDuringPrepareTurnsStale(t *testing.T) {
	dir := t.TempDir()
	writeSyntheticData(t, dir, 1)
	h, _ := newSchemaCrudTestHandlers(t, dir, "")

	if _, err := h.putSource(putSourceInput{File: "events.jsonl", Mappings: jsonMapping("event", "value.host"), Revision: 0}); err != nil {
		t.Fatalf("put_source (seed): %v", err)
	}

	// Simulate call A's lock-free prepare phase, based on revision 1. The
	// prospective is built from the AUTHORING config, exactly as the real
	// write paths do (see prepareSchemaWrite's doc comment).
	h.mu.Lock()
	cfgA := h.sess.authoringCfg
	cfgA.Sources = replaceSource(cfgA.Sources, jsonfacts.Source{File: "events.jsonl", Mappings: jsonMapping("eventA", "value.host")})
	h.mu.Unlock()
	textA, authoringA, runtimeA, dbA, err := h.prepareSchemaWrite(cfgA)
	if err != nil {
		t.Fatalf("prepareSchemaWrite (A): %v", err)
	}

	// Call B lands and completes FULLY (its own prepare + commit) while A's
	// prepare result sits unused — this is the "concurrent write during the
	// lock-free prepare" window.
	bOut, err := h.putSource(putSourceInput{File: "events.jsonl", Mappings: jsonMapping("eventB", "value.host"), Revision: 1})
	if err != nil {
		t.Fatalf("put_source (B): %v", err)
	}
	if bOut.IsStale || bOut.Revision != 2 {
		t.Fatalf("put_source (B): got %+v, want a clean revision-2 write", bOut)
	}

	// Call A now reaches its commit phase, STILL carrying revision 1 — it
	// must be rejected as stale (B already bumped the key to revision 2),
	// not silently overwrite B's write.
	h.mu.Lock()
	result, ok, err := h.commitSchemaWrite(textA, authoringA, runtimeA, dbA, func(newRevs *schemaRevisions) bool {
		_, stillExists := findSource(h.sess.authoringCfg.Sources, "events.jsonl")
		wantRev := 0
		if stillExists {
			wantRev = h.schemaRev.sources["events.jsonl"]
		}
		if wantRev != 1 { // A's revision
			return false
		}
		newRevs.sources["events.jsonl"] = nextRevision(stillExists, 1, h.schemaRev.deletedSources["events.jsonl"])
		return true
	})
	h.mu.Unlock()
	if err != nil {
		t.Fatalf("commitSchemaWrite (A): %v", err)
	}
	if ok {
		t.Fatalf("commitSchemaWrite (A): expected the recheck to reject A as stale (B already bumped the revision), got %+v", result)
	}

	// The session must still reflect B's write, not A's.
	list, err := h.listPredicates(listPredicatesInput{})
	if err != nil {
		t.Fatalf("list_predicates: %v", err)
	}
	if !containsPredicate(list.Predicates, "eventB") {
		t.Error("B's write was lost: eventB predicate missing after A's stale commit was rejected")
	}
	if containsPredicate(list.Predicates, "eventA") {
		t.Error("A's stale write was applied anyway: eventA predicate should not exist")
	}
}

// -- *_from indirections survive agent writes ---------------------------------

// fromIndirectionSchemaYAML is the seed schema for the *_from preservation
// tests: one source plus one matcher whose patterns come from a pattern
// FILE (iocs.txt), not an inline list. The session resolves this into the
// runtime config (patterns baked inline, _from cleared) for matching, but
// the AUTHORING config — and therefore every CRUD write and read — must
// keep the indirection; see session.authoringCfg's doc comment.
const fromIndirectionSchemaYAML = `
sources:
  - file: events.jsonl
    mappings:
      - predicate: event
        args: ["value.host", "value.pid", "value.cmd"]
matchers:
  - predicate: event
    term: 2
    contains_from: iocs.txt
`

// newFromIndirectionHandlers builds the *_from fixture: synthetic events
// data, an iocs.txt pattern file containing "cmd0", and the seed schema
// above. Returns the handlers, the schema path, and the contains/2 fact
// count the resolved matcher produces at load (the extraction baseline
// writes must preserve).
func newFromIndirectionHandlers(t *testing.T) (*mcpHandlers, string, int) {
	t.Helper()
	dir := t.TempDir()
	writeSyntheticData(t, dir, 2)
	mustWriteFile(t, filepath.Join(dir, "iocs.txt"), "cmd0\n")
	h, schemaPath := newSchemaCrudTestHandlers(t, dir, fromIndirectionSchemaYAML)

	sample, err := h.sampleFacts(sampleFactsInput{Predicate: "contains", Arity: 2})
	if err != nil {
		t.Fatalf("sample_facts (baseline): %v", err)
	}
	if sample.Total == 0 {
		t.Fatal("fixture broken: the contains_from matcher produced no contains/2 facts at load")
	}
	return h, schemaPath, sample.Total
}

// TestSchemaWrite_PreservesFromIndirection pins the mechanism fix for the
// resolved-config-on-disk defect: an agent write that touches something
// UNRELATED (a declaration) must not serialize the resolved runtime form —
// the on-disk YAML must still carry the matcher's contains_from key, must
// NOT contain the pattern file's baked-inline patterns, and a restart from
// that file must resolve and match exactly as before the write.
func TestSchemaWrite_PreservesFromIndirection(t *testing.T) {
	h, schemaPath, baseline := newFromIndirectionHandlers(t)

	if _, err := h.putDeclaration(putDeclarationInput{Declaration: declWithUse("event", "an event", "host", "pid", "cmd")}); err != nil {
		t.Fatalf("put_declaration: %v", err)
	}

	data, err := os.ReadFile(schemaPath)
	if err != nil {
		t.Fatalf("reading schema file: %v", err)
	}
	text := string(data)
	if !strings.Contains(text, "contains_from: iocs.txt") {
		t.Errorf("schema file lost the contains_from indirection after an unrelated write:\n%s", text)
	}
	if strings.Contains(text, "cmd0") {
		t.Errorf("schema file has iocs.txt's patterns baked inline after an unrelated write:\n%s", text)
	}

	// Restart from the written file: the indirection must still resolve and
	// the matcher must produce the same facts as before the write.
	dataDir := filepath.Dir(schemaPath)
	fresh, closeFn, err := newMCPHandlers(dataDir, schemaPath, nil, "", 5_000_000_000)
	if err != nil {
		t.Fatalf("newMCPHandlers (fresh): %v", err)
	}
	defer closeFn()
	sample, err := fresh.sampleFacts(sampleFactsInput{Predicate: "contains", Arity: 2})
	if err != nil {
		t.Fatalf("sample_facts (fresh): %v", err)
	}
	if sample.Total != baseline {
		t.Errorf("contains/2 facts after restart = %d, want %d (extraction must be unchanged by the write)", sample.Total, baseline)
	}
}

// TestGetConfig_ShowsFromFieldUnresolved pins get_config to the authoring
// form: the matcher must come back with its contains_from field intact and
// WITHOUT the resolved inline patterns.
func TestGetConfig_ShowsFromFieldUnresolved(t *testing.T) {
	h, _, _ := newFromIndirectionHandlers(t)

	cfg, err := h.getConfig(getConfigInput{})
	if err != nil {
		t.Fatalf("get_config: %v", err)
	}
	if len(cfg.Matchers) != 1 {
		t.Fatalf("get_config: got %d matchers, want 1", len(cfg.Matchers))
	}
	m := cfg.Matchers[0]
	if m.ContainsFrom != "iocs.txt" {
		t.Errorf("get_config: matcher ContainsFrom = %q, want iocs.txt (authoring form, unresolved)", m.ContainsFrom)
	}
	if len(m.Contains) != 0 {
		t.Errorf("get_config: matcher Contains = %v, want empty (patterns must not be baked in)", m.Contains)
	}
}

// TestPutMatcher_FromFieldRoundTrips pins the edit path: editing THAT
// matcher via put_matcher, submitting the authoring form (contains_from
// still set) exactly as a caller re-basing on a CurrentMatcher/get_config
// handback would, must succeed, keep the indirection on disk, and keep
// matching working.
func TestPutMatcher_FromFieldRoundTrips(t *testing.T) {
	h, schemaPath, baseline := newFromIndirectionHandlers(t)

	cfg, err := h.getConfig(getConfigInput{})
	if err != nil {
		t.Fatalf("get_config: %v", err)
	}
	current := cfg.Matchers[0]

	// Re-put the matcher exactly as get_config handed it back (the authoring
	// form): the write must be accepted, bump the revision, and keep the
	// *_from indirection — the shape of a caller re-basing an edit on a
	// CurrentMatcher/get_config handback.
	out, err := h.putMatcher(putMatcherInput{Matcher: current.Matcher, Revision: current.Revision})
	if err != nil {
		t.Fatalf("put_matcher (round trip): %v", err)
	}
	if out.IsStale || out.Revision != current.Revision+1 {
		t.Fatalf("put_matcher (round trip): got %+v, want revision %d, not stale", out, current.Revision+1)
	}

	data, err := os.ReadFile(schemaPath)
	if err != nil {
		t.Fatalf("reading schema file: %v", err)
	}
	if !strings.Contains(string(data), "contains_from: iocs.txt") {
		t.Errorf("schema file lost contains_from after put_matcher round trip:\n%s", data)
	}
	if strings.Contains(string(data), "cmd0") {
		t.Errorf("schema file has baked patterns after put_matcher round trip:\n%s", data)
	}

	sample, err := h.sampleFacts(sampleFactsInput{Predicate: "contains", Arity: 2})
	if err != nil {
		t.Fatalf("sample_facts: %v", err)
	}
	if sample.Total != baseline {
		t.Errorf("contains/2 facts after round trip = %d, want %d", sample.Total, baseline)
	}
}

// -- *_from indirection survives agent writes ---------------------------------

// fromSchemaYAML is a schema whose matcher loads its patterns from a
// pattern FILE (contains_from) rather than an inline list — the authoring
// form that must survive on disk across agent writes (session.authoringCfg's
// doc comment): serializing the RESOLVED runtime config instead would bake
// iocs.txt's patterns inline and drop the contains_from key, silently
// disconnecting future pattern-file edits from extraction.
const fromSchemaYAML = `
sources:
  - file: events.jsonl
    mappings:
      - predicate: event
        args: ["value.host", "value.pid", "value.cmd"]
matchers:
  - predicate: event
    term: 2
    contains_from: iocs.txt
`

// newFromSchemaFixture builds the contains_from fixture: events.jsonl (2
// records), iocs.txt (one pattern, matching record 0's cmd), and a session
// loaded from fromSchemaYAML.
func newFromSchemaFixture(t *testing.T) (h *mcpHandlers, dir, schemaPath string) {
	t.Helper()
	dir = t.TempDir()
	writeSyntheticData(t, dir, 2)
	mustWriteFile(t, filepath.Join(dir, "iocs.txt"), "# ioc patterns\ncmd0\n")
	h, schemaPath = newSchemaCrudTestHandlers(t, dir, fromSchemaYAML)
	return h, dir, schemaPath
}

// countContainsFacts returns the current fact count of contains/2 — the
// predicate the contains_from matcher emits — so tests can prove extraction
// behavior is unchanged across a write + restart.
func countContainsFacts(t *testing.T, h *mcpHandlers) int {
	t.Helper()
	out, err := h.sampleFacts(sampleFactsInput{Predicate: "contains", Arity: 2})
	if err != nil {
		t.Fatalf("sample_facts(contains/2): %v", err)
	}
	return out.Total
}

func TestPutDeclaration_PreservesContainsFromOnDisk(t *testing.T) {
	h, _, schemaPath := newFromSchemaFixture(t)
	factsBefore := countContainsFacts(t, h)
	if factsBefore == 0 {
		t.Fatal("fixture broken: contains_from matcher produced no facts before the write")
	}

	// An UNRELATED write — a declaration — must not disturb the matcher's
	// *_from indirection when the whole file is rewritten canonically.
	if _, err := h.putDeclaration(putDeclarationInput{Declaration: declWithUse("event", "an event", "host", "pid", "cmd")}); err != nil {
		t.Fatalf("put_declaration: %v", err)
	}

	data, err := os.ReadFile(schemaPath)
	if err != nil {
		t.Fatalf("reading schema file: %v", err)
	}
	onDisk := string(data)
	if !strings.Contains(onDisk, "contains_from") || !strings.Contains(onDisk, "iocs.txt") {
		t.Errorf("schema file lost the contains_from indirection after an unrelated write:\n%s", onDisk)
	}
	if strings.Contains(onDisk, "cmd0") {
		t.Errorf("schema file has iocs.txt's patterns baked inline after an unrelated write:\n%s", onDisk)
	}

	// A restart from the written file must still resolve iocs.txt and
	// produce the same match facts as before the write.
	fresh, closeFn, err := newMCPHandlers(filepath.Dir(schemaPath), schemaPath, nil, "", 5_000_000_000)
	if err != nil {
		t.Fatalf("newMCPHandlers (fresh): %v", err)
	}
	defer closeFn()
	if factsAfter := countContainsFacts(t, fresh); factsAfter != factsBefore {
		t.Errorf("contains/2 facts after restart = %d, want %d (extraction behavior must survive the write)", factsAfter, factsBefore)
	}
}

func TestGetConfig_ShowsContainsFromUnresolved(t *testing.T) {
	h, _, _ := newFromSchemaFixture(t)

	cfg, err := h.getConfig(getConfigInput{})
	if err != nil {
		t.Fatalf("get_config: %v", err)
	}
	if len(cfg.Matchers) != 1 {
		t.Fatalf("get_config: got %d matchers, want 1", len(cfg.Matchers))
	}
	m := cfg.Matchers[0]
	if m.ContainsFrom != "iocs.txt" {
		t.Errorf("get_config: matcher ContainsFrom = %q, want iocs.txt (authoring form, unresolved)", m.ContainsFrom)
	}
	if len(m.Contains) != 0 {
		t.Errorf("get_config: matcher has baked inline patterns %v; the authoring form must not", m.Contains)
	}
}

func TestPutMatcher_ContainsFromRoundTrips(t *testing.T) {
	h, _, schemaPath := newFromSchemaFixture(t)

	// Edit THE contains_from matcher itself in authoring form: same key
	// (event, 2, false, false), keep the indirection, add one inline
	// pattern beside it.
	edited, err := h.putMatcher(putMatcherInput{
		Matcher: jsonfacts.Matcher{
			Predicate: "event", Term: 2,
			ContainsFrom: "iocs.txt",
			Contains:     []string{"cmd1"},
		},
		Revision: 1,
	})
	if err != nil {
		t.Fatalf("put_matcher: %v", err)
	}
	if edited.IsStale || edited.Revision != 2 {
		t.Fatalf("put_matcher: got %+v, want revision 2, not stale", edited)
	}

	// On disk: contains_from survives beside the new inline pattern, and
	// iocs.txt's own pattern is still NOT baked in.
	data, err := os.ReadFile(schemaPath)
	if err != nil {
		t.Fatalf("reading schema file: %v", err)
	}
	onDisk := string(data)
	if !strings.Contains(onDisk, "contains_from") {
		t.Errorf("schema file lost contains_from on an edit of the matcher itself:\n%s", onDisk)
	}
	if !strings.Contains(onDisk, "cmd1") {
		t.Errorf("schema file missing the new inline pattern:\n%s", onDisk)
	}
	if strings.Contains(onDisk, "cmd0") {
		t.Errorf("schema file has iocs.txt's pattern baked inline:\n%s", onDisk)
	}

	// get_config hands the edited matcher back in the same authoring form.
	cfg, err := h.getConfig(getConfigInput{})
	if err != nil {
		t.Fatalf("get_config: %v", err)
	}
	if len(cfg.Matchers) != 1 || cfg.Matchers[0].ContainsFrom != "iocs.txt" || len(cfg.Matchers[0].Contains) != 1 {
		t.Errorf("get_config: matcher = %+v, want contains_from iocs.txt plus one inline pattern", cfg.Matchers[0])
	}

	// And matching still works with BOTH pattern sources: iocs.txt's cmd0
	// (record 0) and inline cmd1 (record 1) each produce a contains fact.
	if got := countContainsFacts(t, h); got != 2 {
		t.Errorf("contains/2 facts = %d, want 2 (one from iocs.txt, one from the inline pattern)", got)
	}
}

// -- deterministic serialization ----------------------------------------------

func TestSerializeConfigYAML_StableAndSorted(t *testing.T) {
	dir := t.TempDir()
	writeSyntheticData(t, dir, 1)
	mustWriteFile(t, filepath.Join(dir, "zzz.jsonl"), `{"a": 1}`+"\n")
	mustWriteFile(t, filepath.Join(dir, "aaa.jsonl"), `{"a": 1}`+"\n")
	h, _ := newSchemaCrudTestHandlers(t, dir, "")

	// Insert out of alphabetical order to prove the serializer, not insertion
	// order, controls output order.
	if _, err := h.putSource(putSourceInput{File: "zzz.jsonl", Mappings: jsonMapping("z", "value.a"), Revision: 0}); err != nil {
		t.Fatalf("put_source (zzz): %v", err)
	}
	if _, err := h.putSource(putSourceInput{File: "aaa.jsonl", Mappings: jsonMapping("a", "value.a"), Revision: 0}); err != nil {
		t.Fatalf("put_source (aaa): %v", err)
	}

	h.mu.Lock()
	cfg := h.sess.cfg
	h.mu.Unlock()

	text1, err := serializeConfigYAML(cfg)
	if err != nil {
		t.Fatalf("serializeConfigYAML: %v", err)
	}
	text2, err := serializeConfigYAML(cfg)
	if err != nil {
		t.Fatalf("serializeConfigYAML (again): %v", err)
	}
	if text1 != text2 {
		t.Errorf("serializeConfigYAML: not stable across two calls:\n--- 1 ---\n%s\n--- 2 ---\n%s", text1, text2)
	}

	aaaIdx := strings.Index(text1, "aaa.jsonl")
	zzzIdx := strings.Index(text1, "zzz.jsonl")
	if aaaIdx < 0 || zzzIdx < 0 || aaaIdx > zzzIdx {
		t.Errorf("serializeConfigYAML: sources not sorted by file: %s", text1)
	}
}

// -- predicate_deps ------------------------------------------------------------

func TestPredicateDeps_BaseWithMatcherAndDeclaration(t *testing.T) {
	dir := t.TempDir()
	writeSyntheticData(t, dir, 2)
	h, _ := newSchemaCrudTestHandlers(t, dir, "")
	if _, err := h.putSource(putSourceInput{File: "events.jsonl", Mappings: jsonMapping("event", "value.host", "value.pid", "value.cmd"), Revision: 0}); err != nil {
		t.Fatalf("put_source: %v", err)
	}
	if _, err := h.putMatcher(putMatcherInput{Matcher: matcherWithContains("event", 2, "cmd0")}); err != nil {
		t.Fatalf("put_matcher: %v", err)
	}
	if _, err := h.putDeclaration(putDeclarationInput{Declaration: declWithUse("event", "an event", "host", "pid", "cmd")}); err != nil {
		t.Fatalf("put_declaration: %v", err)
	}

	out, err := h.predicateDeps(predicateDepsInput{Predicate: "event", Arity: 3})
	if err != nil {
		t.Fatalf("predicate_deps: %v", err)
	}
	if len(out.DependsOnMatchers) != 1 || out.DependsOnMatchers[0].Predicate != "event" {
		t.Errorf("predicate_deps: DependsOnMatchers = %+v, want one event matcher", out.DependsOnMatchers)
	}
	if out.Declaration == nil || out.Declaration.Name != "event" {
		t.Errorf("predicate_deps: Declaration = %+v, want event/3", out.Declaration)
	}
}

func TestPredicateDeps_DerivedWithGroupAddress(t *testing.T) {
	dataDir := t.TempDir()
	rulesDir := filepath.Join(dataDir, "rules")
	if err := os.MkdirAll(rulesDir, 0o755); err != nil {
		t.Fatalf("mkdir rules: %v", err)
	}
	mustWriteFile(t, filepath.Join(rulesDir, "b_1.dl"), "b(X) :- a(X).\n")
	h, closeFn, err := newMCPHandlers(dataDir, "", nil, rulesDir, 5_000_000_000)
	if err != nil {
		t.Fatalf("newMCPHandlers: %v", err)
	}
	defer closeFn()

	out, err := h.predicateDeps(predicateDepsInput{Predicate: "b", Arity: 1})
	if err != nil {
		t.Fatalf("predicate_deps: %v", err)
	}
	if len(out.DependsOnGroups) != 1 || out.DependsOnGroups[0].Head != "b" || out.DependsOnGroups[0].File != "b_1.dl" {
		t.Fatalf("predicate_deps: DependsOnGroups = %+v, want one b/1 group with file b_1.dl", out.DependsOnGroups)
	}

	depOut, err := h.predicateDeps(predicateDepsInput{Predicate: "a", Arity: 1})
	if err != nil {
		t.Fatalf("predicate_deps (a/1): %v", err)
	}
	if len(depOut.DependedOnBy) != 1 || depOut.DependedOnBy[0].Head != "b" {
		t.Fatalf("predicate_deps (a/1): DependedOnBy = %+v, want b/1", depOut.DependedOnBy)
	}
}

func TestPredicateDeps_UnknownPredicateErrors(t *testing.T) {
	h, done := newTestHandlers(t, t.TempDir())
	defer done()

	_, err := h.predicateDeps(predicateDepsInput{Predicate: "nope", Arity: 1})
	if err == nil {
		t.Fatal("predicate_deps: expected an error for an unknown predicate")
	}
}

// -- explain_fact --------------------------------------------------------------

func TestExplainFact_DerivedOneStepWithAddress(t *testing.T) {
	dataDir := t.TempDir()
	rulesDir := filepath.Join(dataDir, "rules")
	if err := os.MkdirAll(rulesDir, 0o755); err != nil {
		t.Fatalf("mkdir rules: %v", err)
	}
	mustWriteFile(t, filepath.Join(rulesDir, "a_1.dl"), "a(\"x\").\n")
	mustWriteFile(t, filepath.Join(rulesDir, "b_1.dl"), "b(X) :- a(X).\n")
	h, closeFn, err := newMCPHandlers(dataDir, "", nil, rulesDir, 5_000_000_000)
	if err != nil {
		t.Fatalf("newMCPHandlers: %v", err)
	}
	defer closeFn()

	out, err := h.explainFact(context.Background(), explainFactInput{Fact: `b("x")`})
	if err != nil {
		t.Fatalf("explain_fact: %v", err)
	}
	if !out.Exists || out.Kind != "derived" {
		t.Fatalf("explain_fact: got %+v, want an existing derived fact", out)
	}
	if out.RuleAddress == nil || out.RuleAddress.Head != "b" || out.RuleAddress.File != "b_1.dl" {
		t.Errorf("explain_fact: RuleAddress = %+v, want b/1 at b_1.dl", out.RuleAddress)
	}
	if len(out.Premises) != 1 || out.Premises[0].Fact != `a("x")` || !out.Premises[0].Base {
		t.Errorf("explain_fact: Premises = %+v, want one base premise a(\"x\")", out.Premises)
	}
}

func TestExplainFact_BaseWithDeclarationAndMatchers(t *testing.T) {
	dir := t.TempDir()
	writeSyntheticData(t, dir, 2)
	h, _ := newSchemaCrudTestHandlers(t, dir, "")
	if _, err := h.putSource(putSourceInput{File: "events.jsonl", Mappings: jsonMapping("event", "value.host", "value.pid", "value.cmd"), Revision: 0}); err != nil {
		t.Fatalf("put_source: %v", err)
	}
	if _, err := h.putMatcher(putMatcherInput{Matcher: matcherWithContains("event", 2, "cmd0")}); err != nil {
		t.Fatalf("put_matcher: %v", err)
	}
	if _, err := h.putDeclaration(putDeclarationInput{Declaration: declWithUse("event", "an event", "host", "pid", "cmd")}); err != nil {
		t.Fatalf("put_declaration: %v", err)
	}

	out, err := h.explainFact(context.Background(), explainFactInput{Fact: `event("h0", 0, "cmd0")`})
	if err != nil {
		t.Fatalf("explain_fact: %v", err)
	}
	if !out.Exists || out.Kind != "base" {
		t.Fatalf("explain_fact: got %+v, want an existing base fact", out)
	}
	if out.Declaration == nil || out.Declaration.Name != "event" {
		t.Errorf("explain_fact: Declaration = %+v, want event/3", out.Declaration)
	}
	if len(out.CandidateMatchers) != 1 || out.CandidateMatchers[0].Predicate != "event" {
		t.Errorf("explain_fact: CandidateMatchers = %+v, want one event matcher", out.CandidateMatchers)
	}
}

func TestExplainFact_NonexistentFact(t *testing.T) {
	dir := t.TempDir()
	writeSyntheticData(t, dir, 1)
	h, _ := newSchemaCrudTestHandlers(t, dir, "")
	if _, err := h.putSource(putSourceInput{File: "events.jsonl", Mappings: jsonMapping("event", "value.host"), Revision: 0}); err != nil {
		t.Fatalf("put_source: %v", err)
	}

	out, err := h.explainFact(context.Background(), explainFactInput{Fact: `event("nonexistent")`})
	if err != nil {
		t.Fatalf("explain_fact: %v", err)
	}
	if out.Exists {
		t.Fatalf("explain_fact: got %+v, want Exists=false for a fact the evaluation never produced", out)
	}
}

// -- get_config -----------------------------------------------------------

func TestGetConfig_ReflectsCurrentRevisions(t *testing.T) {
	dir := t.TempDir()
	h, _ := newSchemaCrudTestHandlers(t, dir, "")

	if _, err := h.putDeclaration(putDeclarationInput{Declaration: declWithUse("x", "doc", "a")}); err != nil {
		t.Fatalf("put_declaration: %v", err)
	}
	edited, err := h.putDeclaration(putDeclarationInput{Declaration: declWithUse("x", "doc2", "a"), Revision: 1})
	if err != nil {
		t.Fatalf("put_declaration (edit): %v", err)
	}

	cfg, err := h.getConfig(getConfigInput{})
	if err != nil {
		t.Fatalf("get_config: %v", err)
	}
	if len(cfg.Declarations) != 1 || cfg.Declarations[0].Revision != edited.Revision {
		t.Fatalf("get_config: got %+v, want one declaration at revision %d", cfg.Declarations, edited.Revision)
	}
}

// -- fixture constructors ----------------------------------------------------

// jsonMapping builds a one-mapping []jsonfacts.Mapping in simple mode: pred
// as the predicate, args as its "args" expr-lang expressions.
func jsonMapping(pred string, args ...string) []jsonfacts.Mapping {
	return []jsonfacts.Mapping{{Predicate: pred, Args: args}}
}

// matcherWithContains builds a minimal jsonfacts.Matcher keyed by
// (pred, term, false, false) with one "contains" pattern.
func matcherWithContains(pred string, term int, pattern string) jsonfacts.Matcher {
	return jsonfacts.Matcher{Predicate: pred, Term: term, Contains: []string{pattern}}
}

// jsonfactsMatcherRegex builds a matcher with one (possibly invalid, for
// negative tests) regex pattern.
func jsonfactsMatcherRegex(pred string, term int, pattern string) jsonfacts.Matcher {
	return jsonfacts.Matcher{Predicate: pred, Term: term, RegexMatch: []string{pattern}}
}

// declWithUse builds a datalog.Declaration with the given named terms
// (each Name-only, no Use/Type), for put_declaration fixtures.
func declWithUse(name, use string, termNames ...string) datalog.Declaration {
	terms := make([]datalog.TermDeclaration, len(termNames))
	for i, n := range termNames {
		terms[i] = datalog.TermDeclaration{Name: n}
	}
	return datalog.Declaration{Name: name, Use: use, Terms: terms}
}
