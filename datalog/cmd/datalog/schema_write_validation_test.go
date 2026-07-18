package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"swdunlop.dev/pkg/datalog/jsonfacts"
)

// This file covers two properties of the schema-write path (workbench-v2
// phase 1c/1d): validateConfigKeyUniqueness must not OVER-reject legitimate
// configs, and commitSchemaWrite's whole-file staleness guard must reject a
// write raced by a concurrent edit while letting uncontended sequential
// writes through.

// ---------------------------------------------------------------------------
// validateConfigKeyUniqueness OVER-REJECTION probes.
//
// The uniqueness check keys matchers by (predicate, term, case_insensitive,
// windash) and declarations by (name, arity=len(Terms)). Legitimate configs
// that DIFFER in any of those key fields must still load. Each of these must
// construct handlers without error.
// ---------------------------------------------------------------------------

func mustLoadConfig(t *testing.T, yamlText string) *mcpHandlers {
	t.Helper()
	dir := t.TempDir()
	schemaPath := filepath.Join(dir, "schema.yaml")
	mustWriteFile(t, schemaPath, yamlText)
	h, closeFn, err := newMCPHandlers(dir, schemaPath, nil, "", 5_000_000_000)
	if err != nil {
		t.Fatalf("newMCPHandlers: config was wrongly rejected: %v\nconfig:\n%s", err, yamlText)
	}
	t.Cleanup(func() { closeFn() })
	return h
}

// Two matchers same predicate+term but DIFFERENT case_insensitive => distinct
// keys, must load.
func TestConfigKeyUniqueness_MatchersDifferByCaseInsensitive(t *testing.T) {
	mustLoadConfig(t, `
matchers:
  - predicate: event
    term: 2
    case_insensitive: false
    contains: ["alpha"]
  - predicate: event
    term: 2
    case_insensitive: true
    contains: ["beta"]
`)
}

// Two matchers same predicate+term but DIFFERENT windash => distinct keys.
func TestConfigKeyUniqueness_MatchersDifferByWindash(t *testing.T) {
	mustLoadConfig(t, `
matchers:
  - predicate: event
    term: 2
    windash: false
    contains: ["alpha"]
  - predicate: event
    term: 2
    windash: true
    contains: ["beta"]
`)
}

// Two matchers same predicate but DIFFERENT term => distinct keys.
func TestConfigKeyUniqueness_MatchersDifferByTerm(t *testing.T) {
	mustLoadConfig(t, `
matchers:
  - predicate: event
    term: 1
    contains: ["alpha"]
  - predicate: event
    term: 2
    contains: ["beta"]
`)
}

// Two declarations same Name at DIFFERENT arities (Terms length differs) =>
// legal per NewDeclarationSet, must load.
func TestConfigKeyUniqueness_DeclarationsSameNameDifferentArity(t *testing.T) {
	h := mustLoadConfig(t, `
declarations:
  - name: process
    terms:
      - name: pid
  - name: process
    terms:
      - name: pid
      - name: cmd
`)
	cfg, err := h.getConfig(getConfigInput{})
	if err != nil {
		t.Fatalf("get_config: %v", err)
	}
	got := 0
	for _, d := range cfg.Declarations {
		if d.Name == "process" {
			got++
		}
	}
	if got != 2 {
		t.Errorf("both process declarations should survive load; got %d", got)
	}
}

// Two sources with DIFFERENT File => distinct keys, must load.
func TestConfigKeyUniqueness_SourcesDifferentFile(t *testing.T) {
	dir := t.TempDir()
	// two distinct data files so both sources resolve
	mustWriteFile(t, filepath.Join(dir, "a.jsonl"), "{\"value\":{\"host\":\"h1\"}}\n")
	mustWriteFile(t, filepath.Join(dir, "b.jsonl"), "{\"value\":{\"host\":\"h2\"}}\n")
	schemaPath := filepath.Join(dir, "schema.yaml")
	mustWriteFile(t, schemaPath, `
sources:
  - file: a.jsonl
    mappings:
      - predicate: eventA
        args: ["value.host"]
  - file: b.jsonl
    mappings:
      - predicate: eventB
        args: ["value.host"]
`)
	_, closeFn, err := newMCPHandlers(dir, schemaPath, nil, "", 5_000_000_000)
	if err != nil {
		t.Fatalf("newMCPHandlers: two sources with different files wrongly rejected: %v", err)
	}
	t.Cleanup(func() { closeFn() })
}

// FIX 1 must NOT fire on a CRUD write's own serialized prospective (built by
// replaceX from an already-unique config). A normal put then delete must
// succeed through the re-parse in prepareSchemaWrite -> prepareSchema ->
// parseConfigFormat.
func TestConfigKeyUniqueness_NormalCrudRoundTripSurvivesReparse(t *testing.T) {
	dir := t.TempDir()
	writeSyntheticData(t, dir, 2)
	h, _ := newSchemaCrudTestHandlers(t, dir, "")

	put, err := h.putMatcher(putMatcherInput{Matcher: matcherWithContains("event", 2, "alpha")})
	if err != nil {
		t.Fatalf("put_matcher: %v (validateConfigKeyUniqueness fired spuriously on a unique prospective?)", err)
	}
	if put.IsStale {
		t.Fatalf("put_matcher: unexpected stale: %+v", put)
	}
	// A second, distinct-key matcher.
	if _, err := h.putMatcher(putMatcherInput{Matcher: jsonfacts.Matcher{Predicate: "event", Term: 2, CaseInsensitive: true, Contains: []string{"beta"}}}); err != nil {
		t.Fatalf("put_matcher (case-insensitive variant): %v", err)
	}
	// Editing the first (same key, rev bumped) must round-trip.
	if _, err := h.putMatcher(putMatcherInput{Matcher: matcherWithContains("event", 2, "gamma"), Revision: put.Revision}); err != nil {
		t.Fatalf("put_matcher edit: %v", err)
	}
	// Delete must round-trip through re-parse too.
	del, err := h.deleteMatcher(deleteMatcherInput{Predicate: "event", Term: 2, Revision: put.Revision + 1})
	if err != nil {
		t.Fatalf("delete_matcher: %v", err)
	}
	if del.IsStale {
		t.Fatalf("delete_matcher: unexpected stale: %+v", del)
	}
}

// FIX 1 COVERAGE — the fsnotify reload path (reloadSchema -> prepareSchema ->
// parseConfigFormat). A vim save that introduces a duplicate CRUD key must be
// rejected by the reload, leaving the last-good session intact rather than
// swapping in a corruptible config.
func TestConfigKeyUniqueness_ReloadRejectsDuplicateKey(t *testing.T) {
	wb, schemaPath, _ := watchTestWorkbench(t)

	// Grab the pre-edit schema text and derived state.
	wb.h.mu.Lock()
	goodText := wb.h.sess.schemaText
	wb.h.mu.Unlock()

	// vim save introduces two matchers sharing the CRUD key.
	mustWriteFile(t, schemaPath, `
matchers:
  - predicate: event
    term: 2
    contains: ["alpha"]
  - predicate: event
    term: 2
    contains: ["beta"]
`)
	wb.reloadFromDisk(true, false)

	wb.reloadMu.Lock()
	status := wb.lastReload
	wb.reloadMu.Unlock()
	if status.Err == "" {
		t.Fatal("reload accepted a duplicate-matcher-key schema; expected a rejection")
	}
	if !strings.Contains(status.Err, "duplicate") {
		t.Errorf("reload error = %q, want it to mention the duplicate", status.Err)
	}

	// Last-good session preserved: schemaText unchanged.
	wb.h.mu.Lock()
	nowText := wb.h.sess.schemaText
	wb.h.mu.Unlock()
	if nowText != goodText {
		t.Errorf("reload of a bad schema mutated the live session text:\n got: %s\nwant: %s", nowText, goodText)
	}
}

// ---------------------------------------------------------------------------
// whole-file staleness guard in commitSchemaWrite.
// ---------------------------------------------------------------------------

// OVER-REJECTION: rapid SEQUENTIAL agent writes must all succeed. Each write
// snapshots schemaText fresh (after the prior commit updated it via
// applySchemaLocked), so the guard never fires spuriously.
func TestSchemaWriteStaleness_SequentialWritesAllSucceed(t *testing.T) {
	dir := t.TempDir()
	writeSyntheticData(t, dir, 3)
	h, _ := newSchemaCrudTestHandlers(t, dir, "")

	// 3 back-to-back put_matcher writes at distinct keys.
	m1, err := h.putMatcher(putMatcherInput{Matcher: matcherWithContains("event", 0, "a")})
	if err != nil || m1.IsStale {
		t.Fatalf("put_matcher #1: err=%v stale=%v", err, m1.IsStale)
	}
	m2, err := h.putMatcher(putMatcherInput{Matcher: matcherWithContains("event", 1, "b")})
	if err != nil || m2.IsStale {
		t.Fatalf("put_matcher #2: err=%v stale=%v (whole-file guard fired spuriously on a sequential write?)", err, m2.IsStale)
	}
	m3, err := h.putMatcher(putMatcherInput{Matcher: matcherWithContains("event", 2, "c")})
	if err != nil || m3.IsStale {
		t.Fatalf("put_matcher #3: err=%v stale=%v", err, m3.IsStale)
	}

	// Interleave a put_declaration and a delete_matcher, still sequential.
	d1, err := h.putDeclaration(putDeclarationInput{Declaration: declWithUse("alpha", "u")})
	if err != nil || d1.IsStale {
		t.Fatalf("put_declaration: err=%v stale=%v", err, d1.IsStale)
	}
	del, err := h.deleteMatcher(deleteMatcherInput{Predicate: "event", Term: 0, Revision: m1.Revision})
	if err != nil || del.IsStale {
		t.Fatalf("delete_matcher: err=%v stale=%v (whole-file guard fired spuriously?)", err, del.IsStale)
	}

	// Verify final state: matchers at term 1,2 remain; term-0 removed; alpha present.
	cfg, err := h.getConfig(getConfigInput{})
	if err != nil {
		t.Fatalf("get_config: %v", err)
	}
	terms := map[int]bool{}
	for _, m := range cfg.Matchers {
		terms[m.Term] = true
	}
	if terms[0] {
		t.Errorf("term-0 matcher should have been deleted; matchers=%+v", cfg.Matchers)
	}
	if !terms[1] || !terms[2] {
		t.Errorf("term-1/term-2 matchers should survive sequential writes; matchers=%+v", cfg.Matchers)
	}
	foundAlpha := false
	for _, d := range cfg.Declarations {
		if d.Name == "alpha" {
			foundAlpha = true
		}
	}
	if !foundAlpha {
		t.Errorf("alpha declaration missing after sequential writes; decls=%+v", cfg.Declarations)
	}
}

// CORRECTNESS: the SAME-item race — a reload changing the SAME declaration the
// agent write also targets — must reject as stale (guard OR recheck), and the
// vim edit must survive.
func TestSchemaWriteStaleness_SameItemRaceRejectsStale(t *testing.T) {
	wb, schemaPath, _ := watchTestWorkbench(t)

	seed, err := wb.h.putDeclaration(putDeclarationInput{Declaration: declWithUse("alpha", "before vim")})
	if err != nil || seed.IsStale {
		t.Fatalf("seed alpha: err=%v stale=%v", err, seed.IsStale)
	}

	// Agent step 1 snapshot.
	wb.h.mu.Lock()
	snapText := wb.h.sess.schemaText
	agentCfg := wb.h.sess.authoringCfg
	agentCfg.Declarations = replaceDeclaration(agentCfg.Declarations, declarationKey{Name: "alpha", Arity: 0},
		declWithUse("alpha", "agent edit"))
	wb.h.mu.Unlock()

	// vim edits the SAME item (alpha).
	data, _ := os.ReadFile(schemaPath)
	vimText := strings.Replace(string(data), "before vim", "EDITED BY VIM", 1)
	if vimText == string(data) {
		t.Fatal("fixture: could not find alpha Use text to edit")
	}
	mustWriteFile(t, schemaPath, vimText)
	wb.reloadFromDisk(true, false)

	// Agent write against the stale snapshot.
	text, authoring, runtime, db, err := wb.h.prepareSchemaWrite(agentCfg)
	if err != nil {
		t.Fatalf("prepareSchemaWrite: %v", err)
	}
	wb.h.mu.Lock()
	_, ok, err := wb.h.commitSchemaWrite(snapText, text, authoring, runtime, db, func(newRevs *schemaRevisions) bool {
		key := declarationKey{Name: "alpha", Arity: 0}
		_, still := findDeclaration(wb.h.sess.authoringCfg.Declarations, key)
		if !still || seed.Revision != wb.h.schemaRev.declarations[key] {
			return false
		}
		newRevs.declarations[key] = nextRevision(true, wb.h.schemaRev.declarations[key], wb.h.schemaRev.deletedDeclarations[key])
		return true
	})
	wb.h.mu.Unlock()
	if err != nil {
		t.Fatalf("commitSchemaWrite: %v", err)
	}
	if ok {
		t.Fatal("same-item race: agent write should have been rejected as stale")
	}

	cfg, err := wb.h.getConfig(getConfigInput{})
	if err != nil {
		t.Fatalf("get_config: %v", err)
	}
	for _, d := range cfg.Declarations {
		if d.Name == "alpha" && d.Use != "EDITED BY VIM" {
			t.Errorf("alpha.Use = %q, want vim edit preserved", d.Use)
		}
	}
}

// CORRECTNESS: a DELETE racing a reload of a DIFFERENT item must also reject
// as stale via the whole-file guard, and the other item's edit must survive.
func TestSchemaWriteStaleness_DeleteRacingCrossItemReloadRejects(t *testing.T) {
	wb, schemaPath, _ := watchTestWorkbench(t)

	// Seed two declarations: alpha (the delete target) and keep (the vim edit target).
	if _, err := wb.h.putDeclaration(putDeclarationInput{Declaration: declWithUse("alpha", "doomed")}); err != nil {
		t.Fatalf("seed alpha: %v", err)
	}
	keepSeed, err := wb.h.putDeclaration(putDeclarationInput{Declaration: declWithUse("keep", "before vim")})
	if err != nil {
		t.Fatalf("seed keep: %v", err)
	}
	_ = keepSeed

	// Agent step 1: snapshot text and prepare a DELETE of alpha.
	wb.h.mu.Lock()
	snapText := wb.h.sess.schemaText
	alphaRev := wb.h.schemaRev.declarations[declarationKey{Name: "alpha", Arity: 0}]
	agentCfg := wb.h.sess.authoringCfg
	agentCfg.Declarations = removeDeclaration(agentCfg.Declarations, declarationKey{Name: "alpha", Arity: 0})
	wb.h.mu.Unlock()

	// vim edits a DIFFERENT item (keep) and reloads.
	data, _ := os.ReadFile(schemaPath)
	vimText := strings.Replace(string(data), "before vim", "EDITED BY VIM", 1)
	if vimText == string(data) {
		t.Fatal("fixture: could not find keep Use text")
	}
	mustWriteFile(t, schemaPath, vimText)
	wb.reloadFromDisk(true, false)

	// Agent delete commits against the stale snapshot -> must reject.
	text, authoring, runtime, db, err := wb.h.prepareSchemaWrite(agentCfg)
	if err != nil {
		t.Fatalf("prepareSchemaWrite (delete): %v", err)
	}
	wb.h.mu.Lock()
	_, ok, err := wb.h.commitSchemaWrite(snapText, text, authoring, runtime, db, func(newRevs *schemaRevisions) bool {
		key := declarationKey{Name: "alpha", Arity: 0}
		_, still := findDeclaration(wb.h.sess.authoringCfg.Declarations, key)
		if !still || alphaRev != wb.h.schemaRev.declarations[key] {
			return false
		}
		delete(newRevs.declarations, key)
		if newRevs.deletedDeclarations == nil {
			newRevs.deletedDeclarations = map[declarationKey]int{}
		}
		newRevs.deletedDeclarations[key] = alphaRev
		return true
	})
	wb.h.mu.Unlock()
	if err != nil {
		t.Fatalf("commitSchemaWrite (delete): %v", err)
	}
	if ok {
		t.Fatal("delete racing a cross-item reload should reject as stale")
	}

	// keep's vim edit must survive; alpha must still be present (delete rejected).
	cfg, err := wb.h.getConfig(getConfigInput{})
	if err != nil {
		t.Fatalf("get_config: %v", err)
	}
	var keepUse string
	foundAlpha := false
	for _, d := range cfg.Declarations {
		if d.Name == "keep" {
			keepUse = d.Use
		}
		if d.Name == "alpha" {
			foundAlpha = true
		}
	}
	if keepUse != "EDITED BY VIM" {
		t.Errorf("keep.Use = %q, want vim edit preserved (not clobbered by rejected delete)", keepUse)
	}
	if !foundAlpha {
		t.Error("alpha must remain (its delete was rejected as stale)")
	}
	onDisk, _ := os.ReadFile(schemaPath)
	if !strings.Contains(string(onDisk), "EDITED BY VIM") {
		t.Errorf("on-disk schema lost keep's vim edit:\n%s", onDisk)
	}
}
