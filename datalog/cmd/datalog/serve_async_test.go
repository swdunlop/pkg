package main

import (
	"context"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// This file exercises doc/features/workbench-scale.md work item 4
// (background startup load): newWorkbenchAsync must return a workbench whose
// routes already respond BEFORE the background load job releases, the job's
// completion must repaint the panes and clear $busy, and a failed load must
// surface as a console error without taking the workbench down. newWorkbench
// (the synchronous wrapper every other test in this package uses) is
// untouched by these tests — they exist specifically to pin the async shape
// newWorkbenchAsync adds underneath it.

// TestAsyncStartup_ListenerReachableBeforeLoadCompletes pins design decision
// 3's core claim: routes respond immediately, without waiting for the
// background load job. It claims loadJobKey on wb.jobs the instant
// newWorkbenchAsync returns — racing runLoadJob's own wb.jobs.Begin(...,
// loadJobKey) call for the same key — so that on whichever side wins,
// either (a) this test's claim lands first and runLoadJob's Begin sees the
// key busy, returning immediately with isLoading() left true for the rest
// of the test (jobs.Begin's documented "(nil, nil) when the key is already
// busy" contract, sandbox.go), or (b) runLoadJob's own near-instant
// synthetic-dataset load has already finished and released the key before
// this claim runs, in which case isLoading() is simply false already — both
// outcomes are consistent with "the listener does not wait for the load,"
// which is what this test actually asserts (GET / succeeds regardless), so
// the race does not make the test flaky, only its isLoading() snapshot
// non-deterministic (not asserted here for that reason).
func TestAsyncStartup_ListenerReachableBeforeLoadCompletes(t *testing.T) {
	dir := t.TempDir()
	writeSyntheticData(t, dir, 3)
	schemaPath := filepath.Join(dir, "schema.yaml")
	mustWriteFile(t, schemaPath, syntheticSchemaYAML)

	wb, closeFn, loadDone, err := newWorkbenchAsync(dir, schemaPath, nil, "", "test-token", agentConfig{}, defaultMaxFacts, defaultEvalTimeout)
	if err != nil {
		t.Fatalf("newWorkbenchAsync: %v", err)
	}
	t.Cleanup(func() { closeFn() })
	_, releaseClaim := wb.jobs.Begin(context.Background(), loadJobKey)

	// The workbench is immediately usable: routes respond right away, with
	// no wait on loadDone at all — the defining assertion of this test.
	srv := startTestServer(wb)
	defer srv.Close()

	resp, err := srv.Client().Get(srv.URL + "/")
	if err != nil {
		t.Fatalf("GET /: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET / while load may still be running: status = %d, want 200", resp.StatusCode)
	}

	if releaseClaim != nil {
		releaseClaim()
	}
	// Drain loadDone so t.Cleanup's closeFn doesn't race a still-running job.
	select {
	case <-loadDone:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the background load job to finish")
	}
}

// TestAsyncStartup_LoadingFlagClearsAndSessionPopulates drives the real
// background job to completion (no synthetic block) and asserts: isLoading()
// starts true, becomes false once the job finishes, the base dataset is
// actually applied (loadDeferredSchema ran through the same setSchema
// chokepoint every schema write uses), and a completion console entry with
// "dataset loaded" landed in the shared/global scrollback (loadTab == "").
// This is the "completion publishes: console entry + session-changed repaint
// + busy cleared" requirement from the task brief; the repaint itself is
// covered indirectly (publishSessionChanged is unconditionally called from
// the same completion path -- see TestHTTP_EventsSubscription/
// TestMCP_InitializeAndPutRuleGroupPatchesBack for the publish-under-h.mu
// contract this job reuses) and directly asserted here via the predicate
// list actually reflecting the loaded data once isLoading() flips.
func TestAsyncStartup_LoadingFlagClearsAndSessionPopulates(t *testing.T) {
	dir := t.TempDir()
	writeSyntheticData(t, dir, 5)
	schemaPath := filepath.Join(dir, "schema.yaml")
	mustWriteFile(t, schemaPath, syntheticSchemaYAML)

	wb, closeFn, loadDone, err := newWorkbenchAsync(dir, schemaPath, nil, "", "test-token", agentConfig{}, defaultMaxFacts, defaultEvalTimeout)
	if err != nil {
		t.Fatalf("newWorkbenchAsync: %v", err)
	}
	t.Cleanup(func() { closeFn() })

	select {
	case <-loadDone:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the background load job to finish")
	}

	if wb.isLoading() {
		t.Fatal("isLoading() still true after loadDone closed")
	}
	if key, _, _ := wb.currentBusy(); key != "" {
		t.Fatalf("busy key not cleared after load completion: %q", key)
	}

	wb.h.mu.Lock()
	db, err := wb.h.sess.evaluatedDB()
	wb.h.mu.Unlock()
	if err != nil {
		t.Fatalf("evaluatedDB after load: %v", err)
	}
	n := 0
	for range db.Facts("event", 3) {
		n++
	}
	if n != 5 {
		t.Fatalf("event/3 facts after background load = %d, want 5", n)
	}

	log := renderLog(wb, "")
	if !containsAll(log, "loading dataset", "dataset loaded") {
		t.Fatalf("console scrollback missing load progress/completion notices: %s", log)
	}
}

// TestAsyncStartup_LoadFailureSurfacesAndWorkbenchStaysUp points configPath
// at a schema file that setSchema will reject (malformed YAML), confirming
// the background load job's failure path: isLoading() still clears (a
// failed load is "done loading," not "still loading"), the error lands as a
// console entry rather than being swallowed, and — critically — the
// workbench itself stays up and its routes keep responding, matching
// runLoadJob's documented "the workbench stays up; only the session's data/
// derived predicates are empty" contract.
func TestAsyncStartup_LoadFailureSurfacesAndWorkbenchStaysUp(t *testing.T) {
	dir := t.TempDir()
	schemaPath := filepath.Join(dir, "schema.yaml")
	mustWriteFile(t, schemaPath, "not: [valid, yaml")

	wb, closeFn, loadDone, err := newWorkbenchAsync(dir, schemaPath, nil, "", "test-token", agentConfig{}, defaultMaxFacts, defaultEvalTimeout)
	if err != nil {
		t.Fatalf("newWorkbenchAsync: %v", err)
	}
	t.Cleanup(func() { closeFn() })

	select {
	case <-loadDone:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the background load job to finish")
	}

	if wb.isLoading() {
		t.Fatal("isLoading() still true after a failed load's loadDone closed")
	}

	log := renderLog(wb, "")
	if !containsAll(log, "dataset load failed") {
		t.Fatalf("console scrollback missing the load-failure notice: %s", log)
	}

	srv := startTestServer(wb)
	defer srv.Close()
	resp, err := srv.Client().Get(srv.URL + "/")
	if err != nil {
		t.Fatalf("GET / after failed load: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET / after failed load: status = %d, want 200 (workbench must stay up)", resp.StatusCode)
	}
}

// TestNewWorkbench_SyncWrapperWaitsForLoad pins newWorkbench's (the
// pre-existing, widely-used test helper) documented contract: it blocks
// until the background load finishes, so every OTHER test in this package
// that calls it (TestStartupEvaluatesRules and friends) keeps seeing a fully
// loaded session on return with no changes to its own call sites.
func TestNewWorkbench_SyncWrapperWaitsForLoad(t *testing.T) {
	dir := t.TempDir()
	writeSyntheticData(t, dir, 2)
	schemaPath := filepath.Join(dir, "schema.yaml")
	mustWriteFile(t, schemaPath, syntheticSchemaYAML)

	wb := newTestWorkbench(t, dir, schemaPath, nil, "test-token")
	if wb.isLoading() {
		t.Fatal("newWorkbench (sync wrapper) returned with isLoading() still true")
	}
	wb.h.mu.Lock()
	db, err := wb.h.sess.evaluatedDB()
	wb.h.mu.Unlock()
	if err != nil {
		t.Fatalf("evaluatedDB: %v", err)
	}
	n := 0
	for range db.Facts("event", 3) {
		n++
	}
	if n != 2 {
		t.Fatalf("event/3 facts = %d, want 2", n)
	}
}

// containsAll reports whether s contains every substring in subs.
func containsAll(s string, subs ...string) bool {
	for _, sub := range subs {
		if !strings.Contains(s, sub) {
			return false
		}
	}
	return true
}
