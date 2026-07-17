package main

import (
	"context"
	"fmt"
	"io"
	"iter"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"swdunlop.dev/pkg/datalog/seminaive"
)

// -- test helpers ---------------------------------------------------------

// newTestHandlers builds mcpHandlers rooted at dir (a real directory, so
// os.Root-based confinement applies exactly as it does for `datalog mcp`),
// with no preloaded schema or rules.
func newTestHandlers(t *testing.T, dir string) (*mcpHandlers, func()) {
	t.Helper()
	root, err := openDataRoot(dir)
	if err != nil {
		t.Fatalf("openDataRoot: %v", err)
	}
	h := &mcpHandlers{
		sess:    &session{},
		fsys:    root.FS(),
		confine: root.Confine,
		timeout: 5 * time.Second,
	}
	return h, func() { root.Close() }
}

// newMordorHandlers builds mcpHandlers rooted at the mordor example's zip
// dataset, mirroring newMCPHandlers' zip branch, for the golden loop test.
func newMordorHandlers(t *testing.T) *mcpHandlers {
	t.Helper()
	zipPath := filepath.Join("..", "..", "examples", "mordor", "covenant_copy_smb.zip")
	if _, err := os.Stat(zipPath); err != nil {
		t.Fatalf("mordor zip not found at %s: %v", zipPath, err)
	}
	h, closeFn, err := newMCPHandlers(zipPath, "", nil, 5*time.Second)
	if err != nil {
		t.Fatalf("newMCPHandlers: %v", err)
	}
	t.Cleanup(func() { closeFn() })
	return h
}

// -- golden loop: examples/mordor -----------------------------------------

func TestMordorGoldenLoop(t *testing.T) {
	h := newMordorHandlers(t)

	// sample_input with no file: lists the one JSON file in the zip.
	list, err := h.sampleInput(sampleInputInput{})
	if err != nil {
		t.Fatalf("sample_input (list): %v", err)
	}
	if len(list.Files) != 1 {
		t.Fatalf("sample_input (list): got %d files, want 1: %v", len(list.Files), list.Files)
	}
	dataFile := list.Files[0]
	if !strings.Contains(dataFile, "covenant_copy_smb") {
		t.Fatalf("sample_input (list): unexpected file name %q", dataFile)
	}

	// sample_input with the file: returns lines.
	sample, err := h.sampleInput(sampleInputInput{File: dataFile, Limit: 3})
	if err != nil {
		t.Fatalf("sample_input (file): %v", err)
	}
	if len(sample.Lines) != 3 {
		t.Fatalf("sample_input (file): got %d lines, want 3", len(sample.Lines))
	}
	if sample.TotalLines != 506 {
		t.Fatalf("sample_input (file): got %d total lines, want 506", sample.TotalLines)
	}

	// set_schema with mordor.yaml: assert several exact per-predicate counts
	// (mirroring examples/mordor/mordor_test.go's TestLoadFacts expectations).
	schemaData, err := os.ReadFile(filepath.Join("..", "..", "examples", "mordor", "mordor.yaml"))
	if err != nil {
		t.Fatalf("reading mordor.yaml: %v", err)
	}
	schemaOut, err := h.setSchema(setSchemaInput{Schema: string(schemaData), Format: "yaml"})
	if err != nil {
		t.Fatalf("set_schema: %v", err)
	}
	wantCounts := map[string]int{
		"net_conn":     3,
		"file_create":  5,
		"proc_access":  10,
		"reg_key":      123,
		"reg_value":    59,
		"logon":        2,
		"special_priv": 2,
		"share_access": 2,
		"share_file":   3,
	}
	gotCounts := map[string]int{}
	for _, p := range schemaOut.Predicates {
		gotCounts[p.Name] = p.Facts
	}
	for name, want := range wantCounts {
		if got := gotCounts[name]; got != want {
			t.Errorf("set_schema: predicate %s: got %d facts, want %d", name, got, want)
		}
	}

	// set_rules with rules.dl: assert the 9 head predicates.
	rulesData, err := os.ReadFile(filepath.Join("..", "..", "examples", "mordor", "rules.dl"))
	if err != nil {
		t.Fatalf("reading rules.dl: %v", err)
	}
	rulesOut, err := h.setRules(setRulesInput{Source: string(rulesData)})
	if err != nil {
		t.Fatalf("set_rules: %v", err)
	}
	wantPreds := []string{
		"admin_share", "confirmed_drop", "elevated_lateral_movement",
		"elevated_logon", "exe_drop", "exe_on_disk", "lateral_movement",
		"remote_logon", "smb_conn",
	}
	if len(rulesOut.Predicates) != len(wantPreds) {
		t.Fatalf("set_rules: got %d predicates, want %d: %v", len(rulesOut.Predicates), len(wantPreds), rulesOut.Predicates)
	}
	for i, want := range wantPreds {
		if rulesOut.Predicates[i] != want {
			t.Errorf("set_rules: predicate[%d] = %q, want %q (full list %v)", i, rulesOut.Predicates[i], want, rulesOut.Predicates)
		}
	}

	// query lateral_movement: assert the exact row and non-empty stats.
	queryOut, err := h.query(context.Background(), queryInput{Query: `lateral_movement(User, Src, Target, Path)?`})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if queryOut.Total != 1 {
		t.Fatalf("query lateral_movement: got %d rows, want 1", queryOut.Total)
	}
	wantVars := []string{"User", "Src", "Target", "Path"}
	if len(queryOut.Vars) != len(wantVars) {
		t.Fatalf("query lateral_movement: got vars %v, want %v", queryOut.Vars, wantVars)
	}
	for i, v := range wantVars {
		if queryOut.Vars[i] != v {
			t.Fatalf("query lateral_movement: vars[%d] = %q, want %q", i, queryOut.Vars[i], v)
		}
	}
	row := queryOut.Rows[0]
	wantRow := []any{"pgustavo", "172.18.39.5", "WORKSTATION6.theshire.local", "ProgramData\\GruntHTTP.exe"}
	for i, want := range wantRow {
		if row[i] != want {
			t.Errorf("query lateral_movement: row[%d] = %v, want %v", i, row[i], want)
		}
	}
	if len(queryOut.Stats) == 0 {
		t.Error("query lateral_movement: expected non-empty stats")
	}
}

// -- synthetic dataset for per-handler tests -------------------------------

const syntheticSchemaYAML = `
sources:
  - file: events.jsonl
    mappings:
      - predicate: event
        args: ["value.host", "value.pid", "value.cmd"]
declarations:
  - name: event
    use: "a process execution event"
`

func writeSyntheticData(t *testing.T, dir string, n int) {
	t.Helper()
	var sb strings.Builder
	for i := 0; i < n; i++ {
		sb.WriteString(`{"host": "h` + itoa(i) + `", "pid": ` + itoa(i) + `, "cmd": "cmd` + itoa(i) + `"}` + "\n")
	}
	mustWriteFile(t, filepath.Join(dir, "events.jsonl"), sb.String())
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

// -- set_schema -------------------------------------------------------------

func TestSetSchema_MalformedYAML(t *testing.T) {
	dir := t.TempDir()
	h, done := newTestHandlers(t, dir)
	defer done()

	_, err := h.setSchema(setSchemaInput{Schema: "sources: [this is not: valid: yaml: at: all", Format: "yaml"})
	if err == nil {
		t.Fatal("set_schema: expected error for malformed YAML, got none")
	}
}

func TestSetSchema_EscapingSourceFileRejected(t *testing.T) {
	dir := t.TempDir()
	writeSyntheticData(t, dir, 1)
	// A sibling file outside the data dir.
	outsideDir := t.TempDir()
	mustWriteFile(t, filepath.Join(outsideDir, "secret.jsonl"), `{"host":"x"}`+"\n")

	h, done := newTestHandlers(t, dir)
	defer done()

	badSchema := `
sources:
  - file: ../` + filepath.Base(outsideDir) + `/secret.jsonl
    mappings:
      - predicate: event
        args: ["value.host"]
`
	_, err := h.setSchema(setSchemaInput{Schema: badSchema, Format: "yaml"})
	if err == nil {
		t.Fatal("set_schema: expected error for source file escaping data dir, got none")
	}

	// The session must be left untouched: a subsequent list_predicates call
	// should see no schema-derived predicates.
	out, err := h.listPredicates(listPredicatesInput{})
	if err != nil {
		t.Fatalf("list_predicates after rejected set_schema: %v", err)
	}
	for _, p := range out.Predicates {
		if p.Name == "event" {
			t.Errorf("list_predicates: found %q after set_schema should have been rejected", p.Name)
		}
	}
}

// newMCPHandlers loads startup rule files through session.loadProgram,
// which — unlike the REPL's own loadProgram (repl.go) — has nowhere to run
// or print embedded '?' queries: mcp mode's stdout is the JSON-RPC channel,
// and serve mode has no console this early in startup. Silently dropping
// them was the bug; newMCPHandlers must now warn on stderr, naming the file
// and how many queries it ignored.
func TestNewMCPHandlers_WarnsOnEmbeddedQueriesInRuleFiles(t *testing.T) {
	dir := t.TempDir()
	rulesPath := filepath.Join(dir, "rules.dl")
	mustWriteFile(t, rulesPath, "foo(1).\nfoo(2).\nbar(X) :- foo(X).\nfoo(X)?\nbar(X)?\n")

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	origStderr := os.Stderr
	os.Stderr = w

	h, closeFn, err := newMCPHandlers(dir, "", []string{rulesPath}, 5*time.Second)

	os.Stderr = origStderr
	w.Close()
	var buf strings.Builder
	if _, cerr := io.Copy(&buf, r); cerr != nil {
		t.Fatalf("reading captured stderr: %v", cerr)
	}

	if err != nil {
		t.Fatalf("newMCPHandlers: %v", err)
	}
	defer closeFn()

	got := buf.String()
	if !strings.Contains(got, rulesPath) {
		t.Errorf("warning does not name the rules file %s: %q", rulesPath, got)
	}
	if !strings.Contains(got, "2") {
		t.Errorf("warning does not report the 2 ignored queries: %q", got)
	}

	// The rules themselves still loaded normally (only the queries were
	// dropped) — bar/1 must be a known predicate.
	out, err := h.listPredicates(listPredicatesInput{})
	if err != nil {
		t.Fatalf("list_predicates: %v", err)
	}
	found := false
	for _, p := range out.Predicates {
		if p.Name == "bar" {
			found = true
		}
	}
	if !found {
		t.Error("bar/1 rule from the rules file did not load")
	}
}

func TestSetSchema_Valid(t *testing.T) {
	dir := t.TempDir()
	writeSyntheticData(t, dir, 5)
	h, done := newTestHandlers(t, dir)
	defer done()

	out, err := h.setSchema(setSchemaInput{Schema: syntheticSchemaYAML, Format: "yaml"})
	if err != nil {
		t.Fatalf("set_schema: %v", err)
	}
	if len(out.Predicates) != 1 || out.Predicates[0].Name != "event" || out.Predicates[0].Facts != 5 {
		t.Fatalf("set_schema: got %+v, want one event/3 predicate with 5 facts", out.Predicates)
	}
}

// -- set_rules ----------------------------------------------------------

func TestSetRules_RejectsEmbeddedQuery(t *testing.T) {
	h, done := newTestHandlers(t, t.TempDir())
	defer done()

	_, err := h.setRules(setRulesInput{Source: `foo(X) :- event(_, X, _).` + "\n" + `foo(X)?` + "\n"})
	if err == nil {
		t.Fatal("set_rules: expected error for embedded query, got none")
	}
}

func TestSetRules_TrialCompileError(t *testing.T) {
	h, done := newTestHandlers(t, t.TempDir())
	defer done()

	// Unstratifiable negation cycle: a depends negatively on itself.
	_, err := h.setRules(setRulesInput{Source: `a(X) :- event(_, X, _), not a(X).` + "\n"})
	if err == nil {
		t.Fatal("set_rules: expected trial-compile error for unstratifiable negation, got none")
	}
	if !strings.Contains(err.Error(), "unstratifiable") {
		t.Errorf("set_rules: error %q does not mention unstratifiable", err.Error())
	}
}

// TestSetRules_SurfacesDetachedDocWarning pins the MCP set_rules data-loss
// tell: a program with a detached '%%' doc block (block, blank line, rule)
// parses and compiles cleanly, but the detached doc is silently dropped from
// the stored document. Unlike the workbench, a model driving set_rules over
// MCP has no Check pane, so set_rules must report ruleset.Warnings in its
// output or the dropped doc vanishes with no signal at all. Mirrors
// TestHTTP_RulesCheckSurfacesDetachedDocWarning (rules_editor_test.go) for the
// MCP surface, and setSchemaOutput.Warnings for the sibling tool.
func TestSetRules_SurfacesDetachedDocWarning(t *testing.T) {
	h, done := newTestHandlers(t, t.TempDir())
	defer done()

	src := "%% this doc is detached\n\nfoo(X) :- event(_, X, _).\n"
	out, err := h.setRules(setRulesInput{Source: src})
	if err != nil {
		t.Fatalf("setRules: %v", err)
	}
	joined := strings.Join(out.Warnings, "\n")
	if !strings.Contains(joined, "detached") {
		t.Fatalf("set_rules dropped detached-doc warning; Warnings=%v", out.Warnings)
	}

	// A clean program with attached docs must NOT warn.
	clean, err := h.setRules(setRulesInput{Source: "%% attached doc\nbar(X) :- event(_, X, _).\n"})
	if err != nil {
		t.Fatalf("setRules (clean): %v", err)
	}
	if len(clean.Warnings) != 0 {
		t.Fatalf("clean set_rules should not warn; Warnings=%v", clean.Warnings)
	}
}

func TestSetRules_WholeDocumentReplacement(t *testing.T) {
	dir := t.TempDir()
	writeSyntheticData(t, dir, 3)
	h, done := newTestHandlers(t, dir)
	defer done()

	if _, err := h.setSchema(setSchemaInput{Schema: syntheticSchemaYAML, Format: "yaml"}); err != nil {
		t.Fatalf("set_schema: %v", err)
	}

	if _, err := h.setRules(setRulesInput{Source: `foo(X) :- event(_, X, _).` + "\n"}); err != nil {
		t.Fatalf("set_rules (first): %v", err)
	}
	out, err := h.listPredicates(listPredicatesInput{})
	if err != nil {
		t.Fatalf("list_predicates: %v", err)
	}
	if !containsPredicate(out.Predicates, "foo") {
		t.Fatalf("list_predicates: expected %q after first set_rules, got %+v", "foo", out.Predicates)
	}

	// Second call replaces, not appends: "foo" should disappear, "bar" appear.
	if _, err := h.setRules(setRulesInput{Source: `bar(X) :- event(_, X, _).` + "\n"}); err != nil {
		t.Fatalf("set_rules (second): %v", err)
	}
	out, err = h.listPredicates(listPredicatesInput{})
	if err != nil {
		t.Fatalf("list_predicates: %v", err)
	}
	if containsPredicate(out.Predicates, "foo") {
		t.Errorf("list_predicates: %q still present after replacing set_rules, want gone", "foo")
	}
	if !containsPredicate(out.Predicates, "bar") {
		t.Fatalf("list_predicates: expected %q after second set_rules, got %+v", "bar", out.Predicates)
	}
}

func containsPredicate(preds []predicateInfo, name string) bool {
	for _, p := range preds {
		if p.Name == name {
			return true
		}
	}
	return false
}

// -- query ----------------------------------------------------------------

func TestQuery_DefaultLimitAndTruncation(t *testing.T) {
	dir := t.TempDir()
	writeSyntheticData(t, dir, 150)
	h, done := newTestHandlers(t, dir)
	defer done()

	if _, err := h.setSchema(setSchemaInput{Schema: syntheticSchemaYAML, Format: "yaml"}); err != nil {
		t.Fatalf("set_schema: %v", err)
	}

	out, err := h.query(context.Background(), queryInput{Query: `event(Host, Pid, Cmd)?`})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if out.Total != 150 {
		t.Fatalf("query: total = %d, want 150", out.Total)
	}
	if len(out.Rows) != defaultQueryLimit {
		t.Fatalf("query: serialized %d rows, want default limit %d", len(out.Rows), defaultQueryLimit)
	}
	if !out.Truncated {
		t.Error("query: expected truncated=true when total exceeds default limit")
	}
}

func TestQuery_RejectsAnonymousVariables(t *testing.T) {
	dir := t.TempDir()
	writeSyntheticData(t, dir, 5)
	h, done := newTestHandlers(t, dir)
	defer done()

	if _, err := h.setSchema(setSchemaInput{Schema: syntheticSchemaYAML, Format: "yaml"}); err != nil {
		t.Fatalf("set_schema: %v", err)
	}

	_, err := h.query(context.Background(), queryInput{Query: `event(?, Pid, ?)?`})
	if err == nil {
		t.Fatalf("query: expected anonymous-variable rejection, got success")
	}
	// The error must teach the fix using the model's own query: named
	// variables substituted for the '?'s, the named Pid left alone.
	for _, want := range []string{"anonymous variable", "_Ignored", "event(A, Pid, B)?"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("query error missing %q: %v", want, err)
		}
	}

	// Underscore-prefixed variables remain the accepted don't-care form.
	out, err := h.query(context.Background(), queryInput{Query: `event(_Host, Pid, _Cmd)?`})
	if err != nil {
		t.Fatalf("query with underscore vars: %v", err)
	}
	if out.Total != 5 {
		t.Fatalf("query: total = %d, want 5", out.Total)
	}

	// Inside a negated atom, anonymous variables are exempt from the
	// rejection — there they are the only safe don't-care form (the engine
	// requires negated-atom variables to be positively bound or anonymous).
	out, err = h.query(context.Background(), queryInput{Query: `event(Host, Pid, Cmd), not event(Host, 999999, ?)?`})
	if err != nil {
		t.Fatalf("query with anonymous var in negated atom: %v", err)
	}
	if out.Total != 5 {
		t.Fatalf("negated query: total = %d, want 5", out.Total)
	}
	// And the anonymous variable must not leak into the result columns.
	for _, v := range out.Vars {
		if strings.HasPrefix(v, "?") {
			t.Fatalf("anonymous variable leaked into result columns: %v", out.Vars)
		}
	}
}

func TestQuery_HardCap(t *testing.T) {
	dir := t.TempDir()
	writeSyntheticData(t, dir, 1500)
	h, done := newTestHandlers(t, dir)
	defer done()

	if _, err := h.setSchema(setSchemaInput{Schema: syntheticSchemaYAML, Format: "yaml"}); err != nil {
		t.Fatalf("set_schema: %v", err)
	}

	out, err := h.query(context.Background(), queryInput{Query: `event(Host, Pid, Cmd)?`, Limit: 100000})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if out.Total != 1500 {
		t.Fatalf("query: total = %d, want 1500 (evaluation runs to completion regardless of limit)", out.Total)
	}
	if len(out.Rows) != maxQueryLimit {
		t.Fatalf("query: serialized %d rows, want hard cap %d", len(out.Rows), maxQueryLimit)
	}
	if !out.Truncated {
		t.Error("query: expected truncated=true when hard cap applies")
	}
}

func TestQuery_UnderLimitNotTruncated(t *testing.T) {
	dir := t.TempDir()
	writeSyntheticData(t, dir, 5)
	h, done := newTestHandlers(t, dir)
	defer done()

	if _, err := h.setSchema(setSchemaInput{Schema: syntheticSchemaYAML, Format: "yaml"}); err != nil {
		t.Fatalf("set_schema: %v", err)
	}

	out, err := h.query(context.Background(), queryInput{Query: `event(Host, Pid, Cmd)?`})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if out.Truncated {
		t.Error("query: expected truncated=false when total is under the limit")
	}
	if out.Total != 5 || len(out.Rows) != 5 {
		t.Fatalf("query: got total=%d rows=%d, want 5/5", out.Total, len(out.Rows))
	}
}

// TestQuery_CancelledContext verifies query surfaces a clean error when its
// context is already cancelled, rather than hanging or panicking. Engineering
// a genuinely slow query deterministically (without depending on timing or
// machine speed) is fragile, so this test exercises the same code path —
// runQuery's per-stratum/per-iteration ctx.Err() checks (seminaive/
// transformer.go, seminaive/eval.go) — via an already-cancelled context
// rather than a live timeout race. This does not independently verify the
// configurable -timeout flag itself wires through (it does, mechanically:
// query wraps ctx with context.WithTimeout(ctx, h.timeout) before calling
// runQuery), only that cancellation is honored and produces a clean error.
func TestQuery_CancelledContext(t *testing.T) {
	dir := t.TempDir()
	writeSyntheticData(t, dir, 5)
	h, done := newTestHandlers(t, dir)
	defer done()

	if _, err := h.setSchema(setSchemaInput{Schema: syntheticSchemaYAML, Format: "yaml"}); err != nil {
		t.Fatalf("set_schema: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := h.query(ctx, queryInput{Query: `event(Host, Pid, Cmd)?`})
	if err == nil {
		t.Fatal("query: expected error for already-cancelled context, got none")
	}
}

// TestQuery_DoesNotHoldLockDuringTransform pins the snapshot narrowing:
// query holds h.mu only while snapshotting session state, so a Transform
// that runs for a long time must leave the lock free for other tools and
// the workbench panes. A blocking external predicate parks a query mid-
// Transform deterministically; the test then acquires h.mu (which would
// deadlock under the old whole-call locking) before releasing the query.
func TestQuery_DoesNotHoldLockDuringTransform(t *testing.T) {
	h, done := newTestHandlers(t, t.TempDir())
	defer done()

	inTransform := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	h.sess.engineOpts = []seminaive.Option{
		seminaive.WithExternal("slow", 1, func(ctx context.Context, _ seminaive.Bindings) iter.Seq[[]any] {
			return func(yield func([]any) bool) {
				once.Do(func() { close(inTransform) })
				select {
				case <-release:
				case <-ctx.Done():
				}
				yield([]any{"done"})
			}
		}),
	}
	if _, err := h.setRules(setRulesInput{Source: `r(X) :- slow(X).`}); err != nil {
		t.Fatalf("set_rules: %v", err)
	}

	queryDone := make(chan error, 1)
	go func() {
		out, err := h.query(context.Background(), queryInput{Query: `r(X)?`})
		if err == nil && out.Total != 1 {
			err = fmt.Errorf("query: got %d rows, want 1", out.Total)
		}
		queryDone <- err
	}()

	<-inTransform // the query is now inside Transform

	lockFree := make(chan struct{})
	go func() {
		h.mu.Lock()
		h.mu.Unlock() //nolint:staticcheck // probe: acquire proves the lock is free
		close(lockFree)
	}()
	select {
	case <-lockFree:
	case <-time.After(5 * time.Second):
		t.Fatal("h.mu is held while the query's Transform runs: the snapshot narrowing has regressed")
	}

	close(release)
	if err := <-queryDone; err != nil {
		t.Fatalf("query: %v", err)
	}
}

// -- fact cap: mid-evaluation WithFactLimit enforcement ---------------------
//
// These three tests build handlers through newMCPHandlers (not
// newTestHandlers, which builds a bare &session{} with no engineOpts) so the
// session carries newMCPHandlers' default engineOpts —
// []seminaive.Option{seminaive.WithFactLimit(factCap)} — exactly like
// `datalog mcp`/`datalog serve` do in production. That default is what
// closes the hole below; a handler built the newTestHandlers way would not
// exercise it at all.

// TestQuery_ZeroRulesCrossProductExceedsFactCap is the adversarial
// validation's exact repro: a session with NO rules skips runQuery's base-
// ruleset stage entirely (session.go: "nothing to derive: the EDB snapshot
// IS the whole fixpoint"), so the only Transform that ever runs is the
// synthetic _q_ stage built from the query body itself — and, before every
// session.engineOpts carried WithFactLimit(factCap) by default, nothing
// capped that stage at all. A 3-way self-join over 15 facts derived
// 15^3 = 3375 _q_ rows with no error. It must now halt with a "rule too
// broad" error instead.
func TestQuery_ZeroRulesCrossProductExceedsFactCap(t *testing.T) {
	dir := t.TempDir()
	writeSyntheticData(t, dir, 15)
	h, closeFn, err := newMCPHandlers(dir, "", nil, 5*time.Second)
	if err != nil {
		t.Fatalf("newMCPHandlers: %v", err)
	}
	defer closeFn()

	if _, err := h.setSchema(setSchemaInput{Schema: syntheticSchemaYAML, Format: "yaml"}); err != nil {
		t.Fatalf("set_schema: %v", err)
	}

	out, err := h.query(context.Background(), queryInput{
		Query: `event(A, B, C), event(D, E, F), event(G, H, I)?`,
	})
	if err == nil {
		t.Fatalf("query: expected a fact-cap error, got %d rows (want the 15^3=3375 cross product rejected)", out.Total)
	}
	if !strings.Contains(err.Error(), "rule too broad") {
		t.Errorf("query: error %q does not use the familiar \"rule too broad\" wording", err.Error())
	}
}

// TestQuery_RulesBaseStageExceedsFactCap covers the OTHER query stage: a
// session ruleset (not the query body) that blows the cap. Unlike the
// zero-rules case above, this exercises runQuery's base-ruleset stage
// (session.go's "default:" branch), which computes the ruleset's fixpoint
// before the query's own synthetic _q_ rule ever runs against it — this is
// the stage checkQueryFactCap used to guard post-hoc; WithFactLimit now
// halts it mid-Transform instead.
func TestQuery_RulesBaseStageExceedsFactCap(t *testing.T) {
	dir := t.TempDir()
	writeSyntheticData(t, dir, 40) // cross product: 40*40 = 1600 > factCap (1000)
	h, closeFn, err := newMCPHandlers(dir, "", nil, 5*time.Second)
	if err != nil {
		t.Fatalf("newMCPHandlers: %v", err)
	}
	defer closeFn()

	if _, err := h.setSchema(setSchemaInput{Schema: syntheticSchemaYAML, Format: "yaml"}); err != nil {
		t.Fatalf("set_schema: %v", err)
	}
	if _, err := h.setRules(setRulesInput{Source: "pair(A,B) :- event(A,_,_), event(B,_,_).\n"}); err != nil {
		t.Fatalf("set_rules: %v", err)
	}

	out, err := h.query(context.Background(), queryInput{Query: `pair(X, Y)?`})
	if err == nil {
		t.Fatalf("query: expected a fact-cap error from the base ruleset stage, got %d rows", out.Total)
	}
	if !strings.Contains(err.Error(), "rule too broad") {
		t.Errorf("query: error %q does not use the familiar \"rule too broad\" wording", err.Error())
	}
}

// TestQuery_UnderCapStillSucceeds is the control: a query comfortably under
// factCap must still return its full, correct result through a handler that
// carries the same default WithFactLimit(factCap) the two tests above rely
// on to fail — proving the cap's threshold, not just its existence.
func TestQuery_UnderCapStillSucceeds(t *testing.T) {
	dir := t.TempDir()
	writeSyntheticData(t, dir, 5)
	h, closeFn, err := newMCPHandlers(dir, "", nil, 5*time.Second)
	if err != nil {
		t.Fatalf("newMCPHandlers: %v", err)
	}
	defer closeFn()

	if _, err := h.setSchema(setSchemaInput{Schema: syntheticSchemaYAML, Format: "yaml"}); err != nil {
		t.Fatalf("set_schema: %v", err)
	}

	// 5*5 = 25 rows, well under factCap (1000).
	out, err := h.query(context.Background(), queryInput{Query: `event(A, B, C), event(D, E, F)?`})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if out.Total != 25 {
		t.Fatalf("query: total = %d, want 25", out.Total)
	}
}

// -- derived fixpoint cache: query write-back -------------------------------
//
// These cover the query handler's write-back of a cold-path base fixpoint
// into session.derivedDB (mcp.go's cacheDerivedQuery, called from query()),
// mirroring rules_editor.go's Run write-back. Before this write-back existed,
// snap.runQuery's cold path computed the base fixpoint into a querySnapshot
// value that was simply discarded after the call, so two consecutive queries
// with no mutation between them each recomputed the whole base ruleset from
// scratch — TestQuery_PopulatesAndReusesDerivedCache is written to fail
// without the fix (session.derivedDB stays nil after a successful
// rules-based query).

// TestQuery_PopulatesAndReusesDerivedCache asserts a rules-based MCP query
// populates session.derivedDB (with gen left unchanged — the write-back
// never bumps gen, only mutators do), and that a second identical query
// reuses it rather than recomputing: the cache pointer must be identical
// before and after the second call, since a cache hit never re-enters
// cacheDerivedQuery's write-back at all (lockedSnapshot's cold flag in
// query() only calls it when snap.derived arrived nil).
func TestQuery_PopulatesAndReusesDerivedCache(t *testing.T) {
	dir := t.TempDir()
	writeSyntheticData(t, dir, 5)
	h, done := newTestHandlers(t, dir)
	defer done()

	if _, err := h.setSchema(setSchemaInput{Schema: syntheticSchemaYAML, Format: "yaml"}); err != nil {
		t.Fatalf("set_schema: %v", err)
	}
	if _, err := h.setRules(setRulesInput{Source: `derived(X) :- event(X, _, _).` + "\n"}); err != nil {
		t.Fatalf("set_rules: %v", err)
	}

	genBefore := h.sess.gen
	if h.sess.derivedDB != nil {
		t.Fatal("derivedDB should be nil before any query (set_rules clears it)")
	}

	if _, err := h.query(context.Background(), queryInput{Query: `derived(X)?`}); err != nil {
		t.Fatalf("query (first): %v", err)
	}
	if h.sess.derivedDB == nil {
		t.Fatal("query: expected derivedDB to be populated by the base fixpoint's write-back, got nil")
	}
	if h.sess.gen != genBefore {
		t.Errorf("query: gen changed from %d to %d; write-back must not bump gen", genBefore, h.sess.gen)
	}
	cached := h.sess.derivedDB

	if _, err := h.query(context.Background(), queryInput{Query: `derived(X)?`}); err != nil {
		t.Fatalf("query (second): %v", err)
	}
	if h.sess.derivedDB != cached {
		t.Error("query: second query's derivedDB pointer changed; expected the cached fixpoint to be reused, not recomputed")
	}
}

// TestQuery_CachedDerivedExcludesSyntheticQueryPredicate asserts the cached
// base fixpoint never contains the synthetic _q_ predicate runQuery builds
// per query: querySnapshot.runQuery populates qs.derived with the BASE
// ruleset's fixpoint before it ever compiles/runs the _q_ stage, so this
// holds by construction, but is worth pinning directly given how easy it
// would be for a future change to instead cache runQuery's final output.
func TestQuery_CachedDerivedExcludesSyntheticQueryPredicate(t *testing.T) {
	dir := t.TempDir()
	writeSyntheticData(t, dir, 5)
	h, done := newTestHandlers(t, dir)
	defer done()

	if _, err := h.setSchema(setSchemaInput{Schema: syntheticSchemaYAML, Format: "yaml"}); err != nil {
		t.Fatalf("set_schema: %v", err)
	}
	if _, err := h.setRules(setRulesInput{Source: `derived(X) :- event(X, _, _).` + "\n"}); err != nil {
		t.Fatalf("set_rules: %v", err)
	}

	if _, err := h.query(context.Background(), queryInput{Query: `derived(X)?`}); err != nil {
		t.Fatalf("query: %v", err)
	}
	if h.sess.derivedDB == nil {
		t.Fatal("query: expected derivedDB to be populated")
	}
	for name := range h.sess.derivedDB.Predicates() {
		if name == "_q_" {
			t.Fatal("cached derivedDB contains the synthetic _q_ predicate; only the base fixpoint must be cached")
		}
	}
}

// TestQuery_StaleWriteBackDropped covers the gen guard: a query that pauses
// mid-Transform on its cold path (via a blocking external predicate) races a
// concurrent set_rules that lands before the paused query's base Transform
// returns. set_rules bumps session.gen and clears derivedDB as usual; the
// paused query's write-back, computed against the OLD generation, must be
// silently dropped rather than resurrecting a fixpoint for a ruleset that no
// longer exists — mirroring runApplyRulesDocument's `gen == snapGen` guard
// (rules_editor.go).
func TestQuery_StaleWriteBackDropped(t *testing.T) {
	h, done := newTestHandlers(t, t.TempDir())
	defer done()

	inTransform := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	h.sess.engineOpts = []seminaive.Option{
		seminaive.WithExternal("slow", 1, func(ctx context.Context, _ seminaive.Bindings) iter.Seq[[]any] {
			return func(yield func([]any) bool) {
				once.Do(func() { close(inTransform) })
				select {
				case <-release:
				case <-ctx.Done():
				}
				yield([]any{"done"})
			}
		}),
	}
	if _, err := h.setRules(setRulesInput{Source: `r(X) :- slow(X).` + "\n"}); err != nil {
		t.Fatalf("set_rules (initial): %v", err)
	}

	queryDone := make(chan error, 1)
	go func() {
		_, err := h.query(context.Background(), queryInput{Query: `r(X)?`})
		queryDone <- err
	}()

	<-inTransform // the query's cold-path base stage is now blocked in slow(X)

	// Race a mutation in: bumps gen and clears derivedDB before the paused
	// query's base Transform ever returns.
	if _, err := h.setRules(setRulesInput{Source: `s(X) :- slow(X).` + "\n"}); err != nil {
		t.Fatalf("set_rules (racing): %v", err)
	}

	close(release) // let the paused query's base Transform finish
	if err := <-queryDone; err != nil {
		t.Fatalf("query: %v", err)
	}

	if h.sess.derivedDB != nil {
		t.Error("query: stale write-back landed after a racing set_rules; derivedDB should still be nil")
	}
}

// TestQuery_CacheRefusesOverFactCapBase asserts checkFactCap's total-size
// policy applies to the write-back exactly as it does to Run's
// (rules_editor.go): a base fixpoint whose total fact count exceeds factCap
// must not be cached, even though the query itself succeeds. Zero rules
// (base stage skipped, base = the raw EDB snapshot verbatim) plus a large
// already-loaded dataset is what forces this apart from WithFactLimit, which
// only counts facts newly DERIVED during a Transform call and so never fires
// here at all (see checkFactCap's doc comment in sandbox.go): the query
// itself (`event("h7", Pid, Cmd)?`) derives exactly one _q_ fact, well under
// factCap, so it succeeds — but the 1500-fact base it would otherwise cache
// exceeds factCap on its own.
func TestQuery_CacheRefusesOverFactCapBase(t *testing.T) {
	dir := t.TempDir()
	writeSyntheticData(t, dir, 1500)
	h, closeFn, err := newMCPHandlers(dir, "", nil, 5*time.Second)
	if err != nil {
		t.Fatalf("newMCPHandlers: %v", err)
	}
	defer closeFn()

	if _, err := h.setSchema(setSchemaInput{Schema: syntheticSchemaYAML, Format: "yaml"}); err != nil {
		t.Fatalf("set_schema: %v", err)
	}

	out, err := h.query(context.Background(), queryInput{Query: `event("h7", Pid, Cmd)?`})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if out.Total != 1 {
		t.Fatalf("query: total = %d, want 1", out.Total)
	}
	if h.sess.derivedDB != nil {
		t.Error("query: expected an over-factCap base fixpoint to be refused by the write-back, but derivedDB was populated")
	}
}

// -- list_predicates / sample_facts ----------------------------------------

func TestSampleFacts_Limit(t *testing.T) {
	dir := t.TempDir()
	writeSyntheticData(t, dir, 50)
	h, done := newTestHandlers(t, dir)
	defer done()

	if _, err := h.setSchema(setSchemaInput{Schema: syntheticSchemaYAML, Format: "yaml"}); err != nil {
		t.Fatalf("set_schema: %v", err)
	}

	out, err := h.sampleFacts(sampleFactsInput{Predicate: "event", Arity: 3, Limit: 7})
	if err != nil {
		t.Fatalf("sample_facts: %v", err)
	}
	if len(out.Facts) != 7 {
		t.Fatalf("sample_facts: got %d facts, want 7", len(out.Facts))
	}
	if out.Total != 50 {
		t.Fatalf("sample_facts: total = %d, want 50", out.Total)
	}
	if !out.Truncated {
		t.Error("sample_facts: expected truncated=true")
	}
}

func TestSampleFacts_DefaultLimit(t *testing.T) {
	dir := t.TempDir()
	writeSyntheticData(t, dir, 50)
	h, done := newTestHandlers(t, dir)
	defer done()

	if _, err := h.setSchema(setSchemaInput{Schema: syntheticSchemaYAML, Format: "yaml"}); err != nil {
		t.Fatalf("set_schema: %v", err)
	}

	out, err := h.sampleFacts(sampleFactsInput{Predicate: "event", Arity: 3})
	if err != nil {
		t.Fatalf("sample_facts: %v", err)
	}
	if len(out.Facts) != defaultSampleFactsLimit {
		t.Fatalf("sample_facts: got %d facts, want default limit %d", len(out.Facts), defaultSampleFactsLimit)
	}
}

// The fact-listing tools read the session's evaluated snapshot, not the
// EDB alone: a rule-derived predicate the workbench's Fact Browser shows
// with N facts must not report 0 through sample_facts/list_predicates
// (the agent and the human have to agree on what exists).
func TestSampleFacts_DerivedPredicate(t *testing.T) {
	dir := t.TempDir()
	writeSyntheticData(t, dir, 5)
	h, done := newTestHandlers(t, dir)
	defer done()

	if _, err := h.setSchema(setSchemaInput{Schema: syntheticSchemaYAML, Format: "yaml"}); err != nil {
		t.Fatalf("set_schema: %v", err)
	}
	if _, err := h.setRules(setRulesInput{Source: `active(Host) :- event(Host, Pid, Cmd).`}); err != nil {
		t.Fatalf("set_rules: %v", err)
	}

	// Before evaluation the derived predicate reads as empty everywhere —
	// consistent with the Fact Browser's fallback to the EDB snapshot.
	out, err := h.sampleFacts(sampleFactsInput{Predicate: "active", Arity: 1})
	if err != nil {
		t.Fatalf("sample_facts before evaluate: %v", err)
	}
	if out.Total != 0 {
		t.Fatalf("sample_facts before evaluate: total = %d, want 0", out.Total)
	}

	// A Run (session.evaluate + derivedDB cache, as serve startup and the
	// workbench's Run action both do) makes derived facts visible to both.
	evaluated, _, err := h.sess.evaluate(context.Background())
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}
	h.sess.derivedDB = evaluated

	out, err = h.sampleFacts(sampleFactsInput{Predicate: "active", Arity: 1})
	if err != nil {
		t.Fatalf("sample_facts after evaluate: %v", err)
	}
	if out.Total == 0 {
		t.Fatal("sample_facts after evaluate: derived facts still report 0")
	}

	preds, err := h.listPredicates(listPredicatesInput{})
	if err != nil {
		t.Fatalf("list_predicates: %v", err)
	}
	for _, p := range preds.Predicates {
		if p.Name == "active" && p.Arity == 1 {
			if p.Facts == 0 {
				t.Fatal("list_predicates: derived predicate still counts 0 facts")
			}
			return
		}
	}
	t.Fatal("list_predicates: derived predicate missing from listing")
}

// list_predicates and sample_facts read counts through memory.Database's
// PredicateCounts/FactCount (O(1) per predicate) rather than scanning every
// fact of every predicate, per doc/features/mcp-server.md review item 7.
// PredicateCounts keys on (name, arity) pairs, not name alone — this guards
// the keying: an overloaded predicate name at two arities must report each
// arity's own count, not merge or swap them.
func TestListPredicates_OverloadedArityCounts(t *testing.T) {
	dir := t.TempDir()
	h, done := newTestHandlers(t, dir)
	defer done()

	src := `
tag("x").
tag("a", "b").
tag("c", "d").
tag("e", "f").
`
	if _, err := h.setRules(setRulesInput{Source: src}); err != nil {
		t.Fatalf("set_rules: %v", err)
	}
	evaluated, _, err := h.sess.evaluate(context.Background())
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}
	h.sess.derivedDB = evaluated

	out, err := h.listPredicates(listPredicatesInput{})
	if err != nil {
		t.Fatalf("list_predicates: %v", err)
	}
	got := map[int]int{}
	for _, p := range out.Predicates {
		if p.Name == "tag" {
			got[p.Arity] = p.Facts
		}
	}
	if got[1] != 1 {
		t.Errorf("tag/1 facts = %d, want 1", got[1])
	}
	if got[2] != 3 {
		t.Errorf("tag/2 facts = %d, want 3", got[2])
	}

	one, err := h.sampleFacts(sampleFactsInput{Predicate: "tag", Arity: 1})
	if err != nil {
		t.Fatalf("sample_facts tag/1: %v", err)
	}
	if one.Total != 1 {
		t.Errorf("sample_facts tag/1: total = %d, want 1", one.Total)
	}
	two, err := h.sampleFacts(sampleFactsInput{Predicate: "tag", Arity: 2})
	if err != nil {
		t.Fatalf("sample_facts tag/2: %v", err)
	}
	if two.Total != 3 {
		t.Errorf("sample_facts tag/2: total = %d, want 3", two.Total)
	}
}

// -- describe -----------------------------------------------------------

// TestDescribeTool_ReturnsDeclarationAndRuleRefs is the MCP frontend test
// for the describe tool (mcp.go's describe method, session.describe in
// describe.go): it must surface the declaration's assembled Use, the fact
// count, and derivedBy/consumedBy entries with their own doc comments —
// exactly what the session-level describe returns, since the MCP handler
// is a thin pass-through.
func TestDescribeTool_ReturnsDeclarationAndRuleRefs(t *testing.T) {
	dir := t.TempDir()
	h, done := newTestHandlers(t, dir)
	defer done()

	src := `
%% A host observed doing something.
event(Host, Kind) :- raw(Host, Kind, ?).

%% Hosts with no observed events.
quiet(Host) :- known_host(Host), not event(Host, ?).
`
	if _, err := h.setRules(setRulesInput{Source: src}); err != nil {
		t.Fatalf("set_rules: %v", err)
	}

	out, err := h.describe(describeInput{Predicate: "event"})
	if err != nil {
		t.Fatalf("describe: %v", err)
	}
	if out.Name != "event" {
		t.Fatalf("describe: name = %q, want %q", out.Name, "event")
	}
	if len(out.Arities) != 1 {
		t.Fatalf("describe: got %d arities, want 1", len(out.Arities))
	}
	a := out.Arities[0]
	if a.Arity != 2 {
		t.Fatalf("describe: arity = %d, want 2", a.Arity)
	}
	if len(a.DerivedBy) != 1 {
		t.Fatalf("describe: derivedBy has %d entries, want 1", len(a.DerivedBy))
	}
	if !strings.Contains(a.DerivedBy[0].Doc, "observed doing something") {
		t.Errorf("describe: derivedBy[0].Doc = %q, missing the rule's doc comment", a.DerivedBy[0].Doc)
	}
	if len(a.ConsumedBy) != 1 {
		t.Fatalf("describe: consumedBy has %d entries, want 1 (quiet's negated atom)", len(a.ConsumedBy))
	}
	if !strings.Contains(a.ConsumedBy[0].RuleText, "not event") {
		t.Errorf("describe: consumedBy[0].RuleText = %q, missing the negated atom", a.ConsumedBy[0].RuleText)
	}
}

// TestDescribeTool_UnknownPredicateErrors mirrors sample_facts/explain's
// unknown-input handling: an unrecognized predicate name is a tool error,
// not a silently empty result — see describeUnknownError.
func TestDescribeTool_UnknownPredicateErrors(t *testing.T) {
	dir := t.TempDir()
	h, done := newTestHandlers(t, dir)
	defer done()

	if _, err := h.setRules(setRulesInput{Source: `event("h1").`}); err != nil {
		t.Fatalf("set_rules: %v", err)
	}
	if _, err := h.describe(describeInput{Predicate: "nope"}); err == nil {
		t.Fatal("describe: expected an error for an unknown predicate, got none")
	}
}

// The query tool's description must report the timeout the server was
// actually started with, not a fixed number that only matches one of the
// two /mcp-serving binary modes (`datalog mcp`'s operator-configurable
// --timeout, default 60s, vs `datalog serve`'s fixed 5s evalTimeout).
func TestMCPQueryDescription_MatchesConfiguredTimeout(t *testing.T) {
	got := mcpQueryDescription(5 * time.Second)
	if !strings.Contains(got, "5s") {
		t.Errorf("query tool description does not mention the configured 5s timeout:\n%s", got)
	}
	if strings.Contains(got, "60s") {
		t.Errorf("query tool description still hardcodes 60s even though this server is configured for 5s")
	}
}

// -- sample_input -----------------------------------------------------------

func TestSampleInput_OffsetAndLimit(t *testing.T) {
	dir := t.TempDir()
	writeSyntheticData(t, dir, 20)
	h, done := newTestHandlers(t, dir)
	defer done()

	out, err := h.sampleInput(sampleInputInput{File: "events.jsonl", Offset: 10, Limit: 3})
	if err != nil {
		t.Fatalf("sample_input: %v", err)
	}
	if len(out.Lines) != 3 {
		t.Fatalf("sample_input: got %d lines, want 3", len(out.Lines))
	}
	if out.TotalLines != 20 {
		t.Fatalf("sample_input: total_lines = %d, want 20", out.TotalLines)
	}
	if !strings.Contains(out.Lines[0], `"host": "h10"`) {
		t.Errorf("sample_input: offset 10 line[0] = %q, expected host h10", out.Lines[0])
	}
	if !out.Truncated {
		t.Error("sample_input: expected truncated=true (10+3=13 < 20 total)")
	}
}

func TestSampleInput_NotTruncatedAtEnd(t *testing.T) {
	dir := t.TempDir()
	writeSyntheticData(t, dir, 5)
	h, done := newTestHandlers(t, dir)
	defer done()

	out, err := h.sampleInput(sampleInputInput{File: "events.jsonl", Offset: 0, Limit: 10})
	if err != nil {
		t.Fatalf("sample_input: %v", err)
	}
	if len(out.Lines) != 5 {
		t.Fatalf("sample_input: got %d lines, want 5", len(out.Lines))
	}
	if out.Truncated {
		t.Error("sample_input: expected truncated=false when limit exceeds remaining lines")
	}
}

func TestSampleInput_LineTruncation(t *testing.T) {
	dir := t.TempDir()
	longVal := strings.Repeat("x", sampleInputLineTruncated+500)
	mustWriteFile(t, filepath.Join(dir, "big.jsonl"), `{"v":"`+longVal+`"}`+"\n")
	h, done := newTestHandlers(t, dir)
	defer done()

	out, err := h.sampleInput(sampleInputInput{File: "big.jsonl"})
	if err != nil {
		t.Fatalf("sample_input: %v", err)
	}
	if len(out.Lines) != 1 {
		t.Fatalf("sample_input: got %d lines, want 1", len(out.Lines))
	}
	line := out.Lines[0]
	if !strings.Contains(line, "...[truncated,") {
		t.Errorf("sample_input: expected truncation marker in line, got suffix %q", line[max(0, len(line)-60):])
	}
	if len(line) >= sampleInputLineTruncated+500 {
		t.Errorf("sample_input: line not actually truncated, len=%d", len(line))
	}
}

func TestSampleInput_ListsFiles(t *testing.T) {
	dir := t.TempDir()
	writeSyntheticData(t, dir, 1)
	mustWriteFile(t, filepath.Join(dir, "other.jsonl"), "{}\n")
	h, done := newTestHandlers(t, dir)
	defer done()

	out, err := h.sampleInput(sampleInputInput{})
	if err != nil {
		t.Fatalf("sample_input (list): %v", err)
	}
	if len(out.Files) != 2 {
		t.Fatalf("sample_input (list): got %d files, want 2: %v", len(out.Files), out.Files)
	}
}

func TestSampleInput_EscapeRejected(t *testing.T) {
	dir := t.TempDir()
	writeSyntheticData(t, dir, 1)
	h, done := newTestHandlers(t, dir)
	defer done()

	if _, err := h.sampleInput(sampleInputInput{File: "../etc/passwd"}); err == nil {
		t.Fatal("sample_input: expected error for path escaping data dir, got none")
	}
	if _, err := h.sampleInput(sampleInputInput{File: "/etc/passwd"}); err == nil {
		t.Fatal("sample_input: expected error for absolute path, got none")
	}
}

// -- concurrency smoke test -------------------------------------------------

// TestConcurrentHandlerCalls drives several handler methods from goroutines
// concurrently, locked exactly as registerTools wires them — query manages
// h.mu itself (snapshot-only) and so runs its Transform genuinely in
// parallel with the locked tools — to catch data races (run with -race) in
// session/database sharing.
func TestConcurrentHandlerCalls(t *testing.T) {
	dir := t.TempDir()
	writeSyntheticData(t, dir, 30)
	h, done := newTestHandlers(t, dir)
	defer done()

	if _, err := h.setSchema(setSchemaInput{Schema: syntheticSchemaYAML, Format: "yaml"}); err != nil {
		t.Fatalf("set_schema: %v", err)
	}
	if _, err := h.setRules(setRulesInput{Source: `foo(X) :- event(_, X, _).` + "\n"}); err != nil {
		t.Fatalf("set_rules: %v", err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, 64)
	call := func(fn func() error) {
		defer wg.Done()
		h.mu.Lock()
		defer h.mu.Unlock()
		if err := fn(); err != nil {
			errs <- err
		}
	}

	// query takes h.mu itself (only around its state snapshot), so it is
	// called bare — wrapping it in call() would self-deadlock, just like
	// double-locking in registerTools would.
	callUnlocked := func(fn func() error) {
		defer wg.Done()
		if err := fn(); err != nil {
			errs <- err
		}
	}

	for i := 0; i < 8; i++ {
		wg.Add(4)
		go callUnlocked(func() error {
			_, err := h.query(context.Background(), queryInput{Query: `foo(X)?`})
			return err
		})
		go call(func() error {
			_, err := h.listPredicates(listPredicatesInput{})
			return err
		})
		go call(func() error {
			_, err := h.sampleFacts(sampleFactsInput{Predicate: "event", Arity: 3})
			return err
		})
		go call(func() error {
			_, err := h.sampleInput(sampleInputInput{File: "events.jsonl", Limit: 5})
			return err
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("concurrent handler call: %v", err)
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
