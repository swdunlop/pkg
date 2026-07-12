package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
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
// concurrently, serialized by the same mutex registerTools uses around every
// call, to catch data races (run with -race) in session/database sharing.
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

	for i := 0; i < 8; i++ {
		wg.Add(4)
		go call(func() error {
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
