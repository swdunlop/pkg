package main

import (
	"archive/zip"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"swdunlop.dev/pkg/datalog/syntax"
)

// -- BUG #2: reserved _q_ query predicate ------------------------------------

// TestReservedQueryPredRejectedAsFact is the exact repro from the bug
// report: a program asserting `_q_("boom").` as a fact, then querying
// `event(X)?`, used to return X = "boom" mixed into the real answers with
// no error at all — because runQuery's synthetic query rule
// (session.go's querySnapshot.runQuery, "Build synthetic rule:
// _q_(Var1, ..., VarN) :- body.") writes its result rows into that exact
// predicate, and nothing rejected a user fact of the same name. This test
// asserts setRules now rejects the fact outright, and that loadProgram
// (the REPL's path, which is where the bug report's raw session-level
// repro lands) does too, and that — as a final belt-and-braces check — a
// session that got the `_q_` fact in some other way still cannot leak it
// into an unrelated query's answers... but since the fix is "never let it
// in," the primary assertion is the rejection itself.
func TestReservedQueryPredRejectedAsFact(t *testing.T) {
	s := &session{}
	_, err := s.setRules(`_q_("boom").`)
	if err == nil {
		t.Fatal("setRules: expected error for _q_ fact, got none")
	}
	if !strings.Contains(err.Error(), "_q_") {
		t.Fatalf("setRules error does not name the reserved predicate: %v", err)
	}
}

// TestReservedQueryPredRejectedAsRuleHead covers the rule-head form (not
// just a ground fact) through setRules.
func TestReservedQueryPredRejectedAsRuleHead(t *testing.T) {
	s := &session{}
	_, err := s.setRules(`_q_(X) :- event(X).`)
	if err == nil {
		t.Fatal("setRules: expected error for _q_ rule head, got none")
	}
	if !strings.Contains(err.Error(), "_q_") {
		t.Fatalf("setRules error does not name the reserved predicate: %v", err)
	}
}

// TestReservedQueryPredRejectedAsAggHead covers the aggregate-rule-head
// form through setRules.
func TestReservedQueryPredRejectedAsAggHead(t *testing.T) {
	s := &session{}
	_, err := s.setRules(`_q_(N) :- N = count : event(X).`)
	if err == nil {
		t.Fatal("setRules: expected error for _q_ aggregate rule head, got none")
	}
	if !strings.Contains(err.Error(), "_q_") {
		t.Fatalf("setRules error does not name the reserved predicate: %v", err)
	}
}

// TestReservedQueryPredRejectedAsBodyAtom covers the reserved predicate
// appearing only in a rule body, not as any head.
func TestReservedQueryPredRejectedAsBodyAtom(t *testing.T) {
	s := &session{}
	_, err := s.setRules(`seen(X) :- _q_(X).`)
	if err == nil {
		t.Fatal("setRules: expected error for _q_ in a rule body, got none")
	}
	if !strings.Contains(err.Error(), "_q_") {
		t.Fatalf("setRules error does not name the reserved predicate: %v", err)
	}
}

// TestReservedQueryPredRejectedViaLoadProgram exercises the REPL's
// .load/loadProgram funnel specifically, since it is a separate mutator
// from setRules/setRulesWithQueries and must not be a hole a future call
// site could reintroduce the bug through.
func TestReservedQueryPredRejectedViaLoadProgram(t *testing.T) {
	s := &session{}
	if _, err := s.loadProgram(`_q_("boom").`); err == nil {
		t.Fatal("loadProgram: expected error for _q_ fact, got none")
	}
}

// TestReservedQueryPredRejectedViaSetRulesWithQueries exercises the
// workbench Datalog Editor's funnel.
func TestReservedQueryPredRejectedViaSetRulesWithQueries(t *testing.T) {
	s := &session{}
	if _, err := s.setRulesWithQueries(`_q_("boom").`); err == nil {
		t.Fatal("setRulesWithQueries: expected error for _q_ fact, got none")
	}
}

// TestNormalProgramStillWorksAfterReservedPredCheck is the negative case:
// the new validation must not reject ordinary programs that happen to use
// unrelated predicate names.
func TestNormalProgramStillWorksAfterReservedPredCheck(t *testing.T) {
	s := &session{}
	if _, err := s.setRules(`event("h1", 1, "cmd1").
suspicious(H) :- event(H, _, _).`); err != nil {
		t.Fatalf("setRules: unexpected error for a normal program: %v", err)
	}
	if len(s.rules) != 2 {
		t.Fatalf("setRules: got %d rules, want 2 (the event fact and the suspicious rule)", len(s.rules))
	}
}

// TestReservedQueryPredDoesNotLeakIntoQueryResults is the end-to-end repro
// from the bug report: assert a normal event fact, attempt (and fail) to
// also assert a `_q_` fact via loadProgram, then run `event(X)?` and
// confirm only the real fact comes back — i.e. the rejection actually
// prevents the pollution the bug described, not just that some error is
// returned somewhere.
func TestReservedQueryPredDoesNotLeakIntoQueryResults(t *testing.T) {
	s := &session{}
	if _, err := s.loadProgram(`event("real").`); err != nil {
		t.Fatalf("loadProgram: unexpected error: %v", err)
	}
	if _, err := s.loadProgram(`_q_("boom").`); err == nil {
		t.Fatal("loadProgram: expected error for _q_ fact, got none")
	}

	q, err := syntax.ParseStatement(`event(X)?`)
	if err != nil {
		t.Fatalf("ParseStatement: %v", err)
	}
	query := q.(*syntax.Query)
	rows, vars, _, err := s.runQuery(context.Background(), query)
	if err != nil {
		t.Fatalf("runQuery: %v", err)
	}
	if len(vars) != 1 || vars[0] != "X" {
		t.Fatalf("runQuery vars = %v, want [X]", vars)
	}
	if len(rows) != 1 || len(rows[0]) != 1 || rows[0][0].String() != `"real"` {
		t.Fatalf("runQuery rows = %v, want exactly one row for \"real\" (no _q_ pollution)", rows)
	}
}

// -- BUG #3: CLI loadData drops *_from matcher patterns ----------------------

// TestLoadDataResolvesFromFiles is the CLI-side regression for BUG #3: a
// config whose matcher uses contains_from (a file of patterns, not an
// inline list) previously validated but silently matched against nothing,
// because loadData called cfg.LoadDir directly without first calling
// cfg.ResolveFromFS — unlike the MCP set_schema path (prepareSchema, which
// does call it). Before the fix, zero "flagged" facts were derived, with
// no error or warning. This drives the exact CLI path (session.setDataSource
// + session.loadData) that -c/.reload use.
func TestLoadDataResolvesFromFiles(t *testing.T) {
	dir := t.TempDir()
	mustWriteFile(t, filepath.Join(dir, "events.jsonl"),
		`{"host": "evil.example.com"}`+"\n"+`{"host": "fine.example.com"}`+"\n")
	mustWriteFile(t, filepath.Join(dir, "patterns.txt"), "evil\n")

	cfgYAML := `
sources:
  - file: events.jsonl
    mappings:
      - predicate: host_seen
        args: ["value.host"]
matchers:
  - predicate: host_seen
    term: 0
    contains_from: patterns.txt
`
	cfgPath := filepath.Join(dir, "config.yaml")
	mustWriteFile(t, cfgPath, cfgYAML)

	s := &session{}
	s.setDataSource(cfgPath, dir)
	if err := s.loadData(); err != nil {
		t.Fatalf("loadData: unexpected error: %v", err)
	}

	got := 0
	for range s.dataDB.Facts("contains", 2) {
		got++
	}
	if got != 1 {
		t.Fatalf("loadData: got %d contains facts via contains_from, want 1 (matcher patterns were not resolved)", got)
	}
}

// TestLoadDataResolvesFromFilesInZip is loadFromZip's half of the same fix:
// a zip-packaged data source's matcher *_from file must resolve against
// the zip's own contents, not be silently dropped either.
func TestLoadDataResolvesFromFilesInZip(t *testing.T) {
	dir := t.TempDir()
	zipPath := filepath.Join(dir, "data.zip")
	writeTestZip(t, zipPath, map[string]string{
		"events.jsonl": `{"host": "evil.example.com"}` + "\n" + `{"host": "fine.example.com"}` + "\n",
		"patterns.txt": "evil\n",
	})

	cfgYAML := `
sources:
  - file: events.jsonl
    mappings:
      - predicate: host_seen
        args: ["value.host"]
matchers:
  - predicate: host_seen
    term: 0
    contains_from: patterns.txt
`
	cfgPath := filepath.Join(dir, "config.yaml")
	mustWriteFile(t, cfgPath, cfgYAML)

	s := &session{}
	s.setDataSource(cfgPath, zipPath)
	if err := s.loadData(); err != nil {
		t.Fatalf("loadData (zip): unexpected error: %v", err)
	}

	got := 0
	for range s.dataDB.Facts("contains", 2) {
		got++
	}
	if got != 1 {
		t.Fatalf("loadData (zip): got %d contains facts via contains_from, want 1", got)
	}
}

// writeTestZip creates a zip file at path containing files (name -> content).
func writeTestZip(t *testing.T, path string, files map[string]string) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("creating zip %s: %v", path, err)
	}
	defer f.Close()
	zw := zip.NewWriter(f)
	for name, content := range files {
		w, err := zw.Create(name)
		if err != nil {
			t.Fatalf("zip.Create(%s): %v", name, err)
		}
		if _, err := w.Write([]byte(content)); err != nil {
			t.Fatalf("writing %s into zip: %v", name, err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("closing zip writer: %v", err)
	}
}
