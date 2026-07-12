package jsonfacts_test

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"swdunlop.dev/pkg/datalog"
	"swdunlop.dev/pkg/datalog/jsonfacts"
	"swdunlop.dev/pkg/datalog/seminaive"
	"swdunlop.dev/pkg/datalog/syntax"
)

func TestSimpleMappingCompositeArg(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "events.jsonl", `{"pid":1,"proc":{"name":"sh","args":["-c","id"]}}
`)
	writeSchema(t, dir, map[string]any{
		"sources": []any{
			map[string]any{
				"file": "events.jsonl",
				"mappings": []any{
					map[string]any{
						"predicate": "proc",
						"args":      []string{"value.pid", "value.proc"},
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

	var got []*datalog.Composite
	for row := range db.Facts("proc", 2) {
		c, ok := row[1].(*datalog.Composite)
		if !ok {
			t.Fatalf("expected *Composite, got %T", row[1])
		}
		got = append(got, c)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 proc fact, got %d", len(got))
	}
	if want := `{"args":["-c","id"],"name":"sh"}`; got[0].Canonical() != want {
		t.Errorf("got %s, want %s", got[0].Canonical(), want)
	}
}

func TestImperativeFlattenAndRetain(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "events.jsonl", `{"pid":10,"name":"x.tmp.exe","host":"a"}
{"pid":11,"name":"sh","host":"b"}
`)
	writeSchema(t, dir, map[string]any{
		"sources": []any{
			map[string]any{
				"file": "events.jsonl",
				"mappings": []any{
					map[string]any{
						"expr": `let id = fresh_id(); assert("event", [id, value]); assert("process", [id, value.pid, value.name])`,
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

	// Detection runs on the flat predicate; alerts reach back for the record.
	rs, err := syntax.ParseAll(`
		suspicious(Id) :- process(Id, ?, N), @ends_with(N, ".tmp.exe").
		alert(Id, R) :- suspicious(Id), event(Id, R).
	`)
	if err != nil {
		t.Fatal(err)
	}
	tr, err := seminaive.New().Compile(rs)
	if err != nil {
		t.Fatal(err)
	}
	out, err := tr.Transform(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	enc := jsonfacts.NewEncoder(&buf, []datalog.Declaration{{
		Name:  "alert",
		Terms: []datalog.TermDeclaration{{Name: "id"}, {Name: "record"}},
	}})
	count := 0
	for row := range out.Facts("alert", 2) {
		if err := enc.Encode("alert", row); err != nil {
			t.Fatal(err)
		}
		count++
	}
	if count != 1 {
		t.Fatalf("expected 1 alert, got %d", count)
	}
	line := strings.TrimSpace(buf.String())
	// The full original record survives the pipeline.
	for _, want := range []string{`"pid":10`, `"name":"x.tmp.exe"`, `"host":"a"`} {
		if !strings.Contains(line, want) {
			t.Errorf("alert output %s missing %s", line, want)
		}
	}
}

func TestNullArgBecomesNull(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "e.jsonl", `{"a":null}
`)
	writeSchema(t, dir, map[string]any{
		"sources": []any{
			map[string]any{
				"file": "e.jsonl",
				"mappings": []any{
					map[string]any{"predicate": "p", "args": []string{"value.a"}},
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
	for row := range db.Facts("p", 1) {
		if _, ok := row[0].(datalog.Null); !ok {
			t.Errorf("expected datalog.Null, got %T", row[0])
		}
	}
}

// TestLargeIntegerSurvivesLoad ensures a JSON integer beyond float64's exact
// 2^53 range loads as the precise datalog.Integer, rather than being silently
// rounded by an intermediate float64 decode.
func TestLargeIntegerSurvivesLoad(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "e.jsonl", `{"id":9007199254740993}
`)
	writeSchema(t, dir, map[string]any{
		"sources": []any{
			map[string]any{
				"file": "e.jsonl",
				"mappings": []any{
					map[string]any{"predicate": "p", "args": []string{"value.id"}},
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
	for row := range db.Facts("p", 1) {
		i, ok := row[0].(datalog.Integer)
		if !ok {
			t.Fatalf("expected datalog.Integer, got %T", row[0])
		}
		if i != 9007199254740993 {
			t.Errorf("expected 9007199254740993, got %d (precision lost)", i)
		}
		count++
	}
	if count != 1 {
		t.Fatalf("expected 1 fact, got %d", count)
	}
}

// TestFractionalNumberLoadsAsFloat ensures a JSON number with a fractional
// part loads as datalog.Float, and that a huge float (beyond int64 range)
// still loads without error.
func TestFractionalNumberLoadsAsFloat(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "e.jsonl", `{"a":1.5,"b":1e300}
`)
	writeSchema(t, dir, map[string]any{
		"sources": []any{
			map[string]any{
				"file": "e.jsonl",
				"mappings": []any{
					map[string]any{"predicate": "p", "args": []string{"value.a", "value.b"}},
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
	for row := range db.Facts("p", 2) {
		a, ok := row[0].(datalog.Float)
		if !ok {
			t.Fatalf("expected datalog.Float for a, got %T", row[0])
		}
		if a != 1.5 {
			t.Errorf("expected 1.5, got %v", a)
		}
		b, ok := row[1].(datalog.Float)
		if !ok {
			t.Fatalf("expected datalog.Float for b, got %T", row[1])
		}
		if b != 1e300 {
			t.Errorf("expected 1e300, got %v", b)
		}
		count++
	}
	if count != 1 {
		t.Fatalf("expected 1 fact, got %d", count)
	}
}

// TestExactIntegerFloatNormalizesToInteger ensures a JSON number written
// with a trailing ".0" collapses to datalog.Integer, matching the engine's
// composite/dictionary convention that an exact-integer float and the
// equivalent integer canonicalize identically (see NewComposite's doc
// comment and normalizeFloat in canon.go).
func TestExactIntegerFloatNormalizesToInteger(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "e.jsonl", `{"a":1.0}
`)
	writeSchema(t, dir, map[string]any{
		"sources": []any{
			map[string]any{
				"file": "e.jsonl",
				"mappings": []any{
					map[string]any{"predicate": "p", "args": []string{"value.a"}},
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
	for row := range db.Facts("p", 1) {
		i, ok := row[0].(datalog.Integer)
		if !ok {
			t.Fatalf("expected datalog.Integer, got %T", row[0])
		}
		if i != 1 {
			t.Errorf("expected 1, got %d", i)
		}
	}
}

// TestBoolFieldLoadsAsDatalogBool ensures a JSON boolean loads as
// datalog.Bool, not datalog.Integer(1)/Integer(0), so it dedupes and joins
// correctly against datalog.Bool values produced elsewhere in the engine
// (e.g. the @json_get builtin over a Composite).
func TestBoolFieldLoadsAsDatalogBool(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "e.jsonl", `{"admin":true}
{"admin":false}
`)
	writeSchema(t, dir, map[string]any{
		"sources": []any{
			map[string]any{
				"file": "e.jsonl",
				"mappings": []any{
					map[string]any{"predicate": "p", "args": []string{"value.admin"}},
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
	var got []datalog.Bool
	for row := range db.Facts("p", 1) {
		b, ok := row[0].(datalog.Bool)
		if !ok {
			t.Fatalf("expected datalog.Bool, got %T", row[0])
		}
		got = append(got, b)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 facts, got %d", len(got))
	}
}

// TestBoolJoinsWithJSONGet is an end-to-end regression for the bug where a
// flattened boolean fact (loaded from JSONL) failed to join with a value
// destructured from a Composite via @json_get, because the loader produced
// datalog.Integer while @json_get produces datalog.Bool for the same JSON
// boolean. Both paths must agree on datalog.Bool for the join to fire.
func TestBoolJoinsWithJSONGet(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "events.jsonl", `{"pid":1,"admin":true,"proc":{"name":"sh","admin":true}}
{"pid":2,"admin":false,"proc":{"name":"ls","admin":false}}
`)
	writeSchema(t, dir, map[string]any{
		"sources": []any{
			map[string]any{
				"file": "events.jsonl",
				"mappings": []any{
					map[string]any{
						"expr": `let id = fresh_id(); assert("event", [id, value.proc]); assert("flat_admin", [id, value.admin])`,
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

	rs, err := syntax.ParseAll(`
		joined(Id, N) :- flat_admin(Id, A), event(Id, R), @json_get(R, "admin", A), @json_get(R, "name", N).
	`)
	if err != nil {
		t.Fatal(err)
	}
	tr, err := seminaive.New().Compile(rs)
	if err != nil {
		t.Fatal(err)
	}
	out, err := tr.Transform(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}

	names := map[string]bool{}
	for row := range out.Facts("joined", 2) {
		n, ok := row[1].(datalog.String)
		if !ok {
			t.Fatalf("expected datalog.String name, got %T", row[1])
		}
		names[string(n)] = true
	}
	if !names["sh"] || !names["ls"] {
		t.Errorf("expected joined facts for both sh and ls, got %v", names)
	}
	if len(names) != 2 {
		t.Errorf("expected exactly 2 joined facts, got %d: %v", len(names), names)
	}
}
