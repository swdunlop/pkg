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
