package jsonfacts_test

import (
	"compress/gzip"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"swdunlop.dev/pkg/datalog/jsonfacts"
)

func writeGzFile(t *testing.T, dir, name, content string) {
	t.Helper()
	f, err := os.Create(filepath.Join(dir, name))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	gz := gzip.NewWriter(f)
	if _, err := gz.Write([]byte(content)); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestGzipSource(t *testing.T) {
	dir := t.TempDir()
	writeGzFile(t, dir, "people.jsonl.gz", `{"name":"tom","age":40}
{"name":"bob","age":30}
`)
	writeSchema(t, dir, map[string]any{
		"sources": []any{
			map[string]any{
				"file": "people.jsonl.gz",
				"mappings": []any{
					map[string]any{
						"predicate": "person",
						"args":      []string{"value.name", "value.age"},
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
	for range db.Facts("person", 2) {
		count++
	}
	if count != 2 {
		t.Errorf("expected 2 person facts, got %d", count)
	}
}

func TestGzipSourceRejectsPlainFile(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "people.jsonl.gz", `{"name":"tom","age":40}`+"\n")
	writeSchema(t, dir, map[string]any{
		"sources": []any{
			map[string]any{
				"file": "people.jsonl.gz",
				"mappings": []any{
					map[string]any{
						"predicate": "person",
						"args":      []string{"value.name", "value.age"},
					},
				},
			},
		},
	})

	var cfg jsonfacts.Config
	if err := cfg.LoadSchemaDir(dir); err != nil {
		t.Fatal(err)
	}
	if _, err := cfg.LoadDir(dir); err == nil {
		t.Fatal("expected error loading a non-gzip file with .gz name")
	} else if !strings.Contains(err.Error(), "people.jsonl.gz") {
		t.Errorf("error should name the file, got: %v", err)
	}
}
