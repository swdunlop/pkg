package jsonfacts_test

import (
	"context"
	"testing"
	"testing/fstest"

	"swdunlop.dev/pkg/datalog"
	"swdunlop.dev/pkg/datalog/jsonfacts"
	"swdunlop.dev/pkg/datalog/seminaive"
	"swdunlop.dev/pkg/datalog/syntax"
)

// Validates the Wave-1 tightening: a jsonfacts config's doc-only (zero-Terms)
// declaration is stored DocOnly, so a downstream engine that builds a checking
// set from the loaded database's declarations (via seminaive.WithDeclarations)
// does NOT re-trigger the arity-0 rejection for a rule referencing that
// predicate at its real (non-zero) arity.
func TestValidateDocOnlyTravelsToDownstreamWithDeclarations(t *testing.T) {
	dataFS := fstest.MapFS{
		"events.jsonl": {Data: []byte(`{"id":"e1","kind":"login"}` + "\n")},
	}
	cfg := jsonfacts.Config{
		Declarations: []datalog.Declaration{
			{Name: "event", Use: "documentation only, no terms"},
		},
		Sources: []jsonfacts.Source{{
			File:     "events.jsonl",
			Mappings: []jsonfacts.Mapping{{Predicate: "event", Args: []string{"value.id", "value.kind"}}},
		}},
	}
	db, err := cfg.LoadFS(dataFS)
	if err != nil {
		t.Fatalf("LoadFS: %v", err)
	}

	var stored []datalog.Declaration
	sawDocOnly := false
	for d := range db.Declarations() {
		stored = append(stored, d)
		if d.Name == "event" {
			if !d.DocOnly {
				t.Errorf("stored event declaration should be DocOnly, got %+v", d)
			}
			sawDocOnly = true
		}
	}
	if !sawDocOnly {
		t.Fatal("event declaration missing from Database.Declarations() -- schema display lost")
	}

	rules, err := syntax.ParseAll(`hit(Id) :- event(Id, "login").`)
	if err != nil {
		t.Fatalf("ParseAll: %v", err)
	}
	eng := seminaive.New(seminaive.WithDeclarations(stored))
	tr, err := eng.Compile(rules)
	if err != nil {
		t.Fatalf("Compile with stored decls should succeed, got: %v", err)
	}
	out, err := tr.Transform(context.Background(), db)
	if err != nil {
		t.Fatalf("Transform: %v", err)
	}
	got := 0
	for range out.Facts("hit", 1) {
		got++
	}
	if got != 1 {
		t.Errorf("expected 1 derived hit fact, got %d", got)
	}
}
