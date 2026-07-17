package jsonfacts_test

import (
	"bytes"
	"strings"
	"testing"
	"testing/fstest"

	"swdunlop.dev/pkg/datalog"
	"swdunlop.dev/pkg/datalog/jsonfacts"
)

// --- Bug #8: an imperative (expr:) mapping's per-record runtime error used
// to abort the entire load, while the identical class of error in a
// declarative (filter/arg) mapping only skipped that one record via
// OnMappingError. These tests confirm both mapping modes now agree: a
// per-record runtime error is routed through OnMappingError and skips just
// the offending record, while a compile-time (config-level) error in an
// expr mapping remains fatal. ---

// TestImperativeMappingRuntimeErrorRoutesThroughOnMappingError reproduces
// the exact scenario from the bug report: a JSONL source with one record
// that makes an imperative expr: mapping raise a runtime error, with valid
// records before and after it, and OnMappingError set. The load must
// succeed, the hook must fire naming the bad record, and the good records
// must still produce facts.
func TestImperativeMappingRuntimeErrorRoutesThroughOnMappingError(t *testing.T) {
	dataFS := fstest.MapFS{
		"recs.jsonl": {Data: []byte(`{"id":"r1","tags":["a","b"]}
{"id":"r2","tags":["only-one"]}
{"id":"r3","tags":["c","d"]}
`)},
	}
	cfg := jsonfacts.Config{
		Sources: []jsonfacts.Source{{
			File: "recs.jsonl",
			Mappings: []jsonfacts.Mapping{{
				// Record 2 has only one tag, so tags[1] is a runtime
				// out-of-range error in expr-lang.
				Expr: `assert("rec", [value.id, value.tags[1]])`,
			}},
		}},
	}
	var mappingErrs []error
	cfg.OnMappingError = func(err error) { mappingErrs = append(mappingErrs, err) }

	db, err := cfg.LoadFS(dataFS)
	if err != nil {
		t.Fatalf("LoadFS: expected success (bad record skipped via OnMappingError), got error: %v", err)
	}

	if len(mappingErrs) != 1 {
		t.Fatalf("expected exactly 1 OnMappingError call, got %d: %v", len(mappingErrs), mappingErrs)
	}
	if !strings.Contains(mappingErrs[0].Error(), "line 2") {
		t.Errorf("expected the mapping error to name line 2, got: %v", mappingErrs[0])
	}

	var ids []string
	for row := range db.Facts("rec", 2) {
		ids = append(ids, string(row[0].(datalog.String)))
	}
	if len(ids) != 2 {
		t.Fatalf("expected 2 rec facts (r1 and r3; r2 skipped), got %d: %v", len(ids), ids)
	}
	for _, want := range []string{"r1", "r3"} {
		found := false
		for _, id := range ids {
			if id == want {
				found = true
			}
		}
		if !found {
			t.Errorf("expected rec fact for %q to survive, got %v", want, ids)
		}
	}
}

// TestImperativeMappingRuntimeErrorSkipsWithoutHook confirms the load also
// succeeds (silently skipping the bad record) when OnMappingError is nil,
// matching the declarative path's default (no-hook) behavior.
func TestImperativeMappingRuntimeErrorSkipsWithoutHook(t *testing.T) {
	dataFS := fstest.MapFS{
		"recs.jsonl": {Data: []byte(`{"id":"r1","tags":["a","b"]}
{"id":"r2","tags":["only-one"]}
`)},
	}
	cfg := jsonfacts.Config{
		Sources: []jsonfacts.Source{{
			File: "recs.jsonl",
			Mappings: []jsonfacts.Mapping{{
				Expr: `assert("rec", [value.id, value.tags[1]])`,
			}},
		}},
	}

	db, err := cfg.LoadFS(dataFS)
	if err != nil {
		t.Fatalf("LoadFS: expected success, got error: %v", err)
	}
	count := 0
	for range db.Facts("rec", 2) {
		count++
	}
	if count != 1 {
		t.Errorf("expected 1 rec fact (r1 only), got %d", count)
	}
}

// TestImperativeMappingCompileErrorStillFatal confirms a malformed expr
// that fails to COMPILE (a config-level defect, not a per-record runtime
// defect) is still a fatal error from LoadFS, not silently skipped.
func TestImperativeMappingCompileErrorStillFatal(t *testing.T) {
	dataFS := fstest.MapFS{
		"recs.jsonl": {Data: []byte(`{"id":"r1"}` + "\n")},
	}
	cfg := jsonfacts.Config{
		Sources: []jsonfacts.Source{{
			File: "recs.jsonl",
			Mappings: []jsonfacts.Mapping{{
				// Unbalanced parens: fails to compile, not a runtime error.
				Expr: `assert("rec", [value.id]`,
			}},
		}},
	}
	var mappingErrs []error
	cfg.OnMappingError = func(err error) { mappingErrs = append(mappingErrs, err) }

	_, err := cfg.LoadFS(dataFS)
	if err == nil {
		t.Fatal("expected a fatal error for the expr compile failure, got nil")
	}
	if len(mappingErrs) != 0 {
		t.Errorf("compile-level errors must not route through OnMappingError, got %d calls: %v", len(mappingErrs), mappingErrs)
	}
}

// --- Bug #10: a declaration with duplicate term names (or a named term
// colliding with the positional key another term would fall back to)
// caused Encoder.Encode to silently drop a value via a plain Go map key
// overwrite. ---

// TestDeclarationDuplicateTermNamesRejectedAtConfigValidate confirms a
// Config whose declaration has duplicate term names (e.g. transfer's
// "acct" used twice) is rejected at load time -- before any fact can be
// silently shortened -- rather than allowed through to produce a
// truncated JSON object.
func TestDeclarationDuplicateTermNamesRejectedAtConfigValidate(t *testing.T) {
	dataFS := fstest.MapFS{
		"data.jsonl": {Data: []byte(`{"from":"a1","to":"a2","amount":100}` + "\n")},
	}
	cfg := jsonfacts.Config{
		Sources: []jsonfacts.Source{{
			File: "data.jsonl",
			Mappings: []jsonfacts.Mapping{{
				Predicate: "transfer",
				Args:      []string{"value.from", "value.to", "value.amount"},
			}},
		}},
		Declarations: []datalog.Declaration{{
			Name: "transfer",
			Terms: []datalog.TermDeclaration{
				{Name: "acct"},
				{Name: "acct"}, // duplicate: collides with term 0
				{Name: "amount"},
			},
		}},
	}

	_, err := cfg.LoadFS(dataFS)
	if err == nil {
		t.Fatal("expected LoadFS to reject a declaration with duplicate term names, got nil error")
	}
	if !strings.Contains(err.Error(), "acct") {
		t.Errorf("expected error to name the duplicate term %q, got: %v", "acct", err)
	}
}

// TestEncoderDistinctTermNamesEncodeAllValues is the control case: distinct
// term names must still encode every value, none dropped.
func TestEncoderDistinctTermNamesEncodeAllValues(t *testing.T) {
	decls := []datalog.Declaration{{
		Name: "transfer",
		Terms: []datalog.TermDeclaration{
			{Name: "from_acct"},
			{Name: "to_acct"},
			{Name: "amount"},
		},
	}}
	var buf bytes.Buffer
	enc := jsonfacts.NewEncoder(&buf, decls)
	err := enc.Encode("transfer", []datalog.Constant{
		datalog.String("a1"), datalog.String("a2"), datalog.Integer(100),
	})
	if err != nil {
		t.Fatalf("Encode: unexpected error: %v", err)
	}
	out := buf.String()
	for _, want := range []string{`"from_acct":"a1"`, `"to_acct":"a2"`, `"amount":100`} {
		if !strings.Contains(out, want) {
			t.Errorf("expected encoded line to contain %s, got: %s", want, out)
		}
	}
}

// TestEncoderRejectsDuplicateTermNamesInsteadOfDroppingValue is the
// encoder-level backstop: even a hand-built []datalog.Declaration bypassing
// Config.validate must not let Encode silently overwrite a value via a Go
// map key collision -- it must return an error.
func TestEncoderRejectsDuplicateTermNamesInsteadOfDroppingValue(t *testing.T) {
	decls := []datalog.Declaration{{
		Name: "transfer",
		Terms: []datalog.TermDeclaration{
			{Name: "acct"},
			{Name: "acct"},
			{Name: "amount"},
		},
	}}
	var buf bytes.Buffer
	enc := jsonfacts.NewEncoder(&buf, decls)
	err := enc.Encode("transfer", []datalog.Constant{
		datalog.String("a1"), datalog.String("a2"), datalog.Integer(100),
	})
	if err == nil {
		t.Fatalf("Encode: expected an error for duplicate term names, got success with output: %s", buf.String())
	}
}

// TestEncoderRejectsPositionalKeyCollision probes a term literally named
// "1" colliding with the positional fallback key ("1") that an unnamed
// term at index 1 would use, per termKey's strconv.Itoa(i) fallback
// scheme.
func TestEncoderRejectsPositionalKeyCollision(t *testing.T) {
	decls := []datalog.Declaration{{
		Name: "weird",
		Terms: []datalog.TermDeclaration{
			{Name: "1"}, // collides with index 1's positional fallback key
			{Name: ""},  // unnamed -> falls back to "1"
		},
	}}

	// Defense-in-depth: even if this reaches the encoder directly (bypassing
	// Config.validate), Encode must not silently drop a value.
	var buf bytes.Buffer
	enc := jsonfacts.NewEncoder(&buf, decls)
	err := enc.Encode("weird", []datalog.Constant{datalog.String("x"), datalog.String("y")})
	if err == nil {
		t.Fatalf("Encode: expected an error for positional-key collision, got success with output: %s", buf.String())
	}

	// Also confirm Config.validate catches it at the earliest chokepoint.
	cfg := jsonfacts.Config{Declarations: decls}
	dataFS := fstest.MapFS{}
	if err := cfg.ResolveFromFS(dataFS); err != nil {
		t.Fatal(err)
	}
	if _, err := cfg.LoadFS(dataFS); err == nil {
		t.Fatal("expected Config.LoadFS to reject the positional-key collision declaration, got nil error")
	}
}

// --- Bug #3: a matcher with an unresolved *_from field (e.g.
// "contains_from": "iocs.txt") validates successfully but, if
// ResolveFromFS was never called, compileMatchers previously read only the
// (still-empty) inline pattern list and silently emitted zero derived
// facts with no warning. LoadFS must now fail loudly instead. ---

// TestUnresolvedFromFieldFailsLoudly reproduces the exact scenario: a
// config whose matcher has a *_from field, loaded WITHOUT calling
// ResolveFromFS. Previously this silently produced zero facts; now it must
// return a clear error.
func TestUnresolvedFromFieldFailsLoudly(t *testing.T) {
	dataFS := fstest.MapFS{
		"data.jsonl": {Data: []byte(`{"cmd":"certutil -urlcache"}` + "\n")},
		"iocs.txt":   {Data: []byte("certutil\n")},
	}
	cfg := jsonfacts.Config{
		Sources: []jsonfacts.Source{{
			File: "data.jsonl",
			Mappings: []jsonfacts.Mapping{{
				Predicate: "proc",
				Args:      []string{"value.cmd"},
			}},
		}},
		Matchers: []jsonfacts.Matcher{{
			Predicate:    "proc",
			Term:         0,
			ContainsFrom: "iocs.txt",
		}},
	}

	// Deliberately do NOT call cfg.ResolveFromFS here.
	_, err := cfg.LoadFS(dataFS)
	if err == nil {
		t.Fatal("expected LoadFS to fail loudly on an unresolved _from field, got nil error (silent zero-fact load)")
	}
}

// TestResolvedFromFieldLoadsExpectedFacts is the control case: the same
// config, but with ResolveFromFS called first, must load successfully and
// emit the expected derived facts.
func TestResolvedFromFieldLoadsExpectedFacts(t *testing.T) {
	dataFS := fstest.MapFS{
		"data.jsonl": {Data: []byte(`{"cmd":"certutil -urlcache"}
{"cmd":"notepad.exe"}
`)},
		"iocs.txt": {Data: []byte("certutil\n")},
	}
	cfg := jsonfacts.Config{
		Sources: []jsonfacts.Source{{
			File: "data.jsonl",
			Mappings: []jsonfacts.Mapping{{
				Predicate: "proc",
				Args:      []string{"value.cmd"},
			}},
		}},
		Matchers: []jsonfacts.Matcher{{
			Predicate:    "proc",
			Term:         0,
			ContainsFrom: "iocs.txt",
		}},
	}

	if err := cfg.ResolveFromFS(dataFS); err != nil {
		t.Fatalf("ResolveFromFS: %v", err)
	}

	db, err := cfg.LoadFS(dataFS)
	if err != nil {
		t.Fatalf("LoadFS: %v", err)
	}
	count := 0
	for range db.Facts("contains", 2) {
		count++
	}
	if count != 1 {
		t.Errorf("expected 1 contains fact, got %d", count)
	}
}

// TestMatcherWithoutFromFieldsStillLoads confirms the #3 fix does not
// regress a matcher that legitimately has no *_from fields at all (only
// inline pattern lists).
func TestMatcherWithoutFromFieldsStillLoads(t *testing.T) {
	dataFS := fstest.MapFS{
		"data.jsonl": {Data: []byte(`{"cmd":"certutil -urlcache"}
{"cmd":"notepad.exe"}
`)},
	}
	cfg := jsonfacts.Config{
		Sources: []jsonfacts.Source{{
			File: "data.jsonl",
			Mappings: []jsonfacts.Mapping{{
				Predicate: "proc",
				Args:      []string{"value.cmd"},
			}},
		}},
		Matchers: []jsonfacts.Matcher{{
			Predicate: "proc",
			Term:      0,
			Contains:  []string{"certutil"},
		}},
	}

	db, err := cfg.LoadFS(dataFS)
	if err != nil {
		t.Fatalf("LoadFS: %v", err)
	}
	count := 0
	for range db.Facts("contains", 2) {
		count++
	}
	if count != 1 {
		t.Errorf("expected 1 contains fact, got %d", count)
	}
}
