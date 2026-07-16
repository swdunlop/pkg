package memory_test

import (
	"strings"
	"testing"

	"swdunlop.dev/pkg/datalog"
	"swdunlop.dev/pkg/datalog/memory"
)

// TestQueryWideArityReturnsEmpty verifies that a query with more terms than
// interned.MaxFactArity (16) returns an empty result sequence instead of
// panicking. No fact can have arity > 16, so such a query can never match.
func TestQueryWideArityReturnsEmpty(t *testing.T) {
	b := memory.NewBuilder()
	db := b.Build()

	terms := make([]datalog.Term, 17)
	for i := range terms {
		terms[i] = datalog.Variable("V")
	}
	// Position 16 (the 17th term, out of BoundSet's [16]uint64 range) is a
	// constant -- this used to panic with an out-of-range index.
	terms[16] = datalog.Integer(42)

	count := 0
	for range db.Query("wide", terms...) {
		count++
	}
	if count != 0 {
		t.Fatalf("expected no results for arity-17 query, got %d", count)
	}
}

// TestQueryMaxArityStillWorks verifies that a query at exactly the arity
// limit (16 terms) still functions normally.
func TestQueryMaxArityStillWorks(t *testing.T) {
	b := memory.NewBuilder()

	terms := make([]datalog.Constant, 16)
	for i := range terms {
		terms[i] = datalog.Integer(int64(i))
	}
	if err := b.AddFact(datalog.Fact{Name: "wide16", Terms: terms}); err != nil {
		t.Fatalf("AddFact with arity 16 should succeed: %v", err)
	}
	db := b.Build()

	queryTerms := make([]datalog.Term, 16)
	for i := range queryTerms {
		queryTerms[i] = datalog.Variable("V")
	}
	queryTerms[15] = datalog.Integer(15)

	count := 0
	for row := range db.Query("wide16", queryTerms...) {
		count++
		if len(row) != 16 {
			t.Fatalf("expected row of length 16, got %d", len(row))
		}
		if row[15] != datalog.Integer(15) {
			t.Fatalf("expected last constant to match, got %v", row[15])
		}
	}
	if count != 1 {
		t.Fatalf("expected exactly 1 match, got %d", count)
	}
}

// TestAddFactWideArityReturnsError verifies that Builder.AddFact rejects a
// fact wider than the supported maximum (17 terms) with an error rather
// than truncating or panicking.
func TestAddFactWideArityReturnsError(t *testing.T) {
	b := memory.NewBuilder()

	terms := make([]datalog.Constant, 17)
	for i := range terms {
		terms[i] = datalog.Integer(int64(i))
	}
	err := b.AddFact(datalog.Fact{Name: "toowide", Terms: terms})
	if err == nil {
		t.Fatal("expected an error adding a fact with arity 17, got nil")
	}
	if !strings.Contains(err.Error(), "17") {
		t.Fatalf("expected error to mention the offending arity, got: %v", err)
	}
}

// TestExtendWideArityReturnsError verifies that Database.Extend rejects a
// fact wider than the supported maximum with an error, leaving the
// original database unmodified.
func TestExtendWideArityReturnsError(t *testing.T) {
	b := memory.NewBuilder()
	if err := b.AddFact(datalog.Fact{
		Name:  "narrow",
		Terms: []datalog.Constant{datalog.String("ok")},
	}); err != nil {
		t.Fatalf("unexpected error adding narrow fact: %v", err)
	}
	db := b.Build()

	terms := make([]datalog.Constant, 17)
	for i := range terms {
		terms[i] = datalog.Integer(int64(i))
	}
	_, err := db.Extend(datalog.Fact{Name: "toowide", Terms: terms})
	if err == nil {
		t.Fatal("expected an error extending with a fact of arity 17, got nil")
	}

	// Original database should still only see the narrow fact.
	count := 0
	for range db.Facts("narrow", 1) {
		count++
	}
	if count != 1 {
		t.Fatalf("expected original database to still have 1 narrow fact, got %d", count)
	}
}

// TestFactCountMatchesFacts verifies FactCount agrees with the row count a
// caller would get by ranging over Facts, for a known predicate/arity, an
// unknown arity of a known predicate, and an entirely unknown predicate.
func TestFactCountMatchesFacts(t *testing.T) {
	b := memory.NewBuilder()
	for i := 0; i < 5; i++ {
		if err := b.AddFact(datalog.Fact{Name: "event", Terms: []datalog.Constant{datalog.Integer(int64(i))}}); err != nil {
			t.Fatalf("AddFact: %v", err)
		}
	}
	if err := b.AddFact(datalog.Fact{Name: "event", Terms: []datalog.Constant{datalog.String("a"), datalog.String("b")}}); err != nil {
		t.Fatalf("AddFact: %v", err)
	}
	db := b.Build()

	if got := db.FactCount("event", 1); got != 5 {
		t.Errorf("FactCount(event, 1) = %d, want 5", got)
	}
	if got := db.FactCount("event", 2); got != 1 {
		t.Errorf("FactCount(event, 2) = %d, want 1", got)
	}
	if got := db.FactCount("event", 3); got != 0 {
		t.Errorf("FactCount(event, 3) = %d, want 0 (known predicate, unused arity)", got)
	}
	if got := db.FactCount("nope", 1); got != 0 {
		t.Errorf("FactCount(nope, 1) = %d, want 0 (unknown predicate)", got)
	}

	scanned := 0
	for range db.Facts("event", 1) {
		scanned++
	}
	if scanned != db.FactCount("event", 1) {
		t.Errorf("FactCount and ranging over Facts disagree: %d vs %d", db.FactCount("event", 1), scanned)
	}
}

// TestPredicateCountsMatchesFacts verifies PredicateCounts enumerates
// exactly the (name, arity) pairs Predicates does, each paired with the
// same count ranging over Facts would produce, and that TotalFactCount is
// their sum.
func TestPredicateCountsMatchesFacts(t *testing.T) {
	b := memory.NewBuilder()
	facts := []datalog.Fact{
		{Name: "event", Terms: []datalog.Constant{datalog.Integer(1)}},
		{Name: "event", Terms: []datalog.Constant{datalog.Integer(2)}},
		{Name: "event", Terms: []datalog.Constant{datalog.Integer(3)}},
		{Name: "proc", Terms: []datalog.Constant{datalog.String("a"), datalog.String("b")}},
	}
	for _, f := range facts {
		if err := b.AddFact(f); err != nil {
			t.Fatalf("AddFact(%v): %v", f, err)
		}
	}
	db := b.Build()

	got := map[memory.PredArity]int{}
	for pa, n := range db.PredicateCounts() {
		got[pa] = n
	}
	want := map[memory.PredArity]int{
		{Name: "event", Arity: 1}: 3,
		{Name: "proc", Arity: 2}:  1,
	}
	if len(got) != len(want) {
		t.Fatalf("PredicateCounts: got %d entries, want %d: %v", len(got), len(want), got)
	}
	for pa, n := range want {
		if got[pa] != n {
			t.Errorf("PredicateCounts[%v] = %d, want %d", pa, got[pa], n)
		}
	}

	// Cross-check against Predicates()/Facts() directly.
	for name, arity := range db.Predicates() {
		scanned := 0
		for range db.Facts(name, arity) {
			scanned++
		}
		pa := memory.PredArity{Name: name, Arity: arity}
		if got[pa] != scanned {
			t.Errorf("PredicateCounts[%v] = %d, but ranging over Facts gives %d", pa, got[pa], scanned)
		}
	}

	if total := db.TotalFactCount(); total != len(facts) {
		t.Errorf("TotalFactCount() = %d, want %d", total, len(facts))
	}
}
