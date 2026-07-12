package interned

import (
	"math"
	"testing"

	"swdunlop.dev/pkg/datalog"
)

// TestInternNaNCollapsesToOneID is the regression test for the suspicion
// that Dict.Intern of float64 NaN never hits its own index (NaN != NaN
// under Go's ==, which map key lookup relies on), minting a fresh ID and a
// dead index entry on every call. Two independently-produced NaN values
// (not the same Go float64 bit pattern, though that wouldn't matter either
// way since Go maps never treat NaN == NaN) must intern to the same ID.
func TestInternNaNCollapsesToOneID(t *testing.T) {
	d := NewDict()
	id1 := d.Intern(math.NaN())
	id2 := d.Intern(-math.NaN()) // a different NaN bit pattern
	id3 := d.Intern(math.NaN())

	if id1 != id2 || id1 != id3 {
		t.Fatalf("expected all NaN interns to collapse to one ID, got %d, %d, %d", id1, id2, id3)
	}
	if d.Len() != 1 {
		t.Fatalf("expected exactly 1 dict entry for repeated NaN interning, got %d", d.Len())
	}
}

// TestInternNaNViaConstant exercises the same path through InternConstant,
// the entry point used when interning a fact term (datalog.Float wraps a
// float64 and can carry NaN if a caller constructs one directly via the Go
// API, even though the Datalog parser and `is` arithmetic can never
// produce NaN through source syntax or evaluation -- the lexer's
// readNumber only ever consumes digit runs, never letters, so a "nan"
// token can't reach strconv.ParseFloat; and applyBinOp/applyBinOpFloat in
// seminaive both guard "/" with an explicit r == 0 check that fails rather
// than dividing, so 0.0/0.0 never executes).
func TestInternNaNViaConstant(t *testing.T) {
	d := NewDict()
	c1 := datalog.Float(math.NaN())
	c2 := datalog.Float(math.NaN())
	id1 := d.InternConstant(c1)
	id2 := d.InternConstant(c2)
	if id1 != id2 {
		t.Fatalf("expected repeated NaN constants to intern identically, got %d and %d", id1, id2)
	}
	if d.Len() != 1 {
		t.Fatalf("expected exactly 1 dict entry, got %d", d.Len())
	}
}

// TestHasFindsInternedNaN checks that Has, like Intern, resolves NaN
// through the same canonical index key.
func TestHasFindsInternedNaN(t *testing.T) {
	d := NewDict()
	id := d.Intern(math.NaN())
	got, ok := d.Has(math.NaN())
	if !ok || got != id {
		t.Fatalf("Has(NaN): got %d %v, want %d true", got, ok, id)
	}
}
