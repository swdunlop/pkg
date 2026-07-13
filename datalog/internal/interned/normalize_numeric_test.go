package interned

import (
	"math"
	"testing"
)

// TestNormalizeNumericTwoTo63StaysFloat is the regression test for the
// arm64 mis-intern bug: 2^63 (9223372036854775808.0) is exactly
// representable as a float64 but is one past math.MaxInt64, so it must
// never be converted to int64. The old guard (convert to int64, then
// check the round-trip: float64(int64(f)) == f) trusted Go's
// float64->int64 conversion for an out-of-range input, which is
// implementation-defined -- on arm64, FCVTZS saturates to MaxInt64, and
// float64(MaxInt64) rounds back up to exactly 2^63, so the old guard
// would have wrongly accepted this value on that platform (while
// correctly rejecting it on amd64, where the conversion produces
// MinInt64 instead). The explicit range pre-check added to
// NormalizeNumeric rejects 2^63 before ever performing that conversion,
// so the result is identical on every platform regardless of what the
// out-of-range conversion happens to produce.
func TestNormalizeNumericTwoTo63StaysFloat(t *testing.T) {
	const twoTo63 = 9223372036854775808.0 // 2^63, exactly representable
	got := NormalizeNumeric(twoTo63)
	f, ok := got.(float64)
	if !ok {
		t.Fatalf("expected 2^63 to stay a float64, got %T(%v)", got, got)
	}
	if f != twoTo63 {
		t.Fatalf("expected value to be unchanged, got %v", f)
	}
}

// TestNormalizeNumericNegativeTwoTo63IsMinInt64 pins the inclusive lower
// boundary: -2^63 is exactly representable as a float64 AND is a valid
// int64 (math.MinInt64), so it must intern as int64, not float64.
func TestNormalizeNumericNegativeTwoTo63IsMinInt64(t *testing.T) {
	const negTwoTo63 = -9223372036854775808.0 // -2^63 == math.MinInt64
	got := NormalizeNumeric(negTwoTo63)
	i, ok := got.(int64)
	if !ok {
		t.Fatalf("expected -2^63 to normalize to int64, got %T(%v)", got, got)
	}
	if i != math.MinInt64 {
		t.Fatalf("expected math.MinInt64, got %d", i)
	}
}

// TestNormalizeNumericMaxInt64AsFloatIsConsistent pins the near-boundary
// case one below 2^63: float64(math.MaxInt64) cannot represent
// MaxInt64 (9223372036854775807) exactly -- the nearest float64 rounds up
// to exactly 2^63 -- so as a float64 value it is indistinguishable from
// 2^63 and must be treated the same way: stays a float64, not int64.
func TestNormalizeNumericMaxInt64AsFloatIsConsistent(t *testing.T) {
	f := float64(int64(math.MaxInt64))
	if f != 9223372036854775808.0 {
		t.Fatalf("test assumption broken: float64(MaxInt64) = %v, want 2^63", f)
	}
	got := NormalizeNumeric(f)
	if _, ok := got.(float64); !ok {
		t.Fatalf("expected float64(MaxInt64) (== 2^63) to stay a float64, got %T(%v)", got, got)
	}
}

// TestNormalizeNumericJustBelowMaxBoundInterns checks a genuine in-range
// large integer-valued float still normalizes to int64 as before -- the
// new range check must not reject legitimate large values.
func TestNormalizeNumericJustBelowMaxBoundInterns(t *testing.T) {
	f := 9223372036854774784.0 // largest float64 strictly less than 2^63, exactly representable
	got := NormalizeNumeric(f)
	i, ok := got.(int64)
	if !ok {
		t.Fatalf("expected in-range float to normalize to int64, got %T(%v)", got, got)
	}
	if float64(i) != f {
		t.Fatalf("round-trip mismatch: int64 %d as float64 = %v, want %v", i, float64(i), f)
	}
}

// TestNormalizeNumericNaNStaysFloat checks that NaN, which compares false
// against every bound in the new range check, falls through untouched
// rather than being accidentally accepted or panicking.
func TestNormalizeNumericNaNStaysFloat(t *testing.T) {
	got := NormalizeNumeric(math.NaN())
	f, ok := got.(float64)
	if !ok {
		t.Fatalf("expected NaN to stay a float64, got %T(%v)", got, got)
	}
	if !math.IsNaN(f) {
		t.Fatalf("expected NaN to remain NaN, got %v", f)
	}
}

// TestNormalizeNumericPositiveInfStaysFloat and its negative counterpart
// check the other non-finite values also fail the range check cleanly.
func TestNormalizeNumericPositiveInfStaysFloat(t *testing.T) {
	got := NormalizeNumeric(math.Inf(1))
	f, ok := got.(float64)
	if !ok || !math.IsInf(f, 1) {
		t.Fatalf("expected +Inf to stay a float64 +Inf, got %T(%v)", got, got)
	}
}

func TestNormalizeNumericNegativeInfStaysFloat(t *testing.T) {
	got := NormalizeNumeric(math.Inf(-1))
	f, ok := got.(float64)
	if !ok || !math.IsInf(f, -1) {
		t.Fatalf("expected -Inf to stay a float64 -Inf, got %T(%v)", got, got)
	}
}

// TestNormalizeNumericOrdinaryIntegerStillInterns is a sanity check that
// the common case -- a small integer-valued float -- still normalizes to
// int64 after adding the range pre-check.
func TestNormalizeNumericOrdinaryIntegerStillInterns(t *testing.T) {
	got := NormalizeNumeric(float64(42))
	i, ok := got.(int64)
	if !ok || i != 42 {
		t.Fatalf("expected 42.0 to normalize to int64(42), got %T(%v)", got, got)
	}
}

// TestNormalizeNumericNonIntegerFloatStaysFloat checks a fractional value
// still stays a float64 (range check passes but the round-trip check
// correctly rejects it).
func TestNormalizeNumericNonIntegerFloatStaysFloat(t *testing.T) {
	got := NormalizeNumeric(3.5)
	f, ok := got.(float64)
	if !ok || f != 3.5 {
		t.Fatalf("expected 3.5 to stay a float64, got %T(%v)", got, got)
	}
}
