package datalog

import (
	"encoding/json"
	"math"
	"strconv"
	"strings"
	"testing"
)

func mustComposite(t *testing.T, v any) *Composite {
	t.Helper()
	c, err := NewComposite(v)
	if err != nil {
		t.Fatalf("NewComposite(%v): %v", v, err)
	}
	return c
}

func TestCanonicalKeySorting(t *testing.T) {
	c := mustComposite(t, map[string]any{"zeta": 1, "alpha": 2, "mid": 3})
	want := `{"alpha":2,"mid":3,"zeta":1}`
	if c.Canonical() != want {
		t.Errorf("got %s, want %s", c.Canonical(), want)
	}
}

func TestCanonicalNumberNormalization(t *testing.T) {
	fromFloat := mustComposite(t, map[string]any{"pid": 1.0})
	fromInt := mustComposite(t, map[string]any{"pid": 1})
	if fromFloat.Canonical() != fromInt.Canonical() {
		t.Errorf("1.0 and 1 canonicalize differently: %s vs %s",
			fromFloat.Canonical(), fromInt.Canonical())
	}
	if fromInt.Canonical() != `{"pid":1}` {
		t.Errorf("got %s, want {\"pid\":1}", fromInt.Canonical())
	}

	frac := mustComposite(t, []any{1.5})
	if frac.Canonical() != "[1.5]" {
		t.Errorf("got %s, want [1.5]", frac.Canonical())
	}
}

func TestCanonicalNegativeZero(t *testing.T) {
	negZero := mustComposite(t, []any{math.Copysign(0, -1)})
	zero := mustComposite(t, []any{0})
	if negZero.Canonical() != zero.Canonical() {
		t.Errorf("-0.0 and 0 canonicalize differently: %s vs %s",
			negZero.Canonical(), zero.Canonical())
	}
}

func TestCanonicalLargeFloat(t *testing.T) {
	// 1e19 exceeds int64 range and must stay a float, formatted like Float.String.
	c := mustComposite(t, []any{1e19})
	if c.Canonical() != "[1e+19]" {
		t.Errorf("got %s, want [1e+19]", c.Canonical())
	}
	// The largest exactly representable int64-valued float64 normalizes to an integer.
	c = mustComposite(t, []any{float64(1 << 62)})
	if c.Canonical() != "[4611686018427387904]" {
		t.Errorf("got %s, want [4611686018427387904]", c.Canonical())
	}
}

func TestCanonicalNesting(t *testing.T) {
	c := mustComposite(t, map[string]any{
		"proc": map[string]any{"name": "sh", "pid": 42.0},
		"tags": []any{"b", "a"},
	})
	want := `{"proc":{"name":"sh","pid":42},"tags":["b","a"]}`
	if c.Canonical() != want {
		t.Errorf("got %s, want %s", c.Canonical(), want)
	}
}

func TestCanonicalNestedComposite(t *testing.T) {
	inner := mustComposite(t, map[string]any{"x": 1})
	outer := mustComposite(t, map[string]any{"inner": inner})
	if outer.Canonical() != `{"inner":{"x":1}}` {
		t.Errorf("got %s", outer.Canonical())
	}
}

func TestNewCompositeErrors(t *testing.T) {
	cases := []struct {
		name string
		v    any
	}{
		{"NaN", []any{math.NaN()}},
		{"+Inf", []any{math.Inf(1)}},
		{"-Inf", map[string]any{"x": math.Inf(-1)}},
		{"scalar string", "hello"},
		{"scalar int", 42},
		{"scalar nil", nil},
		{"uint64 overflow", []any{uint64(math.MaxUint64)}},
		{"unsupported type", []any{make(chan int)}},
	}
	for _, tc := range cases {
		if _, err := NewComposite(tc.v); err == nil {
			t.Errorf("%s: expected error, got none", tc.name)
		}
	}
}

func TestDecodedNormalization(t *testing.T) {
	c := mustComposite(t, map[string]any{"pid": 1.0, "load": 0.5, "on": true, "gone": nil})
	m := c.Value().(map[string]any)
	if v, ok := m["pid"].(int64); !ok || v != 1 {
		t.Errorf("pid: got %T %v, want int64 1", m["pid"], m["pid"])
	}
	if v, ok := m["load"].(float64); !ok || v != 0.5 {
		t.Errorf("load: got %T %v, want float64 0.5", m["load"], m["load"])
	}
	if v, ok := m["on"].(bool); !ok || !v {
		t.Errorf("on: got %T %v, want true", m["on"], m["on"])
	}
	if v, present := m["gone"]; !present || v != nil {
		t.Errorf("gone: got %v (present=%v), want nil", v, present)
	}
}

func TestDecodedIsACopy(t *testing.T) {
	input := map[string]any{"list": []any{1, 2}}
	c := mustComposite(t, input)
	input["list"].([]any)[0] = 99
	input["new"] = "surprise"
	m := c.Value().(map[string]any)
	if len(m) != 1 {
		t.Errorf("composite gained a key from input mutation: %v", m)
	}
	if v := m["list"].([]any)[0].(int64); v != 1 {
		t.Errorf("composite element changed by input mutation: %v", v)
	}
}

func TestParseComposite(t *testing.T) {
	c, err := ParseComposite([]byte(`{"b": 2, "a": 1.0}`))
	if err != nil {
		t.Fatalf("ParseComposite: %v", err)
	}
	if c.Canonical() != `{"a":1,"b":2}` {
		t.Errorf("got %s", c.Canonical())
	}
}

func TestParseCompositeDuplicateKey(t *testing.T) {
	_, err := ParseComposite([]byte(`{"a": 1, "a": 2}`))
	if err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Errorf("expected duplicate key error, got %v", err)
	}
	// Nested duplicates too.
	_, err = ParseComposite([]byte(`{"outer": {"a": 1, "a": 2}}`))
	if err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Errorf("expected nested duplicate key error, got %v", err)
	}
}

func TestParseCompositeTrailingData(t *testing.T) {
	if _, err := ParseComposite([]byte(`{"a": 1} {"b": 2}`)); err == nil {
		t.Error("expected error for trailing data")
	}
}

func TestParseCompositeLargeInteger(t *testing.T) {
	// json.Number path must keep int64 precision beyond float64's 2^53.
	c, err := ParseComposite([]byte(`[9007199254740993]`))
	if err != nil {
		t.Fatalf("ParseComposite: %v", err)
	}
	if c.Canonical() != "[9007199254740993]" {
		t.Errorf("got %s, want [9007199254740993]", c.Canonical())
	}
}

func TestCompositeMarshalJSON(t *testing.T) {
	c := mustComposite(t, map[string]any{"b": 1, "a": []any{true, nil, "x"}})
	data, err := json.Marshal(c)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if string(data) != `{"a":[true,null,"x"],"b":1}` {
		t.Errorf("got %s", data)
	}
	// Canonical form is itself valid JSON that round-trips.
	var v any
	if err := json.Unmarshal(data, &v); err != nil {
		t.Fatalf("canonical form is not valid JSON: %v", err)
	}
	c2, err := NewComposite(v)
	if err != nil {
		t.Fatalf("re-canonicalize: %v", err)
	}
	if c2.Canonical() != c.Canonical() {
		t.Errorf("round trip changed canonical form: %s vs %s", c2.Canonical(), c.Canonical())
	}
}

func TestCompositeStringEqualsCanonical(t *testing.T) {
	c := mustComposite(t, []any{1, "two"})
	if c.String() != c.Canonical() {
		t.Errorf("String %q != Canonical %q", c.String(), c.Canonical())
	}
}

func TestBoolNullConstants(t *testing.T) {
	if Bool(true).String() != "true" || Bool(false).String() != "false" {
		t.Error("Bool.String mismatch")
	}
	if (Null{}).String() != "null" {
		t.Error("Null.String mismatch")
	}
	// Both must satisfy Constant.
	var _ Constant = Bool(true)
	var _ Constant = Null{}
}

func TestTermJSONCheckConstant(t *testing.T) {
	c := mustComposite(t, []any{1})
	if !TermJSON.CheckConstant(c) {
		t.Error("TermJSON should match *Composite")
	}
	if TermJSON.CheckConstant(String("x")) {
		t.Error("TermJSON should not match String")
	}
	if !TermAny.CheckConstant(c) {
		t.Error("TermAny should match *Composite")
	}
}

// --- normalizeFloat int64 boundary regression tests ---
//
// These mirror internal/interned/dict_test.go / normalize_numeric_test.go's
// coverage of NormalizeNumeric, since normalizeFloat must agree with it bit
// for bit -- a composite's canonical form and a dict-interned scalar must
// never diverge for the same input. The bug: the old guard converted first
// and checked the round-trip (`i := int64(f); float64(i) == f`), but Go's
// float64->int64 conversion is implementation-defined outside int64's range.
// On arm64, FCVTZS saturates an out-of-range float to MaxInt64, and
// float64(MaxInt64) rounds back up to exactly 2^63, so the round-trip guard
// wrongly accepted f == 2^63 as int64 MaxInt64 (9223372036854775807) -- one
// off from the true magnitude of 2^63, and indistinguishable from a genuine
// MaxInt64 composite -- while amd64 rejected the same input and kept it a
// float64. Reachable from the public API via ParseComposite on a JSONL
// number literal like 9223372036854775808.

// TestCanonicalTwoTo63StaysFloat pins the exact input that diverged across
// platforms under the old round-trip-only guard: 2^63 is exactly
// representable as a float64 but one past math.MaxInt64, so it must stay a
// float64 on every platform, never become an int64.
func TestCanonicalTwoTo63StaysFloat(t *testing.T) {
	const twoTo63 = 9223372036854775808.0 // 2^63, exactly representable
	c := mustComposite(t, []any{twoTo63})
	arr := c.Value().([]any)
	f, ok := arr[0].(float64)
	if !ok {
		t.Fatalf("expected 2^63 to stay a float64, got %T(%v)", arr[0], arr[0])
	}
	if f != twoTo63 {
		t.Fatalf("value changed: got %v, want %v", f, twoTo63)
	}
	want := "[" + strconv.FormatFloat(twoTo63, 'g', -1, 64) + "]"
	if c.Canonical() != want {
		t.Errorf("got %s, want %s", c.Canonical(), want)
	}
}

// TestCanonicalTwoTo63DoesNotCollideWithMaxInt64 is the direct regression
// test for the corruption this caused: ParseComposite("[9223372036854775808]")
// must not canonicalize to the same text as the genuine MaxInt64 composite.
// Before the fix, arm64 collapsed the two into one canonical form, silently
// merging two structurally different composites (and joining facts that
// should not have joined).
func TestCanonicalTwoTo63DoesNotCollideWithMaxInt64(t *testing.T) {
	c, err := ParseComposite([]byte("[9223372036854775808]"))
	if err != nil {
		t.Fatalf("ParseComposite: %v", err)
	}
	d := mustComposite(t, []any{int64(9223372036854775807)}) // math.MaxInt64
	if c.Canonical() == d.Canonical() {
		t.Errorf("2^63 and MaxInt64 canonicalize identically: %s", c.Canonical())
	}
	if d.Canonical() != "[9223372036854775807]" {
		t.Errorf("MaxInt64 composite: got %s, want [9223372036854775807]", d.Canonical())
	}
}

// TestCanonicalNegativeTwoTo63IsMinInt64 pins the inclusive lower boundary:
// -2^63 is exactly representable as a float64 AND is itself a valid int64
// (math.MinInt64), so it must normalize to int64, not stay a float64.
func TestCanonicalNegativeTwoTo63IsMinInt64(t *testing.T) {
	const negTwoTo63 = -9223372036854775808.0 // -2^63 == math.MinInt64
	c := mustComposite(t, []any{negTwoTo63})
	arr := c.Value().([]any)
	i, ok := arr[0].(int64)
	if !ok || i != math.MinInt64 {
		t.Fatalf("expected -2^63 to normalize to int64 MinInt64, got %T(%v)", arr[0], arr[0])
	}
	if c.Canonical() != "[-9223372036854775808]" {
		t.Errorf("got %s, want [-9223372036854775808]", c.Canonical())
	}
}

// TestCanonicalMaxInt64AsFloatStaysFloat covers "2^63-1 as a float64": since
// math.MaxInt64 (2^63-1) is NOT exactly representable as a float64, the
// nearest representable value rounds UP to exactly 2^63 -- so as a float64
// it is bit-for-bit indistinguishable from 2^63 and must be treated the
// same way: stays a float64, never becomes int64 MaxInt64. This is the
// exact case that made the old round-trip guard's arm64/amd64 divergence
// reachable in practice.
func TestCanonicalMaxInt64AsFloatStaysFloat(t *testing.T) {
	f := float64(int64(math.MaxInt64))
	if f != 9223372036854775808.0 {
		t.Fatalf("test assumption broken: float64(MaxInt64) = %v, want 2^63", f)
	}
	c := mustComposite(t, []any{f})
	arr := c.Value().([]any)
	if _, ok := arr[0].(float64); !ok {
		t.Fatalf("expected float64(MaxInt64) (== 2^63) to stay a float64, got %T(%v)", arr[0], arr[0])
	}
}

// TestCanonicalMaxInt64AsInt64StaysExact is the control case: MaxInt64 fed
// in as an actual int64 (not routed through normalizeFloat at all) must
// round-trip exactly, unaffected by the float boundary logic above.
func TestCanonicalMaxInt64AsInt64StaysExact(t *testing.T) {
	c := mustComposite(t, []any{int64(math.MaxInt64)})
	if c.Canonical() != "[9223372036854775807]" {
		t.Errorf("got %s, want [9223372036854775807]", c.Canonical())
	}
}

// --- invalid UTF-8 rejection regression tests ---
//
// encodeCanonical used json.Marshal for strings, which silently substitutes
// U+FFFD for invalid UTF-8 bytes. The decoded form (Value()) keeps the raw
// invalid bytes, so two composites with different invalid byte sequences
// canonicalized identically -- violating the documented Canonical() equality
// contract (structurally equal iff canonical forms equal) and letting two
// invalid-UTF-8 map keys collide into one canonical key. The fix rejects
// invalid UTF-8 at normalizeJSON, before it ever reaches the encoder.

func TestNewCompositeRejectsInvalidUTF8Leaf(t *testing.T) {
	if _, err := NewComposite([]any{"\xff"}); err == nil {
		t.Error("expected error for invalid UTF-8 leaf string, got none")
	}
}

func TestNewCompositeDistinctInvalidUTF8LeavesBothRejected(t *testing.T) {
	// \xff and \xfe are distinct invalid byte sequences that both collapsed
	// to the same U+FFFD replacement under the old json.Marshal-based
	// encoder. Both must now be rejected rather than silently coerced to
	// the same canonical form.
	for _, s := range []string{"\xff", "\xfe"} {
		if _, err := NewComposite([]any{s}); err == nil {
			t.Errorf("expected error for invalid UTF-8 string %q, got none", s)
		}
	}
}

func TestNewCompositeRejectsInvalidUTF8MapKey(t *testing.T) {
	if _, err := NewComposite(map[string]any{"\xff": 1}); err == nil {
		t.Error("expected error for invalid UTF-8 object key, got none")
	}
	// Two distinct invalid-UTF-8 keys used to be able to canonicalize with
	// duplicate-looking text; both must be rejected outright.
	if _, err := NewComposite(map[string]any{"\xfe": 1}); err == nil {
		t.Error("expected error for invalid UTF-8 object key, got none")
	}
}

func TestNewCompositeRejectsInvalidUTF8Nested(t *testing.T) {
	if _, err := NewComposite(map[string]any{"outer": []any{"ok", "\xff"}}); err == nil {
		t.Error("expected error for nested invalid UTF-8 string, got none")
	}
}

func TestNewCompositeValidUnicodeStillWorks(t *testing.T) {
	// Sanity check that the UTF-8 validity check doesn't reject legitimate
	// multi-byte runes.
	c := mustComposite(t, []any{"héllo wörld 😀"})
	if c.Canonical() == "" {
		t.Fatal("expected a canonical encoding for valid unicode")
	}
}

// --- nil nested *Composite regression test ---

// TestNewCompositeNilNestedCompositeErrors is the regression test for a
// panic: a nested nil *Composite value used to reach `val.decoded` on a nil
// pointer inside normalizeJSON, unlike every other invalid leaf type, which
// returns an error. It must return a descriptive error instead.
func TestNewCompositeNilNestedCompositeErrors(t *testing.T) {
	var nilComposite *Composite
	if _, err := NewComposite([]any{nilComposite}); err == nil {
		t.Error("expected error for nested nil *Composite, got none")
	}
	if _, err := NewComposite(map[string]any{"x": nilComposite}); err == nil {
		t.Error("expected error for nested nil *Composite as map value, got none")
	}
}
