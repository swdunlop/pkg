package datalog

import (
	"encoding/json"
	"math"
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
