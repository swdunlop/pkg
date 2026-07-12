package datalog

import "testing"

// FuzzParseComposite checks that ParseComposite never panics on arbitrary
// bytes, and that whenever it succeeds, its canonical encoding is a
// fixpoint: re-parsing the canonical bytes must succeed and produce
// identical canonical bytes (canon(decode(canon(x))) == canon(x)).
func FuzzParseComposite(f *testing.F) {
	seeds := []string{
		// basic shapes
		`{}`,
		`[]`,
		`{"a":1}`,
		`{"a":1.0}`,
		`[1,2,3]`,
		`null`,
		`true`,
		`false`,

		// key ordering / duplicate keys
		`{"zeta":1,"alpha":2,"mid":3}`,
		`{"a":1,"a":2}`,

		// nesting
		`{"a":{"b":{"c":[1,2,{"d":3}]}}}`,
		`[[1,2],[3,4],[{"x":1}]]`,
		`{}`,
		`{"empty_obj":{},"empty_arr":[]}`,

		// numbers: int64 boundary, big ints beyond 2^53
		`{"n":9007199254740993}`,
		`{"n":9223372036854775807}`,
		`{"n":-9223372036854775808}`,
		`{"n":18446744073709551615}`,
		`{"n":9007199254740992}`,

		// floats
		`{"f":1.0}`,
		`{"f":1e300}`,
		`{"f":-0.0}`,
		`{"f":0.1}`,
		`{"f":-1.5e-10}`,
		`{"f":1}`,

		// 1.0 == 1 equivalence pair
		`{"a":1.0}`,
		`{"a":1}`,

		// unicode + escaped strings
		`{"s":"héllo wörld"}`,
		`{"s":"é́"}`,
		`{"s":"line1\nline2\ttab"}`,
		`{"s":"emoji 😀"}`,
		`{"s":"quote\"backslash\\"}`,

		// deep nesting
		`[[[[[[[[[[1]]]]]]]]]]`,

		// malformed / edge inputs that must not panic
		``,
		`{`,
		`[`,
		`{"a":}`,
		`{"a":1,}`,
		`[1,]`,
		`nul`,
		`{"a":NaN}`,
		`{"a":Infinity}`,
		`"just a string"`,
		`123`,
		`-123`,
		`{"a":1}{"b":2}`,
		`  {"a": 1}  `,
		`{"a": "unterminated}`,
		`{" ":1}`,
	}
	for _, s := range seeds {
		f.Add([]byte(s))
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		c, err := ParseComposite(data)
		if err != nil {
			return
		}

		canon1 := c.Canonical()

		c2, err := ParseComposite([]byte(canon1))
		if err != nil {
			t.Fatalf("re-parsing canonical bytes failed: %v\ncanonical: %s\noriginal input: %q", err, canon1, data)
		}
		canon2 := c2.Canonical()

		if canon1 != canon2 {
			t.Fatalf("canonical encoding is not a fixpoint:\n  canon(x)        = %s\n  canon(decode(canon(x))) = %s\noriginal input: %q", canon1, canon2, data)
		}
	})
}
