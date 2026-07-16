package datalog

import (
	"fmt"
	"testing"
)

// BenchmarkNewCompositeManyStrings exercises encodeCanonical's string-leaf
// and object-key encoding path heavily: an object with many string-valued
// keys, each canonicalized via writeJSONString (previously json.Marshal)
// once per NewComposite call.
func BenchmarkNewCompositeManyStrings(b *testing.B) {
	obj := make(map[string]any, 200)
	for i := range 200 {
		obj[fmt.Sprintf("key-%d-name", i)] = fmt.Sprintf("some string value number %d with a few words", i)
	}

	b.ResetTimer()
	for b.Loop() {
		c, err := NewComposite(obj)
		if err != nil {
			b.Fatal(err)
		}
		if c.Canonical() == "" {
			b.Fatal("empty canonical form")
		}
	}
}

// BenchmarkNewCompositeEscapeHeavyStrings is the same shape but with strings
// that force the escaping path (quotes, backslashes, control characters) on
// every leaf, rather than the fast unescaped-run path.
func BenchmarkNewCompositeEscapeHeavyStrings(b *testing.B) {
	obj := make(map[string]any, 200)
	for i := range 200 {
		obj[fmt.Sprintf("key-%d", i)] = fmt.Sprintf("val \"%d\" with \\backslash\\ and \ttab and \nnewline and <html> & stuff", i)
	}

	b.ResetTimer()
	for b.Loop() {
		c, err := NewComposite(obj)
		if err != nil {
			b.Fatal(err)
		}
		if c.Canonical() == "" {
			b.Fatal("empty canonical form")
		}
	}
}
