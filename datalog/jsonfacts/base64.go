package jsonfacts

import (
	"encoding/base64"
	"strings"
	"unicode/utf16"
)

// base64Variant holds pre-computed base64 search strings for a single pattern.
type base64Variant struct {
	pred            string   // output predicate (e.g. "base64_contains")
	searchStrings   []string // base64 substrings to search for (3 offsets)
	originalPattern string   // plaintext pattern for fact emission
}

// base64Offsets returns the 3 base64-encoded variants of data at byte offsets 0, 1, 2.
// Each variant is the substring that would appear in a base64-encoded stream containing
// the data at the given alignment.
func base64Offsets(data []byte) [3]string {
	var result [3]string
	for offset := 0; offset < 3; offset++ {
		padded := make([]byte, offset+len(data))
		copy(padded[offset:], data)
		encoded := base64.StdEncoding.EncodeToString(padded)

		skip := 0
		switch offset {
		case 1:
			skip = 2
		case 2:
			skip = 3
		}
		encoded = encoded[skip:]
		encoded = strings.TrimRight(encoded, "=")
		result[offset] = encoded
	}
	return result
}

// toUTF16LE encodes a string as UTF-16LE bytes.
func toUTF16LE(s string) []byte {
	runes := []rune(s)
	u16 := utf16.Encode(runes)
	out := make([]byte, len(u16)*2)
	for i, v := range u16 {
		out[i*2] = byte(v)
		out[i*2+1] = byte(v >> 8)
	}
	return out
}

// compileBase64Patterns pre-computes all search variants for a list of patterns.
func compileBase64Patterns(patterns []string, utf16le bool, pred string) []base64Variant {
	variants := make([]base64Variant, len(patterns))
	for i, p := range patterns {
		var data []byte
		if utf16le {
			data = toUTF16LE(p)
		} else {
			data = []byte(p)
		}
		offsets := base64Offsets(data)
		seen := make(map[string]struct{})
		var ss []string
		for _, o := range offsets {
			if _, ok := seen[o]; !ok && o != "" {
				seen[o] = struct{}{}
				ss = append(ss, o)
			}
		}
		variants[i] = base64Variant{
			pred:            pred,
			searchStrings:   ss,
			originalPattern: p,
		}
	}
	return variants
}
