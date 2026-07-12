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
// the data at the given alignment, trimmed to only the characters whose 6 bits are fully
// determined by data itself.
//
// Base64 packs 3 input bytes into 4 output characters (6 bits per character). When data
// is embedded at a leading offset of 1 or 2 unknown bytes, the first 1-2 output characters
// straddle the boundary between the unknown prefix and data, so their bits are partly
// determined by bytes we don't know: those characters must be skipped (skip=2 for offset 1,
// skip=3 for offset 2). Symmetrically, at the tail end, the base64 group containing the
// last byte(s) of data may also include unknown trailing bytes when (offset+len(data)) is
// not a multiple of 3 bytes; the final character of that group is then partly determined by
// those unknown trailing bytes and must also be trimmed, not just '=' padding. The count of
// fully-determined characters is floor((offset+len(data))*4/3): that many characters, from
// the start of the encoding, depend only on bytes up through offset+len(data), so slicing
// to that index (after also applying the leading skip) yields exactly the substring that
// data alone determines, regardless of what precedes or follows it in the real stream.
func base64Offsets(data []byte) [3]string {
	var result [3]string
	for offset := 0; offset < 3; offset++ {
		padded := make([]byte, offset+len(data))
		copy(padded[offset:], data)
		encoded := base64.StdEncoding.EncodeToString(padded)
		encoded = strings.TrimRight(encoded, "=")

		skip := 0
		switch offset {
		case 1:
			skip = 2
		case 2:
			skip = 3
		}
		end := (offset + len(data)) * 4 / 3
		if end > len(encoded) {
			end = len(encoded)
		}
		if end < skip {
			end = skip
		}
		result[offset] = encoded[skip:end]
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
