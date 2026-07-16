package datalog

import (
	"encoding/json"
	"math/rand"
	"strings"
	"testing"
)

// referenceJSONString returns json.Marshal's encoding of s as a string,
// using the same default (escapeHTML enabled) settings writeJSONString must
// match. json.Marshal on a Go string can only fail for a handful of value
// types (channels, funcs, cyclic structures, unsupported numeric kinds); a
// string is never one of them, so the error is deliberately ignored here.
func referenceJSONString(t *testing.T, s string) string {
	t.Helper()
	b, err := json.Marshal(s)
	if err != nil {
		t.Fatalf("json.Marshal(%q) unexpectedly failed: %v", s, err)
	}
	return string(b)
}

// gotJSONString runs writeJSONString the same way encodeCanonical does: into
// a fresh strings.Builder, returning the accumulated text.
func gotJSONString(s string) string {
	var buf strings.Builder
	writeJSONString(&buf, s)
	return buf.String()
}

func assertByteIdentical(t *testing.T, s string) {
	t.Helper()
	want := referenceJSONString(t, s)
	got := gotJSONString(s)
	if got != want {
		t.Fatalf("writeJSONString diverges from json.Marshal for input %q:\n  json.Marshal = %q\n  writeJSONString = %q", s, want, got)
	}
}

// TestWriteJSONStringMatchesMarshalTable is a table of adversarial inputs --
// quotes, backslashes, every control character, the three HTML-sensitive
// bytes, DEL, high Unicode (including astral-plane code points requiring a
// UTF-16 surrogate pair in json.Marshal's own escape path for control
// characters, though not for plain high-codepoint text, which json.Marshal
// emits as raw UTF-8), invalid UTF-8 sequences of several shapes, and the
// U+2028/U+2029 line/paragraph separators -- asserting writeJSONString's
// output is byte-for-byte identical to json.Marshal's for every one of them.
// This is the correctness gate the task calls out explicitly: if this test
// can't be made to pass, writeJSONString must not replace json.Marshal in
// encodeCanonical, since any divergence silently changes existing canonical
// forms and breaks fact identity.
func TestWriteJSONStringMatchesMarshalTable(t *testing.T) {
	cases := []string{
		"",
		"plain ascii",
		`quote " inside`,
		`backslash \ inside`,
		"both \" and \\ together",
		"tab\there",
		"newline\nhere",
		"carriage\rreturn",
		"null\x00byte",
		"bell\x07char",
		"escape\x1bchar",
		"del\x7fchar",
		"<script>alert(1)</script>",
		"a&b",
		"a<b>c&d",
		"unicode: héllo wörld",
		"emoji: \U0001F600 \U0001F4A9",
		"astral plane: \U00010000 \U0010FFFF",
		"line sep:   end",
		"para sep:   end",
		"both seps:   ",
		"mixed: < & >\"\\\t\n\r",
		"� literal replacement char",
		// Every control character 0x00-0x1F, individually.
		func() string {
			var b strings.Builder
			for c := 0; c < 0x20; c++ {
				b.WriteByte(byte(c))
			}
			return b.String()
		}(),
		// Invalid UTF-8: lone continuation byte, truncated multi-byte
		// sequence, overlong encoding, and a byte with no valid meaning in
		// UTF-8 at all.
		"lone continuation: \x80 end",
		"truncated 2-byte: \xc2 end",
		"truncated 3-byte: \xe0\xa0 end",
		"truncated 4-byte: \xf0\x90\x80 end",
		"overlong: \xc0\xaf end",
		"invalid byte: \xff end",
		"invalid byte: \xfe end",
		"surrogate half encoded raw: \xed\xa0\x80 end",
		"mixed valid/invalid: h\xffi\xfej",
		strings.Repeat("\xff", 8),
	}
	for _, s := range cases {
		s := s
		t.Run("", func(t *testing.T) {
			assertByteIdentical(t, s)
		})
	}
}

// TestWriteJSONStringMatchesMarshalFuzz randomly generates strings over a
// byte alphabet weighted toward the escape-relevant bytes (control
// characters, quote, backslash, HTML-sensitive bytes, high bytes that form
// both valid and invalid UTF-8) and checks byte-for-byte agreement with
// json.Marshal for each. Deterministic seed for reproducibility.
func TestWriteJSONStringMatchesMarshalFuzz(t *testing.T) {
	alphabet := []byte{
		0x00, 0x01, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x1b, 0x1f,
		0x20, 0x21, 0x22, 0x26, 0x27, 0x2f,
		0x3c, 0x3d, 0x3e,
		0x41, 0x61, 0x7a, 0x7e, 0x7f,
		0x80, 0x81, 0xc0, 0xc2, 0xdf, 0xe0, 0xed, 0xef,
		0xf0, 0xf4, 0xf5, 0xfe, 0xff,
		0x5c, // backslash
	}
	// Also seed some multi-byte valid UTF-8 sequences (including U+2028,
	// U+2029, U+FFFD, and an astral-plane rune) so the fuzz corpus isn't
	// entirely single-byte.
	validSeqs := []string{
		string(rune(0x2028)),
		string(rune(0x2029)),
		string(rune(0xFFFD)),
		string(rune(0x1F600)),
		string(rune(0x00E9)), // é
		string(rune(0x4E2D)), // 中
	}

	rng := rand.New(rand.NewSource(20260716))
	for i := range 2000 {
		n := rng.Intn(24)
		var b strings.Builder
		for range n {
			if rng.Intn(4) == 0 {
				b.WriteString(validSeqs[rng.Intn(len(validSeqs))])
			} else {
				b.WriteByte(alphabet[rng.Intn(len(alphabet))])
			}
		}
		s := b.String()
		want := referenceJSONString(t, s)
		got := gotJSONString(s)
		if got != want {
			t.Fatalf("fuzz case %d: writeJSONString diverges from json.Marshal for input %q:\n  json.Marshal = %q\n  writeJSONString = %q", i, s, want, got)
		}
	}
}
