package datalog

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"
)

// NewComposite canonicalizes a JSON object or array value into a [Composite]
// constant. The value may be a map[string]any, []any, or a nested mixture of
// those with string, bool, nil, numeric, and *Composite leaves — the shape
// encoding/json produces, plus common Go numeric types.
//
// Numbers are normalized the way the engine's dictionary normalizes scalars:
// a float that is an exact int64 becomes an int64, so {"pid": 1.0} and
// {"pid": 1} canonicalize identically and join. NaN and infinities are
// rejected. The decoded form is rebuilt during normalization, so later
// mutation of the input does not affect the composite.
func NewComposite(v any) (*Composite, error) {
	decoded, err := normalizeJSON(v)
	if err != nil {
		return nil, err
	}
	switch decoded.(type) {
	case map[string]any, []any:
	default:
		return nil, fmt.Errorf("composite must be a JSON object or array, got %T", v)
	}
	var buf strings.Builder
	encodeCanonical(&buf, decoded)
	return &Composite{canon: buf.String(), decoded: decoded}, nil
}

// NewCompositeTrusted builds a [Composite] directly from a value tree the
// caller guarantees is already normalized -- every string valid UTF-8, every
// number already an int64 or a non-NaN/non-Inf float64, every nested
// object/array a plain map[string]any/[]any (no float32, json.Number, or
// *Composite leaves) -- exactly the invariant NewComposite's normalizeJSON
// pass establishes. It skips that pass (and the copy it makes) entirely, so
// violating the invariant produces a Composite whose canonical form silently
// disagrees with what NewComposite would have produced for equivalent JSON.
//
// This exists for callers that already hold a normalized subtree pulled
// straight out of an existing Composite's decoded form -- e.g. seminaive's
// @json_get/@json_each/@json_items builtins extract a map/slice value that
// was already normalized when its parent Composite was built, so re-running
// normalizeJSON's recursive copy-and-validate on every extraction in the
// fixpoint's inner loop is pure waste. v must be a map[string]any or []any,
// as NewComposite requires; NewCompositeTrusted panics otherwise, since by
// construction a scalar leaf of a normalized tree never reaches here (callers
// route scalars through jsonValue's own switch, not this constructor).
func NewCompositeTrusted(v any) *Composite {
	switch v.(type) {
	case map[string]any, []any:
	default:
		panic(fmt.Sprintf("datalog: NewCompositeTrusted requires a map[string]any or []any, got %T", v))
	}
	var buf strings.Builder
	encodeCanonical(&buf, v)
	return &Composite{canon: buf.String(), decoded: v}
}

// ParseComposite decodes JSON text into a [Composite]. Unlike encoding/json,
// it rejects objects with duplicate keys, since silently keeping one of two
// conflicting values would corrupt joins.
func ParseComposite(data []byte) (*Composite, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	v, err := decodeStrict(dec)
	if err != nil {
		return nil, err
	}
	// Ensure no trailing tokens after the value.
	if _, err := dec.Token(); err == nil {
		return nil, fmt.Errorf("unexpected data after JSON value")
	}
	return NewComposite(v)
}

// decodeStrict decodes one JSON value from dec, erroring on duplicate object keys.
func decodeStrict(dec *json.Decoder) (any, error) {
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	return decodeStrictToken(dec, tok)
}

func decodeStrictToken(dec *json.Decoder, tok json.Token) (any, error) {
	switch t := tok.(type) {
	case json.Delim:
		switch t {
		case '{':
			obj := map[string]any{}
			for dec.More() {
				keyTok, err := dec.Token()
				if err != nil {
					return nil, err
				}
				key := keyTok.(string)
				if _, dup := obj[key]; dup {
					return nil, fmt.Errorf("duplicate object key %q", key)
				}
				val, err := decodeStrict(dec)
				if err != nil {
					return nil, err
				}
				obj[key] = val
			}
			if _, err := dec.Token(); err != nil { // consume '}'
				return nil, err
			}
			return obj, nil
		case '[':
			arr := []any{}
			for dec.More() {
				val, err := decodeStrict(dec)
				if err != nil {
					return nil, err
				}
				arr = append(arr, val)
			}
			if _, err := dec.Token(); err != nil { // consume ']'
				return nil, err
			}
			return arr, nil
		}
		return nil, fmt.Errorf("unexpected delimiter %v", t)
	default:
		return tok, nil // string, json.Number, bool, nil
	}
}

// normalizeJSON rebuilds a value with normalized leaves: exact-integer floats
// become int64, float32 widens to float64, json.Number resolves, and nested
// *Composite values contribute their decoded form. Maps and slices are copied.
func normalizeJSON(v any) (any, error) {
	switch val := v.(type) {
	case nil:
		return nil, nil
	case bool:
		return val, nil
	case string:
		if !utf8.ValidString(val) {
			return nil, fmt.Errorf("invalid UTF-8 in string value %q", val)
		}
		return val, nil
	case map[string]any:
		m := make(map[string]any, len(val))
		for k, elem := range val {
			if !utf8.ValidString(k) {
				return nil, fmt.Errorf("invalid UTF-8 in object key %q", k)
			}
			n, err := normalizeJSON(elem)
			if err != nil {
				return nil, fmt.Errorf("key %q: %w", k, err)
			}
			m[k] = n
		}
		return m, nil
	case []any:
		s := make([]any, len(val))
		for i, elem := range val {
			n, err := normalizeJSON(elem)
			if err != nil {
				return nil, fmt.Errorf("index %d: %w", i, err)
			}
			s[i] = n
		}
		return s, nil
	case *Composite:
		if val == nil {
			return nil, fmt.Errorf("composite cannot contain a nil *Composite")
		}
		return val.decoded, nil
	case float64:
		return normalizeFloat(val)
	case float32:
		return normalizeFloat(float64(val))
	case int:
		return int64(val), nil
	case int8:
		return int64(val), nil
	case int16:
		return int64(val), nil
	case int32:
		return int64(val), nil
	case int64:
		return val, nil
	case uint:
		return normalizeUint(uint64(val))
	case uint8:
		return int64(val), nil
	case uint16:
		return int64(val), nil
	case uint32:
		return int64(val), nil
	case uint64:
		return normalizeUint(val)
	case json.Number:
		if i, err := val.Int64(); err == nil {
			return i, nil
		}
		f, err := val.Float64()
		if err != nil {
			return nil, fmt.Errorf("invalid number %q: %w", val.String(), err)
		}
		return normalizeFloat(f)
	default:
		return nil, fmt.Errorf("unsupported value type %T in composite", v)
	}
}

// minInt64AsFloat and maxInt64BoundAsFloat bound the range of float64 values
// normalizeFloat will even attempt to convert to int64. Mirrors
// internal/interned/dict.go's NormalizeNumeric -- see that function's doc
// comment for the full arm64/amd64 rationale. In short: minInt64AsFloat
// (-2^63) is exactly representable and is itself math.MinInt64, so the lower
// bound is inclusive; maxInt64BoundAsFloat (2^63) is also exactly
// representable but is one past math.MaxInt64 (which itself is NOT exactly
// representable as a float64 -- it rounds up to 2^63), so the upper bound is
// exclusive. The range is checked explicitly before ever converting to
// int64, rather than converting first and checking the round-trip:
// float64->int64 conversion is implementation-defined for out-of-range
// values (on arm64, FCVTZS saturates to MaxInt64, and float64(MaxInt64)
// rounds back up to exactly 2^63, so a round-trip-only guard would wrongly
// accept 2^63 as MaxInt64 -- one off from its true magnitude -- while amd64
// rejects the same input). The two normalizers must agree so a composite's
// canonical form and a dict-interned value never diverge on the same input.
const (
	minInt64AsFloat      = -9223372036854775808.0 // -2^63, exactly representable, == math.MinInt64
	maxInt64BoundAsFloat = 9223372036854775808.0  // 2^63, exactly representable, one past math.MaxInt64
)

func normalizeFloat(f float64) (any, error) {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return nil, fmt.Errorf("composite cannot contain NaN or infinity")
	}
	if f >= minInt64AsFloat && f < maxInt64BoundAsFloat {
		if i := int64(f); float64(i) == f {
			return i, nil
		}
	}
	return f, nil
}

func normalizeUint(u uint64) (any, error) {
	if u > math.MaxInt64 {
		return nil, fmt.Errorf("integer %d overflows int64", u)
	}
	return int64(u), nil
}

// encodeCanonical writes the canonical JSON encoding of a normalized value:
// object keys sorted lexicographically, integers in base 10, floats formatted
// as strconv.FormatFloat(f, 'g', -1, 64) — matching Float.String.
func encodeCanonical(buf *strings.Builder, v any) {
	switch val := v.(type) {
	case nil:
		buf.WriteString("null")
	case bool:
		if val {
			buf.WriteString("true")
		} else {
			buf.WriteString("false")
		}
	case int64:
		buf.WriteString(strconv.FormatInt(val, 10))
	case float64:
		buf.WriteString(strconv.FormatFloat(val, 'g', -1, 64))
	case string:
		// writeJSONString cannot fail here: it never errors, but is only
		// byte-identical to json.Marshal (see its doc comment and
		// canon_escape_test.go's differential test) because normalizeJSON
		// already rejects invalid UTF-8 for every string and object key
		// before a value ever reaches encodeCanonical (see normalizeJSON's
		// string and map[string]any cases) -- json.Marshal would otherwise
		// silently substitute U+FFFD for invalid bytes, collapsing distinct
		// decoded strings (e.g. "\xff" and "\xfe") into the same canonical
		// text.
		writeJSONString(buf, val)
	case map[string]any:
		keys := make([]string, 0, len(val))
		for k := range val {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		buf.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				buf.WriteByte(',')
			}
			writeJSONString(buf, k)
			buf.WriteByte(':')
			encodeCanonical(buf, val[k])
		}
		buf.WriteByte('}')
	case []any:
		buf.WriteByte('[')
		for i, elem := range val {
			if i > 0 {
				buf.WriteByte(',')
			}
			encodeCanonical(buf, elem)
		}
		buf.WriteByte(']')
	default:
		// normalizeJSON guarantees this is unreachable.
		panic(fmt.Sprintf("unnormalized value %T in canonical encoding", v))
	}
}

// jsonHexDigits are the lowercase hex digits json.Marshal uses in unicode
// escapes.
const jsonHexDigits = "0123456789abcdef"

// writeUnicodeEscape writes r as a backslash-u escape (4 lowercase hex
// digits), matching json.Marshal's format. Building the 6-byte sequence
// from r's numeric value via jsonHexDigits, rather than a source literal
// containing the escape text itself, sidesteps any risk of that source text
// being transcribed incorrectly.
func writeUnicodeEscape(buf *strings.Builder, r rune) {
	buf.WriteByte(0x5c) // backslash
	buf.WriteByte('u')
	buf.WriteByte(jsonHexDigits[(r>>12)&0xf])
	buf.WriteByte(jsonHexDigits[(r>>8)&0xf])
	buf.WriteByte(jsonHexDigits[(r>>4)&0xf])
	buf.WriteByte(jsonHexDigits[r&0xf])
}

// writeJSONString writes s's JSON string encoding (including the
// surrounding quotes) directly into buf, replacing the
// json.Marshal-per-string-leaf allocation encodeCanonical previously paid
// for every string leaf and object key in a composite -- a real cost in the
// fixpoint's inner loop, where the same composite is re-canonicalized many
// times over.
//
// This must stay byte-for-byte identical to json.Marshal's default output
// (escapeHTML enabled, which is what json.Marshal and json.NewEncoder use
// unless SetEscapeHTML(false) is called -- neither this package nor
// datalog.NewComposite's callers ever disable it) or every existing
// Composite's canonical form -- and therefore its identity, since
// Canonical() equality is the whole contract -- silently changes. It
// mirrors encoding/json's encodeState.string: the quote and backslash bytes
// get a one-byte escape, backspace/form-feed/newline/carriage-return/tab get
// their short escapes, every other byte below 0x20 and the three HTML-unsafe bytes
// (escaped because escapeHTML is on) get a unicode escape, invalid UTF-8
// becomes the unicode escape for the replacement character (matching
// json.Marshal's own substitution -- normalizeJSON rejects invalid UTF-8
// before a string ever reaches here, so this branch is unreached in
// production, but the differential test in canon_escape_test.go exercises
// it directly to confirm this function would still agree with json.Marshal
// if that invariant were ever bypassed), and the line/paragraph separator
// runes get their own unicode escape unconditionally (JSONP safety, applied
// regardless of escapeHTML). See canon_escape_test.go for the fuzz/table
// differential test asserting byte-for-byte equality against json.Marshal
// across adversarial inputs -- do not change this function's escaping
// rules without re-running it.
func writeJSONString(buf *strings.Builder, s string) {
	const lineSeparator = rune(0x2028)
	const paragraphSeparator = rune(0x2029)

	buf.WriteByte('"')
	start := 0
	for i := 0; i < len(s); {
		b := s[i]
		if b < utf8.RuneSelf {
			if jsonSafeASCII(b) {
				i++
				continue
			}
			if start < i {
				buf.WriteString(s[start:i])
			}
			switch b {
			case '"':
				buf.WriteByte(0x5c)
				buf.WriteByte('"')
			case 0x5c: // backslash
				buf.WriteByte(0x5c)
				buf.WriteByte(0x5c)
			case '\b':
				buf.WriteByte(0x5c)
				buf.WriteByte('b')
			case '\f':
				buf.WriteByte(0x5c)
				buf.WriteByte('f')
			case '\n':
				buf.WriteByte(0x5c)
				buf.WriteByte('n')
			case '\r':
				buf.WriteByte(0x5c)
				buf.WriteByte('r')
			case '\t':
				buf.WriteByte(0x5c)
				buf.WriteByte('t')
			default:
				// Below 0x20 (a control character) or one of the three
				// HTML-unsafe ASCII bytes -- see jsonSafeASCII.
				writeUnicodeEscape(buf, rune(b))
			}
			i++
			start = i
			continue
		}
		c, size := utf8.DecodeRuneInString(s[i:])
		if c == utf8.RuneError && size == 1 {
			if start < i {
				buf.WriteString(s[start:i])
			}
			writeUnicodeEscape(buf, utf8.RuneError)
			i += size
			start = i
			continue
		}
		if c == lineSeparator || c == paragraphSeparator {
			if start < i {
				buf.WriteString(s[start:i])
			}
			writeUnicodeEscape(buf, c)
			i += size
			start = i
			continue
		}
		i += size
	}
	if start < len(s) {
		buf.WriteString(s[start:])
	}
	buf.WriteByte('"')
}

// jsonSafeASCII reports whether ASCII byte b can be written to a JSON
// string literal unescaped, under json.Marshal's default (escapeHTML=true)
// rules: every byte below 0x20 needs an escape, and '"', '\\', '<', '>',
// '&' need one regardless of position -- everything else in the ASCII
// range, including 0x7f (DEL), is safe (matches encoding/json's safeSet,
// which does not escape DEL).
func jsonSafeASCII(b byte) bool {
	if b < 0x20 {
		return false
	}
	switch b {
	case '"', '\\', '<', '>', '&':
		return false
	}
	return true
}
