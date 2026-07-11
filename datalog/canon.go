package datalog

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
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
		return val, nil
	case map[string]any:
		m := make(map[string]any, len(val))
		for k, elem := range val {
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

func normalizeFloat(f float64) (any, error) {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return nil, fmt.Errorf("composite cannot contain NaN or infinity")
	}
	if i := int64(f); float64(i) == f {
		return i, nil
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
		encoded, _ := json.Marshal(val) // marshaling a string cannot fail
		buf.Write(encoded)
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
			encoded, _ := json.Marshal(k)
			buf.Write(encoded)
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
