package interned

import (
	"cmp"
	"fmt"
	"maps"
	"math"
	"slices"

	"swdunlop.dev/pkg/datalog"
)

// Dict maps values to sequential uint64 IDs (dictionary encoding / string interning).
// Built in a collection pass, then frozen (sorted) for evaluation. New values
// from is-expressions can be appended dynamically after freeze.
//
// The zero value is not usable; create with NewDict.
type Dict struct {
	values []any          // id -> value (O(1) by index)
	index  map[any]uint64 // value -> id (O(1) lookup)
	frozen bool           // true after Freeze; appended values won't be sorted
}

// NewDict creates an empty dictionary.
func NewDict() *Dict {
	return &Dict{
		index: make(map[any]uint64),
	}
}

// compositeKey is the comparable index key for a *datalog.Composite: its
// canonical encoding. It is a distinct type so a composite can never collide
// with a String term whose value happens to be JSON text.
type compositeKey string

// nanKey is the comparable index key for float64 NaN. NaN != NaN under Go's
// == (the map key comparison the index relies on), so every distinct NaN
// payload would otherwise miss the index and mint a fresh ID -- equal-
// looking terms with different identities, plus a dead index entry per
// call. All NaN payloads canonicalize to this single zero-field struct
// value, which does compare equal to itself, so repeated NaN interning
// hits the index like any other value. NaN is not producible by the
// Datalog source language itself (the lexer never produces a "nan" float
// token, and `is` division-by-zero is guarded to fail rather than produce
// NaN -- see applyBinOp/applyBinOpFloat in seminaive), but it can still
// enter the dict through the Go API (a caller building a
// datalog.Float(math.NaN()) fact by hand) or a user builtin/external
// returning math.NaN() via InternUser, so the guard sits at this entry
// point rather than relying on every caller to avoid NaN. Once interned,
// NaN is one value with one ID: joins and "=" treat it as equal to itself
// (interned identity), while ordering comparisons treat it as unordered
// (see seminaive's compareValues).
type nanKey struct{}

// indexKey converts a value to the comparable key it is indexed under.
// Composites are not comparable, so they index under their canonical
// encoding; NaN never equals itself, so it indexes under a fixed sentinel;
// everything else indexes as itself.
func indexKey(v any) any {
	switch c := v.(type) {
	case *datalog.Composite:
		return compositeKey(c.Canonical())
	case float64:
		if math.IsNaN(c) {
			return nanKey{}
		}
	}
	return v
}

// Intern returns the ID for a value, assigning a new one if needed.
// Integer-valued float64 values are normalized to int64 so that
// JSON-loaded numbers (always float64) match Datalog integer literals.
// *datalog.Composite values are hash-consed by canonical encoding. All NaN
// payloads collapse to one interned value (see nanKey).
//
// Intern trusts v to be one of the dict-native types (int64, float64,
// string, datalog.ID, datalog.Bool, datalog.Null, *datalog.Composite); a
// value of any other type is stored as-is and only fails much later, when
// it is read back out. Values produced by user code must enter through
// InternUser, which validates and converts at the boundary.
func (d *Dict) Intern(v any) uint64 {
	v = NormalizeNumeric(v)
	key := indexKey(v)
	if id, ok := d.index[key]; ok {
		return id
	}
	id := uint64(len(d.values))
	d.values = append(d.values, v)
	d.index[key] = id
	return id
}

// InternConstant interns a typed datalog.Constant, extracting its Go
// primitive value via ConstantToAny -- the one switch over Constant kinds.
func (d *Dict) InternConstant(c datalog.Constant) uint64 {
	return d.Intern(ConstantToAny(c))
}

// ConstantToAny extracts the Go primitive from a typed datalog.Constant,
// without normalizing numeric types. Callers that feed the result into
// Dict.Intern or Dict.Has don't need to normalize first -- both already
// call NormalizeNumeric internally. Callers doing their own comparisons or
// arithmetic on the result (e.g. seminaive's compareValues/applyBinOp) must
// handle int64/float64 mixing explicitly rather than relying on this
// function to collapse them.
func ConstantToAny(c datalog.Constant) any {
	switch v := c.(type) {
	case datalog.Float:
		return float64(v)
	case datalog.Integer:
		return int64(v)
	case datalog.String:
		return string(v)
	case datalog.ID:
		return v
	case datalog.Bool:
		return v
	case datalog.Null:
		return v
	case *datalog.Composite:
		return v
	}
	panic("unknown constant type")
}

// minInt64AsFloat and maxInt64BoundAsFloat bound the range of float64
// values NormalizeNumeric will even attempt to convert to int64.
// minInt64AsFloat (-2^63) is exactly representable as a float64 and is
// itself a valid int64 (math.MinInt64), so the lower bound is inclusive.
// maxInt64BoundAsFloat (2^63) is also exactly representable as a float64,
// but 2^63 itself overflows int64 (math.MaxInt64 is 2^63-1, which is NOT
// exactly representable as a float64 -- the nearest representable value
// rounds up to 2^63), so the upper bound is exclusive.
const (
	minInt64AsFloat      = -9223372036854775808.0 // -2^63, exactly representable, == math.MinInt64
	maxInt64BoundAsFloat = 9223372036854775808.0  // 2^63, exactly representable, one past math.MaxInt64
)

// NormalizeNumeric converts float64 values that represent exact integers
// to int64, ensuring JSON numbers and Datalog integer literals intern
// identically.
//
// The range is checked explicitly before ever converting to int64, rather
// than converting first and checking the round-trip: Go's float64->int64
// conversion is implementation-defined for out-of-range values, and at
// least one real platform disagrees with amd64 about the result. On
// arm64, FCVTZS saturates an out-of-range float to MaxInt64, and
// float64(math.MaxInt64) rounds back up to exactly 2^63 -- so a
// round-trip-only guard (float64(int64(f)) == f) would accept f == 2^63
// and mint it as int64 math.MaxInt64 (9223372036854775807), a value one
// off from the true magnitude and indistinguishable from a genuine
// MaxInt64 fact, while amd64 would reject the same input. Reachable from
// the public API via a JSONL number literal like 9223372036854775808
// (strconv.ParseInt fails, jsonfacts falls back to json.Number.Float64,
// which returns exactly 2^63). The explicit pre-check never performs the
// implementation-defined conversion for any out-of-range float, so the
// result is identical on every platform: 2^63 (and anything >=) stays a
// float64, never becomes an int64.
func NormalizeNumeric(v any) any {
	if f, ok := v.(float64); ok {
		if f >= minInt64AsFloat && f < maxInt64BoundAsFloat {
			i := int64(f)
			if float64(i) == f {
				return i
			}
		}
	}
	return v
}

// Resolve returns the value for an ID. Panics if id is out of range.
func (d *Dict) Resolve(id uint64) any {
	return d.values[id]
}

// ResolveConstant resolves a dict ID to a typed datalog.Constant.
// A *datalog.Composite is returned as-is, so repeated resolution yields
// pointer-identical results within one dict.
func (d *Dict) ResolveConstant(id uint64) datalog.Constant {
	switch v := d.values[id].(type) {
	case float64:
		return datalog.Float(v)
	case int64:
		return datalog.Integer(v)
	case string:
		return datalog.String(v)
	case datalog.ID:
		return v
	case datalog.Bool:
		return v
	case datalog.Null:
		return v
	case *datalog.Composite:
		return v
	}
	panic("unknown value type in dict")
}

// Len returns the number of interned values.
func (d *Dict) Len() int {
	return len(d.values)
}

// Has reports whether a value is already interned.
func (d *Dict) Has(v any) (uint64, bool) {
	id, ok := d.index[indexKey(NormalizeNumeric(v))]
	return id, ok
}

// Freeze sorts the dictionary by value and reassigns IDs to preserve
// value ordering. Must be called after all EDB values are collected and
// before any facts are stored with these IDs. Returns a remap table
// (old ID -> new ID) for callers that buffered old IDs.
//
// Type ordering: float64 < int64 < string (consistent but arbitrary
// for cross-type; compareValues rejects cross-type < / >).
func (d *Dict) Freeze() []uint64 {
	n := len(d.values)
	if n == 0 {
		d.frozen = true
		return nil
	}

	// Build sortable entries.
	type entry struct {
		value any
		oldID uint64
	}
	entries := make([]entry, n)
	for i, v := range d.values {
		entries[i] = entry{v, uint64(i)}
	}

	slices.SortFunc(entries, func(a, b entry) int {
		return dictCompare(a.value, b.value)
	})

	// Rebuild values slice and remap table.
	remap := make([]uint64, n)
	for newID, e := range entries {
		d.values[newID] = e.value
		d.index[indexKey(e.value)] = uint64(newID)
		remap[e.oldID] = uint64(newID)
	}

	d.frozen = true
	return remap
}

// dictCompare orders values for sorted dictionary construction.
// Type order: float64 (0) < int64 (1) < string (2) < ID (3) < composite (4)
// < bool (5) < null (6).
func dictCompare(a, b any) int {
	ta, tb := typeOrder(a), typeOrder(b)
	if ta != tb {
		return cmp.Compare(ta, tb)
	}
	switch va := a.(type) {
	case float64:
		return cmp.Compare(va, b.(float64))
	case int64:
		return cmp.Compare(va, b.(int64))
	case string:
		return cmp.Compare(va, b.(string))
	case datalog.ID:
		return cmp.Compare(va, b.(datalog.ID))
	case *datalog.Composite:
		return cmp.Compare(va.Canonical(), b.(*datalog.Composite).Canonical())
	case datalog.Bool:
		vb := b.(datalog.Bool)
		switch {
		case va == vb:
			return 0
		case !bool(va):
			return -1
		default:
			return 1
		}
	}
	return 0
}

// Clone returns a deep copy of the dictionary. The clone preserves frozen state
// and all interned values, so facts from the original dict remain valid.
func (d *Dict) Clone() *Dict {
	values := make([]any, len(d.values))
	copy(values, d.values)
	index := make(map[any]uint64, len(d.index))
	maps.Copy(index, d.index)
	return &Dict{
		values: values,
		index:  index,
		frozen: d.frozen,
	}
}

func typeOrder(v any) int {
	switch v.(type) {
	case float64:
		return 0
	case int64:
		return 1
	case string:
		return 2
	case datalog.ID:
		return 3
	case *datalog.Composite:
		return 4
	case datalog.Bool:
		return 5
	case datalog.Null:
		return 6
	default:
		return 7
	}
}

// InternFact converts a datalog.Fact to an InternedFact.
func (d *Dict) InternFact(fact datalog.Fact) (InternedFact, error) {
	var f InternedFact
	if len(fact.Terms) > MaxFactArity {
		return f, fmt.Errorf("fact %s has arity %d, exceeds maximum %d", fact.Name, len(fact.Terms), MaxFactArity)
	}
	f.Pred = d.Intern(fact.Name)
	f.Arity = len(fact.Terms)
	for i, c := range fact.Terms {
		f.Values[i] = d.InternConstant(c)
	}
	return f, nil
}

// DeInternFact converts an InternedFact back to a datalog.Fact.
func (d *Dict) DeInternFact(f InternedFact) datalog.Fact {
	terms := make([]datalog.Constant, f.Arity)
	for i := range f.Arity {
		terms[i] = d.ResolveConstant(f.Values[i])
	}
	return datalog.Fact{
		Name:  d.Resolve(f.Pred).(string),
		Terms: terms,
	}
}
