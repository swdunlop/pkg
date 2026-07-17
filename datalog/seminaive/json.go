package seminaive

import (
	"sort"

	"swdunlop.dev/pkg/datalog"
)

// registerJSONBuiltins installs the composite destructuring builtins. They are
// registered by every engine (not opt-in) because destructuring patterns in
// rule text desugar to them, so they must always exist. All of them only
// produce subterms (or size-bounded derivatives) of their inputs, preserving
// Datalog's finite-universe termination guarantee.
func registerJSONBuiltins(e *Engine) {
	WithBuiltin("@json_get", jsonGet)(e)
	WithBuiltin("@json_len", jsonLen)(e)
	WithBuiltin("@json_type", jsonType)(e)
	WithBuiltin("@json_slice", jsonSlice)(e)
	WithMultiBuiltin("@json_each", 1, jsonEach)(e)
	WithMultiBuiltin("@json_items", 2, jsonItems)(e)
}

// builtinBuiltinArity is the authoritative expected total-arity table
// (inputs + output(s)) for the always-registered JSON destructuring
// builtins, each of which hardcodes its own `len(inputs) != N` runtime
// check (see jsonGet et al.) that a wrong-arity call silently fails --
// e.g. @json_get(X, V) with one input instead of two just returns (nil,
// false) forever, indistinguishable from "key not found". Driven by
// checkBodyBuiltins (engine.go) so a wrong-arity call to one of these
// fixed-shape builtins fails at Compile instead of compiling clean and
// never firing. Keyed by the atom's full arity (arg count in rule text,
// matching len(a.Terms)), the same unit constraintBuiltinArity and
// checkRuleArity use -- not the BuiltinFunc's inputs-only count -- so one
// table entry works for both single-output builtins (registered via
// WithBuiltin, whose atom arity is len(inputs)+1) and multi-output builtins
// (registered via WithMultiBuiltin, whose atom arity is len(inputs)+outputs).
//
// User-registered builtins (WithBuiltin/WithMultiBuiltin outside this file)
// have no declared arity anywhere in the Option API -- only multiBuiltin.outputs
// is known, never the input count -- so they cannot be arity-checked at
// compile time by this table; checkBodyBuiltins only validates the names
// this package itself defines.
var builtinBuiltinArity = map[string]int{
	"@json_get":   3, // Obj/Arr, Key/Idx, V
	"@json_len":   2, // ArrOrObj, N
	"@json_type":  2, // V, T
	"@json_slice": 3, // Arr, From, T
	"@json_each":  2, // Arr, Elem
	"@json_items": 3, // Obj, K, V
}

// jsonValue converts a normalized JSON sub-value (from a Composite's decoded
// form) into an engine value: scalars pass through, true/false/null become
// the dedicated constants, and nested objects/arrays become composites in
// their own right.
//
// The map[string]any/[]any case uses datalog.NewCompositeTrusted rather than
// NewComposite: v is always a subtree pulled straight out of an existing
// Composite's already-normalized decoded form (every caller reaches jsonValue
// with a value read from a *datalog.Composite's Value()), so re-running
// NewComposite's normalizeJSON copy-and-validate pass on it is redundant work
// paid on every extraction in the fixpoint's inner loop. NewCompositeTrusted
// only re-derives the canonical encoding, which it cannot avoid producing.
func jsonValue(v any) (any, bool) {
	switch val := v.(type) {
	case nil:
		return datalog.Null{}, true
	case bool:
		return datalog.Bool(val), true
	case string, int64, float64:
		return val, true
	case map[string]any, []any:
		return datalog.NewCompositeTrusted(val), true
	}
	return nil, false
}

// jsonGet implements @json_get(Obj, Key, V) / @json_get(Arr, Idx, V).
// Fails on missing key, out-of-range index, or non-composite input.
func jsonGet(inputs []any) (any, bool) {
	if len(inputs) != 2 {
		return nil, false
	}
	c, ok := inputs[0].(*datalog.Composite)
	if !ok {
		return nil, false
	}
	switch container := c.Value().(type) {
	case map[string]any:
		key, ok := inputs[1].(string)
		if !ok {
			return nil, false
		}
		v, present := container[key]
		if !present {
			return nil, false
		}
		return jsonValue(v)
	case []any:
		idx, ok := inputs[1].(int64)
		if !ok || idx < 0 || idx >= int64(len(container)) {
			return nil, false
		}
		return jsonValue(container[idx])
	}
	return nil, false
}

// jsonLen implements @json_len(ArrOrObj, N).
func jsonLen(inputs []any) (any, bool) {
	if len(inputs) != 1 {
		return nil, false
	}
	c, ok := inputs[0].(*datalog.Composite)
	if !ok {
		return nil, false
	}
	switch container := c.Value().(type) {
	case map[string]any:
		return int64(len(container)), true
	case []any:
		return int64(len(container)), true
	}
	return nil, false
}

// jsonType implements @json_type(V, T), classifying any engine value as
// "object", "array", "string", "integer", "float", "bool", "null", or "id".
func jsonType(inputs []any) (any, bool) {
	if len(inputs) != 1 {
		return nil, false
	}
	switch v := inputs[0].(type) {
	case *datalog.Composite:
		switch v.Value().(type) {
		case map[string]any:
			return "object", true
		case []any:
			return "array", true
		}
		return nil, false
	case string:
		return "string", true
	case int64:
		return "integer", true
	case float64:
		return "float", true
	case datalog.Bool:
		return "bool", true
	case datalog.Null:
		return "null", true
	case datalog.ID:
		return "id", true
	}
	return nil, false
}

// jsonSlice implements @json_slice(Arr, From, T): the suffix of Arr starting
// at index From. Slices strictly shrink, so list-tail recursion terminates.
func jsonSlice(inputs []any) (any, bool) {
	if len(inputs) != 2 {
		return nil, false
	}
	c, ok := inputs[0].(*datalog.Composite)
	if !ok {
		return nil, false
	}
	arr, ok := c.Value().([]any)
	if !ok {
		return nil, false
	}
	from, ok := inputs[1].(int64)
	if !ok || from < 0 || from > int64(len(arr)) {
		return nil, false
	}
	// arr is already the normalized decoded form of c; arr[from:] reuses its
	// (already-normalized) elements under a new slice header, so this is the
	// same already-normalized-subtree case jsonValue documents.
	return datalog.NewCompositeTrusted(arr[from:]), true
}

// jsonEach implements @json_each(Arr, Elem), yielding each array element.
func jsonEach(inputs []any, yield func(outputs []any) bool) {
	if len(inputs) != 1 {
		return
	}
	c, ok := inputs[0].(*datalog.Composite)
	if !ok {
		return
	}
	arr, ok := c.Value().([]any)
	if !ok {
		return
	}
	for _, elem := range arr {
		v, ok := jsonValue(elem)
		if !ok {
			continue
		}
		if !yield([]any{v}) {
			return
		}
	}
}

// jsonItems implements @json_items(Obj, K, V), yielding each key/value pair
// in sorted key order.
func jsonItems(inputs []any, yield func(outputs []any) bool) {
	if len(inputs) != 1 {
		return
	}
	c, ok := inputs[0].(*datalog.Composite)
	if !ok {
		return
	}
	obj, ok := c.Value().(map[string]any)
	if !ok {
		return
	}
	keys := make([]string, 0, len(obj))
	for k := range obj {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		v, ok := jsonValue(obj[k])
		if !ok {
			continue
		}
		if !yield([]any{k, v}) {
			return
		}
	}
}
