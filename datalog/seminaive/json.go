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

// jsonValue converts a normalized JSON sub-value (from a Composite's decoded
// form) into an engine value: scalars pass through, true/false/null become
// the dedicated constants, and nested objects/arrays become composites in
// their own right.
func jsonValue(v any) (any, bool) {
	switch val := v.(type) {
	case nil:
		return datalog.Null{}, true
	case bool:
		return datalog.Bool(val), true
	case string, int64, float64:
		return val, true
	case map[string]any, []any:
		c, err := datalog.NewComposite(val)
		if err != nil {
			return nil, false
		}
		return c, true
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
	tail, err := datalog.NewComposite(arr[from:])
	if err != nil {
		return nil, false
	}
	return tail, true
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
