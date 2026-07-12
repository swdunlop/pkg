package seminaive

import (
	"fmt"
	"math"

	"swdunlop.dev/pkg/datalog"
)

// normalizeUserValue converts a value returned by a user-supplied
// ExternalFunc, BuiltinFunc, or MultiBuiltinFunc into one of the types the
// dict natively accepts (see internal/interned.Dict.Intern and
// ResolveConstant): int64, float64, string, datalog.ID, datalog.Bool,
// datalog.Null, or *datalog.Composite.
//
// The dict itself interns any value handed to it without checking its type
// -- Dict.Intern's type switch only runs later, in ResolveConstant, when the
// value is read back out. A user function that hands back a bare Go bool,
// int, or uint therefore succeeds at interning time and only panics (or,
// for int/uint, silently mismatches int64-typed data facts, since they hash
// to a distinct dict entry) much later, far from the callsite that caused
// it. normalizeUserValue is the boundary check that catches this at the
// moment a user value is about to enter the dict, so the error names the
// offending predicate while that context is still available.
//
// datalog.Constant values (the typed wrapper the syntax/rule layer uses)
// are unwrapped to their underlying dict-native representation, in case a
// user function returns one directly instead of a bare Go value -- both
// ExternalFunc and BuiltinFunc are documented to return plain Go values,
// but accepting the Constant form too is a cheap, unsurprising
// accommodation.
func normalizeUserValue(v any) (any, error) {
	switch val := v.(type) {
	// Already dict-native; pass through unchanged.
	case int64, float64, string, datalog.ID, datalog.Bool, datalog.Null, *datalog.Composite:
		return val, nil

	// datalog.Constant wrapper types, unwrapped to their native form.
	case datalog.Integer:
		return int64(val), nil
	case datalog.Float:
		return float64(val), nil
	case datalog.String:
		return string(val), nil

	// Go integer types, widened to int64.
	case int:
		return int64(val), nil
	case int8:
		return int64(val), nil
	case int16:
		return int64(val), nil
	case int32:
		return int64(val), nil
	case uint:
		return uint64ToInt64(uint64(val))
	case uint8:
		return int64(val), nil
	case uint16:
		return int64(val), nil
	case uint32:
		return int64(val), nil
	case uint64:
		return uint64ToInt64(val)

	// Go float32, widened to float64.
	case float32:
		return float64(val), nil

	// Go bool, wrapped as the dict-native Bool constant.
	case bool:
		return datalog.Bool(val), nil

	default:
		return nil, fmt.Errorf("unsupported value type %T", v)
	}
}

// uint64ToInt64 converts a uint64 to int64, erroring if the value overflows
// int64 -- the dict has no native unsigned integer representation, and
// silently wrapping to a negative number would corrupt the value rather
// than just its type.
func uint64ToInt64(v uint64) (any, error) {
	if v > math.MaxInt64 {
		return nil, fmt.Errorf("value %d overflows int64", v)
	}
	return int64(v), nil
}
