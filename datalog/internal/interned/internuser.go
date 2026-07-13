package interned

import (
	"fmt"
	"math"

	"swdunlop.dev/pkg/datalog"
)

// InternUser validates and interns a value produced by user code (an
// ExternalFunc, BuiltinFunc, or MultiBuiltinFunc), converting it to one of
// the types the dict natively stores: int64, float64, string, datalog.ID,
// datalog.Bool, datalog.Null, or *datalog.Composite.
//
// This is the designated entry point for values the engine does not
// control. Dict.Intern trusts its input -- its type switch only runs later,
// in ResolveConstant, when the value is read back out -- so a user function
// handing back a bare Go bool, int, or uint would succeed at interning time
// and only panic (or, for int/uint, silently mismatch int64-typed data
// facts, since they hash to a distinct dict entry) much later, far from the
// callsite that caused it. Every ingress of user-produced values must go
// through InternUser so the error is raised at the boundary, while the
// predicate that produced the value is still known to the caller.
//
// datalog.Constant values (the typed wrapper the syntax/rule layer uses)
// are unwrapped via ConstantToAny, in case a user function returns one
// directly instead of a bare Go value; Go's other numeric types widen to
// int64/float64, with uint64 overflow rejected rather than wrapped.
func (d *Dict) InternUser(v any) (uint64, error) {
	switch val := v.(type) {
	// Already dict-native; intern as-is. (datalog.ID, Bool, Null, and
	// *Composite are also Constants, but need no unwrapping.)
	case int64, float64, string, datalog.ID, datalog.Bool, datalog.Null, *datalog.Composite:
		return d.Intern(val), nil

	// Go integer types, widened to int64.
	case int:
		return d.Intern(int64(val)), nil
	case int8:
		return d.Intern(int64(val)), nil
	case int16:
		return d.Intern(int64(val)), nil
	case int32:
		return d.Intern(int64(val)), nil
	case uint:
		return d.internUint64(uint64(val))
	case uint8:
		return d.Intern(int64(val)), nil
	case uint16:
		return d.Intern(int64(val)), nil
	case uint32:
		return d.Intern(int64(val)), nil
	case uint64:
		return d.internUint64(val)

	// Go float32, widened to float64.
	case float32:
		return d.Intern(float64(val)), nil

	// Go bool, wrapped as the dict-native Bool constant.
	case bool:
		return d.Intern(datalog.Bool(val)), nil

	default:
		// Any remaining Constant (Integer, Float, String, or a future
		// addition) unwraps through the one canonical switch.
		if c, ok := v.(datalog.Constant); ok {
			return d.Intern(ConstantToAny(c)), nil
		}
		return 0, fmt.Errorf("unsupported value type %T", v)
	}
}

// internUint64 interns a uint64 as int64, erroring if the value overflows --
// the dict has no native unsigned integer representation, and silently
// wrapping to a negative number would corrupt the value rather than just
// its type.
func (d *Dict) internUint64(v uint64) (uint64, error) {
	if v > math.MaxInt64 {
		return 0, fmt.Errorf("value %d overflows int64", v)
	}
	return d.Intern(int64(v)), nil
}
