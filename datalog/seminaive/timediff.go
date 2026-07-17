package seminaive

import (
	"time"

	"swdunlop.dev/pkg/datalog/internal/interned"
)

// TimeDiff is a BuiltinFunc that computes the difference between two timestamps
// in seconds. Timestamps may be RFC3339 strings or numeric epoch values (int64
// or float64 seconds since Unix epoch). The result is an int64 when both inputs
// are whole seconds and the difference is within int64 range, otherwise float64.
//
// The whole-seconds/int64 conversion is delegated to
// interned.NormalizeNumeric rather than a local `diff == float64(int64(diff))`
// round-trip check: Go's float64->int64 conversion is implementation-defined
// for out-of-range values (on arm64, FCVTZS saturates to MaxInt64, and
// float64(MaxInt64) rounds back up to exactly 2^63, so a round-trip-only
// guard would wrongly accept diff == 2^63 as MaxInt64 on arm64 while amd64's
// conversion yields a different, equally wrong, result -- a
// platform-dependent derived fact for the identical input).
// NormalizeNumeric already guards the exact range TimeDiff needs and is the
// single source of truth other callers (Dict interning, JSON number
// canonicalization) already agree with, so reusing it here keeps all three
// in lockstep instead of re-deriving the bound a third time.
func TimeDiff(inputs []any) (any, bool) {
	if len(inputs) != 2 {
		return nil, false
	}
	a, ok := toEpoch(inputs[0])
	if !ok {
		return nil, false
	}
	b, ok := toEpoch(inputs[1])
	if !ok {
		return nil, false
	}
	diff := a - b
	return interned.NormalizeNumeric(diff), true
}

// toEpoch converts a value to a float64 Unix epoch in seconds.
func toEpoch(v any) (float64, bool) {
	switch t := v.(type) {
	case int64:
		return float64(t), true
	case float64:
		return t, true
	case string:
		parsed, err := time.Parse(time.RFC3339, t)
		if err != nil {
			parsed, err = time.Parse(time.RFC3339Nano, t)
			if err != nil {
				return 0, false
			}
		}
		return float64(parsed.Unix()) + float64(parsed.Nanosecond())/1e9, true
	}
	return 0, false
}
