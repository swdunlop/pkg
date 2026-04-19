package seminaive

import (
	"time"
)

// TimeDiff is a BuiltinFunc that computes the difference between two timestamps
// in seconds. Timestamps may be RFC3339 strings or numeric epoch values (int64
// or float64 seconds since Unix epoch). The result is an int64 when both inputs
// are whole seconds, otherwise float64.
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
	if diff == float64(int64(diff)) {
		return int64(diff), true
	}
	return diff, true
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
