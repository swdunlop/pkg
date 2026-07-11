package datalog

import (
	"fmt"
	"strconv"
)

type Float float64

func (val Float) isaConstant()   {}
func (val Float) isaTerm()       {}
func (val Float) String() string { return strconv.FormatFloat(float64(val), 'g', -1, 64) }

type Integer int64

func (val Integer) isaConstant()   {}
func (val Integer) isaTerm()       {}
func (val Integer) String() string { return strconv.FormatInt(int64(val), 10) }

type String string

func (val String) isaConstant()   {}
func (val String) isaTerm()       {}
func (val String) String() string { return fmt.Sprintf(`%q`, string(val)) }

// ID is a synthetic unique identifier generated during JSONL loading.
// IDs serve as join keys between facts derived from the same input record.
type ID uint64

func (val ID) isaConstant()   {}
func (val ID) isaTerm()       {}
func (val ID) String() string { return "#" + strconv.FormatUint(uint64(val), 10) }

// Bool is a boolean constant. It exists so JSON true/false extracted from a
// [Composite] have a scalar representation distinct from the strings "true"
// and "false".
type Bool bool

func (val Bool) isaConstant() {}
func (val Bool) isaTerm()     {}
func (val Bool) String() string {
	if val {
		return "true"
	}
	return "false"
}

// Null is the JSON null constant. It exists so JSON null extracted from a
// [Composite] has a scalar representation distinct from any string.
type Null struct{}

func (Null) isaConstant()   {}
func (Null) isaTerm()       {}
func (Null) String() string { return "null" }

// Composite is a JSON object or array value, treated as an atomic constant.
// Equality is structural, established at interning time via a canonical
// encoding (sorted object keys, normalized numbers). The engine never looks
// inside a composite; it only compares interned IDs.
//
// Composite is always used by pointer. The struct must never be compared
// with ==; cross-instance equality is a.Canonical() == b.Canonical(), and
// inside the engine it is dictionary ID equality.
//
// Callers must not mutate the value returned by [Composite.Value]: the
// decoded form is shared by every holder of the composite.
type Composite struct {
	canon   string // canonical JSON encoding (sorted keys, normalized numbers)
	decoded any    // map[string]any | []any, normalized
}

func (c *Composite) isaConstant() {}
func (c *Composite) isaTerm()     {}

// Value returns the decoded form of the composite: a map[string]any or []any
// whose nested numbers are normalized (int64 when exact, float64 otherwise).
// Callers must not mutate the result.
func (c *Composite) Value() any { return c.decoded }

// Canonical returns the canonical JSON encoding of the composite. Two
// composites are structurally equal exactly when their canonical forms
// are equal.
func (c *Composite) Canonical() string { return c.canon }

// String returns the canonical JSON encoding, for term printing.
func (c *Composite) String() string { return c.canon }

// MarshalJSON emits the canonical JSON encoding.
func (c *Composite) MarshalJSON() ([]byte, error) { return []byte(c.canon), nil }

type Variable string

func (ref Variable) isaTerm()       {}
func (ref Variable) String() string { return string(ref) }

// Constant is a ground term (non-variable). This interface is sealed;
// the only implementations are [String], [Integer], [Float], [ID],
// [Bool], [Null], and [*Composite].
type Constant interface {
	Term
	isaConstant()
}

// Term represents a value in a Datalog atom — either a [Constant] or a [Variable].
// This interface is sealed; the only implementations are [String], [Integer],
// [Float], [ID], [Bool], [Null], [*Composite], and [Variable].
type Term interface {
	isaTerm()
	String() string // returns a string representation of the term.
}
