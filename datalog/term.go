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

type Variable string

func (ref Variable) isaTerm()       {}
func (ref Variable) String() string { return string(ref) }

// Constant is a ground term (non-variable). This interface is sealed;
// the only implementations are [String], [Integer], [Float], and [ID].
type Constant interface {
	Term
	isaConstant()
}

// Term represents a value in a Datalog atom — either a [Constant] or a [Variable].
// This interface is sealed; the only implementations are [String], [Integer],
// [Float], [ID], and [Variable].
type Term interface {
	isaTerm()
	String() string // returns a string representation of the term.
}
