package interned

import (
	"strings"
	"testing"

	"swdunlop.dev/pkg/datalog"
)

// TestCompileAtomVRejectsOverArityAtom is the regression test for
// CompileAtomV's own bounds guard on CompiledAtom.Arity. HashAndGroundV,
// GroundV, and BoundSet.Set (via BoundArgsV) all write into fixed
// [MaxFactArity] arrays trusting ca.Arity with no bounds check of their
// own; the only thing keeping that safe today is that every current
// caller of CompileAtomV routes through seminaive's Engine.Compile, which
// rejects over-wide atoms first via checkRuleArity. This test calls
// CompileAtomV directly -- bypassing checkRuleArity entirely, as a future
// second compile path inside this package might -- to confirm the
// interned package guards itself rather than depending entirely on that
// distant, external gate.
func TestCompileAtomVRejectsOverArityAtom(t *testing.T) {
	dict := NewDict()
	terms := make([]datalog.Term, MaxFactArity+1)
	for i := range terms {
		terms[i] = datalog.Integer(i)
	}

	_, err := CompileAtomV("wide_pred", terms, dict, nil)
	if err == nil {
		t.Fatalf("expected CompileAtomV to return an error on an atom wider than MaxFactArity")
	}
	if !strings.Contains(err.Error(), "exceeds maximum") {
		t.Fatalf("expected a labeled arity-exceeded error message, got %v", err)
	}
}

// TestCompileAtomVAcceptsMaxArityAtom checks the boundary itself: exactly
// MaxFactArity terms must compile without error (off-by-one guard against a
// fencepost error in the arity check).
func TestCompileAtomVAcceptsMaxArityAtom(t *testing.T) {
	dict := NewDict()
	terms := make([]datalog.Term, MaxFactArity)
	for i := range terms {
		terms[i] = datalog.Integer(i)
	}
	ca, err := CompileAtomV("full_pred", terms, dict, nil)
	if err != nil {
		t.Fatalf("expected CompileAtomV to accept an atom at exactly MaxFactArity, got error: %v", err)
	}
	if ca.Arity != MaxFactArity {
		t.Fatalf("expected arity %d, got %d", MaxFactArity, ca.Arity)
	}
}

// TestCompileAtomRejectsOverArityAtom checks the CompileAtom convenience
// wrapper (used directly by seminaive's aggregate head compilation)
// inherits the same guard as CompileAtomV.
func TestCompileAtomRejectsOverArityAtom(t *testing.T) {
	dict := NewDict()
	terms := make([]datalog.Term, MaxFactArity+1)
	for i := range terms {
		terms[i] = datalog.Integer(i)
	}

	_, err := CompileAtom("wide_pred", terms, dict)
	if err == nil {
		t.Fatalf("expected CompileAtom to return an error on an atom wider than MaxFactArity")
	}
}
