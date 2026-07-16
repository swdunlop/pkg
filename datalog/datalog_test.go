package datalog

import "testing"

// --- DeclarationSet keyed by (name, arity) regression tests ---
//
// DeclarationSet used to be keyed by predicate name alone, while the fact
// store itself keys by name+arity (see internal/interned.PredArityI).
// Declaring both p/1 and p/2 meant the second declaration silently
// overwrote (NewDeclarationSet's first-wins rule: silently lost to) the
// first in the map, so whichever arity survived was enforced against facts
// of the OTHER arity too -- rejecting genuinely valid records. Keying by
// (name, arity) lets both declarations coexist and each is checked
// independently.

// TestDeclarationSetDistinctAritiesBothSurvive is the direct regression test
// for the collision: declaring p/1 and p/2 must not drop either one, and a
// fact must be checked against ITS OWN arity's declaration, not whichever
// one happened to survive under the old name-only key.
func TestDeclarationSetDistinctAritiesBothSurvive(t *testing.T) {
	ds := NewDeclarationSet(func(yield func(Declaration) bool) {
		if !yield(Declaration{
			Name:  "p",
			Terms: []TermDeclaration{{Name: "a", Type: TermString}},
		}) {
			return
		}
		yield(Declaration{
			Name: "p",
			Terms: []TermDeclaration{
				{Name: "a", Type: TermString},
				{Name: "b", Type: TermInteger},
			},
		})
	})

	// p/1 with a valid String term must pass against the p/1 declaration.
	if err := ds.CheckFact(Fact{Name: "p", Terms: []Constant{String("x")}}); err != nil {
		t.Errorf("p/1: expected no error, got: %v", err)
	}
	// p/2 with valid String+Integer terms must pass against the p/2
	// declaration -- before the fix, this either lost its declaration
	// entirely or was checked against p/1's schema and rejected.
	if err := ds.CheckFact(Fact{Name: "p", Terms: []Constant{String("x"), Integer(5)}}); err != nil {
		t.Errorf("p/2: expected no error, got: %v", err)
	}
	// p/2 with a type violation in its own second term must still be caught.
	if err := ds.CheckFact(Fact{Name: "p", Terms: []Constant{String("x"), String("not an int")}}); err == nil {
		t.Error("p/2: expected type mismatch error for string in integer position")
	}
	// p/3 matches neither declared arity: reported as an arity mismatch,
	// not silently passed.
	if err := ds.CheckFact(Fact{Name: "p", Terms: []Constant{String("x"), Integer(5), Integer(6)}}); err == nil {
		t.Error("p/3: expected arity mismatch error")
	}
}

// TestDeclarationSetCheckAtomDistinctArities mirrors the CheckFact case for
// CheckAtom, including a Variable term (which must pass type checks
// unconditionally) at the position that differs between the two arities.
func TestDeclarationSetCheckAtomDistinctArities(t *testing.T) {
	ds := NewDeclarationSet(func(yield func(Declaration) bool) {
		if !yield(Declaration{Name: "q", Terms: []TermDeclaration{{Name: "a", Type: TermInteger}}}) {
			return
		}
		yield(Declaration{Name: "q", Terms: []TermDeclaration{
			{Name: "a", Type: TermInteger},
			{Name: "b", Type: TermString},
		}})
	})

	if err := ds.CheckAtom("q", []Term{Integer(1)}); err != nil {
		t.Errorf("q/1: expected no error, got: %v", err)
	}
	if err := ds.CheckAtom("q", []Term{Integer(1), Variable("X")}); err != nil {
		t.Errorf("q/2 with variable: expected no error, got: %v", err)
	}
	if err := ds.CheckAtom("q", []Term{Integer(1), Integer(2)}); err == nil {
		t.Error("q/2: expected type mismatch for integer in string position")
	}
}

// TestDeclarationSetArityMismatchAcrossDeclaredArities checks that a fact
// whose arity doesn't match ANY declared arity for its name still produces
// an arity-mismatch error, rather than silently passing as "undeclared".
// This preserves the pre-existing arity-checking behavior for the common
// case of a single declared arity per predicate.
func TestDeclarationSetArityMismatchAcrossDeclaredArities(t *testing.T) {
	ds := NewDeclarationSet(func(yield func(Declaration) bool) {
		yield(Declaration{
			Name: "event",
			Terms: []TermDeclaration{
				{Name: "id", Type: TermString},
				{Name: "severity", Type: TermInteger},
			},
		})
	})

	err := ds.CheckFact(Fact{Name: "event", Terms: []Constant{String("evt1")}})
	if err == nil {
		t.Error("expected arity mismatch error for event/1 when only event/2 is declared")
	}
}

// TestDeclarationSetUndeclaredNamePasses checks that a predicate name with
// no declaration at any arity is left entirely unchecked.
func TestDeclarationSetUndeclaredNamePasses(t *testing.T) {
	ds := NewDeclarationSet(func(yield func(Declaration) bool) {
		yield(Declaration{Name: "known", Terms: []TermDeclaration{{Name: "a", Type: TermString}}})
	})
	err := ds.CheckFact(Fact{Name: "unknown", Terms: []Constant{String("anything")}})
	if err != nil {
		t.Errorf("expected no error for undeclared predicate, got: %v", err)
	}
}

// TestDeclarationSetZeroTermsDeclaresArityZeroOnly documents (and pins) the
// resolution to the "zero Terms disables all checking" conflation: a
// Declaration with nil/empty Terms registers as the schema for arity 0 --
// not as a wildcard "no schema, any arity" marker for its name. Facts at
// other arities of the same name remain unchecked only if there is no
// declaration for them specifically.
func TestDeclarationSetZeroTermsDeclaresArityZeroOnly(t *testing.T) {
	ds := NewDeclarationSet(func(yield func(Declaration) bool) {
		yield(Declaration{Name: "flag"}) // nil Terms: schema for flag/0
	})

	// flag/0 matches the declared (empty) schema: nothing to check, passes.
	if err := ds.CheckFact(Fact{Name: "flag"}); err != nil {
		t.Errorf("flag/0: expected no error, got: %v", err)
	}
	// flag/2 has no declaration of its own; the only declaration for "flag"
	// is at arity 0, so this is an arity mismatch, not a silent pass.
	if err := ds.CheckFact(Fact{Name: "flag", Terms: []Constant{Integer(1), Integer(2)}}); err == nil {
		t.Error("flag/2: expected arity mismatch error (only flag/0 is declared)")
	}
}
