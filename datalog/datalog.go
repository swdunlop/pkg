package datalog

import (
	"context"
	"fmt"
	"iter"
	"sort"
)

// Empty is a Database with no declarations, no facts, nothing to query.
type Empty struct{}

// Predicates implements [Database] by yielding nothing.
func (Empty) Predicates() iter.Seq2[string, int] {
	return func(yield func(string, int) bool) {}
}

// Declarations implements [Database] by declaring nothing.
func (Empty) Declarations() iter.Seq[Declaration] {
	return func(yield func(Declaration) bool) {}
}

// Facts implements [Database] by yielding nothing.
func (e Empty) Facts(pred string, arity int) iter.Seq[[]Constant] {
	return func(yield func([]Constant) bool) {}
}

// Query implements [Database] by matching nothing.
func (e Empty) Query(pred string, terms ...Term) iter.Seq[[]Constant] {
	return func(yield func([]Constant) bool) {}
}

var _ Database = Empty{}

// A Database contains facts organized by predicates that may be queried by presenting terms.
type Database interface {
	// Predicates iterates through all predicate name/arity pairs that have at least one fact.
	Predicates() iter.Seq2[string, int]

	// Declarations iterates through ordered distinct declarations of predicates.
	Declarations() iter.Seq[Declaration]

	// Facts iterates through ordered distinct facts for a given predicate.
	Facts(pred string, arity int) iter.Seq[[]Constant]

	// Query iterates through facts in the database that match the predicate and terms.
	Query(pred string, terms ...Term) iter.Seq[[]Constant]
}

// A Transformer derives a new database from an existing one.
type Transformer interface {
	// Declaration returns the expanded / refined set of declarations.
	Declarations(ctx context.Context, input Database) iter.Seq[Declaration]

	// Transform derives information from the input database to produce an output database.
	Transform(ctx context.Context, input Database) (output Database, err error)
}

// A Fact is a predicated relationship of constants.
type Fact struct {
	Name  string     `json:"name"`
	Terms []Constant `json:"terms"`
}

// A Declaration describes a predicate by naming its terms.
type Declaration struct {
	Name  string            `json:"name"`  // the name of the predicate
	Use   string            `json:"use"`   // markdown text explaining how the predicate is used
	Terms []TermDeclaration `json:"terms"` // the defined name of the terms for the predicate
}

// TermDeclaration describes the name and use of a term in a declaration.
type TermDeclaration struct {
	Name string   `json:"name"`           // the name of the term, empty for anonymous terms
	Use  string   `json:"use"`            // markdown text explaining the term's meaning.
	Type TermType `json:"type,omitempty"` // optional type constraint: "string", "integer", "float", "id", or "" (any)
}

// TermType constrains the expected type of a term in a declaration.
type TermType string

const (
	TermAny     TermType = ""        // matches any constant (default)
	TermString  TermType = "string"  // matches String
	TermInteger TermType = "integer" // matches Integer
	TermFloat   TermType = "float"   // matches Float
	TermID      TermType = "id"      // matches ID
	TermJSON    TermType = "json"    // matches *Composite
)

// CheckConstant reports whether a constant matches the declared type.
// Returns true when typ is empty (any).
func (typ TermType) CheckConstant(c Constant) bool {
	switch typ {
	case TermAny:
		return true
	case TermString:
		_, ok := c.(String)
		return ok
	case TermInteger:
		_, ok := c.(Integer)
		return ok
	case TermFloat:
		_, ok := c.(Float)
		return ok
	case TermID:
		_, ok := c.(ID)
		return ok
	case TermJSON:
		_, ok := c.(*Composite)
		return ok
	}
	return false
}

// CheckTerm reports whether a term (if it is a constant) matches the declared type.
// Variables always pass since their types are not statically known.
func (typ TermType) CheckTerm(t Term) bool {
	if typ == TermAny {
		return true
	}
	c, ok := t.(Constant)
	if !ok {
		return true // variables pass
	}
	return typ.CheckConstant(c)
}

// declKey indexes a DeclarationSet by predicate name AND arity, matching how
// the fact store itself is keyed (see internal/interned.PredArityI). Keying
// by name alone would let a second declaration for the same name but a
// different arity (e.g. p/1 then p/2) silently overwrite -- or, with the
// first-wins rule below, silently lose to -- the first one, so the loader
// would reject valid records under the dropped arity and CheckAtom would
// fail compilation for rules that legitimately use it.
type declKey struct {
	Name  string
	Arity int
}

// DeclarationSet indexes declarations by predicate name AND arity (see
// [declKey]) for type checking. A predicate may have a distinct declaration
// per arity it is used at; declaring name/1 does not affect checking for
// name/2, and vice versa, and both are independently enforced.
//
// A (name, arity) pair with no matching declaration but with a declaration
// for the same name at a DIFFERENT arity is an arity mismatch (CheckFact and
// CheckAtom report it as such). A name with no declaration at any arity is
// simply unchecked. This is also the only "no schema" state: a
// Declaration{Name: "p"} with nil/empty Terms registers as the schema for
// p/0 (an empty term list, i.e. arity 0) like any other declaration -- it
// does not exempt every arity of p from checking. A caller that wants "p
// exists but I don't want its arity checked" (e.g. seminaive's rule-head
// bookkeeping, which registers a bare Declaration{Name: r.Head.Pred} purely
// so the predicate shows up in Database.Declarations() for schema
// display/encoding -- see seminaive/transformer.go) must not feed that
// declaration into a DeclarationSet used for CheckFact/CheckAtom: doing so
// would flag every non-zero arity of the rule as a mismatch.
type DeclarationSet map[declKey]Declaration

// NewDeclarationSet builds a DeclarationSet from declarations, keyed by
// (name, arity). The first declaration seen for a given (name, arity) wins;
// later ones with the same name and arity are ignored. Declarations for the
// same name at different arities are independent and both kept.
func NewDeclarationSet(decls iter.Seq[Declaration]) DeclarationSet {
	ds := DeclarationSet{}
	for d := range decls {
		k := declKey{d.Name, len(d.Terms)}
		if _, exists := ds[k]; !exists {
			ds[k] = d
		}
	}
	return ds
}

// declaredArities returns the sorted, distinct arities that have a
// declaration under name, or nil if name has none. Used only to build a
// helpful arity-mismatch message when an exact (name, arity) lookup misses
// but the name is declared under some other arity -- see [DeclarationSet].
func (ds DeclarationSet) declaredArities(name string) []int {
	var arities []int
	for k := range ds {
		if k.Name == name {
			arities = append(arities, k.Arity)
		}
	}
	sort.Ints(arities)
	return arities
}

// CheckFact validates that a fact's terms match the declared types and
// arity. Returns nil if no declaration exists anywhere for f.Name, or if a
// declaration exists for f's exact (name, arity) and all its checks pass.
// Returns an arity-mismatch error if f.Name is declared but only at other
// arities; see [DeclarationSet].
func (ds DeclarationSet) CheckFact(f Fact) error {
	d, ok := ds[declKey{f.Name, len(f.Terms)}]
	if !ok {
		if arities := ds.declaredArities(f.Name); len(arities) > 0 {
			return fmt.Errorf("predicate %s: expected arity %v, got %d", f.Name, arities, len(f.Terms))
		}
		return nil
	}
	for i, td := range d.Terms {
		if !td.Type.CheckConstant(f.Terms[i]) {
			return fmt.Errorf("predicate %s term %d (%s): expected %s, got %T", f.Name, i, td.Name, td.Type, f.Terms[i])
		}
	}
	return nil
}

// CheckAtom validates that an atom's constant terms match declared types and
// arity. Variables are not checked. Returns nil if no declaration exists
// anywhere for pred, or if a declaration exists for the exact (pred,
// len(terms)) and all its checks pass. Returns an arity-mismatch error if
// pred is declared but only at other arities; see [DeclarationSet].
func (ds DeclarationSet) CheckAtom(pred string, terms []Term) error {
	d, ok := ds[declKey{pred, len(terms)}]
	if !ok {
		if arities := ds.declaredArities(pred); len(arities) > 0 {
			return fmt.Errorf("predicate %s: expected arity %v, got %d", pred, arities, len(terms))
		}
		return nil
	}
	for i, td := range d.Terms {
		if !td.Type.CheckTerm(terms[i]) {
			return fmt.Errorf("predicate %s term %d (%s): expected %s, got %T", pred, i, td.Name, td.Type, terms[i])
		}
	}
	return nil
}
