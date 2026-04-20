package datalog

import (
	"context"
	"fmt"
	"iter"
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

// DeclarationSet indexes declarations by predicate name for type checking.
type DeclarationSet map[string]Declaration

// NewDeclarationSet builds a DeclarationSet from declarations.
func NewDeclarationSet(decls iter.Seq[Declaration]) DeclarationSet {
	ds := DeclarationSet{}
	for d := range decls {
		if _, exists := ds[d.Name]; !exists {
			ds[d.Name] = d
		}
	}
	return ds
}

// CheckFact validates that a fact's terms match the declared types and arity.
// Returns nil if no declaration exists or if all checks pass.
func (ds DeclarationSet) CheckFact(f Fact) error {
	d, ok := ds[f.Name]
	if !ok || len(d.Terms) == 0 {
		return nil
	}
	if len(f.Terms) != len(d.Terms) {
		return fmt.Errorf("predicate %s: expected arity %d, got %d", f.Name, len(d.Terms), len(f.Terms))
	}
	for i, td := range d.Terms {
		if !td.Type.CheckConstant(f.Terms[i]) {
			return fmt.Errorf("predicate %s term %d (%s): expected %s, got %T", f.Name, i, td.Name, td.Type, f.Terms[i])
		}
	}
	return nil
}

// CheckAtom validates that an atom's constant terms match declared types and arity.
// Variables are not checked. Returns nil if no declaration exists.
func (ds DeclarationSet) CheckAtom(pred string, terms []Term) error {
	d, ok := ds[pred]
	if !ok || len(d.Terms) == 0 {
		return nil
	}
	if len(terms) != len(d.Terms) {
		return fmt.Errorf("predicate %s: expected arity %d, got %d", pred, len(d.Terms), len(terms))
	}
	for i, td := range d.Terms {
		if !td.Type.CheckTerm(terms[i]) {
			return fmt.Errorf("predicate %s term %d (%s): expected %s, got %T", pred, i, td.Name, td.Type, terms[i])
		}
	}
	return nil
}
