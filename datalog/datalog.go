package datalog

import (
	"context"
	"iter"
)

// Empty is a Database with no declarations, no facts, nothing to query.
type Empty struct{}

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
	Name string `json:"name"` // the name of the term, empty for anonymous terms
	Use  string `json:"use"`  // markdown text explaining the term's meaning.
}
