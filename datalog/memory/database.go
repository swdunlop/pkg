package memory

import (
	"iter"

	"swdunlop.dev/pkg/datalog"
	"swdunlop.dev/pkg/datalog/internal/interned"
)

// Database implements datalog.Database with dictionary-encoded in-memory storage.
type Database struct {
	dict  *interned.Dict
	facts interned.InternedFactSet
	decls []datalog.Declaration
}

var _ datalog.Database = (*Database)(nil)

func init() {
	interned.Memory = interned.MemoryHook{
		Unwrap: func(db datalog.Database) (*interned.Dict, interned.InternedFactSet, []datalog.Declaration, bool) {
			mdb, ok := db.(*Database)
			if !ok {
				return nil, interned.InternedFactSet{}, nil, false
			}
			return mdb.dict, mdb.facts, mdb.decls, true
		},
		Wrap: func(dict *interned.Dict, facts interned.InternedFactSet, decls []datalog.Declaration) datalog.Database {
			return &Database{dict: dict, facts: facts, decls: decls}
		},
	}
}

// Declarations iterates through ordered distinct declarations of predicates.
func (db *Database) Declarations() iter.Seq[datalog.Declaration] {
	return func(yield func(datalog.Declaration) bool) {
		for _, d := range db.decls {
			if !yield(d) {
				return
			}
		}
	}
}

// Facts iterates through ordered distinct facts for a given predicate.
func (db *Database) Facts(pred string, arity int) iter.Seq[[]datalog.Constant] {
	predID, ok := db.dict.Has(pred)
	if !ok {
		return func(yield func([]datalog.Constant) bool) {}
	}
	return func(yield func([]datalog.Constant) bool) {
		for _, fact := range db.facts.Get(predID, arity) {
			row := make([]datalog.Constant, arity)
			for i := range arity {
				row[i] = db.dict.ResolveConstant(fact.Values[i])
			}
			if !yield(row) {
				return
			}
		}
	}
}

// Query iterates through facts in the database that match the predicate and terms.
// Constants in terms must match exactly; Variables act as wildcards.
func (db *Database) Query(pred string, terms ...datalog.Term) iter.Seq[[]datalog.Constant] {
	predID, ok := db.dict.Has(pred)
	if !ok {
		return func(yield func([]datalog.Constant) bool) {}
	}
	arity := len(terms)

	// Build bound set from constant positions.
	var bs interned.BoundSet
	for i, t := range terms {
		if c, ok := t.(datalog.Constant); ok {
			cID, has := db.dict.Has(constantToAny(c))
			if !has {
				// Constant not in dict means no facts can match.
				return func(yield func([]datalog.Constant) bool) {}
			}
			bs.Set(i, cID)
		}
	}

	return func(yield func([]datalog.Constant) bool) {
		scan := db.facts.Scan(predID, arity, &bs)
		for i := range scan.Len() {
			fact := scan.Fact(i)
			if !matchFact(fact, terms, &bs) {
				continue
			}
			row := make([]datalog.Constant, arity)
			for j := range arity {
				row[j] = db.dict.ResolveConstant(fact.Values[j])
			}
			if !yield(row) {
				return
			}
		}
	}
}

// matchFact checks whether an InternedFact matches a query pattern.
// The boundSet already filters via the byArg0 index, but non-leading
// constant positions still need manual checking.
func matchFact(fact *interned.InternedFact, terms []datalog.Term, bs *interned.BoundSet) bool {
	for i := range fact.Arity {
		if val, ok := bs.Get(i); ok {
			if fact.Values[i] != val {
				return false
			}
		}
	}
	return true
}

// constantToAny extracts the Go primitive from a typed datalog.Constant.
func constantToAny(c datalog.Constant) any {
	switch v := c.(type) {
	case datalog.Float:
		f := float64(v)
		if i := int64(f); float64(i) == f {
			return i
		}
		return f
	case datalog.Integer:
		return int64(v)
	case datalog.String:
		return string(v)
	case datalog.ID:
		return v
	}
	panic("unknown constant type")
}

// Predicates returns all predicate name/arity pairs that have at least one fact.
func (db *Database) Predicates() iter.Seq2[string, int] {
	return func(yield func(string, int) bool) {
		for key := range db.facts.ByPred {
			pred := db.dict.Resolve(key.Pred).(string)
			if !yield(pred, key.Arity) {
				return
			}
		}
	}
}

// Extend returns a new database containing all facts from db plus the extra facts.
// The original database is not modified.
func (db *Database) Extend(extra ...datalog.Fact) (*Database, error) {
	dict := db.dict.Clone()
	facts := db.facts.Clone()
	for _, f := range extra {
		interned, err := dict.InternFact(f)
		if err != nil {
			return nil, err
		}
		facts.Add(interned)
	}
	decls := make([]datalog.Declaration, len(db.decls))
	copy(decls, db.decls)
	return &Database{dict: dict, facts: facts, decls: decls}, nil
}

// Builder constructs a Database programmatically.
type Builder struct {
	dict  *interned.Dict
	facts interned.InternedFactSet
	decls []datalog.Declaration
}

// NewBuilder creates a Builder for constructing a Database.
func NewBuilder() *Builder {
	return &Builder{
		dict:  interned.NewDict(),
		facts: interned.NewInternedFactSet(),
	}
}

// AddDeclaration adds a predicate declaration to the database.
func (b *Builder) AddDeclaration(d datalog.Declaration) {
	b.decls = append(b.decls, d)
}

// AddFact adds a fact to the database.
func (b *Builder) AddFact(f datalog.Fact) error {
	interned, err := b.dict.InternFact(f)
	if err != nil {
		return err
	}
	b.facts.Add(interned)
	return nil
}

// Build finalizes the database and returns it.
func (b *Builder) Build() *Database {
	return &Database{
		dict:  b.dict,
		facts: b.facts,
		decls: b.decls,
	}
}
