package memory

import (
	"iter"
	"sync"

	"swdunlop.dev/pkg/datalog"
	"swdunlop.dev/pkg/datalog/internal/interned"
)

// Database implements datalog.Database with dictionary-encoded in-memory storage.
type Database struct {
	dict  *interned.Dict
	facts interned.InternedFactSet
	decls []datalog.Declaration

	// scanMu guards facts.Scan, which lazily builds column indexes and
	// therefore mutates the fact set. Query is a public API and may be
	// called concurrently; the results a Scan returns are append-only
	// slices, so only the Scan call itself needs the lock.
	scanMu sync.Mutex
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
	if arity > interned.MaxFactArity {
		// No fact can have this many terms (interned.MaxFactArity is the
		// hard limit), so a query this wide can never match anything.
		return func(yield func([]datalog.Constant) bool) {}
	}

	// Build bound set from constant positions.
	var bs interned.BoundSet
	for i, t := range terms {
		if c, ok := t.(datalog.Constant); ok {
			cID, has := db.dict.Has(interned.ConstantToAny(c))
			if !has {
				// Constant not in dict means no facts can match.
				return func(yield func([]datalog.Constant) bool) {}
			}
			bs.Set(i, cID)
		}
	}

	return func(yield func([]datalog.Constant) bool) {
		db.scanMu.Lock()
		scan := db.facts.Scan(predID, arity, &bs)
		db.scanMu.Unlock()
		for i := range scan.Len() {
			fact := scan.Fact(i)
			if !interned.MatchesBound(&bs, fact) {
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

// FactCount returns the number of facts stored for one predicate/arity
// pair. It is O(1) — a fact-slice length via the underlying InternedFactSet,
// not a scan — unlike counting by ranging over Facts, which additionally
// resolves every term of every row just to be discarded by the counter.
// Returns 0 for a predicate/arity pair with no facts, whether or not the
// predicate name is known to the database at all.
func (db *Database) FactCount(pred string, arity int) int {
	predID, ok := db.dict.Has(pred)
	if !ok {
		return 0
	}
	return len(db.facts.Get(predID, arity))
}

// PredArity names one predicate/arity pair, the key PredicateCounts iterates
// over — predicates may be overloaded by arity, so a bare name is not a
// unique key on its own.
type PredArity struct {
	Name  string
	Arity int
}

// PredicateCounts iterates every predicate/arity pair that has at least one
// fact (the same set Predicates enumerates) paired with its fact count.
// Each count is O(1) (the same fact-slice length FactCount uses), so the
// whole iteration is O(#predicates), not O(#facts) — the fix for
// mcp.go's list_predicates and sample_facts, which previously ranged over
// every fact of every predicate under the session's lock just to count
// them (doc/features/mcp-server.md review item 7).
func (db *Database) PredicateCounts() iter.Seq2[PredArity, int] {
	return func(yield func(PredArity, int) bool) {
		for key := range db.facts.ByPred {
			pred := db.dict.Resolve(key.Pred).(string)
			n := len(db.facts.Get(key.Pred, key.Arity))
			if !yield(PredArity{Name: pred, Arity: key.Arity}, n) {
				return
			}
		}
	}
}

// TotalFactCount returns the total number of facts across every predicate
// in db, in O(#predicates) (summing each predicate's O(1) count) rather
// than O(#facts) with every term of every row resolved and discarded.
func (db *Database) TotalFactCount() int {
	total := 0
	for _, n := range db.PredicateCounts() {
		total += n
	}
	return total
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
