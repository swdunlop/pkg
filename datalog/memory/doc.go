// Package memory implements [datalog.Database] with dictionary-encoded in-memory storage.
//
// All constants (strings, integers, floats, IDs) are interned into a dictionary
// that maps each unique value to a compact uint64 identifier. Facts are stored as
// tuples of these identifiers, enabling fast equality checks and low memory overhead
// for datasets with many repeated values.
//
// # Building a Database
//
// Use [NewBuilder] to construct a database programmatically:
//
//	b := memory.NewBuilder()
//	b.AddDeclaration(datalog.Declaration{
//	    Name: "parent",
//	    Terms: []datalog.TermDeclaration{{Name: "parent"}, {Name: "child"}},
//	})
//	b.AddFact(datalog.Fact{Name: "parent", Terms: []datalog.Constant{
//	    datalog.String("tom"), datalog.String("bob"),
//	}})
//	db := b.Build()
//
// In most applications, databases are constructed by the jsonfacts package rather
// than by hand.
//
// # Querying
//
// The [Database.Query] method matches facts using a pattern of constants and variables.
// Constants must match exactly; variables act as wildcards:
//
//	for row := range db.Query("parent", datalog.String("tom"), datalog.Variable("Child")) {
//	    fmt.Println(row[1]) // prints each child of tom
//	}
//
// Use [Database.Facts] to iterate all facts for a predicate without filtering.
//
// # Extending
//
// [Database.Extend] creates a new database containing the original facts plus
// additional ones, without modifying the original. This is used by the REPL to
// layer interactively-entered facts on top of data loaded from JSONL.
package memory
