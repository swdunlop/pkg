package interned

import "swdunlop.dev/pkg/datalog"

// MemoryHook provides functions for the seminaive engine to access
// memory.Database internals without exporting them in the public API.
// Registered by memory's init function.
type MemoryHook struct {
	// Unwrap extracts the dictionary, interned facts, and declarations from
	// a memory database. Returns ok=false if db is not a memory database.
	Unwrap func(db datalog.Database) (dict *Dict, facts InternedFactSet, decls []datalog.Declaration, ok bool)

	// Wrap constructs a new memory database from interned internals.
	Wrap func(dict *Dict, facts InternedFactSet, decls []datalog.Declaration) datalog.Database
}

// Memory is registered by the memory package's init function.
var Memory MemoryHook
