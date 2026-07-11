// Package datalog defines the core interfaces and types for a Datalog query engine.
//
// Datalog is a declarative logic programming language used for deductive databases.
// This package provides the foundation for loading facts, applying inference rules,
// and querying derived results. It is designed for embedding in Go applications that
// process structured data -- particularly streams of security telemetry in JSONL format.
//
// # Core Interfaces
//
// The two central interfaces are [Database] and [Transformer]:
//
//   - A [Database] holds a set of ground facts organized by predicate. It supports
//     iteration over declarations, facts, and pattern-matched queries.
//   - A [Transformer] takes an input [Database] and produces an output [Database]
//     by applying Datalog rules to derive new facts.
//
// # Term Types
//
// Datalog operates on terms, which are either constants or variables:
//
//   - [String] -- a string literal (e.g., "alice", "/bin/sh")
//   - [Integer] -- a 64-bit integer (e.g., 42, 1024)
//   - [Float] -- a 64-bit float (e.g., 3.14)
//   - [ID] -- a synthetic unique identifier generated during JSONL loading,
//     used as a join key between facts derived from the same input record
//   - [Bool], [Null] -- JSON true/false/null extracted from composites
//   - [Composite] -- a JSON object or array treated as one atomic constant
//   - [Variable] -- a logic variable used in rules and queries (e.g., X, Owner)
//
// Constants implement the [Constant] interface; both constants and variables
// implement the [Term] interface.
//
// # Composite Terms
//
// A [Composite] is a JSON object or array stored as a single constant. It is
// canonicalized at construction — object keys sorted, numbers normalized so
// 1.0 and 1 are the same value — and hash-consed at interning time, so two
// structurally equal JSON values are one term and join on equality no matter
// where they were loaded from. The engine never looks inside a composite:
// unification, negation, and count aggregates all compare identities.
// There is no structural unification and no variables inside stored terms.
//
// Rules reach inside composites with destructuring patterns (see the syntax
// package) or the @json_* builtins registered by the seminaive engine:
//
//	suspicious(P) :- process(P, {name: Name, ppid: 4}), @ends_with(Name, ".tmp.exe").
//
// Matching is open: {name: N} matches any object that has a "name" key,
// whatever else it carries. A missing key is not an error; the candidate
// simply fails to match. Order comparisons (< <= > >=), sum/min/max, and
// string builtins fail on composites; = and != compare identities.
//
// Termination: Datalog's guarantee of a finite term universe depends on
// builtin outputs being subterms, or size-bounded derivatives, of their
// inputs. The destructuring builtins (@json_get, @json_slice, @json_each,
// @json_items) satisfy this — they only produce values reachable inside
// loaded facts, and slices strictly shrink, so list-tail recursion
// terminates. Growing constructors (merging, setting keys, patterns in rule
// heads) would break the guarantee and are not provided; patterns are
// rejected in rule heads and under negation at parse time.
//
// Memory: the interning dictionary retains every distinct composite (both
// its canonical string and decoded form) for the lifetime of the database.
// Prefer the flatten-and-retain idiom described in the jsonfacts package:
// flatten hot fields into scalar predicates for indexed joins, and assert
// each raw record once for provenance. Callers must not mutate the value
// returned by [Composite.Value]; it is shared by every holder of the term.
//
// # Facts and Declarations
//
// A [Fact] is a ground predicate instance: a predicate name with a tuple of constants.
// For example, the fact parent("tom", "bob") asserts that tom is a parent of bob.
//
// A [Declaration] describes a predicate's schema by naming its terms, enabling
// structured JSONL output via the jsonfacts [Encoder].
//
// # Typical Usage
//
// A typical application follows this pipeline:
//
//  1. Load facts from JSONL files using a jsonfacts [Config]:
//
//     var cfg jsonfacts.Config
//     cfg.LoadSchemaDir("schemas")
//     db, err := cfg.LoadDir("data")
//
//  2. Parse and compile Datalog rules:
//
//     tr, err := syntax.Parse(seminaive.New(), rules)
//
//  3. Apply the rules to derive new facts:
//
//     output, err := tr.Transform(ctx, db)
//
//  4. Query results:
//
//     for row := range output.Query("verdict", datalog.Variable("File"), datalog.Variable("Risk")) {
//         fmt.Println(row[0], row[1])
//     }
//
// # Subpackages
//
//   - [swdunlop.dev/pkg/datalog/syntax] -- Datalog parser and abstract syntax tree
//   - [swdunlop.dev/pkg/datalog/memory] -- dictionary-encoded in-memory database
//   - [swdunlop.dev/pkg/datalog/seminaive] -- semi-naive evaluation engine
//   - [swdunlop.dev/pkg/datalog/jsonfacts] -- JSONL schema loading and encoding
package datalog
