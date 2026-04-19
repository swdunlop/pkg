// Package jsonfacts loads JSONL files into a Datalog [memory.Database] according to
// a declarative schema configuration, and encodes query results back to JSONL.
//
// # Overview
//
// Security telemetry, audit logs, and other operational data often arrive as
// directories of JSONL (newline-delimited JSON) files. This package bridges
// that format to Datalog by defining how JSON fields map to predicate arguments.
//
// The pipeline is:
//
//  1. Define a [Config] with sources, matchers, and declarations.
//  2. Call [Config.LoadDir] or [Config.LoadFS] to produce a [memory.Database].
//  3. Feed the database to a Datalog transformer for rule-based analysis.
//  4. Encode results with [Encoder] for downstream consumption.
//
// # Configuration
//
// A [Config] contains three sections:
//
//   - Sources -- map JSONL files to predicates via [Mapping] entries
//   - Matchers -- apply string pattern matching to loaded facts
//   - Declarations -- name predicate terms for structured JSONL output
//
// Configurations are JSON-serializable and can be loaded from a directory of
// schema files using [Config.LoadSchemaDir] or [Config.LoadSchemaFS]. Multiple
// schema files are merged together, allowing modular organization:
//
//	var cfg jsonfacts.Config
//	cfg.LoadSchemaDir("schemas/")    // loads all *.json files and merges
//	db, err := cfg.LoadDir("data/")  // loads JSONL files according to merged config
//
// # Simple Mappings
//
// A simple [Mapping] extracts one fact per JSONL line using expr-lang expressions
// to select each argument:
//
//	{
//	    "predicate": "process",
//	    "args": ["value.pid", "value.name", "value.cmdline"],
//	    "filter": "value.pid != 0"
//	}
//
// The variable "value" refers to the parsed JSON object for the current record.
// The optional "filter" expression must evaluate to true for the fact to be emitted.
//
// # Imperative Mappings
//
// When a single JSON line should produce multiple facts (e.g., one-to-many
// relationships), use an imperative [Mapping] with an expr program:
//
//	{
//	    "expr": "let id = fresh_id(); assert(\"email\", [id, value.sender, value.time]); map(value.recipients, assert(\"email_to\", [id, #]))"
//	}
//
// Available functions in imperative mode:
//   - fresh_id() -- generates a synthetic [datalog.ID] for joining related facts
//   - assert(pred, args) -- emits a fact with the given predicate and argument array
//   - match_contains(pred, key, haystack, patterns) -- emits pred(key, pattern) for each matching substring
//   - match_starts_with, match_ends_with, match_regex -- analogous string matchers
//
// # Matchers
//
// A [Matcher] scans facts of a named predicate after loading and emits derived
// match facts. This is useful for detecting known-bad patterns in command lines,
// file paths, URLs, or other string fields without writing Datalog rules:
//
//	{
//	    "predicate": "process",
//	    "term": 2,
//	    "contains": ["certutil", "bitsadmin", "Invoke-WebRequest"],
//	    "case_insensitive": true,
//	    "windash": true
//	}
//
// This matcher scans the third term (index 2) of each process/3 fact and emits
// ci_wd_contains(value, pattern) facts for each match. The predicate name is
// built from modifier prefixes: "ci_" for case-insensitive, "wd_" for windash.
//
// Match types:
//   - contains / contains_from -- substring matching
//   - starts_with / starts_with_from -- prefix matching
//   - ends_with / ends_with_from -- suffix matching
//   - regex_match / regex_match_from -- regular expression matching
//   - base64 / base64_from -- base64-encoded substring detection (all 3 alignment offsets)
//   - base64_utf16le / base64_utf16le_from -- base64 with UTF-16LE encoding (for PowerShell -EncodedCommand)
//   - cidr / cidr_from -- IP address membership in CIDR networks
//
// The "_from" variants load patterns from an external file (one pattern per line,
// # comments supported), making the configuration self-contained after loading.
//
// Matchers use a compiled regex "gate" to skip facts that cannot possibly match
// any pattern, avoiding per-pattern overhead on non-matching strings.
//
// # Declarations
//
// [datalog.Declaration] entries name the terms of each predicate so the [Encoder]
// can produce human-readable JSONL output with named fields:
//
//	{
//	    "declarations": [{
//	        "name": "process",
//	        "terms": [{"name": "pid"}, {"name": "name"}, {"name": "cmdline"}]
//	    }]
//	}
//
// Without declarations, the encoder uses positional indices ("0", "1", ...) as keys.
//
// # Encoder
//
// [NewEncoder] creates an [Encoder] that writes facts as JSONL. Each fact is
// encoded as a single-key JSON object where the key is the predicate name:
//
//	{"process": {"pid": 1234, "name": "cmd.exe", "cmdline": "cmd /c whoami"}}
package jsonfacts
