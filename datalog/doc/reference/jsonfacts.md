# JSON Fact Extraction Reference

This manual describes the `jsonfacts` layer: how JSONL files are mapped
into Datalog facts by a declarative schema, how matchers derive
pattern-match facts at load time, and how query results are encoded back
to JSONL. It is independent of the rule language itself, which is
documented in the [Datalog Language Reference](datalog.md).

The pipeline:

```
schema (sources, matchers, declarations)
        │
JSONL files ──→ mappings ──→ facts ──→ matchers ──→ match facts
        │                                   │
        └──────────── memory database ◄─────┘
                            │
                    Datalog rules / queries
                            │
                    Encoder ──→ JSONL output
```

## 1. Schema files

A schema is a single configuration with three sections, all optional:

```yaml
sources:        # map JSONL files to predicates
  - file: processes.jsonl
    mappings:
      - predicate: process
        args: ["value.pid", "value.name", "value.cmdline"]

matchers:       # scan loaded facts for patterns
  - predicate: process
    term: 2
    contains: [certutil, bitsadmin]

declarations:   # name predicate terms for output and documentation
  - name: process
    use: "A running process observed on a host."
    terms:
      - {name: pid, use: "Process ID"}
      - {name: name}
      - {name: cmdline}
```

### 1.1 Formats

- The `jsonfacts` package itself reads **JSON** schemas.
  `LoadSchemaDir`/`LoadSchemaFS` load every `*.json` file in a directory
  (non-recursive, lexical filename order) and merge them.
- The `datalog` CLI additionally accepts **YAML** for the `-c` schema
  file, chosen by extension (`.yaml`/`.yml`); YAML documents use exactly
  the same keys as the JSON form.

### 1.2 Merging

Merging multiple schema files is pure concatenation: sources, matchers,
and declarations from each file are appended in file order. There is no
conflict detection or deduplication; two files may contribute mappings for
the same predicate. This supports organizing schemas by data source or
concern (e.g. `processes.json`, `network.json`, `declarations.json`).

### 1.3 Validation

Schema load validates structure:

- every mapping must have either `expr` or `predicate` (and `args`);
  supplying both `expr` and any of `predicate`/`args`/`filter` is an
  error — the two mapping styles are mutually exclusive;
- every matcher must name a predicate, have `term >= 0`, and carry at
  least one pattern list or `_from` file;
- inline `regex_match` patterns must compile and inline `cidr` patterns
  must parse.

Expressions (`args`, `filter`, `expr`) are **not** compiled at schema-load
time; a misspelled identifier surfaces as an error when the data is
loaded.

## 2. Sources and mappings

Each source names one JSONL file and a list of mappings applied to every
line of that file:

| Key | Type | Meaning |
|-----|------|---------|
| `file` | string | Path to a JSONL file, relative to the data directory. |
| `mappings` | list | Mappings, each run against every line. |

A source file that cannot be opened is a load error. Every mapping in a
source runs against every line, so one line may yield facts for several
predicates.

Field extraction and filtering use [expr-lang](https://expr-lang.org/)
expressions. The expression environment is deliberately minimal: the
single variable `value` (the decoded JSON of the current line), the
imperative functions listed in §2.2, and expr-lang's own builtins
(`map`, `filter`, `len`, `type`, …). There is no access to the
filesystem, environment, or any other ambient state, and unknown
identifiers are compile errors.

### 2.1 Simple mappings

```json
{
    "predicate": "process",
    "args": ["value.pid", "value.name", "value.cmdline"],
    "filter": "value.pid != 0"
}
```

| Key | Type | Meaning |
|-----|------|---------|
| `predicate` | string | Predicate to assert. |
| `args` | list of expressions | One expression per term, evaluated against `value`. |
| `filter` | expression (optional) | Must evaluate to boolean `true` for the fact to be emitted. |

Per line, a simple mapping emits at most one fact. Behavior in edge
cases:

- **Filter is strictly boolean.** A filter that evaluates to anything
  other than `true`/`false` — including `nil` from a misspelled field
  name — skips the record and reports through the error hook (§2.4). A
  `false` filter skips silently.
- **An arg expression error** skips the whole fact for that mapping and
  reports through the error hook.
- **An arg that evaluates to `nil`** (e.g. an absent field) still emits
  the fact, with a Null term in that position, and reports a warning
  through the error hook.

### 2.2 Imperative mappings

When one line should produce multiple facts, a mapping may instead be a
single expr program:

```json
{"expr": "let id = fresh_id();
          assert(\"email\", [id, value.sender, value.time]);
          map(value.recipients, assert(\"email_to\", [id, #]))"}
```

Functions available (these, and only these, beyond expr-lang's builtins):

| Function | Behavior |
|----------|----------|
| `fresh_id()` | Returns a new synthetic ID. The counter is global to one load: every call across all lines, mappings, and source files yields a distinct ID (`#0`, `#1`, …). Use one `let id = fresh_id()` per record to join the facts derived from it. |
| `assert(pred, args)` | Emits the fact `pred(args...)`. `pred` must be a string, `args` an array; each element converts per §2.3. Returns `true`. |
| `match_contains(pred, key, haystack, patterns)` | For each string in `patterns` that is a substring of `haystack`, emits `pred(key, pattern)`. Returns the match count. A non-string `haystack` matches nothing. Non-string patterns are skipped. |
| `match_starts_with(pred, key, str, patterns)` | Same, prefix match. |
| `match_ends_with(pred, key, str, patterns)` | Same, suffix match. |
| `match_regex(pred, key, str, patterns)` | Same, Go/RE2 regexp match; patterns that fail to compile are silently skipped. |

The `match_*` functions are plain string tests — the `case_insensitive` and
`windash` modifiers belong to declarative matchers (§3) only.

Unlike simple mappings, a runtime error in an imperative program (e.g. an
out-of-range index) **aborts the entire load** with the file, line, and
mapping identified. Imperative and simple mappings may be mixed within
one source.

### 2.3 Value conversion

Every extracted value — a simple-mapping arg result or an `assert`
argument — converts to a Datalog constant:

| Expression result | Datalog term |
|-------------------|--------------|
| string | String |
| integer | Integer |
| float that is an exact integer | Integer (normalized) |
| other float | Float |
| bool | Bool |
| `nil` (JSON null / missing field) | Null |
| JSON object or array | Composite (atomic; see the language reference §3.2) |
| ID from `fresh_id()` | ID |
| anything else | String via formatting |

JSON numbers are decoded losslessly: integers survive beyond 2^53 (full
int64 range), and only genuinely fractional or out-of-range values become
floats. `1.0` and `1` load as the same Integer constant.

An object or array value becomes a single **composite** term rather than
an error. The recommended idiom for large records is *flatten and
retain*: assert the hot fields as flat predicates for indexed joins, and
assert the raw record once under the same synthetic ID for provenance:

```json
{"expr": "let id = fresh_id();
          assert(\"event\", [id, value]);
          assert(\"process\", [id, value.pid, value.name, value.cmdline])"}
```

Detection rules then run on the flat predicates, and only findings reach
back through `event(Id, Record)` for evidence. Composites are hash-consed
(each distinct record stored once) but are retained for the lifetime of
the database, and destructuring them does not use column indexes.

### 2.4 Load-time error hooks (Go API)

Three optional callbacks on `Config`, never serialized in schema files:

- `OnMappingError` — receives simple-mapping filter/arg failures (§2.1);
  if unset, those records are skipped silently.
- `OnMatcherWarning` — receives non-fatal matcher warnings (§3.6).
- `OnTypeError` — enables declaration-based type checking of loaded facts
  (§4.2); if unset, no type checking occurs.

## 3. Matchers

A matcher scans one term position of every loaded fact of a named
predicate and emits derived match facts — precomputed pattern tables that
rules join against, instead of running string builtins per rule:

```yaml
matchers:
  - predicate: process
    term: 2                    # third term (0-based)
    case_insensitive: true
    windash: true
    contains: [certutil, bitsadmin]
    contains_from: lolbins.txt
    regex_match: ["T[0-9]{4}\\.[0-9]{3}"]
```

### 3.1 When matchers run

Matchers run once, after all sources are loaded and before the database
is built. They scan **loaded facts only** — facts later derived by
Datalog rules are never scanned. The scanned term must be a String;
facts whose term at the given index is any other type (including
composites), or whose arity is too small, are skipped.

### 3.2 Matcher keys

| Key | Type | Meaning |
|-----|------|---------|
| `predicate` | string | Predicate whose facts are scanned. Required. |
| `term` | int | 0-based index of the term to scan. Required, ≥ 0. |
| `case_insensitive` | bool | Case-insensitive matching (§3.4). |
| `windash` | bool | `-`/`/` flag equivalence (§3.4). |
| `contains`, `starts_with`, `ends_with`, `regex_match`, `base64`, `base64_utf16le`, `cidr` | list | Inline pattern lists (§3.3). |
| `contains_from`, `starts_with_from`, `ends_with_from`, `regex_match_from`, `base64_from`, `base64_utf16le_from`, `cidr_from` | string | Pattern files (§3.5). |

One matcher may carry several pattern kinds; each kind emits its own
predicate.

### 3.3 Match types and emitted predicates

Every emitted fact has arity 2: `pred(Value, Pattern)`, where `Value` is
the full scanned string and `Pattern` is the pattern as written in the
schema (for `windash`, the original form, even when the swapped variant
matched). Duplicate (predicate, value, pattern) triples are emitted once.

| Kind | Emitted predicate | Match semantics |
|------|-------------------|-----------------|
| `contains` | `contains` (modifiable) | Substring. |
| `starts_with` | `starts_with` (modifiable) | Prefix. |
| `ends_with` | `ends_with` (`ci_` only) | Suffix. |
| `regex_match` | `regex_match` (`ci_` only) | Go/RE2 regexp. |
| `base64` | `base64_contains` (fixed) | Pattern found in base64-encoded data at any of the 3 byte alignments (§3.4). |
| `base64_utf16le` | `base64_utf16le_contains` (fixed) | Pattern UTF-16LE-encoded first, then as `base64` (targets PowerShell `-EncodedCommand`). |
| `cidr` | `cidr_match` (fixed) | Scanned string parses as an IP address contained in the CIDR network. |

Modifier prefixes are prepended to the base name, `ci_` before `wd_`:

| Modifiers | `contains` becomes |
|-----------|--------------------|
| none | `contains` |
| `case_insensitive` | `ci_contains` |
| `windash` | `wd_contains` |
| both | `ci_wd_contains` |

Scope limits — the prefixes only apply where they are meaningful:

- `windash` affects **only** `contains` and `starts_with`. `ends_with`
  and `regex_match` take at most a `ci_` prefix, never `wd_`.
- `base64`, `base64_utf16le`, and `cidr` predicates are **never**
  prefixed; both modifiers are ignored for them.

### 3.4 Matching semantics in detail

- **`case_insensitive`** lowercases both the scanned string and the
  patterns (simple lowercasing, not full Unicode case folding). For
  `regex_match` it prefixes patterns with `(?i)`.
- **`windash`** matches Windows command-line flags written with either
  dash or slash: a pattern beginning with `-` also matches with a leading
  `/`, and vice versa. Patterns beginning with any other character get no
  variant. The emitted fact always carries the original pattern.
- **`base64`** computes, for each pattern, the base64 encodings at byte
  offsets 0, 1, and 2 (trimming the alignment-dependent leading and
  trailing characters), and reports a match if any variant is a substring
  of the scanned string. This finds a plaintext pattern inside base64
  data regardless of where the encoded region begins.
- **`cidr`** parses networks with Go's `netip` (IPv4 and IPv6). A scanned
  string that does not parse as an IP address simply never matches.
- **`regex_match`** uses Go's RE2 syntax (no backreferences).

### 3.5 Pattern files (`_from`)

Each pattern kind has a `_from` variant naming an external file:

```
# lolbins.txt — one pattern per line, # comments
certutil
bitsadmin
Invoke-WebRequest
```

Lines are whitespace-trimmed; blank lines and lines starting with `#` are
skipped (whole-line comments only). Loaded patterns are appended to the
inline list of the same kind and the `_from` field is cleared, making the
resolved config self-contained. There is no deduplication. A missing
pattern file is an error.

Resolution root: via `LoadSchemaDir`/`LoadSchemaFS`, pattern files
resolve relative to the **schema** directory. In the `datalog` CLI,
they resolve relative to the **data** directory (`-d`), inside its
confinement boundary (§6).

### 3.6 The gate prefilter

Each matcher compiles a combined "gate" regexp from all its patterns so
strings that cannot match any pattern are skipped cheaply. The gate is
only an optimization: results are identical with or without it. If a
gate fails to compile (e.g. a very large pattern list exceeding the
regexp size limit), matching falls back to checking every pattern and the
`OnMatcherWarning` hook is notified.

## 4. Declarations

Declarations document predicates and name their terms:

```json
{
    "declarations": [{
        "name": "process",
        "use": "A running process observed on a host.",
        "terms": [
            {"name": "pid", "use": "Process ID", "type": "integer"},
            {"name": "name", "use": "Executable name"},
            {"name": "cmdline", "use": "Full command line"}
        ]
    }]
}
```

| Key | Meaning |
|-----|---------|
| `name` | Predicate name. |
| `use` | Documentation (markdown), surfaced by the CLI, MCP server, and workbench. |
| `terms` | One entry per term; arity is the length of this list. `p` declared with 3 terms declares `p/3` only. |
| `terms[].name` | Field name for encoder output; empty means positional. |
| `terms[].use` | Per-term documentation. |
| `terms[].type` | Optional: `string`, `integer`, `float`, `id`, or `json`; empty accepts any type. |

Declarations flow into the loaded database (`Database.Declarations()`)
and from there to the encoder and to the engine's optional compile-time
type checking of rules.

### 4.2 Type checking

When the Go caller sets `OnTypeError`, every loaded fact matching a
declaration (by name and arity) is checked against the declared term
types; violations are reported through the hook without aborting the
load. Without the hook, no checking occurs.

## 5. Loading

`Config.LoadDir(dir)` (or `LoadFS`) reads every source and produces an
in-memory database:

1. The config is validated (§1.3).
2. Each source file is read line by line, applying every mapping to every
   line.
3. Matchers scan the loaded facts and append their match facts.
4. If enabled, type checking runs (§4.2).
5. The database is built, carrying facts and declarations.

Line handling:

- Empty and whitespace-only lines are skipped.
- A UTF-8 byte-order mark at the start of any line is stripped.
- A line that is not valid JSON, or that has trailing data after the JSON
  value, **aborts the load** with the file and line number — malformed
  input is never silently skipped.
- Lines are limited to 10 MiB.
- Facts are produced in source order, line order, mapping order; the
  database deduplicates identical facts.

The `datalog` CLI accepts either a directory or a `.zip` archive for
`-d`; a zip is read in place with the same semantics.

## 6. Sandboxing and confinement

Two properties bound what a schema can do, relevant when schemas come
from a less-trusted author (e.g. a model driving the MCP server):

- **Expressions are sandboxed.** The expr environment contains only
  `value`, the §2.2 functions, and expr-lang builtins. Identifiers such
  as `os`, `exec`, or `env` do not resolve — they are compile errors —
  and there is no I/O of any kind inside expressions.
- **File references are confined (CLI/MCP/serve).** Every path a schema
  mentions — `sources[].file` and every `_from` pattern file — is
  resolved inside the `-d` data root, enforced at the syscall level
  (`os.Root`). Absolute paths, `..` escapes, and symlinks pointing
  outside the root are rejected.

## 7. Encoding results to JSONL

`jsonfacts.NewEncoder(w, declarations)` writes facts back out as JSONL.
Each fact becomes one line: a single-key object whose key is the
predicate name and whose value is an object of the fact's terms:

```json
{"process": {"pid": 1234, "name": "cmd.exe", "cmdline": "cmd /c whoami"}}
```

- Field names come from the declaration matching the predicate name and
  arity. Terms without a declared name — or the whole fact, if no
  declaration matches — use the 0-based position as the key (`"0"`,
  `"1"`, …).
- Keys within the JSON object appear in alphabetical order (JSON object
  key order, not term order).

Term encoding:

| Term | JSON |
|------|------|
| String | string |
| Integer | number |
| Float | number |
| Bool | `true` / `false` |
| Null | `null` |
| ID | number (the raw ID, not `"#N"`) |
| Composite | its decoded JSON value — the original record round-trips |

Because a Composite encodes as its full decoded value, the
flatten-and-retain idiom (§2.3) lets alert output carry complete original
records as evidence.
