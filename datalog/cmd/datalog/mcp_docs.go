package main

// This file collects the model-facing documentation strings for the MCP
// tool surface (datalog mcp). They are load-bearing API docs: a model that
// has never seen this project must be able to author a working jsonfacts
// schema and Datalog ruleset from this text alone. Keep them here, out of
// mcp.go, so the wiring stays readable.

// mcpServerInstructions is passed as the server's own "instructions" field
// (server.WithInstructions), which most MCP clients surface to the model
// as system-level guidance before any tool is even called.
const mcpServerInstructions = `This server exposes one Datalog evaluation session backed by an
operator-chosen data directory. Nothing is written back to disk; the
session lives only for this connection.

Before anything else, call list_predicates. A session often arrives
already populated with the operator's schema and rules - answer
questions against it with query and sample_facts. set_schema and
set_rules REPLACE the operator's working documents; enter the authoring
loop below only when the session is empty or the operator has asked you
to change the mappings or rules - never just to answer a question.

Workflow loop (repeat as needed):
  1. sample_input   - look at raw JSONL records to learn field names.
  2. set_schema     - submit a jsonfacts config mapping JSON fields to
                       predicates; check the returned per-predicate fact
                       counts. Zero facts for a predicate means the
                       mapping is wrong - iterate.
  3. set_rules      - submit a Datalog program (rules only, no embedded
                       queries) deriving higher-level predicates from the
                       loaded facts. Fix parse/compile errors and resubmit.
  4. query          - run one query and inspect vars/rows/stats. Iterate
                       on the rules or schema based on what comes back.

Each of set_schema and set_rules replaces the whole document; there is no
incremental edit API. Submit the complete schema or ruleset text every
time, the same way a human would save a file.`

// mcpSetSchemaDescription documents the jsonfacts config format for
// set_schema: sources, matchers, and declarations, condensed from
// jsonfacts/doc.go and README.md.
const mcpSetSchemaDescription = `Replace the session's data-loading schema (a jsonfacts config) and reload
data from the confined data directory. Part of the workflow loop: call
sample_input first to see raw record shapes, then iterate set_schema
against the returned per-predicate fact counts until every predicate you
need has a plausible (non-zero) count.

The schema has three sections, each optional:

sources - one entry per JSONL file plus one or more mappings from JSON
fields to predicate facts. Two mapping styles, mutually exclusive within
one mapping entry:

  Simple mode: each element of "args" is an expr-lang expression
  evaluated against "value" (the parsed JSON object for the current
  line); one fact is emitted per line unless "filter" evaluates false.

    {"sources": [{"file": "processes.jsonl", "mappings": [
      {"predicate": "process",
       "args": ["value.pid", "value.name", "value.cmdline"],
       "filter": "value.pid != 0"}
    ]}]}

    Given {"pid": 1234, "name": "cmd.exe", "cmdline": "cmd /c whoami"}
    this emits process(1234, "cmd.exe", "cmd /c whoami").

  Imperative mode: an "expr" program using fresh_id()/assert() to emit
  zero or more facts per line - use this for one-to-many relationships
  or when correlating several derived facts under one synthetic ID.

    {"expr": "let id = fresh_id(); assert(\"email\", [id, value.sender, value.time]); map(value.recipients, assert(\"email_to\", [id, #]))"}

    Given {"sender": "alice", "time": "t", "recipients": ["bob","carol"]}
    this emits email(#0, "alice", "t"), email_to(#0, "bob"), email_to(#0, "carol").

  Available imperative-mode functions:
    fresh_id()                                  - a synthetic join-key ID
    assert(pred, args)                          - emit one fact
    match_contains(pred, key, haystack, pats)   - emit pred(key, pat) per substring match
    match_starts_with(pred, key, str, pats)     - same, prefix match
    match_ends_with(pred, key, str, pats)       - same, suffix match
    match_regex(pred, key, str, pats)           - same, regex match

  A mapping argument (or assert() argument) that evaluates to a JSON
  object or array becomes one atomic composite term rather than an
  error - useful for asserting a whole raw record for later evidence
  while also flattening the hot fields you need to join on:

    {"expr": "let id = fresh_id(); assert(\"event\", [id, value]); assert(\"process\", [id, value.pid, value.name])"}

matchers - scan an already-loaded predicate's string term for patterns
and emit derived match facts, without writing Datalog rules for simple
string matching:

    {"matchers": [{"predicate": "process", "term": 2,
                    "case_insensitive": true, "windash": true,
                    "contains": ["certutil", "bitsadmin"]}]}

  This scans term index 2 (0-based) of every process/N fact and emits
  ci_wd_contains(value, pattern) for each match. The emitted predicate
  name is built from the match type plus modifier prefixes:
    contains / starts_with / ends_with / regex_match
    base64 (-> base64_contains) / base64_utf16le (-> base64_utf16le_contains, for PowerShell -EncodedCommand)
    cidr (-> cidr_match, term must hold an IP string)
  Modifiers: case_insensitive adds a "ci_" prefix; windash (match "-" and
  "/" interchangeably, for Windows CLI flags) adds "wd_"; both together
  give "ci_wd_".

  Every match type has a "*_from" sibling field (contains_from,
  starts_with_from, ends_with_from, regex_match_from, base64_from,
  base64_utf16le_from, cidr_from) that loads patterns from a file in the
  data directory instead of (or merged with) the inline list - one
  pattern per line, "#"-prefixed lines are comments. File references,
  like source files, are confined to the operator's data directory.

declarations - name a predicate's terms, purely for documentation and
for structured tool output (list_predicates' "use" field):

    {"declarations": [{"name": "process", "use": "A running process.",
                        "terms": [{"name": "pid"}, {"name": "name"}, {"name": "cmdline"}]}]}

Submit "format" as "yaml" (default) or "json" to match how you wrote the
"schema" text. The whole config is replaced on each call - there is no
incremental add_source/add_matcher API. On success you get per-predicate
fact counts; on failure (bad file reference, bad expr, invalid regex/CIDR,
etc.) you get the underlying error, and the previous schema stays active.`

// mcpDatalogSyntaxSummary is the condensed Datalog syntax reference shared
// by set_rules and query descriptions, condensed from README.md.
const mcpDatalogSyntaxSummary = `Datalog syntax summary:
  Fact:        parent("tom", "bob").
  Rule:        ancestor(X, Y) :- parent(X, Z), ancestor(Z, Y).
  Query:       ancestor("tom", X)?
  Negation:    orphan(X) :- person(X), not parent(?, X).
  Comparison:  X != Y   |   Amt > 1000   |   X <= Y
  Arithmetic:  cost(Item, Total) :- price(Item, P), tax_rate(Rate), Total is P * (1 + Rate).
  Aggregate:   alert_count(Sev, N) :- N = count : alert(?, Sev, ?).
               total_bytes(Host, T) :- T = sum(Bytes) : traffic(Host, Bytes).
               (also: min, max)
  Builtins:    @contains(Str, "needle")   - constraint form (in the body, must hold)
               @time_diff(T2, T1, D)      - binding form (D is the output)
  Comments start with "%". "?" alone is the anonymous variable (matches
  anything, binds nothing) - legal in rule bodies, but rejected in the
  query tool's arguments, where every position should be a named variable
  or an underscore-prefixed one. A statement ends with "." (fact or rule)
  or "?" (query).`

// mcpSetRulesDescription documents set_rules: whole-document replacement,
// the syntax summary, and the embedded-query rejection.
const mcpSetRulesDescription = `Replace the session's Datalog ruleset (facts and rules; not queries - see
below) with the given source text. Part of the workflow loop: after
set_schema produces the predicates you need, write rules deriving
higher-level observations from them, then use the query tool to inspect
results and iterate here on parse/compile errors.

` + mcpDatalogSyntaxSummary + `

The whole document replaces the previous one - there is no incremental
add_rule/delete_rule API; resubmit the complete ruleset text each time,
as you would save a file.

Embedded queries (statements ending in "?") are rejected here with an
error: put queries in the query tool's "query" argument instead, so each
query's results are returned directly rather than silently discarded.

On success, returns the list of predicates the ruleset defines (rule and
aggregate-rule heads). On failure, returns the parser or compiler error
verbatim, including line:column and the offending source line - use it to
locate and fix the problem, then resubmit the whole document.`

// mcpQueryDescription documents the query tool.
const mcpQueryDescription = `Evaluate one Datalog query against the current schema + rules + loaded
data, and return matching rows plus per-stratum evaluation stats. This is
the last step of the workflow loop: sample_input -> set_schema (check fact
counts) -> set_rules (check parse/compile errors) -> query (check results
and stats, then go back and adjust schema/rules as needed).

` + mcpDatalogSyntaxSummary + `

The "query" argument is a single query statement. Name every variable you
want returned as a column, and use an underscore-prefixed variable for
positions you don't care about - the anonymous variable '?' is rejected
in query arguments (it is only for aggregate rule bodies, via set_rules):
  suspicious(Host, Pid, Cmd)?
  exe_drop(Host, User, _Share, Path, _Ip)?

A query over a predicate that no loaded data or rule defines is not an
error - it just returns 0 rows. If a count is unexpectedly zero, check
the predicate's name and arity with list_predicates before concluding
the facts don't exist.

"limit" caps how many rows are serialized into the response (default 100,
hard cap 1000 - values above the cap are silently clamped, not rejected).
The query still evaluates to completion regardless of limit; "total" is
the true row count and "truncated" reports whether rows were cut off in
the response. Evaluation runs under a server-configured timeout (see the
--timeout flag the operator started the server with; default 60s) - a
runaway recursive rule will time out rather than hang the session.

"stats" reports one entry per evaluation stratum: which predicates it
covers, how many rules/aggregates/facts were involved, how many fixpoint
iterations it took, and how long it took (duration_ms). Use this to
diagnose a rule that never reaches a fixpoint (hits the iteration cap) or
that is unexpectedly slow.`

// mcpListPredicatesDescription documents list_predicates.
const mcpListPredicatesDescription = `List every predicate currently known to the session: predicates loaded
from data (via set_schema) and predicates defined by rules (via
set_rules), together with their arity, current fact count, and - when the
schema declared it - a human-readable "use" description. Call this to
get an overview before writing rules or queries, or after set_schema/
set_rules to confirm what changed.`

// mcpSampleFactsDescription documents sample_facts.
const mcpSampleFactsDescription = `Return up to "limit" facts (default 20) for one predicate/arity pair, plus
the true total fact count and whether the response was truncated. Use
this to sanity-check that a source or matcher in your schema produced the
facts you expected, or to see example values before writing a rule or
query against a predicate.`

// mcpSampleInputDescription documents sample_input.
const mcpSampleInputDescription = `Read raw lines from a file in the operator's data directory, or - when
"file" is omitted - list the files available there. This is the first
step of the workflow loop: look at raw records before writing a
set_schema mapping, so field names in "args"/"expr" expressions
(value.foo, value.bar.baz, ...) are not guesses.

With "file" set: returns up to "limit" lines (default 10) starting at
0-based line "offset" (default 0). Individual lines longer than about
4KiB are truncated with a trailing marker noting the original byte
length, so one huge line cannot blow out the response.

With "file" omitted: lists every file reachable under the data
directory (or, when the operator started the server with a .zip data
source, every file in the zip), regardless of extension - JSONL sources
are the common case but pattern files (*_from) live there too.

All file references are confined to the operator's data directory; paths
that escape it (absolute paths, "..") are rejected.`
