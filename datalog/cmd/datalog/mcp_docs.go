package main

import "time"

// This file collects the model-facing documentation strings for the MCP
// tool surface (datalog mcp). They are load-bearing API docs: a model that
// has never seen this project must be able to author a working jsonfacts
// schema and Datalog ruleset from this text alone. Keep them here, out of
// mcp.go, so the wiring stays readable.

// mcpServerInstructions is passed as the server's own "instructions" field
// (server.WithInstructions), which most MCP clients surface to the model
// as system-level guidance before any tool is even called.
const mcpServerInstructions = `This server exposes one Datalog evaluation session backed by an
operator-chosen data directory. Session state (schema, loaded facts) lives
only for this connection; rule groups written via put_rule_group/
delete_rule_group land on disk immediately in the operator's rules/
directory (see below) — that part IS persisted.

Before anything else, call list_predicates. A session often arrives
already populated with the operator's schema and rules - answer
questions against it with query and sample_facts. set_schema REPLACES the
operator's whole data-loading document; put_rule_group/delete_rule_group
edit ONE rule group at a time. Enter the authoring loop below only when
the session is empty or the operator has asked you to change the mappings
or rules - never just to answer a question.

Workflow loop (repeat as needed):
  1. sample_input      - look at raw JSONL records to learn field names.
  2. set_schema        - submit a jsonfacts config mapping JSON fields to
                          predicates; check the returned per-predicate fact
                          counts. Zero facts for a predicate means the
                          mapping is wrong - iterate.
  3. put_rule_group     - create or edit ONE rule group (all statements
                          sharing one head predicate/arity) deriving
                          higher-level predicates from the loaded facts.
                          Fix parse/compile errors and resubmit; a stale
                          revision comes back with the group's current
                          text so you can retry against it.
  4. query             - run one query and inspect vars/rows/stats. Iterate
                          on the rules or schema based on what comes back.
  5. explain           - given one fact from a query result, get its full
                          derivation tree (rule + body facts, recursively) -
                          use this to justify a finding instead of inventing
                          a justification for it.

list_predicates is the cheap index; before writing a rule or query against
an unfamiliar predicate, call describe instead of guessing at its terms or
meaning - it returns the predicate's declared/assembled documentation,
term names, current fact count, and every rule that derives or consumes
it, per arity. list_rule_groups/get_rule_group are the rule-group-shaped
counterpart: use them to see what groups exist and their current text/
revision before editing one with put_rule_group.

set_schema replaces the whole data-loading document; there is no
incremental add_source/add_matcher API. put_rule_group/delete_rule_group are
already incremental — each call touches exactly one rule group's file,
identified by (head, arity), never the whole ruleset.

` + mcpDialectPrimer

// mcpDialectPrimer explains Datalog itself and this dialect's deltas for a
// model that has never seen the project. It rides in the server
// instructions (so agents read it before any tool call) and is written to
// preempt the failure modes observed in live sessions: hand-joining query
// results, bare contains vs @contains, counting rows by eye, and treating
// 0 rows as an error.
const mcpDialectPrimer = `Datalog primer:

Datalog is a logic query language. A program is facts (ground tuples,
e.g. parent("tom", "bob").) plus rules that derive new facts from old
ones (head :- body). Evaluation is bottom-up to a fixpoint: every
derivable fact gets derived, so recursion (ancestors, reachability) is
natural and always terminates. There are no function symbols and no
control flow - a rule is a statement of logic, not a procedure, and rule
order does not matter.

The engine is the reasoner. A query body is a conjunction: shared
variables between atoms ARE the join, comparisons and @builtins filter,
'not' expresses absence. State what must hold and read the bindings
back; never fetch two predicates and correlate their rows yourself, and
never count rows by eye when an aggregate rule can count exactly.

This dialect, beyond textbook Datalog:
  - Constants: "strings", integers, floats, and composite JSON values
    (a mapped object/array is one atomic term - it joins as a whole).
  - Variables are capitalized (Host, X). _Name is a named don't-care,
    excluded from query result columns. '?' and a bare '_' are anonymous:
    each occurrence is a fresh variable, as in Prolog.
  - Negation must be stratified (no recursion through 'not'), and
    variables in a negated atom must be positively bound or anonymous.
  - Arithmetic binds through 'is': Total is P * (1 + Rate).
  - Aggregates (count, sum, min, max) are rule bodies, not query
    expressions: alert_count(Sev, N) :- N = count : alert(?, Sev, ?).
  - @-sigil builtins (@contains, @starts_with, @ends_with, @time_diff,
    ...) are evaluated by the engine. The same names WITHOUT the sigil
    are ordinary fact predicates emitted by schema matchers, holding
    only the patterns the schema declared - do not confuse them.
  - A query against an undefined predicate returns 0 rows, not an
    error; verify names and arities with list_predicates.
  - Statements end with "." (facts, rules) or "?" (queries); comments
    start with "%".`

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
// by put_rule_group and query descriptions, condensed from README.md.
const mcpDatalogSyntaxSummary = `Datalog syntax summary:
  Fact:        parent("tom", "bob").
  Rule:        ancestor(X, Y) :- parent(X, Z), ancestor(Z, Y).
  Query:       ancestor("tom", X)?
               remote_logon(H, User, _A, _B), smb_conn(H, _C, _D)?
               (comma-separated atoms are ONE conjunctive query, joined on
                shared variables; comparisons, @builtins, and negation may
                appear in the body too)
  Negation:    orphan(X) :- person(X), not parent(?, X).
               (variables in a negated atom must be bound by a positive
                atom or anonymous)
  Comparison:  X != Y   |   Amt > 1000   |   X <= Y
  Arithmetic:  cost(Item, Total) :- price(Item, P), tax_rate(Rate), Total is P * (1 + Rate).
  Aggregate:   alert_count(Sev, N) :- N = count : alert(?, Sev, ?).
               total_bytes(Host, T) :- T = sum(Bytes) : traffic(Host, Bytes).
               (also: min, max; aggregates are RULES - to answer "how
                many", put_rule_group an aggregate, then query its head)
  Builtins:    @contains(Str, "needle")   - constraint form (in the body, must hold)
               @time_diff(T2, T1, D)      - binding form (D is the output)
               The @ sigil is required: contains(X, P) WITHOUT it names a
               matcher-emitted fact predicate that holds only the patterns
               declared in the schema - silently 0 rows for anything else.
  Comments start with "%". A "%%" block on the lines immediately above a
  statement (no blank line between) is a doc comment attached to it -
  describe surfaces it, and it explains the predicate. A "%%" block
  separated by a blank line attaches to nothing and is dropped
  (put_rule_group reports these in "warnings"). "?" alone and a bare "_" are anonymous
  variables (each occurrence is distinct, matches anything, binds
  nothing) - legal anywhere in rule bodies; in the query tool's arguments
  they are allowed only inside negated atoms (where they are the required
  don't-care form), and every other position should be a named variable
  or an underscore-prefixed one (_Ignored). A statement ends with "."
  (fact or rule) or "?" (query).`

// mcpPutRuleGroupDescription documents put_rule_group: one rule group at a
// time (not the whole ruleset), the syntax summary, the head/arity
// constraint, the revision-based staleness protocol, and the
// all-or-nothing full-ruleset validation order (doc/features/workbench-v2.md
// design decision 4).
const mcpPutRuleGroupDescription = `Create or replace ONE rule group: every statement sharing one head
predicate/arity, e.g. every rule (and fact) whose head is alert/2. Part of
the workflow loop: after set_schema produces the predicates you need,
write one or more rule groups deriving higher-level observations from
them, then use the query tool to inspect results and iterate here on
parse/compile errors.

` + mcpDatalogSyntaxSummary + `

"head"/"arity" name the group; "text" must contain ONLY statements with
that exact head and arity (a mix of heads, or a head that doesn't match
the arguments, is rejected before anything else). Embedded queries
(statements ending in "?") are rejected here too: put queries in the query
tool's "query" argument instead.

"revision" is the staleness guard (see also list_rule_groups/get_rule_group,
which report a group's current revision):
  - 0 (or omitted): create a NEW group. If a group already exists at this
    head/arity, the call is rejected as stale ("is_stale": true) with no
    current content to hand back — call get_rule_group to see what's there.
  - the group's CURRENT revision: edit it. Any other value (including 0,
    if the group already exists) is rejected as stale, with the current
    "current_text"/"current_revision" returned so you can re-base your
    edit and resubmit with the right revision.
A stale rejection is NOT a tool error: it comes back as a normal
successful result with "is_stale": true, precisely so the current text and
revision ride along as structured data you can act on immediately.

Validation, all-or-nothing: text is parsed, checked against head/arity,
then a TRIAL compile of the FULL prospective ruleset (every other existing
group unchanged, plus this one) runs before anything is written — a group
that parses fine alone but breaks stratification or safety somewhere else
in the ruleset is refused, and nothing on disk changes. Only after all of
that succeeds does the group's file get written (atomically) and the
session reload.

On success, returns the new "revision", the "file" written, and
per-predicate fact counts (like set_schema's). On failure (parse error,
wrong head/arity, or a stratification/safety break), returns the
underlying error verbatim, including line:column when available, and
nothing was written.`

// mcpDeleteRuleGroupDescription documents delete_rule_group: same
// revision-based staleness protocol as put_rule_group, but for removal.
const mcpDeleteRuleGroupDescription = `Delete one rule group (identified by "head"/"arity") entirely, removing its
file from the rules directory. "revision" must equal the group's CURRENT
revision (see list_rule_groups/get_rule_group) — any other value, or
naming a group that does not exist, is rejected as stale ("is_stale":
true, with "current_text"/"current_revision" attached when the group does
exist) rather than as a tool error, exactly like put_rule_group's
staleness protocol.

Removing a group cannot itself break stratification (only ADDING a rule
can introduce a cycle), so there is no trial-compile step here: a
dependent rule that referenced the deleted head simply reads that
predicate as always empty afterward (per this dialect's "unknown predicate
is 0 rows, not an error" rule), not a compile failure.

On success, returns the "file" removed and per-predicate fact counts (like
set_schema's), reflecting the ruleset with this group gone.`

// mcpListRuleGroupsDescription documents list_rule_groups.
const mcpListRuleGroupsDescription = `List every rule group currently in the session's rules directory, in
filename order: each group's "head", "arity", "file", current "revision",
and "statements" count (how many rules/facts it holds). Call this before
put_rule_group/delete_rule_group to find a group's current revision (the
staleness guard both writes require), or to see what already exists before
adding a new one.`

// mcpGetRuleGroupDescription documents get_rule_group.
const mcpGetRuleGroupDescription = `Return one rule group's exact on-disk text plus its current "revision" —
the "head"/"arity" pair identifies the group (see list_rule_groups for the
index). Use this immediately before editing a group with put_rule_group:
its "revision" is exactly the value put_rule_group needs to accept the
edit, and its "text" is the safe starting point for your changes (verbatim
what's on disk right now, including comments and formatting).`

// mcpQueryDescription documents the query tool. It takes the server's
// actual per-query timeout rather than hardcoding a number: `datalog mcp`
// defaults to 60s but is operator-configurable via --timeout, while
// `datalog serve`'s /mcp mount has no such flag and always runs under the
// same 5s evalTimeout as the rest of the web UI (Run/Apply/agent query) —
// a single fixed claim in the doc text would be wrong for whichever mode
// didn't match it. See registerTools' call site.
func mcpQueryDescription(timeout time.Duration) string {
	return `Evaluate one Datalog query against the current schema + rules + loaded
data, and return matching rows plus per-stratum evaluation stats. This is
the last step of the workflow loop: sample_input -> set_schema (check fact
counts) -> put_rule_group (check parse/compile errors) -> query (check
results and stats, then go back and adjust schema/rules as needed).

` + mcpDatalogSyntaxSummary + `

The "query" argument is a single query statement, and its body is a
conjunction: comma-separated atoms evaluated together, joined on shared
variables, with comparisons, @builtins, and negation allowed. Let the
engine do the joining - to relate two predicates, share a variable
between their atoms:
  remote_logon(Host, User, _A, _B), smb_conn(Host, _C, _D)?
Do NOT query each predicate separately and correlate the rows yourself:
the engine's join is exact and complete, while hand-matching two
truncated row lists is neither, and one conjunctive query is cheaper
than two queries plus reasoning.

Name every variable you want returned as a column, and use an
underscore-prefixed variable for positions you don't care about - the
anonymous variables '?' and bare '_' are rejected in positive query
atoms:
  suspicious(Host, Pid, Cmd)?
  exe_drop(Host, User, _Share, Path, _Ip)?
Inside a NEGATED atom, anonymous variables are instead the required
don't-care form ("SMB sources nobody logged into"):
  smb_conn(H, _S, _D), not remote_logon(H, ?, ?, ?)?

To answer "how many ..." questions, do not count rows by eye: define an
aggregate rule via put_rule_group (N = count : ...) and query its head.

A query over a predicate that no loaded data or rule defines is not an
error - it just returns 0 rows. If a count is unexpectedly zero, check
the predicate's name and arity with list_predicates before concluding
the facts don't exist.

"limit" caps how many rows are serialized into the response (default 100,
hard cap 1000 - values above the cap are silently clamped, not rejected).
The query still evaluates to completion regardless of limit; "total" is
the true row count and "truncated" reports whether rows were cut off in
the response. Evaluation runs under this server's configured timeout,
` + timeout.String() + ` - a runaway recursive rule will time out rather than
hang the session.

"stats" reports one entry per evaluation stratum: which predicates it
covers, how many rules/aggregates/facts were involved, how many fixpoint
iterations it took, and how long it took (duration_ms). Use this to
diagnose a rule that never reaches a fixpoint (hits the iteration cap) or
that is unexpectedly slow.`
}

// mcpExplainDescription documents the explain tool.
const mcpExplainDescription = `Explain one derived fact: which rule fired, over which body facts, to
produce it. Use this after query to justify a result to a human reviewer,
or to narrate why a specific concerning fact was flagged, instead of
inventing a justification.

"fact" is one ground fact, exactly as it would appear in a rules document
or a query result row - predicate name plus constant terms, e.g.
concern("ws01", 87). It must name a fact the CURRENT schema+rules+data
evaluation actually produced (a query row you just got back, or a fact
sample_facts returned) - a predicate/arity/term combination that was never
derived returns an error, not an empty tree.

The response is a rendered derivation tree: the fact, the rule that
derived it, the ground body facts that satisfied that rule (each in turn
explained one level, recursively, down to base facts - facts loaded from
data or asserted directly, marked "[base fact]" with nothing further to
explain), and any constraint/comparison/negation detail lines the rule's
body evaluated. An aggregate head (defined with count/sum/min/max) renders
its group's true solution count and up to 10 sampled contributor tuples,
each explained the same way.

"depth" caps how many levels deep the tree recurses (default 8) - it only
affects how much of a large tree prints, never which rule or facts are
reported at the top level.`

// mcpListPredicatesDescription documents list_predicates.
const mcpListPredicatesDescription = `List every predicate currently known to the session: predicates loaded
from data (via set_schema) and predicates defined by rules (via
put_rule_group or the rules the operator loaded at startup), together with
their arity, current fact count, and - when the schema declared it - a
human-readable "use" description. Call this to get an overview before
writing rules or queries, or after set_schema/put_rule_group to confirm
what changed.`

// mcpDescribeDescription documents describe.
const mcpDescribeDescription = `Describe one predicate by name: everything known about it, across every
arity it is defined or referenced under. Use this before writing a rule or
query against a predicate you have not used yet, instead of guessing at
its terms from a rule that happens to mention it - list_predicates stays
the cheap index (name, arity, fact count, one-line "use"); describe is the
deep dive.

For each arity the predicate is known under, returns:
  arity        - the arity itself.
  factCount    - how many facts currently exist for this predicate/arity
                 (0 for a predicate that is only referenced, never loaded
                 or derived).
  declaration  - name, docs ("use", markdown), and per-term names/docs/
                 types, when known. For a rule-derived predicate this may
                 be ASSEMBLED automatically: a term gets a name when every
                 rule with this head uses the same variable name in that
                 position, and "use" is the concatenation of every
                 documented rule's %% doc comment for this head - so a
                 predicate with no explicit jsonfacts declaration can
                 still come back documented. Omitted entirely if nothing
                 is known (bare rule reference, no declaration).
  derivedBy    - every rule (plain or aggregate) whose HEAD is this
                 predicate/arity: its full source text plus its own %%
                 doc comment, if any. Empty for a base (EDB) predicate
                 loaded only from data.
  consumedBy   - every rule whose BODY references this predicate/arity in
                 any position, including a negated atom ("not ...") or an
                 aggregate rule's body: same shape as derivedBy. Use this
                 to see what would break before changing a predicate's
                 shape.

An unknown predicate (no facts, no declaration, no rule head or body
reference at all) is an error - check the name with list_predicates
first.`

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
