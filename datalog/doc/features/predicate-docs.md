# Feature: Doc comments for rules and predicates

## Problem

The system's documentation story is asymmetric. Base predicates loaded
through jsonfacts can carry rich docs — `datalog.Declaration.Use` and
per-term `TermDeclaration.Use` are markdown, and `list_predicates`
already surfaces them — but everything on the rules side is opaque:

- **Rules cannot be documented at all.** `%` comments are discarded at
  the lexer (`syntax/parse.go`, "line comment" branch); `syntax.Rule` is
  just `{Head, Body}`. There is no way to record *why* a rule asserts
  `port_scan` short of prose in a README the engine never sees.
- **Derived predicates have no term names.** The transformer emits
  DocOnly declarations for rule heads (`seminaive/transformer.go`,
  `Declarations`), but they are bare names — no `Use`, no `Terms`. A
  reader (human or LLM) asking "what is the 3rd term of `port_scan`?"
  must find every rule with that head and reverse-engineer the position
  from variable usage.
- **No mechanical "describe" surface.** To understand one predicate, an
  LLM must fetch the *entire* rules document and read source. There is
  no call that answers: what does this predicate mean, what are its
  terms, which rules derive it, which rules consume it.

This matters disproportionately for the intended workflow: the middle
strata of the detection ontology is human-owned, and the model builds
down from it (mappings) and up from it (chains). The contract only works
if the model can *query* the contract instead of inferring it.

## Design constraints and observations

**1. Head variable names are already documentation.** In
`port_scan(Src, Dst, PortCount) :- ...`, the head names its own terms.
This is mechanically available at compile time and costs the author
nothing. Term-name derivation should exploit it before asking anyone to
write anything.

**2. The DocOnly mechanism is the right carrier.** `datalog.Declaration`
with `DocOnly: true` already flows from the transformer through
`Database.Declarations()` to `list_predicates` and the encoder, and is
structurally prevented from becoming an arity-0 schema
(`datalog.go`, Declaration doc comment). Enriching those declarations —
rather than inventing a parallel docs channel — means every existing
consumer (MCP, workbench, encoder) improves without modification.

**3. Docs must survive the round trip.** The session's rules document is
canonical text; `FuzzParseAll` pins print→reparse fidelity. A doc
comment that vanishes on reparse (or shifts to the wrong rule) is a
silent data-loss bug of exactly the class we hunt. Attachment must be
deterministic and the renderer must emit docs back.

**4. Two granularities, one syntax.** "Why does this rule assert a port
scan" is rule-level; "what does port_scan mean" is predicate-level.
Both need a home in `.dl` text, but inventing a declaration statement
form is a bigger language change than this feature needs.

## Proposed solution

### Syntax: `%%` doc comments

A contiguous block of lines beginning `%%` immediately preceding a
statement attaches to that statement (Go convention: a blank line or a
plain `%` comment breaks attachment; a detached `%%` block is a parse
warning, not an error). Ordinary `%` comments stay discarded.

```prolog
%% A source address probed many distinct ports on one target within
%% the window. PortCount is the number of distinct destination ports.
port_scan(Src, Dst, PortCount) :-
    PortCount = count(Port) : conn(Src, ?, Dst, Port, ?),
    PortCount > 20.
```

AST changes: `syntax.Rule`, `syntax.AggregateRule`, and `syntax.Query`
gain a `Doc string` field (facts are `Rule`, so documented facts come
free). `Rule.String()` / `AggregateRule.String()` render the doc block
back with `%%` prefixes; `FuzzParseAll` gains doc-comment equality in
its round-trip assertions and seed corpus.

`%%` is currently a legal (empty) `%` comment, so no existing program
changes meaning; a program that happened to start a comment with `%%`
now attaches it, which is the intended reading anyway.

### Predicate-level docs assembled at compile time

The transformer's rule-head bookkeeping is upgraded from bare DocOnly
declarations to assembled ones:

- **Terms:** if every rule for a head (same name+arity) uses the same
  variable name at position i, that name becomes `Terms[i].Name`
  (lower-cased to match jsonfacts convention); conflicting or
  non-variable positions stay unnamed. Emitted with `DocOnly: true`
  still — derived declarations must never become type constraints.
- **Use:** the concatenation of the head's rule docs, one paragraph per
  documented rule. Each rule's doc explains why the predicate holds in
  that case, so the concatenation *is* the predicate's meaning — the
  same way Go assembles package docs from multiple files. A predicate
  whose rules are all undocumented gets term names only.
- An explicit jsonfacts declaration for the same (name, arity) wins
  outright, unchanged — the operator's schema outranks assembly.

This lands in `transformer.Declarations` (the existing merge point), so
`list_predicates`, the workbench Fact Browser, and `jsonfacts.Encoder`
all pick it up with zero changes of their own.

### `describe`: the mechanical access surface

One session-level operation, exposed everywhere the session already is:

```
describe(name) -> {
  arities: [{
    arity, factCount, declaration,        // docs, terms, types, DocOnly
    derivedBy: [{ruleText, doc}],         // rules with this head
    consumedBy: [{ruleText, doc}],        // rules referencing it in a body
  }],
}
```

`derivedBy`/`consumedBy` come from a walk of the session's current
`syntax.Ruleset` — no engine changes. Surfaces:

- **MCP:** a `describe` tool beside `list_predicates`/`sample_facts`.
  `list_predicates` stays the cheap index; `describe` is the deep dive
  the model calls before writing a rule against a predicate.
- **REPL:** `.describe <pred>`.
- **Workbench:** Fact Browser predicate headers render the assembled
  `Use` and named term columns instead of positional `0, 1, 2`.

## Work

1. **Lexer/parser** (`syntax/parse.go`): capture `%%` blocks, attach to
   the following statement; `Doc` fields on `Rule`, `AggregateRule`,
   `Query`; render docs in `String()`; warning for detached blocks.
2. **Round-trip:** extend `FuzzParseAll` and the seed corpus with
   doc-comment fidelity (content, attachment target, multi-line blocks,
   `%%` vs `%` adjacency).
3. **Transformer** (`seminaive/transformer.go`): assemble term names
   from head variables and `Use` from rule docs in `Declarations`;
   regression tests for the conflict, multi-rule, multi-arity, and
   explicit-declaration-wins cases.
4. **`describe`** in `cmd/datalog` (session level): ruleset walk +
   declaration lookup + fact counts (the O(1) count APIs exist); MCP
   tool, REPL command, Fact Browser headers.
5. **Docs:** `doc/reference/datalog.md` (comment section, new appendix
   entry), README syntax table, MCP tool docs (`mcp_docs.go`).

Suggested order: 1 → 2 land together (syntax is the contract), then 3
and 4 independently.

## Risks / open questions

- **Attachment ambiguity.** The blank-line rule must be pinned by tests
  (doc block, blank line, rule → detached; doc block directly above →
  attached). Get this wrong and docs silently migrate between rules on
  reformat — worth a dedicated fuzz property (attachment stable under
  print→reparse).
- **Concatenated Use can mislead.** Ten rules deriving one head produce
  a ten-paragraph Use. Acceptable for now; if it becomes noise, a
  convention like "first doc block wins for the predicate, later ones
  are rule-only" can be added — but start with the simple assembly.
- **Doc edits are semantic no-ops that still bump the session
  generation.** Editing only a `%%` comment recompiles and invalidates
  the derived-query cache. Correct, merely wasteful; not worth
  special-casing.

## Out of scope

- A declaration *statement* in `.dl` syntax (term types, constraints in
  rule text). If assembled docs prove insufficient, that is its own
  feature; DocOnly assembly must land first regardless.
- Doc search / full-text query over docs.
- Rendering docs into provenance explanations — that synergy belongs to
  the provenance feature (see `provenance.md`), which consumes
  `Rule.Doc` and assembled declarations as-is.
