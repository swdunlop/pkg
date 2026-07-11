# Feature: Infer Select/Update/Delete column lists from struct tags

## Problem

Every typed query call site restates the column list that the target struct
already declares via `db:"..."` tags:

```go
db.Select[Block](`block`, `id`, `story`, `active`, `type`, `pos`, `text`, `png`, `info`)
```

In the medea project (the first non-test consumer of pkg/db) that exact
literal appears five times, and the same kind of repetition appears at every
typed call site.

`Declare[T]` only half-solves this:

```go
blocks := db.Declare[Block]("block", "id", "story", "active", "type", "pos", "text", "png", "info")
blocks.Select().Where(...)
```

The column list is still hand-maintained. Worse, both forms can drift from
the struct: rename a field's `db:"..."` tag, and the explicit SELECT list
keeps the old name silently — the scanner just stops finding the column,
returns the zero value, no error.

## The design constraint: SELECT and INSERT are not symmetric

Before deciding *how* to infer, pin down *where* inference is safe.

The scanner matches columns to struct fields **by name**. A SELECT-side
column list is scanned positionally over the SQLite statement's named output
columns; reordering the Go struct's fields doesn't change which SQLite column
feeds which Go field. SELECT is reorder-safe.

INSERT-side columns are **positional**. The user writes the value tuple as
`?` placeholders and the package binds them in the order they appear in
`Exec(ctx, args...)`. If the column list is inferred from struct field
declaration order, then renaming a tag has the same silent-zero failure mode
as today, but **reordering a struct field** has a *much* worse failure mode:
every inferred INSERT call site silently rebinds the wrong values to the
wrong columns. Same types? It compiles. Schema constraint catches it? Maybe,
maybe not. Caught in code review? Only by someone who knows this rule.

So the asymmetry is:

| operation                  | name-matched | position-matched | reorder-safe to infer? |
| -------------------------- | ------------ | ---------------- | ---------------------- |
| SELECT column list         | yes          | no               | yes                    |
| SELECT/UPDATE/DELETE RETURNING | yes      | no               | yes                    |
| INSERT column list         | no           | yes              | **no**                 |

INSERT is the one place in this package where column order is part of the
contract with the caller. Anywhere else, inference is a pure win. At INSERT,
inferring from struct field order trades the (rare) tag-rename footgun for
the (much more common) field-reorder footgun.

**Design decision:** inference is for SELECT-side operations only. INSERT
keeps explicit columns. This isn't a limitation to apologize for — it's the
right behavior. The implementer can re-litigate if they find a way to make
INSERT inference reorder-safe (named-args binding, etc.), but the default is
"don't infer INSERT."

## Declare's role under this design

Reviewer feedback nailed the value proposition of `Declare`: it's the
type-level lock on column order. With inline inference for SELECT-side
operations, `Declare[T](table, cols...)` becomes the canonical way to write
INSERTs against a typed table — the column list is named once, at package
scope, and reuses across every insert site. Renaming a field's `db:"..."`
tag still produces the existing drift behavior (silent zeros on scan), but
reordering struct fields no longer affects INSERT order, because the order
lives in the Declare call.

So both forms exist for different reasons:

- **Inline `Select[T]("table")`** (no columns) for one-off reads. Inference
  fills in the SELECT list from the struct schema. Safe.
- **`Declare[T]("table", "a", "b", "c")`** at package scope for any code
  that issues INSERTs against this table. The explicit column list pins the
  insert order independent of struct layout, and the same Declaration's
  `.Select()` / `.Update()` / `.Delete()` reuse those columns for symmetry
  with the INSERT.

That's the actual reason `Declare` existed in the first place: to stop
having to write so many tokens in selects. Column inference makes it more
useful, not redundant.

## Proposed solution

### API change: inline inference on SELECT-side builders

When `Select[T]`/`Update[T]` (RETURNING)/`Delete[T]` (RETURNING) is called
with no explicit columns, fall back to `ColumnsOf[T]()`.

```go
// Before:
db.Select[Block]("block", "id", "story", "active", "type", "pos", "text", "png", "info").
    Where("story = ?")

// After:
db.Select[Block]("block").Where("story = ?")
```

Today `Select[T](table)` with no columns emits `SELECT *`. With this change
it emits the inferred column list. `SELECT *` becomes opt-in via an explicit
`"*"` column, or `db.SelectAll[T](table)` if a named helper is preferred —
that's an implementation choice. Document the migration in the package doc.

`Insert[T]` is intentionally **not** included. Inserter still requires
explicit insertions. Use `Declare[T](table, cols...).Insert(...)` to get the
deduplication benefit at INSERT sites.

### `Declare[T]` without columns

`Declare[T]("table")` (no columns) infers, treating the result like the
inline form but bound to a name. This is the convenience path for "I have a
type that maps to a table and I only do SELECTs against it." Trying to call
`.Insert()` on a column-less Declaration should error explicitly — pointing
the user at "pass columns to Declare to get inferred-INSERT behavior."

### Tag: `db:"col,ro"`

Mark a field as scanned in but never inserted, for SQLite-managed columns
(auto-increment PKs, `DEFAULT CURRENT_TIMESTAMP`, generated columns):

```go
type Block struct {
    ID        uint64    `db:"id,ro"`
    CreatedAt time.Time `db:"created_at,ro"`
    StoryID   uint64    `db:"story"`
    // ...
}
```

`ro` reads as "read-only" and is short enough that struct tags with `db`,
`json`, and possibly `yaml` stay manageable. It composes with the existing
`json` modifier in either order: `db:"info,json,ro"` or `db:"info,ro,json"`.

Under the current design, `ro` only matters for the `Declare.Insert(...)`
inferred-from-columns path (if implemented) — it tells the column resolver
which fields to exclude when generating an insertion list from a declared
column set. Pure-inline-inference doesn't touch INSERT, so `ro` could
arguably be deferred until inferred-INSERT is actually wanted. Implementer
call.

### `ColumnsOf[T]`

Expose a top-level helper for consumers who want the inferred column list
without going through a builder:

```go
cols, err := db.ColumnsOf[Block]()
// cols = []string{"id", "story", "active", "type", "pos", "text", "png", "info"}
```

Backed by a small exported function in `internal/scanner` that triggers
`findStructSchema` + `schema.build` (eager schema-error surfacing) and
returns field names in declaration order, skipping `db:"-"`, anonymous
fields, and unexported fields.

## Work

1. **Exported column resolver in scanner.**

   ```go
   // in internal/scanner:
   func ColumnsOf[T any]() ([]string, error)
   ```

   Calls `findStructSchema(reflect.TypeOf(*new(T)))`, triggers `schema.build`
   (so a bad tag errors immediately, not at first scan), then returns
   `schema.Fields[i].Name` in order. Top-level `db.ColumnsOf[T]` is a thin
   re-export.

2. **Wire into builders.** When `Select[T]`/`Update[T]`/`Delete[T]` (and
   the RETURNING method on each) is called with no columns, call
   `ColumnsOf[T]` and use the result. Cache on the builder so repeated
   `.SQL()` doesn't reflect-walk again. (Scanner cache makes this cheap, but
   one extra slice copy per SQL render is worth avoiding.)

3. **`Declare[T](table)` (no columns) → infer.** Same path. Call from
   `Declare`'s constructor when `insertions` is empty.

4. **Insert path stays explicit.** `Inserter[T]` doesn't gain a fallback.
   `Declare[T](table).Insert(...)` (column-less Declare followed by an
   inferred-Insert) errors — direct the user at `Declare[T](table, "a",
   "b").Insert(...)`.

5. **`db:"col,ro"` parsing.** Add `ReadOnly bool` to `fieldSchema`,
   recognized from the tag's modifier list. Used only by the column
   resolver's "insertable subset" path if/when that's implemented.

6. **Tests.** New tests, alongside the existing ones (don't migrate the
   explicit-columns tests — they pin a still-supported path that needs
   coverage):

   - Inferred columns from explicit `db:"col"` tags.
   - Inferred columns from default snake_case inference.
   - `db:"-"` exclusion.
   - JSON columns (`db:"col,json"`) appear as `col` in the column list.
   - `ColumnsOf[T]` direct call.
   - `Declare[T](table)` (no columns) + `.Select()` emits inferred columns.
   - `Declare[T](table)` + `.Insert(...)` errors clearly.
   - Explicit columns still override inferred (mostly: the existing tests
     cover this, but a single explicit-overrides-inferred test pins the
     intent).
   - `db:"col,ro"` tag round-trip via `ColumnsOf` with an "insertable
     subset" filter, if you implement that filter.

7. **Docs.** Update the package doc comment in `db.go` to describe:

   - Column inference: when it kicks in, what it does, how to opt out
     (`"*"`).
   - The SELECT/INSERT asymmetry explicitly. This is the load-bearing
     paragraph — anyone wondering why `Insert[T]("table")` doesn't infer
     should find the answer in the package doc, not have to read this spec.
   - `db:"-"` and `db:"col,ro"` semantics.
   - That field-declaration-order does **not** affect any inferred output
     (only column names matter).

## Risks / open questions

- **Pointer-to-struct fields in T.** Scanner handles pointer-to-struct for
  NULL semantics, but they don't have a single column name. Decide:
  flatten with a prefix (matching the scanner's existing `prefix +
  "_" + name` rule), or refuse during inference with a clear "use
  `db:"-"` to exclude this field" error. Easiest is "refuse"; flattening is
  defensible but introduces a new shape of column name.

- **Anonymous (embedded) fields.** Scanner currently skips fields with
  empty resolved names. Keep parity. Worth a one-line note in the doc.

- **Schema cache lifetime.** Already keyed by `reflect.Type`, lives forever.
  Fine for the small fixed set of medea types. One-line comment is enough.

- **`Select[T](table)` with no columns currently emits `SELECT *`.** With
  this change it emits the inferred list. Internal package tests are the
  only consumers; nothing external breaks. Mention in the package doc.

## Out of scope

- DDL generation / migrations.
- Reverse mapping (column → struct field outside the scanner's own use).
- Reflective binding of struct fields as INSERT values (separate feature:
  the current API takes positional `?` args).
- Inferred-INSERT. The reorder footgun above is why. A future feature that
  changes the INSERT API to use named-arg binding (e.g., a struct value
  rather than a `...any` tail) could reopen this — but it's a separate
  design conversation, not an extension of this one.

## First call sites to migrate after merge

In medea (`~/src/medea/medea/medea.go`):

- `ListSettings`, `FetchSetting`, `UpdateSetting` (RETURNING via Update, if
  used) — same `setting` column list, currently repeated.
- `ListStories`, `FetchStory`, `UpdateStory`, `FetchContext`,
  `FetchStorySetting` — same `story` column list.
- `FetchHistory`, `fetchBlocks`, `fetchBlock` — same `block` column list.

For each table, the cleanest pattern after this lands is:

```go
var stories = db.Declare[Story]("story")            // inferred columns
var settings = db.Declare[Setting]("setting")       // inferred columns
var blocks = db.Declare[Block]("block")             // inferred columns
```

…with INSERT sites staying on explicit-column `Declare` calls (or inline
`Insert[T](table, "a", "b", "c")`) so column order is pinned independent of
struct layout.
