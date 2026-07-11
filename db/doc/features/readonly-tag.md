# Feature: `db:"col,ro"` tag for read-only / SQLite-managed columns

## Problem

SQLite tables commonly have columns that the application reads but never
writes: auto-increment primary keys, `DEFAULT CURRENT_TIMESTAMP`, generated
columns, trigger-populated audit fields. With inferred column lists (see
`FEATURE-column-inference.md`), there's currently no way to say "this field
is part of the row I scan out, but exclude it from any inferred *write*
column list."

This only matters once an inferred-INSERT (or inferred-UPDATE-SET) path
exists. Pure-inline inference for SELECT-side operations does not need this
tag — every scanned field is a column you want to read.

## Proposed tag

```go
type Block struct {
    ID        uint64    `db:"id,ro"`
    CreatedAt time.Time `db:"created_at,ro"`
    StoryID   uint64    `db:"story"`
}
```

`ro` reads as "read-only" — short enough that tags with `db`, `json`, and
possibly `yaml` stay manageable. Composes with `json` in either order:
`db:"info,json,ro"` or `db:"info,ro,json"`.

## Semantics

- A field with `ro` is included in `ColumnsOf[T]()` (the SELECT-side
  inference path) — you still read this column.
- A field with `ro` is **excluded** from any "insertable subset" /
  "updateable subset" inference helper, once those exist.
- No effect on hand-written explicit column lists. The tag is purely
  advisory for inference.

## Implementation sketch

1. Add `ReadOnly bool` to `fieldSchema` in `internal/scanner/scanner.go`.
2. Recognize `"ro"` in the modifier list during `fieldSchema.build`.
3. Add `WritableColumnsOf[T]()` (or rename — open) that returns the same
   list as `ColumnsOf[T]` minus `ro` fields, alongside a corresponding
   cached `WritableNames []string` slice on `structSchema`.
4. Wire into whatever inferred-INSERT or inferred-UPDATE-SET path is being
   added at the time.

## Why deferred from the column-inference feature

The column-inference feature is explicitly SELECT-side only. INSERT
inference is out of scope there because struct-field-order is not safe as
the source of insert-column-order (see that spec's "design constraint"
section). Until there's a feature that *needs* the insertable-subset
distinction, `ro` would parse with no behavioral effect — pure dead weight
in the tag parser.

This file exists so the tag spelling is preserved when the time comes.
Pick this up alongside whichever feature introduces named-arg INSERT
binding or a similar reorder-safe write path.
