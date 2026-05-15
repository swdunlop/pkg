// Package db provides a high-level API for interacting with SQLite databases using
// zombiezen.com/go/sqlite. It offers a type-safe way to map Go structs to SQL
// tables and columns, simplifying CRUD operations.
//
// # Struct Tagging and Scanning
//
// The package uses the "db" struct tag to map struct fields to database columns.
//
//	type User struct {
//	    ID       int    `db:"id"`
//	    Username string `db:"username"`
//	    Meta     Meta   `db:"meta,json"` // Scanned as JSON
//	}
//
// If the "db" tag is missing, the field name is converted to snake_case to infer
// the column name (e.g., "UserName" -> "user_name"). Use `db:"-"` to ignore a field.
//
// Nested struct fields (and pointer-to-struct fields) are flattened with a
// "parent_child" prefix to form the column name — a field named "Foo" of type
// Inner expands to columns "foo_a", "foo_b" matching Inner's own column list.
// Use `db:"-"` to exclude a field from this expansion.
//
// # Column inference: the SELECT/INSERT asymmetry
//
// This package distinguishes SELECT-side and INSERT-side column lists because
// they have very different reorder safety:
//
//   - SELECT, RETURNING, UPDATE-RETURNING, DELETE-RETURNING columns are matched
//     by NAME against the SQLite statement's output. Reordering the Go
//     struct's fields cannot misroute a value to the wrong destination.
//   - INSERT column lists are POSITIONAL. The caller writes the `?` value
//     tuple as `Exec(ctx, args...)`. If the package inferred the column list
//     from struct-field declaration order, *reordering a struct field* would
//     silently rebind every value to the wrong column — same types, still
//     compiles, may not be caught by schema constraints.
//
// The package therefore infers columns *only* for SELECT-side operations:
//
//   - [Select][T](table) with no columns → inferred from T.
//   - [Selector.Returning] / [Updater.Returning] / [Deleter.Returning] called
//     with no arguments → inferred from T.
//   - [Declare][T](table) with no columns → inferred from T, available to
//     downstream [Declaration.Select], [Declaration.Update], [Declaration.Delete].
//
// INSERT keeps explicit columns. Use [Declare][T](table, "a", "b") to pin the
// insert-column order at package scope, independent of struct layout, then
// call .Insert(...) at every insert site. A side benefit: tag renames in T
// now produce a loud SQLite "no such column" error against the inferred
// SELECT list rather than a silent zero-scan.
//
// To force a literal `SELECT *`, pass "*" as the only column:
// `db.Select[T](table, "*")`.
//
// [ColumnsOf][T] is exported for callers who want the inferred list without
// going through a builder.
//
// Column inference requires T to be a struct (a single pointer level is
// dereferenced). Non-struct T — basic types, slices, maps — must use explicit
// columns; inference returns an error rather than silently emitting SELECT *.
//
// # Supported Types
//
// The scanner supports the following Go types for column mapping:
//
//   - Basic types: string, int, int64, uint64, bool, float64, []byte
//   - Pointers: Supported (scans into the value or sets to nil if NULL)
//   - Nested structs: Supported via the "db" tag or inferred names.
//   - JSON Columns: Use `db:"colname,json"` to scan a JSON text column into a struct,
//     map, or slice field. This is useful for storing complex data types like
//     []string or configuration maps.
//
// # Implicit JSON for Top-Level Scan Targets
//
// When the scan target itself is a slice (other than []byte) or a map, the
// column's text is decoded as JSON without requiring a struct or tag:
//
//	tags, err := db.Select[[]string]("posts", "tags").Where("id=?").Exec1(ctx, 1)
//	meta, err := db.Select[map[string]int]("info", "data").Exec1(ctx)
//
// []byte remains the BLOB escape hatch and is never JSON-decoded. For JSON
// fields nested inside a struct, use the `db:"col,json"` tag.
//
// # SQL Safety
//
// Table names, column names, WHERE clauses, JOIN conditions, and other structural
// SQL fragments are interpolated directly into the generated SQL string. Only values
// bound via ? placeholders are parameterized. Callers must ensure that all structural
// string arguments (table names, column names, WHERE/JOIN/ORDER BY expressions) are
// not derived from untrusted input.
//
// # Context Handling
//
// Database connections are managed via the context. Use db.With(ctx, conn) to bind
// a connection to a context, and then pass that context to db functions.
//
// # Iterators
//
// Several methods return [iter.Seq] iterators that report errors via a pointer:
//
//	var err error
//	for user := range users.Select().Iter(ctx, &err) {
//	    fmt.Println(user.Name)
//	}
//	if err != nil {
//	    return err
//	}
package db

// TODO: add an interface with ScanColumns() map[string]func(stmt *sqlite.Stmt, col int) to override using reflection to scan a value.

import (
	"context"
	"encoding/json"
	"fmt"
	"iter"
	"math"
	"strings"
	"swdunlop.dev/pkg/db/internal/scanner"
	"sync/atomic"

	"zombiezen.com/go/sqlite"
)

// From returns the database connection from the current context. Returns nil if
// no connection is bound.
func From(ctx context.Context) *sqlite.Conn {
	conn, _ := ctx.Value(ctxDB{}).(*sqlite.Conn)
	return conn
}

// With binds a database connection into a new context that can be recovered using From.
func With(ctx context.Context, conn *sqlite.Conn) context.Context {
	return context.WithValue(ctx, ctxDB{}, conn)
}

type ctxDB struct{}

// Tx runs fn inside a SAVEPOINT transaction on the connection in ctx. If fn
// returns a non-nil error or panics, the savepoint is rolled back; otherwise it
// is released. Calls to Tx may be nested — each creates a new savepoint.
func Tx(ctx context.Context, fn func(context.Context) error) (rerr error) {
	conn := From(ctx)
	if conn == nil {
		return fmt.Errorf("missing sqlite connection")
	}
	name := fmt.Sprintf("_tx_%d", savepointN.Add(1))
	if err := execConn(conn, "SAVEPOINT "+name); err != nil {
		return err
	}
	panicked := true
	defer func() {
		if panicked || rerr != nil {
			execConn(conn, "ROLLBACK TO "+name)
			execConn(conn, "RELEASE "+name)
		} else {
			if err := execConn(conn, "RELEASE "+name); err != nil {
				rerr = err
			}
		}
	}()
	rerr = fn(ctx)
	panicked = false
	return rerr
}

// TxV is the value-returning form of [Tx]. The function runs inside a
// SAVEPOINT; on error or panic the savepoint is rolled back and the zero
// value of T is returned alongside the error. Use this instead of closing
// over a local variable from outside the transaction:
//
//	story, err := db.TxV(ctx, func(ctx context.Context) (*Story, error) {
//	    return medea.CreateStory(ctx, userID, spec)
//	})
func TxV[T any](ctx context.Context, fn func(context.Context) (T, error)) (T, error) {
	var ret T
	err := Tx(ctx, func(ctx context.Context) error {
		var err error
		ret, err = fn(ctx)
		return err
	})
	if err != nil {
		var zero T
		return zero, err
	}
	return ret, nil
}

var savepointN atomic.Uint64

// execConn executes a SQL statement on a connection without caching. Used for
// transaction control statements (SAVEPOINT, RELEASE, ROLLBACK TO) which have
// unique names and should not pollute the prepared-statement cache.
func execConn(conn *sqlite.Conn, sql string) error {
	stmt, _, err := conn.PrepareTransient(sql)
	if err != nil {
		return err
	}
	defer stmt.Finalize()
	_, err = stmt.Step()
	return err
}

// --- cons-list types ---

// cons is an immutable string cons-list used for WHERE, RETURNING, ORDER BY,
// GROUP BY, HAVING, and column lists.
type cons struct {
	item string
	next *cons
}

func (c *cons) prepend(s string) *cons { return &cons{s, c} }

// collect returns the items in insertion order (the list is stored in reverse).
func (c *cons) collect() []string {
	if c == nil {
		return nil
	}
	var n int
	for p := c; p != nil; p = p.next {
		n++
	}
	out := make([]string, n)
	for p := c; p != nil; p = p.next {
		n--
		out[n] = p.item
	}
	return out
}

// setEntry is a cons-list node for UPDATE SET clauses. expr is the SQL
// right-hand side ("?" for [Updater.Set], a caller-supplied expression for
// [Updater.SetExpr]); values are the bind args consumed by expr's placeholders.
type setEntry struct {
	column string
	expr   string
	values []any
	next   *setEntry
}

func (e *setEntry) collectColumns() []string {
	if e == nil {
		return nil
	}
	var n int
	for p := e; p != nil; p = p.next {
		n++
	}
	out := make([]string, n)
	for p := e; p != nil; p = p.next {
		n--
		out[n] = p.column
	}
	return out
}

// collectExprs returns the per-entry SQL right-hand side in insertion order,
// parallel to collectColumns.
func (e *setEntry) collectExprs() []string {
	if e == nil {
		return nil
	}
	var n int
	for p := e; p != nil; p = p.next {
		n++
	}
	out := make([]string, n)
	for p := e; p != nil; p = p.next {
		n--
		out[n] = p.expr
	}
	return out
}

// collectValues returns the flattened bind args from every entry, in insertion
// order. A Set("c", v) entry contributes one value; a SetExpr("c", "expr", a,
// b) entry contributes len(args) values.
func (e *setEntry) collectValues() []any {
	if e == nil {
		return nil
	}
	// Walk forward, stash in reverse order, then unwind so insertion order
	// is preserved when flattening variadic args.
	var entries []*setEntry
	for p := e; p != nil; p = p.next {
		entries = append(entries, p)
	}
	var out []any
	for i := len(entries) - 1; i >= 0; i-- {
		out = append(out, entries[i].values...)
	}
	return out
}

// joinEntry is a cons-list node for JOIN clauses.
type joinEntry struct {
	kind  string // "JOIN", "LEFT JOIN", etc.
	table string
	on    *cons
	next  *joinEntry
}

// valuesEntry is a cons-list node for multi-row INSERT VALUES.
type valuesEntry struct {
	placeholders *cons
	next         *valuesEntry
}

// --- Declaration ---

// ColumnsOf returns the column names a value of T expands into when scanned,
// inferred from struct tags (or snake_case'd field names when untagged). See
// [scanner.ColumnsOf] for the full rules; this is a re-export so callers can
// stay inside this package.
func ColumnsOf[T any]() ([]string, error) {
	return scanner.ColumnsOf[T]()
}

// Declare declares that there is a table with the specified columns in the
// database and associates it with a type, making it easier to select and
// insert.
//
// When no columns are passed, the column list is inferred from T's struct
// tags via [ColumnsOf]. Inference failures (e.g. T is not a struct) are
// stored on the Declaration and surfaced from any derived builder's Exec
// call.
func Declare[T any](table string, columns ...string) Declaration[T] {
	d := Declaration[T]{table: table, columns: columns}
	if len(columns) == 0 {
		inferred, err := ColumnsOf[T]()
		if err != nil {
			d.buildErr = err
		} else {
			d.columns = inferred
		}
	}
	return d
}

// A Declaration describes how a Go type is related to a table and a set of columns.
type Declaration[T any] struct {
	table    string
	columns  []string
	buildErr error
}

// Select returns a selector with the table and columns determined by the declaration.
func (cfg Declaration[T]) Select() Selector[T] {
	var cols *cons
	for _, c := range cfg.columns {
		cols = cols.prepend(c)
	}
	return Selector[T]{
		from:     cfg.table,
		columns:  cols,
		buildErr: cfg.buildErr,
	}
}

// Update returns an Updater with the table determined by the declaration.
// The declaration's columns are carried as the default RETURNING list, used
// when [Updater.Returning] is called with no arguments.
func (cfg Declaration[T]) Update() Updater[T] {
	return Updater[T]{
		table:    cfg.table,
		declCols: cfg.columns,
		buildErr: cfg.buildErr,
	}
}

// Insert returns an Inserter with the table and returned columns determined
// by the declaration.
//
// Calling Insert with no insertions on a Declaration that also has no columns
// (typically `Declare[T](table).Insert()` where T's column list was deferred
// to inference) is an error: there's nothing to insert and nothing to
// RETURNING. The error is surfaced at Exec time.
func (cfg Declaration[T]) Insert(insertions ...string) Inserter[T] {
	ins := Inserter[T]{table: cfg.table, buildErr: cfg.buildErr}
	if ins.buildErr == nil && len(insertions) == 0 {
		ins.buildErr = fmt.Errorf(`Declare(%q).Insert(): no insertions provided — pass insertion columns to Insert, e.g. Declare[T](%q, "a", "b").Insert("a", "b")`, cfg.table, cfg.table)
		return ins
	}
	var retCols *cons
	for _, c := range cfg.columns {
		retCols = retCols.prepend(c)
	}
	var insCols *cons
	var ph *cons
	for _, c := range insertions {
		insCols = insCols.prepend(c)
		ph = ph.prepend("?")
	}
	ins.returning = retCols
	ins.insertions = insCols
	ins.values = &valuesEntry{placeholders: ph}
	return ins
}

// Delete returns a Deleter with the table determined by the declaration.
// The declaration's columns are carried as the default RETURNING list, used
// when [Deleter.Returning] is called with no arguments.
func (cfg Declaration[T]) Delete() Deleter[T] {
	return Deleter[T]{
		table:    cfg.table,
		declCols: cfg.columns,
		buildErr: cfg.buildErr,
	}
}

// --- Select ---

// Select returns a Selector associated with a Go type, a SQL table and a list
// of column expressions.
//
// When no columns are specified, the column list is inferred from T's struct
// tags via [ColumnsOf]. Inference failure (e.g. T is not a struct) is stored
// on the Selector and surfaced at Exec time. To force a literal SELECT *, pass
// "*" as the only column.
func Select[T any](table string, columns ...string) Selector[T] {
	var cols *cons
	var buildErr error
	if len(columns) == 0 {
		inferred, err := ColumnsOf[T]()
		if err != nil {
			buildErr = err
		} else {
			columns = inferred
		}
	}
	for _, c := range columns {
		cols = cols.prepend(c)
	}
	return Selector[T]{
		from:     table,
		columns:  cols,
		buildErr: buildErr,
	}
}

// A Selector associates a Go type with a specification for how to select information from
// the database.
type Selector[T any] struct {
	from     string
	joins    *joinEntry
	columns  *cons
	where    *cons
	groupBy  *cons
	having   *cons
	orderBy  *cons
	limit    int
	offset   int
	buildErr error
}

// Join adds a JOIN to the selection using the named table.
func (cfg Selector[T]) Join(table string, on ...string) Selector[T] {
	var onCons *cons
	for _, o := range on {
		onCons = onCons.prepend(o)
	}
	cfg.joins = &joinEntry{kind: "JOIN", table: table, on: onCons, next: cfg.joins}
	return cfg
}

// LeftJoin adds a LEFT JOIN to the selection using the named table.
func (cfg Selector[T]) LeftJoin(table string, on ...string) Selector[T] {
	var onCons *cons
	for _, o := range on {
		onCons = onCons.prepend(o)
	}
	cfg.joins = &joinEntry{kind: "LEFT JOIN", table: table, on: onCons, next: cfg.joins}
	return cfg
}

// Where adds constraining clauses to the selector. Each clause is logically AND,
// successive uses of WHERE will also be AND.
// To express OR conditions, combine them in a single clause string: .Where("a = ? OR b = ?")
func (cfg Selector[T]) Where(clauses ...string) Selector[T] {
	for _, c := range clauses {
		cfg.where = cfg.where.prepend(c)
	}
	return cfg
}

// GroupBy adds a GROUP BY clause to the selector.
func (cfg Selector[T]) GroupBy(columns ...string) Selector[T] {
	for _, c := range columns {
		cfg.groupBy = cfg.groupBy.prepend(c)
	}
	return cfg
}

// Having adds a HAVING clause to the selector. Each clause is logically AND.
func (cfg Selector[T]) Having(clauses ...string) Selector[T] {
	for _, c := range clauses {
		cfg.having = cfg.having.prepend(c)
	}
	return cfg
}

// OrderBy adds an ORDER BY clause to the selector.
func (cfg Selector[T]) OrderBy(columns ...string) Selector[T] {
	for _, c := range columns {
		cfg.orderBy = cfg.orderBy.prepend(c)
	}
	return cfg
}

// Limit adds a LIMIT clause to the selector.
func (cfg Selector[T]) Limit(limit int) Selector[T] {
	cfg.limit = limit
	return cfg
}

// Offset adds an OFFSET clause to the selector.
func (cfg Selector[T]) Offset(offset int) Selector[T] {
	cfg.offset = offset
	return cfg
}

// Exec1 executes the query and returns a single result, or an error if there were no results.
func (cfg Selector[T]) Exec1(ctx context.Context, args ...any) (ret T, err error) {
	if cfg.buildErr != nil {
		return ret, cfg.buildErr
	}
	stmt, err := start(ctx, cfg.SQL(), args...)
	if err != nil {
		return
	}
	return scanner.Get[T](stmt)
}

// ExecN executes the query and returns a slice of results.
func (cfg Selector[T]) ExecN(ctx context.Context, args ...any) (ret []T, err error) {
	if cfg.buildErr != nil {
		return nil, cfg.buildErr
	}
	stmt, err := start(ctx, cfg.SQL(), args...)
	if err != nil {
		return
	}
	return scanner.List[T](stmt)
}

// Iter returns a Go iterator. If an error is encountered, *rerr will be updated and the iterator will stop.
func (cfg Selector[T]) Iter(ctx context.Context, rerr *error, args ...any) iter.Seq[T] {
	if cfg.buildErr != nil {
		*rerr = cfg.buildErr
		return func(yield func(T) bool) {}
	}
	stmt, err := start(ctx, cfg.SQL(), args...)
	if err != nil {
		*rerr = err
		return func(yield func(T) bool) { *rerr = err }
	}
	return scanner.Iter[T](rerr, stmt)
}

// SQL returns the SQL string for the selector.
func (cfg Selector[T]) SQL() string {
	var buf strings.Builder
	buf.WriteString(`SELECT `)
	columns := cfg.columns.collect()
	if len(columns) > 0 {
		buildList(&buf, `, `, columns[0], columns[1:]...)
	} else {
		buf.WriteString(`*`)
	}
	buf.WriteString(` FROM `)
	buf.WriteString(cfg.from)

	// Joins are stored in reverse order (most recent prepended), collect them
	joins := collectJoins(cfg.joins)
	for _, j := range joins {
		buf.WriteString(` `)
		buf.WriteString(j.kind)
		buf.WriteString(` `)
		buf.WriteString(j.table)
		buf.WriteString(` ON `)
		onItems := j.on.collect()
		buildList(&buf, ` AND `, onItems[0], onItems[1:]...)
	}

	buildWhere(&buf, cfg.where.collect()...)

	if gb := cfg.groupBy.collect(); len(gb) > 0 {
		buf.WriteString(` GROUP BY `)
		buildList(&buf, `, `, gb[0], gb[1:]...)
	}
	if hv := cfg.having.collect(); len(hv) > 0 {
		buf.WriteString(` HAVING (`)
		buildList(&buf, `) AND (`, hv[0], hv[1:]...)
		buf.WriteString(`)`)
	}
	if ob := cfg.orderBy.collect(); len(ob) > 0 {
		buf.WriteString(` ORDER BY `)
		buildList(&buf, `, `, ob[0], ob[1:]...)
	}
	if cfg.limit > 0 {
		fmt.Fprintf(&buf, ` LIMIT %d`, cfg.limit)
	}
	if cfg.offset > 0 {
		fmt.Fprintf(&buf, ` OFFSET %d`, cfg.offset)
	}
	return buf.String()
}

// collectJoins reverses the prepended join list into insertion order.
func collectJoins(j *joinEntry) []joinEntry {
	if j == nil {
		return nil
	}
	var n int
	for p := j; p != nil; p = p.next {
		n++
	}
	out := make([]joinEntry, n)
	for p := j; p != nil; p = p.next {
		n--
		out[n] = *p
	}
	return out
}

// --- Exec / Update (raw SQL) ---

// Exec executes a statement and returns the number of rows affected.
func Exec(ctx context.Context, sql string, args ...any) (int, error) {
	conn := From(ctx)
	if conn == nil {
		return 0, fmt.Errorf(`missing sqlite connection`)
	}
	stmt, err := conn.Prepare(sql)
	if err != nil {
		return 0, fmt.Errorf("prepare %q: %w", sql, err)
	}
	if err := bindStatement(stmt, args...); err != nil {
		return 0, fmt.Errorf("bind %q: %w", sql, err)
	}
	for {
		hasRow, err := stmt.Step()
		if err != nil {
			return 0, err
		}
		if !hasRow {
			break
		}
	}
	return conn.Changes(), nil
}

// --- Updater ---

// An Updater constructs an UPDATE statement.
type Updater[T any] struct {
	table     string
	sets      *setEntry
	where     *cons
	returning *cons
	// declCols is the column list carried over from [Declaration.Update],
	// used as the default RETURNING list when [Updater.Returning] is called
	// with no arguments. Empty for updaters not created from a declaration.
	declCols []string
	buildErr error
}

// Set adds a SET clause to the update statement that binds value as a
// parameter (column = ?). Use SetExpr when the right-hand side needs to
// reference other columns or call SQL functions.
func (cfg Updater[T]) Set(column string, value any) Updater[T] {
	cfg.sets = &setEntry{column: column, expr: `?`, values: []any{value}, next: cfg.sets}
	return cfg
}

// SetExpr adds a SET clause using a raw SQL expression for the right-hand
// side. Use this when the value depends on the row's current state or another
// column, e.g. SET value = value + 1. Any ? placeholders in expr consume args
// in order, before any WHERE-clause args:
//
//	db.Update[struct{}]("clock").
//	    SetExpr("value", "value + ?", 1).
//	    Where("id = ?").
//	    Exec(ctx, 1)
func (cfg Updater[T]) SetExpr(column string, expr string, args ...any) Updater[T] {
	cfg.sets = &setEntry{column: column, expr: expr, values: args, next: cfg.sets}
	return cfg
}

// Where adds constraining clauses to the updater.
// To express OR conditions, combine them in a single clause string: .Where("a = ? OR b = ?")
func (cfg Updater[T]) Where(clauses ...string) Updater[T] {
	for _, c := range clauses {
		cfg.where = cfg.where.prepend(c)
	}
	return cfg
}

// Returning specifies the returned values from the update.
//
// When no columns are passed, the list defaults to the Declaration's columns
// (if this Updater was created via [Declaration.Update]); otherwise it is
// inferred from T's struct tags via [ColumnsOf]. Inference failure (e.g. T is
// not a struct) is stored on the Updater and surfaced at Exec time.
func (cfg Updater[T]) Returning(columns ...string) Updater[T] {
	if len(columns) == 0 {
		if len(cfg.declCols) > 0 {
			columns = cfg.declCols
		} else {
			inferred, err := ColumnsOf[T]()
			if err != nil {
				if cfg.buildErr == nil {
					cfg.buildErr = err
				}
				return cfg
			}
			columns = inferred
		}
	}
	for _, c := range columns {
		cfg.returning = cfg.returning.prepend(c)
	}
	return cfg
}

// Exec executes the update statement and returns the number of rows affected.
// If Returning() was called, use Exec1, ExecN, or Iter instead to capture results.
func (cfg Updater[T]) Exec(ctx context.Context, args ...any) (count int, err error) {
	if cfg.buildErr != nil {
		return 0, cfg.buildErr
	}
	if cfg.sets == nil {
		return 0, fmt.Errorf("updater has no SET clauses")
	}
	if cfg.returning != nil {
		return 0, fmt.Errorf("updater has RETURNING clause; use Exec1, ExecN, or Iter instead of Exec")
	}
	allArgs := append(cfg.sets.collectValues(), args...)
	return Exec(ctx, cfg.SQL(), allArgs...)
}

// Exec1 executes the update statement and returns a single result.
func (cfg Updater[T]) Exec1(ctx context.Context, args ...any) (ret T, err error) {
	if cfg.buildErr != nil {
		return ret, cfg.buildErr
	}
	if cfg.sets == nil {
		return ret, fmt.Errorf("updater has no SET clauses")
	}
	allArgs := append(cfg.sets.collectValues(), args...)
	stmt, err := start(ctx, cfg.SQL(), allArgs...)
	if err != nil {
		return
	}
	return scanner.Get[T](stmt)
}

// ExecN executes the update statement and returns a slice of results.
func (cfg Updater[T]) ExecN(ctx context.Context, args ...any) (ret []T, err error) {
	if cfg.buildErr != nil {
		return nil, cfg.buildErr
	}
	if cfg.sets == nil {
		return nil, fmt.Errorf("updater has no SET clauses")
	}
	allArgs := append(cfg.sets.collectValues(), args...)
	stmt, err := start(ctx, cfg.SQL(), allArgs...)
	if err != nil {
		return
	}
	return scanner.List[T](stmt)
}

// Iter returns a Go iterator for the results of the update.
func (cfg Updater[T]) Iter(ctx context.Context, rerr *error, args ...any) iter.Seq[T] {
	if cfg.buildErr != nil {
		*rerr = cfg.buildErr
		return func(yield func(T) bool) {}
	}
	if cfg.sets == nil {
		err := fmt.Errorf("updater has no SET clauses")
		*rerr = err
		return func(yield func(T) bool) {}
	}
	allArgs := append(cfg.sets.collectValues(), args...)
	stmt, err := start(ctx, cfg.SQL(), allArgs...)
	if err != nil {
		*rerr = err
		return func(yield func(T) bool) { *rerr = err }
	}
	return scanner.Iter[T](rerr, stmt)
}

// SQL returns the SQL string for the updater.
func (cfg Updater[T]) SQL() string {
	var buf strings.Builder
	buf.WriteString(`UPDATE `)
	buf.WriteString(cfg.table)
	buf.WriteString(` SET `)
	cols := cfg.sets.collectColumns()
	exprs := cfg.sets.collectExprs()
	for i, col := range cols {
		if i > 0 {
			buf.WriteString(`, `)
		}
		buf.WriteString(col)
		buf.WriteString(` = `)
		buf.WriteString(exprs[i])
	}
	buildWhere(&buf, cfg.where.collect()...)
	buildReturning(&buf, cfg.returning.collect()...)
	return buf.String()
}

// --- Inserter ---

// InsertInto returns a builder for an INSERT statement without a type parameter.
// This is a convenience for Insert[struct{}] when RETURNING is not needed.
func InsertInto(table string, columns ...string) Inserter[struct{}] {
	return Insert[struct{}](table, columns...)
}

// Insert returns a builder that will insert the specified columns into a table.
func Insert[T any](table string, insertions ...string) Inserter[T] {
	var insCols *cons
	var ph *cons
	for _, c := range insertions {
		insCols = insCols.prepend(c)
		ph = ph.prepend("?")
	}
	return Inserter[T]{
		table:      table,
		insertions: insCols,
		values:     &valuesEntry{placeholders: ph},
	}
}

// An Inserter constructs an INSERT statement.
type Inserter[T any] struct {
	table      string
	insertions *cons
	returning  *cons
	values     *valuesEntry
	onConflict string
	buildErr   error
}

// OnConflict adds an ON CONFLICT clause to the insert statement.
func (cfg Inserter[T]) OnConflict(clause string) Inserter[T] {
	cfg.onConflict = clause
	return cfg
}

// Returning specifies the returned values from the insert. You must use this if you want to
// scan results unless the inserter was created from a Declaration, since the Declaration
// specifies this for you.
func (cfg Inserter[T]) Returning(values ...string) Inserter[T] {
	for _, v := range values {
		cfg.returning = cfg.returning.prepend(v)
	}
	return cfg
}

// Values adds another row of positional placeholders for multi-row insert.
func (cfg Inserter[T]) Values(placeholders ...string) Inserter[T] {
	var ph *cons
	for _, p := range placeholders {
		ph = ph.prepend(p)
	}
	cfg.values = &valuesEntry{placeholders: ph, next: cfg.values}
	return cfg
}

// Exec executes the insert statement and returns the number of rows inserted.
// If Returning() was called, use Exec1, ExecN, or Iter instead to capture results.
func (cfg Inserter[T]) Exec(ctx context.Context, args ...any) (int, error) {
	if cfg.buildErr != nil {
		return 0, cfg.buildErr
	}
	if cfg.returning != nil {
		return 0, fmt.Errorf("inserter has RETURNING clause; use Exec1, ExecN, or Iter instead of Exec")
	}
	return Exec(ctx, cfg.SQL(), args...)
}

// Exec1 executes the insert statement and returns a single result.
// Returns io.EOF if there was no result.
func (cfg Inserter[T]) Exec1(ctx context.Context, args ...any) (result T, err error) {
	if cfg.buildErr != nil {
		return result, cfg.buildErr
	}
	stmt, err := start(ctx, cfg.SQL(), args...)
	if err != nil {
		return
	}
	return scanner.Get[T](stmt)
}

// ExecN executes the insert statement and returns a slice of results.
func (cfg Inserter[T]) ExecN(ctx context.Context, args ...any) (ret []T, err error) {
	if cfg.buildErr != nil {
		return nil, cfg.buildErr
	}
	stmt, err := start(ctx, cfg.SQL(), args...)
	if err != nil {
		return
	}
	return scanner.List[T](stmt)
}

// Iter returns a Go iterator for the results of the insert.
func (cfg Inserter[T]) Iter(ctx context.Context, rerr *error, args ...any) iter.Seq[T] {
	if cfg.buildErr != nil {
		*rerr = cfg.buildErr
		return func(yield func(T) bool) {}
	}
	stmt, err := start(ctx, cfg.SQL(), args...)
	if err != nil {
		*rerr = err
		return func(yield func(T) bool) { *rerr = err }
	}
	return scanner.Iter[T](rerr, stmt)
}

// SQL returns the SQL string for the inserter.
func (cfg Inserter[T]) SQL() string {
	var buf strings.Builder
	buf.WriteString(`INSERT INTO `)
	buf.WriteString(cfg.table)
	buf.WriteString(`(`)
	ins := cfg.insertions.collect()
	buildList(&buf, `, `, ins[0], ins[1:]...)
	buf.WriteString(`) VALUES `)

	// Collect values entries in insertion order (reverse the cons-list)
	rows := collectValues(cfg.values)
	for i, row := range rows {
		if i > 0 {
			buf.WriteString(`, `)
		}
		buf.WriteString(`(`)
		ph := row.collect()
		buildList(&buf, `, `, ph[0], ph[1:]...)
		buf.WriteString(`)`)
	}

	if cfg.onConflict != `` {
		buf.WriteString(` ON CONFLICT `)
		buf.WriteString(cfg.onConflict)
	}
	buildReturning(&buf, cfg.returning.collect()...)
	return buf.String()
}

// collectValues reverses the valuesEntry cons-list into insertion order.
func collectValues(v *valuesEntry) []*cons {
	if v == nil {
		return nil
	}
	var n int
	for p := v; p != nil; p = p.next {
		n++
	}
	out := make([]*cons, n)
	for p := v; p != nil; p = p.next {
		n--
		out[n] = p.placeholders
	}
	return out
}

// --- Deleter ---

// DeleteFrom returns a builder for a DELETE statement without a type parameter.
// This is a convenience for Delete[struct{}] when RETURNING is not needed.
func DeleteFrom(table string) Deleter[struct{}] {
	return Deleter[struct{}]{table: table}
}

// Delete returns a builder for a DELETE statement on the given table.
func Delete[T any](table string) Deleter[T] {
	return Deleter[T]{table: table}
}

// A Deleter constructs a DELETE statement.
type Deleter[T any] struct {
	table     string
	where     *cons
	returning *cons
	// declCols is the column list carried over from [Declaration.Delete],
	// used as the default RETURNING list when [Deleter.Returning] is called
	// with no arguments. Empty for deleters not created from a declaration.
	declCols []string
	buildErr error
}

// Where adds constraining clauses to the deleter.
// To express OR conditions, combine them in a single clause string: .Where("a = ? OR b = ?")
func (cfg Deleter[T]) Where(clauses ...string) Deleter[T] {
	for _, c := range clauses {
		cfg.where = cfg.where.prepend(c)
	}
	return cfg
}

// Returning specifies the returned values from the delete.
//
// When no columns are passed, the list defaults to the Declaration's columns
// (if this Deleter was created via [Declaration.Delete]); otherwise it is
// inferred from T's struct tags via [ColumnsOf]. Inference failure (e.g. T is
// not a struct) is stored on the Deleter and surfaced at Exec time.
func (cfg Deleter[T]) Returning(columns ...string) Deleter[T] {
	if len(columns) == 0 {
		if len(cfg.declCols) > 0 {
			columns = cfg.declCols
		} else {
			inferred, err := ColumnsOf[T]()
			if err != nil {
				if cfg.buildErr == nil {
					cfg.buildErr = err
				}
				return cfg
			}
			columns = inferred
		}
	}
	for _, c := range columns {
		cfg.returning = cfg.returning.prepend(c)
	}
	return cfg
}

// Exec executes the delete statement and returns the number of rows affected.
func (cfg Deleter[T]) Exec(ctx context.Context, args ...any) (count int, err error) {
	if cfg.buildErr != nil {
		return 0, cfg.buildErr
	}
	if cfg.returning != nil {
		return 0, fmt.Errorf("deleter has RETURNING clause; use Exec1, ExecN, or Iter instead of Exec")
	}
	return Exec(ctx, cfg.SQL(), args...)
}

// Exec1 executes the delete statement and returns a single result.
func (cfg Deleter[T]) Exec1(ctx context.Context, args ...any) (ret T, err error) {
	if cfg.buildErr != nil {
		return ret, cfg.buildErr
	}
	stmt, err := start(ctx, cfg.SQL(), args...)
	if err != nil {
		return
	}
	return scanner.Get[T](stmt)
}

// ExecN executes the delete statement and returns a slice of results.
func (cfg Deleter[T]) ExecN(ctx context.Context, args ...any) (ret []T, err error) {
	if cfg.buildErr != nil {
		return nil, cfg.buildErr
	}
	stmt, err := start(ctx, cfg.SQL(), args...)
	if err != nil {
		return
	}
	return scanner.List[T](stmt)
}

// Iter returns a Go iterator for the results of the delete.
func (cfg Deleter[T]) Iter(ctx context.Context, rerr *error, args ...any) iter.Seq[T] {
	if cfg.buildErr != nil {
		*rerr = cfg.buildErr
		return func(yield func(T) bool) {}
	}
	stmt, err := start(ctx, cfg.SQL(), args...)
	if err != nil {
		*rerr = err
		return func(yield func(T) bool) { *rerr = err }
	}
	return scanner.Iter[T](rerr, stmt)
}

// SQL returns the SQL string for the deleter.
func (cfg Deleter[T]) SQL() string {
	var buf strings.Builder
	buf.WriteString(`DELETE FROM `)
	buf.WriteString(cfg.table)
	buildWhere(&buf, cfg.where.collect()...)
	buildReturning(&buf, cfg.returning.collect()...)
	return buf.String()
}

// --- Internal helpers ---

// start will prepare a statement and bind it with arguments; the returned statement should not be finalized -- this lets
// the sqlite package reuse the resulting statement.
func start(ctx context.Context, sql string, args ...any) (stmt *sqlite.Stmt, err error) {
	conn := From(ctx)
	if conn == nil {
		return nil, fmt.Errorf(`missing sqlite connection`)
	}
	stmt, err = conn.Prepare(sql) // note: prep caches the statement in the connection unless we finalize it.
	if err != nil {
		return nil, fmt.Errorf("prepare %q: %w", sql, err)
	}
	err = bindStatement(stmt, args...)
	if err != nil {
		return nil, fmt.Errorf("bind %q: %w", sql, err)
	}
	return stmt, nil
}

// JSON wraps a value so bindStatement encodes it as JSON text. This is the
// bind-side companion to the `db:"col,json"` scan tag, and lets callers pass
// structs, maps, or slices to Exec / Exec1 / ExecN without pre-marshaling:
//
//	db.Exec(ctx, `INSERT INTO t(info) VALUES (?)`, db.JSON(myStruct))
//
// json.RawMessage values are bound as TEXT directly without re-marshaling.
func JSON(v any) JSONArg { return JSONArg{value: v} }

// JSONArg is the opaque wrapper returned by [JSON]. The field is unexported so
// callers route everything through JSON; bindStatement detects the type and
// marshals.
type JSONArg struct{ value any }

// bindStatement binds the values to the prepared statement parameters.
func bindStatement(stmt *sqlite.Stmt, values ...any) error {
	for i, v := range values {
		idx := i + 1
		switch v := v.(type) {
		case nil:
			stmt.BindNull(idx)
		case string:
			stmt.BindText(idx, v)
		case int:
			stmt.BindInt64(idx, int64(v))
		case int8:
			stmt.BindInt64(idx, int64(v))
		case int16:
			stmt.BindInt64(idx, int64(v))
		case int32:
			stmt.BindInt64(idx, int64(v))
		case int64:
			stmt.BindInt64(idx, v)
		case uint:
			stmt.BindInt64(idx, int64(v))
		case uint8:
			stmt.BindInt64(idx, int64(v))
		case uint16:
			stmt.BindInt64(idx, int64(v))
		case uint32:
			stmt.BindInt64(idx, int64(v))
		case uint64:
			if v > math.MaxInt64 {
				return fmt.Errorf("uint64 value %d overflows int64", v)
			}
			stmt.BindInt64(idx, int64(v))
		case float32:
			stmt.BindFloat(idx, float64(v))
		case float64:
			stmt.BindFloat(idx, v)
		case bool:
			stmt.BindBool(idx, v)
		case []byte:
			stmt.BindBytes(idx, v)
		case json.RawMessage:
			// Already-encoded JSON; bind as TEXT (not BLOB) since SQLite's
			// JSON path operators expect text. Must precede []byte if a
			// case for []byte ever moves above this — type-switch matches
			// the named type exactly.
			stmt.BindText(idx, string(v))
		case JSONArg:
			raw, err := json.Marshal(v.value)
			if err != nil {
				return fmt.Errorf("marshal json arg %d: %w", idx, err)
			}
			stmt.BindText(idx, string(raw))
		default:
			return fmt.Errorf("unsupported bind type %T", v)
		}
	}
	return nil
}

func buildWhere(buf *strings.Builder, where ...string) {
	if len(where) == 0 {
		return
	}
	buf.WriteString(` WHERE (`)
	buildList(buf, `) AND (`, where[0], where[1:]...)
	buf.WriteString(`)`)
}

func buildReturning(buf *strings.Builder, columns ...string) {
	if len(columns) == 0 {
		return
	}
	buf.WriteString(` RETURNING `)
	buildList(buf, `, `, columns[0], columns[1:]...)
}

func buildList(buf *strings.Builder, sep string, first string, rest ...string) {
	buf.WriteString(first)
	for _, item := range rest {
		buf.WriteString(sep)
		buf.WriteString(item)
	}
}
