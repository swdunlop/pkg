package db_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"swdunlop.dev/pkg/db"

	"zombiezen.com/go/sqlite"
)

func setup(t *testing.T) context.Context {
	conn, err := sqlite.OpenConn(":memory:", 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return db.With(context.Background(), conn)
}

func exec(t *testing.T, ctx context.Context, sql string) {
	conn := db.From(ctx)
	stmt, err := conn.Prepare(sql)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stmt.Finalize() }()
	if _, err := stmt.Step(); err != nil {
		t.Fatal(err)
	}
}

func TestSelect_Struct(t *testing.T) {
	ctx := setup(t)

	exec(t, ctx, "CREATE TABLE users (id INTEGER, name TEXT)")
	exec(t, ctx, "INSERT INTO users VALUES (1, 'alice')")
	exec(t, ctx, "INSERT INTO users VALUES (2, 'bob')")

	type User struct {
		ID   int
		Name string
	}

	// Select All
	users, err := db.Select[User]("users").ExecN(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(users) != 2 {
		t.Errorf("expected 2 users, got %d", len(users))
	}
	if users[0].Name != "alice" {
		t.Errorf("expected alice, got %s", users[0].Name)
	}

	// Select One with Where
	user, err := db.Select[User]("users").Where("id = ?").Exec1(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}
	if user.Name != "bob" {
		t.Errorf("expected bob, got %s", user.Name)
	}

	// Select Columns (override struct default?)
	users2, err := db.Select[User]("users", "name").Where("id = ?").ExecN(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(users2) != 1 {
		t.Fatal("expected 1 user")
	}
	if users2[0].Name != "alice" {
		t.Error("expected alice")
	}
	if users2[0].ID != 0 {
		t.Error("expected ID 0 since it wasn't selected")
	}
}

func TestInsert_Returning(t *testing.T) {
	ctx := setup(t)
	exec(t, ctx, "CREATE TABLE items (id INTEGER PRIMARY KEY, val TEXT)")

	id, err := db.Insert[int]("items", "val").Returning("id").Exec1(ctx, "test")
	if err != nil {
		t.Fatal(err)
	}
	if id != 1 {
		t.Errorf("expected id 1, got %d", id)
	}

	id2, err := db.Insert[int]("items", "val").Returning("id").Exec1(ctx, "test2")
	if err != nil {
		t.Fatal(err)
	}
	if id2 != 2 {
		t.Errorf("expected id 2, got %d", id2)
	}
}

func TestInsert_ExecN(t *testing.T) {
	ctx := setup(t)
	exec(t, ctx, "CREATE TABLE items (id INTEGER PRIMARY KEY, val TEXT)")

	// Insert multiple rows and get all IDs back
	ids, err := db.Insert[int]("items", "val").
		Values("?").
		Returning("id").
		ExecN(ctx, "a", "b")
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 {
		t.Errorf("expected 2 ids, got %d", len(ids))
	}
}

func TestScalarSelect(t *testing.T) {
	ctx := setup(t)
	exec(t, ctx, "CREATE TABLE nums (val INTEGER)")
	exec(t, ctx, "INSERT INTO nums VALUES (100)")

	val, err := db.Select[int]("nums", "val").Exec1(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if val != 100 {
		t.Errorf("expected 100, got %d", val)
	}
}

func TestUpdate(t *testing.T) {
	ctx := setup(t)
	exec(t, ctx, "CREATE TABLE config (key TEXT, val TEXT)")
	exec(t, ctx, "INSERT INTO config VALUES ('theme', 'light')")

	count, err := db.Exec(ctx, "UPDATE config SET val = ? WHERE key = ?", "dark", "theme")
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("expected 1 update, got %d", count)
	}

	// Verify update
	type Config struct {
		Key string
		Val string
	}
	c, err := db.Select[Config]("config").Where("key = ?").Exec1(ctx, "theme")
	if err != nil {
		t.Fatal(err)
	}
	if c.Val != "dark" {
		t.Errorf("expected dark, got %s", c.Val)
	}
}

func TestUpdater(t *testing.T) {
	ctx := setup(t)
	exec(t, ctx, "CREATE TABLE users (id INTEGER, name TEXT, active INTEGER)")
	exec(t, ctx, "INSERT INTO users VALUES (1, 'bob', 0)")

	type User struct {
		ID     int
		Name   string
		Active bool
	}
	users := db.Declare[User]("users", "id", "name", "active")

	count, err := users.Update().Set("active", 1).Where("id = ?").Exec(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("expected 1 update, got %d", count)
	}

	u, err := users.Select().Where("id = ?").Exec1(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !u.Active {
		t.Error("expected active")
	}
}

func TestUpdater_Returning(t *testing.T) {
	ctx := setup(t)
	exec(t, ctx, "CREATE TABLE counters (id INTEGER PRIMARY KEY, val INTEGER)")
	exec(t, ctx, "INSERT INTO counters VALUES (1, 10)")
	exec(t, ctx, "INSERT INTO counters VALUES (2, 20)")

	// Exec1 with Returning
	type Counter struct {
		ID  int
		Val int
	}
	counters := db.Declare[Counter]("counters", "id", "val")

	c1, err := counters.Update().Set("val", 11).Where("id = ?").Returning("id", "val").Exec1(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if c1.Val != 11 {
		t.Errorf("expected 11, got %d", c1.Val)
	}

	// ExecN with Returning (update multiple rows)
	c2, err := counters.Update().Set("val", 30).Where("val > ?").Returning("id", "val").ExecN(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(c2) != 2 {
		t.Errorf("expected 2 updates, got %d", len(c2))
	}
	for _, c := range c2 {
		if c.Val != 30 {
			t.Errorf("expected 30, got %d", c.Val)
		}
	}

	// Iter with Returning
	var rerr error
	iter := counters.Update().Set("val", 40).Where("val > ?").Returning("id", "val").Iter(ctx, &rerr, 20)
	count := 0
	for c := range iter {
		count++
		if c.Val != 40 {
			t.Errorf("expected 40, got %d", c.Val)
		}
	}
	if rerr != nil {
		t.Fatal(rerr)
	}
	if count != 2 {
		t.Errorf("expected 2 updates via Iter, got %d", count)
	}
}

func TestExec(t *testing.T) {
	ctx := setup(t)
	_, err := db.Exec(ctx, "CREATE TABLE foo (id INTEGER)")
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(ctx, "INSERT INTO foo VALUES (1)")
	if err != nil {
		t.Fatal(err)
	}
}

func TestFrom_NilContext(t *testing.T) {
	conn := db.From(context.Background())
	if conn != nil {
		t.Error("expected nil connection from empty context")
	}
}

func TestDelete(t *testing.T) {
	ctx := setup(t)
	exec(t, ctx, "CREATE TABLE items (id INTEGER PRIMARY KEY, val TEXT)")
	exec(t, ctx, "INSERT INTO items VALUES (1, 'a')")
	exec(t, ctx, "INSERT INTO items VALUES (2, 'b')")
	exec(t, ctx, "INSERT INTO items VALUES (3, 'c')")

	// Delete with count
	count, err := db.Delete[struct{}]("items").Where("id = ?").Exec(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("expected 1 delete, got %d", count)
	}

	// Delete with Returning
	type Item struct {
		ID  int
		Val string
	}
	deleted, err := db.Delete[Item]("items").Where("id > ?").Returning("id", "val").ExecN(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(deleted) != 2 {
		t.Errorf("expected 2 deleted, got %d", len(deleted))
	}
}

func TestDelete_Declaration(t *testing.T) {
	ctx := setup(t)
	exec(t, ctx, "CREATE TABLE items (id INTEGER PRIMARY KEY, val TEXT)")
	exec(t, ctx, "INSERT INTO items VALUES (1, 'a')")

	type Item struct {
		ID  int
		Val string
	}
	items := db.Declare[Item]("items", "id", "val")

	deleted, err := items.Delete().Where("id = ?").Returning("id", "val").Exec1(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if deleted.Val != "a" {
		t.Errorf("expected 'a', got %s", deleted.Val)
	}
}

func TestSelector_GroupBy_Having(t *testing.T) {
	ctx := setup(t)
	exec(t, ctx, "CREATE TABLE sales (category TEXT, amount INTEGER)")
	exec(t, ctx, "INSERT INTO sales VALUES ('a', 10)")
	exec(t, ctx, "INSERT INTO sales VALUES ('a', 5)")
	exec(t, ctx, "INSERT INTO sales VALUES ('b', 5)")
	exec(t, ctx, "INSERT INTO sales VALUES ('b', 100)")

	type Result struct {
		Category string
		Total    int
	}

	results, err := db.Select[Result]("sales", "category", "SUM(amount) as total").
		GroupBy("category").
		Having("SUM(amount) > ?").
		OrderBy("category").
		ExecN(ctx, 25)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Category != "b" {
		t.Errorf("expected category 'b', got %s", results[0].Category)
	}
	if results[0].Total != 105 {
		t.Errorf("expected total 105, got %d", results[0].Total)
	}
}

func TestSelector_LeftJoin(t *testing.T) {
	ctx := setup(t)
	exec(t, ctx, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	exec(t, ctx, "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, item TEXT)")
	exec(t, ctx, "INSERT INTO users VALUES (1, 'alice')")
	exec(t, ctx, "INSERT INTO users VALUES (2, 'bob')")
	exec(t, ctx, "INSERT INTO orders VALUES (1, 1, 'widget')")

	type Result struct {
		Name string
		Item *string
	}

	results, err := db.Select[Result]("users", "users.name", "orders.item").
		LeftJoin("orders", "orders.user_id = users.id").
		OrderBy("users.name").
		ExecN(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if results[0].Name != "alice" || results[0].Item == nil || *results[0].Item != "widget" {
		t.Errorf("unexpected first result: %+v", results[0])
	}
	if results[1].Name != "bob" || results[1].Item != nil {
		t.Errorf("expected bob with nil item, got %+v", results[1])
	}
}

func TestSelector_Join(t *testing.T) {
	ctx := setup(t)
	exec(t, ctx, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	exec(t, ctx, "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, item TEXT)")
	exec(t, ctx, "INSERT INTO users VALUES (1, 'alice')")
	exec(t, ctx, "INSERT INTO users VALUES (2, 'bob')")
	exec(t, ctx, "INSERT INTO orders VALUES (1, 1, 'widget')")

	type Result struct {
		Name string
		Item string
	}

	results, err := db.Select[Result]("users", "users.name", "orders.item").
		Join("orders", "orders.user_id = users.id").
		ExecN(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Name != "alice" {
		t.Errorf("expected alice, got %s", results[0].Name)
	}
}

func TestSelector_SQL(t *testing.T) {
	sql := db.Select[int]("t", "a", "b").
		Join("j", "j.id = t.id").
		LeftJoin("k", "k.id = t.id").
		Where("x = ?").
		GroupBy("a").
		Having("COUNT(*) > ?").
		OrderBy("b DESC").
		Limit(10).
		Offset(5).
		SQL()
	expected := "SELECT a, b FROM t JOIN j ON j.id = t.id LEFT JOIN k ON k.id = t.id WHERE (x = ?) GROUP BY a HAVING (COUNT(*) > ?) ORDER BY b DESC LIMIT 10 OFFSET 5"
	if sql != expected {
		t.Errorf("SQL mismatch:\ngot:  %s\nwant: %s", sql, expected)
	}
}

func TestUpdater_NoSet(t *testing.T) {
	ctx := setup(t)
	exec(t, ctx, "CREATE TABLE items (id INTEGER, val TEXT)")

	_, err := db.Declare[struct{}]("items").Update().Where("id = ?").Exec(ctx, 1)
	if err == nil {
		t.Error("expected error for updater with no SET clauses")
	}
}

func TestUpdater_ReturningGuard(t *testing.T) {
	ctx := setup(t)
	exec(t, ctx, "CREATE TABLE items (id INTEGER, val TEXT)")

	_, err := db.Declare[struct{}]("items").Update().Set("val", "x").Returning("id").Exec(ctx)
	if err == nil {
		t.Error("expected error when using Exec with RETURNING clause")
	}
}

func TestDeleter_ReturningGuard(t *testing.T) {
	ctx := setup(t)
	exec(t, ctx, "CREATE TABLE items (id INTEGER)")

	_, err := db.Delete[struct{}]("items").Where("id = ?").Returning("id").Exec(ctx, 1)
	if err == nil {
		t.Error("expected error when using Exec with RETURNING clause on deleter")
	}
}

func TestDelete_SQL(t *testing.T) {
	sql := db.Delete[int]("items").Where("id = ?").Returning("id").SQL()
	expected := "DELETE FROM items WHERE (id = ?) RETURNING id"
	if sql != expected {
		t.Errorf("SQL mismatch:\ngot:  %s\nwant: %s", sql, expected)
	}
}

func TestInsert_MultiRow_SQL(t *testing.T) {
	sql := db.Insert[int]("items", "a", "b").Values("?", "?").SQL()
	expected := "INSERT INTO items(a, b) VALUES (?, ?), (?, ?)"
	if sql != expected {
		t.Errorf("SQL mismatch:\ngot:  %s\nwant: %s", sql, expected)
	}
}

func TestValueReceivers_Immutable(t *testing.T) {
	// Verify that value receivers produce independent copies
	base := db.Select[int]("t", "a").Where("x = ?")
	a := base.Where("y = ?")
	b := base.Where("z = ?")

	sqlA := a.SQL()
	sqlB := b.SQL()

	if sqlA == sqlB {
		t.Errorf("expected different SQL from branched selectors, both got: %s", sqlA)
	}
	expectedA := "SELECT a FROM t WHERE (x = ?) AND (y = ?)"
	expectedB := "SELECT a FROM t WHERE (x = ?) AND (z = ?)"
	if sqlA != expectedA {
		t.Errorf("sqlA mismatch:\ngot:  %s\nwant: %s", sqlA, expectedA)
	}
	if sqlB != expectedB {
		t.Errorf("sqlB mismatch:\ngot:  %s\nwant: %s", sqlB, expectedB)
	}
}

func TestTx_Commit(t *testing.T) {
	ctx := setup(t)
	exec(t, ctx, "CREATE TABLE items (id INTEGER PRIMARY KEY, val TEXT)")

	err := db.Tx(ctx, func(ctx context.Context) error {
		_, err := db.Exec(ctx, "INSERT INTO items VALUES (1, 'a')")
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	val, err := db.Select[string]("items", "val").Where("id = ?").Exec1(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if val != "a" {
		t.Errorf("expected 'a', got %s", val)
	}
}

func TestTx_Rollback(t *testing.T) {
	ctx := setup(t)
	exec(t, ctx, "CREATE TABLE items (id INTEGER PRIMARY KEY, val TEXT)")
	exec(t, ctx, "INSERT INTO items VALUES (1, 'original')")

	err := db.Tx(ctx, func(ctx context.Context) error {
		if _, err := db.Exec(ctx, "UPDATE items SET val = 'changed' WHERE id = 1"); err != nil {
			return err
		}
		return fmt.Errorf("intentional rollback")
	})
	if err == nil {
		t.Fatal("expected error")
	}

	val, err := db.Select[string]("items", "val").Where("id = ?").Exec1(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if val != "original" {
		t.Errorf("expected 'original' after rollback, got %s", val)
	}
}

func TestTx_Nested(t *testing.T) {
	ctx := setup(t)
	exec(t, ctx, "CREATE TABLE items (id INTEGER PRIMARY KEY, val TEXT)")
	exec(t, ctx, "INSERT INTO items VALUES (1, 'original')")

	err := db.Tx(ctx, func(ctx context.Context) error {
		if _, err := db.Exec(ctx, "UPDATE items SET val = 'outer' WHERE id = 1"); err != nil {
			return err
		}
		// Inner tx fails — should roll back inner changes only
		_ = db.Tx(ctx, func(ctx context.Context) error {
			if _, err := db.Exec(ctx, "UPDATE items SET val = 'inner' WHERE id = 1"); err != nil {
				return err
			}
			return fmt.Errorf("inner rollback")
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	val, err := db.Select[string]("items", "val").Where("id = ?").Exec1(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if val != "outer" {
		t.Errorf("expected 'outer' after inner rollback, got %s", val)
	}
}

func TestTx_Panic(t *testing.T) {
	ctx := setup(t)
	exec(t, ctx, "CREATE TABLE items (id INTEGER PRIMARY KEY, val TEXT)")
	exec(t, ctx, "INSERT INTO items VALUES (1, 'original')")

	func() {
		defer func() { recover() }()
		_ = db.Tx(ctx, func(ctx context.Context) error {
			db.Exec(ctx, "UPDATE items SET val = 'panicked' WHERE id = 1")
			panic("boom")
		})
	}()

	val, err := db.Select[string]("items", "val").Where("id = ?").Exec1(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if val != "original" {
		t.Errorf("expected 'original' after panic rollback, got %s", val)
	}
}

func TestTx_NoConnection(t *testing.T) {
	err := db.Tx(context.Background(), func(ctx context.Context) error {
		return nil
	})
	if err == nil {
		t.Error("expected error for missing connection")
	}
}

func TestDeleteFrom(t *testing.T) {
	ctx := setup(t)
	exec(t, ctx, "CREATE TABLE items (id INTEGER PRIMARY KEY, val TEXT)")
	exec(t, ctx, "INSERT INTO items VALUES (1, 'a')")
	exec(t, ctx, "INSERT INTO items VALUES (2, 'b')")

	count, err := db.DeleteFrom("items").Where("id = ?").Exec(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("expected 1 delete, got %d", count)
	}
}

func TestInsertInto(t *testing.T) {
	ctx := setup(t)
	exec(t, ctx, "CREATE TABLE items (id INTEGER PRIMARY KEY, val TEXT)")

	count, err := db.InsertInto("items", "val").Exec(ctx, "test")
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("expected 1 insert, got %d", count)
	}
}

func TestInserter_Exec_ReturningGuard(t *testing.T) {
	ctx := setup(t)
	exec(t, ctx, "CREATE TABLE items (id INTEGER PRIMARY KEY, val TEXT)")

	_, err := db.Insert[int]("items", "val").Returning("id").Exec(ctx, "test")
	if err == nil {
		t.Error("expected error when using Exec with RETURNING clause on inserter")
	}
}

// TestTx_Exec1Inside ensures that running Selector.Exec1 inside a Tx does not
// leave the cached prepared statement in a running state. Before the scanner
// reset fix, the trailing RELEASE SAVEPOINT failed with "cannot release
// savepoint - SQL statements in progress".
func TestTx_Exec1Inside(t *testing.T) {
	ctx := setup(t)
	exec(t, ctx, "CREATE TABLE items (id INTEGER PRIMARY KEY, val TEXT)")
	exec(t, ctx, "INSERT INTO items VALUES (1, 'a')")

	err := db.Tx(ctx, func(ctx context.Context) error {
		v, err := db.Select[string]("items", "val").Where("id = ?").Exec1(ctx, 1)
		if err != nil {
			return err
		}
		if v != "a" {
			t.Errorf("expected 'a', got %q", v)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("tx: %v", err)
	}
}

// TestTx_InsertReturning_Inside is the INSERT...RETURNING analog: the same
// scanner.Get path is taken, so the same bug would surface.
func TestTx_InsertReturning_Inside(t *testing.T) {
	ctx := setup(t)
	exec(t, ctx, "CREATE TABLE items (id INTEGER PRIMARY KEY, val TEXT)")

	err := db.Tx(ctx, func(ctx context.Context) error {
		id, err := db.Insert[int]("items", "val").Returning("id").Exec1(ctx, "x")
		if err != nil {
			return err
		}
		if id != 1 {
			t.Errorf("expected id 1, got %d", id)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("tx: %v", err)
	}
}

// TestSelect_TopLevelMap exercises the implicit-JSON path for a map-typed
// scan target. Before the scanner change, this would fail with
// "unsupported type map[string]int".
func TestSelect_TopLevelMap(t *testing.T) {
	ctx := setup(t)
	exec(t, ctx, "CREATE TABLE info (data TEXT)")
	exec(t, ctx, `INSERT INTO info VALUES ('{"a":1,"b":2}')`)

	m, err := db.Select[map[string]int]("info", "data").Exec1(ctx)
	if err != nil {
		t.Fatalf("Exec1: %v", err)
	}
	if m["a"] != 1 || m["b"] != 2 {
		t.Errorf("expected {a:1 b:2}, got %v", m)
	}
}

// TestSelect_TopLevelSliceInt covers the generalized slice-as-JSON path: any
// slice other than []byte should decode from JSON text.
func TestSelect_TopLevelSliceInt(t *testing.T) {
	ctx := setup(t)
	exec(t, ctx, "CREATE TABLE info (data TEXT)")
	exec(t, ctx, `INSERT INTO info VALUES ('[1,2,3]')`)

	xs, err := db.Select[[]int]("info", "data").Exec1(ctx)
	if err != nil {
		t.Fatalf("Exec1: %v", err)
	}
	if len(xs) != 3 || xs[0] != 1 || xs[1] != 2 || xs[2] != 3 {
		t.Errorf("expected [1 2 3], got %v", xs)
	}
}

// TestSelect_TopLevelRawMessageSlice guards the named-byte-slice edge case:
// json.RawMessage is `type RawMessage []byte`, so a refactor of the []byte
// detection could accidentally route []json.RawMessage to the BLOB arm and
// silently produce garbage. The element Kind is reflect.Slice (not Uint8),
// so it must fall through to the JSON path.
func TestSelect_TopLevelRawMessageSlice(t *testing.T) {
	ctx := setup(t)
	exec(t, ctx, "CREATE TABLE info (data TEXT)")
	exec(t, ctx, `INSERT INTO info VALUES ('[{"a":1},"x",2]')`)

	xs, err := db.Select[[]json.RawMessage]("info", "data").Exec1(ctx)
	if err != nil {
		t.Fatalf("Exec1: %v", err)
	}
	if len(xs) != 3 {
		t.Fatalf("expected 3 elements, got %d (%v)", len(xs), xs)
	}
	if string(xs[0]) != `{"a":1}` || string(xs[1]) != `"x"` || string(xs[2]) != `2` {
		t.Errorf("unexpected raw messages: %q %q %q", xs[0], xs[1], xs[2])
	}
}

func TestBind_Uint64Overflow(t *testing.T) {
	ctx := setup(t)
	exec(t, ctx, "CREATE TABLE nums (val INTEGER)")

	var overflow uint64 = 1<<63 + 1
	_, err := db.Exec(ctx, "INSERT INTO nums VALUES (?)", overflow)
	if err == nil {
		t.Error("expected error for uint64 overflow")
	}
}

// TestBind_JSON exercises db.JSON: an arbitrary struct/map/slice argument is
// JSON-marshaled and bound as SQLite TEXT, round-tripping through the scan
// tag.
func TestBind_JSON(t *testing.T) {
	ctx := setup(t)
	exec(t, ctx, "CREATE TABLE docs (id INTEGER PRIMARY KEY, info TEXT)")

	info := map[string]int{"a": 1, "b": 2}
	if _, err := db.Exec(ctx, "INSERT INTO docs(info) VALUES (?)", db.JSON(info)); err != nil {
		t.Fatalf("insert with db.JSON: %v", err)
	}

	type Doc struct {
		ID   int            `db:"id"`
		Info map[string]int `db:"info,json"`
	}
	got, err := db.Select[Doc]("docs", "id", "info").Where("id = ?").Exec1(ctx, 1)
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if got.Info["a"] != 1 || got.Info["b"] != 2 {
		t.Errorf("round-trip mismatch: %+v", got.Info)
	}

	// SQLite's json_extract should see the value as JSON, proving it was bound
	// as TEXT rather than as the Go-rendered map syntax.
	v, err := db.Select[int]("docs", "json_extract(info, '$.a')").Where("id = ?").Exec1(ctx, 1)
	if err != nil {
		t.Fatalf("json_extract: %v", err)
	}
	if v != 1 {
		t.Errorf("json_extract: got %d want 1", v)
	}
}

// TestBind_RawMessage confirms json.RawMessage takes the TEXT path, not the
// []byte BLOB path. SQLite TEXT vs BLOB affects whether JSON operators apply.
func TestBind_RawMessage(t *testing.T) {
	ctx := setup(t)
	exec(t, ctx, "CREATE TABLE docs (info TEXT)")

	if _, err := db.Exec(ctx, "INSERT INTO docs VALUES (?)", json.RawMessage(`{"x":7}`)); err != nil {
		t.Fatalf("insert raw message: %v", err)
	}
	v, err := db.Select[int]("docs", "json_extract(info, '$.x')").Exec1(ctx)
	if err != nil {
		t.Fatalf("json_extract: %v", err)
	}
	if v != 7 {
		t.Errorf("got %d want 7", v)
	}
}

// TestBind_JSON_MarshalError surfaces marshaling failures rather than binding
// garbage.
func TestBind_JSON_MarshalError(t *testing.T) {
	ctx := setup(t)
	exec(t, ctx, "CREATE TABLE docs (info TEXT)")

	// channels are not JSON-marshalable.
	_, err := db.Exec(ctx, "INSERT INTO docs VALUES (?)", db.JSON(make(chan int)))
	if err == nil {
		t.Fatal("expected marshal error")
	}
}

// TestUpdater_SetExpr covers the SQL-expression variant of Set. Without it the
// builder can't say SET value = value + 1 because Set binds its value as a
// parameter.
func TestUpdater_SetExpr(t *testing.T) {
	ctx := setup(t)
	exec(t, ctx, "CREATE TABLE counters (id INTEGER PRIMARY KEY, val INTEGER NOT NULL DEFAULT 0)")
	exec(t, ctx, "INSERT INTO counters(id, val) VALUES (1, 10)")

	// SET value = value + ? with a placeholder consuming a SetExpr arg.
	if _, err := db.Declare[struct{}]("counters").Update().
		SetExpr("val", "val + ?", 5).
		Where("id = ?").
		Exec(ctx, 1); err != nil {
		t.Fatalf("SetExpr: %v", err)
	}
	v, err := db.Select[int]("counters", "val").Where("id = ?").Exec1(ctx, 1)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if v != 15 {
		t.Errorf("got %d want 15", v)
	}

	// Mixing Set (bound value) and SetExpr in one update; arg order in the
	// final statement is: SetExpr args, then Set args, then Where args, all
	// in column-prepend insertion order.
	if _, err := db.Declare[struct{}]("counters").Update().
		SetExpr("val", "val * ?", 2).
		Set("id", 1).
		Where("id = ?").
		Exec(ctx, 1); err != nil {
		t.Fatalf("mixed Set/SetExpr: %v", err)
	}
	v2, err := db.Select[int]("counters", "val").Where("id = ?").Exec1(ctx, 1)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if v2 != 30 {
		t.Errorf("got %d want 30", v2)
	}
}

// TestSelect_InferredColumns verifies that Select[T] with no columns infers
// the column list from T's struct tags rather than emitting SELECT *.
func TestSelect_InferredColumns(t *testing.T) {
	type Block struct {
		ID    int    `db:"id"`
		Story int    `db:"story"`
		Text  string `db:"text"`
	}
	sql := db.Select[Block]("block").SQL()
	expected := "SELECT id, story, text FROM block"
	if sql != expected {
		t.Errorf("SQL mismatch:\ngot:  %s\nwant: %s", sql, expected)
	}
}

// TestSelect_InferredColumns_Roundtrip exercises the inferred-column path end
// to end against a real SQLite connection.
func TestSelect_InferredColumns_Roundtrip(t *testing.T) {
	ctx := setup(t)
	exec(t, ctx, "CREATE TABLE block (id INTEGER, story INTEGER, text TEXT)")
	exec(t, ctx, "INSERT INTO block VALUES (1, 100, 'hello')")

	type Block struct {
		ID    int    `db:"id"`
		Story int    `db:"story"`
		Text  string `db:"text"`
	}
	b, err := db.Select[Block]("block").Where("id = ?").Exec1(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if b.ID != 1 || b.Story != 100 || b.Text != "hello" {
		t.Errorf("roundtrip mismatch: %+v", b)
	}
}

// TestSelect_NonStructInferenceError pins that asking for inference on a
// non-struct type (no columns and T is e.g. int) errors at Exec rather than
// silently falling back to SELECT *.
func TestSelect_NonStructInferenceError(t *testing.T) {
	ctx := setup(t)
	exec(t, ctx, "CREATE TABLE nums (val INTEGER)")
	exec(t, ctx, "INSERT INTO nums VALUES (1)")

	_, err := db.Select[int]("nums").Exec1(ctx)
	if err == nil {
		t.Error("expected error when inferring columns for non-struct T")
	}
}

// TestUpdater_Returning_Inferred: Returning() with no args infers from T.
func TestUpdater_Returning_Inferred(t *testing.T) {
	ctx := setup(t)
	exec(t, ctx, "CREATE TABLE counters (id INTEGER PRIMARY KEY, val INTEGER)")
	exec(t, ctx, "INSERT INTO counters VALUES (1, 10)")

	type Counter struct {
		ID  int `db:"id"`
		Val int `db:"val"`
	}
	c, err := db.Declare[Counter]("counters").Update().Set("val", 11).Where("id = ?").Returning().Exec1(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if c.ID != 1 || c.Val != 11 {
		t.Errorf("got %+v want {1 11}", c)
	}
}

// TestDeclaration_Returning_UsesDeclCols pins that Returning() with no args
// on a Declaration-derived Updater/Deleter reuses the Declaration's column
// list rather than re-inferring from T. The difference matters when the
// caller deliberately pinned a narrower-or-wider column list at the Declare
// site (e.g. to lock insert-order independent of struct layout, with a
// RETURNING list that mirrors the same explicit set).
func TestDeclaration_Returning_UsesDeclCols(t *testing.T) {
	ctx := setup(t)
	exec(t, ctx, "CREATE TABLE items (id INTEGER PRIMARY KEY, val TEXT, extra TEXT)")
	exec(t, ctx, "INSERT INTO items(val, extra) VALUES ('hello', 'ignored')")

	// Item has three fields but the Declaration pins only two columns. A
	// bare Returning() must honor that — not silently re-infer the third.
	type Item struct {
		ID    int    `db:"id"`
		Val   string `db:"val"`
		Extra string `db:"extra"`
	}
	updSQL := db.Declare[Item]("items", "id", "val").Update().
		Set("val", "world").Where("id = ?").Returning().SQL()
	expectedUpd := "UPDATE items SET val = ? WHERE (id = ?) RETURNING id, val"
	if updSQL != expectedUpd {
		t.Errorf("update SQL mismatch:\ngot:  %s\nwant: %s", updSQL, expectedUpd)
	}

	delSQL := db.Declare[Item]("items", "id", "val").Delete().
		Where("id = ?").Returning().SQL()
	expectedDel := "DELETE FROM items WHERE (id = ?) RETURNING id, val"
	if delSQL != expectedDel {
		t.Errorf("delete SQL mismatch:\ngot:  %s\nwant: %s", delSQL, expectedDel)
	}
}

// TestDeleter_Returning_Inferred: Returning() with no args infers from T.
func TestDeleter_Returning_Inferred(t *testing.T) {
	ctx := setup(t)
	exec(t, ctx, "CREATE TABLE items (id INTEGER PRIMARY KEY, val TEXT)")
	exec(t, ctx, "INSERT INTO items VALUES (1, 'a')")

	type Item struct {
		ID  int    `db:"id"`
		Val string `db:"val"`
	}
	got, err := db.Delete[Item]("items").Where("id = ?").Returning().Exec1(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != 1 || got.Val != "a" {
		t.Errorf("got %+v want {1 a}", got)
	}
}

// TestDeclare_NoColumns_Select: Declare[T](table) with no columns + Select()
// emits the inferred list.
func TestDeclare_NoColumns_Select(t *testing.T) {
	type Item struct {
		ID  int    `db:"id"`
		Val string `db:"val"`
	}
	sql := db.Declare[Item]("items").Select().SQL()
	expected := "SELECT id, val FROM items"
	if sql != expected {
		t.Errorf("SQL mismatch:\ngot:  %s\nwant: %s", sql, expected)
	}
}

// TestDeclare_NoColumns_Insert_NoInsertions errors clearly when both the
// Declaration and the Insert have nothing to write.
func TestDeclare_NoColumns_Insert_NoInsertions(t *testing.T) {
	ctx := setup(t)
	exec(t, ctx, "CREATE TABLE items (id INTEGER PRIMARY KEY, val TEXT)")

	type Item struct {
		ID  int    `db:"id"`
		Val string `db:"val"`
	}
	_, err := db.Declare[Item]("items").Insert().Exec(ctx)
	if err == nil {
		t.Error("expected error for Declare(...).Insert() with no insertions")
	}
}

// TestDeclare_NoColumns_Insert_ExplicitInsertions: even with inferred columns
// on the Declaration (used for RETURNING), an explicit Insert("a","b") works.
func TestDeclare_NoColumns_Insert_ExplicitInsertions(t *testing.T) {
	ctx := setup(t)
	exec(t, ctx, "CREATE TABLE items (id INTEGER PRIMARY KEY, val TEXT)")

	type Item struct {
		ID  int    `db:"id"`
		Val string `db:"val"`
	}
	// Declaration has inferred columns {id, val}; they become the RETURNING list.
	got, err := db.Declare[Item]("items").Insert("val").Exec1(ctx, "hello")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if got.ID != 1 || got.Val != "hello" {
		t.Errorf("got %+v want {1 hello}", got)
	}
}

// TestSelect_ExplicitColumnsOverrideInference pins that an explicit column
// list still takes precedence (and `"*"` is the literal SELECT * escape).
func TestSelect_ExplicitColumnsOverrideInference(t *testing.T) {
	type Item struct {
		ID  int    `db:"id"`
		Val string `db:"val"`
	}
	// Explicit columns override the inferred list.
	sql := db.Select[Item]("items", "id").SQL()
	if sql != "SELECT id FROM items" {
		t.Errorf("expected explicit columns; got %s", sql)
	}
	// "*" is the literal escape hatch.
	starSQL := db.Select[Item]("items", "*").SQL()
	if starSQL != "SELECT * FROM items" {
		t.Errorf("expected SELECT *; got %s", starSQL)
	}
}

// TestUpdater_SetExpr_SQL pins the generated SQL so future refactors don't
// silently rearrange the SET-list.
func TestUpdater_SetExpr_SQL(t *testing.T) {
	sql := db.Declare[struct{}]("counters").Update().
		SetExpr("val", "val + ?", 1).
		Set("name", "x").
		Where("id = ?").
		SQL()
	expected := "UPDATE counters SET val = val + ?, name = ? WHERE (id = ?)"
	if sql != expected {
		t.Errorf("SQL mismatch:\ngot:  %s\nwant: %s", sql, expected)
	}
}

// TestTxV_Commit confirms the value-returning Tx propagates the return value
// when fn succeeds.
func TestTxV_Commit(t *testing.T) {
	ctx := setup(t)
	exec(t, ctx, "CREATE TABLE items (id INTEGER PRIMARY KEY, val TEXT)")

	val, err := db.TxV(ctx, func(ctx context.Context) (string, error) {
		if _, err := db.Exec(ctx, "INSERT INTO items(id, val) VALUES (1, 'a')"); err != nil {
			return "", err
		}
		return db.Select[string]("items", "val").Where("id = ?").Exec1(ctx, 1)
	})
	if err != nil {
		t.Fatalf("TxV: %v", err)
	}
	if val != "a" {
		t.Errorf("got %q want 'a'", val)
	}
}

// TestTxV_Rollback confirms the value-returning Tx rolls back on error and
// returns the zero value of T.
func TestTxV_Rollback(t *testing.T) {
	ctx := setup(t)
	exec(t, ctx, "CREATE TABLE items (id INTEGER PRIMARY KEY, val TEXT)")
	exec(t, ctx, "INSERT INTO items VALUES (1, 'original')")

	val, err := db.TxV(ctx, func(ctx context.Context) (string, error) {
		if _, err := db.Exec(ctx, "UPDATE items SET val = 'changed' WHERE id = 1"); err != nil {
			return "", err
		}
		return "would-be-result", fmt.Errorf("intentional rollback")
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if val != "" {
		t.Errorf("expected zero string on rollback, got %q", val)
	}
	got, err := db.Select[string]("items", "val").Where("id = ?").Exec1(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if got != "original" {
		t.Errorf("expected 'original' after rollback, got %q", got)
	}
}
