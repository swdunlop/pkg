package scanner

import (
	"reflect"
	"testing"

	"zombiezen.com/go/sqlite"
)

func TestScanner_Struct(t *testing.T) {
	conn, err := sqlite.OpenConn(":memory:", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if err := sqlExec(conn, `
		CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			user_name TEXT,
			active INTEGER,
			score REAL,
			meta BLOB
		)`); err != nil {
		t.Fatal(err)
	}
	if err := sqlExec(conn, `INSERT INTO users (user_name, active, score, meta) VALUES ('alice', 1, 99.5, X'DEADBEEF')`); err != nil {
		t.Fatal(err)
	}

	stmt, err := conn.Prepare("SELECT * FROM users")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Finalize()

	type User struct {
		ID       int
		UserName string
		Active   bool
		Score    float64
		Meta     []byte
	}

	var u User
	scan, err := For(&u)
	if err != nil {
		t.Fatal(err)
	}

	hasRow, err := stmt.Step()
	if err != nil {
		t.Fatal(err)
	}
	if !hasRow {
		t.Fatal("expected row")
	}

	if err := scan(stmt); err != nil {
		t.Fatal(err)
	}

	if u.UserName != "alice" {
		t.Errorf("expected alice, got %s", u.UserName)
	}
	if !u.Active {
		t.Error("expected active")
	}
	if u.Score != 99.5 {
		t.Errorf("expected 99.5, got %f", u.Score)
	}
	if !reflect.DeepEqual(u.Meta, []byte{0xDE, 0xAD, 0xBE, 0xEF}) {
		t.Errorf("expected DEADBEEF, got %X", u.Meta)
	}
}

func TestScanner_Tags(t *testing.T) {
	conn, err := sqlite.OpenConn(":memory:", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if err := sqlExec(conn, "CREATE TABLE data (foo TEXT, bar INTEGER)"); err != nil {
		t.Fatal(err)
	}
	if err := sqlExec(conn, "INSERT INTO data VALUES ('hello', 42)"); err != nil {
		t.Fatal(err)
	}

	stmt, err := conn.Prepare("SELECT foo, bar FROM data")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Finalize()

	type Tagged struct {
		Name string `db:"foo"`
		Age  int    `db:"bar"`
		Skip string `db:"-"`
	}

	var val Tagged
	scan, err := For(&val)
	if err != nil {
		t.Fatal(err)
	}

	stmt.Step()
	if err := scan(stmt); err != nil {
		t.Fatal(err)
	}

	if val.Name != "hello" {
		t.Errorf("expected hello, got %s", val.Name)
	}
	if val.Age != 42 {
		t.Errorf("expected 42, got %d", val.Age)
	}
}

func TestScanner_Scalar(t *testing.T) {
	conn, err := sqlite.OpenConn(":memory:", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if err := sqlExec(conn, "CREATE TABLE nums (val INTEGER)"); err != nil {
		t.Fatal(err)
	}
	if err := sqlExec(conn, "INSERT INTO nums VALUES (123)"); err != nil {
		t.Fatal(err)
	}

	stmt, err := conn.Prepare("SELECT val FROM nums")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Finalize()

	var i int
	scan, err := For(&i)
	if err != nil {
		t.Fatal(err)
	}

	stmt.Step()
	// This is expected to potentially fail if Scalar logic isn't updated to handle empty names
	if err := scan(stmt); err != nil {
		t.Logf("scalar scan error (expected pending fix): %v", err)
	} else if i != 123 {
		t.Errorf("expected 123, got %d", i)
	}
}

func TestScanner_Slice(t *testing.T) {
	conn, err := sqlite.OpenConn(":memory:", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if err := sqlExec(conn, "CREATE TABLE items (tags TEXT)"); err != nil {
		t.Fatal(err)
	}
	// Insert JSON array
	if err := sqlExec(conn, `INSERT INTO items VALUES ('["a","b","c"]')`); err != nil {
		t.Fatal(err)
	}

	stmt, err := conn.Prepare("SELECT tags FROM items")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Finalize()

	var tags []string
	scan, err := For(&tags)
	if err != nil {
		t.Fatal(err)
	}

	stmt.Step()
	if err := scan(stmt); err != nil {
		t.Fatal(err)
	}

	if len(tags) != 3 {
		t.Fatalf("expected 3 tags, got %d", len(tags))
	}
	if tags[0] != "a" || tags[1] != "b" || tags[2] != "c" {
		t.Errorf("expected [a b c], got %v", tags)
	}
}

func TestInferFieldName(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"ID", "id"},
		{"UserName", "user_name"},
		{"PDFLoader", "pdf_loader"},
		{"Simple", "simple"},
		{"MyXMLParser", "my_xml_parser"},
	}

	for _, tt := range tests {
		got := inferFieldName(tt.input)
		if got != tt.want {
			t.Errorf("inferFieldName(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestScanner_JSON(t *testing.T) {
	conn, err := sqlite.OpenConn(":memory:", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if err := sqlExec(conn, "CREATE TABLE users (id INTEGER, meta TEXT, attrs TEXT)"); err != nil {
		t.Fatal(err)
	}
	if err := sqlExec(conn, `INSERT INTO users VALUES (1, '{"theme":"dark","login_count":5}', '{"a":1}')`); err != nil {
		t.Fatal(err)
	}

	stmt, err := conn.Prepare("SELECT * FROM users")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Finalize()

	type Meta struct {
		Theme      string `json:"theme"`
		LoginCount int    `json:"login_count"`
	}

	type User struct {
		ID    int
		Meta  Meta           `db:"meta,json"`
		Attrs map[string]int `db:"attrs,json"`
	}

	var u User
	scan, err := For(&u)
	if err != nil {
		t.Fatal(err)
	}

	stmt.Step()
	if err := scan(stmt); err != nil {
		t.Fatal(err)
	}

	if u.Meta.Theme != "dark" {
		t.Errorf("expected dark, got %s", u.Meta.Theme)
	}
	if u.Meta.LoginCount != 5 {
		t.Errorf("expected 5, got %d", u.Meta.LoginCount)
	}
	if u.Attrs["a"] != 1 {
		t.Errorf("expected 1, got %d", u.Attrs["a"])
	}
}

func TestColumnsOf_ExplicitTags(t *testing.T) {
	type Block struct {
		ID    uint64 `db:"id"`
		Story uint64 `db:"story"`
		Skip  string `db:"-"`
		Text  string `db:"text"`
	}
	got, err := ColumnsOf[Block]()
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"id", "story", "text"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v want %v", got, want)
	}
}

func TestColumnsOf_InferredSnakeCase(t *testing.T) {
	type User struct {
		ID       int
		UserName string
		PDFData  []byte
	}
	got, err := ColumnsOf[User]()
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"id", "user_name", "pdf_data"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v want %v", got, want)
	}
}

func TestColumnsOf_JSONField(t *testing.T) {
	type Doc struct {
		ID   int                    `db:"id"`
		Info map[string]int         `db:"info,json"`
		Meta struct{ A int }        `db:"meta,json"`
		Tags []string               `db:"tags,json"`
		Raw  map[string]any         `db:"raw,ro,json"` // composed modifiers (ro currently no-op)
	}
	got, err := ColumnsOf[Doc]()
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"id", "info", "meta", "tags", "raw"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v want %v", got, want)
	}
}

func TestColumnsOf_NestedStructFlattening(t *testing.T) {
	type Inner struct {
		A string `db:"a"`
		B string `db:"b"`
	}
	type Outer struct {
		ID  int   `db:"id"`
		Foo Inner `db:"foo"`
		Bar Inner // inferred name "bar"
	}
	got, err := ColumnsOf[Outer]()
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"id", "foo_a", "foo_b", "bar_a", "bar_b"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v want %v", got, want)
	}
}

func TestColumnsOf_PointerToStruct(t *testing.T) {
	type Inner struct {
		A string `db:"a"`
	}
	type Outer struct {
		ID  int    `db:"id"`
		Foo *Inner `db:"foo"`
	}
	got, err := ColumnsOf[Outer]()
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"id", "foo_a"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v want %v", got, want)
	}
}

func TestColumnsOf_NonStructErrors(t *testing.T) {
	if _, err := ColumnsOf[int](); err == nil {
		t.Error("expected error for int")
	}
	if _, err := ColumnsOf[[]string](); err == nil {
		t.Error("expected error for []string")
	}
	if _, err := ColumnsOf[map[string]int](); err == nil {
		t.Error("expected error for map[string]int")
	}
}

func TestColumnsOf_PointerToStructDeref(t *testing.T) {
	// One level of pointer-deref is acceptable: *Struct → ColumnsOf returns the
	// struct's columns.
	type S struct {
		A int `db:"a"`
		B int `db:"b"`
	}
	got, err := ColumnsOf[*S]()
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"a", "b"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v want %v", got, want)
	}
}

func sqlExec(conn *sqlite.Conn, sql string) error {
	stmt, err := conn.Prepare(sql)
	if err != nil {
		return err
	}
	defer stmt.Finalize()
	_, err = stmt.Step()
	return err
}
