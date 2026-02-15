// package scanner implements scanning Go types from a Sqlite statement, using reflection to analyze Go structures
// and assignment for basic Go types.
package scanner

import (
	"encoding/json"
	"fmt"
	"io"
	"iter"
	"reflect"
	"slices"
	"strings"
	"sync"
	"unicode"

	"zombiezen.com/go/sqlite"
)

// List collects results from iterating through a statement.
func List[T any](stmt *sqlite.Stmt) ([]T, error) {
	var err error
	items := slices.Collect(Iter[T](&err, stmt))
	return items, err
}

// Iter steps through a statement, yielding results.
func Iter[T any](rerr *error, stmt *sqlite.Stmt) iter.Seq[T] {
	return func(yield func(T) bool) {
		var it T
		scan, err := For(&it)
		if err != nil {
			*rerr = err
			return
		}
		for {
			hasRow, stepErr := stmt.Step()
			if stepErr != nil {
				*rerr = stepErr
				return
			}
			if !hasRow {
				break
			}
			if scanErr := scan(stmt); scanErr != nil {
				*rerr = scanErr
				return
			}
			if !yield(it) {
				return
			}
		}
	}
}

// Get steps through a statement then scans a value, returns io.EOF if there were no more results.
func Get[T any](stmt *sqlite.Stmt) (ret T, err error) {
	scan, err := For(&ret)
	if err != nil {
		return
	}
	hasRow, err := stmt.Step()
	if err != nil {
		return
	}
	if !hasRow {
		return ret, io.EOF
	}
	err = scan(stmt)
	return
}

// For returns a scanner function that populates the fields of ref using values scanned from a Sqlite statement.
func For[T any](ref *T) (scan Fn, err error) { return forValue(reflect.ValueOf(ref), ``) }

// forValue uses reflection to build a scanner with an optional prefix.
func forValue(rv reflect.Value, prefix string) (scan Fn, err error) {
	rt := rv.Type()
	makeScanner, err := forType(rt)
	if err != nil {
		return nil, err
	}
	return makeScanner(rv, prefix)

}

// forType returns a function that makes scanners for values of a given type and optional prefix using the
// standard "reflect" package.
func forType(rt reflect.Type) (
	makeScanner func(rv reflect.Value, prefix string) (scan Fn, err error),
	err error,
) {
	// Handle pointers by dereferencing. Allocates when non-NULL, sets nil when NULL.
	if rt.Kind() == reflect.Ptr {
		elemType := rt.Elem()
		elemScanner, err := forType(elemType)
		if err != nil {
			return nil, err
		}
		return func(rv reflect.Value, name string) (Fn, error) {
			// Build a test scanner to validate the type is scannable
			tmp := reflect.New(elemType)
			if _, err := elemScanner(tmp.Elem(), name); err != nil {
				return nil, err
			}
			var colIndex = -1
			var cachedScan Fn
			return func(stmt *sqlite.Stmt) error {
				if colIndex == -1 {
					if name == "" {
						colIndex = 0
					} else {
						colIndex = stmt.ColumnIndex(name)
					}
				}
				if colIndex == -1 {
					return nil
				}
				if stmt.ColumnType(colIndex) == sqlite.TypeNull {
					rv.Set(reflect.Zero(rt))
					cachedScan = nil // invalidate after NULL
					return nil
				}
				// Allocate and scan
				if rv.IsNil() {
					rv.Set(reflect.New(elemType))
				}
				if cachedScan == nil {
					var err error
					cachedScan, err = elemScanner(rv.Elem(), name)
					if err != nil {
						return err
					}
				}
				return cachedScan(stmt)
			}, nil
		}, nil
	}

	// Handle structs by using the schema
	if rt.Kind() == reflect.Struct {
		schema := findStructSchema(rt)
		schema.once.Do(func() { schema.build(rt) })
		if schema.err != nil {
			return nil, schema.err
		}
		return func(rv reflect.Value, prefix string) (scan Fn, err error) {
			return schema.makeScanner(rv, prefix)
		}, nil
	}

	// Handle basic types
	return func(rv reflect.Value, name string) (Fn, error) {
		// Optimization: we could look up the column index on the first run
		var colIndex = -1

		return func(stmt *sqlite.Stmt) error {
			if colIndex == -1 {
				if name == "" {
					colIndex = 0
				} else {
					colIndex = stmt.ColumnIndex(name)
				}
			}
			if colIndex == -1 {
				return nil
			}
			if stmt.ColumnType(colIndex) == sqlite.TypeNull {
				return nil
			}

			switch rt.Kind() {
			case reflect.String:
				rv.SetString(stmt.ColumnText(colIndex))
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				rv.SetInt(stmt.ColumnInt64(colIndex))
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				rv.SetUint(uint64(stmt.ColumnInt64(colIndex)))
			case reflect.Bool:
				rv.SetBool(stmt.ColumnInt64(colIndex) != 0)
			case reflect.Float32, reflect.Float64:
				rv.SetFloat(stmt.ColumnFloat(colIndex))
			case reflect.Slice:
				if rt.Elem().Kind() == reflect.Uint8 {
					// Handle []byte
					n := stmt.ColumnLen(colIndex)
					buf := make([]byte, n)
					stmt.ColumnBytes(colIndex, buf)
					rv.SetBytes(buf)
				} else if rt.Elem().Kind() == reflect.String {
					// Handle []string as JSON
					var val []string
					txt := stmt.ColumnText(colIndex)
					if err := json.Unmarshal([]byte(txt), &val); err != nil {
						return fmt.Errorf("cannot unmarshal %q into %v: %w", txt, rt, err)
					}
					rv.Set(reflect.ValueOf(val))
				} else {
					return fmt.Errorf("unsupported slice type %v", rt)
				}
			default:
				return fmt.Errorf("unsupported type %v", rt)
			}
			return nil
		}, nil
	}, nil
}

// findStructSchema finds the structured schema for a given type, using a cache to amortize the cost of analyzing
// fields in structures.
func findStructSchema(rt reflect.Type) *structSchema {
	schemaCacheControl.Lock()
	defer schemaCacheControl.Unlock()
	schema, ok := schemaCache[rt]
	if ok {
		return schema
	}
	schema = &structSchema{}
	schemaCache[rt] = schema
	return schema
}

var (
	schemaCacheControl sync.Mutex
	schemaCache        = make(map[reflect.Type]*structSchema)
)

type structSchema struct {
	Fields []fieldSchema

	once sync.Once
	err  error
}

func (schema *structSchema) build(rt reflect.Type) {
	numField := rt.NumField()
	for i := range numField {
		ft := rt.Field(i)
		if !ft.IsExported() {
			continue // ignore private fields
		}
		var field fieldSchema
		field.Index = i
		schema.err = field.build(ft)
		if schema.err != nil {
			return // stop, we have a field error.
		}
		if field.makeScanner == nil {
			continue // ignore fields that will not scan.
		}
		schema.Fields = append(schema.Fields, field)
	}
}

func (schema *structSchema) makeScanner(rv reflect.Value, prefix string) (scan Fn, err error) {
	scanners := make([]Fn, len(schema.Fields))
	for i := range schema.Fields {
		var err error
		name := schema.Fields[i].Name
		if prefix != "" {
			name = prefix + "_" + name
		}
		scanners[i], err = schema.Fields[i].makeScanner(rv.Field(schema.Fields[i].Index), name)
		if err != nil {
			return nil, err
		}
	}
	return func(stmt *sqlite.Stmt) (err error) {
		for _, fn := range scanners {
			err := fn(stmt)
			if err != nil {
				return err
			}
		}
		return nil
	}, nil
}

type fieldSchema struct {
	Index int
	Name  string

	makeScanner func(rv reflect.Value, prefix string) (Fn, error)
}

func (schema *fieldSchema) build(ft reflect.StructField) error {
	tag := strings.Split(ft.Tag.Get(`db`), `,`)
	for i, item := range tag {
		tag[i] = strings.TrimSpace(item)
	}
	if len(tag) == 0 || tag[0] == "" {
		schema.Name = inferFieldName(ft.Name)
	} else if tag[0] != `-` {
		schema.Name = tag[0] // we ignore fields named "-", much like Go.
	}
	if schema.Name == `` {
		return nil // ignore anonymous fields.
	}

	// Check for "json" option
	isJSON := false
	for _, t := range tag[1:] {
		if t == "json" {
			isJSON = true
			break
		}
	}

	err := validateName(schema.Name) // check for SQL validity of the name.
	if err != nil {
		return err
	}

	if isJSON {
		schema.makeScanner = makeJSONScanner(ft.Type)
		return nil
	}

	schema.makeScanner, err = forType(ft.Type)
	if err != nil {
		return fmt.Errorf("cannot scan %v from %v: %w", ft.Type.String(), schema.Name, err)
	}
	return nil
}

func makeJSONScanner(rt reflect.Type) func(reflect.Value, string) (Fn, error) {
	return func(rv reflect.Value, name string) (Fn, error) {
		var colIndex = -1
		return func(stmt *sqlite.Stmt) error {
			if colIndex == -1 {
				if name == "" {
					colIndex = 0
				} else {
					colIndex = stmt.ColumnIndex(name)
				}
			}
			if colIndex == -1 {
				return nil
			}
			if stmt.ColumnType(colIndex) == sqlite.TypeNull {
				return nil
			}

			txt := stmt.ColumnText(colIndex)
			// Unmarshal using a new instance of the type to handle pointers/maps correctly
			val := reflect.New(rt).Interface()
			if err := json.Unmarshal([]byte(txt), val); err != nil {
				return fmt.Errorf("cannot unmarshal %q into %v: %w", txt, rt, err)
			}
			rv.Set(reflect.ValueOf(val).Elem())
			return nil
		}, nil
	}
}

// inferFieldName converts a titlecased Go structure field name into a SQL column name by splitting it up into words, then joining
// those words with underscores.
func inferFieldName(name string) string {
	var words []string
	start := 0
	runes := []rune(name)
	for i, r := range runes {
		if i > 0 && unicode.IsUpper(r) {
			prev := runes[i-1]
			nextLower := false
			if i+1 < len(runes) && unicode.IsLower(runes[i+1]) {
				nextLower = true
			}

			if unicode.IsLower(prev) || (unicode.IsUpper(prev) && nextLower) {
				words = append(words, string(runes[start:i]))
				start = i
			}
		}
	}
	words = append(words, string(runes[start:]))
	return strings.ToLower(strings.Join(words, "_"))
}

// validateName checks if the provided column name is valid in SQL.
func validateName(name string) error {
	for _, r := range name {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_') {
			return fmt.Errorf("invalid column name %q", name)
		}
	}
	return nil
}

type Fn func(stmt *sqlite.Stmt) (err error)
