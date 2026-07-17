package jsonfacts

import (
	"encoding/json"
	"fmt"
	"io"
	"strconv"

	"swdunlop.dev/pkg/datalog"
)

// Encoder writes facts as JSONL (one JSON object per line).
// Term names are derived from declarations provided at construction.
type Encoder struct {
	w     io.Writer
	decls map[declKey][]string // (pred, arity) -> term names
}

type declKey struct {
	pred  string
	arity int
}

// NewEncoder creates an Encoder that writes to w. The declarations provide
// term names for encoding facts as named JSON fields.
func NewEncoder(w io.Writer, decls []datalog.Declaration) *Encoder {
	m := make(map[declKey][]string)
	for _, d := range decls {
		names := make([]string, len(d.Terms))
		for i, t := range d.Terms {
			names[i] = t.Name
		}
		m[declKey{d.Name, len(d.Terms)}] = names
	}
	return &Encoder{w: w, decls: m}
}

// Encode writes a single fact as a JSONL line.
// If a matching declaration provides term names, the output is:
//
//	{"predicate": {"term1": val1, "term2": val2, ...}}
//
// Otherwise, terms are keyed by their positional index as strings.
//
// Encode returns an error rather than silently dropping a value if two
// terms of the matching declaration resolve to the same JSON object key
// (obj[key] = ... below is a plain Go map write, so a duplicate key would
// otherwise silently overwrite an earlier term's value with a later one's).
// Config.validate rejects such a declaration before any fact reaches this
// point when the declarations came from a Config, but this check is the
// backstop for any other caller that builds a []datalog.Declaration by hand
// and passes it directly to NewEncoder without going through Config.
func (e *Encoder) Encode(pred string, row []datalog.Constant) error {
	obj := make(map[string]any, len(row))
	names := e.decls[declKey{pred, len(row)}]
	seen := make(map[string]int, len(row))
	for i, c := range row {
		key := termKey(names, i)
		if prev, ok := seen[key]; ok {
			return fmt.Errorf("encoding %s: term %d and term %d both resolve to JSON key %q; declare distinct term names to avoid silently dropping a value", pred, prev, i, key)
		}
		seen[key] = i
		obj[key] = constantToJSON(c)
	}

	line := map[string]any{pred: obj}
	data, err := json.Marshal(line)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	_, err = e.w.Write(data)
	return err
}

func termKey(names []string, i int) string {
	if i < len(names) && names[i] != "" {
		return names[i]
	}
	return strconv.Itoa(i) // "0", "1", ..., "10", "11", ... for any arity
}

func constantToJSON(c datalog.Constant) any {
	switch v := c.(type) {
	case datalog.String:
		return string(v)
	case datalog.Integer:
		return int64(v)
	case datalog.Float:
		return float64(v)
	case datalog.ID:
		return uint64(v)
	case datalog.Bool:
		return bool(v)
	case datalog.Null:
		return nil
	case *datalog.Composite:
		// Encodes as the decoded JSON value, so records asserted whole
		// round-trip through the pipeline.
		return v.Value()
	default:
		return c.String()
	}
}
