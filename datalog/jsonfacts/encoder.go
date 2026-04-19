package jsonfacts

import (
	"encoding/json"
	"io"

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
func (e *Encoder) Encode(pred string, row []datalog.Constant) error {
	obj := make(map[string]any, len(row))
	names := e.decls[declKey{pred, len(row)}]
	for i, c := range row {
		key := termKey(names, i)
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
	return string(rune('0' + i)) // "0", "1", ..., "9" for small arities
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
	default:
		return c.String()
	}
}
