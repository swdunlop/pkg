package jsonfacts

import (
	"github.com/expr-lang/expr"
)

// AssertedFact is one fact an expr program asserted during EvalExpr —
// collected for display rather than loaded into any database.
type AssertedFact struct {
	Predicate string
	Args      []any
}

// EvalExpr compiles and runs one expr program against a single decoded
// JSONL record, with the same environment ({"value": record}) and builtins
// (fresh_id, assert, match_contains/starts_with/ends_with/regex) an
// imperative mapping expr gets at load time. Asserted facts are collected
// and returned instead of being loaded anywhere: this is the workbench's
// `!` probe (doc/features/workbench-v2.md design decision 8), answering
// "what would this expr do against that record" without touching the
// session. The fresh_id counter starts at zero on every call, so probe
// output is deterministic for a given (src, record) pair.
func EvalExpr(src string, record any) (result any, asserted []AssertedFact, err error) {
	env := map[string]any{"value": record}
	counter := &idCounter{}
	prog, err := compileImperative(src, env, counter, func(pred string, args []any) {
		asserted = append(asserted, AssertedFact{Predicate: pred, Args: args})
	})
	if err != nil {
		return nil, nil, err
	}
	result, err = expr.Run(prog, env)
	if err != nil {
		return nil, nil, err
	}
	return result, asserted, nil
}
