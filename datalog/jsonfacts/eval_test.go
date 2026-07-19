package jsonfacts

import "testing"

// TestEvalExpr pins the workbench `!` probe's contract: the mapping-expr
// environment (value bound to the record), asserted facts collected rather
// than loaded, and compile errors surfaced.
func TestEvalExpr(t *testing.T) {
	record := map[string]any{"host": "WS6", "pid": float64(42)}

	result, asserted, err := EvalExpr(`value.host`, record)
	if err != nil {
		t.Fatalf("EvalExpr: %v", err)
	}
	if result != "WS6" || len(asserted) != 0 {
		t.Fatalf("got (%v, %v)", result, asserted)
	}

	_, asserted, err = EvalExpr(`assert("seen", [value.host, value.pid])`, record)
	if err != nil {
		t.Fatalf("EvalExpr assert: %v", err)
	}
	if len(asserted) != 1 || asserted[0].Predicate != "seen" || asserted[0].Args[0] != "WS6" {
		t.Fatalf("asserted = %+v", asserted)
	}

	if _, _, err := EvalExpr(`value.`, record); err == nil {
		t.Fatal("expected a compile error for malformed expr")
	}
}
