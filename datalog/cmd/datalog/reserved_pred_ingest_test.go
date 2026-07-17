package main

import (
	"context"
	"io"
	"strings"
	"testing"

	"swdunlop.dev/pkg/datalog/syntax"
)

// TestReplExecStatementRejectsReservedPred is the regression test for the
// funnel gap the validator found: repl.execStatement parsed a single statement
// via syntax.ParseStatement and appended it straight into the session without
// the reservedQueryPred check, so a user could inject a `_q_` fact that later
// leaked into unrelated query results (the exact BUG #2 silent-wrong-answer,
// still live on the interactive/pipe REPL path).
func TestReplExecStatementRejectsReservedPred(t *testing.T) {
	r := &repl{session: &session{}, out: io.Discard}

	if err := r.execStatement(`_q_("boom").`); err == nil {
		t.Fatal("expected execStatement to reject a _q_ fact, got nil error")
	} else if !strings.Contains(err.Error(), "reserved") {
		t.Fatalf("expected a reservation error, got: %v", err)
	}
	if len(r.facts) != 0 {
		t.Fatalf("rejected _q_ fact must not enter the session, got %d facts", len(r.facts))
	}

	// A normal fact still loads.
	if err := r.execStatement(`event("real").`); err != nil {
		t.Fatalf("normal fact should load: %v", err)
	}
	if len(r.facts) != 1 {
		t.Fatalf("expected 1 real fact, got %d", len(r.facts))
	}

	// A _q_ rule head and a _q_ query atom are also rejected.
	if err := r.execStatement(`_q_(X) :- event(X).`); err == nil {
		t.Fatal("expected execStatement to reject a _q_ rule head")
	}
	if err := r.execStatement(`_q_(X)?`); err == nil {
		t.Fatal("expected execStatement to reject a _q_ query atom")
	}
}

// TestMCPQueryRejectsReservedPred covers the second gap: the MCP query handler
// used syntax.ParseStatement without the reservation check, accepting
// `_q_(X)?` that the console handler rejects.
func TestMCPQueryRejectsReservedPred(t *testing.T) {
	h := &mcpHandlers{sess: &session{}}
	_, err := h.query(context.Background(), queryInput{Query: `_q_(X)?`})
	if err == nil {
		t.Fatal("expected MCP query handler to reject a _q_ query atom, got nil error")
	}
	if !strings.Contains(err.Error(), "reserved") {
		t.Fatalf("expected a reservation error, got: %v", err)
	}
}

// TestValidateStatementNoReservedPredAcceptsNormal guards against over-rejection
// at the single-statement surface: only the exact predicate _q_ is reserved.
func TestValidateStatementNoReservedPredAcceptsNormal(t *testing.T) {
	for _, src := range []string{`event("x").`, `out(X) :- event(X).`, `_query(X)?`, `q_(1).`} {
		stmt, err := syntax.ParseStatement(src)
		if err != nil {
			t.Fatalf("parse %q: %v", src, err)
		}
		if err := validateStatementNoReservedPred(stmt); err != nil {
			t.Errorf("%q should be accepted, got: %v", src, err)
		}
	}
}
