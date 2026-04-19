package finbench_test

import (
	"context"
	"testing"

	"swdunlop.dev/pkg/datalog"
	"swdunlop.dev/pkg/datalog/memory"
	"swdunlop.dev/pkg/datalog/seminaive"
	"swdunlop.dev/pkg/datalog/syntax"
)

// TestCycleDetection verifies the full cyclic-ownership logic with synthetic data.
//
// Setup:
//
//	person P1 owns accounts A1, A3
//	person P2 owns accounts A2, A4
//	transfers: A1->A2, A2->A3, A4->A1
//
// Expected cycles:
//
//	P1: A1->A2->A3 (length 2) -- P1 owns A1 and A3
//	P2: A4->A1->A2 (length 2) -- P2 owns A4 and A2
func TestCycleDetection(t *testing.T) {
	b := memory.NewBuilder()

	// Ownership
	addFact(t, b, "person_own_account", "P1", "A1")
	addFact(t, b, "person_own_account", "P1", "A3")
	addFact(t, b, "person_own_account", "P2", "A2")
	addFact(t, b, "person_own_account", "P2", "A4")

	// Transfers
	addFact(t, b, "account_transfer", "A1", "A2", "100.0")
	addFact(t, b, "account_transfer", "A2", "A3", "50.0")
	addFact(t, b, "account_transfer", "A4", "A1", "200.0")

	input := b.Build()

	rs, err := syntax.ParseAll(`
		owns(Owner, Account) :- person_own_account(Owner, Account).
		transfer(From, To) :- account_transfer(From, To, _).
		reachable(From, To) :- transfer(From, To).
		reachable(From, To) :- reachable(From, Mid), transfer(Mid, To).
		cycle(Owner, A1, A2) :- owns(Owner, A1), owns(Owner, A2), A1 != A2, reachable(A1, A2).
	`)
	if err != nil {
		t.Fatal(err)
	}
	tr, err := seminaive.New().Compile(rs)
	if err != nil {
		t.Fatal(err)
	}

	output, err := tr.Transform(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}

	type cycle struct{ owner, a1, a2 string }
	var results []cycle
	for row := range output.Facts("cycle", 3) {
		results = append(results, cycle{
			owner: string(row[0].(datalog.String)),
			a1:    string(row[1].(datalog.String)),
			a2:    string(row[2].(datalog.String)),
		})
	}

	t.Logf("Found %d cycle result(s):", len(results))
	for _, c := range results {
		t.Logf("  Owner=%s A1=%s A2=%s", c.owner, c.a1, c.a2)
	}

	// Expect two cycles:
	//   P1: A1->...->A3
	//   P2: A4->...->A2
	expect := map[string][2]string{
		"P1": {"A1", "A3"},
		"P2": {"A4", "A2"},
	}
	for _, c := range results {
		pair, ok := expect[c.owner]
		if !ok {
			t.Errorf("unexpected owner %s", c.owner)
			continue
		}
		if c.a1 != pair[0] || c.a2 != pair[1] {
			t.Errorf("owner %s: got A1=%s A2=%s, want A1=%s A2=%s",
				c.owner, c.a1, c.a2, pair[0], pair[1])
		}
		delete(expect, c.owner)
	}
	for owner, pair := range expect {
		t.Errorf("missing cycle for %s (%s->%s)", owner, pair[0], pair[1])
	}
}

func addFact(t *testing.T, b *memory.Builder, pred string, args ...string) {
	t.Helper()
	terms := make([]datalog.Constant, len(args))
	for i, a := range args {
		terms[i] = datalog.String(a)
	}
	b.AddFact(datalog.Fact{Name: pred, Terms: terms})
}
