package finbench_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"swdunlop.dev/pkg/datalog"
	"swdunlop.dev/pkg/datalog/jsonfacts"
	"swdunlop.dev/pkg/datalog/seminaive"
	"swdunlop.dev/pkg/datalog/syntax"
)

// TestFinBenchCycleQuery loads the real SF0.01 dataset and runs the cycle query.
// Requires setup.sh to have been run first to download and convert the data.
func TestFinBenchCycleQuery(t *testing.T) {
	if _, err := os.Stat(filepath.Join("data", "jsonl", "account_transfer.jsonl")); err != nil {
		t.Skip("FinBench data not available; run setup.sh first")
	}

	cfg := jsonfacts.Config{
		Sources: []jsonfacts.Source{
			{
				File: "data/jsonl/person_own_account.jsonl",
				Mappings: []jsonfacts.Mapping{{
					Predicate: "person_own_account",
					Args:      []string{"value.personId", "value.accountId"},
				}},
			},
			{
				File: "data/jsonl/account_transfer.jsonl",
				Mappings: []jsonfacts.Mapping{{
					Predicate: "account_transfer",
					Args:      []string{"value.fromId", "value.toId", "value.amount"},
				}},
			},
			{
				File: "data/jsonl/person.jsonl",
				Mappings: []jsonfacts.Mapping{{
					Predicate: "person_name",
					Args:      []string{"value.personId", "value.personName"},
				}},
			},
			{
				File: "data/jsonl/company_own_account.jsonl",
				Mappings: []jsonfacts.Mapping{{
					Predicate: "company_own_account",
					Args:      []string{"value.companyId", "value.accountId"},
				}},
			},
			{
				File: "data/jsonl/company.jsonl",
				Mappings: []jsonfacts.Mapping{{
					Predicate: "company_name",
					Args:      []string{"value.companyId", "value.companyName"},
				}},
			},
			{
				File: "data/jsonl/account.jsonl",
				Mappings: []jsonfacts.Mapping{{
					Predicate: "account_type",
					Args:      []string{"value.accountId", "value.accoutType"},
				}},
			},
		},
	}

	input, err := cfg.LoadDir(".")
	if err != nil {
		t.Fatal(err)
	}

	rs, err := syntax.ParseAll(`
		owns(Owner, Account) :- person_own_account(Owner, Account).
		owns(Owner, Account) :- company_own_account(Owner, Account).
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

	count := 0
	for row := range output.Facts("cycle", 3) {
		owner := string(row[0].(datalog.String))
		a1 := string(row[1].(datalog.String))
		a2 := string(row[2].(datalog.String))
		t.Logf("  Owner=%s A1=%s A2=%s", owner, a1, a2)
		count++
	}
	t.Logf("Found %d cycle(s)", count)
}
