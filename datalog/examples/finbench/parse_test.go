package finbench_test

import (
	"testing"

	"swdunlop.dev/pkg/datalog/syntax"
)

// TestRulesParse verifies that every rule from the FinBench example parses.
func TestRulesParse(t *testing.T) {
	rules := []string{
		`owns(Owner, Account) :- person_own_account(Owner, Account).`,
		`owns(Owner, Account) :- company_own_account(Owner, Account).`,
		`transfer(From, To) :- account_transfer(From, To, _).`,
		`reachable(From, To) :- transfer(From, To).`,
		`reachable(From, To) :- reachable(From, Mid), transfer(Mid, To).`,
		`cycle(Owner, A1, A2) :- owns(Owner, A1), owns(Owner, A2), A1 != A2, reachable(A1, A2).`,
	}
	for _, src := range rules {
		result, err := syntax.ParseStatement(src)
		if err != nil {
			t.Errorf("parse %q: %v", src, err)
			continue
		}
		r, ok := result.(*syntax.Rule)
		if !ok {
			t.Errorf("parse %q: expected *Rule, got %T", src, result)
			continue
		}
		t.Logf("OK: %s", r)
	}
}
