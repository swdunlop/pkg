package seminaive_test

import (
	"context"
	"strings"
	"testing"

	"swdunlop.dev/pkg/datalog/memory"
	"swdunlop.dev/pkg/datalog/seminaive"
	"swdunlop.dev/pkg/datalog/syntax"
)

// TestProbeRuleIdentityInterleavedStrata stresses the flat-index threading:
// rules whose source order does NOT match their stratum grouping order, with
// the same head predicate appearing in more than one rule, so a witness that
// points at the wrong flat index would render the wrong rule text.
func TestProbeRuleIdentityInterleavedStrata(t *testing.T) {
	// Deliberately interleave strata in source order:
	//  rule0: c depends on b (stratum 2)
	//  rule1: a depends on base (stratum 1)
	//  rule2: b depends on a (stratum 2? no: b<-a, a<-base, so b is stratum 2, c stratum 3)
	//  rule3: c depends on b again (another rule, same head c)
	// So flat indices 0..3 land in strata out of order; the per-stratum
	// ruleIdx must recover each rule's true text.
	const rules = `
		c(X) :- b(X), tag1(X).
		a(X) :- base(X).
		b(X) :- a(X).
		c(X) :- b(X), tag2(X).
	`
	// Add tag facts so both c-rules can fire distinctly.
	b2 := memory.NewBuilder()
	b2.AddFact(fact("base", str("x")))
	b2.AddFact(fact("base", str("y")))
	b2.AddFact(fact("tag1", str("x")))
	b2.AddFact(fact("tag2", str("y")))
	input := b2.Build()

	rs, err := syntax.ParseAll(rules)
	if err != nil {
		t.Fatal(err)
	}
	prov := seminaive.NewProvenance()
	tr, err := seminaive.New(seminaive.WithProvenance(prov)).Compile(rs)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tr.Transform(context.Background(), input); err != nil {
		t.Fatal(err)
	}

	// c(x) must be derived by the tag1 rule; c(y) by the tag2 rule.
	dx, ok := prov.Explain(fact("c", str("x")))
	if !ok {
		t.Fatal("Explain c(x) not found")
	}
	if !strings.Contains(dx.Rule, "tag1(X)") {
		t.Errorf("c(x) should cite the tag1 rule, got: %q", dx.Rule)
	}
	dy, ok := prov.Explain(fact("c", str("y")))
	if !ok {
		t.Fatal("Explain c(y) not found")
	}
	if !strings.Contains(dy.Rule, "tag2(X)") {
		t.Errorf("c(y) should cite the tag2 rule, got: %q", dy.Rule)
	}

	// a(x) must cite the a rule; b(x) the b rule.
	da, _ := prov.Explain(fact("a", str("x")))
	if !strings.Contains(da.Rule, "a(X) :- base(X)") {
		t.Errorf("a(x) rule text wrong: %q", da.Rule)
	}
	db, _ := prov.Explain(fact("b", str("x")))
	if !strings.Contains(db.Rule, "b(X) :- a(X)") {
		t.Errorf("b(x) rule text wrong: %q", db.Rule)
	}
}

// TestProbeAggRuleIdentityInterleaved checks aggregate flat-index threading
// with plain and aggregate rules interleaved in source order and multiple
// aggregate rules whose head predicates differ.
func TestProbeAggRuleIdentityInterleaved(t *testing.T) {
	b := memory.NewBuilder()
	b.AddFact(fact("m", str("h"), i(1)))
	b.AddFact(fact("m", str("h"), i(5)))
	b.AddFact(fact("m", str("h"), i(9)))
	input := b.Build()

	// Two aggregate rules over the same body predicate, plus a plain rule
	// interleaved between them.
	const rules = `
		mx(H, V) :- V = max(X) : m(H, X).
		passthrough(H, X) :- m(H, X).
		mn(H, V) :- V = min(X) : m(H, X).
	`
	rs, err := syntax.ParseAll(rules)
	if err != nil {
		t.Fatal(err)
	}
	prov := seminaive.NewProvenance()
	tr, err := seminaive.New(seminaive.WithProvenance(prov)).Compile(rs)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tr.Transform(context.Background(), input); err != nil {
		t.Fatal(err)
	}

	dmax, ok := prov.Explain(fact("mx", str("h"), i(9)))
	if !ok {
		t.Fatal("Explain mx not found")
	}
	if !strings.Contains(dmax.Rule, "max(X)") {
		t.Errorf("mx should cite the max rule, got: %q", dmax.Rule)
	}
	dmin, ok := prov.Explain(fact("mn", str("h"), i(1)))
	if !ok {
		t.Fatal("Explain mn not found")
	}
	if !strings.Contains(dmin.Rule, "min(X)") {
		t.Errorf("mn should cite the min rule, got: %q", dmin.Rule)
	}
}
