package seminaive_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"swdunlop.dev/pkg/datalog"
	"swdunlop.dev/pkg/datalog/memory"
	"swdunlop.dev/pkg/datalog/seminaive"
	"swdunlop.dev/pkg/datalog/syntax"
)

// TestProvenanceTwoRuleChain proves the core mechanism end-to-end: a base
// fact feeds a first-order rule, whose output feeds a second rule, and
// Explain/ExplainTree recover both hops correctly -- rule text, body facts,
// and base-fact leaves.
func TestProvenanceTwoRuleChain(t *testing.T) {
	b := memory.NewBuilder()
	b.AddFact(datalog.Fact{Name: "parent", Terms: []datalog.Constant{datalog.String("tom"), datalog.String("bob")}})
	b.AddFact(datalog.Fact{Name: "parent", Terms: []datalog.Constant{datalog.String("bob"), datalog.String("ann")}})
	input := b.Build()

	rs, err := syntax.ParseAll(`
		grandparent(X, Z) :- parent(X, Y), parent(Y, Z).
		ancestor(X, Z) :- grandparent(X, Z).
	`)
	if err != nil {
		t.Fatal(err)
	}

	prov := seminaive.NewProvenance()
	tr, err := seminaive.New(seminaive.WithProvenance(prov)).Compile(rs)
	if err != nil {
		t.Fatal(err)
	}

	output, err := tr.Transform(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	_ = output

	ancestor := datalog.Fact{Name: "ancestor", Terms: []datalog.Constant{datalog.String("tom"), datalog.String("ann")}}
	d, ok := prov.Explain(ancestor)
	if !ok {
		t.Fatalf("Explain(%v) not found", ancestor)
	}
	if d.Base {
		t.Fatalf("ancestor fact should not be a base fact")
	}
	if !strings.Contains(d.Rule, "ancestor(X, Z) :- grandparent(X, Z)") {
		t.Errorf("unexpected rule text: %q", d.Rule)
	}
	if len(d.Body) != 1 || d.Body[0].Fact.Name != "grandparent" {
		t.Fatalf("expected one grandparent body fact, got %+v", d.Body)
	}

	tree, ok := prov.ExplainTree(ancestor)
	if !ok {
		t.Fatalf("ExplainTree(%v) not found", ancestor)
	}
	if len(tree.Body) != 1 {
		t.Fatalf("expected 1 child, got %d", len(tree.Body))
	}
	gp := tree.Body[0]
	if gp.Base {
		t.Fatalf("grandparent should have its own witness, not be base")
	}
	if !strings.Contains(gp.Rule, "grandparent(X, Z) :- parent(X, Y), parent(Y, Z)") {
		t.Errorf("unexpected grandparent rule text: %q", gp.Rule)
	}
	if len(gp.Body) != 2 {
		t.Fatalf("expected 2 parent body facts, got %d", len(gp.Body))
	}
	for _, p := range gp.Body {
		if p.Fact.Name != "parent" {
			t.Errorf("expected parent body fact, got %s", p.Fact.Name)
		}
		if !p.Base {
			t.Errorf("parent(%v) should be a base fact", p.Fact.Terms)
		}
	}

	rendered := tree.String()
	if !strings.Contains(rendered, "ancestor(") || !strings.Contains(rendered, "grandparent(") || !strings.Contains(rendered, "[base fact]") {
		t.Errorf("rendered tree missing expected content:\n%s", rendered)
	}
	t.Logf("rendered tree:\n%s", rendered)
}

// TestProvenanceDisabledChangesNothing confirms that compiling and
// transforming without WithProvenance behaves identically to before the
// feature existed -- no observable side effects, same results.
func TestProvenanceDisabledChangesNothing(t *testing.T) {
	b := memory.NewBuilder()
	b.AddFact(datalog.Fact{Name: "parent", Terms: []datalog.Constant{datalog.String("tom"), datalog.String("bob")}})
	b.AddFact(datalog.Fact{Name: "parent", Terms: []datalog.Constant{datalog.String("bob"), datalog.String("ann")}})
	input := b.Build()

	rs, err := syntax.ParseAll(`grandparent(X, Z) :- parent(X, Y), parent(Y, Z).`)
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

	var results []string
	for row := range output.Facts("grandparent", 2) {
		results = append(results, string(row[0].(datalog.String))+","+string(row[1].(datalog.String)))
	}
	if len(results) != 1 || results[0] != "tom,ann" {
		t.Errorf("expected [tom,ann], got %v", results)
	}
}

// TestProvenanceBaseFact confirms that an input-database fact (never
// derived) explains as a base fact via a Provenance-enabled Transform, even
// though it appears in the output database.
func TestProvenanceBaseFact(t *testing.T) {
	b := memory.NewBuilder()
	b.AddFact(datalog.Fact{Name: "parent", Terms: []datalog.Constant{datalog.String("tom"), datalog.String("bob")}})
	input := b.Build()

	rs, err := syntax.ParseAll(`nobody(X) :- parent(X, Y), parent(Y, X).`) // never fires; just needs a rule present
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

	parent := datalog.Fact{Name: "parent", Terms: []datalog.Constant{datalog.String("tom"), datalog.String("bob")}}
	d, ok := prov.Explain(parent)
	if !ok {
		t.Fatalf("Explain(%v) not found", parent)
	}
	if !d.Base {
		t.Errorf("input fact should explain as base, got %+v", d)
	}
}

// TestProvenanceAssertedFact confirms a fact asserted directly in the
// ruleset (a rule with an empty, fully-ground body) also explains as base --
// the spec calls out that asserted facts get no witness, same as input
// facts, distinguished only by "absence from the map".
func TestProvenanceAssertedFact(t *testing.T) {
	input := memory.NewBuilder().Build()
	rs, err := syntax.ParseAll(`likes("ann", "pizza").`)
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

	fact := datalog.Fact{Name: "likes", Terms: []datalog.Constant{datalog.String("ann"), datalog.String("pizza")}}
	d, ok := prov.Explain(fact)
	if !ok {
		t.Fatalf("Explain(%v) not found", fact)
	}
	if !d.Base {
		t.Errorf("asserted fact should explain as base, got %+v", d)
	}
}

// TestProvenanceConstraintDetail confirms a constraint (comparison) body
// item grounds to a readable detail line citing the resolved values.
func TestProvenanceConstraintDetail(t *testing.T) {
	b := memory.NewBuilder()
	b.AddFact(datalog.Fact{Name: "port_count", Terms: []datalog.Constant{datalog.String("ws01"), datalog.Integer(34)}})
	input := b.Build()

	rs, err := syntax.ParseAll(`concern(H) :- port_count(H, N), N > 20.`)
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

	fact := datalog.Fact{Name: "concern", Terms: []datalog.Constant{datalog.String("ws01")}}
	d, ok := prov.Explain(fact)
	if !ok {
		t.Fatalf("Explain(%v) not found", fact)
	}
	if len(d.Detail) != 1 {
		t.Fatalf("expected 1 detail line, got %v", d.Detail)
	}
	if !strings.Contains(d.Detail[0], "34") || !strings.Contains(d.Detail[0], ">") || !strings.Contains(d.Detail[0], "20") {
		t.Errorf("unexpected constraint detail: %q", d.Detail[0])
	}
}

// TestProvenanceNegationDetail confirms a negated body atom grounds to a
// detail line describing the atom that had no match.
func TestProvenanceNegationDetail(t *testing.T) {
	b := memory.NewBuilder()
	b.AddFact(datalog.Fact{Name: "user", Terms: []datalog.Constant{datalog.String("ann")}})
	input := b.Build()

	rs, err := syntax.ParseAll(`orphan(X) :- user(X), not banned(X).`)
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

	fact := datalog.Fact{Name: "orphan", Terms: []datalog.Constant{datalog.String("ann")}}
	d, ok := prov.Explain(fact)
	if !ok {
		t.Fatalf("Explain(%v) not found", fact)
	}
	if len(d.Detail) != 1 {
		t.Fatalf("expected 1 detail line, got %v", d.Detail)
	}
	if !strings.Contains(d.Detail[0], "not") || !strings.Contains(d.Detail[0], "banned") || !strings.Contains(d.Detail[0], "ann") {
		t.Errorf("unexpected negation detail: %q", d.Detail[0])
	}
}

// TestProvenanceDiscardedOnFactLimitAbort confirms that a Transform aborted
// by WithFactLimit leaves the caller's Provenance exactly as it was before
// the call -- no partial witnesses from the failed run leak through.
func TestProvenanceDiscardedOnFactLimitAbort(t *testing.T) {
	b := memory.NewBuilder()
	for i := 0; i < 50; i++ {
		b.AddFact(datalog.Fact{Name: "node", Terms: []datalog.Constant{datalog.Integer(int64(i))}})
	}
	input := b.Build()

	// Cross product blows past a tiny fact limit.
	rs, err := syntax.ParseAll(`pair(X, Y) :- node(X), node(Y).`)
	if err != nil {
		t.Fatal(err)
	}
	prov := seminaive.NewProvenance()
	tr, err := seminaive.New(seminaive.WithProvenance(prov), seminaive.WithFactLimit(5)).Compile(rs)
	if err != nil {
		t.Fatal(err)
	}

	_, err = tr.Transform(context.Background(), input)
	if err == nil {
		t.Fatalf("expected a FactLimitError")
	}
	var fle seminaive.FactLimitError
	if !errors.As(err, &fle) {
		t.Fatalf("expected FactLimitError, got %v", err)
	}

	// prov must still be in its pre-Transform (empty) state: Explain finds
	// nothing for a pair fact, since install() never ran.
	probe := datalog.Fact{Name: "pair", Terms: []datalog.Constant{datalog.Integer(0), datalog.Integer(0)}}
	if _, ok := prov.Explain(probe); ok {
		t.Fatalf("Explain found a witness after an aborted Transform; partial provenance leaked")
	}
}

// TestProvenanceRepeatedBodyAtomGroundsDistinctFacts covers the
// double-occurrence grounding trap called out in the feature spec: a body
// atom appearing twice with different variables must ground to two distinct
// facts under the winning substitution, not the same fact twice.
func TestProvenanceRepeatedBodyAtomGroundsDistinctFacts(t *testing.T) {
	b := memory.NewBuilder()
	b.AddFact(datalog.Fact{Name: "edge", Terms: []datalog.Constant{datalog.String("a"), datalog.String("b")}})
	b.AddFact(datalog.Fact{Name: "edge", Terms: []datalog.Constant{datalog.String("b"), datalog.String("c")}})
	input := b.Build()

	rs, err := syntax.ParseAll(`path2(X, Z) :- edge(X, Y), edge(Y, Z).`)
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

	fact := datalog.Fact{Name: "path2", Terms: []datalog.Constant{datalog.String("a"), datalog.String("c")}}
	d, ok := prov.Explain(fact)
	if !ok {
		t.Fatalf("Explain(%v) not found", fact)
	}
	if len(d.Body) != 2 {
		t.Fatalf("expected 2 body facts, got %d", len(d.Body))
	}
	if d.Body[0].Fact.Terms[0].(datalog.String) == d.Body[1].Fact.Terms[0].(datalog.String) &&
		d.Body[0].Fact.Terms[1].(datalog.String) == d.Body[1].Fact.Terms[1].(datalog.String) {
		t.Errorf("expected two distinct edge facts, got the same fact twice: %+v", d.Body)
	}
}
