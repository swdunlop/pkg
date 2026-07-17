package seminaive_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"swdunlop.dev/pkg/datalog"
	"swdunlop.dev/pkg/datalog/memory"
	"swdunlop.dev/pkg/datalog/seminaive"
	"swdunlop.dev/pkg/datalog/syntax"
)

// TestProvenanceAggregateSumUnderCap confirms an aggregate witness under
// witnessSampleCap (10) reports the true group cardinality, samples every
// solution (Sampled false), and every sampled body fact resolves to a
// genuine fact in the output database -- the "a witness citing a
// nonexistent fact is a lie" invariant the spec calls out for correctness
// tests.
func TestProvenanceAggregateSumUnderCap(t *testing.T) {
	b := memory.NewBuilder()
	weights := []int64{10, 20, 30}
	for _, w := range weights {
		b.AddFact(datalog.Fact{Name: "indicator", Terms: []datalog.Constant{datalog.String("ws01"), datalog.Integer(w)}})
	}
	input := b.Build()

	rs, err := syntax.ParseAll(`concern(H, S) :- S = sum(W) : indicator(H, W).`)
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

	fact := datalog.Fact{Name: "concern", Terms: []datalog.Constant{datalog.String("ws01"), datalog.Integer(60)}}
	d, ok := prov.Explain(fact)
	if !ok {
		t.Fatalf("Explain(%v) not found", fact)
	}
	if !d.Aggregate {
		t.Fatalf("expected an aggregate Derivation, got %+v", d)
	}
	if d.GroupCount != 3 {
		t.Errorf("expected GroupCount 3, got %d", d.GroupCount)
	}
	if d.Sampled {
		t.Errorf("group is under the sample cap; expected Sampled false")
	}
	if len(d.Body) != 3 {
		t.Fatalf("expected 3 sampled solutions, got %d", len(d.Body))
	}
	if !strings.Contains(d.Rule, "concern(H, S) :- S = sum(W) : indicator(H, W)") {
		t.Errorf("unexpected rule text: %q", d.Rule)
	}

	// Every sampled body fact must genuinely exist in the output database.
	seenWeights := map[int64]bool{}
	for _, sample := range d.Body {
		if len(sample.Body) != 1 || sample.Body[0].Fact.Name != "indicator" {
			t.Fatalf("expected one indicator body fact per sample, got %+v", sample.Body)
		}
		bf := sample.Body[0]
		if !bf.Base {
			t.Errorf("indicator(%v) should be a base fact", bf.Fact.Terms)
		}
		w := int64(bf.Fact.Terms[1].(datalog.Integer))
		seenWeights[w] = true
		found := false
		for row := range input.Facts("indicator", 2) {
			if row[0].(datalog.String) == bf.Fact.Terms[0].(datalog.String) &&
				int64(row[1].(datalog.Integer)) == w {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("sampled body fact indicator(%v, %d) does not exist in the input database -- witness lies", bf.Fact.Terms[0], w)
		}
	}
	for _, w := range weights {
		if !seenWeights[w] {
			t.Errorf("expected weight %d among the sampled solutions, got %v", w, seenWeights)
		}
	}

	rendered := d.String()
	if !strings.Contains(rendered, "aggregated over 3 solutions (all shown):") {
		t.Errorf("rendered tree missing expected summary line:\n%s", rendered)
	}
	t.Logf("rendered:\n%s", rendered)
}

// TestProvenanceAggregateCountOverCap confirms a group whose true
// cardinality exceeds witnessSampleCap (10) reports the exact cardinality
// while capping the retained sample at 10 and marking Sampled true --
// proving the "count over 500k solutions must not materialize them"
// requirement holds at a scale small enough to assert exactly, and that the
// cap/count numbers are honest (cap != count).
func TestProvenanceAggregateCountOverCap(t *testing.T) {
	const n = 37
	b := memory.NewBuilder()
	for i := range n {
		b.AddFact(datalog.Fact{Name: "conn", Terms: []datalog.Constant{datalog.String("ws01"), datalog.Integer(int64(i))}})
	}
	input := b.Build()

	rs, err := syntax.ParseAll(`total(H, N) :- N = count : conn(H, X).`)
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

	fact := datalog.Fact{Name: "total", Terms: []datalog.Constant{datalog.String("ws01"), datalog.Integer(n)}}
	d, ok := prov.Explain(fact)
	if !ok {
		t.Fatalf("Explain(%v) not found", fact)
	}
	if !d.Aggregate {
		t.Fatalf("expected an aggregate Derivation, got %+v", d)
	}
	if d.GroupCount != n {
		t.Errorf("expected GroupCount %d, got %d", n, d.GroupCount)
	}
	if !d.Sampled {
		t.Errorf("expected Sampled true (cardinality exceeds the cap)")
	}
	if len(d.Body) != 10 {
		t.Fatalf("expected exactly 10 sampled solutions (witnessSampleCap), got %d", len(d.Body))
	}

	rendered := d.String()
	if !strings.Contains(rendered, fmt.Sprintf("aggregated over %d solutions (first 10 shown):", n)) {
		t.Errorf("rendered tree missing expected capped summary line:\n%s", rendered)
	}
}

// TestProvenanceAggregateExplainTreeRecursesIntoContributor confirms
// ExplainTree recurses from an aggregate head into a sampled contributor's
// own witness -- not just a base-fact leaf -- when that contributor is
// itself derived by a plain rule, matching the spec's port_scan example
// (indicator(...) with its own "rule: ..." subtree).
func TestProvenanceAggregateExplainTreeRecursesIntoContributor(t *testing.T) {
	b := memory.NewBuilder()
	b.AddFact(datalog.Fact{Name: "port_scan", Terms: []datalog.Constant{datalog.String("10.0.0.5"), datalog.String("ws01"), datalog.Integer(34)}})
	input := b.Build()

	rs, err := syntax.ParseAll(`
		indicator(H, W) :- port_scan(Src, H, PortCount), PortCount > 20, W is PortCount + 6.
		concern(H, S) :- S = sum(W) : indicator(H, W).
	`)
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

	fact := datalog.Fact{Name: "concern", Terms: []datalog.Constant{datalog.String("ws01"), datalog.Integer(40)}}
	tree, ok := prov.ExplainTree(fact)
	if !ok {
		t.Fatalf("ExplainTree(%v) not found", fact)
	}
	if !tree.Aggregate {
		t.Fatalf("expected an aggregate Derivation, got %+v", tree)
	}
	if len(tree.Body) != 1 {
		t.Fatalf("expected 1 sampled solution, got %d", len(tree.Body))
	}
	sample := tree.Body[0]
	if len(sample.Body) != 1 || sample.Body[0].Fact.Name != "indicator" {
		t.Fatalf("expected one indicator body fact, got %+v", sample.Body)
	}
	indicator := sample.Body[0]
	if indicator.Base {
		t.Fatalf("indicator should have its own witness (derived by a plain rule), not be base")
	}
	if !strings.Contains(indicator.Rule, "indicator(H, W) :-") {
		t.Errorf("unexpected indicator rule text: %q", indicator.Rule)
	}
	if len(indicator.Body) != 1 || indicator.Body[0].Fact.Name != "port_scan" {
		t.Fatalf("expected indicator's own witness to cite port_scan, got %+v", indicator.Body)
	}
	if !indicator.Body[0].Base {
		t.Errorf("port_scan should be a base fact")
	}

	rendered := tree.String()
	if !strings.Contains(rendered, "indicator(") || !strings.Contains(rendered, "port_scan(") {
		t.Errorf("rendered tree missing expected nested content:\n%s", rendered)
	}
	t.Logf("rendered:\n%s", rendered)
}

// TestProvenanceAggregateDisabledNoOp confirms an aggregate-derived fact
// explains as a base fact (no witness at all) when provenance is not
// requested via WithProvenance -- the same "absence from the map is the
// leaf marker" contract plain rules already have, extended to aggregates.
func TestProvenanceAggregateDisabledNoOp(t *testing.T) {
	b := memory.NewBuilder()
	b.AddFact(datalog.Fact{Name: "indicator", Terms: []datalog.Constant{datalog.String("ws01"), datalog.Integer(10)}})
	input := b.Build()

	rs, err := syntax.ParseAll(`concern(H, S) :- S = sum(W) : indicator(H, W).`)
	if err != nil {
		t.Fatal(err)
	}
	// No WithProvenance option -- disabled path.
	tr, err := seminaive.New().Compile(rs)
	if err != nil {
		t.Fatal(err)
	}
	output, err := tr.Transform(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for row := range output.Facts("concern", 2) {
		if row[0].(datalog.String) == "ws01" && int64(row[1].(datalog.Integer)) == 10 {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected concern(\"ws01\", 10) in output")
	}
}

// TestProvenanceAggregateDeterministicAcrossTransforms confirms two
// separate Transform runs of the same Engine over the same input produce
// identical aggregate witnesses -- same cardinality, same sampled tuples --
// matching the spec's "deterministic given fixed iteration order" claim.
func TestProvenanceAggregateDeterministicAcrossTransforms(t *testing.T) {
	b := memory.NewBuilder()
	for i := 0; i < 25; i++ {
		b.AddFact(datalog.Fact{Name: "indicator", Terms: []datalog.Constant{datalog.String("ws01"), datalog.Integer(int64(i))}})
	}
	input := b.Build()

	rs, err := syntax.ParseAll(`concern(H, S) :- S = sum(W) : indicator(H, W).`)
	if err != nil {
		t.Fatal(err)
	}

	runOnce := func() (int, []int64) {
		prov := seminaive.NewProvenance()
		tr, err := seminaive.New(seminaive.WithProvenance(prov)).Compile(rs)
		if err != nil {
			t.Fatal(err)
		}
		output, err := tr.Transform(context.Background(), input)
		if err != nil {
			t.Fatal(err)
		}
		var head datalog.Fact
		for row := range output.Facts("concern", 2) {
			head = datalog.Fact{Name: "concern", Terms: row}
		}
		d, ok := prov.Explain(head)
		if !ok {
			t.Fatalf("Explain(%v) not found", head)
		}
		var weights []int64
		for _, sample := range d.Body {
			weights = append(weights, int64(sample.Body[0].Fact.Terms[1].(datalog.Integer)))
		}
		return d.GroupCount, weights
	}

	count1, weights1 := runOnce()
	count2, weights2 := runOnce()

	if count1 != count2 {
		t.Errorf("group count differs across runs: %d vs %d", count1, count2)
	}
	if len(weights1) != len(weights2) {
		t.Fatalf("sample size differs across runs: %d vs %d", len(weights1), len(weights2))
	}
	for i := range weights1 {
		if weights1[i] != weights2[i] {
			t.Errorf("sample %d differs across runs: %d vs %d", i, weights1[i], weights2[i])
		}
	}
}
