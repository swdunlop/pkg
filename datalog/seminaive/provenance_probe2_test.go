package seminaive_test

import (
	"context"
	"strings"
	"testing"

	"swdunlop.dev/pkg/datalog"
	"swdunlop.dev/pkg/datalog/memory"
	"swdunlop.dev/pkg/datalog/seminaive"
	"swdunlop.dev/pkg/datalog/syntax"
)

// TestProbeDetailSubFidelity checks that when many bindings of the same rule
// fire, each head's witness detail line reflects THAT head's binding, not a
// later one clobbered by backtracking. A shared reordered sub reused across
// solutions would leak the wrong value into the detail.
func TestProbeDetailSubFidelity(t *testing.T) {
	b := memory.NewBuilder()
	// Several hosts each with a distinct port count over threshold; each
	// concern's detail should show its own N.
	b.AddFact(fact("port_count", str("ws01"), i(21)))
	b.AddFact(fact("port_count", str("ws02"), i(34)))
	b.AddFact(fact("port_count", str("ws03"), i(99)))
	input := b.Build()

	rs, err := syntax.ParseAll(`concern(H, N) :- port_count(H, N), N > 20.`)
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

	cases := map[string]string{
		"ws01": "21",
		"ws02": "34",
		"ws03": "99",
	}
	for host, n := range cases {
		var head datalog.Fact
		out, _ := tr.Transform(context.Background(), input)
		for row := range out.Facts("concern", 2) {
			if row[0].(datalog.String) == datalog.String(host) {
				head = datalog.Fact{Name: "concern", Terms: row}
			}
		}
		d, ok := prov.Explain(head)
		if !ok {
			t.Fatalf("Explain concern(%s) not found", host)
		}
		// Detail should contain "N > 20" ground, i.e. "<n> > 20".
		joined := strings.Join(d.Detail, " | ")
		if !strings.Contains(joined, n+" > 20") {
			t.Errorf("host %s: detail should show %q > 20, got detail=%v", host, n, d.Detail)
		}
		// And the body fact port_count must show the same N.
		if len(d.Body) != 1 {
			t.Fatalf("host %s: expected 1 body fact, got %d", host, len(d.Body))
		}
		bn := int64(d.Body[0].Fact.Terms[1].(datalog.Integer))
		if bn != int64(head.Terms[1].(datalog.Integer)) {
			t.Errorf("host %s: body port_count N=%d disagrees with head N=%v", host, bn, head.Terms[1])
		}
	}
}

// TestExplainRejectsNonexistentFact is the regression for a found defect:
// Explain must NOT report a fact that was never produced as a base-fact leaf.
// factKey can mint a valid interned key for any fact whose predicate and
// constants happen to be interned for unrelated reasons (here, concern is a
// head predicate and 34 appears in port_count), so without a produced-set
// membership check Explain returned ok=true/Base=true for concern(34) -- a
// fact that does not exist in the output at all. That is a provenance lie:
// the worst defect class for this pipeline. Explain/ExplainTree must report
// such a fact as not-found (ok=false).
func TestExplainRejectsNonexistentFact(t *testing.T) {
	b := memory.NewBuilder()
	b.AddFact(fact("port_count", str("ws01"), i(34)))
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

	// concern(34): predicate "concern" and constant 34 are both interned
	// (concern is a head predicate; 34 appears in port_count) but the fact
	// concern(34) was never produced.
	nonexistent := fact("concern", i(34))
	if d, ok := prov.Explain(nonexistent); ok {
		t.Errorf("Explain(concern(34)) must be not-found; got ok=true Base=%v -- provenance lie", d.Base)
	}
	if _, ok := prov.ExplainTree(nonexistent); ok {
		t.Errorf("ExplainTree(concern(34)) must be not-found; got ok=true -- provenance lie")
	}

	// The genuinely-produced facts still resolve.
	if _, ok := prov.Explain(fact("concern", str("ws01"))); !ok {
		t.Errorf("Explain(concern(\"ws01\")) should resolve for a produced fact")
	}
	if _, ok := prov.Explain(fact("port_count", str("ws01"), i(34))); !ok {
		t.Errorf("Explain of a produced base fact should resolve (ok=true, Base)")
	}
}
