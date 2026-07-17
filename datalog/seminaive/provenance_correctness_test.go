package seminaive_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"swdunlop.dev/pkg/datalog"
	"swdunlop.dev/pkg/datalog/memory"
	"swdunlop.dev/pkg/datalog/seminaive"
	"swdunlop.dev/pkg/datalog/syntax"
)

// --- shared helpers -------------------------------------------------------

// allFacts walks every predicate/arity a database advertises and returns
// every fact it holds -- the exhaustive enumeration the honesty checker and
// the replay property both need, since neither can be handed just the
// "interesting" derived facts by the test author (the whole point is to
// catch a witness that lies about a fact the author didn't think to check).
func allFacts(db datalog.Database) []datalog.Fact {
	var out []datalog.Fact
	for pred, arity := range db.Predicates() {
		for row := range db.Facts(pred, arity) {
			terms := make([]datalog.Constant, len(row))
			copy(terms, row)
			out = append(out, datalog.Fact{Name: pred, Terms: terms})
		}
	}
	return out
}

// factKey renders a fact as a comparable string key, used by the honesty
// checker to confirm a witness's cited body fact is genuinely present in the
// output database (a set of interned keys isn't available to the test from
// outside the seminaive package, so this is the black-box equivalent: same
// predicate, same terms, rendered the same way datalog.Constant.String
// already canonicalizes literals).
func factKey(f datalog.Fact) string {
	var buf strings.Builder
	buf.WriteString(f.Name)
	buf.WriteByte('/')
	for _, t := range f.Terms {
		buf.WriteString(t.String())
		buf.WriteByte(',')
	}
	return buf.String()
}

// assertWitnessesHonest is the centerpiece checker required by the feature
// spec's step 4: for every derived fact in db's output, Explain must
// succeed, and every body fact a witness cites (recursively, for aggregate
// samples and their own body facts too) must be a genuine fact of db --
// "a witness citing a nonexistent fact is a lie" is cheap to check
// exhaustively, so this checks every fact, not a sample.
func assertWitnessesHonest(t *testing.T, prov *seminaive.Provenance, db datalog.Database) {
	t.Helper()
	present := map[string]bool{}
	for _, f := range allFacts(db) {
		present[factKey(f)] = true
	}
	for _, f := range allFacts(db) {
		d, ok := prov.Explain(f)
		if !ok {
			t.Errorf("Explain(%s%s) failed for a fact present in the output database", f.Name, termsStr(f.Terms))
			continue
		}
		assertDerivationHonest(t, present, f, d)
	}
}

func assertDerivationHonest(t *testing.T, present map[string]bool, f datalog.Fact, d seminaive.Derivation) {
	t.Helper()
	if d.Base {
		return // leaf: no body facts to check.
	}
	if d.Aggregate {
		for _, sample := range d.Body {
			for _, bf := range sample.Body {
				if !present[factKey(bf.Fact)] {
					t.Errorf("witness for %s%s (aggregate) cites nonexistent body fact %s%s",
						f.Name, termsStr(f.Terms), bf.Fact.Name, termsStr(bf.Fact.Terms))
					continue
				}
				// A sampled contributor may itself be derived; recurse one
				// level via Explain-shape info already present on bf (Base).
			}
		}
		return
	}
	for _, bf := range d.Body {
		if !present[factKey(bf.Fact)] {
			t.Errorf("witness for %s%s cites nonexistent body fact %s%s",
				f.Name, termsStr(f.Terms), bf.Fact.Name, termsStr(bf.Fact.Terms))
		}
	}
}

func termsStr(terms []datalog.Constant) string {
	var buf strings.Builder
	buf.WriteByte('(')
	for i, t := range terms {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(t.String())
	}
	buf.WriteByte(')')
	return buf.String()
}

// assertExplainTreeHonest walks ExplainTree for every derived fact (deeper
// than the one-step assertWitnessesHonest above) and confirms every node
// along the way -- however deep, however many times a shared subtree is
// repeated -- resolves to a fact genuinely present in db. This exercises the
// recursive walker/caps/sharing machinery, not just Explain's one-step form.
func assertExplainTreeHonest(t *testing.T, prov *seminaive.Provenance, db datalog.Database, opts ...seminaive.TreeOption) {
	t.Helper()
	present := map[string]bool{}
	for _, f := range allFacts(db) {
		present[factKey(f)] = true
	}
	for _, f := range allFacts(db) {
		tree, ok := prov.ExplainTree(f, opts...)
		if !ok {
			t.Errorf("ExplainTree(%s%s) failed for a fact present in the output database", f.Name, termsStr(f.Terms))
			continue
		}
		walkTreeHonest(t, present, tree)
	}
}

func walkTreeHonest(t *testing.T, present map[string]bool, d seminaive.Derivation) {
	t.Helper()
	if !present[factKey(d.Fact)] {
		t.Errorf("derivation tree cites nonexistent fact %s%s", d.Fact.Name, termsStr(d.Fact.Terms))
		return
	}
	if d.Base || d.Repeated || d.Truncated {
		return
	}
	for _, child := range d.Body {
		walkTreeHonest(t, present, child)
	}
}

// buildAndRun compiles rules with provenance enabled, transforms input, and
// fails the test on any error -- the common setup every case in the battery
// below shares.
func buildAndRun(t *testing.T, rules string, facts ...datalog.Fact) (datalog.Database, *seminaive.Provenance) {
	t.Helper()
	return buildAndRunWith(t, nil, rules, facts...)
}

// buildAndRunWith is buildAndRun plus extra engine options (e.g.
// seminaive.WithBuiltin("@time_diff", seminaive.TimeDiff), which is not
// registered by default -- see seminaive/doc.go's example).
func buildAndRunWith(t *testing.T, extraOpts []seminaive.Option, rules string, facts ...datalog.Fact) (datalog.Database, *seminaive.Provenance) {
	t.Helper()
	b := memory.NewBuilder()
	for _, f := range facts {
		if err := b.AddFact(f); err != nil {
			t.Fatalf("AddFact(%v): %v", f, err)
		}
	}
	input := b.Build()

	rs, err := syntax.ParseAll(rules)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	prov := seminaive.NewProvenance()
	opts := append([]seminaive.Option{seminaive.WithProvenance(prov)}, extraOpts...)
	tr, err := seminaive.New(opts...).Compile(rs)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	output, err := tr.Transform(context.Background(), input)
	if err != nil {
		t.Fatalf("transform: %v", err)
	}
	return output, prov
}

// timeDiffOpt registers @time_diff as a bind builtin ("D is @time_diff(A, B)"
// is not valid syntax -- is-expressions only take arithmetic operators over
// terms, not builtin calls; @time_diff(A, B, D) is an ordinary binding-builtin
// atom instead, same shape as the JSON builtins), shared by every case below
// that needs it.
var timeDiffOpt = []seminaive.Option{seminaive.WithBuiltin("@time_diff", seminaive.TimeDiff)}

// --- 1. Witness honesty invariant, driven over a battery of rulesets ------

// TestHonestyBattery drives assertWitnessesHonest and assertExplainTreeHonest
// exhaustively over every derived fact of a battery of rulesets covering the
// shapes the feature spec calls out: multi-stratum, negation, constraints,
// builtins, is-expressions, aggregates (sum/count/min/max), recursive rules,
// mutual recursion, head/base-fact predicate collisions, and asserted facts.
func TestHonestyBattery(t *testing.T) {
	cases := []struct {
		name  string
		rules string
		facts []datalog.Fact
	}{
		{
			name: "multi-stratum chain",
			rules: `
				grandparent(X, Z) :- parent(X, Y), parent(Y, Z).
				ancestor(X, Z) :- grandparent(X, Z).
				ancestor(X, Z) :- ancestor(X, Y), parent(Y, Z).
			`,
			facts: []datalog.Fact{
				fact("parent", str("tom"), str("bob")),
				fact("parent", str("bob"), str("ann")),
				fact("parent", str("ann"), str("sue")),
			},
		},
		{
			name: "negation",
			rules: `
				banned("bob").
				orphan(X) :- user(X), not banned(X).
			`,
			facts: []datalog.Fact{
				fact("user", str("ann")),
				fact("user", str("bob")),
			},
		},
		{
			name:  "constraint",
			rules: `concern(H) :- port_count(H, N), N > 20.`,
			facts: []datalog.Fact{
				fact("port_count", str("ws01"), i(34)),
				fact("port_count", str("ws02"), i(5)),
			},
		},
		{
			name:  "builtin time_diff",
			rules: `slow(H) :- request(H, T0, T1), @time_diff(T1, T0, D), D > 30.`,
			facts: []datalog.Fact{
				fact("request", str("ws01"), i(1700000000), i(1700000060)),
				fact("request", str("ws02"), i(1700000000), i(1700000010)),
			},
		},
		{
			name:  "is-expression",
			rules: `doubled(H, Y) :- indicator(H, X), Y is X * 2.`,
			facts: []datalog.Fact{
				fact("indicator", str("ws01"), i(21)),
			},
		},
		{
			name:  "aggregate sum",
			rules: `concern(H, S) :- S = sum(W) : indicator(H, W).`,
			facts: []datalog.Fact{
				fact("indicator", str("ws01"), i(10)),
				fact("indicator", str("ws01"), i(20)),
				fact("indicator", str("ws01"), i(30)),
			},
		},
		{
			name:  "aggregate count",
			rules: `total(H, N) :- N = count : conn(H, X).`,
			facts: []datalog.Fact{
				fact("conn", str("ws01"), i(1)),
				fact("conn", str("ws01"), i(2)),
				fact("conn", str("ws01"), i(3)),
			},
		},
		{
			name:  "aggregate min/max",
			rules: `range_lo(H, Lo) :- Lo = min(V) : sample(H, V). range_hi(H, Hi) :- Hi = max(V) : sample(H, V).`,
			facts: []datalog.Fact{
				fact("sample", str("ws01"), i(3)),
				fact("sample", str("ws01"), i(9)),
				fact("sample", str("ws01"), i(1)),
			},
		},
		{
			name: "recursive transitive closure",
			rules: `
				path(X, Y) :- edge(X, Y).
				path(X, Z) :- edge(X, Y), path(Y, Z).
			`,
			facts: []datalog.Fact{
				fact("edge", str("a"), str("b")),
				fact("edge", str("b"), str("c")),
				fact("edge", str("c"), str("d")),
			},
		},
		{
			name: "mutual recursion (even/odd)",
			rules: `
				even("z").
				even(X) :- succ(Y, X), odd(Y).
				odd(X) :- succ(Y, X), even(Y).
			`,
			facts: []datalog.Fact{
				fact("succ", str("z"), str("s1")),
				fact("succ", str("s1"), str("s2")),
				fact("succ", str("s2"), str("s3")),
				fact("succ", str("s3"), str("s4")),
			},
		},
		{
			name: "head collides with base fact predicate",
			rules: `
				user("derived_from_rule").
				user(X) :- alias(X, Y), user(Y).
			`,
			facts: []datalog.Fact{
				fact("user", str("base_user")),
				fact("alias", str("nickname"), str("base_user")),
			},
		},
		{
			name:  "asserted facts directly in ruleset",
			rules: `likes("ann", "pizza"). likes("bob", "sushi").`,
			facts: nil,
		},
		{
			name:  "double-occurrence self-join (distinct facts)",
			rules: `path2(X, Z) :- edge(X, Y), edge(Y, Z).`,
			facts: []datalog.Fact{
				fact("edge", str("a"), str("b")),
				fact("edge", str("b"), str("c")),
			},
		},
		{
			name:  "double-occurrence self-join (same fact grounds twice)",
			rules: `path2(X, Z) :- edge(X, Y), edge(Y, Z).`,
			facts: []datalog.Fact{
				fact("edge", str("a"), str("a")),
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			output, prov := buildAndRunWith(t, timeDiffOpt, tc.rules, tc.facts...)
			assertWitnessesHonest(t, prov, output)
			assertExplainTreeHonest(t, prov, output)
		})
	}
}

// --- 2. Replay property -----------------------------------------------

// TestReplayProperty covers the spec's property-check candidate: for each
// derived fact's witness, replaying its cited rule over a database
// containing only the witness's cited body facts re-derives exactly that
// head. This proves a witness's body facts are not just "present somewhere
// in the output" (assertWitnessesHonest) but "individually sufficient" -- a
// stronger, closer-to-proof property.
//
// Negation makes naive replay unsound in general (a negated atom that had no
// match in the *original*, larger database is trivially still unmatched in
// the *reduced* database, since removing facts can only make more negations
// succeed, never fewer) -- so this only exercises rules without negation,
// where replay is a sound re-derivation check by construction.
func TestReplayProperty(t *testing.T) {
	cases := []struct {
		name  string
		rules string
		facts []datalog.Fact
	}{
		{
			name: "grandparent chain",
			rules: `
				grandparent(X, Z) :- parent(X, Y), parent(Y, Z).
			`,
			facts: []datalog.Fact{
				fact("parent", str("tom"), str("bob")),
				fact("parent", str("bob"), str("ann")),
			},
		},
		{
			name:  "constraint",
			rules: `concern(H) :- port_count(H, N), N > 20.`,
			facts: []datalog.Fact{
				fact("port_count", str("ws01"), i(34)),
			},
		},
		{
			name: "recursive transitive closure",
			rules: `
				path(X, Y) :- edge(X, Y).
				path(X, Z) :- edge(X, Y), path(Y, Z).
			`,
			facts: []datalog.Fact{
				fact("edge", str("a"), str("b")),
				fact("edge", str("b"), str("c")),
				fact("edge", str("c"), str("d")),
			},
		},
		{
			name:  "double-occurrence self-join",
			rules: `path2(X, Z) :- edge(X, Y), edge(Y, Z).`,
			facts: []datalog.Fact{
				fact("edge", str("a"), str("b")),
				fact("edge", str("b"), str("c")),
			},
		},
		{
			name:  "builtin time_diff",
			rules: `slow(H) :- request(H, T0, T1), @time_diff(T1, T0, D), D > 30.`,
			facts: []datalog.Fact{
				fact("request", str("ws01"), i(1700000000), i(1700000060)),
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			output, prov := buildAndRunWith(t, timeDiffOpt, tc.rules, tc.facts...)
			for _, f := range allFacts(output) {
				d, ok := prov.Explain(f)
				if !ok || d.Base {
					continue // base facts have nothing to replay.
				}
				replayOne(t, timeDiffOpt, f, d)
			}
		})
	}
}

// replayOne replays a single derived fact's witness: build a database
// containing only the witness's cited body facts, parse just the rule text
// that produced it, run a one-rule Transform, and assert the head fact
// reappears exactly. opts carries any engine options (e.g. builtin
// registrations) the witness's own rule text depends on -- the replayed
// Engine must agree with the one that produced the original witness.
func replayOne(t *testing.T, opts []seminaive.Option, f datalog.Fact, d seminaive.Derivation) {
	t.Helper()
	if d.Rule == "" {
		t.Errorf("witness for %s%s has empty rule text", f.Name, termsStr(f.Terms))
		return
	}
	b := memory.NewBuilder()
	seen := map[string]bool{}
	for _, bf := range d.Body {
		k := factKey(bf.Fact)
		if seen[k] {
			continue // double-occurrence self-join: same fact cited twice, add once.
		}
		seen[k] = true
		if err := b.AddFact(bf.Fact); err != nil {
			t.Fatalf("AddFact(%v): %v", bf.Fact, err)
		}
	}
	reduced := b.Build()

	rs, err := syntax.ParseAll(d.Rule)
	if err != nil {
		t.Fatalf("parsing witness rule text %q: %v", d.Rule, err)
	}
	tr, err := seminaive.New(opts...).Compile(rs)
	if err != nil {
		t.Fatalf("compiling witness rule text %q: %v", d.Rule, err)
	}
	out, err := tr.Transform(context.Background(), reduced)
	if err != nil {
		t.Fatalf("replaying witness rule %q over its cited body facts: %v", d.Rule, err)
	}

	found := false
	for row := range out.Facts(f.Name, len(f.Terms)) {
		if termsEqual(row, f.Terms) {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("replaying rule %q over cited body facts %v did not re-derive %s%s",
			d.Rule, d.Body, f.Name, termsStr(f.Terms))
	}
}

func termsEqual(a, b []datalog.Constant) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].String() != b[i].String() {
			return false
		}
	}
	return true
}

// TestReplayPropertyAggregate covers the aggregate variant the spec calls
// out separately: sampled contributors are genuinely real facts (already
// covered by the honesty checker) and, for small groups where every solution
// was sampled (Sampled == false), the sample count independently recounts to
// the group's own true cardinality -- an aggregate-shaped analogue of replay
// since there is no single "cited rule + body" to naively re-run for a
// streaming accumulator.
func TestReplayPropertyAggregate(t *testing.T) {
	output, prov := buildAndRun(t, `total(H, N) :- N = count : conn(H, X).`,
		fact("conn", str("ws01"), i(1)),
		fact("conn", str("ws01"), i(2)),
		fact("conn", str("ws01"), i(3)),
	)
	f := fact("total", str("ws01"), i(3))
	d, ok := prov.Explain(f)
	if !ok {
		t.Fatalf("Explain(%v) not found", f)
	}
	if !d.Aggregate {
		t.Fatalf("expected an aggregate derivation")
	}
	if d.Sampled {
		t.Fatalf("group of 3 is under witnessSampleCap; expected every solution sampled")
	}
	if len(d.Body) != d.GroupCount {
		t.Errorf("sample count %d does not match independently-known group count %d", len(d.Body), d.GroupCount)
	}
	assertWitnessesHonest(t, prov, output)
}

// --- 3. Determinism --------------------------------------------------

// TestDeterminismAcrossRepeatedTransforms confirms repeated Transforms of
// the same ruleset+database produce identical witnesses -- same rule index
// (via rendered rule text, the only externally observable proxy), same body
// keys, same detail strings -- across at least 3 runs. The ruleset has two
// rules that can derive the same head (via different paths reaching the same
// grandparent pair is awkward to construct without changing cardinality, so
// this instead uses two distinct rules for the *same* predicate where one
// consistently wins under fixed rule/iteration order) to also pin down
// first-witness-bias stability.
func TestDeterminismAcrossRepeatedTransforms(t *testing.T) {
	b := memory.NewBuilder()
	b.AddFact(fact("parent", str("tom"), str("bob")))
	b.AddFact(fact("parent", str("bob"), str("ann")))
	b.AddFact(fact("direct", str("tom"), str("ann")))
	input := b.Build()

	// Two rules can each derive related("tom","ann"): first by the direct
	// fact, second transitively -- first-witness wins deterministically since
	// rule order and iteration order are both fixed by the ruleset text.
	const rules = `
		related(X, Y) :- direct(X, Y).
		related(X, Z) :- parent(X, Y), parent(Y, Z).
	`
	rs, err := syntax.ParseAll(rules)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	type snapshot struct {
		rule   string
		body   []string
		detail []string
	}
	runOnce := func() map[string]snapshot {
		prov := seminaive.NewProvenance()
		tr, err := seminaive.New(seminaive.WithProvenance(prov)).Compile(rs)
		if err != nil {
			t.Fatalf("compile: %v", err)
		}
		output, err := tr.Transform(context.Background(), input)
		if err != nil {
			t.Fatalf("transform: %v", err)
		}
		snap := map[string]snapshot{}
		for _, f := range allFacts(output) {
			d, ok := prov.Explain(f)
			if !ok {
				t.Fatalf("Explain(%v) not found", f)
			}
			var bodyKeys []string
			for _, bf := range d.Body {
				bodyKeys = append(bodyKeys, factKey(bf.Fact))
			}
			snap[factKey(f)] = snapshot{rule: d.Rule, body: bodyKeys, detail: append([]string(nil), d.Detail...)}
		}
		return snap
	}

	const runs = 3
	var snapshots []map[string]snapshot
	for i := 0; i < runs; i++ {
		snapshots = append(snapshots, runOnce())
	}

	base := snapshots[0]
	for i := 1; i < runs; i++ {
		cur := snapshots[i]
		if len(base) != len(cur) {
			t.Fatalf("run %d: fact count differs from run 0: %d vs %d", i, len(cur), len(base))
		}
		for k, want := range base {
			got, ok := cur[k]
			if !ok {
				t.Fatalf("run %d: fact %s missing (present in run 0)", i, k)
			}
			if got.rule != want.rule {
				t.Errorf("run %d: fact %s rule text differs:\n  run0: %q\n  run%d: %q", i, k, want.rule, i, got.rule)
			}
			if !sliceEqual(got.body, want.body) {
				t.Errorf("run %d: fact %s body keys differ:\n  run0: %v\n  run%d: %v", i, k, want.body, i, got.body)
			}
			if !sliceEqual(got.detail, want.detail) {
				t.Errorf("run %d: fact %s detail differs:\n  run0: %v\n  run%d: %v", i, k, want.detail, i, got.detail)
			}
		}
	}

	// Pin down which rule won for related("tom","ann") specifically, and that
	// it's stable -- first-witness bias must be deterministic, not merely
	// "some rule, consistently."
	key := factKey(fact("related", str("tom"), str("ann")))
	snap, ok := base[key]
	if !ok {
		t.Fatalf("expected related(\"tom\", \"ann\") to be derived")
	}
	if !strings.Contains(snap.rule, "related(X, Y) :- direct(X, Y)") {
		t.Errorf("expected the direct-fact rule to win first-witness, got rule: %q", snap.rule)
	}
}

func sliceEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestDeterminismAggregateAcrossRepeatedTransforms mirrors the plain-rule
// determinism check for an aggregate witness with a group over the sample
// cap, confirming the capped sample itself (not just cardinality) is stable
// across runs -- provenance_aggregate_test.go already covers the sum case;
// this adds a from-scratch multi-run loop (3 runs, matching this file's
// "at least 3" requirement) over a different aggregate kind (min) for
// independent coverage.
func TestDeterminismAggregateAcrossRepeatedTransforms(t *testing.T) {
	b := memory.NewBuilder()
	for i := 0; i < 15; i++ {
		b.AddFact(fact("sample", str("ws01"), integer(int64(i))))
	}
	input := b.Build()

	rs, err := syntax.ParseAll(`lo(H, V) :- V = min(X) : sample(H, X).`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	runOnce := func() []int64 {
		prov := seminaive.NewProvenance()
		tr, err := seminaive.New(seminaive.WithProvenance(prov)).Compile(rs)
		if err != nil {
			t.Fatalf("compile: %v", err)
		}
		output, err := tr.Transform(context.Background(), input)
		if err != nil {
			t.Fatalf("transform: %v", err)
		}
		var head datalog.Fact
		for row := range output.Facts("lo", 2) {
			head = datalog.Fact{Name: "lo", Terms: row}
		}
		d, ok := prov.Explain(head)
		if !ok {
			t.Fatalf("Explain(%v) not found", head)
		}
		var vals []int64
		for _, s := range d.Body {
			vals = append(vals, int64(s.Body[0].Fact.Terms[1].(datalog.Integer)))
		}
		return vals
	}

	base := runOnce()
	for i := 1; i < 3; i++ {
		got := runOnce()
		if len(got) != len(base) {
			t.Fatalf("run %d: sample length differs: %d vs %d", i, len(got), len(base))
		}
		for j := range got {
			if got[j] != base[j] {
				t.Errorf("run %d: sample[%d] differs: %d vs %d", i, j, got[j], base[j])
			}
		}
	}
}

// --- 4. Double-occurrence grounding ------------------------------------

// TestDoubleOccurrenceGroundingDistinct confirms a body atom appearing twice
// with different variables grounds to two distinct cited facts.
func TestDoubleOccurrenceGroundingDistinct(t *testing.T) {
	output, prov := buildAndRun(t, `path2(X, Z) :- edge(X, Y), edge(Y, Z).`,
		fact("edge", str("a"), str("b")),
		fact("edge", str("b"), str("c")),
	)
	d, ok := prov.Explain(fact("path2", str("a"), str("c")))
	if !ok {
		t.Fatalf("Explain not found")
	}
	if len(d.Body) != 2 {
		t.Fatalf("expected 2 body facts, got %d: %v", len(d.Body), d.Body)
	}
	if factKey(d.Body[0].Fact) == factKey(d.Body[1].Fact) {
		t.Errorf("expected two distinct edge facts, got the same fact twice: %+v", d.Body)
	}
	assertWitnessesHonest(t, prov, output)
}

// TestDoubleOccurrenceGroundingSelfJoin confirms the self-join case where
// both occurrences of a repeated body atom ground to the SAME fact
// (edge(a,a) deriving path2(a,a) via X=Y=Z=a) -- the witness must still
// resolve (whether the implementation dedupes to one body entry or repeats
// the same key twice, either is acceptable per the spec; this asserts
// whichever shape it produces resolves honestly).
func TestDoubleOccurrenceGroundingSelfJoin(t *testing.T) {
	output, prov := buildAndRun(t, `path2(X, Z) :- edge(X, Y), edge(Y, Z).`,
		fact("edge", str("a"), str("a")),
	)
	d, ok := prov.Explain(fact("path2", str("a"), str("a")))
	if !ok {
		t.Fatalf("Explain not found")
	}
	if len(d.Body) == 0 {
		t.Fatalf("expected at least one body fact citing edge(a,a), got none")
	}
	want := factKey(fact("edge", str("a"), str("a")))
	for _, bf := range d.Body {
		if factKey(bf.Fact) != want {
			t.Errorf("expected every body citation to be edge(a,a), got %s", factKey(bf.Fact))
		}
		if !bf.Base {
			t.Errorf("edge(a,a) should be a base fact")
		}
	}
	assertWitnessesHonest(t, prov, output)
}

// --- 5. Discard on abort -----------------------------------------------

// TestDiscardOnAbortPreservesPriorProvenance confirms that WithFactLimit
// abort, context cancellation, and an eval error each leave a
// previously-populated Provenance (from an earlier successful Transform)
// fully intact -- the old explanations still resolve against the old dict --
// rather than merely leaving an empty Provenance the way
// TestProvenanceDiscardedOnFactLimitAbort (provenance_test.go) already
// checks starting from a fresh, never-populated Provenance.
func TestDiscardOnAbortPreservesPriorProvenance(t *testing.T) {
	priorInput := memory.NewBuilder()
	priorInput.AddFact(fact("parent", str("tom"), str("bob")))
	priorInput.AddFact(fact("parent", str("bob"), str("ann")))
	priorDB := priorInput.Build()

	priorRules := `grandparent(X, Z) :- parent(X, Y), parent(Y, Z).`
	priorRS, err := syntax.ParseAll(priorRules)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	prov := seminaive.NewProvenance()

	// Populate prov with a successful run first.
	priorTr, err := seminaive.New(seminaive.WithProvenance(prov)).Compile(priorRS)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if _, err := priorTr.Transform(context.Background(), priorDB); err != nil {
		t.Fatalf("prior transform: %v", err)
	}
	priorFact := fact("grandparent", str("tom"), str("ann"))
	priorDerivation, ok := prov.Explain(priorFact)
	if !ok || priorDerivation.Base {
		t.Fatalf("expected a resolvable non-base witness for the prior run's grandparent fact")
	}

	assertPriorIntact := func(t *testing.T, label string) {
		t.Helper()
		d, ok := prov.Explain(priorFact)
		if !ok {
			t.Fatalf("%s: prior provenance lost after abort; Explain no longer resolves %v", label, priorFact)
		}
		if d.Base != priorDerivation.Base || d.Rule != priorDerivation.Rule {
			t.Fatalf("%s: prior provenance corrupted after abort: got %+v, want %+v", label, d, priorDerivation)
		}
		if len(d.Body) != len(priorDerivation.Body) {
			t.Fatalf("%s: prior provenance body count changed after abort: got %d, want %d", label, len(d.Body), len(priorDerivation.Body))
		}
	}

	t.Run("fact limit abort", func(t *testing.T) {
		b := memory.NewBuilder()
		for i := 0; i < 50; i++ {
			b.AddFact(fact("node", integer(int64(i))))
		}
		input := b.Build()
		rs, err := syntax.ParseAll(`pair(X, Y) :- node(X), node(Y).`)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		tr, err := seminaive.New(seminaive.WithProvenance(prov), seminaive.WithFactLimit(5)).Compile(rs)
		if err != nil {
			t.Fatalf("compile: %v", err)
		}
		_, err = tr.Transform(context.Background(), input)
		var fle seminaive.FactLimitError
		if !errors.As(err, &fle) {
			t.Fatalf("expected a FactLimitError, got %v", err)
		}
		assertPriorIntact(t, "fact limit abort")
		if _, ok := prov.Explain(fact("pair", i(0), i(0))); ok {
			t.Errorf("aborted run's fact should not resolve")
		}
	})

	t.Run("context cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // cancel immediately, before Transform runs any work.
		b := memory.NewBuilder()
		b.AddFact(fact("node", integer(1)))
		input := b.Build()
		rs, err := syntax.ParseAll(`same(X) :- node(X).`)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		tr, err := seminaive.New(seminaive.WithProvenance(prov)).Compile(rs)
		if err != nil {
			t.Fatalf("compile: %v", err)
		}
		_, err = tr.Transform(ctx, input)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
		assertPriorIntact(t, "context cancellation")
	})

	t.Run("eval error", func(t *testing.T) {
		// An int64 overflow in an is-expression aborts the Transform with an
		// error (arithmeticOverflowError -> ordinary error), exercising the
		// non-FactLimit, non-cancellation error-discard path.
		b := memory.NewBuilder()
		input := b.Build()
		rs, err := syntax.ParseAll(`overflow(Y) :- Y is 9223372036854775807 + 1.`)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		tr, err := seminaive.New(seminaive.WithProvenance(prov)).Compile(rs)
		if err != nil {
			t.Fatalf("compile: %v", err)
		}
		_, err = tr.Transform(context.Background(), input)
		if err == nil {
			t.Fatalf("expected an overflow error, got nil")
		}
		assertPriorIntact(t, "eval error")
	})
}

// --- 6. Renderings -------------------------------------------------------

// TestRenderingsShapes exercises golden-ish (substring/shape) assertions for
// negation, constraint, builtin, is-expression, and aggregate detail lines.
func TestRenderingsShapes(t *testing.T) {
	t.Run("negation", func(t *testing.T) {
		_, prov := buildAndRun(t, `orphan(X) :- user(X), not banned(X).`, fact("user", str("ann")))
		d, ok := prov.Explain(fact("orphan", str("ann")))
		if !ok {
			t.Fatalf("Explain not found")
		}
		rendered := d.String()
		if !strings.Contains(rendered, "not banned(") {
			t.Errorf("rendering missing negation detail:\n%s", rendered)
		}
	})

	t.Run("constraint", func(t *testing.T) {
		_, prov := buildAndRun(t, `concern(H) :- port_count(H, N), N > 20.`, fact("port_count", str("ws01"), i(34)))
		d, ok := prov.Explain(fact("concern", str("ws01")))
		if !ok {
			t.Fatalf("Explain not found")
		}
		rendered := d.String()
		if !strings.Contains(rendered, "34") || !strings.Contains(rendered, ">") || !strings.Contains(rendered, "20") {
			t.Errorf("rendering missing constraint detail:\n%s", rendered)
		}
	})

	t.Run("builtin time_diff", func(t *testing.T) {
		_, prov := buildAndRunWith(t, timeDiffOpt, `slow(H) :- request(H, T0, T1), @time_diff(T1, T0, D), D > 30.`,
			fact("request", str("ws01"), i(1700000000), i(1700000060)))
		d, ok := prov.Explain(fact("slow", str("ws01")))
		if !ok {
			t.Fatalf("Explain not found")
		}
		rendered := d.String()
		if !strings.Contains(rendered, "time_diff") {
			t.Errorf("rendering missing builtin detail:\n%s", rendered)
		}
		if !strings.Contains(rendered, "60") {
			t.Errorf("rendering missing time_diff result value:\n%s", rendered)
		}
	})

	t.Run("is-expression", func(t *testing.T) {
		_, prov := buildAndRun(t, `doubled(H, Y) :- indicator(H, X), Y is X * 2.`, fact("indicator", str("ws01"), i(21)))
		d, ok := prov.Explain(fact("doubled", str("ws01"), i(42)))
		if !ok {
			t.Fatalf("Explain not found")
		}
		rendered := d.String()
		if !strings.Contains(rendered, "is") || !strings.Contains(rendered, "42") {
			t.Errorf("rendering missing is-expression detail:\n%s", rendered)
		}
	})

	t.Run("aggregate", func(t *testing.T) {
		_, prov := buildAndRun(t, `total(H, N) :- N = count : conn(H, X).`,
			fact("conn", str("ws01"), i(1)), fact("conn", str("ws01"), i(2)))
		d, ok := prov.Explain(fact("total", str("ws01"), i(2)))
		if !ok {
			t.Fatalf("Explain not found")
		}
		rendered := d.String()
		if !strings.Contains(rendered, "aggregated over 2 solutions (all shown):") {
			t.Errorf("rendering missing aggregate summary line:\n%s", rendered)
		}
		if !strings.Contains(rendered, "conn(") {
			t.Errorf("rendering missing sampled contributor facts:\n%s", rendered)
		}
	})
}

// TestExplainTreeMaxDepthTruncates confirms MaxDepth actually truncates a
// deep recursive derivation and marks the truncated node.
func TestExplainTreeMaxDepthTruncates(t *testing.T) {
	b := memory.NewBuilder()
	const n = 10
	for i := 0; i < n; i++ {
		b.AddFact(fact("edge", integer(int64(i)), integer(int64(i+1))))
	}
	input := b.Build()
	rs, err := syntax.ParseAll(`
		path(X, Y) :- edge(X, Y).
		path(X, Z) :- edge(X, Y), path(Y, Z).
	`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	prov := seminaive.NewProvenance()
	tr, err := seminaive.New(seminaive.WithProvenance(prov)).Compile(rs)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if _, err := tr.Transform(context.Background(), input); err != nil {
		t.Fatalf("transform: %v", err)
	}

	// path(0, n) is derived via a chain n hops deep.
	head := fact("path", integer(0), integer(int64(n)))
	tree, ok := prov.ExplainTree(head, seminaive.MaxDepth(2))
	if !ok {
		t.Fatalf("ExplainTree not found")
	}
	if !hasTruncated(tree) {
		t.Errorf("expected MaxDepth(2) to truncate somewhere in a %d-hop chain, tree:\n%s", n, tree.String())
	}

	// A generous depth resolves the whole chain without truncation.
	fullTree, ok := prov.ExplainTree(head, seminaive.MaxDepth(n+2))
	if !ok {
		t.Fatalf("ExplainTree not found")
	}
	if hasTruncated(fullTree) {
		t.Errorf("did not expect truncation with a generous MaxDepth, tree:\n%s", fullTree.String())
	}
}

// TestExplainTreeMaxNodesTruncates confirms MaxNodes actually truncates a
// wide derivation tree and marks the truncated node.
func TestExplainTreeMaxNodesTruncates(t *testing.T) {
	b := memory.NewBuilder()
	const n = 30
	for i := 0; i < n; i++ {
		b.AddFact(fact("indicator", str("ws01"), integer(int64(i))))
	}
	input := b.Build()
	rs, err := syntax.ParseAll(`concern(H, S) :- S = sum(W) : indicator(H, W).`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	prov := seminaive.NewProvenance()
	tr, err := seminaive.New(seminaive.WithProvenance(prov)).Compile(rs)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	output, err := tr.Transform(context.Background(), input)
	if err != nil {
		t.Fatalf("transform: %v", err)
	}
	var head datalog.Fact
	for row := range output.Facts("concern", 2) {
		head = datalog.Fact{Name: "concern", Terms: row}
	}
	tree, ok := prov.ExplainTree(head, seminaive.MaxNodes(3))
	if !ok {
		t.Fatalf("ExplainTree not found")
	}
	if !hasTruncated(tree) {
		t.Errorf("expected MaxNodes(3) to truncate a group of %d, tree:\n%s", n, tree.String())
	}
}

func hasTruncated(d seminaive.Derivation) bool {
	if d.Truncated {
		return true
	}
	for _, c := range d.Body {
		if hasTruncated(c) {
			return true
		}
	}
	return false
}

// TestExplainTreeRepeatedSharedSubtree confirms a fact appearing as a body
// fact of more than one parent renders fully once and as a Repeated
// back-reference thereafter. This requires the shared fact to itself be
// *derived* (have its own witness/subtree): a base fact has no subtree to
// share in the first place (Body is always empty for a base leaf, so
// re-printing "[base fact]" twice is already exactly as cheap as a Repeated
// marker) -- explainTreeLocked's hasWitness check, deliberately, returns
// before ever consulting seen/Repeated for a base fact (see provenance.go).
// So shared(1) here is itself a one-hop derivation (from root(1)), giving it
// a real subtree that ExplainTree must actually collapse on its second
// occurrence.
func TestExplainTreeRepeatedSharedSubtree(t *testing.T) {
	output, prov := buildAndRun(t, `
		shared(X) :- root(X).
		a(X) :- shared(X), tag("a").
		b(X) :- shared(X), tag("b").
		both(X) :- a(X), b(X).
	`,
		fact("root", i(1)),
		fact("tag", str("a")),
		fact("tag", str("b")),
	)
	_ = output
	tree, ok := prov.ExplainTree(fact("both", i(1)))
	if !ok {
		t.Fatalf("ExplainTree not found")
	}
	if len(tree.Body) != 2 {
		t.Fatalf("expected 2 body facts (a(1), b(1)), got %d", len(tree.Body))
	}
	// Both a(1) and b(1) cite shared(1); one occurrence across the whole tree
	// must be Repeated (the second time shared(1) is reached) since shared(1)
	// is itself derived (has a real subtree -- root(1) -- to collapse).
	var sharedOccurrences int
	var repeatedOccurrences int
	var walk func(d seminaive.Derivation)
	walk = func(d seminaive.Derivation) {
		if d.Fact.Name == "shared" {
			sharedOccurrences++
			if d.Repeated {
				repeatedOccurrences++
				if len(d.Body) != 0 {
					t.Errorf("a Repeated node must not re-expand its body, got %+v", d.Body)
				}
			} else if len(d.Body) != 1 || d.Body[0].Fact.Name != "root" {
				t.Errorf("expected shared(1)'s first (non-Repeated) occurrence to expand into root(1), got %+v", d.Body)
			}
		}
		for _, c := range d.Body {
			walk(c)
		}
	}
	walk(tree)
	if sharedOccurrences < 2 {
		t.Fatalf("expected shared(1) to appear at least twice in the tree, got %d", sharedOccurrences)
	}
	if repeatedOccurrences == 0 {
		t.Errorf("expected at least one Repeated back-reference to shared(1), tree:\n%s", tree.String())
	}
}

// --- 7. Table-driven property alternative to a fuzz target -----------------
//
// The codebase's existing Fuzz* entry points (canon_fuzz_test.go,
// syntax/fuzz_test.go) fuzz a single well-typed input shape (canonical
// encoding round-trip, parser text) against a fixed grammar/corpus.
// Provenance's correctness property spans a much larger surface -- an
// entire ruleset (multiple rules, predicates, and an input database) has to
// be *jointly* well-formed (safe, stratifiable, arity-consistent) for
// Transform to even run, so a byte-fuzzer mutating ruleset text would spend
// nearly all its budget on parse/compile-reject inputs rather than
// exercising the emit-seam witness capture this feature is about. A
// table-driven property test over a deliberately varied set of rulesets
// (distinct from TestHonestyBattery's cases, so the two suites don't just
// duplicate coverage) gets the same "many varied programs, one property,
// exhaustive check" coverage without paying that fuzzer tax -- consistent
// with the spec's "if wiring a fuzz target is disproportionate, a
// table-driven property test over ~10 varied rulesets satisfies the spirit"
// escape hatch.
func TestHonestyPropertyAcrossVariedRulesets(t *testing.T) {
	type program struct {
		name  string
		rules string
		facts []datalog.Fact
	}
	progs := []program{
		{"chain3", `p1(X,Y) :- p0(X,Y). p2(X,Z) :- p1(X,Y), p0(Y,Z).`,
			[]datalog.Fact{fact("p0", str("a"), str("b")), fact("p0", str("b"), str("c"))}},
		{"negation+aggregate", `
			excluded("x").
			kept(N) :- item(N), not excluded(N).
			total(C) :- C = count : kept(?).
		`, []datalog.Fact{fact("item", str("x")), fact("item", str("y")), fact("item", str("z"))}},
		{"builtin+constraint", `
			flagged(H) :- event(H, T0, T1), @time_diff(T1, T0, D), D > 10.
		`, []datalog.Fact{fact("event", str("h1"), i(0), i(20)), fact("event", str("h2"), i(0), i(5))}},
		{"deep recursion", `
			reach(X, Y) :- link(X, Y).
			reach(X, Z) :- link(X, Y), reach(Y, Z).
		`, chainFacts("link", 8)},
		{"mutual recursion", `
			even("z").
			even(X) :- succ(Y, X), odd(Y).
			odd(X) :- succ(Y, X), even(Y).
		`, chainFacts("succ", 6)},
		{"min/max/sum together", `
			stats1(H, Lo) :- Lo = min(V) : m(H, V).
			stats2(H, Hi) :- Hi = max(V) : m(H, V).
			stats3(H, S) :- S = sum(V) : m(H, V).
		`, []datalog.Fact{fact("m", str("h"), i(5)), fact("m", str("h"), i(1)), fact("m", str("h"), i(9))}},
		{"asserted plus derived same predicate", `
			seed(1).
			seed(X) :- prior(X), X < 5.
		`, []datalog.Fact{fact("prior", i(2)), fact("prior", i(7))}},
		{"double-occurrence wide", `tri(X,Y,Z) :- e(X,Y), e(Y,Z), e(Z,X).`,
			[]datalog.Fact{fact("e", str("a"), str("b")), fact("e", str("b"), str("c")), fact("e", str("c"), str("a"))}},
		{"is-expression chain", `
			step1(X, Y) :- src(X), Y is X + 1.
			step2(X, Z) :- step1(X, Y), Z is Y * 2.
		`, []datalog.Fact{fact("src", i(3)), fact("src", i(10))}},
		{"head collides with base", `
			node("root").
			node(X) :- child(X, Y), node(Y).
		`, []datalog.Fact{fact("node", str("leaf")), fact("child", str("mid"), str("leaf"))}},
	}

	for _, p := range progs {
		t.Run(p.name, func(t *testing.T) {
			output, prov := buildAndRunWith(t, timeDiffOpt, p.rules, p.facts...)
			assertWitnessesHonest(t, prov, output)
			assertExplainTreeHonest(t, prov, output)
		})
	}
}

// chainFacts builds a chain of n `pred(i, i+1)` facts for i in [0, n).
func chainFacts(pred string, n int) []datalog.Fact {
	facts := make([]datalog.Fact, 0, n)
	for i := 0; i < n; i++ {
		facts = append(facts, fact(pred, str(fmt.Sprintf("n%d", i)), str(fmt.Sprintf("n%d", i+1))))
	}
	return facts
}

// --- small fact-building helpers used throughout this file -----------------

func fact(name string, terms ...datalog.Constant) datalog.Fact {
	return datalog.Fact{Name: name, Terms: terms}
}

func str(s string) datalog.Constant { return datalog.String(s) }
func i(n int64) datalog.Constant    { return datalog.Integer(n) }

// integer is an alias for i, used in a few cases below where a bare "i" read
// less clearly next to other single-letter identifiers in scope.
func integer(n int64) datalog.Constant { return datalog.Integer(n) }
