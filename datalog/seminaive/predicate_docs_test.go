package seminaive_test

import (
	"context"
	"testing"

	"swdunlop.dev/pkg/datalog"
	"swdunlop.dev/pkg/datalog/memory"
	"swdunlop.dev/pkg/datalog/seminaive"
	"swdunlop.dev/pkg/datalog/syntax"
)

// compileAndTransform is a small helper for the predicate-docs tests below:
// parse src, compile it with no explicit declarations (unless the caller
// wires them in separately), and run Transform against an empty database so
// Declarations() reflects the transformer's own assembly plus whatever the
// input carried.
func compileAndTransform(t *testing.T, eng *seminaive.Engine, src string, input datalog.Database) datalog.Database {
	t.Helper()
	rs, err := syntax.ParseAll(src)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	tr, err := eng.Compile(rs)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	out, err := tr.Transform(context.Background(), input)
	if err != nil {
		t.Fatalf("transform: %v", err)
	}
	return out
}

func declFor(t *testing.T, db datalog.Database, name string, arity int) (datalog.Declaration, bool) {
	t.Helper()
	for d := range db.Declarations() {
		if d.Name == name && len(d.Terms) == arity {
			return d, true
		}
	}
	return datalog.Declaration{}, false
}

func termNames(d datalog.Declaration) []string {
	names := make([]string, len(d.Terms))
	for i, td := range d.Terms {
		names[i] = td.Name
	}
	return names
}

// TestAssembledTermNamesAgreement pins the base case: every rule for a head
// uses the same variable name at each position, so that name (lower-cased)
// becomes the term name.
func TestAssembledTermNamesAgreement(t *testing.T) {
	eng := seminaive.New()
	out := compileAndTransform(t, eng, `
grandparent(X, Z) :- parent(X, Y), parent(Y, Z).
`, memory.NewBuilder().Build())

	d, ok := declFor(t, out, "grandparent", 2)
	if !ok {
		t.Fatal("expected an assembled grandparent/2 declaration")
	}
	if !d.DocOnly {
		t.Errorf("expected DocOnly, got %+v", d)
	}
	got := termNames(d)
	want := []string{"x", "z"}
	if got[0] != want[0] || got[1] != want[1] {
		t.Errorf("term names = %v, want %v", got, want)
	}
}

// TestAssembledTermNamesConflict pins the conflict case from the spec: two
// rules for the same head disagree on the variable name at a position, so
// that position stays unnamed. Non-conflicting positions still get named.
func TestAssembledTermNamesConflict(t *testing.T) {
	eng := seminaive.New()
	out := compileAndTransform(t, eng, `
related(A, B) :- parent(A, B).
related(A, C) :- sibling(A, C).
`, memory.NewBuilder().Build())

	d, ok := declFor(t, out, "related", 2)
	if !ok {
		t.Fatal("expected an assembled related/2 declaration")
	}
	got := termNames(d)
	if got[0] != "a" {
		t.Errorf("position 0 should agree on 'a', got %q", got[0])
	}
	if got[1] != "" {
		t.Errorf("position 1 conflicts (b vs c) and should stay unnamed, got %q", got[1])
	}
}

// TestAssembledTermNamesCaseSensitiveConflict pins that agreement is
// checked case-sensitively: Src and SRC are DISTINCT variables in the
// language, so two rules using them at the same position genuinely disagree
// and the position must stay unnamed -- not silently merge to one
// lower-cased "src". Lower-casing is a display convention applied only to an
// already-agreed name, never the agreement key.
func TestAssembledTermNamesCaseSensitiveConflict(t *testing.T) {
	eng := seminaive.New()
	out := compileAndTransform(t, eng, `
p(Src, Dst) :- a(Src, Dst).
p(SRC, Dst) :- b(SRC, Dst).
`, memory.NewBuilder().Build())

	d, ok := declFor(t, out, "p", 2)
	if !ok {
		t.Fatal("expected an assembled p/2 declaration")
	}
	got := termNames(d)
	if got[0] != "" {
		t.Errorf("position 0 disagrees (Src vs SRC, distinct variables) and must stay unnamed, got %q", got[0])
	}
	if got[1] != "dst" {
		t.Errorf("position 1 agrees on Dst and should be named 'dst', got %q", got[1])
	}
}

// TestAssembledUseConcatenatesRuleDocsInOrder pins the Use-assembly rule:
// documented rules for the same head concatenate their docs, in rule order,
// separated by a blank line.
func TestAssembledUseConcatenatesRuleDocsInOrder(t *testing.T) {
	eng := seminaive.New()
	out := compileAndTransform(t, eng, `
%% First: direct parent-of-parent.
grandparent(X, Z) :- parent(X, Y), parent(Y, Z).
%% Second: adopted grandparent via guardianship.
grandparent(X, Z) :- guardian(X, Y), parent(Y, Z).
`, memory.NewBuilder().Build())

	d, ok := declFor(t, out, "grandparent", 2)
	if !ok {
		t.Fatal("expected an assembled grandparent/2 declaration")
	}
	want := "First: direct parent-of-parent.\n\nSecond: adopted grandparent via guardianship."
	if d.Use != want {
		t.Errorf("Use = %q, want %q", d.Use, want)
	}
}

// TestAssembledUseSkipsUndocumentedRules pins that a mix of documented and
// undocumented rules for the same head contributes only the documented
// rules' text to Use, still in rule order.
func TestAssembledUseSkipsUndocumentedRules(t *testing.T) {
	eng := seminaive.New()
	out := compileAndTransform(t, eng, `
related(A, B) :- parent(A, B).
%% Siblings count as related too.
related(A, B) :- sibling(A, B).
`, memory.NewBuilder().Build())

	d, ok := declFor(t, out, "related", 2)
	if !ok {
		t.Fatal("expected an assembled related/2 declaration")
	}
	want := "Siblings count as related too."
	if d.Use != want {
		t.Errorf("Use = %q, want %q", d.Use, want)
	}
}

// TestAssembledUseEmptyWhenAllRulesUndocumented pins that a head whose
// rules are all undocumented gets term names only -- an empty Use, not a
// placeholder.
func TestAssembledUseEmptyWhenAllRulesUndocumented(t *testing.T) {
	eng := seminaive.New()
	out := compileAndTransform(t, eng, `
grandparent(X, Z) :- parent(X, Y), parent(Y, Z).
`, memory.NewBuilder().Build())

	d, ok := declFor(t, out, "grandparent", 2)
	if !ok {
		t.Fatal("expected an assembled grandparent/2 declaration")
	}
	if d.Use != "" {
		t.Errorf("Use = %q, want empty", d.Use)
	}
	if got := termNames(d); got[0] != "x" || got[1] != "z" {
		t.Errorf("term names = %v, want [x z]", got)
	}
}

// TestAssembledMultiArityHeadsIndependent pins that two rules deriving the
// same predicate name at different arities are assembled independently --
// term-name agreement/conflict at arity 2 must not affect arity 3, and vice
// versa.
func TestAssembledMultiArityHeadsIndependent(t *testing.T) {
	eng := seminaive.New()
	out := compileAndTransform(t, eng, `
%% Two-place form.
p(X, Y) :- parent(X, Y).
%% Three-place form.
p(X, Y, Z) :- parent(X, Y), parent(Y, Z).
`, memory.NewBuilder().Build())

	d2, ok := declFor(t, out, "p", 2)
	if !ok {
		t.Fatal("expected an assembled p/2 declaration")
	}
	if got := termNames(d2); got[0] != "x" || got[1] != "y" {
		t.Errorf("p/2 term names = %v, want [x y]", got)
	}
	if d2.Use != "Two-place form." {
		t.Errorf("p/2 Use = %q, want %q", d2.Use, "Two-place form.")
	}

	d3, ok := declFor(t, out, "p", 3)
	if !ok {
		t.Fatal("expected an assembled p/3 declaration")
	}
	if got := termNames(d3); got[0] != "x" || got[1] != "y" || got[2] != "z" {
		t.Errorf("p/3 term names = %v, want [x y z]", got)
	}
	if d3.Use != "Three-place form." {
		t.Errorf("p/3 Use = %q, want %q", d3.Use, "Three-place form.")
	}
}

// TestAssembledExplicitDeclarationWins pins that an explicit jsonfacts-style
// declaration for a name outranks assembly outright -- even at an arity the
// explicit declaration doesn't cover, matching the pre-existing (name, not
// name+arity) precedence the bare-marker bookkeeping already had.
func TestAssembledExplicitDeclarationWins(t *testing.T) {
	b := memory.NewBuilder()
	b.AddDeclaration(datalog.Declaration{
		Name: "grandparent",
		Use:  "Explicit operator-authored doc.",
		Terms: []datalog.TermDeclaration{
			{Name: "ancestor"}, {Name: "descendant"},
		},
	})
	input := b.Build()

	eng := seminaive.New(seminaive.WithDeclarations(sliceDecls(input)))
	out := compileAndTransform(t, eng, `
%% This doc must never appear -- the explicit declaration wins.
grandparent(X, Z) :- parent(X, Y), parent(Y, Z).
`, input)

	d, ok := declFor(t, out, "grandparent", 2)
	if !ok {
		t.Fatal("expected grandparent/2 declaration to survive")
	}
	if d.DocOnly {
		t.Errorf("expected the explicit (non-DocOnly) declaration to win, got DocOnly: %+v", d)
	}
	if d.Use != "Explicit operator-authored doc." {
		t.Errorf("Use = %q, want the explicit declaration's Use", d.Use)
	}
	got := termNames(d)
	if got[0] != "ancestor" || got[1] != "descendant" {
		t.Errorf("term names = %v, want explicit [ancestor descendant]", got)
	}
}

// sliceDecls collects a Database's declarations into a slice, for feeding
// into seminaive.WithDeclarations the way a caller assembling a multi-stage
// pipeline would.
func sliceDecls(db datalog.Database) []datalog.Declaration {
	var decls []datalog.Declaration
	for d := range db.Declarations() {
		decls = append(decls, d)
	}
	return decls
}

// TestAssembledAggregateRuleHeadsParticipate pins that aggregate-rule heads
// are assembled the same way plain-rule heads are: the head's variables
// (including the aggregate result variable) name their positions, and the
// aggregate rule's Doc contributes to Use.
func TestAssembledAggregateRuleHeadsParticipate(t *testing.T) {
	eng := seminaive.New()
	out := compileAndTransform(t, eng, `
%% PortCount is the number of distinct destination ports.
port_scan(Src, PortCount) :- PortCount = count : conn(Src, Port).
`, memory.NewBuilder().Build())

	d, ok := declFor(t, out, "port_scan", 2)
	if !ok {
		t.Fatal("expected an assembled port_scan/2 declaration")
	}
	if got := termNames(d); got[0] != "src" || got[1] != "portcount" {
		t.Errorf("term names = %v, want [src portcount]", got)
	}
	if d.Use != "PortCount is the number of distinct destination ports." {
		t.Errorf("Use = %q", d.Use)
	}
}

// TestAssembledAggregateAndPlainRulesShareHead pins that a plain rule and an
// aggregate rule deriving the same (name, arity) head are assembled
// together: term-name agreement/conflict and doc concatenation span both
// rule kinds, not just one.
func TestAssembledAggregateAndPlainRulesShareHead(t *testing.T) {
	eng := seminaive.New()
	out := compileAndTransform(t, eng, `
%% Directly observed high-volume source.
alert(Src, Count) :- flagged(Src, Count).
%% Derived from a count aggregate over connections.
alert(Src, Count) :- Count = count : conn(Src, Dst).
`, memory.NewBuilder().Build())

	d, ok := declFor(t, out, "alert", 2)
	if !ok {
		t.Fatal("expected an assembled alert/2 declaration")
	}
	if got := termNames(d); got[0] != "src" || got[1] != "count" {
		t.Errorf("term names = %v, want [src count]", got)
	}
	want := "Directly observed high-volume source.\n\nDerived from a count aggregate over connections."
	if d.Use != want {
		t.Errorf("Use = %q, want %q", d.Use, want)
	}
}

// TestAssembledNonVariableHeadPositionStaysUnnamed pins that a constant in a
// head position (a rule that partially specializes its own head) leaves
// that position unnamed even though every other rule for the head might
// agree on a variable name there.
func TestAssembledNonVariableHeadPositionStaysUnnamed(t *testing.T) {
	eng := seminaive.New()
	out := compileAndTransform(t, eng, `
status(Src, "blocked") :- flagged(Src).
status(Src, Reason) :- override(Src, Reason).
`, memory.NewBuilder().Build())

	d, ok := declFor(t, out, "status", 2)
	if !ok {
		t.Fatal("expected an assembled status/2 declaration")
	}
	got := termNames(d)
	if got[0] != "src" {
		t.Errorf("position 0 should agree on 'src', got %q", got[0])
	}
	if got[1] != "" {
		t.Errorf("position 1 holds a constant in one rule and should stay unnamed, got %q", got[1])
	}
}

// TestAssembledTermNamesLowerCased pins the lower-casing rule: head
// variables are conventionally upper-camel in .dl source, but the
// jsonfacts term-name convention is lower-case, so assembly must fold case.
func TestAssembledTermNamesLowerCased(t *testing.T) {
	eng := seminaive.New()
	out := compileAndTransform(t, eng, `
p(SrcAddr, DstPort) :- conn(SrcAddr, DstPort).
`, memory.NewBuilder().Build())

	d, ok := declFor(t, out, "p", 2)
	if !ok {
		t.Fatal("expected an assembled p/2 declaration")
	}
	got := termNames(d)
	if got[0] != "srcaddr" || got[1] != "dstport" {
		t.Errorf("term names = %v, want [srcaddr dstport] (lower-cased)", got)
	}
}

// TestAssembledDeclarationsFlowsThroughDeclarationsIterator is a
// belt-and-suspenders check that assembled declarations reach
// Database.Declarations() the same way the pre-existing bare-marker
// declarations did (see TestStageTwoAcceptsStageOneDocOnlyRuleHeadDeclarations
// in seminaive_test.go), so downstream consumers (list_predicates, the
// encoder, a second compile stage) keep working unmodified.
func TestAssembledDeclarationsFlowsThroughDeclarationsIterator(t *testing.T) {
	eng := seminaive.New()
	rs, err := syntax.ParseAll(`
%% X begat Z, transitively.
grandparent(X, Z) :- parent(X, Y), parent(Y, Z).
`)
	if err != nil {
		t.Fatal(err)
	}
	tr, err := eng.Compile(rs)
	if err != nil {
		t.Fatal(err)
	}

	// Before Transform is ever called, Declarations() (the Transformer
	// interface method, not the output Database) must already report the
	// assembled declaration -- this is what a caller previews before
	// running the pipeline.
	found := false
	for d := range tr.Declarations(context.Background(), memory.NewBuilder().Build()) {
		if d.Name == "grandparent" && len(d.Terms) == 2 {
			found = true
			if d.Use != "X begat Z, transitively." {
				t.Errorf("preview Use = %q", d.Use)
			}
		}
	}
	if !found {
		t.Fatal("expected Transformer.Declarations to preview the assembled grandparent/2 declaration")
	}

	out, err := tr.Transform(context.Background(), memory.NewBuilder().Build())
	if err != nil {
		t.Fatal(err)
	}
	d, ok := declFor(t, out, "grandparent", 2)
	if !ok {
		t.Fatal("expected assembled grandparent/2 declaration in output Declarations()")
	}
	if got := termNames(d); got[0] != "x" || got[1] != "z" {
		t.Errorf("term names = %v, want [x z]", got)
	}
}

// TestAssembledMultiArityPreviewViaDeclarations pins the multi-arity
// suppression invariant on the Transformer.Declarations() PREVIEW path
// specifically -- the code is duplicated between Declarations() and
// Transform(), and TestAssembledMultiArityHeadsIndependent only exercises
// the Transform() path. Two rule-derived arities of the same name (p/1 and
// p/2, both only rule-derived) must both survive the preview; if the
// preview loop ever re-marks `seen` for an assembled entry, the second
// arity vanishes here.
func TestAssembledMultiArityPreviewViaDeclarations(t *testing.T) {
	eng := seminaive.New()
	rs, err := syntax.ParseAll(`
p(X) :- unary(X).
p(X, Y) :- binary(X, Y).
`)
	if err != nil {
		t.Fatal(err)
	}
	tr, err := eng.Compile(rs)
	if err != nil {
		t.Fatal(err)
	}

	arities := map[int]bool{}
	for d := range tr.Declarations(context.Background(), memory.NewBuilder().Build()) {
		if d.Name == "p" {
			arities[len(d.Terms)] = true
		}
	}
	if !arities[1] || !arities[2] {
		t.Fatalf("both p/1 and p/2 must be previewed via Declarations(); got arities %v", arities)
	}
}
