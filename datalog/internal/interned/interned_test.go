package interned

import (
	"testing"
)

// mkFact builds an InternedFact for tests.
func mkFact(pred uint64, values ...uint64) InternedFact {
	f := InternedFact{Pred: pred, Arity: len(values)}
	copy(f.Values[:], values)
	return f
}

// bruteScan filters facts by MatchesBound, the reference behavior Scan
// must reproduce regardless of which index it chooses.
func bruteScan(fs InternedFactSet, pred uint64, arity int, bound *BoundSet) []InternedFact {
	var out []InternedFact
	for _, f := range fs.Get(pred, arity) {
		if MatchesBound(bound, &f) {
			out = append(out, f)
		}
	}
	return out
}

// collect materializes a ScanResult, applying MatchesBound as the
// evaluator does (an indexed scan only guarantees one column matches).
func collect(r ScanResult, bound *BoundSet) []InternedFact {
	var out []InternedFact
	for i := range r.Len() {
		f := r.Fact(i)
		if MatchesBound(bound, f) {
			out = append(out, *f)
		}
	}
	return out
}

func sameFacts(t *testing.T, got, want []InternedFact) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d facts, want %d", len(got), len(want))
	}
	index := make(map[uint64]int, len(want))
	for _, f := range want {
		index[InternedFactHash(f)]++
	}
	for _, f := range got {
		h := InternedFactHash(f)
		if index[h] == 0 {
			t.Fatalf("unexpected fact %v", f)
		}
		index[h]--
	}
}

// fillFacts adds n arity-3 facts of the shape pred(i, i%groups, 42).
func fillFacts(fs InternedFactSet, pred uint64, n int, groups uint64) {
	for i := range n {
		fs.Add(mkFact(pred, uint64(1000+i), uint64(i)%groups, 42))
	}
}

func TestScanNonZeroColumn(t *testing.T) {
	fs := NewInternedFactSet()
	const pred = 7
	fillFacts(fs, pred, 200, 10)

	for col := range 3 {
		var bound BoundSet
		switch col {
		case 0:
			bound.Set(0, 1005)
		case 1:
			bound.Set(1, 3)
		case 2:
			bound.Set(2, 42)
		}
		got := collect(fs.Scan(pred, 3, &bound), &bound)
		want := bruteScan(fs, pred, 3, &bound)
		sameFacts(t, got, want)
	}
}

func TestScanMultipleBoundColumns(t *testing.T) {
	fs := NewInternedFactSet()
	const pred = 7
	fillFacts(fs, pred, 200, 10)

	var bound BoundSet
	bound.Set(1, 4)
	bound.Set(2, 42)
	got := collect(fs.Scan(pred, 3, &bound), &bound)
	want := bruteScan(fs, pred, 3, &bound)
	sameFacts(t, got, want)
	if len(got) != 20 {
		t.Fatalf("expected 20 matches, got %d", len(got))
	}
}

func TestScanCatchUpAfterAdd(t *testing.T) {
	fs := NewInternedFactSet()
	const pred = 7
	fillFacts(fs, pred, 100, 10)

	var bound BoundSet
	bound.Set(1, 5)
	first := collect(fs.Scan(pred, 3, &bound), &bound)
	if len(first) != 10 {
		t.Fatalf("expected 10 matches before append, got %d", len(first))
	}

	// Append facts after the index was built; the watermark must catch up.
	fs.Add(mkFact(pred, 5000, 5, 42))
	fs.Add(mkFact(pred, 5001, 5, 99))

	got := collect(fs.Scan(pred, 3, &bound), &bound)
	want := bruteScan(fs, pred, 3, &bound)
	sameFacts(t, got, want)
	if len(got) != 12 {
		t.Fatalf("expected 12 matches after append, got %d", len(got))
	}
}

func TestScanAfterMerge(t *testing.T) {
	const pred = 7

	// Build an index on the destination first, then merge more facts in
	// from both a light and a full source; the index must self-heal.
	fs := NewInternedFactSet()
	fillFacts(fs, pred, 100, 10)
	var bound BoundSet
	bound.Set(1, 2)
	collect(fs.Scan(pred, 3, &bound), &bound) // force index build

	light := NewLightInternedFactSet()
	light.Add(mkFact(pred, 6000, 2, 42))
	fs.Merge(light)

	full := NewInternedFactSet()
	full.Add(mkFact(pred, 6001, 2, 42))
	// New (pred, arity) key through the adopt-wholesale path too.
	full.Add(mkFact(pred+1, 6002, 2))
	fs.Merge(full)

	got := collect(fs.Scan(pred, 3, &bound), &bound)
	want := bruteScan(fs, pred, 3, &bound)
	sameFacts(t, got, want)
	if len(got) != 12 {
		t.Fatalf("expected 12 matches after merges, got %d", len(got))
	}

	var b2 BoundSet
	b2.Set(0, 6002)
	got2 := collect(fs.Scan(pred+1, 2, &b2), &b2)
	if len(got2) != 1 {
		t.Fatalf("expected adopted predicate to be scannable, got %d facts", len(got2))
	}
}

func TestScanBelowThresholdBuildsNoIndex(t *testing.T) {
	fs := NewInternedFactSet()
	const pred = 7
	fillFacts(fs, pred, minIndexSize-1, 4)

	var bound BoundSet
	bound.Set(1, 1)
	got := collect(fs.Scan(pred, 3, &bound), &bound)
	want := bruteScan(fs, pred, 3, &bound)
	sameFacts(t, got, want)

	if len(fs.ByCol[PredArityI{pred, 3}]) != 0 {
		t.Fatalf("expected no column index below threshold, got %v", fs.ByCol[PredArityI{pred, 3}])
	}

	// Crossing the threshold flips Scan into index-building mode.
	fs.Add(mkFact(pred, 9000, 1, 42))
	collect(fs.Scan(pred, 3, &bound), &bound)
	if fs.ByCol[PredArityI{pred, 3}][1] == nil {
		t.Fatal("expected column 1 index once threshold reached")
	}
}

func TestScanNoMatchReturnsEmptyIndexed(t *testing.T) {
	fs := NewInternedFactSet()
	const pred = 7
	fillFacts(fs, pred, 100, 10)

	var bound BoundSet
	bound.Set(1, 999) // value never present
	r := fs.Scan(pred, 3, &bound)
	if r.Len() != 0 {
		t.Fatalf("expected 0 facts, got %d", r.Len())
	}
	if r.indices == nil {
		t.Fatal("expected indexed empty result, got unindexed full scan")
	}
}

func TestLightSetNeverIndexes(t *testing.T) {
	fs := NewLightInternedFactSet()
	const pred = 7
	fillFacts(fs, pred, 200, 10)

	var bound BoundSet
	bound.Set(1, 3)
	got := collect(fs.Scan(pred, 3, &bound), &bound)
	want := bruteScan(fs, pred, 3, &bound)
	sameFacts(t, got, want)
	if fs.ByCol != nil {
		t.Fatal("light set must not grow a ByCol map")
	}
}

func TestCloneStartsIndexCold(t *testing.T) {
	fs := NewInternedFactSet()
	const pred = 7
	fillFacts(fs, pred, 100, 10)

	var bound BoundSet
	bound.Set(1, 3)
	collect(fs.Scan(pred, 3, &bound), &bound) // build index on original

	clone := fs.Clone()
	if len(clone.ByCol) != 0 {
		t.Fatal("clone must not copy column indexes")
	}
	got := collect(clone.Scan(pred, 3, &bound), &bound)
	want := bruteScan(clone, pred, 3, &bound)
	sameFacts(t, got, want)
	if clone.ByCol[PredArityI{pred, 3}][1] == nil {
		t.Fatal("clone should rebuild indexes on demand")
	}
}

func TestChooseColumnPrefersHigherCardinality(t *testing.T) {
	fs := NewInternedFactSet()
	const pred = 7
	// Column 1 has 10 distinct values, column 2 has 2.
	for i := range 200 {
		fs.Add(mkFact(pred, uint64(1000+i), uint64(i)%10, uint64(i)%2))
	}

	// Build both single-column indexes.
	var b1 BoundSet
	b1.Set(1, 3)
	fs.Scan(pred, 3, &b1)
	var b2 BoundSet
	b2.Set(2, 0)
	fs.Scan(pred, 3, &b2)

	var both BoundSet
	both.Set(1, 3)
	both.Set(2, 0)
	col := fs.chooseColumn(PredArityI{pred, 3}, &both, 200)
	if col != 1 {
		t.Fatalf("expected column 1 (higher cardinality), got %d", col)
	}
}
