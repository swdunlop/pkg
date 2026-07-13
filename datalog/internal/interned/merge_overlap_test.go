package interned

import "testing"

// TestMergeOverlappingFactNotDuplicated is the regression test for the bug
// where Merge blindly appended other's fact slices even when a fact's hash
// key was already present in fs.Index. That mismatch meant fs.Index (one
// entry) and the underlying factChunks (two copies) disagreed, so Get/Scan
// iteration yielded the fact twice.
//
// This is reachable from the public API: an aggregate rule's derived facts
// (interned/aggregate.go's evalAggregates) are only deduplicated against
// each other, not against the accumulated `existing` set, before being
// merged in with existing.Merge(aggDerived). If EDB data (or a lower
// stratum) already contains a fact with the same predicate/arity/values
// that the aggregate rule derives, the two copies collide on Merge.
func TestMergeOverlappingFactNotDuplicated(t *testing.T) {
	const pred = 7
	dst := NewInternedFactSet()
	f := mkFact(pred, 1, 2, 3)
	dst.Add(f)

	src := NewLightInternedFactSet()
	src.Add(f) // same fact (same hash key) added independently to src

	dst.Merge(src)

	facts := dst.Get(pred, 3)
	if len(facts) != 1 {
		t.Fatalf("expected 1 fact after merging an overlapping set, got %d", len(facts))
	}
	if len(dst.Index) != 1 {
		t.Fatalf("expected 1 index entry, got %d", len(dst.Index))
	}
}

// TestMergeDisjointFactsStillWork guards against a fix that accidentally
// drops facts that are NOT already present.
func TestMergeDisjointFactsStillWork(t *testing.T) {
	const pred = 7
	dst := NewInternedFactSet()
	dst.Add(mkFact(pred, 1, 2, 3))

	src := NewLightInternedFactSet()
	src.Add(mkFact(pred, 4, 5, 6))
	src.Add(mkFact(pred, 7, 8, 9))

	dst.Merge(src)

	facts := dst.Get(pred, 3)
	if len(facts) != 3 {
		t.Fatalf("expected 3 facts after merging disjoint sets, got %d", len(facts))
	}
	if len(dst.Index) != 3 {
		t.Fatalf("expected 3 index entries, got %d", len(dst.Index))
	}
}

// TestMergeAdoptsNewPredicateWholesale checks the fast path for a
// (pred,arity) key the destination has never seen: it should adopt the
// source's factChunks directly rather than copying fact-by-fact.
func TestMergeAdoptsNewPredicateWholesale(t *testing.T) {
	const pred = 7
	dst := NewInternedFactSet()

	src := NewLightInternedFactSet()
	src.Add(mkFact(pred, 1, 2, 3))
	src.Add(mkFact(pred, 4, 5, 6))

	dst.Merge(src)

	facts := dst.Get(pred, 3)
	if len(facts) != 2 {
		t.Fatalf("expected 2 facts adopted wholesale, got %d", len(facts))
	}
}

// TestMergePartialOverlap mixes overlapping and new facts under the same
// predicate to ensure only the duplicate is deduplicated.
func TestMergePartialOverlap(t *testing.T) {
	const pred = 7
	dst := NewInternedFactSet()
	shared := mkFact(pred, 1, 2, 3)
	dst.Add(shared)

	src := NewLightInternedFactSet()
	src.Add(shared)
	src.Add(mkFact(pred, 9, 9, 9))

	dst.Merge(src)

	facts := dst.Get(pred, 3)
	if len(facts) != 2 {
		t.Fatalf("expected 2 facts (1 shared deduped + 1 new), got %d", len(facts))
	}
}

// TestMergeWholesaleAdoptedIndexStaysConsistent is the regression test for
// Merge's single index-maintenance mechanism: a (pred,arity) key fs has
// never seen is adopted wholesale (fs.ByPred[k] = ofc), and that path must
// still record every one of those facts' hash keys in fs.Index itself,
// rather than depending on a separate bulk copy of other.Index. A version
// of Merge that maintained the index two ways (per-fact writes for the
// overlap path, plus a trailing maps.Copy of the whole other.Index for the
// wholesale path) could drift if the two mechanisms ever disagreed about
// which facts were actually stored; pinning both paths through one
// mechanism means a wholesale-adopted fact set is immediately queryable
// and its Index entries are exactly the facts present in ByPred, with no
// stray entries and no missing ones -- verified here by mixing a
// wholesale-adopted predicate with an overlap-deduped one in the same
// Merge call.
func TestMergeWholesaleAdoptedIndexStaysConsistent(t *testing.T) {
	const predOld, predNew = 7, 8
	dst := NewInternedFactSet()
	shared := mkFact(predOld, 1, 2, 3)
	dst.Add(shared)

	src := NewLightInternedFactSet()
	src.Add(shared) // overlap on predOld, already in dst
	wf1 := mkFact(predNew, 4, 5, 6)
	wf2 := mkFact(predNew, 7, 8, 9)
	src.Add(wf1) // predNew is new to dst: adopted wholesale
	src.Add(wf2)

	dst.Merge(src)

	oldFacts := dst.Get(predOld, 3)
	if len(oldFacts) != 1 {
		t.Fatalf("predOld: expected 1 fact after dedup, got %d", len(oldFacts))
	}
	newFacts := dst.Get(predNew, 3)
	if len(newFacts) != 2 {
		t.Fatalf("predNew: expected 2 wholesale-adopted facts, got %d", len(newFacts))
	}
	// Index must have exactly 3 entries: the deduped shared fact, plus the
	// two wholesale-adopted facts -- no double counting, no missing entries.
	if len(dst.Index) != 3 {
		t.Fatalf("expected 3 index entries, got %d", len(dst.Index))
	}
	for _, f := range []InternedFact{shared, wf1, wf2} {
		if _, ok := dst.Index[InternedFactHash(f)]; !ok {
			t.Fatalf("expected Index to contain hash for fact %v", f)
		}
	}
}
