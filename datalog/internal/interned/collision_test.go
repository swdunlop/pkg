package interned

import "testing"

// TestAddWithKeyCollisionMechanism documents (rather than fixes) the
// consequence of AddWithKey trusting its key argument as fact identity with
// no equality check. Finding a genuine 64-bit FNV-1a collision by brute
// force is computationally infeasible in a unit test (birthday bound: 50%
// probability requires ~2^32 facts), so this test injects an artificial
// collision via AddWithKey's exposed key parameter -- the same mechanism a
// real collision would trigger, just without needing ~4 billion facts to
// find one.
//
// Two DIFFERENT facts sharing a key: the second Add is reported as a
// duplicate (returns false) and is never stored. This is the reachable
// failure mode named in the suspicion -- silently dropping a real fact, and
// (via negation elsewhere in the evaluator) potentially flipping derived
// results.
//
// See AddWithKey's doc comment for why this is left as an accepted,
// probabilistic trade-off rather than fixed: the birthday-bound collision
// probability at any realistic fact-set size is negligible, and a
// stored-fact equality check on every hash hit would slow the semi-naive
// fixpoint's hottest insertion path (measured ~5% slower, roughly 2x the
// allocations, in a synthetic 200k-fact benchmark comparing AddWithKey's
// current hash-only check against a locator-based fact-compare variant).
func TestAddWithKeyCollisionMechanism(t *testing.T) {
	fs := NewInternedFactSet()
	const pred = 7
	f1 := mkFact(pred, 1, 2, 3)
	f2 := mkFact(pred, 9, 9, 9) // genuinely different fact, same key below
	const sharedKey = uint64(0xDEADBEEF)

	if ok := fs.AddWithKey(f1, sharedKey); !ok {
		t.Fatalf("expected first add to succeed")
	}
	if ok := fs.AddWithKey(f2, sharedKey); ok {
		t.Fatalf("expected second add under a colliding key to be (incorrectly) reported as a duplicate")
	}

	facts := fs.Get(pred, 3)
	if len(facts) != 1 {
		t.Fatalf("expected exactly 1 fact stored (f2 silently dropped by the collision), got %d", len(facts))
	}
	if facts[0] != f1 {
		t.Fatalf("expected the surviving fact to be f1, got %v", facts[0])
	}
}
