package interned

import (
	"maps"
	"math/bits"

	"swdunlop.dev/pkg/datalog"
)

// MaxFactArity is the maximum supported predicate arity.
// InternedFact uses a fixed-size array to eliminate per-fact heap allocation.
const MaxFactArity = 16

// InternedFact is a ground fact with all values replaced by dictionary IDs.
// Values is a fixed-size array -- no pointers, no GC scanning.
type InternedFact struct {
	Pred   uint64
	Arity  int
	Values [MaxFactArity]uint64
}

// InternedSub is a substitution with interned values.
type InternedSub []InternedSubEntry

// InternedSubEntry binds a variable name to an interned dictionary ID.
type InternedSubEntry struct {
	Name  string
	Value uint64
}

// Get looks up a variable name. Returns (value, true) if found.
func (s InternedSub) Get(name string) (uint64, bool) {
	for i := range s {
		if s[i].Name == name {
			return s[i].Value, true
		}
	}
	return 0, false
}

// Clone returns a copy of the substitution.
func (s InternedSub) Clone() InternedSub {
	c := make(InternedSub, len(s))
	copy(c, s)
	return c
}

// --- varSub: indexed variable substitution (zero-alloc hot path) ---

// MaxRuleVars is the maximum number of distinct variables in a single rule.
const MaxRuleVars = 16

// VarSub is a fixed-size substitution indexed by per-rule variable number.
// O(1) get/set via integer index, stack-allocatable, zero GC pressure.
type VarSub struct {
	Vals [MaxRuleVars]uint64
	Mask uint16
}

func (vs *VarSub) Set(idx int, val uint64) {
	vs.Vals[idx] = val
	vs.Mask |= 1 << uint(idx)
}

func (vs *VarSub) Clear(idx int) {
	vs.Mask &^= 1 << uint(idx)
}

// --- Compiled atoms (pre-interned rule terms) ---

// CompiledTerm is a rule term with constants pre-interned to uint64 IDs.
type CompiledTerm struct {
	VarName string // non-empty for variables
	VarIdx  int8   // per-rule variable index (-1 for constants)
	ConstID uint64 // pre-interned constant ID (valid when VarName == "")
}

// CompiledAtom is a rule atom with predicate and constants pre-interned.
type CompiledAtom struct {
	Pred  uint64
	Arity int
	Terms []CompiledTerm
}

// CompileAtom pre-interns an atom's predicate and constant terms.
func CompileAtom(pred string, terms []datalog.Term, dict *Dict) CompiledAtom {
	return CompileAtomV(pred, terms, dict, nil)
}

// CompileAtomV pre-interns an atom and assigns variable indices from varMap.
// If varMap is non-nil, new variables are added and indexed.
func CompileAtomV(pred string, terms []datalog.Term, dict *Dict, varMap map[string]int8) CompiledAtom {
	compiled := make([]CompiledTerm, len(terms))
	for i, t := range terms {
		switch v := t.(type) {
		case datalog.Variable:
			name := string(v)
			idx := int8(-1)
			if varMap != nil {
				if existing, ok := varMap[name]; ok {
					idx = existing
				} else {
					idx = int8(len(varMap))
					varMap[name] = idx
				}
			}
			compiled[i] = CompiledTerm{VarName: name, VarIdx: idx}
		case datalog.Constant:
			compiled[i] = CompiledTerm{VarIdx: -1, ConstID: dict.InternConstant(v)}
		}
	}
	return CompiledAtom{
		Pred:  dict.Intern(pred),
		Arity: len(terms),
		Terms: compiled,
	}
}

// --- boundSet (stack-allocated replacement for map[int]uint64) ---

// BoundSet tracks which argument positions are bound to interned IDs.
// Uses a bitmask and fixed-size array -- zero allocation.
type BoundSet struct {
	Vals [MaxFactArity]uint64
	Mask uint16
}

func (b *BoundSet) Set(pos int, val uint64) {
	b.Vals[pos] = val
	b.Mask |= 1 << uint(pos)
}

func (b *BoundSet) Get(pos int) (uint64, bool) {
	if b.Mask>>uint(pos)&1 != 0 {
		return b.Vals[pos], true
	}
	return 0, false
}

// MatchesBound returns true if all bound positions in bs match the fact's values.
// Use as a fast pre-filter before calling UnifyV.
func MatchesBound(bs *BoundSet, fact *InternedFact) bool {
	mask := bs.Mask
	for mask != 0 {
		pos := bits.TrailingZeros16(mask)
		if fact.Values[pos] != bs.Vals[pos] {
			return false
		}
		mask &= mask - 1
	}
	return true
}

// --- factChunks: growable slice of facts ---

// factChunks stores InternedFacts in a single growable slice.
type factChunks struct {
	facts []InternedFact
}

func (fc *factChunks) append(f InternedFact) {
	fc.facts = append(fc.facts, f)
}

func (fc *factChunks) toSlice() []InternedFact {
	return fc.facts
}

// --- PredArityI and InternedFactSet ---

// PredArityI is a zero-allocation map key for interned predicate+arity lookups.
type PredArityI struct {
	Pred  uint64
	Arity int
}

// colIndex indexes one argument position of one (pred, arity) fact slice.
// Facts at positions >= indexedUpTo are not yet indexed; catchUp extends
// the index to cover them. Facts are append-only, so entries never go stale.
type colIndex struct {
	m           map[uint64][]int32 // value -> positions in factChunks.facts
	indexedUpTo int32
}

// catchUp extends the index to cover facts appended since the last scan.
func (ci *colIndex) catchUp(facts []InternedFact, col int) {
	for i := ci.indexedUpTo; i < int32(len(facts)); i++ {
		v := facts[i].Values[col]
		ci.m[v] = append(ci.m[v], i)
	}
	ci.indexedUpTo = int32(len(facts))
}

// minIndexSize is the minimum fact count before Scan builds a column index.
// Below this, a full scan filtered by MatchesBound is cheaper than a map.
const minIndexSize = 64

// InternedFactSet is an in-memory set of interned facts.
type InternedFactSet struct {
	ByPred map[PredArityI]*factChunks
	ByCol  map[PredArityI]map[int]*colIndex // nil for light sets; built lazily by Scan
	Index  map[uint64]struct{}
}

// emptyIndices is a sentinel for "indexed scan, no matches" (distinct from nil = unindexed).
var emptyIndices = make([]int32, 0)

// ScanResult holds the result of a Scan operation. When indices is non-nil,
// only the indexed facts should be iterated; when nil, iterate all facts.
type ScanResult struct {
	facts   []InternedFact
	indices []int32 // nil = unindexed (iterate all facts); non-nil = indexed
}

// Len returns the number of facts in the scan result.
func (r ScanResult) Len() int {
	if r.indices != nil {
		return len(r.indices)
	}
	return len(r.facts)
}

// Fact returns a pointer to the i-th fact in the scan result.
func (r ScanResult) Fact(i int) *InternedFact {
	if r.indices != nil {
		return &r.facts[r.indices[i]]
	}
	return &r.facts[i]
}

func NewInternedFactSet() InternedFactSet {
	return InternedFactSet{
		ByPred: make(map[PredArityI]*factChunks),
		ByCol:  make(map[PredArityI]map[int]*colIndex),
		Index:  make(map[uint64]struct{}),
	}
}

func NewInternedFactSetCap(indexCap int) InternedFactSet {
	return InternedFactSet{
		ByPred: make(map[PredArityI]*factChunks),
		ByCol:  make(map[PredArityI]map[int]*colIndex),
		Index:  make(map[uint64]struct{}, indexCap),
	}
}

func NewLightInternedFactSet() InternedFactSet {
	return InternedFactSet{
		ByPred: make(map[PredArityI]*factChunks),
		Index:  make(map[uint64]struct{}),
	}
}

const (
	FNVOffset64 = 14695981039346656037
	FNVPrime64  = 1099511628211
)

// InternedFactHash computes the FNV-1a hash for an InternedFact.
func InternedFactHash(f InternedFact) uint64 {
	h := uint64(FNVOffset64)
	h ^= f.Pred
	h *= FNVPrime64
	for i := range f.Arity {
		h ^= 0xff
		h *= FNVPrime64
		h ^= f.Values[i]
		h *= FNVPrime64
	}
	return h
}

// GroundCompiled resolves a compiled atom under an InternedSub.
func GroundCompiled(ca CompiledAtom, sub InternedSub) (InternedFact, bool) {
	var f InternedFact
	f.Pred = ca.Pred
	f.Arity = ca.Arity
	for i, t := range ca.Terms {
		if t.VarName != "" {
			v, ok := sub.Get(t.VarName)
			if !ok {
				return InternedFact{}, false
			}
			f.Values[i] = v
		} else {
			f.Values[i] = t.ConstID
		}
	}
	return f, true
}

// --- VarSub-based variants (indexed, zero-alloc hot path) ---

// AllTermsBoundV reports whether every term is resolved under a VarSub.
func AllTermsBoundV(ca CompiledAtom, sub *VarSub) bool {
	for i := range ca.Arity {
		t := ca.Terms[i]
		if t.VarIdx >= 0 {
			if sub.Mask>>uint(t.VarIdx)&1 == 0 {
				return false
			}
		}
	}
	return true
}

// HashAndGroundV computes the FNV-1a hash and InternedFact for a compiled
// head atom using a VarSub. Single pass, zero allocation.
func HashAndGroundV(ca CompiledAtom, sub *VarSub) (InternedFact, uint64, bool) {
	var f InternedFact
	f.Pred = ca.Pred
	f.Arity = ca.Arity
	h := uint64(FNVOffset64)
	h ^= ca.Pred
	h *= FNVPrime64
	for i := range ca.Arity {
		t := ca.Terms[i]
		h ^= 0xff
		h *= FNVPrime64
		if t.VarIdx >= 0 {
			if sub.Mask>>uint(t.VarIdx)&1 == 0 {
				return InternedFact{}, 0, false
			}
			v := sub.Vals[t.VarIdx]
			f.Values[i] = v
			h ^= v
		} else {
			f.Values[i] = t.ConstID
			h ^= t.ConstID
		}
		h *= FNVPrime64
	}
	return f, h, true
}

// GroundV resolves a compiled atom under a VarSub.
func GroundV(ca CompiledAtom, sub *VarSub) (InternedFact, bool) {
	var f InternedFact
	f.Pred = ca.Pred
	f.Arity = ca.Arity
	for i := range ca.Arity {
		t := ca.Terms[i]
		if t.VarIdx >= 0 {
			if sub.Mask>>uint(t.VarIdx)&1 == 0 {
				return InternedFact{}, false
			}
			f.Values[i] = sub.Vals[t.VarIdx]
		} else {
			f.Values[i] = t.ConstID
		}
	}
	return f, true
}

// UnifyV unifies a compiled atom against an InternedFact using a VarSub.
// Returns true if unification succeeds. On success, sub is extended in place;
// on failure, any partial bindings are rolled back.
func UnifyV(ca CompiledAtom, fact *InternedFact, sub *VarSub) bool {
	if ca.Pred != fact.Pred || ca.Arity != fact.Arity {
		return false
	}

	// Pass 1: check compatibility and count new bindings.
	var newMask uint16
	for i := range ca.Arity {
		val := fact.Values[i]
		t := ca.Terms[i]
		if t.VarIdx < 0 {
			if t.ConstID != val {
				return false
			}
		} else {
			bit := uint16(1) << uint(t.VarIdx)
			if sub.Mask&bit != 0 {
				if sub.Vals[t.VarIdx] != val {
					return false
				}
			} else {
				newMask |= bit
			}
		}
	}

	// Pass 2: apply new bindings (only reached on success).
	if newMask != 0 {
		sub.Mask |= newMask
		for i := range ca.Arity {
			t := ca.Terms[i]
			if t.VarIdx >= 0 && newMask>>uint(t.VarIdx)&1 != 0 {
				sub.Vals[t.VarIdx] = fact.Values[i]
			}
		}
	}
	return true
}

// BoundArgsV computes bound argument positions from a compiled atom and VarSub.
func BoundArgsV(ca CompiledAtom, sub *VarSub) BoundSet {
	var bs BoundSet
	for i := range ca.Arity {
		t := ca.Terms[i]
		if t.VarIdx < 0 {
			bs.Set(i, t.ConstID)
		} else if sub.Mask>>uint(t.VarIdx)&1 != 0 {
			bs.Set(i, sub.Vals[t.VarIdx])
		}
	}
	return bs
}

func (fs InternedFactSet) Add(fact InternedFact) bool {
	return fs.AddWithKey(fact, InternedFactHash(fact))
}

// AddWithKey trusts fk as the fact's identity with no equality check against
// any fact already stored under that key -- a 64-bit FNV-1a hash collision
// between two DIFFERENT facts would silently drop the second fact (see
// TestAddWithKeyCollisionMechanism). This is an accepted, deliberate
// trade-off, not an oversight: the birthday bound puts a 50% collision
// probability at roughly 2^32 facts in a single set, and adding a
// stored-fact equality check on every hash hit would require the index to
// carry a locator back to the fact (not just presence), which measurably
// slows this function -- it is called for every derived fact in the
// semi-naive fixpoint's innermost loop. Revisit only if a fact set is ever
// expected to approach billions of facts.
func (fs InternedFactSet) AddWithKey(fact InternedFact, fk uint64) bool {
	if _, exists := fs.Index[fk]; exists {
		return false
	}
	fs.Index[fk] = struct{}{}
	k := PredArityI{fact.Pred, fact.Arity}
	fc := fs.ByPred[k]
	if fc == nil {
		fc = &factChunks{}
		fs.ByPred[k] = fc
	}
	fc.append(fact)
	return true
}

// AddUnchecked adds a fact without checking the Index for duplicates.
// The caller must have already verified that the fact is not present.
func (fs InternedFactSet) AddUnchecked(fact InternedFact, fk uint64) {
	fs.Index[fk] = struct{}{}
	k := PredArityI{fact.Pred, fact.Arity}
	fc := fs.ByPred[k]
	if fc == nil {
		fc = &factChunks{}
		fs.ByPred[k] = fc
	}
	fc.append(fact)
}

func (fs InternedFactSet) Get(pred uint64, arity int) []InternedFact {
	if fc := fs.ByPred[PredArityI{pred, arity}]; fc != nil {
		return fc.toSlice()
	}
	return nil
}

// Scan returns facts matching bound arguments. When a column index is
// available (or worth building) for a bound position, uses it for O(1)
// lookup; otherwise falls back to a full predicate scan.
//
// Scan builds and extends column indexes lazily, so it mutates the set.
// It is NOT safe for concurrent use -- same contract as Add, but
// non-obvious because scans look read-only.
func (fs InternedFactSet) Scan(pred uint64, arity int, bound *BoundSet) ScanResult {
	k := PredArityI{pred, arity}
	fc := fs.ByPred[k]
	if fc == nil {
		return ScanResult{}
	}
	if fs.ByCol == nil || bound.Mask == 0 {
		return ScanResult{facts: fc.facts}
	}
	col := fs.chooseColumn(k, bound, len(fc.facts))
	if col < 0 {
		return ScanResult{facts: fc.facts}
	}
	ci := fs.colIndexFor(k, col)
	ci.catchUp(fc.facts, col)
	val, _ := bound.Get(col)
	indices := ci.m[val]
	if indices == nil {
		return ScanResult{indices: emptyIndices}
	}
	return ScanResult{facts: fc.facts, indices: indices}
}

// chooseColumn picks the bound column to scan on, or -1 for a full scan.
// Prefers an existing index with the highest cardinality (len of its map
// is a free selectivity estimate); otherwise builds on the lowest bound
// position, unless the relation is too small to be worth indexing.
func (fs InternedFactSet) chooseColumn(k PredArityI, bound *BoundSet, factCount int) int {
	cols := fs.ByCol[k]
	best, bestCard := -1, 0
	first := -1
	mask := bound.Mask
	for mask != 0 {
		pos := bits.TrailingZeros16(mask)
		mask &= mask - 1
		if first < 0 {
			first = pos
		}
		if ci := cols[pos]; ci != nil && len(ci.m) > bestCard {
			best, bestCard = pos, len(ci.m)
		}
	}
	if best >= 0 {
		return best
	}
	if factCount < minIndexSize {
		return -1
	}
	return first
}

// colIndexFor returns the index for (k, col), creating it if absent.
func (fs InternedFactSet) colIndexFor(k PredArityI, col int) *colIndex {
	cols := fs.ByCol[k]
	if cols == nil {
		cols = make(map[int]*colIndex)
		fs.ByCol[k] = cols
	}
	ci := cols[col]
	if ci == nil {
		ci = &colIndex{m: make(map[uint64][]int32)}
		cols[col] = ci
	}
	return ci
}

// Merge copies all facts from other into fs in bulk. Existing column
// indexes stay valid (facts are append-only) and catch up on next Scan;
// no index maintenance is needed here.
//
// Callers that already guarantee disjointness (e.g. evalRules, whose emit
// closure checks both existing.Index and emitted.Index before adding a
// fact) pay only the cost of the (pred,arity) presence check below, since
// dfc is nil whenever fs has no facts yet for that key. Callers that can't
// make that guarantee (e.g. an aggregate rule's derived facts, which are
// only deduplicated against each other, not against the accumulated
// existing set) rely on the per-fact hash check to avoid storing the same
// fact twice under one hash-index entry.
func (fs InternedFactSet) Merge(other InternedFactSet) {
	for k, ofc := range other.ByPred {
		dfc := fs.ByPred[k]
		if dfc == nil {
			fs.ByPred[k] = ofc
			continue
		}
		for _, f := range ofc.facts {
			fk := InternedFactHash(f)
			if _, exists := fs.Index[fk]; exists {
				continue
			}
			fs.Index[fk] = struct{}{}
			dfc.append(f)
		}
	}
	// Any (pred,arity) key present only in other was adopted wholesale
	// above; copy remaining hash keys (facts under predicates fs never
	// saw) that the per-fact loop didn't already add.
	maps.Copy(fs.Index, other.Index)
}

// Clone returns a deep copy of the fact set. Column indexes are not
// copied -- the clone starts index-cold and rebuilds them on demand.
func (fs InternedFactSet) Clone() InternedFactSet {
	result := InternedFactSet{
		ByPred: make(map[PredArityI]*factChunks, len(fs.ByPred)),
		Index:  make(map[uint64]struct{}, len(fs.Index)),
	}
	for k := range fs.Index {
		result.Index[k] = struct{}{}
	}
	for k, fc := range fs.ByPred {
		cp := make([]InternedFact, len(fc.facts))
		copy(cp, fc.facts)
		result.ByPred[k] = &factChunks{facts: cp}
	}
	if fs.ByCol != nil {
		result.ByCol = make(map[PredArityI]map[int]*colIndex)
	}
	return result
}

// Remap updates all IDs in the fact set using the given remap table (from Dict.Freeze).
func (fs InternedFactSet) Remap(remap []uint64) InternedFactSet {
	result := NewInternedFactSet()
	for _, fc := range fs.ByPred {
		for _, f := range fc.facts {
			f.Pred = remap[f.Pred]
			for i := range f.Arity {
				f.Values[i] = remap[f.Values[i]]
			}
			result.Add(f)
		}
	}
	return result
}
