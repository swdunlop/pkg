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

func (fc *factChunks) appendFrom(other *factChunks) {
	fc.facts = append(fc.facts, other.facts...)
}

// --- PredArityI and InternedFactSet ---

// PredArityI is a zero-allocation map key for interned predicate+arity lookups.
type PredArityI struct {
	Pred  uint64
	Arity int
}

// InternedFactSet is an in-memory set of interned facts.
type InternedFactSet struct {
	ByPred map[PredArityI]*factChunks
	ByArg0 map[PredArityI]map[uint64][]int32 // nil for light sets; values are indices into ByPred factChunks
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
		ByArg0: make(map[PredArityI]map[uint64][]int32),
		Index:  make(map[uint64]struct{}),
	}
}

func NewInternedFactSetCap(indexCap int) InternedFactSet {
	return InternedFactSet{
		ByPred: make(map[PredArityI]*factChunks),
		ByArg0: make(map[PredArityI]map[uint64][]int32),
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

// AllTermsBound reports whether every term in a compiled atom is resolved
// under the given substitution.
func AllTermsBound(ca CompiledAtom, sub InternedSub) bool {
	for i := range ca.Arity {
		t := ca.Terms[i]
		if t.VarName != "" {
			if _, ok := sub.Get(t.VarName); !ok {
				return false
			}
		}
	}
	return true
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

// UnifyCompiled unifies a compiled atom against an InternedFact.
// No dict lookups needed -- constants are pre-interned.
func UnifyCompiled(ca CompiledAtom, fact InternedFact, sub InternedSub) (InternedSub, bool) {
	if ca.Pred != fact.Pred || ca.Arity != fact.Arity {
		return nil, false
	}

	// Pass 1: check compatibility without allocating.
	newBindings := 0
	for i := range ca.Arity {
		val := fact.Values[i]
		t := ca.Terms[i]
		if t.VarName == "" {
			if t.ConstID != val {
				return nil, false
			}
		} else {
			if existing, ok := sub.Get(t.VarName); ok {
				if existing != val {
					return nil, false
				}
			} else {
				newBindings++
			}
		}
	}

	// Pass 2: extend using append (DFS-safe backing array reuse).
	if newBindings == 0 {
		return sub, true
	}
	result := sub
	for i := range ca.Arity {
		t := ca.Terms[i]
		if t.VarName != "" {
			if _, bound := result.Get(t.VarName); !bound {
				result = append(result, InternedSubEntry{t.VarName, fact.Values[i]})
			}
		}
	}
	return result, true
}

// BoundArgsCompiled computes bound argument positions from a compiled atom.
func BoundArgsCompiled(ca CompiledAtom, sub InternedSub) BoundSet {
	var bs BoundSet
	for i := range ca.Arity {
		t := ca.Terms[i]
		if t.VarName == "" {
			bs.Set(i, t.ConstID)
		} else if val, ok := sub.Get(t.VarName); ok {
			bs.Set(i, val)
		}
	}
	return bs
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
	if fs.ByArg0 != nil && fact.Arity > 0 {
		idx := int32(len(fc.facts) - 1)
		m := fs.ByArg0[k]
		if m == nil {
			m = make(map[uint64][]int32)
			fs.ByArg0[k] = m
		}
		m[fact.Values[0]] = append(m[fact.Values[0]], idx)
	}
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
	if fs.ByArg0 != nil && fact.Arity > 0 {
		idx := int32(len(fc.facts) - 1)
		m := fs.ByArg0[k]
		if m == nil {
			m = make(map[uint64][]int32)
			fs.ByArg0[k] = m
		}
		m[fact.Values[0]] = append(m[fact.Values[0]], idx)
	}
}

func (fs InternedFactSet) Get(pred uint64, arity int) []InternedFact {
	if fc := fs.ByPred[PredArityI{pred, arity}]; fc != nil {
		return fc.toSlice()
	}
	return nil
}

// Scan returns facts matching bound arguments. If arg0 is bound and the
// ByArg0 index is available, uses it for O(1) lookup; otherwise falls back
// to a full predicate scan.
func (fs InternedFactSet) Scan(pred uint64, arity int, bound *BoundSet) ScanResult {
	k := PredArityI{pred, arity}
	if val, ok := bound.Get(0); ok && fs.ByArg0 != nil {
		if m := fs.ByArg0[k]; m != nil {
			fc := fs.ByPred[k]
			if fc == nil {
				return ScanResult{}
			}
			indices := m[val]
			if indices == nil {
				return ScanResult{indices: emptyIndices}
			}
			return ScanResult{facts: fc.facts, indices: indices}
		}
	}
	if fc := fs.ByPred[k]; fc != nil {
		return ScanResult{facts: fc.facts}
	}
	return ScanResult{}
}

// Merge copies all facts from other into fs in bulk.
func (fs InternedFactSet) Merge(other InternedFactSet) {
	maps.Copy(fs.Index, other.Index)
	for k, ofc := range other.ByPred {
		if dfc := fs.ByPred[k]; dfc != nil {
			oldLen := int32(len(dfc.facts))
			dfc.appendFrom(ofc)
			if fs.ByArg0 != nil {
				if other.ByArg0 != nil {
					// Full-to-full: offset other's indices.
					if om := other.ByArg0[k]; om != nil {
						dm := fs.ByArg0[k]
						if dm == nil {
							dm = make(map[uint64][]int32, len(om))
							fs.ByArg0[k] = dm
						}
						for val, indices := range om {
							for _, idx := range indices {
								dm[val] = append(dm[val], oldLen+idx)
							}
						}
					}
				} else {
					// Light-to-full: build indices from facts.
					dm := fs.ByArg0[k]
					if dm == nil {
						dm = make(map[uint64][]int32)
						fs.ByArg0[k] = dm
					}
					for i := range ofc.facts {
						f := &ofc.facts[i]
						if f.Arity > 0 {
							dm[f.Values[0]] = append(dm[f.Values[0]], oldLen+int32(i))
						}
					}
				}
			}
		} else {
			fs.ByPred[k] = ofc
			if fs.ByArg0 != nil {
				if other.ByArg0 != nil {
					// Full-to-full: steal other's ByArg0 entry (indices valid as-is).
					if om := other.ByArg0[k]; om != nil {
						fs.ByArg0[k] = om
					}
				} else {
					// Light-to-full: build indices from facts.
					m := make(map[uint64][]int32)
					for i := range ofc.facts {
						f := &ofc.facts[i]
						if f.Arity > 0 {
							m[f.Values[0]] = append(m[f.Values[0]], int32(i))
						}
					}
					if len(m) > 0 {
						fs.ByArg0[k] = m
					}
				}
			}
		}
	}
}

// Clone returns a deep copy of the fact set.
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
	if fs.ByArg0 != nil {
		result.ByArg0 = make(map[PredArityI]map[uint64][]int32, len(fs.ByArg0))
		for k, m := range fs.ByArg0 {
			newM := make(map[uint64][]int32, len(m))
			for arg0, indices := range m {
				cp := make([]int32, len(indices))
				copy(cp, indices)
				newM[arg0] = cp
			}
			result.ByArg0[k] = newM
		}
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
