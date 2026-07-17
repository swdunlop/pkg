package seminaive

import (
	"fmt"
	"strings"
	"sync"

	"swdunlop.dev/pkg/datalog"
	"swdunlop.dev/pkg/datalog/internal/interned"
	"swdunlop.dev/pkg/datalog/syntax"
)

// witness is one recorded derivation: either a plain rule's join witness
// (rule/body/detail, as below) or an aggregate's group witness (agg fields,
// see witnessSampleCap and buildAggWitness in aggregate.go), distinguished
// by isAgg. Both share one map (recorder.witnesses/Provenance.witnesses)
// keyed by fact key, since a fact is derived by exactly one rule or one
// aggregate, never both -- Explain and ExplainTree branch on isAgg to render
// the right shape.
type witness struct {
	rule int // index into Provenance.rules (plain) or Provenance.aggRules (aggregate, isAgg true) -- see ruleText/aggRuleText

	// body holds the positive join atoms' fact keys (identity, matching the
	// spec's wire shape and cheap to compare/dedupe) paired with the exact
	// grounded InternedFact each key was computed from (HashAndGroundV
	// already produces both at the emit seam -- see eval.go -- so storing
	// the fact costs nothing extra there and avoids needing a key->fact
	// reverse index into the dict/fact sets at explain time, long after the
	// evaluation's existing/emitted sets are gone). Unused (nil) for an
	// aggregate witness -- see sample below.
	body []bodyWitness

	detail []string // ground renderings: constraints, is-expressions, builtins, negated atoms (plain witness only; empty for aggregate)

	// --- aggregate-only fields (isAgg == true) ---

	isAgg      bool // true when this witness explains an aggregate head, not a plain-rule head
	groupCount int  // the group's true solution count (may exceed len(sample) -- see witnessSampleCap)

	// sample holds up to witnessSampleCap contributor tuples, captured as
	// the group streamed past (first N in iteration order, deterministic
	// given fixed rule/join order -- see evalAggregates). Each entry is one
	// sampled solution's own body/detail grounding, built the same way
	// buildWitness grounds a plain rule's body, so ExplainTree can recurse
	// into a sampled contributor's own derivation exactly like any other
	// body fact.
	sample []witnessSample
}

// witnessSample is one sampled contributor solution of an aggregate group:
// the ground body facts and detail lines of the aggregate's condition under
// that solution's substitution, mirroring a plain witness's body/detail
// shape so the text renderer and ExplainTree treat a sample entry the same
// way they treat a plain rule's body items.
type witnessSample struct {
	body   []bodyWitness
	detail []string
}

// witnessSampleCap bounds how many contributor tuples an aggregate witness
// retains per group, captured as the group streams past (see aggregate.go's
// aggGroup). A count over 500k solutions must not materialize them: the cap
// is enforced at the point of capture, not by sampling from a materialized
// slice afterward, so per-group memory for provenance stays O(cap) instead
// of O(group size) regardless of how many solutions actually stream past.
const witnessSampleCap = 10

// bodyWitness pairs a positive body atom's fact key with the exact fact it
// was ground to under the winning substitution.
type bodyWitness struct {
	key  uint64
	fact interned.InternedFact
}

// buildWitness assembles a witness from a rule's source-order body items and
// the winning substitution, at the moment its head fact is first emitted
// (see eval.go's emit closure). items is the rule body in original source
// order (compiledRule.sourceItems, NOT the join-selectivity-reordered
// cr.body/bodyByDelta): rendering detail lines in source order, rather than
// whatever order the join happened to evaluate them in, is what makes the
// rendered explanation read like the rule the author wrote.
//
// For each item:
//   - bodyItemJoin: grounds the atom under sub via HashAndGroundV (the same
//     function the head uses), reproducing the exact fact that satisfied
//     this join -- a body atom appearing twice with different variables
//     grounds to two distinct facts here, correctly, since each occurrence
//     has its own bodyItem with its own compiled terms.
//   - bodyItemCompare, bodyItemIs, bodyItemBind, bodyItemBindMulti: ground
//     to a detail string via groundDetail (self-evident given their inputs,
//     per the feature spec).
//   - negativeBody atoms (tracked separately from items, like evaluation
//     itself defers them -- see compileBody's doc comment): ground to the
//     atom that had no match, appended to detail after the positive items'
//     details, prefixed with "not " to distinguish a negation-detail line
//     from a constraint-detail line at render time.
//
// A body item that fails to ground under sub (should not happen: sub is the
// exact substitution that just satisfied the whole body, including this
// item) is skipped rather than panicking -- witness capture must never be
// able to crash an otherwise-successful Transform.
func buildWitness(flatIdx int, items []bodyItem, negativeBody []interned.CompiledAtom, sub *interned.VarSub, dict *interned.Dict) witness {
	body, detail := groundBody(items, negativeBody, sub, dict)
	return witness{rule: flatIdx, body: body, detail: detail}
}

// groundBody grounds a rule/aggregate body's items and negated atoms under
// the winning substitution, the shared worker behind buildWitness (plain
// rules) and buildAggSample (aggregate.go's per-solution contributor
// sampling) -- both need exactly this body/detail shape, differing only in
// what they wrap it in (a witness vs. a witnessSample). See buildWitness's
// doc comment above for the per-item grounding rules this implements.
func groundBody(items []bodyItem, negativeBody []interned.CompiledAtom, sub *interned.VarSub, dict *interned.Dict) (body []bodyWitness, detail []string) {
	for _, item := range items {
		switch item.kind {
		case bodyItemJoin:
			fact, fk, ok := interned.HashAndGroundV(item.ca, sub)
			if !ok {
				continue
			}
			body = append(body, bodyWitness{key: fk, fact: fact})
		case bodyItemCompare, bodyItemIs, bodyItemBind, bodyItemBindMulti:
			if line, ok := groundDetail(item, sub, dict); ok {
				detail = append(detail, line)
			}
		}
	}
	for _, neg := range negativeBody {
		detail = append(detail, "not "+groundAtomString(neg, sub, dict))
	}
	return body, detail
}

// buildAggSample grounds one aggregate-group contributor solution's body
// items into a witnessSample, the aggregate mirror of buildWitness: same
// grounding rules (see groundBody), packaged for aggGroup's capped sample
// slice (see witnessSampleCap) instead of a plain rule's witness.
func buildAggSample(items []bodyItem, negativeBody []interned.CompiledAtom, sub *interned.VarSub, dict *interned.Dict) witnessSample {
	body, detail := groundBody(items, negativeBody, sub, dict)
	return witnessSample{body: body, detail: detail}
}

// groundDetail renders a single non-join body item's ground text, given the
// substitution that satisfied the whole rule body. Constraints render as
// "LHS op RHS" with each side resolved to its concrete value (e.g. "34 > 20"
// for PortCount > 20 with PortCount = 34); is-expressions and binding
// builtins render as "Var = value" using the original atom/expression text
// on the left so the reader can see which rule term produced the value.
func groundDetail(item bodyItem, sub *interned.VarSub, dict *interned.Dict) (string, bool) {
	switch item.kind {
	case bodyItemCompare:
		if len(item.ca.Terms) < 2 {
			return "", false
		}
		lv, lok := resolveCompiledTermValue(item.ca.Terms[0], sub, dict)
		rv, rok := resolveCompiledTermValue(item.ca.Terms[1], sub, dict)
		if !lok || !rok {
			return "", false
		}
		return fmt.Sprintf("%s %s %s", detailValue(lv), item.atom.Pred, detailValue(rv)), true
	case bodyItemIs:
		idx := item.outVarIdx
		if idx < 0 || sub.Mask>>uint(idx)&1 == 0 {
			return "", false
		}
		return fmt.Sprintf("%s is %s = %s", item.atom.Terms[0].String(), item.atom.Expr.String(), detailValue(dict.Resolve(sub.Vals[idx]))), true
	case bodyItemBind:
		idx := item.outVarIdx
		var out string
		if idx >= 0 && sub.Mask>>uint(idx)&1 != 0 {
			out = detailValue(dict.Resolve(sub.Vals[idx]))
		} else if last := item.ca.Terms[item.ca.Arity-1]; last.VarIdx < 0 {
			out = detailValue(dict.Resolve(last.ConstID))
		} else {
			return "", false
		}
		return fmt.Sprintf("%s = %s", groundAtomString(item.ca, sub, dict), out), true
	case bodyItemBindMulti:
		// Multi-result builtins can bind several output positions per call;
		// the ground atom text (groundAtomString) already shows every
		// resolved argument, inputs and outputs alike, so there is no
		// separate "= value" suffix the way there is for the single-output
		// bodyItemBind case above.
		return groundAtomString(item.ca, sub, dict), true
	}
	return "", false
}

// groundAtomString renders a compiled atom's predicate and resolved
// arguments as "pred(arg1, arg2, ...)", resolving any unbound variable
// position (possible only for a negated atom, whose variables need not all
// be bound the same way a positive atom's are -- see checkBodySafety) as
// "_".
func groundAtomString(ca interned.CompiledAtom, sub *interned.VarSub, dict *interned.Dict) string {
	var buf strings.Builder
	buf.WriteString(dict.Resolve(ca.Pred).(string))
	buf.WriteByte('(')
	for i := range ca.Arity {
		if i > 0 {
			buf.WriteString(", ")
		}
		if v, ok := resolveCompiledTermValue(ca.Terms[i], sub, dict); ok {
			buf.WriteString(detailValue(v))
		} else {
			buf.WriteByte('_')
		}
	}
	buf.WriteByte(')')
	return buf.String()
}

// detailValue renders a resolved Go value (string, int64, float64, or any
// other dict-native type) the same way a datalog.Constant would render,
// mirroring interned.ConstantToAny's type switch in reverse -- these are the
// exact dict-native types Dict.Resolve/resolveCompiledTermValue can hand
// back (see Dict.Intern's doc comment).
func detailValue(v any) string {
	switch tv := v.(type) {
	case float64:
		return datalog.Float(tv).String()
	case int64:
		return datalog.Integer(tv).String()
	case string:
		return datalog.String(tv).String()
	case datalog.ID:
		return tv.String()
	case datalog.Bool:
		return tv.String()
	case datalog.Null:
		return tv.String()
	case *datalog.Composite:
		return tv.String()
	default:
		return fmt.Sprintf("%v", tv)
	}
}

// recorder accumulates witnesses for one Transform call. It is created fresh
// per evaluation and only installed into the caller-visible Provenance after
// Transform succeeds (see Provenance.install) -- so a WithFactLimit abort,
// context cancellation, or any other eval error discards its partial content
// along with the partial results it would have explained, instead of leaving
// a Provenance that half-explains a database the caller never received.
type recorder struct {
	witnesses map[uint64]witness // fact key -> first derivation (first witness wins, see eval.go's emit seam)
}

func newRecorder() *recorder {
	return &recorder{witnesses: make(map[uint64]witness)}
}

// record stores fact fk's witness, but only the first time fk is seen: the
// semi-naive emit seam in eval.go only calls record once per fact (right
// after AddUnchecked, which itself only runs once per fact -- see the
// existing.Index/emitted.Index dedup checks ahead of it), so this guard is
// belt-and-suspenders against any future caller that forgets that
// discipline, not the primary dedup mechanism.
func (r *recorder) record(fk uint64, w witness) {
	if _, exists := r.witnesses[fk]; exists {
		return
	}
	r.witnesses[fk] = w
}

// Provenance is a per-Transform witness recorder. Passing NewProvenance()'s
// result to WithProvenance enables witness capture: after a successful
// Transform, Explain and ExplainTree resolve a derived fact back to the rule
// and body facts that produced it.
//
// A Provenance is bound to exactly one Engine (via WithProvenance) and is
// re-populated by every successful Transform that Engine's Compile result
// runs -- so if the caller runs Transform more than once (e.g. re-evaluating
// after the input database changes), the Provenance reflects only the most
// recent successful run. Callers that need to explain facts from an older
// run must snapshot or re-derive before transforming again (see the
// feature's session-cache-interaction note in doc/features/provenance.md for
// how cmd/datalog handles this by caching a Provenance alongside the
// database generation it explains).
//
// The zero value is not usable; create with NewProvenance.
type Provenance struct {
	mu sync.RWMutex

	// rules is the flat, Compile-order list of plain rules the engine was
	// compiled with -- installed once by Compile, immutable thereafter. A
	// witness's rule field indexes into this slice; stratification only ever
	// regroups rules by reference into per-stratum slices (see stratify.go),
	// so this flat slice is the one stable numbering that survives that
	// regrouping and lets Explain recover a rule's text regardless of which
	// stratum it ended up in.
	rules []syntax.Rule

	// aggRules is the flat, Compile-order list of aggregate rules -- the
	// aggregate mirror of rules above, indexed by an aggregate witness's
	// rule field (isAgg true). stratify.go's stratum.aggRuleIdx carries this
	// same flat numbering through stratification's regrouping, exactly like
	// ruleIdx does for plain rules.
	aggRules []syntax.AggregateRule

	// dict is the frozen Dict from the evaluation that produced the current
	// witnesses. It is immutable after publish (see internal/interned's
	// clone-before-publish discipline), so resolving a witness's fact keys
	// through it is safe to do lazily, long after Transform returns, without
	// copying at record time.
	dict *interned.Dict

	// witnesses is swapped in wholesale by install once a Transform
	// completes successfully; readers (Explain/ExplainTree) only ever see a
	// fully-populated map for a fully-succeeded run, never a partial one.
	witnesses map[uint64]witness

	// present is the set of fact keys the Transform actually produced (input
	// facts, asserted facts, and every derived fact) -- the produced
	// database's own hash index, swapped in atomically beside witnesses. It
	// is the authority for whether a queried fact exists at all: factKey can
	// mint a hash for any fact whose predicate and constants merely happen to
	// be interned (they get interned for unrelated reasons -- a head
	// predicate that never fired for these terms, a constant appearing in a
	// different fact), so without this membership check Explain would report
	// a fact that was never produced as ok=true/Base=true -- a base-fact
	// "leaf" that is actually a lie. Explain/ExplainTree consult it so a
	// fact absent from the produced database is a not-found (ok=false), and
	// only a genuinely-produced witness-less fact reports Base.
	present map[uint64]struct{}
}

// NewProvenance creates a disabled-until-attached witness recorder. Pass it
// to WithProvenance when building an Engine to enable witness capture for
// every Transform that Engine's compiled Transformer runs.
func NewProvenance() *Provenance {
	return &Provenance{}
}

// WithProvenance enables witness capture for every Transform produced by
// this Engine's Compile, recording derivations into p. p can then be queried
// with Explain/ExplainTree after a successful Transform.
//
// The disabled path (no WithProvenance option, or p == nil) costs exactly
// one nil check at the emit seam in eval.go's evalRules -- no allocation, no
// extra bookkeeping in join, reorder, or delta machinery.
func WithProvenance(p *Provenance) Option {
	return func(e *Engine) { e.provenance = p }
}

// install replaces p's rule lists, dict, and witness map atomically, called
// once by transformer.Transform after a Transform completes successfully.
// Nothing about a failed or in-progress Transform is ever visible through p:
// the caller's *Provenance is only mutated here, at the very end of a
// successful run.
func (p *Provenance) install(rules []syntax.Rule, aggRules []syntax.AggregateRule, dict *interned.Dict, witnesses map[uint64]witness, present map[uint64]struct{}) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.rules = rules
	p.aggRules = aggRules
	p.dict = dict
	p.witnesses = witnesses
	p.present = present
}

// ruleText renders the plain rule at the given flat index, or "" if the
// index is out of range (defensive; every witness's rule index is assigned
// from this same rules slice at record time, so out-of-range should not
// happen).
func (p *Provenance) ruleText(idx int) string {
	if idx < 0 || idx >= len(p.rules) {
		return ""
	}
	// Rule.String() renders the %% doc block above the rule (round-trip
	// fidelity); Derivation splits the two -- Rule carries bare rule text,
	// Doc carries the doc -- so strip the doc before rendering.
	r := p.rules[idx]
	r.Doc = ""
	return r.String()
}

// ruleDoc returns the plain rule's %% doc text at the given flat index,
// "" when undocumented or out of range.
func (p *Provenance) ruleDoc(idx int) string {
	if idx < 0 || idx >= len(p.rules) {
		return ""
	}
	return p.rules[idx].Doc
}

// aggRuleText renders the aggregate rule at the given flat index, the
// aggregate mirror of ruleText above.
func (p *Provenance) aggRuleText(idx int) string {
	if idx < 0 || idx >= len(p.aggRules) {
		return ""
	}
	r := p.aggRules[idx]
	r.Doc = ""
	return r.String()
}

// aggRuleDoc is ruleDoc's aggregate mirror.
func (p *Provenance) aggRuleDoc(idx int) string {
	if idx < 0 || idx >= len(p.aggRules) {
		return ""
	}
	return p.aggRules[idx].Doc
}

// Derivation is one step of a witness-provenance explanation: the resolved
// fact, the rule that derived it (rendered as text -- and, once the
// predicate-docs feature lands, as Doc), the ground detail lines for its
// non-join body items, and its resolved body facts (each itself explainable,
// recursively, via ExplainTree).
type Derivation struct {
	Fact datalog.Fact // the resolved derived fact this derivation explains

	// Base is true when Fact has no recorded witness -- it was asserted
	// directly in the ruleset or loaded from the input database. Rule, Doc,
	// Detail, and Body are all zero/empty when Base is true; there is
	// nothing further to explain.
	Base bool

	Rule string // the deriving rule's rendered text, doc block excluded (empty when Base)
	Doc  string // the deriving rule's %% doc text (syntax.Rule.Doc / AggregateRule.Doc; empty when undocumented)

	Detail []string // ground renderings of constraints, is-expressions, builtins, and negated atoms

	Body []Derivation // the resolved body facts, each explained one level; see ExplainTree for recursion/sharing/cap semantics

	// Aggregate is true when this Derivation explains an aggregate head
	// rather than a plain-rule head. GroupCount and Sampled are only
	// meaningful when Aggregate is true; Body then holds the sampled
	// contributor tuples (up to witnessSampleCap of them, see
	// aggregate.go), each itself a Derivation of the aggregate condition's
	// ground body facts under that sample's substitution -- explainable
	// recursively exactly like a plain rule's body facts.
	Aggregate bool

	// GroupCount is the aggregate group's true solution count -- e.g. "3"
	// in "S = 87 aggregated over 3 solutions" -- independent of how many
	// contributor tuples were actually sampled (Body's length). Zero when
	// Aggregate is false.
	GroupCount int

	// Sampled is true when GroupCount exceeds len(Body): the rendered
	// explanation must say "first N shown" rather than "all shown". False
	// when Aggregate is false, or when every group solution fit under
	// witnessSampleCap and Body already lists them all.
	Sampled bool

	// Repeated is true when ExplainTree has already rendered this exact fact
	// elsewhere in the tree and this node is a back-reference to it (see
	// ExplainTree's doc comment on shared subtrees). Body is empty on a
	// repeated node -- the full derivation is the earlier occurrence.
	Repeated bool

	// Truncated is true when ExplainTree stopped recursing into this node's
	// body because MaxDepth or MaxNodes was reached; Body is empty (or
	// partially filled, see ExplainTree) in that case too.
	Truncated bool
}

// Explain resolves one step of fact's derivation: the rule that produced it
// and its immediate body facts (also resolved, but not recursively
// explained -- see ExplainTree for the full tree). ok is false when fact is
// not a datalog.Fact this Provenance's most recent successful Transform
// knows about at all (neither derived-with-witness nor otherwise
// resolvable); a base fact (asserted or input) is reported with ok true and
// Base set, not as a failed lookup, since "no witness" is itself a
// meaningful answer.
func (p *Provenance) Explain(fact datalog.Fact) (Derivation, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.dict == nil {
		return Derivation{}, false
	}
	fk, ok := p.resolveKey(fact)
	if !ok {
		return Derivation{}, false
	}
	return p.explainLocked(fk, fact), true
}

// resolveKey interns fact's key under p's dict and confirms the fact was
// actually produced by the Transform (present in the produced database). A
// fact whose predicate/constants merely happen to be interned but that was
// never produced returns ok=false here, so Explain/ExplainTree report it as
// not-found rather than as a (lying) base-fact leaf -- see the present field's
// doc comment. Callers must hold p.mu.
func (p *Provenance) resolveKey(fact datalog.Fact) (uint64, bool) {
	fk, ok := p.factKey(fact)
	if !ok {
		return 0, false
	}
	if _, ok := p.present[fk]; !ok {
		return 0, false
	}
	return fk, true
}

// factKey looks up the interned fact key for fact under p's dict, without
// requiring the caller to already have interned IDs. Every term must already
// be interned (it was, if fact came from the database this Provenance's
// Transform produced); an unrecognized predicate or constant means fact
// simply isn't in this database, reported as ok=false rather than minting a
// new (and never-matching) ID.
func (p *Provenance) factKey(fact datalog.Fact) (uint64, bool) {
	predID, ok := p.dict.Has(fact.Name)
	if !ok {
		return 0, false
	}
	var f interned.InternedFact
	f.Pred = predID
	f.Arity = len(fact.Terms)
	if f.Arity > interned.MaxFactArity {
		return 0, false
	}
	for i, c := range fact.Terms {
		id, ok := p.dict.Has(interned.ConstantToAny(c))
		if !ok {
			return 0, false
		}
		f.Values[i] = id
	}
	return interned.InternedFactHash(f), true
}

// explainLocked builds a one-step Derivation for fk/fact. Callers must hold
// p.mu (for reading).
func (p *Provenance) explainLocked(fk uint64, fact datalog.Fact) Derivation {
	w, ok := p.witnesses[fk]
	if !ok {
		return Derivation{Fact: fact, Base: true}
	}
	if w.isAgg {
		return p.explainAggLocked(w, fact, func(bw bodyWitness) Derivation {
			bfact := p.dict.DeInternFact(bw.fact)
			_, hasWitness := p.witnesses[bw.key]
			return Derivation{Fact: bfact, Base: !hasWitness}
		})
	}
	body := make([]Derivation, len(w.body))
	for i, bw := range w.body {
		bfact := p.dict.DeInternFact(bw.fact)
		_, hasWitness := p.witnesses[bw.key]
		body[i] = Derivation{Fact: bfact, Base: !hasWitness}
	}
	return Derivation{
		Fact:   fact,
		Rule:   p.ruleText(w.rule),
		Doc:    p.ruleDoc(w.rule),
		Detail: w.detail,
		Body:   body,
	}
}

// explainAggLocked builds an aggregate Derivation from an aggregate witness:
// the aggregate rule's text, its group cardinality/sampling flags, and one
// child Derivation per sampled contributor solution. Each sample's own body
// facts are resolved via bodyDerive (a one-level Explain-style resolution,
// supplied by the caller so explainLocked and explainTreeLocked can each use
// their own recursion depth/sharing semantics for the sample's own body
// while still sharing this aggregate-shaping logic). A sample's several
// ground body facts don't individually correspond to one head fact the way
// a plain rule's body does, so each sample renders as a synthetic
// Derivation whose Fact is the aggregate head itself (repeated) and whose
// Body/Detail are the sample's own grounding -- see render's aggregate
// branch for how this shape prints.
func (p *Provenance) explainAggLocked(w witness, fact datalog.Fact, bodyDerive func(bodyWitness) Derivation) Derivation {
	samples := make([]Derivation, len(w.sample))
	for i, s := range w.sample {
		sb := make([]Derivation, len(s.body))
		for j, bw := range s.body {
			sb[j] = bodyDerive(bw)
		}
		samples[i] = Derivation{Fact: fact, Detail: s.detail, Body: sb}
	}
	return Derivation{
		Fact:       fact,
		Rule:       p.aggRuleText(w.rule),
		Doc:        p.aggRuleDoc(w.rule),
		Body:       samples,
		Aggregate:  true,
		GroupCount: w.groupCount,
		Sampled:    w.groupCount > len(w.sample),
	}
}

// Default caps for ExplainTree, chosen as output-size hygiene, not
// correctness: cycles are structurally impossible (a fact's witness only
// ever cites facts that were already known before it was derived), so the
// caps exist purely to keep a wide/deep derivation's rendered tree readable
// rather than to prevent runaway recursion.
const (
	defaultMaxDepth = 8
	defaultMaxNodes = 200
)

// TreeOption configures ExplainTree.
type TreeOption func(*treeConfig)

type treeConfig struct {
	maxDepth int
	maxNodes int
}

// MaxDepth caps how many levels ExplainTree recurses into a derivation's
// body before marking further nodes Truncated. The root fact is depth 0.
func MaxDepth(n int) TreeOption {
	return func(c *treeConfig) { c.maxDepth = n }
}

// MaxNodes caps the total number of Derivation nodes ExplainTree will
// materialize (including repeated-reference nodes) across the whole tree,
// regardless of depth.
func MaxNodes(n int) TreeOption {
	return func(c *treeConfig) { c.maxNodes = n }
}

// ExplainTree recursively resolves fact's full derivation tree. Shared
// subtrees -- the same fact appearing as a body fact of more than one parent
// -- render fully at their first occurrence and as a Repeated back-reference
// (Fact set, Body empty) at every later occurrence, so the walk is always
// linear in the number of distinct facts touched, never exponential in the
// number of derivation paths. Cycles cannot occur (a fact's witness only
// cites facts that existed before it was derived), so MaxDepth and MaxNodes
// are output-size caps, not safety limits: once either is reached, the
// offending node is returned with Truncated set and an empty Body instead of
// continuing to recurse.
func (p *Provenance) ExplainTree(fact datalog.Fact, opts ...TreeOption) (Derivation, bool) {
	cfg := treeConfig{maxDepth: defaultMaxDepth, maxNodes: defaultMaxNodes}
	for _, o := range opts {
		o(&cfg)
	}

	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.dict == nil {
		return Derivation{}, false
	}
	fk, ok := p.resolveKey(fact)
	if !ok {
		return Derivation{}, false
	}

	nodes := 1
	seen := map[uint64]bool{}
	return p.explainTreeLocked(fk, fact, 0, &cfg, &nodes, seen), true
}

// explainTreeLocked is ExplainTree's recursive worker. Callers must hold
// p.mu (for reading). seen tracks fact keys already fully rendered elsewhere
// in this call's tree, so a later occurrence becomes a Repeated
// back-reference instead of re-expanding (and re-counting against nodes)
// the same subtree.
func (p *Provenance) explainTreeLocked(fk uint64, fact datalog.Fact, depth int, cfg *treeConfig, nodes *int, seen map[uint64]bool) Derivation {
	w, hasWitness := p.witnesses[fk]
	if !hasWitness {
		seen[fk] = true
		return Derivation{Fact: fact, Base: true}
	}

	if seen[fk] {
		return Derivation{Fact: fact, Repeated: true}
	}
	seen[fk] = true

	if w.isAgg {
		return p.explainTreeAggLocked(w, fact, depth, cfg, nodes, seen)
	}

	d := Derivation{
		Fact:   fact,
		Rule:   p.ruleText(w.rule),
		Doc:    p.ruleDoc(w.rule),
		Detail: w.detail,
	}

	if depth >= cfg.maxDepth || *nodes >= cfg.maxNodes {
		d.Truncated = true
		return d
	}

	d.Body = make([]Derivation, 0, len(w.body))
	for _, bw := range w.body {
		if *nodes >= cfg.maxNodes {
			d.Truncated = true
			break
		}
		*nodes++
		bfact := p.dict.DeInternFact(bw.fact)
		d.Body = append(d.Body, p.explainTreeLocked(bw.key, bfact, depth+1, cfg, nodes, seen))
	}
	return d
}

// explainTreeAggLocked is explainTreeLocked's aggregate branch: it builds a
// tree node per sampled contributor solution, recursing into each sampled
// body fact's own witness (so a sampled contributor that is itself derived
// by a plain rule -- the spec's port_scan example -- expands to that rule's
// full subtree, not just a base-fact leaf) subject to the same depth/node
// caps as any other recursion.
//
// Unlike a plain rule's body (one Derivation per body atom, each a distinct
// fact), an aggregate's sample is a *tuple* of ground body facts sharing one
// substitution; there is no single "child fact" to key seen/Repeated
// tracking on for the sample itself, so each sample renders as its own
// synthetic node (Fact set to the aggregate head, Detail/Body from the
// sample) and only its constituent body facts participate in seen/Repeated
// sharing -- matching explainAggLocked's one-step shape.
func (p *Provenance) explainTreeAggLocked(w witness, fact datalog.Fact, depth int, cfg *treeConfig, nodes *int, seen map[uint64]bool) Derivation {
	d := Derivation{
		Fact:       fact,
		Rule:       p.aggRuleText(w.rule),
		Doc:        p.aggRuleDoc(w.rule),
		Aggregate:  true,
		GroupCount: w.groupCount,
		Sampled:    w.groupCount > len(w.sample),
	}

	if depth >= cfg.maxDepth || *nodes >= cfg.maxNodes {
		d.Truncated = true
		return d
	}

	d.Body = make([]Derivation, 0, len(w.sample))
	for _, s := range w.sample {
		if *nodes >= cfg.maxNodes {
			d.Truncated = true
			break
		}
		*nodes++
		sb := make([]Derivation, 0, len(s.body))
		for _, bw := range s.body {
			if *nodes >= cfg.maxNodes {
				d.Truncated = true
				break
			}
			*nodes++
			bfact := p.dict.DeInternFact(bw.fact)
			sb = append(sb, p.explainTreeLocked(bw.key, bfact, depth+1, cfg, nodes, seen))
		}
		d.Body = append(d.Body, Derivation{Fact: fact, Detail: s.detail, Body: sb})
	}
	return d
}

// --- text rendering ---

// String renders d as a multi-line unicode box-drawing tree, matching the
// form described in doc/features/provenance.md: the fact on the first line,
// then (for a non-base fact) the deriving rule, its detail lines, and its
// body facts as child branches, each recursively rendered the same way.
// Base facts render with a "[base fact]" marker and no further detail.
// Repeated/Truncated nodes are marked inline instead of re-expanding.
func (d Derivation) String() string {
	var buf strings.Builder
	d.render(&buf, "", true, true)
	return strings.TrimRight(buf.String(), "\n")
}

// render writes d to buf. prefix is the accumulated indentation for
// continuation lines; isRoot suppresses the branch glyph on the very first
// line (the root fact has no parent connector); last controls whether this
// node uses the "last child" corner (└─) or a "middle child" tee (├─) when
// it is not the root.
func (d Derivation) render(buf *strings.Builder, prefix string, isRoot bool, last bool) {
	line := d.Fact.Name + termsString(d.Fact.Terms)
	switch {
	case d.Base:
		line += "   [base fact]"
	case d.Repeated:
		line += "   [see above]"
	case d.Truncated && len(d.Body) == 0 && d.Rule == "":
		line += "   [truncated]"
	}

	if isRoot {
		buf.WriteString(line)
		buf.WriteByte('\n')
	} else {
		connector := "├─ "
		childPrefix := prefix + "│  "
		if last {
			connector = "└─ "
			childPrefix = prefix + "   "
		}
		buf.WriteString(prefix)
		buf.WriteString(connector)
		buf.WriteString(line)
		buf.WriteByte('\n')
		prefix = childPrefix
	}

	if d.Base || d.Repeated {
		return
	}

	detailPrefix := prefix
	// A documented rule cites its %% doc ahead of the rule text (the spec's
	// "explanations cite rule docs" payoff) -- the doc is the human "why",
	// the rule text the mechanical one.
	if d.Doc != "" {
		for _, docLine := range strings.Split(d.Doc, "\n") {
			buf.WriteString(prefix)
			buf.WriteString("%% ")
			buf.WriteString(docLine)
			buf.WriteByte('\n')
		}
	}
	if d.Rule != "" {
		buf.WriteString(prefix)
		buf.WriteString("rule: ")
		buf.WriteString(d.Rule)
		buf.WriteByte('\n')
	}
	for _, line := range d.Detail {
		buf.WriteString(detailPrefix)
		buf.WriteString(line)
		buf.WriteByte('\n')
	}
	if d.Aggregate {
		buf.WriteString(detailPrefix)
		buf.WriteString(aggregateSummaryLine(d))
		buf.WriteByte('\n')
	}
	if d.Truncated {
		buf.WriteString(detailPrefix)
		buf.WriteString("… truncated (depth/node limit reached) …\n")
	}

	for i, child := range d.Body {
		if d.Aggregate {
			// Each Body entry is a sample -- a tuple of ground body facts
			// sharing one substitution, not a single addressable fact (see
			// explainAggLocked) -- so it has no fact line of its own to
			// print; render its constituent body facts directly as this
			// aggregate node's children instead of nesting an extra,
			// fact-less level in the tree.
			child.renderAggSample(buf, prefix, i == len(d.Body)-1)
			continue
		}
		child.render(buf, prefix, false, i == len(d.Body)-1)
	}
}

// aggregateSummaryLine renders the spec's "S = 87 aggregated over 3
// solutions (all shown):" / "... first 10 shown" line. The aggregate
// result's rendered value isn't separately stored on Derivation (the head
// fact's own last term already carries it, per the aggregate rule's
// ResultVar convention), so this reads it off d.Fact.Terms directly, the
// same value the fact line above already displayed.
func aggregateSummaryLine(d Derivation) string {
	var buf strings.Builder
	if n := len(d.Fact.Terms); n > 0 {
		buf.WriteString(d.Fact.Terms[n-1].String())
		buf.WriteString(" aggregated over ")
	} else {
		buf.WriteString("aggregated over ")
	}
	fmt.Fprintf(&buf, "%d solution", d.GroupCount)
	if d.GroupCount != 1 {
		buf.WriteByte('s')
	}
	if d.Sampled {
		fmt.Fprintf(&buf, " (first %d shown):", len(d.Body))
	} else {
		buf.WriteString(" (all shown):")
	}
	return buf.String()
}

// renderAggSample renders one aggregate sample's constituent body facts (and
// any non-join detail lines, e.g. a constraint inside the aggregate's
// condition) as direct children of the aggregate node, skipping the
// sample's own (fact-less) line -- see render's Aggregate branch above for
// why a sample has nothing of its own to print. detail lines render first
// (mirroring buildWitness's source-order convention for a plain rule's
// body), then each sampled body fact, each of which may itself expand into
// a full subtree via ExplainTree's recursion.
func (d Derivation) renderAggSample(buf *strings.Builder, prefix string, last bool) {
	total := len(d.Detail) + len(d.Body)
	pos := 0
	for _, line := range d.Detail {
		pos++
		writeTreeLine(buf, prefix, last && pos == total, line)
	}
	for _, child := range d.Body {
		pos++
		child.render(buf, prefix, false, last && pos == total)
	}
}

// writeTreeLine writes a single non-Derivation detail line as its own tree
// leaf, using the same box-drawing connectors as render's Derivation
// children, so a sample's constraint detail lines and its body facts read
// as siblings in the rendered tree.
func writeTreeLine(buf *strings.Builder, prefix string, last bool, line string) {
	connector := "├─ "
	if last {
		connector = "└─ "
	}
	buf.WriteString(prefix)
	buf.WriteString(connector)
	buf.WriteString(line)
	buf.WriteByte('\n')
}

// termsString renders a fact's terms as "(a, b, c)" the same way
// syntax.Atom.String renders constant terms.
func termsString(terms []datalog.Constant) string {
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
