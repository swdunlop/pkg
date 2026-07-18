package main

import (
	"fmt"
	"path/filepath"

	"swdunlop.dev/pkg/datalog"
	"swdunlop.dev/pkg/datalog/jsonfacts"
	"swdunlop.dev/pkg/datalog/memory"
)

// This file implements the structured schema CRUD tools (phase 1c,
// doc/features/workbench-v2.md design decision 4: "Structured CRUD replaces
// whole-document tools" — the schema half; rules_crud.go is the rule-group
// half). It replaces the whole-document set_schema MCP tool with six keyed
// tools — put_source/delete_source, put_matcher/delete_matcher,
// put_declaration/delete_declaration — each carrying ONE item of the
// jsonfacts.Config document as structured JSON, mirroring rules_crud.go's
// shape and staleness discipline throughout:
//
//   - Keys, per the spec verbatim (design decision 4's "Keys"): a source is
//     keyed by its File; a declaration is keyed by (Name, arity=len(Terms))
//     — see schema_serialize.go's declarationKey doc comment for why arity,
//     not bare Name, disambiguates (datalog.NewDeclarationSet itself keys a
//     DeclarationSet by (Name, len(Terms)), so two declarations sharing a
//     Name at different arities are legal, pre-existing, distinct entries);
//     a matcher is keyed by (Predicate, Term, CaseInsensitive, Windash) —
//     see matcherKey.
//   - Every keyed item carries an in-memory, per-process revision exactly
//     like a rule group's (schemaRevisions below): start at 1 at load, put
//     with revision 0 means create (stale if the key exists), put with
//     revision N means edit (stale unless N equals the key's current
//     revision), delete requires the current revision. A stale rejection
//     rides the OUTPUT struct (IsStale + the current item + its revision,
//     nil Go error) for the exact reason putRuleGroupOutput's doc comment
//     gives: mcp-go's NewStructuredToolHandler discards a non-nil error's
//     TResult entirely, so there is no way to hand back structured recovery
//     data through a Go `error` return.
//   - Every write is full validate + reload, all-or-nothing (design decision
//     4's ordering, mirrored from set_schema/prepareSchema): build the
//     prospective jsonfacts.Config with the one keyed change applied,
//     serialize it deterministically (schema_serialize.go), run it through
//     prepareSchema (parse/confine/resolve-*_from/load — the SAME expensive,
//     lock-free path set_schema already used), THEN take h.mu, re-check the
//     touched key's revision (a concurrent write during the lock-free
//     prepare must turn into a stale rejection, not a lost update — see
//     schemaWriter's doc comment), write the file atomically
//     (writeFileAtomic, shared with rules_crud.go per design decision 5),
//     and swap via applySchemaLocked (the existing set_schema mechanism).
//     Only the touched part's revision is bumped.
//
// A session with no configPath (mcpHandlers.configPath, set by
// newMCPHandlers from the -c flag) rejects every schema write with
// errSchemaPathRequired, the schema-flavored twin of rules_crud.go's
// errRulesStoreRequired: there is no file to rewrite, and inventing a
// default path is exactly the kind of silent, surprising side effect this
// package avoids elsewhere (see save.go's identical refusal for the
// human-facing Save button). get_config (schema_reads.go) has no such
// restriction — a schema-less session simply reports empty sections.

// errSchemaPathRequired is returned by every schema-write tool when
// h.configPath is empty: this session was started with no -c flag (or an
// empty schema was never given a path to persist to), so there is no file a
// keyed write could rewrite. Mirrors errRulesStoreRequired's shape and
// rationale (rules_crud.go) — the SAME message for every write tool, so a
// caller learns one wording, not six.
var errSchemaPathRequired = fmt.Errorf("schema editing requires the session to be started with -c " +
	"(a schema file path); this session has no schema file to read or write")

// schemaRevisions tracks the in-memory, per-process revision counter for
// every keyed schema item currently loaded, keyed by the same key types
// schema_serialize.go defines for sorting (matcherKey, declarationKey) plus
// a bare string for sources (File is already a unique key on its own).
// Mirrors ruleStoreGroup.Revision/ruleStore.deletedHighWater's discipline
// (rulestore.go) at a finer grain: since all schema items share ONE file
// (unlike one file per rule group), the revision bookkeeping lives beside
// the session's mcpHandlers rather than inside jsonfacts.Config itself,
// which has and needs no notion of revisions.
//
// deletedHighWater fields mirror ruleStore.deletedHighWater's doc comment
// exactly (rulestore.go): a key that is deleted and later re-created
// resumes numbering from one past its own highest-ever revision, not a
// reset to 1, so a stale-write rejection a caller is still holding for the
// OLD generation of a key can never look valid again for a new generation
// created after a delete.
type schemaRevisions struct {
	sources      map[string]int
	matchers     map[matcherKey]int
	declarations map[declarationKey]int

	deletedSources      map[string]int
	deletedMatchers     map[matcherKey]int
	deletedDeclarations map[declarationKey]int
}

// newSchemaRevisions builds a fresh schemaRevisions with every key in cfg
// starting at revision 1 — the same "every part starts at 1 independently"
// convention loadRuleStore uses for rule groups (ruleStoreGroup.Revision's
// doc comment). Called once at session construction (newMCPHandlers); every
// successful write replaces h.schemaRev with a clone derived from THIS one
// (see schemaWriter.commit), preserving deletedHighWater history rather
// than rebuilding it from scratch.
func newSchemaRevisions(cfg jsonfacts.Config) *schemaRevisions {
	sr := &schemaRevisions{
		sources:             map[string]int{},
		matchers:            map[matcherKey]int{},
		declarations:        map[declarationKey]int{},
		deletedSources:      map[string]int{},
		deletedMatchers:     map[matcherKey]int{},
		deletedDeclarations: map[declarationKey]int{},
	}
	for _, s := range cfg.Sources {
		sr.sources[s.File] = 1
	}
	for _, m := range cfg.Matchers {
		sr.matchers[matcherKeyOf(m)] = 1
	}
	for _, d := range cfg.Declarations {
		sr.declarations[declarationKeyOf(d)] = 1
	}
	return sr
}

// clone returns a deep-enough copy of sr (new maps, same value contents) so
// a write's recheck step can mutate the copy without touching the live
// h.schemaRev until the write is known to succeed (see schemaWriter).
func (sr *schemaRevisions) clone() *schemaRevisions {
	out := &schemaRevisions{
		sources:             make(map[string]int, len(sr.sources)),
		matchers:            make(map[matcherKey]int, len(sr.matchers)),
		declarations:        make(map[declarationKey]int, len(sr.declarations)),
		deletedSources:      make(map[string]int, len(sr.deletedSources)),
		deletedMatchers:     make(map[matcherKey]int, len(sr.deletedMatchers)),
		deletedDeclarations: make(map[declarationKey]int, len(sr.deletedDeclarations)),
	}
	for k, v := range sr.sources {
		out.sources[k] = v
	}
	for k, v := range sr.matchers {
		out.matchers[k] = v
	}
	for k, v := range sr.declarations {
		out.declarations[k] = v
	}
	for k, v := range sr.deletedSources {
		out.deletedSources[k] = v
	}
	for k, v := range sr.deletedMatchers {
		out.deletedMatchers[k] = v
	}
	for k, v := range sr.deletedDeclarations {
		out.deletedDeclarations[k] = v
	}
	return out
}

// nextRevision computes a key's new revision on a successful write:
// current+1 for an edit, or 1 for a brand-new key, or deletedHighWater+1 if
// this key was previously deleted during this process's lifetime —
// mirroring ruleStore.deletedHighWater's doc comment (rulestore.go)
// exactly, at the per-item grain instead of per-file.
func nextRevision(existed bool, currentRevision int, deletedHighWater int) int {
	switch {
	case existed:
		return currentRevision + 1
	case deletedHighWater > 0:
		return deletedHighWater + 1
	default:
		return 1
	}
}

// -- shared write path -------------------------------------------------

// schemaWriteResult is what a successful commitSchemaWrite hands back to its
// caller: the same per-predicate fact-count feedback set_schema/
// put_rule_group already give (applySchemaLocked's own countPredicates
// call, reused rather than re-run here), and any ruleset-style warnings
// (schema writes never produce any today — jsonfacts has no doc-comment-
// warning concept — but the field exists so a future schema-level warning
// source needs no output-shape change).
type schemaWriteResult struct {
	predicates []predicateCount
	warnings   []string
}

// prepareSchemaWrite runs the expensive, lock-free half of every schema
// CRUD write (design decision 3's steps 1-2): serialize prospective
// deterministically (schema_serialize.go) and run it through prepareSchema
// (parse/confine/resolve-*_from/load), exactly like set_schema's own
// lock-free prepare phase. It touches no session state at all, so it is
// safe to call with no lock held; the caller takes h.mu afterward only for
// the cheap recheck-and-swap (see commitSchemaWrite).
//
// prospective must be built from the session's AUTHORING config (see
// session.authoringCfg's doc comment), so *_from indirections survive the
// serialize-and-rewrite; prepareSchema returns both forms back — authoring
// is the canonical re-parse of the serialized text (byte-equivalent in
// content to prospective; exactly what a restart from the written file
// would load), runtime is the resolved copy LoadFS matched against.
func (h *mcpHandlers) prepareSchemaWrite(prospective jsonfacts.Config) (text string, authoring, runtime jsonfacts.Config, db *memory.Database, err error) {
	text, err = serializeConfigYAML(prospective)
	if err != nil {
		return "", jsonfacts.Config{}, jsonfacts.Config{}, nil, fmt.Errorf("serializing schema: %w", err)
	}
	authoring, runtime, db, err = prepareSchema(text, "yaml", h.fsys, h.confine)
	if err != nil {
		return "", jsonfacts.Config{}, jsonfacts.Config{}, nil, err
	}
	return text, authoring, runtime, db, nil
}

// commitSchemaWrite is the ONE chokepoint every schema CRUD write's locked
// half funnels through (design decision 5: "Fix at the mechanism, not the
// call site" — do not write a second copy of this ordering per keyed tool).
// Callers must hold h.mu when calling this (it does not lock itself, unlike
// prepareSchemaWrite, because every caller needs to inspect/mutate other
// session-guarded state — e.g. re-deriving "does the key still exist" — in
// the SAME critical section as the recheck below).
//
// snapshotSchemaText is h.sess.schemaText AS OF the write's step-1 snapshot
// (the same h.mu critical section each put/delete_* method already takes to
// read h.sess.authoringCfg before calling prepareSchemaWrite) — the WHOLE
// document's identity at the moment this write decided what to change.
// commitSchemaWrite rejects the write (ok=false, no error) if
// h.sess.schemaText no longer equals snapshotSchemaText when the commit lock
// is finally held, even when recheck (the touched KEY's revision check)
// would otherwise pass.
//
// This closes the gap recheck alone cannot: commitSchemaWrite rewrites the
// ENTIRE serialized schema file on every write (schema_serialize.go's
// "Deterministic serialization... the WHOLE file is rewritten"), but recheck
// only re-validates the ONE key this write touched. If a vim-save
// (watch.go's reloadSchema) or a concurrent agent write changes a DIFFERENT
// item during prepareSchemaWrite's lock-free window, that item's revision is
// untouched, so recheck alone would pass — and the whole-file rewrite below
// would silently revert the concurrent edit, in memory and on disk, with no
// error at all (see TODO.md's 2026-07-18 phase 1c/1d validation entry: seed
// decl alpha, vim-edit it via reloadSchema, then an agent write adding decl
// beta from the PRE-vim-edit snapshot must not resurrect the old alpha).
// Gating on the whole document's identity, not just the touched key's
// revision, is what makes ANY concurrent schema change — touching the same
// key or a different one — turn into a clean stale rejection instead of a
// silent lost update.
//
// recheck re-validates the touched key's revision against h.sess/h.schemaRev
// as they stand RIGHT NOW (which may have changed since prepareSchemaWrite's
// lock-free window ran) and, if still valid, mutates newRevs (a clone of
// h.schemaRev — see schemaRevisions.clone) to reflect this write's outcome;
// it must NOT mutate h.schemaRev directly, so a rejected recheck (returning
// false) leaves the live state completely untouched. Returns ok=false (no
// error) when either the whole-file guard or recheck rejects — the caller
// turns that into a fresh IsStale-with-current-content output by re-reading
// h.sess/h.schemaRev itself, exactly as putRuleGroup's own staleness paths
// do; a caller cannot distinguish which guard fired, by design (both mean
// the same thing to the agent: re-read get_config and retry).
//
// On ok=true, this writes the file atomically (writeFileAtomic) and swaps
// the session via applySchemaLocked (set_schema's own mechanism), then
// installs newRevs as h.schemaRev.
func (h *mcpHandlers) commitSchemaWrite(
	snapshotSchemaText string,
	text string, authoring, runtime jsonfacts.Config, db *memory.Database,
	recheck func(newRevs *schemaRevisions) bool,
) (result schemaWriteResult, ok bool, err error) {
	if h.sess.schemaText != snapshotSchemaText {
		// The whole document moved on since this write's step-1 snapshot —
		// a vim save, another agent write, or anything else that reached
		// applySchemaLocked in between. Reject unconditionally: the touched
		// key's own revision may still look valid (recheck would pass), but
		// committing now would silently discard whatever changed the OTHER
		// parts of the document. No error, matching every other stale
		// rejection in this file: the caller re-reads get_config and retries
		// against current content.
		return schemaWriteResult{}, false, nil
	}

	newRevs := h.schemaRev.clone()
	if !recheck(newRevs) {
		return schemaWriteResult{}, false, nil
	}

	if err := writeFileAtomic(filepath.Dir(h.configPath), filepath.Base(h.configPath), text); err != nil {
		return schemaWriteResult{}, false, err
	}

	applied, err := h.applySchemaLocked(text, authoring, runtime, db)
	if err != nil {
		return schemaWriteResult{}, false, err
	}
	h.schemaRev = newRevs

	return schemaWriteResult{predicates: applied.Predicates, warnings: applied.Warnings}, true, nil
}

// -- put_source ----------------------------------------------------------

type putSourceInput struct {
	File     string              `json:"file" jsonschema:"the JSONL file this source reads, relative to the data directory; this is the source's key"`
	Mappings []jsonfacts.Mapping `json:"mappings" jsonschema:"one or more mappings from JSON fields to predicate facts (simple predicate/args/filter mode, or imperative expr mode)"`
	Revision int                 `json:"revision" jsonschema:"0 (or omitted) to create a new source; the source's CURRENT revision (from get_config) to edit an existing one — any other value is rejected as stale, with the current item/revision returned"`
}

type putSourceOutput struct {
	IsStale         bool              `json:"is_stale,omitempty"`
	CurrentSource   *jsonfacts.Source `json:"current_source,omitempty"`
	CurrentRevision int               `json:"current_revision,omitempty"`

	File       string           `json:"file,omitempty"`
	Revision   int              `json:"revision,omitempty"`
	Predicates []predicateCount `json:"predicates,omitempty"`
	Warnings   []string         `json:"warnings,omitempty"`
}

// findSource returns a pointer to the source in sources whose File matches
// file, and whether it was found.
func findSource(sources []jsonfacts.Source, file string) (*jsonfacts.Source, bool) {
	for i := range sources {
		if sources[i].File == file {
			s := sources[i]
			return &s, true
		}
	}
	return nil, false
}

// replaceSource returns a copy of sources with any existing entry keyed by
// next.File replaced (or next appended, if no such entry exists).
func replaceSource(sources []jsonfacts.Source, next jsonfacts.Source) []jsonfacts.Source {
	out := make([]jsonfacts.Source, 0, len(sources)+1)
	replaced := false
	for _, s := range sources {
		if s.File == next.File {
			out = append(out, next)
			replaced = true
			continue
		}
		out = append(out, s)
	}
	if !replaced {
		out = append(out, next)
	}
	return out
}

// removeSource returns a copy of sources with the entry keyed by file
// removed.
func removeSource(sources []jsonfacts.Source, file string) []jsonfacts.Source {
	out := make([]jsonfacts.Source, 0, len(sources))
	for _, s := range sources {
		if s.File == file {
			continue
		}
		out = append(out, s)
	}
	return out
}

// putSource creates or edits ONE source (keyed by File), all-or-nothing,
// mirroring putRuleGroup's staleness/validation/atomic-write ordering (see
// this file's header comment): staleness is checked first against the
// CURRENT in-memory config (cheap, no I/O, under h.mu), then the
// prospective whole-Config is built and run through prepareSchemaWrite
// lock-free, and only under h.mu again (commitSchemaWrite) is the key's
// revision re-checked (guarding the lock-free window), the file written,
// and the session swapped.
func (h *mcpHandlers) putSource(in putSourceInput) (putSourceOutput, error) {
	if h.configPath == "" {
		return putSourceOutput{}, errSchemaPathRequired
	}

	h.mu.Lock()
	snapshotText := h.sess.schemaText
	cfg := h.sess.authoringCfg
	current, exists := findSource(cfg.Sources, in.File)
	rev := h.schemaRev.sources[in.File]
	if (!exists && in.Revision != 0) || (exists && in.Revision != rev) {
		h.mu.Unlock()
		out := putSourceOutput{IsStale: true}
		if exists {
			out.CurrentSource, out.CurrentRevision = current, rev
		}
		return out, nil
	}
	prospective := cfg
	prospective.Sources = replaceSource(cfg.Sources, jsonfacts.Source{File: in.File, Mappings: in.Mappings})
	h.mu.Unlock()

	text, authoring, runtime, db, err := h.prepareSchemaWrite(prospective)
	if err != nil {
		return putSourceOutput{}, err
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	result, ok, err := h.commitSchemaWrite(snapshotText, text, authoring, runtime, db, func(newRevs *schemaRevisions) bool {
		_, stillExists := findSource(h.sess.authoringCfg.Sources, in.File)
		wantRev := 0
		if stillExists {
			wantRev = h.schemaRev.sources[in.File]
		}
		if wantRev != in.Revision {
			return false
		}
		newRevs.sources[in.File] = nextRevision(stillExists, in.Revision, h.schemaRev.deletedSources[in.File])
		return true
	})
	if err != nil {
		return putSourceOutput{}, err
	}
	if !ok {
		cur, stillExists := findSource(h.sess.authoringCfg.Sources, in.File)
		out := putSourceOutput{IsStale: true}
		if stillExists {
			out.CurrentSource, out.CurrentRevision = cur, h.schemaRev.sources[in.File]
		}
		return out, nil
	}

	return putSourceOutput{
		File: in.File, Revision: h.schemaRev.sources[in.File],
		Predicates: result.predicates, Warnings: result.warnings,
	}, nil
}

// -- delete_source ---------------------------------------------------------

type deleteSourceInput struct {
	File     string `json:"file" jsonschema:"the source's file key (see get_config)"`
	Revision int    `json:"revision" jsonschema:"the source's CURRENT revision; any other value, or naming a source that does not exist, is rejected as stale"`
}

type deleteSourceOutput struct {
	IsStale         bool              `json:"is_stale,omitempty"`
	CurrentSource   *jsonfacts.Source `json:"current_source,omitempty"`
	CurrentRevision int               `json:"current_revision,omitempty"`

	File       string           `json:"file,omitempty"`
	Predicates []predicateCount `json:"predicates,omitempty"`
}

func (h *mcpHandlers) deleteSource(in deleteSourceInput) (deleteSourceOutput, error) {
	if h.configPath == "" {
		return deleteSourceOutput{}, errSchemaPathRequired
	}

	h.mu.Lock()
	snapshotText := h.sess.schemaText
	cfg := h.sess.authoringCfg
	current, exists := findSource(cfg.Sources, in.File)
	if !exists || in.Revision != h.schemaRev.sources[in.File] {
		out := deleteSourceOutput{IsStale: true}
		if exists {
			out.CurrentSource, out.CurrentRevision = current, h.schemaRev.sources[in.File]
		}
		h.mu.Unlock()
		return out, nil
	}
	prospective := cfg
	prospective.Sources = removeSource(cfg.Sources, in.File)
	h.mu.Unlock()

	text, authoring, runtime, db, err := h.prepareSchemaWrite(prospective)
	if err != nil {
		return deleteSourceOutput{}, err
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	result, ok, err := h.commitSchemaWrite(snapshotText, text, authoring, runtime, db, func(newRevs *schemaRevisions) bool {
		_, stillExists := findSource(h.sess.authoringCfg.Sources, in.File)
		if !stillExists || in.Revision != h.schemaRev.sources[in.File] {
			return false
		}
		if in.Revision > newRevs.deletedSources[in.File] {
			newRevs.deletedSources[in.File] = in.Revision
		}
		delete(newRevs.sources, in.File)
		return true
	})
	if err != nil {
		return deleteSourceOutput{}, err
	}
	if !ok {
		cur, stillExists := findSource(h.sess.authoringCfg.Sources, in.File)
		out := deleteSourceOutput{IsStale: true}
		if stillExists {
			out.CurrentSource, out.CurrentRevision = cur, h.schemaRev.sources[in.File]
		}
		return out, nil
	}

	return deleteSourceOutput{File: in.File, Predicates: result.predicates}, nil
}

// -- put_matcher -----------------------------------------------------------

type putMatcherInput struct {
	jsonfacts.Matcher
	Revision int `json:"revision" jsonschema:"0 (or omitted) to create a new matcher; the matcher's CURRENT revision (from get_config) to edit an existing one — any other value is rejected as stale"`
}

type putMatcherOutput struct {
	IsStale         bool               `json:"is_stale,omitempty"`
	CurrentMatcher  *jsonfacts.Matcher `json:"current_matcher,omitempty"`
	CurrentRevision int                `json:"current_revision,omitempty"`

	Revision   int              `json:"revision,omitempty"`
	Predicates []predicateCount `json:"predicates,omitempty"`
	Warnings   []string         `json:"warnings,omitempty"`
}

func findMatcher(matchers []jsonfacts.Matcher, key matcherKey) (*jsonfacts.Matcher, bool) {
	for i := range matchers {
		if matcherKeyOf(matchers[i]) == key {
			m := matchers[i]
			return &m, true
		}
	}
	return nil, false
}

// replaceMatcher returns a copy of matchers with any existing entry keyed
// by key replaced by next (or next appended, if no such entry exists).
func replaceMatcher(matchers []jsonfacts.Matcher, key matcherKey, next jsonfacts.Matcher) []jsonfacts.Matcher {
	out := make([]jsonfacts.Matcher, 0, len(matchers)+1)
	replaced := false
	for _, m := range matchers {
		if matcherKeyOf(m) == key {
			out = append(out, next)
			replaced = true
			continue
		}
		out = append(out, m)
	}
	if !replaced {
		out = append(out, next)
	}
	return out
}

func removeMatcher(matchers []jsonfacts.Matcher, key matcherKey) []jsonfacts.Matcher {
	out := make([]jsonfacts.Matcher, 0, len(matchers))
	for _, m := range matchers {
		if matcherKeyOf(m) == key {
			continue
		}
		out = append(out, m)
	}
	return out
}

// putMatcher creates or edits ONE matcher, keyed by (Predicate, Term,
// CaseInsensitive, Windash) — design decision 1's matcher key. Because
// those four fields ARE the key, editing one of them (e.g. changing Term)
// targets a DIFFERENT key than the one Revision was checked against; this
// mirrors put_rule_group's own head/arity semantics (editing a rule
// group's head/arity is not expressible as an "edit" either — it is a
// different group). A caller that wants to change a matcher's key fields
// deletes the old key and creates the new one.
func (h *mcpHandlers) putMatcher(in putMatcherInput) (putMatcherOutput, error) {
	if h.configPath == "" {
		return putMatcherOutput{}, errSchemaPathRequired
	}
	key := matcherKeyOf(in.Matcher)

	h.mu.Lock()
	snapshotText := h.sess.schemaText
	cfg := h.sess.authoringCfg
	current, exists := findMatcher(cfg.Matchers, key)
	rev := h.schemaRev.matchers[key]
	if (!exists && in.Revision != 0) || (exists && in.Revision != rev) {
		h.mu.Unlock()
		out := putMatcherOutput{IsStale: true}
		if exists {
			out.CurrentMatcher, out.CurrentRevision = current, rev
		}
		return out, nil
	}
	prospective := cfg
	prospective.Matchers = replaceMatcher(cfg.Matchers, key, in.Matcher)
	h.mu.Unlock()

	text, authoring, runtime, db, err := h.prepareSchemaWrite(prospective)
	if err != nil {
		return putMatcherOutput{}, err
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	result, ok, err := h.commitSchemaWrite(snapshotText, text, authoring, runtime, db, func(newRevs *schemaRevisions) bool {
		_, stillExists := findMatcher(h.sess.authoringCfg.Matchers, key)
		wantRev := 0
		if stillExists {
			wantRev = h.schemaRev.matchers[key]
		}
		if wantRev != in.Revision {
			return false
		}
		newRevs.matchers[key] = nextRevision(stillExists, in.Revision, h.schemaRev.deletedMatchers[key])
		return true
	})
	if err != nil {
		return putMatcherOutput{}, err
	}
	if !ok {
		cur, stillExists := findMatcher(h.sess.authoringCfg.Matchers, key)
		out := putMatcherOutput{IsStale: true}
		if stillExists {
			out.CurrentMatcher, out.CurrentRevision = cur, h.schemaRev.matchers[key]
		}
		return out, nil
	}

	return putMatcherOutput{
		Revision: h.schemaRev.matchers[key], Predicates: result.predicates, Warnings: result.warnings,
	}, nil
}

// -- delete_matcher ----------------------------------------------------------

type deleteMatcherInput struct {
	Predicate       string `json:"predicate" jsonschema:"the matcher's predicate (part of its key)"`
	Term            int    `json:"term" jsonschema:"the matcher's term index (part of its key)"`
	CaseInsensitive bool   `json:"case_insensitive,omitempty" jsonschema:"part of the matcher's key"`
	Windash         bool   `json:"windash,omitempty" jsonschema:"part of the matcher's key"`
	Revision        int    `json:"revision" jsonschema:"the matcher's CURRENT revision; any other value, or naming a matcher that does not exist, is rejected as stale"`
}

type deleteMatcherOutput struct {
	IsStale         bool               `json:"is_stale,omitempty"`
	CurrentMatcher  *jsonfacts.Matcher `json:"current_matcher,omitempty"`
	CurrentRevision int                `json:"current_revision,omitempty"`

	Predicates []predicateCount `json:"predicates,omitempty"`
}

func (h *mcpHandlers) deleteMatcher(in deleteMatcherInput) (deleteMatcherOutput, error) {
	if h.configPath == "" {
		return deleteMatcherOutput{}, errSchemaPathRequired
	}
	key := matcherKey{Predicate: in.Predicate, Term: in.Term, CaseInsensitive: in.CaseInsensitive, Windash: in.Windash}

	h.mu.Lock()
	snapshotText := h.sess.schemaText
	cfg := h.sess.authoringCfg
	current, exists := findMatcher(cfg.Matchers, key)
	if !exists || in.Revision != h.schemaRev.matchers[key] {
		out := deleteMatcherOutput{IsStale: true}
		if exists {
			out.CurrentMatcher, out.CurrentRevision = current, h.schemaRev.matchers[key]
		}
		h.mu.Unlock()
		return out, nil
	}
	prospective := cfg
	prospective.Matchers = removeMatcher(cfg.Matchers, key)
	h.mu.Unlock()

	text, authoring, runtime, db, err := h.prepareSchemaWrite(prospective)
	if err != nil {
		return deleteMatcherOutput{}, err
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	result, ok, err := h.commitSchemaWrite(snapshotText, text, authoring, runtime, db, func(newRevs *schemaRevisions) bool {
		_, stillExists := findMatcher(h.sess.authoringCfg.Matchers, key)
		if !stillExists || in.Revision != h.schemaRev.matchers[key] {
			return false
		}
		if in.Revision > newRevs.deletedMatchers[key] {
			newRevs.deletedMatchers[key] = in.Revision
		}
		delete(newRevs.matchers, key)
		return true
	})
	if err != nil {
		return deleteMatcherOutput{}, err
	}
	if !ok {
		cur, stillExists := findMatcher(h.sess.authoringCfg.Matchers, key)
		out := deleteMatcherOutput{IsStale: true}
		if stillExists {
			out.CurrentMatcher, out.CurrentRevision = cur, h.schemaRev.matchers[key]
		}
		return out, nil
	}

	return deleteMatcherOutput{Predicates: result.predicates}, nil
}

// -- put_declaration ---------------------------------------------------------

type putDeclarationInput struct {
	datalog.Declaration
	Revision int `json:"revision" jsonschema:"0 (or omitted) to create a new declaration; the declaration's CURRENT revision (from get_config) to edit an existing one — any other value is rejected as stale"`
}

type putDeclarationOutput struct {
	IsStale            bool                 `json:"is_stale,omitempty"`
	CurrentDeclaration *datalog.Declaration `json:"current_declaration,omitempty"`
	CurrentRevision    int                  `json:"current_revision,omitempty"`

	Revision   int              `json:"revision,omitempty"`
	Predicates []predicateCount `json:"predicates,omitempty"`
	Warnings   []string         `json:"warnings,omitempty"`
}

func findDeclaration(decls []datalog.Declaration, key declarationKey) (*datalog.Declaration, bool) {
	for i := range decls {
		if declarationKeyOf(decls[i]) == key {
			d := decls[i]
			return &d, true
		}
	}
	return nil, false
}

func replaceDeclaration(decls []datalog.Declaration, key declarationKey, next datalog.Declaration) []datalog.Declaration {
	out := make([]datalog.Declaration, 0, len(decls)+1)
	replaced := false
	for _, d := range decls {
		if declarationKeyOf(d) == key {
			out = append(out, next)
			replaced = true
			continue
		}
		out = append(out, d)
	}
	if !replaced {
		out = append(out, next)
	}
	return out
}

func removeDeclaration(decls []datalog.Declaration, key declarationKey) []datalog.Declaration {
	out := make([]datalog.Declaration, 0, len(decls))
	for _, d := range decls {
		if declarationKeyOf(d) == key {
			continue
		}
		out = append(out, d)
	}
	return out
}

// putDeclaration creates or edits ONE declaration, keyed by (Name,
// arity=len(Terms)) — see schema_serialize.go's declarationKey doc comment
// for why arity disambiguates. As with putMatcher, changing Terms' length
// targets a different key than the one Revision was checked against; a
// caller that wants to change a declaration's arity deletes the old one and
// creates the new one.
func (h *mcpHandlers) putDeclaration(in putDeclarationInput) (putDeclarationOutput, error) {
	if h.configPath == "" {
		return putDeclarationOutput{}, errSchemaPathRequired
	}
	key := declarationKeyOf(in.Declaration)

	h.mu.Lock()
	snapshotText := h.sess.schemaText
	cfg := h.sess.authoringCfg
	current, exists := findDeclaration(cfg.Declarations, key)
	rev := h.schemaRev.declarations[key]
	if (!exists && in.Revision != 0) || (exists && in.Revision != rev) {
		h.mu.Unlock()
		out := putDeclarationOutput{IsStale: true}
		if exists {
			out.CurrentDeclaration, out.CurrentRevision = current, rev
		}
		return out, nil
	}
	prospective := cfg
	prospective.Declarations = replaceDeclaration(cfg.Declarations, key, in.Declaration)
	h.mu.Unlock()

	text, authoring, runtime, db, err := h.prepareSchemaWrite(prospective)
	if err != nil {
		return putDeclarationOutput{}, err
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	result, ok, err := h.commitSchemaWrite(snapshotText, text, authoring, runtime, db, func(newRevs *schemaRevisions) bool {
		_, stillExists := findDeclaration(h.sess.authoringCfg.Declarations, key)
		wantRev := 0
		if stillExists {
			wantRev = h.schemaRev.declarations[key]
		}
		if wantRev != in.Revision {
			return false
		}
		newRevs.declarations[key] = nextRevision(stillExists, in.Revision, h.schemaRev.deletedDeclarations[key])
		return true
	})
	if err != nil {
		return putDeclarationOutput{}, err
	}
	if !ok {
		cur, stillExists := findDeclaration(h.sess.authoringCfg.Declarations, key)
		out := putDeclarationOutput{IsStale: true}
		if stillExists {
			out.CurrentDeclaration, out.CurrentRevision = cur, h.schemaRev.declarations[key]
		}
		return out, nil
	}

	return putDeclarationOutput{
		Revision: h.schemaRev.declarations[key], Predicates: result.predicates, Warnings: result.warnings,
	}, nil
}

// -- delete_declaration -------------------------------------------------------

type deleteDeclarationInput struct {
	Name     string `json:"name" jsonschema:"the declaration's predicate name (part of its key)"`
	Arity    int    `json:"arity" jsonschema:"the declaration's arity, i.e. len(terms) (part of its key)"`
	Revision int    `json:"revision" jsonschema:"the declaration's CURRENT revision; any other value, or naming a declaration that does not exist, is rejected as stale"`
}

type deleteDeclarationOutput struct {
	IsStale            bool                 `json:"is_stale,omitempty"`
	CurrentDeclaration *datalog.Declaration `json:"current_declaration,omitempty"`
	CurrentRevision    int                  `json:"current_revision,omitempty"`

	Predicates []predicateCount `json:"predicates,omitempty"`
}

func (h *mcpHandlers) deleteDeclaration(in deleteDeclarationInput) (deleteDeclarationOutput, error) {
	if h.configPath == "" {
		return deleteDeclarationOutput{}, errSchemaPathRequired
	}
	key := declarationKey{Name: in.Name, Arity: in.Arity}

	h.mu.Lock()
	snapshotText := h.sess.schemaText
	cfg := h.sess.authoringCfg
	current, exists := findDeclaration(cfg.Declarations, key)
	if !exists || in.Revision != h.schemaRev.declarations[key] {
		out := deleteDeclarationOutput{IsStale: true}
		if exists {
			out.CurrentDeclaration, out.CurrentRevision = current, h.schemaRev.declarations[key]
		}
		h.mu.Unlock()
		return out, nil
	}
	prospective := cfg
	prospective.Declarations = removeDeclaration(cfg.Declarations, key)
	h.mu.Unlock()

	text, authoring, runtime, db, err := h.prepareSchemaWrite(prospective)
	if err != nil {
		return deleteDeclarationOutput{}, err
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	result, ok, err := h.commitSchemaWrite(snapshotText, text, authoring, runtime, db, func(newRevs *schemaRevisions) bool {
		_, stillExists := findDeclaration(h.sess.authoringCfg.Declarations, key)
		if !stillExists || in.Revision != h.schemaRev.declarations[key] {
			return false
		}
		if in.Revision > newRevs.deletedDeclarations[key] {
			newRevs.deletedDeclarations[key] = in.Revision
		}
		delete(newRevs.declarations, key)
		return true
	})
	if err != nil {
		return deleteDeclarationOutput{}, err
	}
	if !ok {
		cur, stillExists := findDeclaration(h.sess.authoringCfg.Declarations, key)
		out := deleteDeclarationOutput{IsStale: true}
		if stillExists {
			out.CurrentDeclaration, out.CurrentRevision = cur, h.schemaRev.declarations[key]
		}
		return out, nil
	}

	return deleteDeclarationOutput{Predicates: result.predicates}, nil
}
