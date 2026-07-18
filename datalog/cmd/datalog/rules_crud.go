package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"swdunlop.dev/pkg/datalog/seminaive"
	"swdunlop.dev/pkg/datalog/syntax"
)

// This file implements the structured rule-group CRUD tools (phase 1b,
// doc/features/workbench-v2.md design decision 4: "Structured CRUD replaces
// whole-document tools"). It builds directly on rulestore.go's ruleStore/
// ruleStoreGroup (phase 1a) — per-group Text was already isolated there, so
// this task only adds revision tracking (rulestore.go's ruleStoreGroup.Revision/
// ruleStore.nextRevision) and the four tool methods below.
//
// errRulesStoreRequired is the ONE message every read/write CRUD tool
// returns when h.rules is nil (mcpHandlers.rules's doc comment: nil means
// this session was started with legacy positional rule files, or no rules
// at all, not a rules/ directory). Design decision 6 ("No store, no
// writes") requires reads to error identically to writes here — a caller
// must not learn "the four write tools are unavailable" from four different
// wordings, and a legacy session's whole-document rulesText has no group
// boundaries to list/get in the first place.
var errRulesStoreRequired = fmt.Errorf("rule-group editing requires the session to be started with --rules " +
	"(a rules/ directory store); this session has no rules directory to read or write group files from")

// -- list_rule_groups -------------------------------------------------------

type listRuleGroupsInput struct{}

type ruleGroupSummary struct {
	Head       string `json:"head"`
	Arity      int    `json:"arity"`
	File       string `json:"file"`
	Revision   int    `json:"revision"`
	Statements int    `json:"statements"`
}

type listRuleGroupsOutput struct {
	Groups []ruleGroupSummary `json:"groups"`
}

// listRuleGroups returns every group in filename order (design decision 5:
// "list_rule_groups returns [{head, arity, file, revision, statements}] in
// filename order"), so a caller browsing the store sees the same ordering a
// directory listing (or the concatenated export()) would show.
//
// Reads h.rules with NO lock of its own — callers must hold h.mu. Production
// is race-clean today because every registered caller (mcp.go's dispatch
// wrappers) already holds h.mu, and the watcher's reloadRules swap
// (watch.go) also runs under h.mu, but a future direct caller that skips the
// dispatch wrapper would race that swap. Do not add locking inside this
// method: the dispatch wrappers already hold h.mu, and a lock here would
// double-lock and deadlock against them.
func (h *mcpHandlers) listRuleGroups(_ listRuleGroupsInput) (listRuleGroupsOutput, error) {
	if h.rules == nil {
		return listRuleGroupsOutput{}, errRulesStoreRequired
	}
	out := make([]ruleGroupSummary, 0, len(h.rules.Order))
	for _, k := range h.rules.Order {
		g := h.rules.Groups[k]
		out = append(out, ruleGroupSummary{
			Head:       k.Head,
			Arity:      k.Arity,
			File:       g.File,
			Revision:   g.Revision,
			Statements: len(g.Rules) + len(g.AggRules),
		})
	}
	return listRuleGroupsOutput{Groups: out}, nil
}

// -- get_rule_group -----------------------------------------------------

type getRuleGroupInput struct {
	Head  string `json:"head" jsonschema:"the rule group's head predicate name"`
	Arity int    `json:"arity" jsonschema:"the rule group's head arity"`
}

type getRuleGroupOutput struct {
	Head     string `json:"head"`
	Arity    int    `json:"arity"`
	File     string `json:"file"`
	Revision int    `json:"revision"`
	Text     string `json:"text"`
}

// getRuleGroup returns one group's verbatim on-disk text plus its current
// revision — the shape a caller needs to build a put_rule_group edit (design
// decision 5: "get_rule_group returns {head, arity, file, revision, text}
// with text = the verbatim on-disk content").
//
// Reads h.rules with NO lock of its own — callers must hold h.mu, exactly
// like listRuleGroups above (see its doc comment for the caller-holds-h.mu
// contract and why no lock belongs inside this method).
func (h *mcpHandlers) getRuleGroup(in getRuleGroupInput) (getRuleGroupOutput, error) {
	if h.rules == nil {
		return getRuleGroupOutput{}, errRulesStoreRequired
	}
	key := groupKey{Head: in.Head, Arity: in.Arity}
	g, ok := h.rules.Groups[key]
	if !ok {
		return getRuleGroupOutput{}, fmt.Errorf("get_rule_group: no rule group %s/%d (call list_rule_groups to see what exists)",
			in.Head, in.Arity)
	}
	return getRuleGroupOutput{Head: in.Head, Arity: in.Arity, File: g.File, Revision: g.Revision, Text: g.Text}, nil
}

// -- put_rule_group -----------------------------------------------------

type putRuleGroupInput struct {
	Head     string `json:"head" jsonschema:"the rule group's head predicate name; text must define exactly this head"`
	Arity    int    `json:"arity" jsonschema:"the rule group's head arity; text must define exactly this arity"`
	Text     string `json:"text" jsonschema:"the complete .dl text for this ONE rule group (all statements must share this head/arity); verbatim except a trailing newline is added if missing"`
	Revision int    `json:"revision" jsonschema:"0 (or omitted) to create a new group; the group's CURRENT revision (from list_rule_groups/get_rule_group) to edit an existing one — any other value is rejected as stale, with the current text/revision returned"`
}

type putRuleGroupOutput struct {
	// IsStale is true when the call was rejected because in.Revision did not
	// match reality: either the group doesn't exist and Revision was
	// non-zero, or the group exists and Revision didn't match its current
	// value. See this file's header comment for why this rejection rides in
	// the OUTPUT struct (with the handler returning a nil error) rather than
	// through mcp-go's Go `error` return.
	IsStale bool `json:"is_stale,omitempty"`

	// CurrentText/CurrentRevision are populated only when IsStale is true,
	// and only when the group already exists (a stale create attempt against
	// a NON-existent group has no "current" content to hand back — see
	// putRuleGroup's doc comment). They give the caller exactly what it
	// needs to retry: the content it should have based its edit on, and the
	// revision to submit next time.
	CurrentText     string `json:"current_text,omitempty"`
	CurrentRevision int    `json:"current_revision,omitempty"`

	Head       string           `json:"head,omitempty"`
	Arity      int              `json:"arity,omitempty"`
	Revision   int              `json:"revision,omitempty"`
	File       string           `json:"file,omitempty"`
	Predicates []predicateCount `json:"predicates,omitempty"`
	Warnings   []string         `json:"warnings,omitempty"`
}

// putRuleGroup creates or edits ONE rule group, all-or-nothing, per design
// decision 2's ordering:
//
//  1. parse Text via parseUserProgram (the shared _q_-reservation /
//     detached-doc gate every program-ingest surface uses);
//  2. splitRuleset must yield EXACTLY one group, whose key must equal
//     {Head, Arity} — a Text that defines a different head, mixes heads, or
//     embeds a query is rejected before anything else happens;
//  3. staleness check (see below) — done AFTER parsing so a bad Text is
//     reported as a parse error even against a stale revision (a caller
//     fixing a parse error should not also have to worry about revision
//     races in the same round trip);
//  4. trial-Compile the FULL prospective ruleset (every other group's
//     Rules/AggRules, unchanged, plus this group's freshly parsed
//     statements) — a group that breaks stratification/safety ANYWHERE is
//     refused here, before any disk write;
//  5. only then: write the file (temp-in-same-dir + rename, see
//     writeFileAtomic) and rebuild the session via session.loadRuleStore.
//
// Staleness (design decision 1): if the group does not exist, Revision must
// be 0 (create); if it exists, Revision must equal its CURRENT revision
// (edit). Any other value is a rejection, not a Go error — mcp-go's
// NewStructuredToolHandler serializes a non-nil error as a plain text
// string with no room for structured fields (confirmed by reading the
// vendored handler: on error it returns NewToolResultError(...), discarding
// the TResult value entirely), so there is no way for a caller to recover
// CurrentText/CurrentRevision from an `error` return. Instead, a stale write
// is reported as a normal (nil-error) putRuleGroupOutput with IsStale=true
// and the current content filled in — the model sees structured JSON it can
// act on (re-fetch, or retry with the right revision) rather than a bare
// error string. A create attempt (Revision==0) against an EXISTING group is
// also staleness (someone else created it first) and gets the same
// treatment, with current text/revision filled in exactly as an edit
// conflict would. The one staleness case with nothing to hand back is an
// EDIT attempt (Revision!=0) against a group that does NOT exist: there is
// no "current text" for a group that isn't there, so CurrentText/
// CurrentRevision stay zero — the model should call list_rule_groups to
// re-orient, and IsStale still distinguishes this from a validation error.
func (h *mcpHandlers) putRuleGroup(in putRuleGroupInput) (putRuleGroupOutput, error) {
	if h.rules == nil {
		return putRuleGroupOutput{}, errRulesStoreRequired
	}

	ruleset, err := parseUserProgram(in.Text)
	if err != nil {
		return putRuleGroupOutput{}, err
	}
	groups, order, err := splitRuleset(ruleset)
	if err != nil {
		return putRuleGroupOutput{}, err
	}
	wantKey := groupKey{Head: in.Head, Arity: in.Arity}
	if len(order) == 0 {
		return putRuleGroupOutput{}, fmt.Errorf("put_rule_group: text has no rules or facts")
	}
	if len(order) > 1 || order[0] != wantKey {
		return putRuleGroupOutput{}, fmt.Errorf("put_rule_group: every statement in text must have head "+
			"%s/%d (the head/arity arguments); found %v", in.Head, in.Arity, headsOf(order))
	}
	newGroup := groups[wantKey]
	// verbatimText is what actually lands on disk and in the store's Text
	// field: in.Text as the caller wrote it (trailing newline(s) trimmed,
	// matching loadRuleStore's own convention for on-disk content), NOT
	// newGroup.Text (splitRuleset/renderGroupText's re-rendered canonical
	// form, which normalizes statement separators to a single '\n' — that
	// form is Import/Export-only, per rulestore.go's doc comments). Design
	// decision 2: "the text lands on disk verbatim" — an agent's exact
	// formatting, comments, and blank lines survive a put_rule_group call
	// exactly as they would a vim save.
	verbatimText := strings.TrimRight(in.Text, "\n")

	current, exists := h.rules.Groups[wantKey]
	switch {
	case !exists && in.Revision != 0:
		// Create attempt with a non-zero revision: nothing to have based that
		// revision on. No current content to hand back — see doc comment.
		return putRuleGroupOutput{IsStale: true}, nil
	case exists && in.Revision != current.Revision:
		return putRuleGroupOutput{
			IsStale:         true,
			CurrentText:     current.Text,
			CurrentRevision: current.Revision,
		}, nil
	}

	// A CREATE must not introduce a key whose filename folds onto an existing
	// group's under case-insensitivity ({Foo,1} beside {foo,1}): on a
	// case-insensitive filesystem (macOS/Windows) writeFileAtomic's rename
	// would silently clobber the OTHER group's file while both stayed live in
	// memory — the same hazard splitRuleset/loadRuleStore already reject at
	// import/load time (rulestore.go's checkFilenameCollisions), enforced
	// here through the same shared check so the CRUD surface cannot become
	// the one path that forgets it. Edits are naturally exempt (the key
	// already exists; checkFilenameCollisions ignores equal keys).
	if err := checkFilenameCollisions(append(append([]groupKey{}, h.rules.Order...), wantKey)); err != nil {
		return putRuleGroupOutput{}, fmt.Errorf("put_rule_group: %w", err)
	}

	// Trial-compile the FULL prospective ruleset: every OTHER group's parsed
	// Rules/AggRules untouched, plus this group's freshly parsed statements —
	// so a group that compiles in isolation but breaks stratification/safety
	// against the rest of the store is refused before any disk write.
	prospective := syntax.Ruleset{}
	for _, k := range h.rules.Order {
		if k == wantKey {
			continue // superseded by newGroup below
		}
		g := h.rules.Groups[k]
		prospective.Rules = append(prospective.Rules, g.Rules...)
		prospective.AggRules = append(prospective.AggRules, g.AggRules...)
	}
	prospective.Rules = append(prospective.Rules, newGroup.Rules...)
	prospective.AggRules = append(prospective.AggRules, newGroup.AggRules...)

	if _, err := seminaive.New(h.sess.engineOpts...).Compile(prospective); err != nil {
		return putRuleGroupOutput{}, fmt.Errorf("put_rule_group: %s/%d compiles alone but breaks the full ruleset: %w",
			in.Head, in.Arity, err)
	}

	// Validation is complete — only now does anything touch disk or memory.
	// newRevision starts at 1 for a brand-new key (matching loadRuleStore's
	// own per-group start-at-1), or bumps from the group's current revision
	// for an edit. A key that was previously deleted resumes from one past
	// its own highest-ever revision (h.rules.deletedHighWater) rather than
	// resetting to 1 — see ruleStore.deletedHighWater's doc comment for why.
	newRevision := 1
	switch {
	case exists:
		newRevision = current.Revision + 1
	case h.rules.deletedHighWater[wantKey] > 0:
		newRevision = h.rules.deletedHighWater[wantKey] + 1
	}

	if err := writeFileAtomic(h.rules.Dir, wantKey.filename(), verbatimText); err != nil {
		return putRuleGroupOutput{}, err
	}

	isCreate := !exists
	h.rules.Groups[wantKey] = &ruleStoreGroup{
		Key:      wantKey,
		File:     wantKey.filename(),
		Text:     verbatimText,
		Rules:    newGroup.Rules,
		AggRules: newGroup.AggRules,
		Revision: newRevision,
	}
	if isCreate {
		h.rules.Order = append(h.rules.Order, wantKey)
		sort.Slice(h.rules.Order, func(i, j int) bool {
			return h.rules.Order[i].filename() < h.rules.Order[j].filename()
		})
	}

	if err := h.sess.loadRuleStore(h.rules); err != nil {
		// The store's in-memory Groups/Order were already updated above and
		// the file is already on disk — loadRuleStore failing here would mean
		// the trial-Compile (against the SAME prospective ruleset structure)
		// somehow disagreed with the real rebuild, which should not happen
		// given both build the identical set of rules/aggRules. Surfacing the
		// error rather than silently swallowing it is still the right
		// choice: an operator needs to know the session is now in a
		// last-good-attempt state relative to the store.
		return putRuleGroupOutput{}, fmt.Errorf("put_rule_group: wrote %s but failed to reload the session: %w",
			wantKey.filename(), err)
	}

	full, err := h.sess.buildDB()
	if err != nil {
		return putRuleGroupOutput{}, err
	}
	if h.onChange != nil {
		h.onChange()
	}

	return putRuleGroupOutput{
		Head:       in.Head,
		Arity:      in.Arity,
		Revision:   newRevision,
		File:       wantKey.filename(),
		Predicates: countPredicates(full),
		Warnings:   ruleset.Warnings,
	}, nil
}

// -- delete_rule_group ----------------------------------------------------

type deleteRuleGroupInput struct {
	Head     string `json:"head" jsonschema:"the rule group's head predicate name"`
	Arity    int    `json:"arity" jsonschema:"the rule group's head arity"`
	Revision int    `json:"revision" jsonschema:"the group's CURRENT revision (from list_rule_groups/get_rule_group); any other value is rejected as stale, with the current text/revision returned"`
}

type deleteRuleGroupOutput struct {
	// IsStale/CurrentText/CurrentRevision mirror putRuleGroupOutput's fields
	// exactly — see that type's doc comment. A delete against a group that
	// does not exist at all is ALSO reported as stale (there is nothing to
	// delete, and the caller's Revision could not possibly have been current
	// against nothing), with no current content to hand back.
	IsStale         bool   `json:"is_stale,omitempty"`
	CurrentText     string `json:"current_text,omitempty"`
	CurrentRevision int    `json:"current_revision,omitempty"`

	Head       string           `json:"head,omitempty"`
	Arity      int              `json:"arity,omitempty"`
	File       string           `json:"file,omitempty"`
	Predicates []predicateCount `json:"predicates,omitempty"`
}

// deleteRuleGroup removes one rule group's file and its entry from the
// store, with the same staleness discipline as putRuleGroup (design
// decision 1): Revision must equal the group's CURRENT revision, or the call
// is rejected (IsStale=true, current content attached when the group
// exists) rather than erroring. Unlike putRuleGroup, there is no
// trial-Compile step here — REMOVING a rule group can only shrink the
// stratification graph, never introduce a new cycle or safety violation, so
// there is nothing a full-ruleset trial compile could catch that the
// deletion itself would not already fix. (A dangling reference from another
// group's body to the now-gone predicate is not an error: an undefined
// predicate is always zero facts, per this dialect's existing "unknown
// predicate is 0 rows, not an error" convention — see mcpDialectPrimer.)
func (h *mcpHandlers) deleteRuleGroup(in deleteRuleGroupInput) (deleteRuleGroupOutput, error) {
	if h.rules == nil {
		return deleteRuleGroupOutput{}, errRulesStoreRequired
	}

	key := groupKey{Head: in.Head, Arity: in.Arity}
	current, exists := h.rules.Groups[key]
	if !exists || in.Revision != current.Revision {
		out := deleteRuleGroupOutput{IsStale: true}
		if exists {
			out.CurrentText = current.Text
			out.CurrentRevision = current.Revision
		}
		return out, nil
	}

	full := filepath.Join(h.rules.Dir, current.File)
	if err := os.Remove(full); err != nil {
		return deleteRuleGroupOutput{}, fmt.Errorf("rules store: removing %s: %w", full, err)
	}

	// Record this key's high-water revision BEFORE removing it from Groups,
	// so a later re-create (putRuleGroup) resumes numbering past it instead
	// of colliding with a revision a caller may still be holding onto — see
	// ruleStore.deletedHighWater's doc comment.
	if h.rules.deletedHighWater == nil {
		h.rules.deletedHighWater = map[groupKey]int{}
	}
	if current.Revision > h.rules.deletedHighWater[key] {
		h.rules.deletedHighWater[key] = current.Revision
	}

	delete(h.rules.Groups, key)
	for i, k := range h.rules.Order {
		if k == key {
			h.rules.Order = append(h.rules.Order[:i:i], h.rules.Order[i+1:]...)
			break
		}
	}

	if err := h.sess.loadRuleStore(h.rules); err != nil {
		return deleteRuleGroupOutput{}, fmt.Errorf("delete_rule_group: removed %s but failed to reload the session: %w",
			current.File, err)
	}

	full2, err := h.sess.buildDB()
	if err != nil {
		return deleteRuleGroupOutput{}, err
	}
	if h.onChange != nil {
		h.onChange()
	}

	return deleteRuleGroupOutput{Head: in.Head, Arity: in.Arity, File: current.File, Predicates: countPredicates(full2)}, nil
}
