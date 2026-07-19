package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
	"swdunlop.dev/pkg/datalog"
	"swdunlop.dev/pkg/datalog/jsonfacts"
)

// This file implements phase 1d of workbench v2 (doc/features/workbench-v2.md
// design decision 3, "No textareas, ever. Disk is canonical; fsnotify makes
// it reactive"): the server watches the schema file (-c) and the rules/
// directory (--rules), and a save from vim — or the echo of an agent's own
// CRUD write — triggers debounce → reload → validate → full automatic
// re-evaluation → bus publish. A failed reload keeps the last good state and
// records the error in the persistent status surface (workbench.lastReload).
//
// The reload paths are the SAME chokepoints every other mutation uses:
// loadRuleStore + session.loadRuleStore for the rules directory, and
// prepareSchema + applySchemaLocked for the schema — the watcher adds no
// second way to build session state, only a new trigger for the existing
// ones. Self-writes and vim writes are treated identically (spec: "the
// watcher treats self-writes and vim writes identically; a debounce absorbs
// the echo"): the reload compares disk content against what the session
// already holds and skips the swap — and the re-evaluation — when nothing
// actually changed, which is exactly what an agent-write echo looks like.

// reloadStatus is one reload's recorded outcome — the phase-1d shape of the
// spec's "persistent status surface". Changed lists the per-part revision
// bumps the reload computed ("rule group at_risk/2 modified", "matcher
// admin_share/0 removed", ...) — the same summary decision 8's disk-change
// notices prepend to the next agent turn in phase 2, recorded here because
// the reload already computes it for free. Err is empty on success;
// non-empty means the reload was refused and the last good state kept.
type reloadStatus struct {
	Time    time.Time
	Changed []string
	Err     string
}

// recordReload stores status as the most recent reload outcome (under
// reloadMu) and logs it to stderr — the one place phase 1d surfaces reload
// results; phase 2's status rendering and agent-turn preamble read
// wb.lastReload instead of re-deriving any of this.
func (wb *workbench) recordReload(status reloadStatus) {
	wb.reloadMu.Lock()
	wb.lastReload = status
	wb.reloadSeq++
	wb.reloadMu.Unlock()

	switch {
	case status.Err != "":
		fmt.Fprintf(os.Stderr, "datalog serve: reload failed (keeping last good state): %s\n", status.Err)
	case len(status.Changed) > 0:
		fmt.Fprintf(os.Stderr, "datalog serve: reloaded from disk: %s\n", strings.Join(status.Changed, ", "))
	}
}

// watchDebounce is how long the watcher waits after the last relevant
// filesystem event before reloading. Editors write non-atomically (vim's
// write-then-rename dance emits several events per save) and one save often
// touches several files; the debounce coalesces the burst into one reload.
// 300ms is far below human perception for a "save in vim, glance at the
// browser" loop while comfortably above the intra-save event spacing.
const watchDebounce = 300 * time.Millisecond

// reloadJobKey is the Jobs key for the watcher's automatic re-evaluation —
// registered so the sandbox's Global Cancel can stop a runaway reload
// Transform exactly like a Run's, and doubling as the $busy vocabulary for
// the one-spinner rule while the re-evaluation runs.
const reloadJobKey = "reload"

// startWatcher begins watching the schema file and/or rules directory this
// workbench was started with, returning a stop function. When the session
// has neither (no -c and no --rules), there is nothing on disk to be
// canonical and the returned stop is a no-op. Watches are registered on the
// PARENT DIRECTORIES, not the files: editors replace files by rename, which
// ends a watch registered on the old inode — a directory watch survives the
// rename dance and reports the new file under the same name.
func (wb *workbench) startWatcher() (func(), error) {
	schemaFile := wb.h.configPath
	rulesDir := ""
	if wb.h.rules != nil {
		rulesDir = wb.h.rules.Dir
	}
	if schemaFile == "" && rulesDir == "" {
		return func() {}, nil
	}

	w, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("starting file watcher: %w", err)
	}
	dirs := map[string]bool{}
	if schemaFile != "" {
		dirs[filepath.Dir(schemaFile)] = true
	}
	if rulesDir != "" {
		dirs[filepath.Clean(rulesDir)] = true
	}
	for dir := range dirs {
		if err := w.Add(dir); err != nil {
			w.Close()
			return nil, fmt.Errorf("watching %s: %w", dir, err)
		}
	}

	done := make(chan struct{})
	go wb.watchLoop(w, schemaFile, rulesDir, done)
	return func() {
		w.Close() // closes w.Events, ending watchLoop
		<-done
	}, nil
}

// watchLoop consumes fsnotify events, classifies them against the two
// watched targets, and fires reloadFromDisk after watchDebounce of quiet.
// Only the schema file's exact name and "*.dl" files directly in the rules
// directory are relevant; everything else in the watched directories (vim
// swap files, our own "*.tmp-*" atomic-write temporaries, unrelated
// siblings of the schema file) is ignored by name before it can reset the
// debounce timer.
func (wb *workbench) watchLoop(w *fsnotify.Watcher, schemaFile, rulesDir string, done chan<- struct{}) {
	defer close(done)

	// The timer starts stopped; the channel read below only fires after a
	// relevant event has armed it.
	timer := time.NewTimer(watchDebounce)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()

	dirtySchema, dirtyRules := false, false
	for {
		select {
		case ev, ok := <-w.Events:
			if !ok {
				return
			}
			// Chmod-only events carry no content change (and some editors
			// emit them constantly); everything else — Create, Write,
			// Rename, Remove — can mean the watched content differs now.
			if ev.Op == fsnotify.Chmod {
				continue
			}
			name := filepath.Clean(ev.Name)
			switch {
			case schemaFile != "" && name == filepath.Clean(schemaFile):
				dirtySchema = true
			case rulesDir != "" && filepath.Dir(name) == filepath.Clean(rulesDir) &&
				filepath.Ext(name) == ".dl":
				dirtyRules = true
			default:
				continue
			}
			timer.Reset(watchDebounce)
		case <-timer.C:
			s, r := dirtySchema, dirtyRules
			dirtySchema, dirtyRules = false, false
			wb.reloadFromDisk(s, r)
		case _, ok := <-w.Errors:
			if !ok {
				return
			}
			// Watcher-level errors (overflow, etc.) are not reload failures;
			// the next real event still triggers a reload, and a reload
			// always reads current disk state, so nothing is lost — no
			// status recorded for these.
		}
	}
}

// reloadFromDisk is the debounced reload: re-read the dirty parts from
// disk, validate before swapping (a failed load keeps the last good state),
// reconcile per-part revisions so an agent write staged against pre-save
// content is rejected as stale (spec Risks: "a vim save bumps the per-part
// revision counters on reload"), and — if anything actually changed —
// re-evaluate and repaint. Exported for tests as the watcher-independent
// seam: the fsnotify plumbing above only decides WHEN to call this.
func (wb *workbench) reloadFromDisk(reloadSchema, reloadRules bool) {
	status := reloadStatus{Time: time.Now()}
	var errs []string
	swapped := false

	if reloadRules && wb.h.rules != nil {
		changed, err := wb.reloadRules()
		if err != nil {
			errs = append(errs, err.Error())
		} else if len(changed) > 0 {
			status.Changed = append(status.Changed, changed...)
			swapped = true
		}
	}

	if reloadSchema && wb.h.configPath != "" {
		changed, didSwap, err := wb.reloadSchema()
		if err != nil {
			errs = append(errs, err.Error())
		} else if didSwap {
			status.Changed = append(status.Changed, changed...)
			swapped = true
		}
	}

	status.Err = strings.Join(errs, "; ")

	if swapped {
		// Full automatic re-evaluation (spec decision 3), then repaint.
		// autoReevaluate publishes session-changed itself after the
		// write-back so every open view repaints once, with derived facts.
		if err := wb.autoReevaluate(); err != nil {
			// The reload itself succeeded — the session holds the new
			// state — but its evaluation failed (compile/timeout/cap).
			// That is a status-surface fact, not a reason to roll back:
			// the same edit saved in vim would hit the same error at the
			// next query anyway, and here it is reported immediately.
			if status.Err != "" {
				status.Err += "; "
			}
			status.Err += "re-evaluation: " + err.Error()
		}
	}

	wb.recordReload(status)
}

// reloadRules re-reads the rules/ directory through the loadRuleStore
// chokepoint, reconciles revisions against the current store, and — only if
// some group actually changed — swaps the store and rebuilds the session
// through session.loadRuleStore. The disk read runs under h.mu: group files
// are small (holding the lock for a directory of tiny .dl files is
// microseconds), and reading lock-free would race an agent CRUD write's
// disk+memory update, reverting the session to a pre-write snapshot the
// watcher read mid-update.
func (wb *workbench) reloadRules() ([]string, error) {
	wb.h.mu.Lock()
	defer wb.h.mu.Unlock()

	fresh, err := loadRuleStore(wb.h.rules.Dir)
	if err != nil {
		return nil, err
	}
	changed := reconcileRuleRevisions(wb.h.rules, fresh)
	if len(changed) == 0 {
		// Byte-identical to what the session already holds — the echo of
		// our own CRUD write, or a no-op save. Nothing to swap, nothing to
		// re-evaluate.
		return nil, nil
	}
	if err := wb.h.sess.loadRuleStore(fresh); err != nil {
		// loadRuleStore (the store loader above) already parsed and
		// validated every file, so the session rebuild re-parsing the same
		// text cannot fail in practice; surfacing rather than swallowing it
		// keeps a future divergence loud. The session may be partially
		// rebuilt here (session.loadRuleStore's documented contract) — the
		// same trial-compile-free startup posture, which fails loudly at
		// the next evaluation rather than silently dropping derivations.
		return nil, fmt.Errorf("rebuilding session from %s: %w", wb.h.rules.Dir, err)
	}
	wb.h.rules = fresh
	return changed, nil
}

// reloadSchema re-reads the schema file and, if its text differs from the
// session's current document, runs it through the SAME prepare/apply
// pipeline as set-schema-era writes: prepareSchema lock-free (it loads the
// whole data corpus — far too slow to hold h.mu across), then under h.mu a
// re-read of the file to confirm the prepared text is still what is on disk
// (an agent schema-CRUD write landing during the lock-free window would
// otherwise be clobbered by this reload's older snapshot; its own fsnotify
// echo converges disk and session, but the stale swap must not happen at
// all), then applySchemaLocked and revision reconciliation.
func (wb *workbench) reloadSchema() (changed []string, swapped bool, err error) {
	path := wb.h.configPath
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, false, fmt.Errorf("reading %s: %w", path, err)
	}
	text := string(data)

	wb.h.mu.Lock()
	same := text == wb.h.sess.schemaText
	wb.h.mu.Unlock()
	if same {
		return nil, false, nil // echo of our own write, or a no-op save
	}

	format := "yaml"
	if filepath.Ext(path) == ".json" {
		format = "json"
	}
	authoring, runtime, db, err := prepareSchema(text, format, wb.h.fsys, wb.h.confine)
	if err != nil {
		return nil, false, fmt.Errorf("%s: %w", path, err)
	}

	wb.h.mu.Lock()
	defer wb.h.mu.Unlock()
	cur, rerr := os.ReadFile(path)
	if rerr != nil {
		return nil, false, fmt.Errorf("re-reading %s: %w", path, rerr)
	}
	if string(cur) != text {
		// Disk moved on during the prepare — a newer save or agent write.
		// Its own event is already queued behind this reload; applying our
		// stale snapshot would briefly revert the session, so skip and let
		// that event's reload do the swap.
		return nil, false, nil
	}
	if text == wb.h.sess.schemaText {
		return nil, false, nil // an agent write of this exact text landed meanwhile
	}

	oldCfg := wb.h.sess.authoringCfg
	oldRevs := wb.h.schemaRev
	if _, err := wb.h.applySchemaLocked(text, authoring, runtime, db); err != nil {
		return nil, false, fmt.Errorf("applying %s: %w", path, err)
	}
	newRevs, changed := reconcileSchemaRevisions(oldRevs, oldCfg, authoring)
	wb.h.schemaRev = newRevs
	return changed, true, nil
}

// autoReevaluate runs the full 5s-capped Transform after a successful
// reload swap, mirroring handleRulesRun's snapshot/evaluate/write-back
// split (rules_editor.go): h.mu is held only to capture the snapshot and to
// commit the result under the generation guard; the Transform itself runs
// lock-free under the reload job so Global Cancel can stop it and the
// spinner shows. The write-back is gen-guarded exactly like Run's: if
// anything else mutated the session while the Transform ran, the result is
// discarded (whoever mutated will have invalidated derivedDB themselves).
func (wb *workbench) autoReevaluate() error {
	jobCtx, done := wb.jobs.Begin(context.Background(), reloadJobKey)
	if jobCtx == nil {
		// A previous reload's evaluation is still running. Skip rather than
		// queue: derivedDB was invalidated by the swap, so correctness is
		// unaffected — the next query (or the next save's reload) simply
		// evaluates lazily instead of eagerly.
		return nil
	}
	defer done()
	wb.publishBusy(reloadJobKey)
	defer wb.publishBusy("")

	ctx, cancel := context.WithTimeout(jobCtx, evalTimeout)
	defer cancel()

	wb.h.mu.Lock()
	ruleset, engineOpts, db, snapGen, buildErr := wb.h.sess.snapshotForEvaluate()
	prov := wb.h.sess.newEvalProvenance()
	wb.h.mu.Unlock()

	var evaluated datalog.Database
	evalErr := buildErr
	if buildErr == nil {
		evalErr = <-runRecovered(func() error {
			var err error
			evaluated, err = evaluateSnapshot(ctx, ruleset, engineOpts, db, prov)
			return err
		})
	}
	var capErr error
	if evalErr == nil {
		capErr = checkFactCap(evaluated)
	}

	wb.h.mu.Lock()
	if ctx.Err() == nil && evalErr == nil && capErr == nil && wb.h.sess.gen == snapGen {
		wb.h.sess.derivedDB = evaluated
		wb.h.sess.derivedProv = prov
	}
	wb.publishSessionChanged()
	wb.h.mu.Unlock()

	switch {
	case evalErr != nil:
		return evalErr
	case capErr != nil:
		return capErr
	default:
		return nil
	}
}

// reconcileRuleRevisions carries revision state from the current store into
// a freshly loaded one: an unchanged group (byte-identical Text) keeps its
// revision, a changed group bumps it, a new group starts at 1 (or past its
// deleted high-water), and a removed group records its high-water so a
// later re-create can never make a stale caller's revision look current
// again — the same per-key discipline the CRUD writes enforce
// (rulestore.go's deletedHighWater doc comment), applied to disk edits.
// This is what makes the spec's guard work: "a vim save bumps the per-part
// revision counters on reload, so an agent write staged against pre-save
// content is rejected with the current content handed back." Returns the
// human-readable change list (empty means the fresh store is identical and
// the caller should not swap at all).
func reconcileRuleRevisions(old, fresh *ruleStore) []string {
	var changed []string
	fresh.deletedHighWater = make(map[groupKey]int, len(old.deletedHighWater))
	for k, v := range old.deletedHighWater {
		fresh.deletedHighWater[k] = v
	}
	for _, k := range fresh.Order {
		g := fresh.Groups[k]
		if og, ok := old.Groups[k]; ok {
			if og.Text == g.Text {
				g.Revision = og.Revision
			} else {
				g.Revision = og.Revision + 1
				changed = append(changed, fmt.Sprintf("rule group %s/%d modified", k.Head, k.Arity))
			}
			continue
		}
		g.Revision = 1
		if hw := fresh.deletedHighWater[k]; hw > 0 {
			g.Revision = hw + 1
		}
		changed = append(changed, fmt.Sprintf("rule group %s/%d added", k.Head, k.Arity))
	}
	for _, k := range old.Order {
		if _, ok := fresh.Groups[k]; ok {
			continue
		}
		if rev := old.Groups[k].Revision; rev > fresh.deletedHighWater[k] {
			fresh.deletedHighWater[k] = rev
		}
		changed = append(changed, fmt.Sprintf("rule group %s/%d removed", k.Head, k.Arity))
	}
	return changed
}

// reconcileSchemaRevisions is reconcileRuleRevisions' schema twin, at the
// per-item grain schemaRevisions tracks (schema_crud.go): unchanged items
// (reflect.DeepEqual on the AUTHORING form — the same form the CRUD surface
// compares and hands back) keep their revisions, changed items bump, new
// items start at 1 or past their deleted high-water, removed items record
// their high-water. oldCfg/newCfg must both be authoring-form configs.
func reconcileSchemaRevisions(old *schemaRevisions, oldCfg, newCfg jsonfacts.Config) (*schemaRevisions, []string) {
	out := old.clone()
	var changed []string

	out.sources = map[string]int{}
	for _, s := range newCfg.Sources {
		s := s
		if prev, ok := findSource(oldCfg.Sources, s.File); ok {
			rev := old.sources[s.File]
			if reflect.DeepEqual(*prev, s) {
				out.sources[s.File] = rev
			} else {
				out.sources[s.File] = rev + 1
				changed = append(changed, fmt.Sprintf("source %s modified", s.File))
			}
			continue
		}
		rev := 1
		if hw := old.deletedSources[s.File]; hw > 0 {
			rev = hw + 1
		}
		out.sources[s.File] = rev
		changed = append(changed, fmt.Sprintf("source %s added", s.File))
	}
	for _, s := range oldCfg.Sources {
		if _, ok := findSource(newCfg.Sources, s.File); ok {
			continue
		}
		if rev := old.sources[s.File]; rev > out.deletedSources[s.File] {
			out.deletedSources[s.File] = rev
		}
		changed = append(changed, fmt.Sprintf("source %s removed", s.File))
	}

	out.matchers = map[matcherKey]int{}
	for _, m := range newCfg.Matchers {
		m := m
		key := matcherKeyOf(m)
		if prev, ok := findMatcher(oldCfg.Matchers, key); ok {
			rev := old.matchers[key]
			if reflect.DeepEqual(*prev, m) {
				out.matchers[key] = rev
			} else {
				out.matchers[key] = rev + 1
				changed = append(changed, fmt.Sprintf("matcher %s/%d modified", key.Predicate, key.Term))
			}
			continue
		}
		rev := 1
		if hw := old.deletedMatchers[key]; hw > 0 {
			rev = hw + 1
		}
		out.matchers[key] = rev
		changed = append(changed, fmt.Sprintf("matcher %s/%d added", key.Predicate, key.Term))
	}
	for _, m := range oldCfg.Matchers {
		key := matcherKeyOf(m)
		if _, ok := findMatcher(newCfg.Matchers, key); ok {
			continue
		}
		if rev := old.matchers[key]; rev > out.deletedMatchers[key] {
			out.deletedMatchers[key] = rev
		}
		changed = append(changed, fmt.Sprintf("matcher %s/%d removed", key.Predicate, key.Term))
	}

	out.declarations = map[declarationKey]int{}
	for _, d := range newCfg.Declarations {
		d := d
		key := declarationKeyOf(d)
		if prev, ok := findDeclaration(oldCfg.Declarations, key); ok {
			rev := old.declarations[key]
			if reflect.DeepEqual(*prev, d) {
				out.declarations[key] = rev
			} else {
				out.declarations[key] = rev + 1
				changed = append(changed, fmt.Sprintf("declaration %s/%d modified", key.Name, key.Arity))
			}
			continue
		}
		rev := 1
		if hw := old.deletedDeclarations[key]; hw > 0 {
			rev = hw + 1
		}
		out.declarations[key] = rev
		changed = append(changed, fmt.Sprintf("declaration %s/%d added", key.Name, key.Arity))
	}
	for _, d := range oldCfg.Declarations {
		key := declarationKeyOf(d)
		if _, ok := findDeclaration(newCfg.Declarations, key); ok {
			continue
		}
		if rev := old.declarations[key]; rev > out.deletedDeclarations[key] {
			out.deletedDeclarations[key] = rev
		}
		changed = append(changed, fmt.Sprintf("declaration %s/%d removed", key.Name, key.Arity))
	}

	return out, changed
}
