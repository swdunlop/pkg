package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// watchTestWorkbench builds a --rules + -c workbench over a temp dir with
// one synthetic source, one starting rule group, and the schema written to
// disk (the -c path must exist on disk for the watcher/reload to re-read
// it). Returns the workbench plus the schema path and rules dir.
func watchTestWorkbench(t *testing.T) (wb *workbench, schemaPath, rulesDir string) {
	t.Helper()
	dir := t.TempDir()
	writeSyntheticData(t, dir, 3)

	schemaPath = filepath.Join(dir, "schema.yaml")
	mustWriteFile(t, schemaPath, syntheticSchemaYAML)

	rulesDir = filepath.Join(dir, "rules")
	if err := os.MkdirAll(rulesDir, 0o755); err != nil {
		t.Fatalf("mkdir rules dir: %v", err)
	}
	mustWriteFile(t, filepath.Join(rulesDir, "busy_2.dl"),
		"busy(H, P) :- event(H, P, ?).\n")

	wb, closeFn, err := newWorkbench(dir, schemaPath, nil, rulesDir, "test-token", agentConfig{})
	if err != nil {
		t.Fatalf("newWorkbench: %v", err)
	}
	t.Cleanup(func() { closeFn() })
	return wb, schemaPath, rulesDir
}

// TestReloadFromDisk_RulesEditBumpsRevisionAndReevaluates: a disk edit to
// an existing group file swaps the session, bumps THAT group's revision
// (spec Risks: "a vim save bumps the per-part revision counters on
// reload"), re-evaluates so derived facts reflect the new rules, and
// records the change in the status surface.
func TestReloadFromDisk_RulesEditBumpsRevisionAndReevaluates(t *testing.T) {
	wb, _, rulesDir := watchTestWorkbench(t)

	before, err := wb.getRuleGroupRevision("busy", 2)
	if err != nil {
		t.Fatalf("initial revision: %v", err)
	}

	// vim-style edit: narrow the rule to one host.
	mustWriteFile(t, filepath.Join(rulesDir, "busy_2.dl"),
		"busy(H, P) :- event(H, P, ?), H = \"h1\".\n")
	wb.reloadFromDisk(false, true)

	after, err := wb.getRuleGroupRevision("busy", 2)
	if err != nil {
		t.Fatalf("post-reload revision: %v", err)
	}
	if after != before+1 {
		t.Errorf("revision = %d, want %d (bumped once by the disk edit)", after, before+1)
	}

	// The automatic re-evaluation must have run: derivedDB holds busy/2
	// facts for exactly the narrowed rule (1 of the 3 events).
	wb.h.mu.Lock()
	db := wb.h.sess.derivedDB
	wb.h.mu.Unlock()
	if db == nil {
		t.Fatal("derivedDB is nil after reload; auto re-evaluation did not run")
	}
	n := 0
	for range db.Facts("busy", 2) {
		n++
	}
	if n != 1 {
		t.Errorf("busy/2 facts after narrowed reload = %d, want 1", n)
	}

	wb.reloadMu.Lock()
	status := wb.lastReload
	wb.reloadMu.Unlock()
	if status.Err != "" {
		t.Errorf("reload status Err = %q, want empty", status.Err)
	}
	if len(status.Changed) != 1 || !strings.Contains(status.Changed[0], "busy/2") {
		t.Errorf("reload status Changed = %v, want one busy/2 entry", status.Changed)
	}
}

// TestReloadFromDisk_InvalidRulesKeepsLastGoodState: a half-written or
// invalid group file must not poison the session (spec: "a failed reload
// keeps the last good state and reports the error").
func TestReloadFromDisk_InvalidRulesKeepsLastGoodState(t *testing.T) {
	wb, _, rulesDir := watchTestWorkbench(t)

	wb.h.mu.Lock()
	rulesBefore := wb.h.sess.rulesText
	genBefore := wb.h.sess.gen
	wb.h.mu.Unlock()

	mustWriteFile(t, filepath.Join(rulesDir, "busy_2.dl"),
		"busy(H, P) :- event(H, P,\n") // torn mid-write
	wb.reloadFromDisk(false, true)

	wb.h.mu.Lock()
	rulesAfter := wb.h.sess.rulesText
	genAfter := wb.h.sess.gen
	wb.h.mu.Unlock()
	if rulesAfter != rulesBefore {
		t.Errorf("session rulesText changed on a failed reload")
	}
	if genAfter != genBefore {
		t.Errorf("session gen bumped on a failed reload (gen %d -> %d)", genBefore, genAfter)
	}

	wb.reloadMu.Lock()
	status := wb.lastReload
	wb.reloadMu.Unlock()
	if status.Err == "" {
		t.Error("reload status Err empty, want the parse error")
	}
	if !strings.Contains(status.Err, "busy_2.dl") {
		t.Errorf("reload status Err = %q, want it to name busy_2.dl", status.Err)
	}

	// Recovery: fixing the file reloads cleanly.
	mustWriteFile(t, filepath.Join(rulesDir, "busy_2.dl"),
		"busy(H, P) :- event(H, P, ?).\n")
	wb.reloadFromDisk(false, true)
	wb.reloadMu.Lock()
	status = wb.lastReload
	wb.reloadMu.Unlock()
	if status.Err != "" {
		t.Errorf("reload after fixing the file: Err = %q, want empty", status.Err)
	}
}

// TestReloadFromDisk_EchoOfOwnWriteIsANoOp: after an agent CRUD write, the
// watcher sees the file event, but the reload must recognize disk ==
// session (byte-identical) and skip the swap, keeping the CRUD write's
// revision rather than double-bumping it (spec: "the watcher treats
// self-writes and vim writes identically; reload is idempotent").
func TestReloadFromDisk_EchoOfOwnWriteIsANoOp(t *testing.T) {
	wb, _, _ := watchTestWorkbench(t)

	out, err := wb.h.putRuleGroup(putRuleGroupInput{
		Head: "quiet", Arity: 1,
		Text: `quiet(H) :- event(H, ?, ?), not busy(H, ?).`,
	})
	if err != nil {
		t.Fatalf("put_rule_group: %v", err)
	}
	if out.IsStale {
		t.Fatalf("put_rule_group unexpectedly stale: %+v", out)
	}

	wb.h.mu.Lock()
	genBefore := wb.h.sess.gen
	wb.h.mu.Unlock()

	wb.reloadFromDisk(false, true) // the fsnotify echo of the write above

	rev, err := wb.getRuleGroupRevision("quiet", 1)
	if err != nil {
		t.Fatalf("revision after echo: %v", err)
	}
	if rev != out.Revision {
		t.Errorf("revision after echo reload = %d, want %d (unchanged)", rev, out.Revision)
	}
	wb.h.mu.Lock()
	genAfter := wb.h.sess.gen
	wb.h.mu.Unlock()
	if genAfter != genBefore {
		t.Errorf("echo reload bumped session gen %d -> %d; want no-op", genBefore, genAfter)
	}
}

// TestReloadFromDisk_StaleAgentWriteRejectedAfterVimSave is the spec's
// concurrent-edit guard end-to-end: agent stages an edit against revision
// N, vim saves (reload bumps to N+1), the agent's write must be rejected as
// stale WITH the vim content handed back.
func TestReloadFromDisk_StaleAgentWriteRejectedAfterVimSave(t *testing.T) {
	wb, _, rulesDir := watchTestWorkbench(t)

	staged, err := wb.h.getRuleGroup(getRuleGroupInput{Head: "busy", Arity: 2})
	if err != nil {
		t.Fatalf("get_rule_group: %v", err)
	}

	vimText := "busy(H, P) :- event(H, P, ?), H = \"h2\".\n"
	mustWriteFile(t, filepath.Join(rulesDir, "busy_2.dl"), vimText)
	wb.reloadFromDisk(false, true)

	out, err := wb.h.putRuleGroup(putRuleGroupInput{
		Head: "busy", Arity: 2,
		Text:     "busy(H, P) :- event(H, P, ?), H = \"h0\".",
		Revision: staged.Revision, // staged before the vim save
	})
	if err != nil {
		t.Fatalf("put_rule_group: %v", err)
	}
	if !out.IsStale {
		t.Fatal("agent write staged against pre-save revision was accepted; want stale rejection")
	}
	if out.CurrentRevision != staged.Revision+1 {
		t.Errorf("stale handback revision = %d, want %d", out.CurrentRevision, staged.Revision+1)
	}
	if out.CurrentText != strings.TrimRight(vimText, "\n") {
		t.Errorf("stale handback text = %q, want the vim save's content", out.CurrentText)
	}
}

// TestReloadFromDisk_GroupAddAndRemove: creating and deleting group files
// in vim lands as added/removed groups with the change list naming them.
func TestReloadFromDisk_GroupAddAndRemove(t *testing.T) {
	wb, _, rulesDir := watchTestWorkbench(t)

	mustWriteFile(t, filepath.Join(rulesDir, "seen_1.dl"), "seen(H) :- event(H, ?, ?).\n")
	wb.reloadFromDisk(false, true)

	wb.reloadMu.Lock()
	changed := wb.lastReload.Changed
	wb.reloadMu.Unlock()
	if len(changed) != 1 || !strings.Contains(changed[0], "seen/1 added") {
		t.Errorf("Changed = %v, want [.. seen/1 added]", changed)
	}
	if _, err := wb.getRuleGroupRevision("seen", 1); err != nil {
		t.Fatalf("seen/1 not in store after add: %v", err)
	}

	if err := os.Remove(filepath.Join(rulesDir, "seen_1.dl")); err != nil {
		t.Fatalf("rm seen_1.dl: %v", err)
	}
	wb.reloadFromDisk(false, true)
	if _, err := wb.getRuleGroupRevision("seen", 1); err == nil {
		t.Error("seen/1 still in store after its file was removed")
	}
	// Re-creating the group resumes past its high-water, so a caller
	// holding the pre-delete revision can never look current.
	mustWriteFile(t, filepath.Join(rulesDir, "seen_1.dl"), "seen(H) :- event(H, ?, ?).\n")
	wb.reloadFromDisk(false, true)
	rev, err := wb.getRuleGroupRevision("seen", 1)
	if err != nil {
		t.Fatalf("seen/1 not in store after re-add: %v", err)
	}
	if rev != 2 {
		t.Errorf("re-created group revision = %d, want 2 (past the deleted high-water)", rev)
	}
}

// TestReloadFromDisk_SchemaEditSwapsAndBumps: a vim edit to the schema file
// reloads through prepareSchema/applySchemaLocked, bumps only the touched
// item's revision, and re-extracts facts from the data.
func TestReloadFromDisk_SchemaEditSwapsAndBumps(t *testing.T) {
	wb, schemaPath, _ := watchTestWorkbench(t)

	cfgBefore, err := wb.h.getConfig(getConfigInput{})
	if err != nil {
		t.Fatalf("get_config: %v", err)
	}

	// vim edit: add a declaration; the source is untouched.
	mustWriteFile(t, schemaPath, syntheticSchemaYAML+`  - name: busy
    use: "a host/pid pair that ran something"
`)
	wb.reloadFromDisk(true, false)

	wb.reloadMu.Lock()
	status := wb.lastReload
	wb.reloadMu.Unlock()
	if status.Err != "" {
		t.Fatalf("schema reload failed: %s", status.Err)
	}
	if len(status.Changed) != 1 || !strings.Contains(status.Changed[0], "declaration busy/0 added") {
		t.Errorf("Changed = %v, want [declaration busy/0 added]", status.Changed)
	}

	cfgAfter, err := wb.h.getConfig(getConfigInput{})
	if err != nil {
		t.Fatalf("get_config after reload: %v", err)
	}
	// The untouched source keeps its revision; extraction still works.
	if len(cfgBefore.Sources) != 1 || len(cfgAfter.Sources) != 1 {
		t.Fatalf("sources before/after = %d/%d, want 1/1", len(cfgBefore.Sources), len(cfgAfter.Sources))
	}
	if cfgAfter.Sources[0].Revision != cfgBefore.Sources[0].Revision {
		t.Errorf("untouched source revision changed %d -> %d",
			cfgBefore.Sources[0].Revision, cfgAfter.Sources[0].Revision)
	}
	wb.h.mu.Lock()
	db := wb.h.sess.dataDB
	wb.h.mu.Unlock()
	n := 0
	for range db.Facts("event", 3) {
		n++
	}
	if n != 3 {
		t.Errorf("event/3 facts after schema reload = %d, want 3", n)
	}
}

// TestReloadFromDisk_InvalidSchemaKeepsLastGoodState mirrors the rules
// torn-write test on the schema side.
func TestReloadFromDisk_InvalidSchemaKeepsLastGoodState(t *testing.T) {
	wb, schemaPath, _ := watchTestWorkbench(t)

	wb.h.mu.Lock()
	textBefore := wb.h.sess.schemaText
	wb.h.mu.Unlock()

	mustWriteFile(t, schemaPath, "sources:\n  - file: [torn") // invalid YAML
	wb.reloadFromDisk(true, false)

	wb.h.mu.Lock()
	textAfter := wb.h.sess.schemaText
	wb.h.mu.Unlock()
	if textAfter != textBefore {
		t.Error("session schemaText changed on a failed schema reload")
	}
	wb.reloadMu.Lock()
	status := wb.lastReload
	wb.reloadMu.Unlock()
	if status.Err == "" {
		t.Error("reload status Err empty, want the YAML parse error")
	}
}

// TestWatcher_EndToEnd exercises the real fsnotify plumbing once: start the
// watcher, save a group file the way vim does (write temp + rename), and
// wait for the debounced reload to land. Everything else in this file goes
// through reloadFromDisk directly; this one test proves the events actually
// arrive and the debounce fires.
func TestWatcher_EndToEnd(t *testing.T) {
	wb, _, rulesDir := watchTestWorkbench(t)

	stop, err := wb.startWatcher()
	if err != nil {
		t.Fatalf("startWatcher: %v", err)
	}
	defer stop()

	// vim-style atomic save: write a sibling temp file, rename over.
	tmp := filepath.Join(rulesDir, "busy_2.dl.swp-like")
	mustWriteFile(t, tmp, "busy(H, P) :- event(H, P, ?), H = \"h0\".\n")
	if err := os.Rename(tmp, filepath.Join(rulesDir, "busy_2.dl")); err != nil {
		t.Fatalf("rename: %v", err)
	}

	deadline := time.After(5 * time.Second)
	for {
		wb.reloadMu.Lock()
		changed := wb.lastReload.Changed
		wb.reloadMu.Unlock()
		if len(changed) > 0 {
			if !strings.Contains(changed[0], "busy/2 modified") {
				t.Errorf("Changed = %v, want [rule group busy/2 modified]", changed)
			}
			return
		}
		select {
		case <-deadline:
			t.Fatal("watcher never reloaded after a rename-style save")
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// getRuleGroupRevision reads one group's current revision under h.mu — a
// test-side convenience over the same state list_rule_groups serves.
func (wb *workbench) getRuleGroupRevision(head string, arity int) (int, error) {
	out, err := wb.h.getRuleGroup(getRuleGroupInput{Head: head, Arity: arity})
	if err != nil {
		return 0, err
	}
	return out.Revision, nil
}
