package main

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/mark3labs/kit/pkg/kit"
	"github.com/mark3labs/mcp-go/server"
)

// -- project directory resolution --------------------------------------------

// TestConversationSessionsDir covers conversationSessionsDir's precedence:
// schema file's directory first, rules directory's parent second, cwd last.
func TestConversationSessionsDir(t *testing.T) {
	got, err := conversationSessionsDir("/proj/schema.yaml", "")
	if err != nil {
		t.Fatalf("configPath case: %v", err)
	}
	if want := filepath.Join("/proj", ".datalog", "sessions"); got != want {
		t.Errorf("configPath case = %q, want %q", got, want)
	}

	got, err = conversationSessionsDir("", "/proj/rules")
	if err != nil {
		t.Fatalf("rulesDir case: %v", err)
	}
	if want := filepath.Join("/proj", ".datalog", "sessions"); got != want {
		t.Errorf("rulesDir case = %q, want %q", got, want)
	}

	// configPath wins when both are given, matching newWorkbench's own
	// precedence (the -c schema file is the primary project anchor; --rules
	// can point anywhere).
	got, err = conversationSessionsDir("/proj-a/schema.yaml", "/proj-b/rules")
	if err != nil {
		t.Fatalf("both-given case: %v", err)
	}
	if want := filepath.Join("/proj-a", ".datalog", "sessions"); got != want {
		t.Errorf("both-given case = %q, want %q", got, want)
	}
}

// TestNewWorkbenchWiresConversations covers the minimal wiring this task's
// brief asks for: newWorkbench populates wb.conversations and wb.turnGate
// so the manager is reachable end to end, without any route calling it yet.
func TestNewWorkbenchWiresConversations(t *testing.T) {
	dataDir := t.TempDir()
	schemaDir := t.TempDir()
	configPath := filepath.Join(schemaDir, "schema.yaml")
	if err := os.WriteFile(configPath, []byte("sources: []\nmatchers: []\ndeclarations: []\n"), 0o644); err != nil {
		t.Fatalf("writing schema: %v", err)
	}

	wb := newTestWorkbench(t, dataDir, configPath, nil, "test-token")
	if wb.conversations == nil {
		t.Fatal("wb.conversations is nil")
	}
	if wb.turnGate == nil {
		t.Fatal("wb.turnGate is nil")
	}

	wantDir := filepath.Join(schemaDir, ".datalog", "sessions")
	if wb.conversations.dir != wantDir {
		t.Errorf("wb.conversations.dir = %q, want %q", wb.conversations.dir, wantDir)
	}

	// The manager is actually usable, not just non-nil.
	info, err := wb.conversations.Create(conversationModeQuery)
	if err != nil {
		t.Fatalf("Create via wired manager: %v", err)
	}
	if info.Mode != conversationModeQuery {
		t.Errorf("Create via wired manager mode = %q, want %q", info.Mode, conversationModeQuery)
	}
}

// -- Create/List/Delete/Resume round-trip -----------------------------------

// TestConversationCreateListDeleteRoundTrip covers the full lifecycle
// offline (no *kit.Kit, no LLM/network config — see conversation.go's top
// comment on why kit.OpenTreeSession is the offline-testable seam): Create
// a conversation in each mode, List sees it, Delete removes it and List no
// longer sees it.
func TestConversationCreateListDeleteRoundTrip(t *testing.T) {
	dir := filepath.Join(t.TempDir(), ".datalog", "sessions")
	cm, err := newConversationManager(dir)
	if err != nil {
		t.Fatalf("newConversationManager: %v", err)
	}

	// A fresh project's conversation directory starts empty.
	list, err := cm.List()
	if err != nil {
		t.Fatalf("List (empty): %v", err)
	}
	if len(list) != 0 {
		t.Fatalf("List (empty) = %d entries, want 0", len(list))
	}

	info, err := cm.Create(conversationModeRules)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if info.Mode != conversationModeRules {
		t.Fatalf("Create mode = %q, want %q", info.Mode, conversationModeRules)
	}
	if info.ID == "" {
		t.Fatal("Create: empty ID")
	}

	// Append a message directly through the session file the same way a
	// real turn would (agent.go's kitDriver.Prompt calls
	// sm.AppendMessage as part of PromptResult) — Get must see it.
	appendUserMessage(t, cm.sessionPath(info.ID), "hello there")

	got, err := cm.Get(info.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.MessageCount != 1 {
		t.Fatalf("Get.MessageCount = %d, want 1", got.MessageCount)
	}
	if got.FirstMessage != "hello there" {
		t.Fatalf("Get.FirstMessage = %q, want %q", got.FirstMessage, "hello there")
	}

	list, err = cm.List()
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list) != 1 || list[0].ID != info.ID {
		t.Fatalf("List = %+v, want exactly conversation %s", list, info.ID)
	}

	if err := cm.Delete(info.ID); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	list, err = cm.List()
	if err != nil {
		t.Fatalf("List after Delete: %v", err)
	}
	if len(list) != 0 {
		t.Fatalf("List after Delete = %d entries, want 0", len(list))
	}
	if _, err := cm.Get(info.ID); err == nil {
		t.Fatal("Get after Delete: want error, got nil")
	}
}

// TestConversationCreatedStaysFixed covers a subtle correctness point:
// Created (from the session header's Timestamp) must NOT drift when later
// messages bump the file's mtime — only Modified should move. A regression
// here would silently break "newest first" sorting logic that assumed
// Created was stable, or misreport a conversation's actual age.
func TestConversationCreatedStaysFixed(t *testing.T) {
	dir := filepath.Join(t.TempDir(), ".datalog", "sessions")
	cm, err := newConversationManager(dir)
	if err != nil {
		t.Fatalf("newConversationManager: %v", err)
	}
	info, err := cm.Create(conversationModeQuery)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	created := info.Created
	if created.IsZero() {
		t.Fatal("Create: zero Created timestamp")
	}

	time.Sleep(20 * time.Millisecond)
	appendUserMessage(t, cm.sessionPath(info.ID), "hello")

	got, err := cm.Get(info.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !got.Created.Equal(created) {
		t.Fatalf("Created drifted: %v -> %v", created, got.Created)
	}
	if !got.Modified.After(created) {
		t.Fatalf("Modified = %v, want strictly after Created %v", got.Modified, created)
	}
}

// TestConversationListNewestFirst covers List's ordering contract: newest
// (by Modified/mtime) first. Modified tracks each session file's own
// mtime, bumped by AppendMessage — a conversation touched more recently
// sorts first regardless of Create order.
func TestConversationListNewestFirst(t *testing.T) {
	dir := filepath.Join(t.TempDir(), ".datalog", "sessions")
	cm, err := newConversationManager(dir)
	if err != nil {
		t.Fatalf("newConversationManager: %v", err)
	}

	first, err := cm.Create(conversationModeQuery)
	if err != nil {
		t.Fatalf("Create first: %v", err)
	}
	appendUserMessage(t, cm.sessionPath(first.ID), "first")
	time.Sleep(20 * time.Millisecond) // ensure a distinguishable mtime

	second, err := cm.Create(conversationModeQuery)
	if err != nil {
		t.Fatalf("Create second: %v", err)
	}
	appendUserMessage(t, cm.sessionPath(second.ID), "second")
	time.Sleep(20 * time.Millisecond)

	// Touch "first" again so it becomes the newest despite being created
	// earlier — this is the ordering behavior newest-by-Modified promises
	// that newest-by-Created would get wrong.
	appendUserMessage(t, cm.sessionPath(first.ID), "first again")

	list, err := cm.List()
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("List = %d entries, want 2", len(list))
	}
	if list[0].ID != first.ID || list[1].ID != second.ID {
		t.Fatalf("List order = [%s, %s], want [%s, %s] (most-recently-touched first)",
			list[0].ID, list[1].ID, first.ID, second.ID)
	}
}

// TestConversationModePersistsAcrossResume covers the extension-data round
// trip Resume depends on: Create records a mode, and re-reading the SAME
// on-disk session (via a fresh ModeOf call, mirroring what Resume's first
// step does) reports the identical mode — the mechanism doc/features/
// workbench-v2.md design decision 6 relies on ("resume reconstructs the
// right agent").
func TestConversationModePersistsAcrossResume(t *testing.T) {
	dir := filepath.Join(t.TempDir(), ".datalog", "sessions")
	cm, err := newConversationManager(dir)
	if err != nil {
		t.Fatalf("newConversationManager: %v", err)
	}

	for _, mode := range []conversationMode{conversationModeQuery, conversationModeRules, conversationModeFacts} {
		info, err := cm.Create(mode)
		if err != nil {
			t.Fatalf("Create(%s): %v", mode, err)
		}
		got, err := cm.ModeOf(info.ID)
		if err != nil {
			t.Fatalf("ModeOf(%s) after Create: %v", mode, err)
		}
		if got != mode {
			t.Fatalf("ModeOf(%s) after Create = %q, want %q", info.ID, got, mode)
		}
	}
}

// TestConversationCreateRejectsInvalidMode covers Create's input
// validation gate: an unrecognized mode value must fail rather than
// silently create a conversation with no usable tool surface.
func TestConversationCreateRejectsInvalidMode(t *testing.T) {
	dir := filepath.Join(t.TempDir(), ".datalog", "sessions")
	cm, err := newConversationManager(dir)
	if err != nil {
		t.Fatalf("newConversationManager: %v", err)
	}
	if _, err := cm.Create(conversationMode("bogus")); err == nil {
		t.Fatal("Create(bogus mode): want error, got nil")
	}
}

// -- auto-title truncation ---------------------------------------------------

// TestTruncateTitle covers the ~60-char whole-word truncation this task's
// brief specifies, including the edge cases: exact boundary, no space
// within budget, multi-line collapsing, and empty input.
func TestTruncateTitle(t *testing.T) {
	cases := []struct {
		name string
		in   string
		max  int
		want string
	}{
		{"under budget unchanged", "short title", 60, "short title"},
		{"exact length unchanged", "0123456789", 10, "0123456789"},
		{"whole-word truncation",
			"this is a longer message that will definitely exceed the sixty character budget for a title",
			60,
			"this is a longer message that will definitely exceed the…"},
		{"no space within budget hard-cuts",
			"supercalifragilisticexpialidocioussupercalifragilisticexpialidocious",
			10,
			"supercali…"},
		{"multi-line collapses to one line",
			"first line\nsecond line\nthird line", 60,
			"first line second line third line"},
		{"empty input", "", 60, ""},
		{"whitespace-only input", "   \n\t  ", 60, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := truncateTitle(c.in, c.max)
			if got != c.want {
				t.Errorf("truncateTitle(%q, %d) = %q, want %q", c.in, c.max, got, c.want)
			}
			if runeLen(got) > c.max {
				t.Errorf("truncateTitle(%q, %d) = %q, exceeds max length %d", c.in, c.max, got, c.max)
			}
		})
	}
}

func runeLen(s string) int {
	n := 0
	for range s {
		n++
	}
	return n
}

// TestConversationAutoTitle covers AutoTitle end to end: it truncates and
// sets the session name, readable back through Get.
func TestConversationAutoTitle(t *testing.T) {
	dir := filepath.Join(t.TempDir(), ".datalog", "sessions")
	cm, err := newConversationManager(dir)
	if err != nil {
		t.Fatalf("newConversationManager: %v", err)
	}
	info, err := cm.Create(conversationModeQuery)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	long := "this is a longer message that will definitely exceed the sixty character budget for a title"
	if err := cm.AutoTitle(info.ID, long); err != nil {
		t.Fatalf("AutoTitle: %v", err)
	}

	got, err := cm.Get(info.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	want := truncateTitle(long, conversationTitleMaxLen)
	if got.Name != want {
		t.Fatalf("Name after AutoTitle = %q, want %q", got.Name, want)
	}
}

// -- per-mode tool registration golden lists ---------------------------------

// registeredToolNames wires mode into a fresh MCP server via
// registerToolsForMode and returns the sorted set of registered tool
// names — the golden-list assertion mechanism: a future tool that lands
// in the wrong mode's registration method changes this set and fails the
// test below, exactly what the task brief asks for ("a future tool added
// to the wrong mode fails a test").
func registeredToolNames(t *testing.T, h *mcpHandlers, mode toolMode) []string {
	t.Helper()
	srv := server.NewMCPServer("test", "0.0.0")
	h.registerToolsForMode(srv, mode)
	tools := srv.ListTools()
	names := make([]string, 0, len(tools))
	for name := range tools {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func assertToolSet(t *testing.T, got []string, want []string) {
	t.Helper()
	sort.Strings(want)
	if len(got) != len(want) {
		t.Fatalf("tool set = %v (%d), want %v (%d)", got, len(got), want, len(want))
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("tool set = %v, want %v", got, want)
		}
	}
}

// TestQueryModeToolSet pins Query Mode's exact tool list (doc/features/
// workbench-v2.md design decision 5's "Query Mode" bullet, as enumerated
// in this task's brief): read-only, no write tools registered at all.
func TestQueryModeToolSet(t *testing.T) {
	h, closeFn := newTestHandlers(t, t.TempDir())
	defer closeFn()
	got := registeredToolNames(t, h, toolModeQuery)
	assertToolSet(t, got, []string{
		"query", "list_predicates", "sample_facts", "explain", "explain_fact",
		"describe", "predicate_deps", "sample_input", "get_config",
		"list_rule_groups", "get_rule_group",
	})
}

// TestRulesModeToolSet pins Rules Mode's exact tool list: Query Mode's
// surface plus put_rule_group/delete_rule_group. NOT schema CRUD.
func TestRulesModeToolSet(t *testing.T) {
	h, closeFn := newTestHandlers(t, t.TempDir())
	defer closeFn()
	got := registeredToolNames(t, h, toolModeRules)
	assertToolSet(t, got, []string{
		"query", "list_predicates", "sample_facts", "explain", "explain_fact",
		"describe", "predicate_deps", "sample_input", "get_config",
		"list_rule_groups", "get_rule_group",
		"put_rule_group", "delete_rule_group",
	})
}

// TestFactsModeToolSet pins Facts Mode's exact tool list: sample_input,
// schema CRUD (put/delete source/matcher/declaration + get_config),
// list_predicates, sample_facts. NO query/explain/explain_fact/
// predicate_deps/rule tools.
func TestFactsModeToolSet(t *testing.T) {
	h, closeFn := newTestHandlers(t, t.TempDir())
	defer closeFn()
	got := registeredToolNames(t, h, toolModeFacts)
	assertToolSet(t, got, []string{
		"sample_input", "list_predicates", "sample_facts", "get_config",
		"put_source", "delete_source",
		"put_matcher", "delete_matcher",
		"put_declaration", "delete_declaration",
	})
}

// TestRegisterToolsIsEverything covers registerTools' contract (mcp.go): it
// is the union of all three modes' surfaces, exactly matching its
// pre-phase-2 "everything" behavior for the two call sites that still use
// it (runMCP, newWorkbench's single implicit session).
func TestRegisterToolsIsEverything(t *testing.T) {
	h, closeFn := newTestHandlers(t, t.TempDir())
	defer closeFn()
	srv := server.NewMCPServer("test", "0.0.0")
	h.registerTools(srv)
	tools := srv.ListTools()
	names := make([]string, 0, len(tools))
	for name := range tools {
		names = append(names, name)
	}
	sort.Strings(names)
	assertToolSet(t, names, []string{
		"query", "list_predicates", "sample_facts", "explain", "explain_fact",
		"describe", "predicate_deps", "sample_input", "get_config",
		"list_rule_groups", "get_rule_group",
		"put_rule_group", "delete_rule_group",
		"put_source", "delete_source",
		"put_matcher", "delete_matcher",
		"put_declaration", "delete_declaration",
	})
}

// -- global one-turn gate -----------------------------------------------------

// TestConversationTurnGateContention covers the one-turn-at-a-time
// contract: a second Begin call while the first is still held fails fast
// (rather than blocking) and names the running conversation, matching the
// task brief's "turn running in <name>" wording.
func TestConversationTurnGateContention(t *testing.T) {
	gate := newConversationTurnGate(newJobs())

	ctx1, done1, err := gate.Begin(context.Background(), "conv-1", "First Conversation")
	if err != nil {
		t.Fatalf("first Begin: %v", err)
	}
	if ctx1 == nil {
		t.Fatal("first Begin: nil context")
	}

	_, _, err = gate.Begin(context.Background(), "conv-2", "Second Conversation")
	if err == nil {
		t.Fatal("second Begin while first still held: want error, got nil")
	}
	if want := "turn running in First Conversation"; err.Error() != want {
		t.Fatalf("second Begin error = %q, want %q", err.Error(), want)
	}

	name, running := gate.Running()
	if !running || name != "First Conversation" {
		t.Fatalf("Running() = (%q, %v), want (%q, true)", name, running, "First Conversation")
	}

	done1()

	name, running = gate.Running()
	if running {
		t.Fatalf("Running() after done1() = (%q, %v), want running=false", name, running)
	}

	// After release, a new Begin succeeds.
	ctx3, done3, err := gate.Begin(context.Background(), "conv-3", "Third Conversation")
	if err != nil {
		t.Fatalf("Begin after release: %v", err)
	}
	if ctx3 == nil {
		t.Fatal("Begin after release: nil context")
	}
	done3()
}

// TestConversationTurnGateCancelReleases covers Global Cancel's reach into
// the gate: cancelling the parent context does not itself release the
// gate (jobs.Begin's contract — the caller's done() does that), but the
// gate's own ctx IS the one CancelAll would cancel via jobs.Cancel, so a
// cancelled turn's caller calling done() still frees the gate for the next
// conversation, exactly like the v1 Agent tab's jobs.Begin/done pairing.
func TestConversationTurnGateCancelReleases(t *testing.T) {
	j := newJobs()
	gate := newConversationTurnGate(j)

	_, done, err := gate.Begin(context.Background(), "conv-1", "Conversation")
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	j.Cancel(conversationTurnJobKey) // simulates Global Cancel / Stop
	done()                           // the turn's own cleanup still runs and releases the gate

	_, running := gate.Running()
	if running {
		t.Fatal("gate still held after done() following cancel")
	}
}

// appendUserMessage opens the session file at path directly (via
// kit.OpenTreeSession, the same offline seam conversation.go's own methods
// use) and appends one user message — a stand-in for what a real prompt
// turn's SessionManager.AppendMessage call does, without constructing a
// *kit.Kit (which would need model/provider config).
func appendUserMessage(t *testing.T, path, text string) {
	t.Helper()
	tm, err := kit.OpenTreeSession(path)
	if err != nil {
		t.Fatalf("opening %s: %v", path, err)
	}
	defer tm.Close()
	sm := kit.NewTreeManagerAdapter(tm)
	if _, err := sm.AppendMessage(kit.NewLLMUserMessage(text)); err != nil {
		t.Fatalf("appending message to %s: %v", path, err)
	}
}
