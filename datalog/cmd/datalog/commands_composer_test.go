package main

import (
	"io"
	"strings"
	"testing"

	"github.com/mark3labs/kit/pkg/kit"
)

// newConversationWorkbench pairs a mordor workbench with a temp
// conversation store and one conversation in mode, returning both.
func newConversationWorkbench(t *testing.T, mode conversationMode) (*workbench, *conversationInfo) {
	t.Helper()
	wb := newMordorWorkbench(t)
	cm, err := newConversationManager(t.TempDir())
	if err != nil {
		t.Fatalf("newConversationManager: %v", err)
	}
	wb.conversations = cm
	conv, err := cm.Create(mode)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	return wb, conv
}

// TestComposerQueryCommand pins design decision 8's `?` command: it runs
// the query through the session pipeline, renders the result into the
// conversation's transcript, persists the pair as extension data, queues
// it for the next preamble — and never grants the agent a turn.
func TestComposerQueryCommand(t *testing.T) {
	wb, conv := newConversationWorkbench(t, conversationModeQuery)
	srv := startTestServer(wb)
	defer srv.Close()

	resp := postSignals(t, srv, "/c/"+conv.ID+"/send", map[string]any{
		"prompt": "? lateral_movement(U, S, T, P)?",
	})
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	log := renderLog(wb, conv.ID)
	if !strings.Contains(log, "pgustavo") {
		t.Fatalf("query result missing from transcript: %s", log)
	}
	if !strings.Contains(log, "? lateral_movement") {
		t.Fatalf("command echo missing from transcript: %s", log)
	}

	// No turn: no driver was ever constructed, and no user entry appended.
	wb.agentMu.Lock()
	agent := wb.agent
	wb.agentMu.Unlock()
	if agent != nil {
		t.Fatal("a ? command constructed an agent driver — commands must never grant a turn")
	}
	if strings.Contains(log, `class='console-entry user'`) {
		t.Fatalf("command rendered as a user chat message: %s", log)
	}

	// Queued for the next preamble and persisted in the session file.
	wb.cmdMu.Lock()
	nPending := len(wb.pendingCmds[conv.ID])
	wb.cmdMu.Unlock()
	if nPending != 1 {
		t.Fatalf("pending command queue = %d entries, want 1", nPending)
	}
	tm, err := kit.OpenTreeSession(conv.Path)
	if err != nil {
		t.Fatalf("OpenTreeSession: %v", err)
	}
	defer tm.Close()
	entries := kit.NewTreeManagerAdapter(tm).GetExtensionData(commandExtType)
	if len(entries) != 1 || !strings.Contains(entries[0].Data, "lateral_movement") {
		t.Fatalf("command not persisted as extension data: %+v", entries)
	}
}

// TestComposerExprCommand pins the `!` command: Facts Mode only, needs a
// Data-tab selection, and evaluates with the mapping-expr environment
// (value bound to the decoded record, assert collected not loaded).
func TestComposerExprCommand(t *testing.T) {
	wb, conv := newConversationWorkbench(t, conversationModeFacts)
	srv := startTestServer(wb)
	defer srv.Close()

	// No selection yet: inline error.
	resp := postSignals(t, srv, "/c/"+conv.ID+"/send", map[string]any{"prompt": "! value.host"})
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if log := renderLog(wb, conv.ID); !strings.Contains(log, "no record selected") {
		t.Fatalf("missing no-selection error: %s", log)
	}

	wb.selMu.Lock()
	wb.selFile, wb.selRow, wb.selRecord, wb.selValid = "events.jsonl", 0,
		`{"host": "WS6", "cmd": "GruntHTTP.exe"}`, true
	wb.selMu.Unlock()

	resp = postSignals(t, srv, "/c/"+conv.ID+"/send", map[string]any{
		"prompt": `! assert("probe", [value.host]); value.cmd`,
	})
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	log := renderLog(wb, conv.ID)
	if !strings.Contains(log, "GruntHTTP.exe") {
		t.Fatalf("expr result missing: %s", log)
	}
	if !strings.Contains(log, `probe(&quot;WS6&quot;)`) && !strings.Contains(log, `probe("WS6")`) {
		t.Fatalf("asserted fact missing: %s", log)
	}

	// Query-mode conversations refuse ! (design decision 8: Facts Mode).
	wbQ, convQ := newConversationWorkbench(t, conversationModeQuery)
	srvQ := startTestServer(wbQ)
	defer srvQ.Close()
	respQ := postSignals(t, srvQ, "/c/"+convQ.ID+"/send", map[string]any{"prompt": "! value.host"})
	io.Copy(io.Discard, respQ.Body)
	respQ.Body.Close()
	if log := renderLog(wbQ, convQ.ID); !strings.Contains(log, "Facts conversations only") {
		t.Fatalf("query-mode ! not refused: %s", log)
	}
}

// TestComposerPreamblePrependedToNextTurn pins the decision-8 preamble:
// commands run between turns ride the NEXT prompt framed as the user's own
// actions, disk reloads between turns are noted, the transcript shows only
// the human's text, and consuming clears the queue.
func TestComposerPreamblePrependedToNextTurn(t *testing.T) {
	wb, conv := newConversationWorkbench(t, conversationModeQuery)
	srv := startTestServer(wb)
	defer srv.Close()

	driver := &fakeDriver{events: []agentEvent{{Kind: "message", Text: "done"}}, stopReason: "stop"}
	wb.agent = driver
	wb.agentConvID = conv.ID

	// Turn 1 establishes the conversation's reload watermark.
	resp := postSignals(t, srv, "/c/"+conv.ID+"/send", map[string]any{"prompt": "hello"})
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	waitFor(t, func() bool { return len(driver.promptTexts()) == 1 })

	// Between turns: one command and one disk reload.
	resp = postSignals(t, srv, "/c/"+conv.ID+"/send", map[string]any{
		"prompt": "? lateral_movement(U, S, T, P)?",
	})
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	wb.recordReload(reloadStatus{Changed: []string{"rule group at_risk/2 modified"}})

	// Turn 2 carries both in the preamble, above the user's text.
	resp = postSignals(t, srv, "/c/"+conv.ID+"/send", map[string]any{"prompt": "and now?"})
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	waitFor(t, func() bool { return len(driver.promptTexts()) == 2 })

	prompts := driver.promptTexts()
	if strings.Contains(prompts[0], "workbench context") {
		t.Fatalf("first turn carried a preamble with nothing to say: %q", prompts[0])
	}
	second := prompts[1]
	for _, want := range []string{
		"[workbench context]",
		"user ran these workbench commands",
		"? lateral_movement(U, S, T, P)?",
		"pgustavo", // the command's result rides along
		"changed on disk since your last turn: rule group at_risk/2 modified",
		"[end workbench context]",
	} {
		if !strings.Contains(second, want) {
			t.Fatalf("second prompt missing %q:\n%s", want, second)
		}
	}
	if !strings.HasSuffix(second, "and now?") {
		t.Fatalf("user's own text must end the prompt: %q", second)
	}
	// The transcript shows the user's words, not the framing.
	if log := renderLog(wb, conv.ID); strings.Contains(log, "workbench context") {
		t.Fatalf("preamble leaked into the transcript: %s", log)
	}

	// Queue consumed: a third turn has no preamble.
	resp = postSignals(t, srv, "/c/"+conv.ID+"/send", map[string]any{"prompt": "again"})
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	waitFor(t, func() bool { return len(driver.promptTexts()) == 3 })
	if third := driver.promptTexts()[2]; strings.Contains(third, "workbench context") {
		t.Fatalf("consumed preamble reappeared: %q", third)
	}
}

// TestRenderSessionHistoryInterleavesCommands pins the resume rendering:
// persisted command entries land between the turns they ran between, and a
// persisted prompt's preamble framing is stripped back to the human's own
// words.
func TestRenderSessionHistoryInterleavesCommands(t *testing.T) {
	wb, conv := newConversationWorkbench(t, conversationModeQuery)

	appendUser := func(text string) {
		tm, err := kit.OpenTreeSession(conv.Path)
		if err != nil {
			t.Fatalf("OpenTreeSession: %v", err)
		}
		defer tm.Close()
		_, err = kit.NewTreeManagerAdapter(tm).AppendMessage(kit.LLMMessage{
			Role:    kit.LLMRoleUser,
			Content: []kit.LLMMessagePart{kit.LLMTextPart{Text: text}},
		})
		if err != nil {
			t.Fatalf("AppendMessage: %v", err)
		}
	}

	appendUser("first question")
	wb.persistCommand(conv.ID, commandRecord{Kind: "query", Input: "foo(X)?", Result: "no results"})
	appendUser(framePromptWithPreamble("The user ran these workbench commands...", "second question"))

	tm, err := kit.OpenTreeSession(conv.Path)
	if err != nil {
		t.Fatalf("OpenTreeSession: %v", err)
	}
	defer tm.Close()
	entries := renderSessionHistory(tm)

	kinds := make([]string, len(entries))
	for i, e := range entries {
		kinds[i] = e.kind
	}
	want := []string{"user", "query", "user"}
	if strings.Join(kinds, ",") != strings.Join(want, ",") {
		t.Fatalf("history kinds = %v, want %v", kinds, want)
	}

	var all string
	for _, e := range entries {
		all += renderContent(e.content)
	}
	if !strings.Contains(all, "foo(X)?") || !strings.Contains(all, "no results") {
		t.Fatalf("command entry content missing: %s", all)
	}
	if strings.Contains(all, "workbench context") {
		t.Fatalf("preamble framing not stripped from resumed prompt: %s", all)
	}
	if !strings.Contains(all, "second question") {
		t.Fatalf("stripped prompt lost the user's text: %s", all)
	}
}
