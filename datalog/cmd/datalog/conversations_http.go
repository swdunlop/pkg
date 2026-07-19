package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/mark3labs/kit/pkg/kit"
	html "github.com/swdunlop/html-go"
	"github.com/swdunlop/html-go/datastar"
	"github.com/swdunlop/html-go/tag"
	"swdunlop.dev/pkg/datalog/cmd/datalog/view"
)

// This file is the conversation UI's HTTP surface (doc/features/
// workbench-v2.md phase 2): the full-page render (rail + transcript +
// composer on the left, browser on the right), conversation create/delete
// (plain form POSTs with real navigation — creating or deleting a
// conversation is a URL change, not a Datastar patch), and the send path
// that runs one agent turn under the global one-turn gate
// (conversation.go's conversationTurnGate).

// handleRoot (GET /) lands on the newest conversation, or the empty state
// when none exist yet.
func (wb *workbench) handleRoot(w http.ResponseWriter, r *http.Request) {
	if wb.conversations != nil {
		if infos, err := wb.conversations.List(); err == nil && len(infos) > 0 {
			http.Redirect(w, r, "/c/"+infos[0].ID, http.StatusFound)
			return
		}
	}
	wb.renderConversationPage(w, nil)
}

// handleConversationPage (GET /c/{id}) renders the full page with that
// conversation active. An unknown id (deleted in another tab, stale
// bookmark) falls back to the root redirect rather than a bare 404 — the
// rail is the recovery affordance.
func (wb *workbench) handleConversationPage(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if wb.conversations == nil {
		http.Error(w, "conversations unavailable (see server log)", http.StatusServiceUnavailable)
		return
	}
	info, err := wb.conversations.Get(id)
	if err != nil {
		http.Redirect(w, r, "/", http.StatusFound)
		return
	}
	wb.seedTranscript(info)
	wb.renderConversationPage(w, info)
}

// renderConversationPage renders the two-half page. active is nil for the
// empty state (no conversation selected).
func (wb *workbench) renderConversationPage(w http.ResponseWriter, active *conversationInfo) {
	wb.h.mu.Lock()
	schemaText := wb.h.sess.schemaText
	rulesText := wb.h.sess.rulesText
	wb.h.mu.Unlock()

	var transcript, composer html.Content
	if active != nil {
		transcript = view.Transcript(active.ID, wb.console.Render(active.ID)...)
		composer = view.Composer(active.ID)
	} else {
		transcript = view.EmptyState()
		composer = html.Group{}
	}

	page := view.Page{
		Title: "Datalog Workbench",
		Left:  view.ConversationPane(wb.renderRail(active), transcript, composer),
		Right: view.Browser(schemaText, rulesText),
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	buf := html.Append(nil, page)
	_, _ = w.Write(buf)
}

// renderRail builds the rail from the conversation list (newest-first per
// conversationManager.List). Shared by page loads and the post-turn rail
// refresh (publishRail).
func (wb *workbench) renderRail(active *conversationInfo) html.Content {
	items := []view.RailItem{}
	if wb.conversations != nil {
		infos, err := wb.conversations.List()
		if err == nil {
			for _, info := range infos {
				items = append(items, view.RailItem{
					ID:     info.ID,
					Title:  conversationTitle(info),
					Mode:   string(info.Mode),
					Active: active != nil && info.ID == active.ID,
				})
			}
		}
	}
	return view.Rail(items)
}

// publishRail re-renders the rail on every open page — called after a
// turn ends (Modified order and the auto-title may have changed). The rail
// renders with no active highlight here: each page's own highlight comes
// from its full-page render, and morphing it away on OTHER pages would be
// wrong anyway; the CSS keeps the .active class only on the morphed-in
// content, so the sender's page re-highlights on its next navigation.
// Single-user, two-tabs-max makes this a cosmetic wrinkle, not a bug.
func (wb *workbench) publishRail() {
	wb.bus.Publish(datastar.Elements(wb.renderRail(nil)))
}

// conversationTitle picks the rail label: the auto-title (session name)
// first, the first message as a fallback for sessions that predate their
// title, then "untitled".
func conversationTitle(info conversationInfo) string {
	if info.Name != "" {
		return info.Name
	}
	if info.FirstMessage != "" {
		return truncateTitle(info.FirstMessage, conversationTitleMaxLen)
	}
	return ""
}

// handleConversationCreate (POST /conversations) creates a conversation in
// the picked mode and navigates to it — a plain form POST/redirect, not a
// Datastar action (view/conversation.go's Rail doc comment).
func (wb *workbench) handleConversationCreate(w http.ResponseWriter, r *http.Request) {
	if wb.conversations == nil {
		http.Error(w, "conversations unavailable (see server log)", http.StatusServiceUnavailable)
		return
	}
	mode := conversationMode(r.FormValue("mode"))
	info, err := wb.conversations.Create(mode)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	http.Redirect(w, r, "/c/"+info.ID, http.StatusSeeOther)
}

// handleConversationDelete (POST /c/{id}/delete) removes a conversation:
// cancels its turn if one is running, drops its cached driver, deletes the
// session file, and navigates home. The confirm() guard lives client-side
// in the rail's delete form.
func (wb *workbench) handleConversationDelete(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if wb.conversations == nil {
		http.Error(w, "conversations unavailable (see server log)", http.StatusServiceUnavailable)
		return
	}
	if owner, running := wb.turnGate.Running(); running && owner != "" {
		if ownerID, _ := wb.turnGate.RunningID(); ownerID == id {
			wb.jobs.Cancel(conversationTurnJobKey)
		}
	}
	wb.dropConversationDriver(id)
	wb.console.Clear(id)
	wb.cmdMu.Lock()
	delete(wb.pendingCmds, id)
	delete(wb.reloadSeen, id)
	wb.cmdMu.Unlock()
	if err := wb.conversations.Delete(id); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

// sendSignals mirrors the composer's bound signals (view/conversation.go).
type sendSignals struct {
	Prompt string `json:"prompt"`
}

// handleConversationSend (POST /c/{id}/send) runs one agent turn in this
// conversation. The turn runs in a background goroutine detached from the
// request context — a turn takes as long as the model takes, must survive
// the POST returning, and every page watches it over /events anyway. The
// POST's own stream just clears the prompt input; busy-state, transcript
// entries, and errors all travel the bus.
func (wb *workbench) handleConversationSend(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	var sig sendSignals
	decodeErr := datastar.Decode(&sig, r)

	stream, err := datastar.RequestStream(w, r)
	if err != nil {
		return
	}
	if wb.conversations == nil {
		wb.consoleAppend(id, "error", html.Text("conversations unavailable (see server log)"))
		return
	}
	if decodeErr != nil {
		wb.consoleAppend(id, "error", html.Text(decodeErr.Error()))
		return
	}
	text := strings.TrimSpace(sig.Prompt)
	if text == "" {
		return
	}

	info, err := wb.conversations.Get(id)
	if err != nil {
		wb.consoleAppend(id, "error", html.Text(fmt.Sprintf("conversation unavailable: %v", err)))
		return
	}

	// `?`/`!` commands run and return — a command never grants the agent a
	// turn (design decision 8; commands_composer.go).
	if kind, rest, isCmd := composerCommand(text); isCmd {
		_ = stream.Emit(datastar.Signal(map[string]any{"prompt": ""}))
		wb.runComposerCommand(id, info.Mode, kind, rest)
		return
	}
	name := conversationTitle(*info)
	if name == "" {
		name = "a new conversation"
	}

	// Acquire the gate BEFORE touching the driver cache: conversationDriver
	// closes whichever OTHER conversation's driver is cached, so resolving
	// it first would tear down the running conversation's agent mid-turn
	// just by clicking send elsewhere — the gate is what makes that switch
	// safe. context.Background, not r.Context(): the turn outlives the
	// POST. The gate's jobs entry is the turn's only cancellation path
	// (Stop / CancelAll), and it is global — one turn across ALL
	// conversations (design decision 6).
	jobCtx, done, gateErr := wb.turnGate.Begin(context.Background(), id, name)
	if gateErr != nil {
		wb.consoleAppend(id, "error", html.Text(gateErr.Error()))
		return
	}

	driver, err := wb.conversationDriver(id, info.Mode)
	if err != nil {
		done()
		wb.consoleAppend(id, "error", html.Text(fmt.Sprintf(
			"agent unavailable: %v — configure a model with --model (or KIT_MODEL / ~/.kit.yml) and restart serve", err)))
		return
	}

	// Auto-title on the conversation's first user message (design decision
	// 6), through the LIVE driver rather than a second file handle — the
	// kit session is already open under it. Best-effort: a failed rename
	// must not block the turn.
	if info.MessageCount == 0 {
		if namer, ok := driver.(interface{ SetName(string) error }); ok {
			if title := truncateTitle(text, conversationTitleMaxLen); title != "" {
				_ = namer.SetName(title)
				name = title
			}
		} else if info.Name == "" {
			// A driver with no session of its own (ACP) still gets a rail
			// title: the manager's offline append is safe here precisely
			// because no kit instance holds the file. Guarded on Name ==
			// "" so a restart (which resets MessageCount to the persisted
			// zero — ACP turns never append messages) cannot overwrite an
			// earlier title with a later first-send.
			if title := truncateTitle(text, conversationTitleMaxLen); title != "" {
				_ = wb.conversations.AutoTitle(id, title)
				name = title
			}
		}
	}

	_ = stream.Emit(datastar.Signal(map[string]any{"prompt": ""}))
	wb.consoleAppend(id, "user", html.Text(text))
	wb.publishBusyConv(id, name)

	// The prompt the model sees carries the workbench preamble (design
	// decision 8): command/result pairs run since its last turn, plus a
	// disk-change notice when fsnotify reloaded between turns. Consumed
	// under the gate, so a racing command lands in the NEXT turn's
	// preamble, never a torn half of this one. The transcript shows only
	// the user's own text — the commands already rendered when they ran.
	promptText := framePromptWithPreamble(wb.consumePreamble(id), text)

	// Design decision 7's ACP leg: a driver that cannot carry a
	// per-conversation system prompt (acpDriver — the agent subprocess owns
	// its own) gets the mode instructions as a preamble on the
	// conversation's FIRST prompt instead. Instruction-only scoping: the
	// shared /mcp mount still exposes the full tool surface to an ACP
	// agent — per-conversation tool filtering (a conversation-identity
	// bearer token) stays deferred per the feature doc's risk note.
	promptText = wb.frameModePreamble(driver, id, info.Mode, promptText)

	go func() {
		defer done()
		defer wb.publishBusy("")
		wb.runAgentTurn(jobCtx, driver, promptText, id)
		wb.publishRail()
	}()
}

// conversationDriver returns the cached driver for conversation id,
// constructing it on first use (and closing whichever OTHER conversation's
// driver was cached — one live agent at a time, matching the one-turn
// gate). With --agent set, the shared ACP driver serves every conversation
// (decision 7: ACP is the second-class citizen; per-conversation
// persistence and mode preambles are the phase-2e leg); otherwise each
// conversation gets its own kit agent bound to its session file via
// newConversationKit, so turns persist and resume with full fidelity.
func (wb *workbench) conversationDriver(id string, mode conversationMode) (agentDriver, error) {
	wb.agentMu.Lock()
	defer wb.agentMu.Unlock()
	if wb.agent != nil && wb.agentConvID == id {
		return wb.agent, nil
	}
	if wb.agentCfg.AgentCommand != "" {
		// The shared ACP driver is conversation-agnostic; reuse it across
		// switches rather than respawning the subprocess per conversation.
		if wb.agent != nil {
			wb.agentConvID = id
			return wb.agent, nil
		}
		d, err := newACPDriver(wb.agentCfg)
		if err != nil {
			return nil, err
		}
		wb.agent, wb.agentConvID = d, id
		return d, nil
	}
	if wb.agent != nil {
		_ = wb.agent.Close()
		wb.agent = nil
	}
	consent := newConsentGate(wb, id)
	k, err := newConversationKit(context.Background(), wb.agentCfg, wb.h, wb.conversations.sessionPath(id), mode, consent)
	if err != nil {
		return nil, err
	}
	d := &kitDriver{k: k, consent: consent}
	wb.agent, wb.agentConvID = d, id
	return d, nil
}

// dropConversationDriver closes and forgets the cached driver if it is
// bound to conversation id (delete path; a fatal turn error goes through
// dropAgentDriver instead, which matches by driver identity).
func (wb *workbench) dropConversationDriver(id string) {
	wb.agentMu.Lock()
	var d agentDriver
	if wb.agent != nil && wb.agentConvID == id {
		d, wb.agent, wb.agentConvID = wb.agent, nil, ""
	}
	wb.agentMu.Unlock()
	if d != nil {
		_ = d.Close()
	}
}

// seedTranscript warms the in-memory transcript scrollback (consoleLog,
// tab = conversation id) from the conversation's persisted kit session the
// first time it is opened after a server start: page loads render from
// consoleLog alone, so without this a restart would show every old
// conversation as empty even though the session file holds its history.
// Idempotent by construction — it only runs when consoleLog has nothing
// for this conversation, and in-session every live entry already lands in
// consoleLog, so a conversation that has been active since startup is
// never reseeded. Appends go straight to wb.console (no bus publish): the
// requesting page renders the result directly, and any other open page
// either shows a different conversation (append target absent, no-op) or
// already showed these entries.
func (wb *workbench) seedTranscript(info *conversationInfo) {
	if len(wb.console.Render(info.ID)) > 0 || info.MessageCount == 0 {
		return
	}
	tm, err := kit.OpenTreeSession(info.Path)
	if err != nil {
		return // unreadable history is not fatal to the page; live turns still work
	}
	defer tm.Close()
	for _, entry := range renderSessionHistory(tm) {
		wb.console.Append(info.ID, entry.kind, entry.content)
	}
}

// historyEntry is one rendered transcript entry from a persisted session.
type historyEntry struct {
	kind    string
	content html.Content
}

// renderSessionHistory projects a kit session into the transcript's entry
// vocabulary — the same shapes runAgentTurn streams live
// (user/agent/thought/tool), so a resumed conversation reads identically
// to the turn as it happened. It walks the session tree's current branch
// so persisted `?`/`!` command entries (commandExtType extension data,
// design decision 8) land in order BETWEEN the turns they actually ran
// between, not lumped at either end. Tool results are correlated to their
// calls by ToolCallID first, so each tool entry renders complete (call +
// result) the way a live entry ends up after its result morph. The user's
// prompt text is stripped of any workbench preamble framing
// (framePromptWithPreamble) so the transcript shows what the human typed,
// as it did live.
func renderSessionHistory(tm *kit.TreeManager) []historyEntry {
	sm := kit.NewTreeManagerAdapter(tm)
	msgs := tm.GetLLMMessages()

	results := map[string]agentEvent{}
	for _, msg := range msgs {
		if string(msg.Role) != string(kit.LLMRoleTool) {
			continue
		}
		for _, part := range msg.Content {
			if p, ok := part.(kit.LLMToolResultPart); ok {
				text, isErr := toolResultText(p)
				results[p.ToolCallID] = agentEvent{Result: text, IsError: isErr}
			}
		}
	}

	// Persisted command entries, keyed by tree-entry id so the branch walk
	// below can render each at its own position.
	cmdEntries := map[string]commandRecord{}
	for _, e := range sm.GetExtensionData(commandExtType) {
		var rec commandRecord
		if json.Unmarshal([]byte(e.Data), &rec) == nil {
			cmdEntries[e.ID] = rec
		}
	}

	renderMessage := func(msg kit.LLMMessage) []historyEntry {
		var out []historyEntry
		switch string(msg.Role) {
		case string(kit.LLMRoleUser):
			if text := strippedPreamble(messageText(msg)); text != "" {
				out = append(out, historyEntry{kind: "user", content: html.Text(text)})
			}
		case string(kit.LLMRoleAssistant):
			for _, part := range msg.Content {
				switch p := part.(type) {
				case kit.LLMTextPart:
					if p.Text != "" {
						out = append(out, historyEntry{kind: "agent", content: html.Text(p.Text)})
					}
				case kit.LLMReasoningPart:
					if p.Text != "" {
						out = append(out, historyEntry{kind: "thought", content: thoughtEntry(p.Text)})
					}
				case kit.LLMToolCallPart:
					ev := agentEvent{ToolCallID: p.ToolCallID, ToolName: p.ToolName, ToolArgs: p.Input}
					res, done := results[p.ToolCallID]
					if done {
						ev.Result, ev.IsError = res.Result, res.IsError
					}
					out = append(out, historyEntry{kind: "tool", content: toolEntry(ev, done)})
				}
			}
		}
		return out
	}

	var out []historyEntry
	msgIdx := 0
	for _, be := range sm.GetCurrentBranch() {
		if rec, ok := cmdEntries[be.ID]; ok {
			out = append(out, historyEntry{kind: "query", content: commandHistoryEntry(rec)})
			continue
		}
		if string(be.Type) == "message" && msgIdx < len(msgs) {
			out = append(out, renderMessage(msgs[msgIdx])...)
			msgIdx++
		}
	}
	// A branch shape this walk doesn't recognize (compaction collapsing
	// entries, a future kit format change) must not silently drop turns:
	// render any messages the walk didn't reach, unpositioned but present.
	for ; msgIdx < len(msgs); msgIdx++ {
		out = append(out, renderMessage(msgs[msgIdx])...)
	}
	return out
}

// commandHistoryEntry renders a persisted command/result pair on resume —
// the plain-text sibling of the live rendering (runComposerCommand), which
// carried richer HTML the record deliberately doesn't store.
func commandHistoryEntry(rec commandRecord) html.Content {
	marker := "? "
	if rec.Kind == "expr" {
		marker = "! "
	}
	return html.Group{
		queryEcho(marker + rec.Input),
		tag.New("pre", html.Text(rec.Result)),
	}
}

// strippedPreamble removes the workbench-context framing a persisted
// prompt may carry (framePromptWithPreamble) so a resumed transcript shows
// the human's own words, as the live transcript did.
func strippedPreamble(text string) string {
	const endMark = "[end workbench context]\n\n"
	if strings.HasPrefix(text, "[workbench context]\n") {
		if i := strings.Index(text, endMark); i >= 0 {
			return text[i+len(endMark):]
		}
	}
	return text
}

// messageText joins a message's plain-text parts — a user message is
// always a single TextPart today, but joining is cheap insurance against a
// future multi-part shape.
func messageText(msg kit.LLMMessage) string {
	var parts []string
	for _, part := range msg.Content {
		if p, ok := part.(kit.LLMTextPart); ok && p.Text != "" {
			parts = append(parts, p.Text)
		}
	}
	return strings.Join(parts, "\n")
}

// toolResultText extracts a persisted tool result's text (and error-ness)
// from its typed Output. The concrete fantasy output types aren't aliased
// by kit, so this goes through their JSON form — MarshalJSON is the one
// stable surface they all share — rather than importing charm.land/fantasy
// directly for a type switch.
func toolResultText(p kit.LLMToolResultPart) (string, bool) {
	b, err := json.Marshal(p.Output)
	if err != nil {
		return "", false
	}
	var probe struct {
		Text  string          `json:"text"`
		Error json.RawMessage `json:"error"`
		Value json.RawMessage `json:"value"`
	}
	if json.Unmarshal(b, &probe) != nil {
		return string(b), false
	}
	switch {
	case len(probe.Error) > 0:
		return string(probe.Error), true
	case probe.Text != "":
		return probe.Text, false
	case len(probe.Value) > 0:
		return string(probe.Value), false
	default:
		return string(b), false
	}
}
