package chat

// engine.go carries the HTTP surface (routing, conversation CRUD, send/cancel/
// answer, the SSE feed, and the reference MCP mount) and the turn engine that
// drives a driver.Prompt call's sink events into transcript patches and store
// appends.  Adapted from datalog's conversations_http.go / console.go /
// agent.go, stripped of kit, modes, jobs, consent, and auto-allow per the
// design's strip list.

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/swdunlop/html-go"
	"github.com/swdunlop/html-go/datastar"
	"github.com/swdunlop/html-go/tag"
)

// ServeHTTP routes the component's HTTP surface under basePath.  The base
// prefix is stripped so the mux patterns are relative.
func (rt *runtime) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	base := strings.TrimRight(rt.cfg.basePath, "/")
	rel := strings.TrimPrefix(r.URL.Path, base)
	rel = strings.TrimPrefix(rel, "/")

	// Reference MCP mounts: base/mcp/{profile}.
	if strings.HasPrefix(rel, "mcp/") {
		rt.serveMCP(w, r, rel)
		return
	}

	switch {
	case rel == "conversations" && r.Method == http.MethodPost:
		rt.handleCreate(w, r)
	case rel == "select" && r.Method == http.MethodPost:
		rt.handleSelect(w, r)
	case rel == "delete" && r.Method == http.MethodPost:
		rt.handleDelete(w, r)
	case rel == "send" && r.Method == http.MethodPost:
		rt.handleSend(w, r)
	case rel == "cancel" && r.Method == http.MethodPost:
		rt.handleCancel(w, r)
	case rel == "answer" && r.Method == http.MethodPost:
		rt.handleAnswer(w, r)
	case rel == "events" && rt.cfg.ownBus:
		rt.handleEvents(w, r)
	default:
		http.NotFound(w, r)
	}
}

// serveMCP dispatches a reference MCP mount request: loopback-only, with a
// constant-time bearer check.
func (rt *runtime) serveMCP(w http.ResponseWriter, r *http.Request, rel string) {
	var mount *mcpMount
	for _, m := range rt.cfg.mcp {
		if m.path == rel {
			mount = m
			break
		}
	}
	if mount == nil {
		http.NotFound(w, r)
		return
	}
	// Loopback enforcement: the agent is a local subprocess, so a non-loopback
	// RemoteAddr is never a legitimate caller.
	if host, _, err := splitHostPortLoose(r.RemoteAddr); err != nil || !isLoopback(host) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	if !bearerMatch(r.Header.Get("Authorization"), mount.token) {
		w.Header().Set("WWW-Authenticate", `Bearer realm="chat mcp"`)
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	mount.handler.ServeHTTP(w, r)
}

// --- conversation CRUD ----------------------------------------------------

// handleCreate creates a conversation for the posted profile (default = first
// profile) and selects it, publishing the new rail + a blank transcript.
func (rt *runtime) handleCreate(w http.ResponseWriter, r *http.Request) {
	// Read form fields BEFORE starting the SSE stream: RequestStream commits the
	// response, after which reading the request body (r.FormValue → ParseForm)
	// is unreliable over the wire.  The rail's create/select/delete are form
	// POSTs, so their fields must be captured first.
	profileName := r.FormValue("profile")
	stream, err := datastar.RequestStream(w, r)
	if err != nil {
		return
	}
	if profileName == "" {
		profileName = rt.cfg.profiles[0].Name
	}
	if _, ok := rt.profileByName(profileName); !ok {
		_ = stream.Emit(datastar.Elements(rt.rail(rt.currentActive())))
		return
	}
	meta := ConversationMeta{
		ID:      newID(),
		Profile: profileName,
		Created: time.Now(),
	}
	if err := rt.cfg.store.Create(meta); err != nil {
		return
	}
	rt.selectConversation(meta.ID)
	rt.emitSelection(stream, meta.ID)
}

// handleSelect switches the active conversation, re-publishing the rail and
// replaying the transcript from the store (design decision 8: replay is the
// live rendering path).
func (rt *runtime) handleSelect(w http.ResponseWriter, r *http.Request) {
	id := r.FormValue("id") // before RequestStream commits the response (see handleCreate)
	stream, err := datastar.RequestStream(w, r)
	if err != nil {
		return
	}
	if _, _, err := rt.cfg.store.Read(id); err != nil {
		_ = stream.Emit(datastar.Elements(rt.rail(rt.currentActive())))
		return
	}
	rt.selectConversation(id)
	rt.emitSelection(stream, id)
}

// selectConversation sets the active conversation and resets its entry counter
// so a replay-then-live-turn numbering stays consistent (the counter tracks
// how many entries the transcript currently holds).
func (rt *runtime) selectConversation(id string) {
	_, entries, _ := rt.cfg.store.Read(id)
	rt.mu.Lock()
	rt.activeID = id
	rt.seq[id] = len(entries)
	rt.mu.Unlock()
}

// emitSelection pushes the rail (with the new active highlight), the replayed
// transcript, and the bound composer to the requesting page.
func (rt *runtime) emitSelection(stream datastar.Stream, id string) {
	_, entries, _ := rt.cfg.store.Read(id)
	_ = stream.Emit(datastar.Batch(
		datastar.Elements(rt.rail(id)),
		datastar.Elements(rt.transcript(id, entries)),
		datastar.Elements(rt.composerRegion(id)),
	))
}

// currentActive reads the active conversation id.
func (rt *runtime) currentActive() string {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return rt.activeID
}

// handleDelete removes a conversation; if it is active or holds the running
// turn, the driver is dropped (and turn cancelled) first.
func (rt *runtime) handleDelete(w http.ResponseWriter, r *http.Request) {
	id := r.FormValue("id") // before RequestStream commits the response (see handleCreate)
	stream, err := datastar.RequestStream(w, r)
	if err != nil {
		return
	}

	rt.mu.Lock()
	if rt.turnRunning && rt.turnOwner == id && rt.turnCancel != nil {
		rt.turnCancel()
	}
	var toClose driver
	if rt.liveID == id {
		toClose, rt.live, rt.liveID = rt.live, nil, ""
		rt.preambleSent = false
	}
	if rt.activeID == id {
		rt.activeID = ""
	}
	delete(rt.seq, id)
	rt.mu.Unlock()
	if toClose != nil {
		_ = toClose.Close()
	}

	_ = rt.cfg.store.Delete(id)
	// Re-publish the rail (without this conversation) and reset the pane to the
	// empty state.
	_ = stream.Emit(datastar.Batch(
		datastar.Elements(rt.rail(rt.currentActive())),
		datastar.Elements(rt.transcript("", nil)),
		datastar.Elements(rt.composerRegion("")),
	))
}

// --- send / turn engine ---------------------------------------------------

// promptFrom decodes the composer's bound prompt signal from a send POST.  The
// signal name is not hardcoded: it is read from cfg.signals.Prompt (the one
// SignalNames source) so a host that renamed the prompt signal via Signals(...)
// still reaches the server.  Decoding into a map is the chokepoint that keeps
// every signal-name consumer reading from that same source.
func (rt *runtime) promptFrom(r *http.Request) (string, error) {
	var sig map[string]any
	if err := datastar.Decode(&sig, r); err != nil {
		return "", err
	}
	text, _ := sig[rt.cfg.signals.Prompt].(string)
	return text, nil
}

// handleSend runs one agent turn in the active conversation.  The turn runs in
// a detached goroutine (it outlives the POST); the POST's own stream just
// clears the prompt input.  Busy state, entries, and errors travel the bus.
func (rt *runtime) handleSend(w http.ResponseWriter, r *http.Request) {
	prompt, decodeErr := rt.promptFrom(r)
	stream, err := datastar.RequestStream(w, r)
	if err != nil {
		return
	}

	rt.mu.Lock()
	id := rt.activeID
	rt.mu.Unlock()
	if id == "" {
		// No conversation to anchor an error to: refresh the rail with a hint so
		// the send is not silently dropped (design's no-silent-failure rule).
		_ = stream.Emit(datastar.Batch(
			datastar.Elements(rt.rail("")),
			datastar.Elements(rt.transcript("", nil)),
		))
		return
	}
	if decodeErr != nil {
		rt.appendAndStore(id, Entry{Event: &Event{Kind: EventError, Time: time.Now(),
			Text: fmt.Sprintf("could not read your message: %v", decodeErr)}})
		return
	}
	text := strings.TrimSpace(prompt)
	if text == "" {
		return
	}

	meta, entries, err := rt.cfg.store.Read(id)
	if err != nil {
		rt.appendAndStore(id, Entry{Event: &Event{Kind: EventError, Time: time.Now(),
			Text: fmt.Sprintf("conversation unavailable: %v", err)}})
		return
	}

	// Global one-turn gate: reject a concurrent send with a turn-elsewhere
	// note.  Publish busyConv/busyConvName so other composers show it.
	name := meta.Title
	if name == "" {
		name = truncateTitle(text, titleMaxLen)
	}
	turnCtx, ok, busyName := rt.beginTurn(id, name)
	if !ok {
		// Another turn is running.  Surface an explicit entry naming the
		// conversation that holds the turn (datalog's errConversationTurnBusy
		// wording), so the operator is not left guessing why the send did
		// nothing.  Transient: not stored.
		if busyName == "" {
			busyName = "another conversation"
		}
		rt.publishTransientError(id, "turn running in "+busyName)
		return
	}

	// Resolve the driver (spawns lazily; closes an idle driver for a different
	// conversation first — one live subprocess).  Done AFTER the gate so a
	// send elsewhere can't tear down the running conversation's agent.
	d, spawnErr := rt.conversationDriver(id, r.Host)
	if spawnErr != nil {
		rt.endTurn()
		rt.appendAndStore(id, Entry{Event: &Event{Kind: EventError, Time: time.Now(),
			Text: fmt.Sprintf("agent unavailable: %v", spawnErr)}})
		return
	}

	// Title the conversation from its first prompt if untitled, persisting it so
	// the rail shows it now and after a restart (no LLM auto-title).
	if meta.Title == "" && len(entries) == 0 && name != "" {
		if err := rt.cfg.store.Rename(id, name); err == nil {
			rt.publishRail()
		}
	}

	// Persist and render the user prompt.
	_ = stream.Emit(datastar.Signal(map[string]any{rt.cfg.signals.Prompt: ""}))
	rt.appendAndStore(id, Entry{Prompt: text})
	rt.publishBusy(id, name)

	// Preamble framing: prepend profile Instructions to the first prompt this
	// driver spawn serves (both new and cold-resumed conversations).  Only the
	// user's own text is stored/rendered.
	wire := rt.frameWire(id, meta.Profile, text)

	go func() {
		defer rt.endTurn()
		defer rt.publishBusy("", "")
		rt.runTurn(turnCtx, d, wire, id, name)
	}()
}

// beginTurn acquires the global one-turn gate for conversation id/name,
// deriving a cancellable ctx.  On contention it returns ok=false and the
// display name of the conversation already holding the gate, so the caller can
// name it in the rejection (datalog's errConversationTurnBusy).
func (rt *runtime) beginTurn(id, name string) (ctx context.Context, ok bool, busyName string) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if rt.turnRunning {
		return nil, false, rt.turnOwnerName
	}
	c, cancel := context.WithCancel(context.Background())
	rt.turnRunning = true
	rt.turnOwner, rt.turnOwnerName = id, name
	rt.turnCancel = cancel
	return c, true, ""
}

// endTurn releases the gate.
func (rt *runtime) endTurn() {
	rt.mu.Lock()
	if rt.turnCancel != nil {
		rt.turnCancel() // release the ctx's resources; no-op if already cancelled
	}
	rt.turnRunning = false
	rt.turnOwner, rt.turnOwnerName, rt.turnCancel = "", "", nil
	rt.mu.Unlock()
}

// frameWire builds the wire text for a prompt, prepending the profile
// Instructions preamble on the first prompt of a fresh driver spawn.
func (rt *runtime) frameWire(convID, profileName, text string) string {
	p, ok := rt.profileByName(profileName)
	if !ok || p.Instructions == "" {
		return text
	}
	rt.mu.Lock()
	first := !rt.preambleSent
	rt.preambleSent = true
	rt.mu.Unlock()
	if !first {
		return text
	}
	return p.Instructions + "\n\n" + text
}

// runTurn drives one turn's sink events into transcript patches + store
// appends, then settles the terminal state.  rt.mu is never held across the
// Prompt call.
//
// Streaming accumulation (ported from datalog agent.go's runAgentTurn): real
// ACP agents emit message/thought content as a run of chunks; consecutive
// chunks of one kind accumulate into ONE live entry, morphed in place, and
// finalize to the store as a single Event.  breakStreaming ends whatever
// message/thought entry is accumulating so the NEXT chunk starts a fresh entry
// at the bottom of the transcript rather than morphing something no longer
// adjacent to it (the live-observed interleaving bug: text, then a tool call,
// then more text would otherwise pool all the text at the top).  Any event that
// APPENDS a new entry — tool-call, permission, error, the plan's first update —
// breaks the accumulators; events that morph in place (tool-result, a later
// plan update) do not.  A message chunk resets the thought accumulator and vice
// versa: each kind's entry ends when a DIFFERENT kind appends.
func (rt *runtime) runTurn(ctx context.Context, d driver, wire, convID, name string) {
	var (
		// sinkMu orders accumulator mutations: a real ACP driver invokes the sink
		// from its own notification-pump goroutine, concurrently with the
		// post-Prompt finalize below.
		sinkMu      sync.Mutex
		msgElem     string          // live message entry id, "" when none is accumulating
		msgText     strings.Builder // accumulated message text
		thoughtElem string          // live thought entry id, "" when none is accumulating
		thoughtText strings.Builder // accumulated thought text
		planElem    string          // this turn's plan entry id, "" until its first update
		planState   []PlanEntry     // latest plan state, stored once at turn end
		gotReply    bool
	)

	// finalizeMessage / finalizeThought append the accumulated text to the store
	// as one Event (store only finalized events — never per chunk) and clear the
	// accumulator.  Called when an interrupting append breaks the stream, when
	// the other kind resets it, and at turn end.
	finalizeMessage := func() {
		if msgElem == "" {
			return
		}
		_ = rt.cfg.store.Append(convID, Entry{Event: &Event{Kind: EventMessage, Time: time.Now(), Text: msgText.String()}})
		msgElem = ""
		msgText.Reset()
	}
	finalizeThought := func() {
		if thoughtElem == "" {
			return
		}
		_ = rt.cfg.store.Append(convID, Entry{Event: &Event{Kind: EventThought, Time: time.Now(), Text: thoughtText.String()}})
		thoughtElem = ""
		thoughtText.Reset()
	}
	// breakStreaming ends both accumulators (finalizing each to the store) so the
	// next chunk starts a fresh entry.  Invoked by every appending event.
	breakStreaming := func() {
		finalizeMessage()
		finalizeThought()
	}

	sink := func(ev Event) {
		sinkMu.Lock()
		defer sinkMu.Unlock()
		if ev.Time.IsZero() {
			ev.Time = time.Now()
		}
		switch ev.Kind {
		case EventMessage:
			finalizeThought() // a different kind arrived; end any thought entry
			gotReply = true
			msgText.WriteString(ev.Text)
			live := Event{Kind: EventMessage, Time: ev.Time, Text: msgText.String()}
			if msgElem == "" {
				msgElem = rt.appendLive(convID, &live)
			} else {
				rt.updateEntry(convID, msgElem, &live)
			}
			return
		case EventThought:
			finalizeMessage() // a different kind arrived; end any message entry
			thoughtText.WriteString(ev.Text)
			live := Event{Kind: EventThought, Time: ev.Time, Text: thoughtText.String()}
			if thoughtElem == "" {
				thoughtElem = rt.appendLive(convID, &live)
			} else {
				rt.updateEntry(convID, thoughtElem, &live)
			}
			return
		case EventToolResult:
			// A morph in place when the call is known; does not break streaming.
			// The unknown-call fallback below appends, so it breaks like any append.
			elemID := toolElemID(convID, ev.ToolCallID)
			rt.updateEntry(convID, elemID, &ev)
			_ = rt.cfg.store.Append(convID, Entry{Event: &ev})
			return
		case EventToolCall:
			breakStreaming()
			elemID := toolElemID(convID, ev.ToolCallID)
			rt.publishAppend(convID, rt.eventEntry(convID, elemID, &ev, true))
			_ = rt.cfg.store.Append(convID, Entry{Event: &ev})
			return
		case EventPermission:
			breakStreaming()
			elemID := permElemID(convID, ev.RequestID)
			rt.publishAppend(convID, rt.eventEntry(convID, elemID, &ev, true))
			rt.mu.Lock()
			rt.pending[ev.RequestID] = pendingPerm{convID: convID, elemID: elemID, event: ev}
			rt.mu.Unlock()
			// The permission Event is part of the transcript; store it (replay
			// renders it answerable while pending, resolved once settled).
			_ = rt.cfg.store.Append(convID, Entry{Event: &ev})
			return
		case EventPlan:
			// The plan's first update appends (breaking streaming); every later
			// update morphs that same entry — ACP plan updates always carry the
			// complete list.  Intermediate states are never stored; the final
			// state is appended once at turn end (turn-scoped scaffolding).
			planState = ev.Plan
			if planElem == "" {
				breakStreaming()
				rt.mu.Lock()
				seq := rt.nextSeq(convID)
				rt.mu.Unlock()
				planElem = elemName(convID, seq)
				rt.publishAppend(convID, rt.eventEntry(convID, planElem, &ev, true))
			} else {
				rt.updateEntry(convID, planElem, &ev)
			}
			return
		case EventError:
			breakStreaming()
			rt.appendAndStore(convID, Entry{Event: &ev})
			return
		}
		// Any other kind appends and breaks streaming.
		breakStreaming()
		rt.appendAndStore(convID, Entry{Event: &ev})
	}

	stopReason, err := d.Prompt(ctx, wire, sink)

	// Finalize any still-accumulating stream and store the final plan state.
	// Under sinkMu: a late notification racing Prompt's return must not mutate
	// the accumulators mid-finalize.
	sinkMu.Lock()
	finalizeMessage()
	finalizeThought()
	if planElem != "" {
		_ = rt.cfg.store.Append(convID, Entry{Event: &Event{Kind: EventPlan, Time: time.Now(), Plan: planState}})
	}
	reply := gotReply
	sinkMu.Unlock()

	// Terminal state.
	switch {
	case ctx.Err() != nil:
		rt.appendAndStore(convID, Entry{Event: &Event{Kind: EventError, Time: time.Now(),
			Text: "turn cancelled"}})
	case err != nil:
		rt.appendAndStore(convID, Entry{Event: &Event{Kind: EventError, Time: time.Now(),
			Text: fmt.Sprintf("turn failed: %v", err)}})
		rt.dropDriver(d)
	case !reply:
		if stopReason == "" {
			stopReason = "unknown"
		}
		rt.appendAndStore(convID, Entry{Event: &Event{Kind: EventError, Time: time.Now(),
			Text: "the model ended the turn without a reply (stop reason: " + stopReason + ")"}})
	case stopReason != "" && stopReason != "stop" && stopReason != "end_turn" && stopReason != "tool_calls":
		rt.appendAndStore(convID, Entry{Event: &Event{Kind: EventError, Time: time.Now(),
			Text: "turn ended: " + stopReason}})
	}

	// Expire any permission cards the driver never resolved (cancel/crash).
	rt.expirePending(convID)
	rt.publishRail()
}

// --- entry rendering + patching -------------------------------------------

// appendLive renders an event at a fresh positional element id and appends it
// to the transcript over the bus WITHOUT storing it — the caller finalizes the
// stored form (used for streaming message/thought entries, which store their
// accumulated text once at finalization, not per chunk).  Returns the id.
func (rt *runtime) appendLive(convID string, ev *Event) string {
	rt.mu.Lock()
	seq := rt.nextSeq(convID)
	rt.mu.Unlock()
	elemID := elemName(convID, seq)
	rt.publishAppend(convID, rt.eventEntry(convID, elemID, ev, true))
	return elemID
}

// updateEntry morphs an existing entry in place (matched by its element id).
func (rt *runtime) updateEntry(convID, elemID string, ev *Event) {
	rt.cfg.bus.Publish(datastar.Elements(rt.eventEntry(convID, elemID, ev, true)))
}

// publishTransientError appends a visible error entry to a conversation's
// transcript WITHOUT storing it — for a user-triggered failure that must not be
// silent (a gate-busy rejection, a stale answer) but is not part of the
// persisted transcript.  A time-derived element id keeps it from colliding with
// the positional ids of stored entries.
func (rt *runtime) publishTransientError(convID, text string) {
	elemID := "e-" + convID + "-t" + strconv.FormatInt(time.Now().UnixNano(), 36)
	rt.publishAppend(convID, entryTag(elemID, "error",
		tag.New("p.chat-error", html.Text(text))))
}

// appendAndStore appends an entry (prompt or event) to the transcript over the
// bus and stores it.
func (rt *runtime) appendAndStore(convID string, entry Entry) {
	rt.mu.Lock()
	seq := rt.nextSeq(convID)
	rt.mu.Unlock()
	elemID := elemName(convID, seq)
	var rendered html.Content
	if entry.Prompt != "" {
		rendered = promptEntry(elemID, entry.Prompt)
	} else if entry.Event != nil {
		rendered = rt.eventEntry(convID, elemID, entry.Event, true)
	} else {
		return
	}
	rt.publishAppend(convID, rendered)
	_ = rt.cfg.store.Append(convID, entry)
}

// replayEntry renders one stored entry for transcript replay.  Morphable
// entries (tool calls, permissions) get content-derived stable ids so a
// concurrent live turn's morphs land on the replayed transcript after a
// mid-turn re-select.  A stored permission whose RequestID is STILL pending
// replays as an answerable card (the operator re-selected the running
// conversation mid-permission); only genuinely settled ones render resolved.
func (rt *runtime) replayEntry(convID string, seq int, entry Entry) html.Content {
	if entry.Prompt != "" {
		return promptEntry(elemName(convID, seq), entry.Prompt)
	}
	if entry.Event == nil {
		return html.Group{}
	}
	ev := entry.Event
	switch ev.Kind {
	case EventToolCall, EventToolResult:
		return rt.eventEntry(convID, toolElemID(convID, ev.ToolCallID), ev, false)
	case EventPermission:
		elemID := permElemID(convID, ev.RequestID)
		rt.mu.Lock()
		_, pending := rt.pending[ev.RequestID]
		rt.mu.Unlock()
		// answerable=true for a still-pending request so re-selecting the running
		// conversation mid-permission leaves the operator with live buttons.
		return rt.eventEntry(convID, elemID, ev, pending)
	default:
		return rt.eventEntry(convID, elemName(convID, seq), ev, false)
	}
}

// publishAppend fans one entry out to every open page, appended into the
// conversation's log region.
func (rt *runtime) publishAppend(convID string, rendered html.Content) {
	rt.cfg.bus.Publish(datastar.Elements(rendered,
		datastar.Selector("#"+rt.logID(convID)),
		datastar.Mode("append"),
	))
}

// publishBusy fans the busy signals out.
func (rt *runtime) publishBusy(convID, convName string) {
	key := ""
	if convID != "" {
		key = "agent"
	}
	s := rt.cfg.signals
	rt.cfg.bus.Publish(datastar.Signal(map[string]any{
		s.Busy: key, s.BusyConv: convID, s.BusyConvName: convName,
	}))
}

// publishRail re-renders the rail on every open page after a turn ends.
func (rt *runtime) publishRail() {
	rt.cfg.bus.Publish(datastar.Elements(rt.rail(rt.currentActive())))
}

// --- driver lifecycle -----------------------------------------------------

// conversationDriver returns the live driver for conversation id, spawning it
// on first use and closing an idle driver bound to a different conversation
// (design decision 14: one live subprocess).  reqHost carries the request Host
// so the MCP URL can be built when the ListenAddr option is unset.
func (rt *runtime) conversationDriver(id, reqHost string) (driver, error) {
	rt.mu.Lock()
	if rt.live != nil && rt.liveID == id {
		d := rt.live
		rt.mu.Unlock()
		return d, nil
	}
	old := rt.live
	rt.live, rt.liveID = nil, ""
	rt.mu.Unlock()
	if old != nil {
		_ = old.Close()
	}

	meta, _, err := rt.cfg.store.Read(id)
	if err != nil {
		return nil, err
	}
	profile, ok := rt.profileByName(meta.Profile)
	if !ok {
		return nil, fmt.Errorf("chat: unknown profile %q", meta.Profile)
	}

	mcpName, mcpURL, mcpToken := "", "", ""
	if mount, ok := rt.cfg.mcp[profile.Name]; ok {
		mcpName, mcpToken = profile.Name, mount.token
		mcpURL = rt.mcpURLFor(mount, reqHost)
	} else if profile.MCP.external {
		mcpName, mcpURL, mcpToken = profile.Name, profile.MCP.url, profile.MCP.token
	}

	d, err := rt.spawn(profile, mcpName, mcpURL, mcpToken)
	if err != nil {
		return nil, err
	}
	rt.mu.Lock()
	rt.live, rt.liveID = d, id
	rt.preambleSent = false // fresh spawn → preamble not yet sent
	rt.mu.Unlock()
	return d, nil
}

// spawn is the driver factory, overridable in tests via rt.newDriver.
func (rt *runtime) spawn(profile AgentProfile, mcpName, mcpURL, mcpToken string) (driver, error) {
	if rt.newDriver != nil {
		return rt.newDriver(profile, mcpName, mcpURL, mcpToken)
	}
	return newACPDriver(profile, mcpName, mcpURL, mcpToken)
}

// dropDriver discards the live driver after a fatal turn error so the next
// send respawns.
func (rt *runtime) dropDriver(d driver) {
	rt.mu.Lock()
	if rt.live == d {
		rt.live, rt.liveID = nil, ""
		rt.preambleSent = false
	}
	rt.mu.Unlock()
	_ = d.Close()
}

// --- cancel / answer ------------------------------------------------------

// handleCancel cancels the running turn's ctx.
func (rt *runtime) handleCancel(w http.ResponseWriter, r *http.Request) {
	if _, err := datastar.RequestStream(w, r); err != nil {
		return
	}
	rt.mu.Lock()
	cancel := rt.turnCancel
	rt.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// handleAnswer resolves a permission request and morphs its card to a resolved
// line.  The live driver is read directly — an answer with no live turn has
// nothing to construct a driver for.
func (rt *runtime) handleAnswer(w http.ResponseWriter, r *http.Request) {
	if _, err := datastar.RequestStream(w, r); err != nil {
		return
	}
	requestID := r.URL.Query().Get("requestID")
	optionID := r.URL.Query().Get("optionID")

	rt.mu.Lock()
	pending, ok := rt.pending[requestID]
	if ok {
		delete(rt.pending, requestID)
	}
	d := rt.live
	rt.mu.Unlock()
	if !ok {
		// The requestID is unknown (already resolved, expired by turn-end
		// cleanup, or a stale browser tab replaying an old page).  Anchor a
		// visible error to the conversation the button baked into its URL rather
		// than dropping the click silently (datalog console.go's stale-answer
		// path).  Transient: not stored.
		conv := r.URL.Query().Get("conv")
		if conv == "" {
			conv = rt.currentActive()
		}
		if conv != "" {
			rt.publishTransientError(conv, "this permission request is no longer waiting for an answer")
		}
		return
	}
	if d == nil {
		rt.updateEntryResolved(pending, "error: no agent is running to receive this answer")
		return
	}
	if err := d.Answer(requestID, optionID); err != nil {
		rt.updateEntryResolved(pending, fmt.Sprintf("error: %v", err))
		return
	}
	chosen := optionID
	for _, opt := range pending.event.Options {
		if opt.ID == optionID {
			chosen = opt.Name
			break
		}
	}
	rt.updateEntryResolved(pending, "answered: "+chosen)
}

// updateEntryResolved morphs a permission card to its resolved rendering.
func (rt *runtime) updateEntryResolved(p pendingPerm, note string) {
	ev := p.event
	rt.cfg.bus.Publish(datastar.Elements(
		entryTag(p.elemID, "permission", permissionResolvedBody(&ev, note)),
	))
}

// expirePending morphs every still-pending permission card for convID to a
// cancelled state and clears them (turn end cleanup).
func (rt *runtime) expirePending(convID string) {
	rt.mu.Lock()
	var stale []pendingPerm
	for id, p := range rt.pending {
		if p.convID == convID {
			stale = append(stale, p)
			delete(rt.pending, id)
		}
	}
	rt.mu.Unlock()
	for _, p := range stale {
		rt.updateEntryResolved(p, "cancelled: turn ended before the agent received an answer")
	}
}

// --- SSE feed -------------------------------------------------------------

// handleEvents serves the component's SSE feed (only when it owns the bus):
// subscribe BEFORE writing initial state so a patch landing mid-connect
// queues rather than vanishes, then stream until the client disconnects.
func (rt *runtime) handleEvents(w http.ResponseWriter, r *http.Request) {
	stream, err := datastar.RequestStream(w, r)
	if err != nil {
		return
	}
	bus, ok := rt.cfg.bus.(*DefaultBus)
	if !ok {
		return // ownBus is only true for a DefaultBus
	}
	sub := bus.Subscribe()
	defer sub.Close()

	// Replay the current busy signal so a tab opened mid-turn sees the running
	// state (and its Stop control) immediately.
	rt.mu.Lock()
	running, owner, ownerName := rt.turnRunning, rt.turnOwner, rt.turnOwnerName
	rt.mu.Unlock()
	if running {
		s := rt.cfg.signals
		_ = stream.Emit(datastar.Signal(map[string]any{
			s.Busy: "agent", s.BusyConv: owner, s.BusyConvName: ownerName,
		}))
	}

	for {
		select {
		case <-r.Context().Done():
			return
		case ev := <-sub.Events():
			if err := stream.Emit(ev); err != nil {
				return
			}
		}
	}
}
