package chat

// This file renders the component's HTML: the root pane (rail + transcript +
// composer), and the per-entry transcript renderings shared by the live turn
// engine and store replay (design decision 8 — one rendering path).  The
// idioms are adapted from datalog's cmd/datalog/view package; class hooks are
// kept light and no CSS ships in this phase (design decision 12).

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"

	"github.com/swdunlop/html-go"
	"github.com/swdunlop/html-go/tag"
)

// signalsJSON builds the data-signals object initializing the four configured
// signal names at the component's root tag (design decision 9: scoped at our
// top-level tag).
func (rt *runtime) signalsJSON() string {
	s := rt.cfg.signals
	obj := map[string]any{s.Busy: "", s.BusyConv: "", s.BusyConvName: "", s.Prompt: ""}
	b, _ := json.Marshal(obj)
	return string(b)
}

// root renders the fresh-page hydration view (AppendHTML): the rail of
// choices, the empty-state transcript region, the composer, the signal
// initialization, and — when ownBus — the SSE connect that starts the event
// pump.  It deliberately does NOT restore the active conversation's transcript:
// selection and replay happen only via bus patches from the select route after
// the user picks a conversation.  The component still tracks the active
// conversation server-side (driver lifetime, turn gate), but that never leaks
// into this initial HTML.
func (rt *runtime) root() html.Content {
	container := tag.New("section.chat").
		Set("id", rt.elemID("root")).
		Set("data-signals", rt.signalsJSON())

	// When the component owns the bus it must open the one SSE feed on load.
	// data-init with datalog's proven options: keep the connection when the tab
	// is hidden and disable Datastar's per-request cancellation so the
	// long-lived stream isn't torn down (view/page.go's /events wiring).
	if rt.cfg.ownBus {
		container = container.Set("data-init",
			"@get('"+rt.path("events")+"', {openWhenHidden: true, requestCancellation: 'disabled'})")
	}

	return container.Add(
		rt.rail(""),
		tag.New("div.chat-main").Set("id", rt.elemID("main")).Add(
			rt.transcript("", nil),
			rt.composerRegion(""),
		),
	)
}

// composerRegion wraps the composer in a stable-id container so the select
// route can morph the whole composer (idle for the empty state, bound to the
// chosen conversation after selection) in one patch.
func (rt *runtime) composerRegion(active string) html.Content {
	return tag.New("div.chat-composer-region").
		Set("id", rt.elemID("composer-region")).
		Add(rt.composer(active))
}

// rail renders the conversation list plus a New-conversation control per
// registered profile (design decision 7: one button per profile).  Every rail
// control is a datastar @post action, never a native form: the component has
// no pages of its own, so a browser navigation would land on the raw SSE
// response instead of letting the patches update this page.
func (rt *runtime) rail(active string) html.Content {
	newRow := tag.New("div.chat-new").
		Set("id", rt.elemID("new")).
		Add(tag.New("span.chat-new-label", html.Text("New:")))
	for _, p := range rt.cfg.profiles {
		newRow = newRow.Add(
			tag.New("button.chat-new-profile").
				Set("data-on:click",
					"@post('"+rt.path("conversations")+"?profile="+queryEscape(p.Name)+"')").
				Add(html.Text(p.Name)),
		)
	}

	metas, _ := rt.cfg.store.List()
	items := make(html.Group, 0, len(metas))
	for _, m := range metas {
		items = append(items, rt.railItem(m, active))
	}

	return tag.New("nav.chat-rail").
		Set("id", rt.elemID("rail")).
		Add(newRow, tag.New("ul.chat-list", items))
}

func (rt *runtime) railItem(m ConversationMeta, active string) html.Content {
	li := tag.New("li.chat-item")
	if m.ID == active {
		li = li.Class("active")
	}
	title := m.Title
	if title == "" {
		title = "untitled"
	}
	sel := tag.New("button.chat-link").
		Set("data-on:click", "@post('"+rt.path("select")+"?id="+queryEscape(m.ID)+"')").
		Add(
			tag.New("span.chat-title", html.Text(title)),
			tag.New("span.chat-badge", html.Text(m.Profile)),
		)
	del := tag.New("button.chat-delete[title=delete]").
		Set("data-on:click",
			"confirm('Delete this conversation?') && @post('"+rt.path("delete")+"?id="+queryEscape(m.ID)+"')").
		Add(html.Text("✕"))
	return li.Add(sel, del)
}

// transcript renders the log region with a stable outer id (so the select
// route can morph it wholesale) wrapping the per-conversation append target
// (logID) that live turn entries patch into.  On the empty state it holds a
// hint; on a selected conversation it holds the replayed stored entries.
func (rt *runtime) transcript(active string, entries []Entry) html.Content {
	outer := tag.New("div.chat-transcript").Set("id", rt.elemID("transcript"))
	if active == "" {
		return outer.Add(tag.New("p.chat-hint",
			html.Text("No conversation selected — start one from the rail.")))
	}
	rendered := make(html.Group, 0, len(entries))
	for i, e := range entries {
		rendered = append(rendered, rt.replayEntry(active, i, e))
	}
	return outer.Add(tag.New("div.chat-log").Set("id", rt.logID(active)).Add(rendered...))
}

// composer renders the input bound to the prompt signal, with an action
// button that morphs into Stop while THIS conversation owns the running turn
// (workbench UI conventions: BusyActionButton).
func (rt *runtime) composer(active string) html.Content {
	if active == "" {
		return html.Group{}
	}
	s := rt.cfg.signals
	own := "$" + s.Busy + " === 'agent' && $" + s.BusyConv + " === '" + active + "'"
	sendURL := rt.path("send")
	return tag.New("div.chat-composer").
		Set("id", rt.elemID("composer")).
		Add(
			rt.activityLine(active),
			rt.turnElsewhereNote(active),
			tag.New("div.chat-prompt-box",
				tag.New("textarea.chat-input[rows=3][placeholder=Ask the agent…]").
					Set("spellcheck", "false").
					Set("data-bind:"+s.Prompt).
					Set("data-on:keydown",
						"evt.key === 'Enter' && !evt.shiftKey && (evt.preventDefault(), !$"+s.Busy+" && @post('"+sendURL+"'))"),
				tag.New("button.chat-send").
					Set("data-attr:disabled", "$"+s.Busy+" && !("+own+")").
					Set("data-attr:title", own+" ? 'stop the running turn' : 'send'").
					Set("data-text", own+" ? '■' : '↑'").
					Set("data-on:click", own+" ? @post('"+rt.path("cancel")+"') : @post('"+sendURL+"')").
					Add(html.Text("↑")),
			),
		)
}

// activityLine is the one running indicator — the oat [aria-busy] spinner
// (workbench UI conventions), shown only while this conversation owns the turn.
func (rt *runtime) activityLine(active string) html.Content {
	s := rt.cfg.signals
	return tag.New("div.chat-activity").
		Set("data-show", "$"+s.Busy+" === 'agent' && $"+s.BusyConv+" === '"+active+"'").
		Set("aria-busy", "true").
		Set("data-spinner", "small").
		Add(html.Text("agent turn running…"))
}

// turnElsewhereNote shows when a turn runs in a DIFFERENT conversation.
func (rt *runtime) turnElsewhereNote(active string) html.Content {
	s := rt.cfg.signals
	return tag.New("p.chat-turn-elsewhere").
		Set("data-show", "$"+s.Busy+" === 'agent' && $"+s.BusyConv+" !== '"+active+"'").
		Set("data-text", "'turn running in ' + ($"+s.BusyConvName+" || 'another conversation')")
}

// --- transcript entries ---------------------------------------------------

// entry wraps one transcript line with a stable element id (so a later morph
// can target it) and a kind class hook.
func entryTag(elemID, kind string, content ...html.Content) html.Content {
	return tag.New("div.chat-entry").
		Set("id", elemID).
		Class(kind).
		Add(content...)
}

// promptEntry renders a user prompt.
func promptEntry(elemID, text string) html.Content {
	return entryTag(elemID, "user", tag.New("p.chat-user", html.Text(text)))
}

// eventEntry renders one agent Event.  render is the host permission-body
// override (nil to use the default).  answerable controls whether a permission
// event renders its option buttons (live) or a resolved/expired line (replay).
func (rt *runtime) eventEntry(convID, elemID string, ev *Event, answerable bool) html.Content {
	switch ev.Kind {
	case EventMessage:
		return entryTag(elemID, "agent", markdownBody(ev.Text))
	case EventThought:
		return entryTag(elemID, "thought", thoughtBody(ev.Text))
	case EventToolCall:
		return entryTag(elemID, "tool", toolBody(ev, false))
	case EventToolResult:
		return entryTag(elemID, "tool", toolBody(ev, true))
	case EventError:
		return entryTag(elemID, "error", tag.New("p.chat-error", html.Text(ev.Text)))
	case EventPlan:
		return entryTag(elemID, "plan", planBody(ev.Plan))
	case EventPermission:
		if answerable {
			return entryTag(elemID, "permission", rt.permissionCard(convID, ev))
		}
		return entryTag(elemID, "permission", permissionResolvedBody(ev, "answered or expired in an earlier session"))
	default:
		return entryTag(elemID, "note", html.Text(ev.Text))
	}
}

// thoughtBody renders reasoning as a collapsed disclosure.
func thoughtBody(text string) html.Content {
	return tag.New("details",
		tag.New("summary", html.Text("thinking…")),
		tag.New("pre", html.Text(text)),
	)
}

// toolBody renders one tool call as a collapsed disclosure: a summary line of
// name/args, and the settled result once done.
func toolBody(ev *Event, done bool) html.Content {
	head := ev.Title
	if args := strings.TrimSpace(string(ev.RawInput)); args != "" {
		head += " " + args
	}
	compact, elided := compactArgs(head)
	body := html.Group{}
	if elided {
		body = append(body, tag.New("pre", html.Text(formatJSON(string(ev.RawInput)))))
	}
	if done && ev.Text != "" {
		if ev.IsError {
			body = append(body, tag.New("ul.chat-tool-error", tag.New("li", html.Text(ev.Text))))
		} else {
			body = append(body, tag.New("pre", html.Text(formatJSON(ev.Text))))
		}
	}
	var status html.Content = html.Group{}
	if done && ev.IsError {
		status = tag.New("span.chat-tool-status.error").Set("title", "tool call failed")
	}
	return tag.New("details",
		tag.New("summary.chat-tool-line",
			tag.New("code", html.Text(compact)),
			status,
		),
		body,
	)
}

// planBody renders the agent's plan as a checklist.
func planBody(entries []PlanEntry) html.Content {
	lines := make(html.Group, 0, len(entries))
	for _, e := range entries {
		mark := "☐"
		switch e.Status {
		case "completed":
			mark = "☑"
		case "in_progress":
			mark = "◐"
		}
		class := "chat-plan-line"
		if e.Status != "" {
			class += " " + strings.ReplaceAll(e.Status, "_", "-")
		}
		lines = append(lines, tag.New("li."+class,
			tag.New("span.chat-plan-mark", html.Text(mark)),
			html.Text(" "+e.Content),
		))
	}
	return tag.New("ul.chat-plan", lines)
}

// permissionCard renders a pending permission request: an optional host body,
// then one button per option posting to the answer route.
func (rt *runtime) permissionCard(convID string, ev *Event) html.Content {
	var body html.Content = html.Group{}
	if rt.cfg.renderPermission != nil {
		body = rt.cfg.renderPermission(*ev)
	} else {
		body = html.Group{
			tag.New("p.chat-permission-line",
				html.Text("agent is waiting for permission: "+permissionSummary(ev))),
		}
	}
	buttons := make(html.Group, 0, len(ev.Options))
	for _, opt := range ev.Options {
		class := "chat-permission-option allow"
		if strings.HasPrefix(opt.Kind, "reject") {
			class = "chat-permission-option reject"
		}
		url := rt.path("answer") + "?requestID=" + queryEscape(ev.RequestID) +
			"&optionID=" + queryEscape(opt.ID) + "&conv=" + queryEscape(convID)
		buttons = append(buttons, tag.New("button."+class).
			Set("data-on:click", "@post('"+url+"')").
			Add(html.Text(opt.Name)))
	}
	return html.Group{body, tag.New("div.chat-permission-options", buttons)}
}

// permissionResolvedBody renders a permission after it stops being pending
// (answered live, or replayed from the store), dropping the buttons.
func permissionResolvedBody(ev *Event, note string) html.Content {
	return tag.New("p.chat-permission-line",
		html.Text("permission for "+permissionSummary(ev)+" — "+note))
}

// permissionSummary names the tool a permission request gates.
func permissionSummary(ev *Event) string {
	var b strings.Builder
	b.WriteString(ev.Title)
	if args := strings.TrimSpace(string(ev.RawInput)); args != "" {
		compact, _ := compactArgs(args)
		b.WriteString(" ")
		b.WriteString(compact)
	}
	return b.String()
}

// compactArgs bounds a summary line, reporting whether anything was cut.
func compactArgs(s string) (string, bool) {
	s = strings.Join(strings.Fields(s), " ")
	const max = 120
	if len(s) > max {
		return s[:max] + "…", true
	}
	return s, false
}

// formatJSON pretty-prints a JSON document for a <pre>; non-JSON passes through.
func formatJSON(s string) string {
	trimmed := strings.TrimSpace(s)
	if trimmed == "" || (trimmed[0] != '{' && trimmed[0] != '[') {
		return s
	}
	var buf bytes.Buffer
	if err := json.Indent(&buf, []byte(trimmed), "", "  "); err != nil {
		return s
	}
	return buf.String()
}

// elemName builds a per-conversation transcript element id for a non-morphable
// entry (prompt, message, thought, error, plan) from the conversation id and
// the entry's index.  Morphable entries do NOT use this — see toolElemID /
// permElemID, whose content-derived ids stay valid across a mid-turn re-select.
func elemName(convID string, seq int) string {
	return "e-" + convID + "-" + strconv.Itoa(seq)
}

// toolElemID is the content-derived id of a tool entry (call + its later
// result), keyed by ToolCallID and namespaced to the conversation so the live
// tool-result morph lands on the same element a replayed transcript renders.
func toolElemID(convID, toolCallID string) string {
	return "e-" + convID + "-tool-" + toolCallID
}

// permElemID is the content-derived id of a permission card, keyed by RequestID
// and namespaced to the conversation so an answer's morph lands on the same
// element a replayed transcript renders while the request is still pending.
func permElemID(convID, requestID string) string {
	return "e-" + convID + "-perm-" + requestID
}
