package view

import (
	html "github.com/swdunlop/html-go"
	"github.com/swdunlop/html-go/tag"
)

// This file renders the page's left half (doc/features/workbench-v2.md
// design decision 2): a narrow conversation rail beside the active
// conversation's transcript and composer.
//
// Signal vocabulary (page-wide, declared here since the composer is the one
// writer and every face of the turn state reads them):
//
//   - $busy — the page-wide busy mutex serve.go's publishBusy fans out
//     ("agent" while any conversation's turn runs; "run"/"apply" etc. for
//     other long jobs).
//   - $busyConv / $busyConvName — WHICH conversation owns the running turn
//     (id and display name), published beside $busy by the send handler.
//     The composer of the running conversation shows its activity line and
//     a Stop face; every OTHER conversation's composer disables with a
//     "turn running in <name>" note (design decision 6's global one-turn
//     gate, made visible).
//   - $prompt — the composer's textarea binding.

// RailItem is one conversation in the rail, newest-first (design decision
// 2: mode badge, auto-derived title, delete with confirm).
type RailItem struct {
	ID     string
	Title  string // session name, first-message fallback, or "untitled"
	Mode   string // "query" | "rules" | "facts"
	Active bool
}

// ConversationPane renders the left half: the rail beside the active
// conversation's transcript and composer. transcript and composer may be
// the EmptyState pair when no conversation is selected.
func ConversationPane(rail, transcript, composer html.Content) html.Content {
	return tag.New("section#conversations").
		Set("data-signals", `{busy: '', busyConv: '', busyConvName: '', prompt: ''}`).
		Add(
			rail,
			tag.New("div#conv-main", transcript, composer),
		)
}

// Rail renders the conversation rail: the New Conversation control (an
// inline mode picker — one plain form, three submit buttons, since
// creating a conversation is a real navigation to /c/{id}, not a Datastar
// patch; doc/notes/datastar.md §7) above the conversation list.
func Rail(items []RailItem) html.Content {
	return tag.New("nav#conv-rail",
		tag.New("form#conv-new[method=post][action=/conversations]",
			tag.New("span.conv-new-label", html.Text("New:")),
			newConvButton("query", "Query"),
			newConvButton("rules", "Rules"),
			newConvButton("facts", "Facts"),
		),
		tag.New("ul#conv-list", html.Map(items, railItem)),
	)
}

// newConvButton is one mode's submit button in the New Conversation form;
// the button itself carries the mode (name=mode value=<mode>), so the form
// needs no hidden input and no JS.
func newConvButton(mode, label string) html.Content {
	return tag.New("button.conv-new-mode").
		Set("name", "mode").
		Set("value", mode).
		Add(html.Text(label))
}

func railItem(it RailItem) html.Content {
	li := tag.New("li.conv-item")
	if it.Active {
		li = li.Class("active")
	}
	title := it.Title
	if title == "" {
		title = "untitled"
	}
	// Delete is a plain form POST with a confirm() guard (design decision
	// 2: "delete with confirm") — a real navigation back to /, not a
	// Datastar patch, mirroring the create form above.
	del := tag.New("form.conv-delete[method=post]").
		Set("action", "/c/"+it.ID+"/delete").
		Set("onsubmit", "return confirm('Delete this conversation?')").
		Add(tag.New("button[title=delete]", html.Text("✕")))
	return li.Add(
		tag.New("a.conv-link").Set("href", "/c/"+it.ID).Add(
			tag.New("span.conv-title", html.Text(title)),
			tag.New("span.badge.mode-"+it.Mode, html.Text(it.Mode)),
		),
		del,
	)
}

// Transcript renders the active conversation's transcript log div. Its id
// (LogSelector) is the append target live turn entries patch into over
// /events; entries carry their own ids so streamed updates morph in place
// without touching siblings. history is the page-load rendering of the
// conversation's persisted messages (conversations_http.go renders it from
// the kit session tree).
func Transcript(convID string, history ...html.Content) html.Content {
	return tag.New("div.transcript").Set("id", logID(convID)).Add(history...)
}

// logID/LogSelector name one conversation's transcript log element —
// shared with console.go's consoleAppend so the append target and the
// rendered div always agree.
func logID(convID string) string   { return "log-" + convID }
func LogSelector(id string) string { return "#log-" + id }

// EmptyState renders the transcript area when no conversation is selected:
// a hint pointing at the rail's mode picker.
func EmptyState() html.Content {
	return tag.New("div.transcript.empty#conv-empty",
		tag.New("p.text-light", html.Text("No conversation selected — start one from the rail: Query to investigate, Rules to author rules, Facts to map JSONL into facts.")),
	)
}

// Composer renders the active conversation's input: a chat-style composer
// (the send control is a round icon button inside the textarea's
// bottom-right corner). The button is one control with three faces off the
// shared $busy mutex:
//
//   - idle: ↑ posts the prompt to this conversation's send endpoint
//   - this conversation's turn running: ■ posts /cancel — no spinner ring;
//     the activity line beside it is the pane's one running indicator
//   - anything else busy (another conversation's turn, run/apply): disabled
//
// Enter sends (guarded on !$busy so it cannot double-fire mid-turn),
// Shift+Enter inserts a newline via the browser's default behavior.
func Composer(convID string) html.Content {
	own := "$busy === 'agent' && $busyConv === '" + convID + "'"
	return tag.New("div#composer",
		activityLine(convID),
		turnElsewhereNote(convID),
		tag.New("div.prompt-box",
			tag.New("textarea#prompt[rows=3][placeholder=Ask the agent…]").
				Set("spellcheck", "false").
				Set("data-bind:prompt").
				Set("data-on:keydown", "evt.key === 'Enter' && !evt.shiftKey && (evt.preventDefault(), !$busy && @post('/c/"+convID+"/send'))"),
			tag.New("button#send").
				Set("data-attr:disabled", "$busy && !("+own+")").
				Set("data-attr:title", own+" ? 'stop the running turn' : 'send'").
				Set("data-text", own+" ? '■' : '↑'").
				Set("data-on:click", own+" ? @post('/cancel') : @post('/c/"+convID+"/send')").
				Add(html.Text("↑")),
		),
	)
}

// activityLine is the turn-level activity indicator, pinned between the
// transcript and the composer while THIS conversation's turn runs. It is
// the ONE running indicator in the pane (workbench UI conventions: the oat
// [aria-busy] spinner is the single activity tell) — the composer's ■
// carries no ring and tool entries don't spin. aria-busy is static —
// data-show removes the whole line when this conversation isn't the one
// running, so the spinner never spins unseen.
func activityLine(convID string) html.Content {
	return tag.New("div.agent-activity").
		Set("data-show", "$busy === 'agent' && $busyConv === '"+convID+"'").
		Set("aria-busy", "true").
		Set("data-spinner", "small").
		Add(html.Text("agent turn running…"))
}

// turnElsewhereNote renders the global one-turn gate's visible face
// (design decision 6: "the composer disabled with a 'turn running in
// <conversation>' note"): shown when a turn runs in a DIFFERENT
// conversation than this composer's.
func turnElsewhereNote(convID string) html.Content {
	return tag.New("p.turn-elsewhere.text-light").
		Set("data-show", "$busy === 'agent' && $busyConv !== '"+convID+"'").
		Set("data-text", "'turn running in ' + ($busyConvName || 'another conversation')")
}
