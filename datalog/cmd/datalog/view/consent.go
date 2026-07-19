package view

import (
	"net/url"

	html "github.com/swdunlop/html-go"
	"github.com/swdunlop/html-go/tag"
)

// This file renders consent diff cards (doc/features/workbench-v2.md
// design decision 5): the transcript entry a kit conversation's gated
// write blocks on, showing what the agent wants to change as a line diff
// with Approve/Deny buttons. It reuses the permission entry's CSS
// vocabulary (.permission-line, .permission-options, .permission-option)
// so a consent card and an ACP permission request read as the same kind
// of thing — a turn waiting on the human.

// DiffLine is one rendered diff row. Kind is " " (context), "-" (removed),
// or "+" (added).
type DiffLine struct {
	Kind string
	Text string
}

// ConsentOption is one Approve/Deny button on a consent card, posting the
// same /answer endpoint permission buttons use; Tab anchors a late click's
// error to the right transcript (see permissionEntry).
type ConsentOption struct {
	RequestID string
	OptionID  string
	Label     string
	Reject    bool
	Tab       string
}

// ConsentCard renders the blocked write: the action line (mirroring the
// permission entry's "agent is waiting for permission" phrasing), the
// diff, and the option buttons.
func ConsentCard(action string, diff []DiffLine, options []ConsentOption) html.Content {
	rows := make(html.Group, 0, len(diff))
	for _, ln := range diff {
		class := "diff-line"
		prefix := "  "
		switch ln.Kind {
		case "-":
			class += " del"
			prefix = "- "
		case "+":
			class += " add"
			prefix = "+ "
		}
		rows = append(rows, tag.New("div."+class, html.Text(prefix+ln.Text)))
	}

	buttons := make(html.Group, 0, len(options))
	for _, opt := range options {
		class := "permission-option"
		if opt.Reject {
			class += " reject"
		} else {
			class += " allow"
		}
		buttons = append(buttons, tag.New("button."+class).
			Set("data-on:click", "@post('/answer?requestID="+url.QueryEscape(opt.RequestID)+
				"&optionID="+url.QueryEscape(opt.OptionID)+"&tab="+url.QueryEscape(opt.Tab)+"')").
			Add(html.Text(opt.Label)))
	}

	return html.Group{
		tag.New("p.permission-line",
			html.Text("agent is waiting for approval: "+action)),
		tag.New("pre.consent-diff", rows),
		tag.New("div.permission-options", buttons),
	}
}
