package view

import (
	"strconv"

	html "github.com/swdunlop/html-go"
	"github.com/swdunlop/html-go/tag"
)

// This file renders transcript entries — the server-owned scrollback
// entries console.go's consoleLog appends into a conversation's transcript
// (view/conversation.go's Transcript). The v1 console drawer this file
// used to render died with workbench-v2 phase 2; the entry vocabulary
// (user/agent/thought/tool/error/note/permission/plan, styled by
// .console-entry.<kind>) carried over unchanged.

// ConsoleEntry wraps one scrollback entry. id is the workbench console
// log's sequence number — stable across append and every later morph of the
// same entry — and kind is a CSS hook ("query", "user", "agent", "thought",
// "tool", "error", "note", "permission", "plan").
func ConsoleEntry(id uint64, kind string, content ...html.Content) html.Content {
	return TranscriptEntry(consoleEntryID(id), kind, content...)
}

// TranscriptEntry is ConsoleEntry with a caller-chosen element id — the
// shape page-load history rendering uses (conversations_http.go renders a
// conversation's persisted messages with "h<n>" ids that no live morph
// ever targets, keeping them disjoint from consoleLog's "c<seq>" space).
func TranscriptEntry(elemID, kind string, content ...html.Content) html.Content {
	return tag.New("div.console-entry").
		Set("id", elemID).
		Class(kind).
		Add(content...)
}

func consoleEntryID(id uint64) string {
	return "c" + strconv.FormatUint(id, 10)
}

// ConsoleEntrySelector returns the CSS selector for one entry, for morph
// events targeting it.
func ConsoleEntrySelector(id uint64) string { return "#" + consoleEntryID(id) }
