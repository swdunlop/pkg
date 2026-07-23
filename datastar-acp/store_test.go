package chat

import (
	"testing"
	"time"
)

// TestDirStoreRoundTrip pins the JSONL store's Create/Append/Read/List/Delete
// contract: a created conversation reads back its meta and appended entries in
// order, List returns newest-first, and Delete removes it.
func TestDirStoreRoundTrip(t *testing.T) {
	dir := t.TempDir()
	st := DirStore(dir)

	older := ConversationMeta{ID: newID(), Title: "older", Profile: "p", Created: time.Now().Add(-time.Hour)}
	newer := ConversationMeta{ID: newID(), Title: "newer", Profile: "p", Created: time.Now()}
	if err := st.Create(older); err != nil {
		t.Fatalf("create older: %v", err)
	}
	if err := st.Create(newer); err != nil {
		t.Fatalf("create newer: %v", err)
	}

	// Duplicate id is a clean error, never an overwrite.
	if err := st.Create(older); err == nil {
		t.Fatalf("duplicate create should error")
	}

	entries := []Entry{
		{Prompt: "hello"},
		{Event: &Event{Kind: EventMessage, Text: "hi there", Time: time.Now()}},
		{Event: &Event{Kind: EventToolCall, ToolCallID: "t1", Title: "query", Status: "in_progress"}},
	}
	for _, e := range entries {
		if err := st.Append(newer.ID, e); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	meta, got, err := st.Read(newer.ID)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if meta.Title != "newer" || meta.Profile != "p" {
		t.Fatalf("meta round-trip lost fields: %+v", meta)
	}
	if len(got) != len(entries) {
		t.Fatalf("read %d entries, want %d", len(got), len(entries))
	}
	if got[0].Prompt != "hello" {
		t.Fatalf("first entry prompt = %q", got[0].Prompt)
	}
	if got[1].Event == nil || got[1].Event.Text != "hi there" {
		t.Fatalf("second entry event = %+v", got[1].Event)
	}
	if got[2].Event == nil || got[2].Event.ToolCallID != "t1" {
		t.Fatalf("third entry event = %+v", got[2].Event)
	}

	// Rename rewrites the header without disturbing the transcript.
	if err := st.Rename(newer.ID, "renamed"); err != nil {
		t.Fatalf("rename: %v", err)
	}
	meta, got, err = st.Read(newer.ID)
	if err != nil {
		t.Fatalf("read after rename: %v", err)
	}
	if meta.Title != "renamed" || meta.Profile != "p" {
		t.Fatalf("meta after rename = %+v", meta)
	}
	if len(got) != len(entries) || got[0].Prompt != "hello" {
		t.Fatalf("rename disturbed transcript: %+v", got)
	}
	if err := st.Rename("no-such-id", "x"); err == nil {
		t.Fatalf("rename of missing conversation should error")
	}

	// List is newest-first.
	list, err := st.List()
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("list len %d, want 2", len(list))
	}
	if list[0].ID != newer.ID || list[1].ID != older.ID {
		t.Fatalf("list order wrong: %s then %s", list[0].ID, list[1].ID)
	}

	// Delete removes it; a re-read errors and List drops it.
	if err := st.Delete(newer.ID); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, _, err := st.Read(newer.ID); err == nil {
		t.Fatalf("read after delete should error")
	}
	list, _ = st.List()
	if len(list) != 1 || list[0].ID != older.ID {
		t.Fatalf("list after delete = %+v", list)
	}
	// Delete is idempotent.
	if err := st.Delete(newer.ID); err != nil {
		t.Fatalf("second delete should be nil: %v", err)
	}
}
