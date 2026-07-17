package main

import (
	"strings"
	"testing"

	"swdunlop.dev/pkg/datalog/jsonfacts"
)

// TestExtractRecordMultiSourceConfig is the regression for BUG #9:
// extractRecord built a one-file fstest.MapFS containing only the selected
// source's line, but called cfg.LoadFS(synthetic) with the FULL config —
// unmodified — as-is. LoadFS opens every configured Source unconditionally
// (jsonfacts/loader.go) and returns fs.ErrNotExist the moment one isn't
// present in fsys; with two or more configured sources, previewing ANY row
// used to fail with "file does not exist", because the synthetic FS only
// ever contains the one selected file. The comment claiming other sources
// were "naturally inert" was simply wrong — this test would have failed
// against the pre-fix extractRecord.
func TestExtractRecordMultiSourceConfig(t *testing.T) {
	cfg := jsonfacts.Config{
		Sources: []jsonfacts.Source{
			{
				File: "events.jsonl",
				Mappings: []jsonfacts.Mapping{
					{Predicate: "event", Args: []string{"value.host"}},
				},
			},
			{
				File: "other.jsonl",
				Mappings: []jsonfacts.Mapping{
					{Predicate: "other_event", Args: []string{"value.host"}},
				},
			},
		},
	}

	lines, err := extractRecord(cfg, "events.jsonl", `{"host": "h1"}`)
	if err != nil {
		t.Fatalf("extractRecord with a 2-source config: unexpected error: %v", err)
	}
	if len(lines) != 1 || !strings.Contains(lines[0], "event") || !strings.Contains(lines[0], "h1") {
		t.Fatalf("extractRecord: got %v, want a single event(...) line for h1", lines)
	}

	// Previewing a row from the SECOND source must also work.
	lines2, err := extractRecord(cfg, "other.jsonl", `{"host": "h2"}`)
	if err != nil {
		t.Fatalf("extractRecord for the second source: unexpected error: %v", err)
	}
	if len(lines2) != 1 || !strings.Contains(lines2[0], "other_event") || !strings.Contains(lines2[0], "h2") {
		t.Fatalf("extractRecord (second source): got %v, want a single other_event(...) line for h2", lines2)
	}
}

// TestExtractRecordUnknownFileStillRejected keeps the existing "file not in
// config's sources" guard working after narrowing cfg.Sources.
func TestExtractRecordUnknownFileStillRejected(t *testing.T) {
	cfg := jsonfacts.Config{
		Sources: []jsonfacts.Source{
			{File: "events.jsonl", Mappings: []jsonfacts.Mapping{
				{Predicate: "event", Args: []string{"value.host"}},
			}},
		},
	}
	if _, err := extractRecord(cfg, "missing.jsonl", `{}`); err == nil {
		t.Fatal("extractRecord: expected error for a file absent from cfg.Sources, got none")
	}
}

// TestExtractRecordMatchersStillApply confirms narrowing cfg.Sources to the
// selected file does not also drop cfg.Matchers, which apply by predicate,
// not by source file.
func TestExtractRecordMatchersStillApply(t *testing.T) {
	cfg := jsonfacts.Config{
		Sources: []jsonfacts.Source{
			{
				File: "events.jsonl",
				Mappings: []jsonfacts.Mapping{
					{Predicate: "host_seen", Args: []string{"value.host"}},
				},
			},
			{
				File: "other.jsonl",
				Mappings: []jsonfacts.Mapping{
					{Predicate: "other_event", Args: []string{"value.host"}},
				},
			},
		},
		Matchers: []jsonfacts.Matcher{
			{
				Predicate: "host_seen",
				Term:      0,
				Contains:  []string{"evil"},
			},
		},
	}

	lines, err := extractRecord(cfg, "events.jsonl", `{"host": "evil.example.com"}`)
	if err != nil {
		t.Fatalf("extractRecord: unexpected error: %v", err)
	}
	found := false
	for _, l := range lines {
		if strings.HasPrefix(l, "contains(") {
			found = true
		}
	}
	if !found {
		t.Fatalf("extractRecord: matcher-derived contains fact missing from %v", lines)
	}
}
