package main

import (
	"fmt"
	"sort"
	"strings"
	"testing/fstest"

	"swdunlop.dev/pkg/datalog/jsonfacts"
)

// extractSelectedRow extracts facts for the currently selected row against
// the CURRENT session config (wb.h.sess.cfg), reusing jsonfacts.Config.LoadFS
// itself rather than hand-rolling a parallel extraction path: a synthetic
// in-memory fstest.MapFS is built containing only the selected source
// file, with its content replaced by the single selected line, and the
// config's Matchers/Declarations run against it unchanged (only Sources is
// narrowed — see extractRecord). This keeps single-row preview faithful to
// full-file extraction (same mapping/matcher code, same normalizeToConstant
// rules) without needing a new package-level API.
func (wb *workbench) extractSelectedRow() ([]string, error) {
	wb.selMu.Lock()
	file, raw, valid := wb.selFile, wb.selRecord, wb.selValid
	wb.selMu.Unlock()

	if !valid {
		return nil, fmt.Errorf("no row selected")
	}

	wb.h.mu.Lock()
	cfg := wb.h.sess.cfg
	wb.h.mu.Unlock()

	return extractRecord(cfg, file, raw)
}

// extractRecord runs cfg's Sources/Matchers against a single JSONL line as
// if it were the entirety of file, returning one "predicate(args...)" line
// per extracted fact in a stable (sorted) order.
//
// cfg.Sources is filtered down to just the selected file before loading:
// LoadFS opens every configured source and aborts the whole load with
// fs.ErrNotExist the moment one is missing (jsonfacts/loader.go's LoadFS,
// looping cfg.Sources unconditionally) — it does NOT skip sources absent
// from fsys. The synthetic MapFS below only ever contains the selected
// file, so with two or more configured sources every preview used to fail
// with "file does not exist" (BUG #9); a previous version of this comment
// claimed other sources were "naturally inert," which was simply wrong.
// Matchers/Declarations are left on cfg unfiltered — they apply to facts by
// predicate, not by source file, so narrowing them isn't needed (and would
// be wrong: a matcher over the selected source's own extracted facts must
// still run).
func extractRecord(cfg jsonfacts.Config, file, raw string) ([]string, error) {
	var selected *jsonfacts.Source
	for i := range cfg.Sources {
		if cfg.Sources[i].File == file {
			selected = &cfg.Sources[i]
			break
		}
	}
	if selected == nil {
		return nil, fmt.Errorf("selected file %q is no longer in the config's sources", file)
	}
	cfg.Sources = []jsonfacts.Source{*selected}

	synthetic := fstest.MapFS{
		file: &fstest.MapFile{Data: []byte(raw + "\n")},
	}

	db, err := cfg.LoadFS(synthetic)
	if err != nil {
		return nil, err
	}

	var lines []string
	for name, arity := range db.Predicates() {
		for row := range db.Facts(name, arity) {
			args := make([]string, len(row))
			for i, c := range row {
				args[i] = c.String()
			}
			lines = append(lines, fmt.Sprintf("%s(%s)", name, joinArgs(args)))
		}
	}
	sort.Strings(lines)
	return lines, nil
}

// joinArgs joins fact arguments with ", " for predicate(args...) rendering.
func joinArgs(args []string) string {
	return strings.Join(args, ", ")
}
