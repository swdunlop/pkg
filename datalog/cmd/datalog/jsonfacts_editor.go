package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"testing/fstest"

	html "github.com/swdunlop/html-go"
	"github.com/swdunlop/html-go/datastar"
	"swdunlop.dev/pkg/datalog/cmd/datalog/view"
	"swdunlop.dev/pkg/datalog/jsonfacts"
)

// jsonfactsSignals mirrors the jsonfacts Editor's textarea signal
// (data-bind:schema-text in view.JSONFactsEditor), used by /jsonfacts/preview.
type jsonfactsSignals struct {
	SchemaText string `json:"schemaText"`
}

// jsonfactsApplyJobKey is the Jobs key for the jsonfacts Editor's Apply
// action (doc/notes/datastar.md §9): Begin's per-key busy check needs a
// stable name so a second Apply click while one is in flight is a no-op.
const jsonfactsApplyJobKey = "jsonfacts-apply"

// renderJSONFactsSelection reads the current row selection under wb.selMu
// and returns the #jsonfacts-row and #jsonfacts-output fragments for it —
// used both by GET / (initial page render, design constraint 1: render
// current selection if any) and can be reused by any handler that needs to
// re-render the selection unchanged.
func (wb *workbench) renderJSONFactsSelection() (row html.Content, output html.Content) {
	wb.selMu.Lock()
	valid := wb.selValid
	raw := wb.selRecord
	wb.selMu.Unlock()

	if !valid {
		return view.JSONFactsNoSelection(), view.JSONFactsOutputMessage("no row selected yet")
	}

	pretty := prettyJSONLine(raw)
	lines, err := wb.extractSelectedRow()
	if err != nil {
		return view.JSONFactsRow(pretty), view.JSONFactsOutputMessage(err.Error())
	}
	return view.JSONFactsRow(pretty), view.JSONFactsOutput(lines)
}

// prettyJSONLine pretty-prints raw as JSON via json.MarshalIndent, falling
// back to the raw line verbatim if it doesn't parse as JSON.
func prettyJSONLine(raw string) string {
	var obj any
	if err := json.Unmarshal([]byte(raw), &obj); err != nil {
		return raw
	}
	buf, err := json.MarshalIndent(obj, "", "  ")
	if err != nil {
		return raw
	}
	return string(buf)
}

// extractSelectedRow extracts facts for the currently selected row against
// the CURRENT session config (wb.h.sess.cfg), reusing jsonfacts.Config.LoadFS
// itself rather than hand-rolling a parallel extraction path: a synthetic
// in-memory fstest.MapFS is built containing only the selected source
// file, with its content replaced by the single selected line, and the
// config's real Sources/Matchers/Declarations run against it unchanged.
// This keeps single-row preview faithful to full-file extraction (same
// mapping/matcher code, same normalizeToConstant rules) without needing a
// new package-level API. Matchers scoped to other source files are
// naturally inert since only the selected file exists in the synthetic FS.
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
func extractRecord(cfg jsonfacts.Config, file, raw string) ([]string, error) {
	found := false
	for _, src := range cfg.Sources {
		if src.File == file {
			found = true
			break
		}
	}
	if !found {
		return nil, fmt.Errorf("selected file %q is no longer in the config's sources", file)
	}

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
	if len(args) == 0 {
		return ""
	}
	out := args[0]
	for _, a := range args[1:] {
		out += ", " + a
	}
	return out
}

// handleJSONFactsPreview is the debounced single-row-extraction endpoint
// (POST /jsonfacts/preview): parses the draft YAML, extracts against the
// single currently selected row only (fast, no timeout machinery), and
// patches #jsonfacts-output / #jsonfacts-error. This is read-only against
// the draft text — it never mutates the session; Apply is the explicit
// action that does.
func (wb *workbench) handleJSONFactsPreview(w http.ResponseWriter, r *http.Request) {
	// Decode signals from the request body BEFORE upgrading to an SSE
	// stream: RequestStream starts writing the response (headers/flush),
	// after which the request body is no longer readable.
	var sig jsonfactsSignals
	decodeErr := datastar.Decode(&sig, r)

	stream, err := datastar.RequestStream(w, r)
	if err != nil {
		return
	}

	if decodeErr != nil {
		_ = stream.Emit(datastar.Elements(view.JSONFactsErrors([]string{decodeErr.Error()})))
		return
	}

	cfg, err := parseConfigFormat([]byte(sig.SchemaText), "yaml")
	if err != nil {
		_ = stream.Emit(datastar.Elements(view.JSONFactsErrors([]string{err.Error()})))
		return
	}

	wb.selMu.Lock()
	file, raw, valid := wb.selFile, wb.selRecord, wb.selValid
	wb.selMu.Unlock()

	if !valid {
		_ = stream.Emit(datastar.Batch(
			datastar.Elements(view.JSONFactsErrors(nil)),
			datastar.Elements(view.JSONFactsOutputMessage("no row selected — use \"Test\" in the Data Browser")),
		))
		return
	}

	lines, err := extractRecord(cfg, file, raw)
	if err != nil {
		_ = stream.Emit(datastar.Elements(view.JSONFactsErrors([]string{err.Error()})))
		return
	}

	_ = stream.Emit(datastar.Batch(
		datastar.Elements(view.JSONFactsErrors(nil)),
		datastar.Elements(view.JSONFactsOutput(lines)),
	))
}

// handleJSONFactsApply is the Apply action (POST /jsonfacts/apply): the
// explicit full Transform (doc/notes/datastar.md §9 shape). Gated by
// wb.jobs.Begin so a second Apply click while one is in flight is a no-op;
// the actual work runs through runRecovered under evalTimeout; on success
// it calls the SAME typed MCP handler wb.h.setSchema the MCP tools use
// (design constraint 1 — one pipeline, N frontends) and publishes the
// session-changed notification so every tab's Fact Browser repaints.
func (wb *workbench) handleJSONFactsApply(w http.ResponseWriter, r *http.Request) {
	// Decode signals BEFORE upgrading to an SSE stream — see
	// handleJSONFactsPreview's comment: the request body is unreadable
	// once RequestStream starts writing the response.
	var sig jsonfactsSignals
	decodeErr := datastar.Decode(&sig, r)

	stream, err := datastar.RequestStream(w, r)
	if err != nil {
		return
	}

	if decodeErr != nil {
		_ = stream.Emit(datastar.Elements(view.JSONFactsErrors([]string{decodeErr.Error()})))
		return
	}

	ctx, done := wb.jobs.Begin(r.Context(), jsonfactsApplyJobKey)
	if ctx == nil {
		_ = stream.Emit(datastar.Elements(view.JSONFactsOutputMessage("Apply already running…")))
		return
	}
	defer done()

	ctx, cancel := context.WithTimeout(ctx, evalTimeout)
	defer cancel()

	resultCh := runApplySchema(wb, sig.SchemaText)

	select {
	case <-ctx.Done():
		_ = stream.Emit(datastar.Elements(view.JSONFactsErrors([]string{
			"Apply timed out, schema not changed",
		})))
		return
	case res := <-resultCh:
		if res.err != nil {
			_ = stream.Emit(datastar.Elements(view.JSONFactsErrors([]string{res.err.Error()})))
			return
		}
		_ = stream.Emit(datastar.Batch(
			datastar.Elements(view.JSONFactsErrors(nil)),
			datastar.Elements(view.JSONFactsOutputMessage(fmt.Sprintf("applied: %d predicates loaded", len(res.out.Predicates)))),
		))
	}
}

// applySchemaResult is runApplySchema's result payload.
type applySchemaResult struct {
	out setSchemaOutput
	err error
}

// runApplySchema runs the session mutation in a spawned goroutine
// (doc/features/web-ui.md's goroutine isolation), via runRecovered for
// panic recovery, holding wb.h.mu only around the mutation and the
// subsequent publish — not around the whole SSE response, per the task's
// explicit note. The result (including any panic, translated to an error by
// runRecovered) is delivered on the returned channel exactly once.
func runApplySchema(wb *workbench, schemaText string) <-chan applySchemaResult {
	out := make(chan applySchemaResult, 1)
	var result setSchemaOutput
	done := runRecovered(func() error {
		wb.h.mu.Lock()
		defer wb.h.mu.Unlock()
		var err error
		result, err = wb.h.setSchema(setSchemaInput{Schema: schemaText, Format: "yaml"})
		if err == nil {
			wb.publishSessionChanged()
		}
		return err
	})
	go func() {
		err := <-done
		out <- applySchemaResult{out: result, err: err}
	}()
	return out
}
