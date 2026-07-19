package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net/http"
	"path"
	"strconv"
	"strings"

	html "github.com/swdunlop/html-go"
	"github.com/swdunlop/html-go/datastar"
	"github.com/swdunlop/html-go/tag"
	"swdunlop.dev/pkg/datalog/cmd/datalog/view"
)

// dataPageSize is the number of raw records served per Data Browser chunk
// (doc/features/web-ui.md: "Server-side pagination, 50 rows per chunk").
const dataPageSize = 50

// handleDataList is the Data Browser's file-list handler (GET /data): the
// file list comes from the session's cfg.Sources[].file, not a directory
// walk (design's explicit call-out), read under wb.h.mu since it's session
// state. The first file (if any) is auto-selected and its first chunk of
// rows loaded in the same response, so the pane isn't left empty until the
// operator manually picks a file from the <select>.
func (wb *workbench) handleDataList(w http.ResponseWriter, r *http.Request) {
	stream, err := datastar.RequestStream(w, r)
	if err != nil {
		return
	}

	wb.h.mu.Lock()
	files := make([]string, 0, len(wb.h.sess.cfg.Sources))
	for _, src := range wb.h.sess.cfg.Sources {
		files = append(files, src.File)
	}
	wb.h.mu.Unlock()

	_ = stream.Emit(datastar.Elements(view.DataFileList(files)))
	if len(files) > 0 {
		wb.emitDataFile(stream, files[0], 0, "")
	}
}

// handleDataFile is the Data Browser's paginated-row handler
// (GET /data/{file}?offset=N&filter=S). offset=0 (or absent) replaces the
// row list; offset>0 appends the next chunk via datastar.Mode("append").
// filter is a case-insensitive substring filter over raw lines (design
// decision 9); offset counts MATCHING rows, so a Load More under a filter
// pages through the filtered view, not the whole file.
func (wb *workbench) handleDataFile(w http.ResponseWriter, r *http.Request) {
	stream, err := datastar.RequestStream(w, r)
	if err != nil {
		return
	}

	file := r.PathValue("file")
	filter := r.URL.Query().Get("filter")
	offset := 0
	if raw := r.URL.Query().Get("offset"); raw != "" {
		n, convErr := strconv.Atoi(raw)
		if convErr != nil || n < 0 {
			_ = stream.Emit(datastar.Elements(view.DataErrors([]string{
				fmt.Sprintf("invalid offset %q: must be a non-negative integer", raw),
			})))
			return
		}
		offset = n
	}

	wb.emitDataFile(stream, file, offset, filter)
}

// emitDataFile reads file's row chunk at offset (dataPageSize rows) and
// emits the resulting #data-table-body fragment (or #data-error on
// failure). Files are re-read per request (accepted per the design's
// O(offset) risk note); the model/browser-supplied file ref is passed
// through wb.h.confine before ever reaching wb.h.fsys, since it must never
// escape the data root. Shared by handleDataFile (explicit file switches)
// and handleDataList (auto-loading the first file).
func (wb *workbench) emitDataFile(stream datastar.Stream, file string, offset int, filter string) {
	ref, err := wb.h.confine(file)
	if err != nil {
		_ = stream.Emit(datastar.Elements(view.DataErrors([]string{err.Error()})))
		return
	}

	rows, hasMore, err := readDataChunk(wb, ref, offset, dataPageSize, filter)
	if err != nil {
		_ = stream.Emit(datastar.Elements(view.DataErrors([]string{err.Error()})))
		return
	}

	nextOffset := offset + len(rows)
	selFile, selRow, selValid := wb.currentSelection()

	if offset == 0 {
		// Two fragments: the row list, and the sibling Load More control —
		// the control lives OUTSIDE the ul (view.DataBrowser's shell) so
		// later appends can never bury it mid-list.
		_ = stream.Emit(
			datastar.Elements(view.DataTableBody(file, rows, selFile, selRow, selValid), datastar.Selector("#data-table-body")),
			datastar.Elements(view.DataLoadMore(file, nextOffset, hasMore, filter)),
		)
		return
	}

	// Load More (offset > 0): append just the new rows into the existing
	// list and replace the Load More control with a fresh one, rather than
	// appending the whole DataTableBody fragment — that used to nest a
	// duplicate #data-table-body <ul> inside itself and leave the stale
	// (re-fetchable at the old offset) Load More button live alongside the
	// new one (mirrors fact_browser.go's handleFacts).
	_ = stream.Emit(
		datastar.Elements(view.DataRows(file, rows, selFile, selRow, selValid), datastar.Selector("#data-table-body"), datastar.Mode("append")),
		datastar.Elements(view.DataLoadMore(file, nextOffset, hasMore, filter)),
	)
}

// readDataChunk opens ref (already confined) through wb.h.fsys and returns
// up to limit records starting at the 0-based offset, plus whether more
// rows remain beyond the returned chunk. Blank lines are skipped and do not
// count as rows, matching jsonfacts' own loader behavior. filter, when
// non-empty, keeps only lines containing it (case-insensitive substring
// over the raw line); offset then counts MATCHING rows so pagination pages
// through the filtered view, while each row's Index stays its original
// position in the file — the index is the selection key /data/select
// resolves, so it must never renumber under a filter.
func readDataChunk(wb *workbench, ref string, offset, limit int, filter string) ([]view.DataRowInfo, bool, error) {
	f, err := wb.h.fsys.Open(path.Clean(ref))
	if err != nil {
		return nil, false, fmt.Errorf("opening %s: %w", ref, err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 1024*1024), 10*1024*1024)

	needle := strings.ToLower(filter)
	lineNum := 0
	matched := 0
	var rows []view.DataRowInfo
	hasMore := false
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		idx := lineNum
		lineNum++
		if needle != "" && !strings.Contains(strings.ToLower(string(line)), needle) {
			continue
		}
		if matched >= offset {
			if len(rows) < limit {
				rows = append(rows, view.DataRowInfo{Index: idx, Raw: string(line)})
			} else {
				hasMore = true
				break
			}
		}
		matched++
	}
	if err := scanner.Err(); err != nil {
		return nil, false, fmt.Errorf("reading %s: %w", ref, err)
	}
	return rows, hasMore, nil
}

// handleDataSelect (GET /data/select/{file}/{row}) selects one record as
// the Data tab's current selection — the `!` composer command's evaluation
// target (doc/features/workbench-v2.md design decision 8; the v1 Test
// button this selection used to drive died with the jsonfacts editor) —
// patches the newly and previously selected #data-row-N elements to move
// the highlight, and loads the detail pane with the record pretty-printed
// (design decision 9's master-detail: collapsible nesting via
// view.JSONTree; a line that fails to parse as JSON falls back to raw
// text, since the browser must show what is actually in the file).
func (wb *workbench) handleDataSelect(w http.ResponseWriter, r *http.Request) {
	stream, err := datastar.RequestStream(w, r)
	if err != nil {
		return
	}

	file := r.PathValue("file")
	rowParam := r.PathValue("row")
	row, err := strconv.Atoi(rowParam)
	if err != nil || row < 0 {
		_ = stream.Emit(datastar.Elements(view.DataErrors([]string{
			fmt.Sprintf("invalid row %q: must be a non-negative integer", rowParam),
		})))
		return
	}

	ref, err := wb.h.confine(file)
	if err != nil {
		_ = stream.Emit(datastar.Elements(view.DataErrors([]string{err.Error()})))
		return
	}

	raw, err := readDataLine(wb, ref, row)
	if err != nil {
		_ = stream.Emit(datastar.Elements(view.DataErrors([]string{err.Error()})))
		return
	}

	wb.selMu.Lock()
	prevFile, prevRow, prevRecord, prevValid := wb.selFile, wb.selRow, wb.selRecord, wb.selValid
	wb.selFile = file
	wb.selRow = row
	wb.selRecord = raw
	wb.selValid = true
	wb.selMu.Unlock()

	_ = stream.Emit(datastar.Elements(view.DataRow(file, row, raw, true)))
	// The previously selected row only needs unhighlighting if it's still
	// rendered in the same file's table; a stale id from a different file
	// won't match anything in the DOM, so patching it is a harmless no-op,
	// but skipping the (row == row) case avoids overwriting the just-sent
	// highlighted row with an unhighlighted one.
	if prevValid && prevFile == file && prevRow != row {
		_ = stream.Emit(datastar.Elements(view.DataRow(prevFile, prevRow, prevRecord, false)))
	}
	_ = stream.Emit(datastar.Elements(view.DataDetail(file, row, renderRecordDetail(raw))))
}

// renderRecordDetail builds the detail pane's body for one raw JSONL line:
// the parsed record as a collapsible tree, or the raw text when the line
// isn't valid JSON. Decoding with UseNumber keeps large integers'
// exact source digits instead of float64-rounding them on display.
func renderRecordDetail(raw string) html.Content {
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return tag.New("pre.doc", html.Text(raw))
	}
	return view.JSONTree(v)
}

// readDataLine opens ref (already confined) and returns the row-th
// non-blank line (0-based), or an error if the file has fewer rows.
func readDataLine(wb *workbench, ref string, row int) (string, error) {
	f, err := wb.h.fsys.Open(path.Clean(ref))
	if err != nil {
		return "", fmt.Errorf("opening %s: %w", ref, err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 1024*1024), 10*1024*1024)

	lineNum := 0
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		if lineNum == row {
			return string(line), nil
		}
		lineNum++
	}
	if err := scanner.Err(); err != nil {
		return "", fmt.Errorf("reading %s: %w", ref, err)
	}
	return "", fmt.Errorf("row %d: out of range (file has %d rows)", row, lineNum)
}
