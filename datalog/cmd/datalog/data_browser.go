package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net/http"
	"path"
	"strconv"

	"github.com/swdunlop/html-go/datastar"
	"swdunlop.dev/pkg/datalog/cmd/datalog/view"
)

// dataPageSize is the number of raw records served per Data Browser chunk
// (doc/features/web-ui.md: "Server-side pagination, 50 rows per chunk").
const dataPageSize = 50

// handleDataList is the Data Browser's file-list handler (GET /data): the
// file list comes from the session's cfg.Sources[].file, not a directory
// walk (design's explicit call-out), read under wb.h.mu since it's session
// state.
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
}

// handleDataFile is the Data Browser's paginated-row handler
// (GET /data/{file}?offset=N). Files are re-read per request (accepted
// per the design's O(offset) risk note); the model/browser-supplied file
// ref is passed through wb.h.confine before ever reaching wb.h.fsys, since
// it must never escape the data root. offset=0 (or absent) replaces the
// table body; offset>0 appends the next chunk via datastar.Mode("append").
func (wb *workbench) handleDataFile(w http.ResponseWriter, r *http.Request) {
	stream, err := datastar.RequestStream(w, r)
	if err != nil {
		return
	}

	file := r.PathValue("file")
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

	ref, err := wb.h.confine(file)
	if err != nil {
		_ = stream.Emit(datastar.Elements(view.DataErrors([]string{err.Error()})))
		return
	}

	rows, hasMore, err := readDataChunk(wb, ref, offset, dataPageSize)
	if err != nil {
		_ = stream.Emit(datastar.Elements(view.DataErrors([]string{err.Error()})))
		return
	}

	nextOffset := offset + len(rows)
	body := view.DataTableBody(file, rows, nextOffset, hasMore)

	opts := []datastar.ElementsOption{datastar.Selector("#data-table-body")}
	if offset > 0 {
		opts = append(opts, datastar.Mode("append"))
	}
	_ = stream.Emit(datastar.Elements(body, opts...))
}

// readDataChunk opens ref (already confined) through wb.h.fsys and returns
// up to limit records starting at the 0-based line offset, plus whether
// more rows remain beyond the returned chunk. Blank lines are skipped and
// do not count as rows, matching jsonfacts' own loader behavior.
func readDataChunk(wb *workbench, ref string, offset, limit int) ([]view.DataRowInfo, bool, error) {
	f, err := wb.h.fsys.Open(path.Clean(ref))
	if err != nil {
		return nil, false, fmt.Errorf("opening %s: %w", ref, err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 1024*1024), 10*1024*1024)

	lineNum := 0
	var rows []view.DataRowInfo
	hasMore := false
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		if lineNum >= offset {
			if len(rows) < limit {
				rows = append(rows, view.DataRowInfo{Index: lineNum, Raw: string(line)})
			} else {
				hasMore = true
				break
			}
		}
		lineNum++
	}
	if err := scanner.Err(); err != nil {
		return nil, false, fmt.Errorf("reading %s: %w", ref, err)
	}
	return rows, hasMore, nil
}

// handleJSONFactsTest is the Data Browser's "Test" button handler
// (GET /jsonfacts/test/{file}/{row}): selects that record as the jsonfacts
// editor's evaluation target and, in the same response, patches
// #jsonfacts-row (pretty-printed) and #jsonfacts-output (live extraction of
// that single record against the CURRENT session config).
func (wb *workbench) handleJSONFactsTest(w http.ResponseWriter, r *http.Request) {
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

	var obj any
	prettyErr := json.Unmarshal([]byte(raw), &obj)
	var pretty string
	if prettyErr != nil {
		pretty = raw // fall back to the raw line if it isn't valid JSON
	} else {
		buf, _ := json.MarshalIndent(obj, "", "  ")
		pretty = string(buf)
	}

	wb.selMu.Lock()
	wb.selFile = file
	wb.selRow = row
	wb.selRecord = raw
	wb.selValid = true
	wb.selMu.Unlock()

	_ = stream.Emit(datastar.Elements(view.JSONFactsRow(pretty)))

	lines, extractErr := wb.extractSelectedRow()
	if extractErr != nil {
		_ = stream.Emit(datastar.Elements(view.JSONFactsOutputMessage(extractErr.Error())))
		return
	}
	_ = stream.Emit(datastar.Elements(view.JSONFactsOutput(lines)))
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
