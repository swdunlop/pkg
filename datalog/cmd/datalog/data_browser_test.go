package main

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	html "github.com/swdunlop/html-go"
	"swdunlop.dev/pkg/datalog/cmd/datalog/view"
)

// -- item 5: Load More must not nest a duplicate <ul> or leave a stale ------
// -- button live -------------------------------------------------------------

// TestDataRowsAppendFragmentCarriesNoContainerID is the view-level
// regression: DataRows (the append-mode fragment) must never re-emit the
// #data-table-body container id — appending a fragment that carries that id
// as a child of the element ALREADY on the page with that id (as the old
// code did by reusing the whole DataTableBody for both offset==0 and
// offset>0) nests a duplicate id in the DOM.
func TestDataRowsAppendFragmentCarriesNoContainerID(t *testing.T) {
	rows := []view.DataRowInfo{
		{Index: 0, Raw: `{"a":1}`},
		{Index: 1, Raw: `{"a":2}`},
	}
	out := string(html.Append(nil, view.DataRows("f.jsonl", rows, "", 0, false)))

	if strings.Contains(out, "data-table-body") {
		t.Fatalf("DataRows must not carry the #data-table-body container id (would nest a duplicate on append):\n%s", out)
	}
	if got := strings.Count(out, "<li"); got != len(rows) {
		t.Fatalf("expected exactly %d <li> row elements, got %d:\n%s", len(rows), got, out)
	}
}

// TestHTTP_DataBrowserLoadMoreDoesNotNestOrDuplicate drives the real
// handler across two chunks (an initial replace, then a Load More append)
// over a file with more rows than one page, and inspects the raw SSE
// response bytes: before the fix, the append-mode response embedded a
// second #data-table-body <ul> (nested inside the first, live in the DOM
// from the initial load) and left the old chunk's Load More button in the
// DOM alongside the freshly emitted one, since Mode("append") only adds
// content, never removes it.
func TestHTTP_DataBrowserLoadMoreDoesNotNestOrDuplicate(t *testing.T) {
	dir := t.TempDir()
	// THREE pages, not two: with only two, the second page exhausts the
	// button (morphs to an empty placeholder), which masks the
	// stranded-mid-list bug — a still-live control rendered inside the
	// append-target <ul> gets buried under every appended page.
	writeSyntheticData(t, dir, 2*dataPageSize+20)
	wb := newTestWorkbench(t, dir, "", nil, "test-token")
	srv := startTestServer(wb)
	defer srv.Close()

	applyTestSchema(t, wb, syntheticSchemaYAML)

	// GET /data/{file} (offset 0, an explicit re-select — exercises the
	// same replace path handleDataList's auto-load also takes) primes the
	// first page.
	resp1, err := http.Get(srv.URL + "/data/" + url.PathEscape("events.jsonl"))
	if err != nil {
		t.Fatalf("GET /data/{file}: %v", err)
	}
	body1, _ := io.ReadAll(resp1.Body)
	resp1.Body.Close()
	joined1 := string(body1)
	if got := strings.Count(joined1, "id='data-load-more'"); got != 1 {
		t.Fatalf("initial page: expected exactly one Load More control, got %d:\n%s", got, joined1)
	}
	// Positional regression: the control must be a SIBLING of the row
	// list, never inside it — the <ul> is the append target, so a control
	// inside it would end up stranded between page 1 and page 2 after the
	// first Load More click.
	if ul, ok := cutBetween(joined1, "id='data-table-body'", "</ul>"); !ok {
		t.Fatalf("initial page: no #data-table-body <ul> fragment found:\n%s", joined1)
	} else if strings.Contains(ul, "data-load-more") {
		t.Fatalf("Load More control rendered INSIDE the #data-table-body append target (strands mid-list on 3+ pages):\n%s", ul)
	}

	// GET /data/{file}?offset=dataPageSize (the Load More button's own
	// action) appends the second page; a third page remains, so the fresh
	// control must still be live at the new offset.
	resp2, err := http.Get(srv.URL + "/data/" + url.PathEscape("events.jsonl") + "?offset=50")
	if err != nil {
		t.Fatalf("GET /data/{file}?offset=50: %v", err)
	}
	body2, _ := io.ReadAll(resp2.Body)
	resp2.Body.Close()
	joined2 := string(body2)

	if strings.Contains(joined2, "id='data-table-body'") {
		t.Fatalf("Load More response nests a duplicate #data-table-body container:\n%s", joined2)
	}
	if got := strings.Count(joined2, "id='data-load-more'"); got != 1 {
		t.Fatalf("Load More response must patch in exactly one fresh Load More control (not append a second stale one), got %d:\n%s", got, joined2)
	}
	if !strings.Contains(joined2, "offset=100") {
		t.Fatalf("mid-file Load More response must carry a live control at the next offset (100):\n%s", joined2)
	}
}

// cutBetween returns the substring of s after the first occurrence of open
// up to the next occurrence of close.
func cutBetween(s, open, close string) (string, bool) {
	_, rest, ok := strings.Cut(s, open)
	if !ok {
		return "", false
	}
	inner, _, ok := strings.Cut(rest, close)
	return inner, ok
}

// -- item 8: subdirectory source files must be selectable in the dropdown ---

// TestDataFileListEscapesFileNameForURL is the view-level regression:
// DataFileList's data-on:change handler builds a GET /data/{file} action
// from the client-local $dataFile signal. GET /data/{file} (serve.go) is a
// single-segment route ({file}, not {file...}) — a source name containing a
// "/" (a file in a subdirectory, e.g. "logs/events.jsonl") must be
// URL-escaped before being concatenated into the action string, or the
// request widens into an extra path segment the route never matches,
// 404ing silently with no error surfaced to the user. This mirrors how the
// row-level links elsewhere in this file already escape a file name via
// pathEscape (url.PathEscape) before embedding it in an action string; here
// the value comes from a live client-side signal rather than a
// server-rendered Go string, so the equivalent escaping has to run in JS
// (encodeURIComponent) rather than at render time.
func TestDataFileListEscapesFileNameForURL(t *testing.T) {
	out := string(html.Append(nil, view.DataFileList([]string{"logs/events.jsonl"})))
	if !strings.Contains(out, "encodeURIComponent($dataFile)") {
		t.Fatalf("DataFileList's data-on:change must URL-escape $dataFile (encodeURIComponent) before building /data/{file}, got:\n%s", out)
	}
}

// TestHTTP_DataBrowserSubdirectoryFileRoundTrips is the end-to-end
// complement to the view-level test above: it hits GET /data/{file}
// directly with a subdirectory source name, escaped exactly as the fixed
// dropdown now escapes it (url.PathEscape mirrors encodeURIComponent for
// this purpose — both percent-encode "/" as "%2F"), and confirms the route
// resolves it back to the original multi-segment name and serves that
// file's rows rather than 404ing.
func TestHTTP_DataBrowserSubdirectoryFileRoundTrips(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "logs"), 0755); err != nil {
		t.Fatal(err)
	}
	mustWriteFile(t, filepath.Join(dir, "logs", "events.jsonl"),
		`{"host": "h0", "pid": 0, "cmd": "cmd0"}`+"\n")

	wb := newTestWorkbench(t, dir, "", nil, "test-token")
	srv := startTestServer(wb)
	defer srv.Close()

	const schemaYAML = `
sources:
  - file: logs/events.jsonl
    mappings:
      - predicate: event
        args: ["value.host", "value.pid", "value.cmd"]
declarations:
  - name: event
    use: "a process execution event"
`
	applyTestSchema(t, wb, schemaYAML)

	dataResp, err := http.Get(srv.URL + "/data/" + url.PathEscape("logs/events.jsonl"))
	if err != nil {
		t.Fatalf("GET /data/{file}: %v", err)
	}
	defer dataResp.Body.Close()
	if dataResp.StatusCode != http.StatusOK {
		t.Fatalf("GET /data/{file}: status = %d, want 200", dataResp.StatusCode)
	}
	body, _ := io.ReadAll(dataResp.Body)
	joined := string(body)
	if !strings.Contains(joined, "h0") {
		t.Fatalf("expected the subdirectory file's row content in the response, got:\n%s", joined)
	}
	if strings.Contains(joined, "data-error") && strings.Contains(joined, "li") {
		t.Fatalf("subdirectory file selection produced an error fragment:\n%s", joined)
	}
}

// TestHTTP_DataBrowserGzipSourceShowsRecordText pins jsonfacts.OpenSource as
// the Data Browser's read chokepoint: a .gz source (how OpTC eCAR ships)
// must render its decompressed record text, not raw gzip bytes — the
// mojibake observed on the first OpTC run. Covers both the chunk listing
// (readDataChunk) and the selection detail (readDataLine).
func TestHTTP_DataBrowserGzipSourceShowsRecordText(t *testing.T) {
	dir := t.TempDir()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err := gz.Write([]byte(`{"host": "h0", "pid": 0, "cmd": "cmd0"}` + "\n" +
		`{"host": "h1", "pid": 1, "cmd": "cmd1"}` + "\n")); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "events.jsonl.gz"), buf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}

	wb := newTestWorkbench(t, dir, "", nil, "test-token")
	srv := startTestServer(wb)
	defer srv.Close()

	const schemaYAML = `
sources:
  - file: events.jsonl.gz
    mappings:
      - predicate: event
        args: ["value.host", "value.pid", "value.cmd"]
declarations:
  - name: event
    use: "a process execution event"
`
	applyTestSchema(t, wb, schemaYAML)

	get := func(path string) string {
		t.Helper()
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET %s: status = %d, want 200", path, resp.StatusCode)
		}
		body, _ := io.ReadAll(resp.Body)
		return string(body)
	}

	rows := get("/data/" + url.PathEscape("events.jsonl.gz"))
	if !strings.Contains(rows, "h1") {
		t.Fatalf("gz source rows lack decompressed record text (mojibake regression):\n%s", rows)
	}
	detail := get("/data/select/" + url.PathEscape("events.jsonl.gz") + "/1")
	if !strings.Contains(detail, "h1") {
		t.Fatalf("gz source detail pane lacks decompressed record text:\n%s", detail)
	}

	// The agent-facing sample_input tool reads through the same chokepoint.
	out, err := wb.h.sampleInput(sampleInputInput{File: "events.jsonl.gz", Limit: 2})
	if err != nil {
		t.Fatalf("sample_input on gz source: %v", err)
	}
	if len(out.Lines) != 2 || !strings.Contains(out.Lines[0], "h0") {
		t.Fatalf("sample_input on gz source returned %#v, want decompressed record lines", out.Lines)
	}
}

// -- phase 3: master-detail + filter (workbench-v2 design decision 9) -------

// TestHTTP_DataBrowserFilter pins the substring filter's contract: only
// matching rows return, row Index stays the ORIGINAL file position (it is
// the /data/select key), pagination offsets count matching rows, and the
// Load More control re-submits the same filter so the next chunk pages the
// filtered view rather than the whole file.
func TestHTTP_DataBrowserFilter(t *testing.T) {
	dir := t.TempDir()
	// 120 rows; "cmd1" matches cmd1, cmd1x, cmd10x, cmd11x — i.e. rows 1 and
	// 10..19 and 100..119: 31 matches total, so one filtered page of 50
	// covers them all, while an unfiltered page would not reach row 100.
	writeSyntheticData(t, dir, 120)
	wb := newTestWorkbench(t, dir, "", nil, "test-token")
	srv := startTestServer(wb)
	defer srv.Close()
	applyTestSchema(t, wb, syntheticSchemaYAML)

	resp, err := http.Get(srv.URL + "/data/events.jsonl?filter=" + url.QueryEscape("CMD11"))
	if err != nil {
		t.Fatalf("GET /data with filter: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	joined := string(body)

	// Case-insensitive: CMD11 matches cmd11, cmd110..cmd119 — 11 rows.
	if got := strings.Count(joined, "<li"); got != 11 {
		t.Fatalf("filter=CMD11 should match 11 rows, got %d:\n%s", got, joined)
	}
	// Original file positions survive filtering — row 110 keeps id 110.
	if !strings.Contains(joined, "id='data-row-110'") {
		t.Fatalf("filtered rows must keep their original file index as the row id:\n%s", joined)
	}
	if strings.Contains(joined, "id='data-row-0'") {
		t.Fatalf("non-matching row 0 leaked through filter:\n%s", joined)
	}
}

// TestHTTP_DataBrowserFilterPagination drives a filter with more matches
// than one page and confirms the Load More control carries the filter and
// the next offset addresses matching rows.
func TestHTTP_DataBrowserFilterPagination(t *testing.T) {
	dir := t.TempDir()
	writeSyntheticData(t, dir, 200) // "cmd1" matches 1, 10-19, 100-199: 112 rows
	wb := newTestWorkbench(t, dir, "", nil, "test-token")
	srv := startTestServer(wb)
	defer srv.Close()
	applyTestSchema(t, wb, syntheticSchemaYAML)

	resp, err := http.Get(srv.URL + "/data/events.jsonl?filter=cmd1")
	if err != nil {
		t.Fatalf("GET /data with filter: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	joined := string(body)
	if !strings.Contains(joined, "offset=50&amp;filter=cmd1") {
		t.Fatalf("Load More under a filter must carry offset-over-matches AND the filter itself:\n%s", joined)
	}

	// Second page: offset counts MATCHES (50 matched rows deep), so the
	// page must start mid-hundreds, not at file row 50.
	resp2, err := http.Get(srv.URL + "/data/events.jsonl?offset=50&filter=cmd1")
	if err != nil {
		t.Fatalf("GET page 2: %v", err)
	}
	body2, _ := io.ReadAll(resp2.Body)
	resp2.Body.Close()
	joined2 := string(body2)
	// Matches in file order: 1, 10..19, 100..199. The 51st match (offset 50)
	// is file row 139.
	if !strings.Contains(joined2, "id='data-row-139'") {
		t.Fatalf("offset=50 under filter must resume at the 51st MATCH (file row 139):\n%s", joined2)
	}
}

// TestHTTP_DataSelectLoadsDetailPane pins the master-detail contract:
// selecting a record patches #data-detail with the parsed record as a
// collapsible JSON tree (keys visible, nesting via <details>).
func TestHTTP_DataSelectLoadsDetailPane(t *testing.T) {
	dir := t.TempDir()
	mustWriteFile(t, filepath.Join(dir, "events.jsonl"),
		`{"host": "h0", "pid": 12345678901234567890, "nest": {"a": [1, 2]}}`+"\n")
	wb := newTestWorkbench(t, dir, "", nil, "test-token")
	srv := startTestServer(wb)
	defer srv.Close()
	applyTestSchema(t, wb, syntheticSchemaYAML)

	resp, err := http.Get(srv.URL + "/data/select/events.jsonl/0")
	if err != nil {
		t.Fatalf("GET /data/select: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	joined := string(body)

	if !strings.Contains(joined, "id='data-detail'") {
		t.Fatalf("select must patch the #data-detail pane:\n%s", joined)
	}
	if !strings.Contains(joined, "json-tree") || !strings.Contains(joined, ">host<") {
		t.Fatalf("detail pane must render the record as a JSON tree with keys:\n%s", joined)
	}
	if !strings.Contains(joined, "nest") || !strings.Contains(joined, "[0]") {
		t.Fatalf("nested objects/arrays must render structurally:\n%s", joined)
	}
	// UseNumber: a 20-digit integer must keep its exact source digits, not
	// float64-round on display.
	if !strings.Contains(joined, "12345678901234567890") {
		t.Fatalf("large integers must render with exact source digits:\n%s", joined)
	}
	// Selection state moved too — the `!` command's eval target.
	if _, _, valid := wb.currentSelection(); !valid {
		t.Fatal("selection must be recorded for the ! command's eval target")
	}
}
