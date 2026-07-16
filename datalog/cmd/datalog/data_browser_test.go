package main

import (
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

	if _, err := postSignalsSetSchema(t, srv); err != nil {
		t.Fatalf("priming schema: %v", err)
	}

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
	resp := postSignals(t, srv, "/jsonfacts/apply", map[string]any{"schemaText": schemaYAML})
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

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
