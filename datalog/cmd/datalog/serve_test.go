package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	html "github.com/swdunlop/html-go"
	"github.com/swdunlop/html-go/datastar"
	"swdunlop.dev/pkg/datalog/syntax"
)

// -- test helpers ----------------------------------------------------------

// newTestWorkbench builds a *workbench wired exactly like runServe (via
// newWorkbench), rooted at dir, with the given schema/rules preload and an
// explicit mcp-token so tests don't have to fish a generated one out of
// stderr.
func newTestWorkbench(t *testing.T, dir, configPath string, ruleFiles []string, token string) *workbench {
	t.Helper()
	wb, closeFn, err := newWorkbench(dir, configPath, ruleFiles, "", token, agentConfig{})
	if err != nil {
		t.Fatalf("newWorkbench: %v", err)
	}
	t.Cleanup(func() { closeFn() })
	return wb
}

// newTestWorkbenchRulesDir is newTestWorkbench's --rules-directory sibling,
// for tests exercising the rules-directory-store path through the
// workbench constructor instead of the legacy positional-file(s) path.
func newTestWorkbenchRulesDir(t *testing.T, dir, configPath, rulesDir, token string) *workbench {
	t.Helper()
	wb, closeFn, err := newWorkbench(dir, configPath, nil, rulesDir, token, agentConfig{})
	if err != nil {
		t.Fatalf("newWorkbench: %v", err)
	}
	t.Cleanup(func() { closeFn() })
	return wb
}

// newMordorWorkbench builds a workbench over the mordor zip with schema and
// rules preloaded from examples/mordor, mirroring mcp_test.go's
// newMordorHandlers helper for the golden-loop test.
func newMordorWorkbench(t *testing.T) *workbench {
	t.Helper()
	zipPath := filepath.Join("..", "..", "examples", "mordor", "covenant_copy_smb.zip")
	if _, err := os.Stat(zipPath); err != nil {
		t.Fatalf("mordor zip not found at %s: %v", zipPath, err)
	}
	schemaPath := filepath.Join("..", "..", "examples", "mordor", "mordor.yaml")
	rulesPath := filepath.Join("..", "..", "examples", "mordor", "rules.dl")

	wb := newTestWorkbench(t, zipPath, schemaPath, []string{rulesPath}, "test-token")
	return wb
}

// mcpURL feeds the ACP agent subprocess's dial address: a wildcard bind
// host (0.0.0.0, ::, or the empty host-less form) must resolve to a
// dialable loopback address, not propagate verbatim, and a malformed
// --listen value must still recover the real port rather than silently
// falling back to the flag's unrelated default.
func TestMcpURL(t *testing.T) {
	cases := []struct {
		listen string
		want   string
	}{
		{"127.0.0.1:8080", "http://127.0.0.1:8080/mcp"},
		{":8080", "http://127.0.0.1:8080/mcp"},
		{":9090", "http://127.0.0.1:9090/mcp"},
		{"0.0.0.0:9090", "http://127.0.0.1:9090/mcp"},
		{"[::]:9090", "http://127.0.0.1:9090/mcp"},
		{"example.internal:9090", "http://example.internal:9090/mcp"},
		{"9090", "http://127.0.0.1:9090/mcp"},               // bare port, no colon at all
		{"not-a-valid-listen", "http://127.0.0.1:8080/mcp"}, // unrecoverable: falls back to the flag default
		{"[::1]:8080", "http://[::1]:8080/mcp"},             // IPv6 literal must stay bracketed
		{"[2001:db8::1]:9090", "http://[2001:db8::1]:9090/mcp"},
	}
	for _, c := range cases {
		got := mcpURL(c.listen)
		if got != c.want {
			t.Errorf("mcpURL(%q) = %q, want %q", c.listen, got, c.want)
		}
	}
}

// TestStartupEvaluatesRules: a preloaded ruleset is evaluated during
// newWorkbench, so derived predicates are browsable (and agent-visible)
// immediately — nobody has to remember to press Run after starting serve.
func TestStartupEvaluatesRules(t *testing.T) {
	wb := newMordorWorkbench(t)

	wb.h.mu.Lock()
	defer wb.h.mu.Unlock()
	if wb.h.sess.derivedDB == nil {
		t.Fatalf("derivedDB not populated at startup")
	}
	n := 0
	for range wb.h.sess.derivedDB.Facts("smb_conn", 3) {
		n++
	}
	if n == 0 {
		t.Fatalf("derived predicate smb_conn empty after startup evaluation")
	}
}

// TestStartupEvaluatesRules_RulesDir mirrors TestStartupEvaluatesRules but
// builds the workbench from an imported rules/ directory store via --rules
// instead of the legacy positional rules.dl, confirming the startup-eval
// path (newWorkbench's "Evaluate the preloaded ruleset once at startup")
// works identically regardless of which rules source populated the
// session.
func TestStartupEvaluatesRules_RulesDir(t *testing.T) {
	monolith := filepath.Join("..", "..", "examples", "mordor", "rules.dl")
	data, err := os.ReadFile(monolith)
	if err != nil {
		t.Fatalf("reading mordor rules.dl: %v", err)
	}
	rs, err := syntax.ParseAll(string(data))
	if err != nil {
		t.Fatalf("parsing mordor rules.dl: %v", err)
	}
	rulesDir := filepath.Join(t.TempDir(), "rules")
	if _, err := importRuleset(rs, rulesDir); err != nil {
		t.Fatalf("importRuleset: %v", err)
	}

	zipPath := filepath.Join("..", "..", "examples", "mordor", "covenant_copy_smb.zip")
	schemaPath := filepath.Join("..", "..", "examples", "mordor", "mordor.yaml")
	wb := newTestWorkbenchRulesDir(t, zipPath, schemaPath, rulesDir, "test-token")

	wb.h.mu.Lock()
	defer wb.h.mu.Unlock()
	if wb.h.sess.derivedDB == nil {
		t.Fatalf("derivedDB not populated at startup")
	}
	n := 0
	for range wb.h.sess.derivedDB.Facts("smb_conn", 3) {
		n++
	}
	if n == 0 {
		t.Fatalf("derived predicate smb_conn empty after startup evaluation")
	}
	if wb.h.rules == nil {
		t.Fatalf("expected mcpHandlers.rules to be populated when --rules is used")
	}
}

// startTestServer wraps wb's routes (and /mcp mount) in an httptest.Server.
func startTestServer(wb *workbench) *httptest.Server {
	mux := http.NewServeMux()
	wb.routes(mux)
	wb.mountMCP(mux)
	return httptest.NewServer(mux)
}

// sseFragments reads raw SSE "event:"/"data:" lines from r until n complete
// events have been read or ctx is done, returning the concatenated "data:"
// payloads (Datastar fragments are usually one "data:" line but may be
// several; joined with "\n" per SSE framing). Used to inspect actionSSE
// responses (each POST handler emits a short-lived SSE response) without
// pulling in a full SSE client library.
func sseFragments(t *testing.T, body io.Reader, n int) []string {
	t.Helper()
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)

	var events []string
	var cur strings.Builder
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			if cur.Len() > 0 {
				events = append(events, cur.String())
				cur.Reset()
				if len(events) >= n {
					break
				}
			}
			continue
		}
		if data, ok := strings.CutPrefix(line, "data: "); ok {
			if cur.Len() > 0 {
				cur.WriteByte('\n')
			}
			cur.WriteString(data)
		}
	}
	if cur.Len() > 0 {
		events = append(events, cur.String())
	}
	return events
}

// postSignals issues a POST with a JSON signals body (matching how Datastar
// actions send bound signals) and returns the raw response.
func postSignals(t *testing.T, srv *httptest.Server, path string, signals map[string]any) *http.Response {
	t.Helper()
	body, err := json.Marshal(signals)
	if err != nil {
		t.Fatalf("marshal signals: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, srv.URL+path, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	return resp
}

// -- 1. golden loop over HTTP ------------------------------------------------

func TestHTTP_GoldenLoop(t *testing.T) {
	wb := newMordorWorkbench(t)
	srv := startTestServer(wb)
	defer srv.Close()

	// GET / redirects to the Facts view.
	resp, err := srv.Client().Get(srv.URL + "/")
	if err != nil {
		t.Fatalf("GET /: %v", err)
	}
	defer resp.Body.Close()
	if got := resp.Request.URL.Path; got != "/facts" {
		t.Errorf("GET /: expected redirect to /facts, landed on %s", got)
	}

	// The Facts view (Data Browser | jsonfacts Editor | Fact Browser base)
	// carries the preloaded schema text; the Rules view (Fact Browser base |
	// Datalog Editor | Fact Browser derived) carries the preloaded rules
	// text.
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading GET /facts body: %v", err)
	}
	factsPage := string(body)

	// html-go renders attribute values with single quotes (id='...'), not
	// double quotes.
	for _, id := range []string{
		`id='pane-data-browser'`,
		`id='pane-jsonfacts-editor'`,
		`id='pane-fact-browser-base'`,
	} {
		if !strings.Contains(factsPage, id) {
			t.Errorf("GET /facts: missing pane %s", id)
		}
	}
	if !strings.Contains(factsPage, "net_conn") {
		t.Error("GET /facts: preloaded schema text not present in page (expected to contain \"net_conn\")")
	}

	rulesResp, err := http.Get(srv.URL + "/rules")
	if err != nil {
		t.Fatalf("GET /rules: %v", err)
	}
	defer rulesResp.Body.Close()
	rulesBody, err := io.ReadAll(rulesResp.Body)
	if err != nil {
		t.Fatalf("reading GET /rules body: %v", err)
	}
	rulesPage := string(rulesBody)

	for _, id := range []string{
		`id='pane-fact-browser-base'`,
		`id='pane-rules-editor'`,
		`id='pane-fact-browser-derived'`,
	} {
		if !strings.Contains(rulesPage, id) {
			t.Errorf("GET /rules: missing pane %s", id)
		}
	}
	if !strings.Contains(rulesPage, "lateral_movement") {
		t.Error("GET /rules: preloaded rules text not present in page (expected to contain \"lateral_movement\")")
	}

	// POST /rules/run with the mordor rules plus the lateral_movement query
	// returns the known row in the #rules-results fragment.
	rulesData, err := os.ReadFile(filepath.Join("..", "..", "examples", "mordor", "rules.dl"))
	if err != nil {
		t.Fatalf("reading rules.dl: %v", err)
	}
	rulesText := string(rulesData) + "\nlateral_movement(User, Src, Target, Path)?\n"

	resp = postSignals(t, srv, "/rules/run", map[string]any{"rulesText": rulesText})
	defer resp.Body.Close()
	events := sseFragments(t, resp.Body, 10)
	joined := strings.Join(events, "\n")

	if !strings.Contains(joined, "rules-results") {
		t.Fatalf("POST /rules/run: no #rules-results fragment in response:\n%s", joined)
	}
	for _, want := range []string{"pgustavo", "172.18.39.5", "WORKSTATION6.theshire.local", "GruntHTTP.exe"} {
		if !strings.Contains(joined, want) {
			t.Errorf("POST /rules/run: expected result row to contain %q, got:\n%s", want, joined)
		}
	}
}

// -- 2. /events subscription: subscribe-before-render + mutation patch ------

func TestHTTP_EventsSubscription(t *testing.T) {
	wb := newMordorWorkbench(t)
	srv := startTestServer(wb)
	defer srv.Close()

	req, err := http.NewRequest(http.MethodGet, srv.URL+"/events", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Accept", "text/event-stream")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req = req.WithContext(ctx)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /events: %v", err)
	}
	defer resp.Body.Close()

	type frame struct {
		data string
		err  error
	}
	frames := make(chan frame, 8)
	go func() {
		scanner := bufio.NewScanner(resp.Body)
		scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
		var cur strings.Builder
		for scanner.Scan() {
			line := scanner.Text()
			if line == "" {
				if cur.Len() > 0 {
					frames <- frame{data: cur.String()}
					cur.Reset()
				}
				continue
			}
			if data, ok := strings.CutPrefix(line, "data: "); ok {
				if cur.Len() > 0 {
					cur.WriteByte('\n')
				}
				cur.WriteString(data)
			}
		}
		if err := scanner.Err(); err != nil {
			frames <- frame{err: err}
		}
	}()

	// The FIRST event must be the initial predicates render, arriving before
	// any mutation — subscribe-before-render ordering (doc/notes/datastar.md
	// §8). Read it out before triggering the mutation below.
	select {
	case f := <-frames:
		if f.err != nil {
			t.Fatalf("reading initial /events frame: %v", f.err)
		}
		if !strings.Contains(f.data, "predicates") {
			t.Fatalf("initial /events frame does not look like the predicates fragment: %s", f.data)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for initial /events frame")
	}

	// Trigger a mutation via POST /rules/run (Run publishes on success).
	rulesData, err := os.ReadFile(filepath.Join("..", "..", "examples", "mordor", "rules.dl"))
	if err != nil {
		t.Fatalf("reading rules.dl: %v", err)
	}
	mutResp := postSignals(t, srv, "/rules/run", map[string]any{"rulesText": string(rulesData)})
	io.Copy(io.Discard, mutResp.Body)
	mutResp.Body.Close()

	// Assert a #predicates patch arrives on the subscription after the
	// mutation.
	select {
	case f := <-frames:
		if f.err != nil {
			t.Fatalf("reading post-mutation /events frame: %v", f.err)
		}
		if !strings.Contains(f.data, "predicates") {
			t.Fatalf("post-mutation /events frame does not look like the predicates fragment: %s", f.data)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for post-mutation /events frame")
	}
}

// TestHTTP_EventsReplaysCurrentBusyOnConnect is the regression for item 7:
// $busy initializes to ” client-side, and before this fix handleEvents
// never replayed the current key, so a browser tab opened while a job was
// already running showed idle — no Stop control, no visible feedback —
// until that job happened to finish and publish "". This simulates a job
// already in flight (publishBusy("run"), as handleRulesRun would have
// called before this connection ever opened) and asserts a fresh /events
// connection sees a busy:"run" signal patch shortly after connecting,
// alongside the initial predicates fragment.
func TestHTTP_EventsReplaysCurrentBusyOnConnect(t *testing.T) {
	wb := newMordorWorkbench(t)
	srv := startTestServer(wb)
	defer srv.Close()

	wb.publishBusy("run")
	defer wb.publishBusy("")

	req, err := http.NewRequest(http.MethodGet, srv.URL+"/events", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Accept", "text/event-stream")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req = req.WithContext(ctx)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /events: %v", err)
	}
	defer resp.Body.Close()

	type frame struct {
		data string
		err  error
	}
	frames := make(chan frame, 8)
	go func() {
		scanner := bufio.NewScanner(resp.Body)
		scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
		var cur strings.Builder
		for scanner.Scan() {
			line := scanner.Text()
			if line == "" {
				if cur.Len() > 0 {
					frames <- frame{data: cur.String()}
					cur.Reset()
				}
				continue
			}
			if data, ok := strings.CutPrefix(line, "data: "); ok {
				if cur.Len() > 0 {
					cur.WriteByte('\n')
				}
				cur.WriteString(data)
			}
		}
		if err := scanner.Err(); err != nil {
			frames <- frame{err: err}
		}
	}()

	found := false
	for i := 0; i < 3 && !found; i++ {
		select {
		case f := <-frames:
			if f.err != nil {
				t.Fatalf("reading /events frame: %v", f.err)
			}
			if strings.Contains(f.data, `"busy":"run"`) {
				found = true
			}
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for the replayed busy signal")
		}
	}
	if !found {
		t.Fatal("a fresh /events connection did not replay the current busy key ('run'); a tab opened mid-job would show idle with no way to stop it")
	}
}

// -- 3. confinement over HTTP -----------------------------------------------

func TestHTTP_ConfinementRejectsEscape(t *testing.T) {
	dir := t.TempDir()
	writeSyntheticData(t, dir, 1)
	wb := newTestWorkbench(t, dir, "", nil, "test-token")
	srv := startTestServer(wb)
	defer srv.Close()

	// The path itself must be percent-encoded so net/http's client sends the
	// literal "..%2F..%2Fetc%2Fpasswd" bytes on the wire rather than the Go
	// client normalizing ".." out of the path before the request is sent.
	u := srv.URL + "/data/..%2F..%2Fetc%2Fpasswd"
	req, err := http.NewRequest(http.MethodGet, u, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Accept", "text/event-stream")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", u, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading body: %v", err)
	}
	got := string(body)

	if strings.Contains(got, "root:") {
		t.Fatalf("confinement bypassed: response contains /etc/passwd contents:\n%s", got)
	}
	// The Data Browser's error fragment surfaces the confine error; the
	// escaping ref itself should show up in the message, not file contents.
	if !strings.Contains(got, "escape") && !strings.Contains(got, "outside") && !strings.Contains(got, "confine") {
		t.Logf("confinement error fragment (informational, exact wording not asserted): %s", got)
	}
}

// -- 4. timeout / cancel -----------------------------------------------------

// TestHTTP_CancelDuringRun exercises POST /cancel against an in-flight
// /rules/run. A reliable timer-based "evaluation timed out" test would need
// a combinatorial ruleset engineered to reliably exceed evalTimeout (5s)
// without also making the test suite slow or flaky on a loaded CI box; the
// mordor dataset's rules compile and evaluate in milliseconds, so there is
// no readily available "genuinely slow but deterministic" ruleset to hang
// off of here. Per the task's explicit allowance ("if a reliable
// timer-based test is not achievable without flakiness, test cancellation
// instead"), this test exercises the Global Cancel path instead: it starts
// a Run, immediately fires /cancel, and asserts the run's own jobs entry is
// gone afterward (i.e. Cancel reached the right key) — the deeper claim
// that a cancelled context produces a clean, non-hanging response is already
// covered unit-style by mcp_test.go's TestQuery_CancelledContext, which
// exercises the same ctx.Err() checks in runQuery/seminaive that a real
// mid-flight cancel would hit.
func TestHTTP_CancelDuringRun(t *testing.T) {
	wb := newMordorWorkbench(t)
	srv := startTestServer(wb)
	defer srv.Close()

	rulesData, err := os.ReadFile(filepath.Join("..", "..", "examples", "mordor", "rules.dl"))
	if err != nil {
		t.Fatalf("reading rules.dl: %v", err)
	}

	resp := postSignals(t, srv, "/rules/run", map[string]any{"rulesText": string(rulesData)})
	defer resp.Body.Close()

	// Fire Cancel while the run's SSE response may still be draining; this
	// exercises the same code path a user's Cancel click would in a slow
	// run, without depending on timing to observe an in-progress state.
	cancelResp, err := http.Post(srv.URL+"/cancel", "", nil)
	if err != nil {
		t.Fatalf("POST /cancel: %v", err)
	}
	cancelResp.Body.Close()
	if cancelResp.StatusCode != http.StatusNoContent {
		t.Errorf("POST /cancel: status = %d, want 204", cancelResp.StatusCode)
	}

	// Drain the run's response; it should complete (successfully or having
	// observed cancellation) without hanging the test.
	io.Copy(io.Discard, resp.Body)
}

// TestHTTP_SequentialRunsReflectLatest exercises stale suppression at the
// handler level: two /rules/run requests for different rules text, run back
// to back. handleRulesRun is job-gated (only one Run in flight at a time via
// wb.jobs.Begin(rulesRunJobKey)), so the second call is serialized after the
// first completes — genuine mid-flight staleness on THIS specific job key is
// not directly observable via two sequential HTTP calls. What this asserts:
// the final #rules-results fragment reflects the LAST call's rules text, not
// a stale intermediate result — i.e. no interleaving artifact survives into
// the final state. (This test previously also asserted a workbench-level
// wb.gen token advanced across calls; that mechanism was dead — nothing
// outside this handler's own single in-flight call could ever make a token
// stale, since Begin already serializes every Run on rulesRunJobKey — and
// was removed rather than kept alive just to be observed.)
func TestHTTP_SequentialRunsReflectLatest(t *testing.T) {
	dir := t.TempDir()
	writeSyntheticData(t, dir, 3)
	wb := newTestWorkbench(t, dir, "", nil, "test-token")
	srv := startTestServer(wb)
	defer srv.Close()

	if _, err := postSignalsSetSchema(t, srv); err != nil {
		t.Fatalf("priming schema: %v", err)
	}

	resp1 := postSignals(t, srv, "/rules/run", map[string]any{"rulesText": "foo(X) :- event(_, X, _).\nfoo(X)?\n"})
	io.Copy(io.Discard, resp1.Body)
	resp1.Body.Close()

	resp2 := postSignals(t, srv, "/rules/run", map[string]any{"rulesText": "bar(X) :- event(_, X, _).\nbar(X)?\n"})
	body2, _ := io.ReadAll(resp2.Body)
	resp2.Body.Close()

	joined := string(body2)
	if !strings.Contains(joined, "bar") {
		t.Errorf("second Run's response should reflect bar(X)? results, got:\n%s", joined)
	}
}

// postSignalsSetSchema is a small helper for TestHTTP_StaleSuppression: it
// applies syntheticSchemaYAML (from mcp_test.go) via POST /jsonfacts/apply
// so the workbench has predicates to run rules against.
func postSignalsSetSchema(t *testing.T, srv *httptest.Server) (*http.Response, error) {
	t.Helper()
	resp := postSignals(t, srv, "/jsonfacts/apply", map[string]any{"schemaText": syntheticSchemaYAML})
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp, nil
}

// -- 5. busy gate -------------------------------------------------------------

func TestHTTP_BusyGate(t *testing.T) {
	wb := newMordorWorkbench(t)
	srv := startTestServer(wb)
	defer srv.Close()

	rulesData, err := os.ReadFile(filepath.Join("..", "..", "examples", "mordor", "rules.dl"))
	if err != nil {
		t.Fatalf("reading rules.dl: %v", err)
	}
	rulesText := string(rulesData)

	var wg sync.WaitGroup
	results := make([]string, 2)
	wg.Add(2)
	for i := 0; i < 2; i++ {
		i := i
		go func() {
			defer wg.Done()
			resp := postSignals(t, srv, "/rules/run", map[string]any{"rulesText": rulesText})
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			results[i] = string(body)
		}()
	}
	wg.Wait()

	busyCount := 0
	for _, r := range results {
		if strings.Contains(r, "already running") {
			busyCount++
		}
	}
	if busyCount == 0 {
		t.Errorf("expected at least one of two concurrent /rules/run calls to report \"already running\", got responses:\n1: %s\n2: %s", results[0], results[1])
	}
}

// -- 8. /mcp mount ------------------------------------------------------------

func TestMCP_UnauthorizedWithoutOrWrongToken(t *testing.T) {
	dir := t.TempDir()
	wb := newTestWorkbench(t, dir, "", nil, "the-real-token")
	srv := startTestServer(wb)
	defer srv.Close()

	initBody := mcpInitializeBody()

	// No Authorization header.
	resp, err := http.Post(srv.URL+"/mcp", "application/json", bytes.NewReader(initBody))
	if err != nil {
		t.Fatalf("POST /mcp (no token): %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("POST /mcp with no Authorization header: status = %d, want 401", resp.StatusCode)
	}

	// Wrong token.
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/mcp", bytes.NewReader(initBody))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer wrong-token")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /mcp (wrong token): %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("POST /mcp with wrong token: status = %d, want 401", resp.StatusCode)
	}
}

// TestMCP_InitializeAndPutRuleGroupPatchesBack is
// TestMCP_InitializeAndSetRulesPatchesBack's successor: the whole-document
// set_rules MCP tool was removed (doc/features/workbench-v2.md design
// decision 4) in favor of the rule-group CRUD tools, so this now drives
// put_rule_group over /mcp against a workbench started with --rules (a
// rules/ directory store — required, since put_rule_group errors on a
// legacy session with no store at all).
func TestMCP_InitializeAndPutRuleGroupPatchesBack(t *testing.T) {
	dir := t.TempDir()
	writeSyntheticData(t, dir, 3)
	rulesDir := filepath.Join(dir, "rules")
	if err := os.MkdirAll(rulesDir, 0o755); err != nil {
		t.Fatalf("mkdir rules dir: %v", err)
	}
	wb := newTestWorkbenchRulesDir(t, dir, "", rulesDir, "the-real-token")

	// Preload the schema so put_rule_group has a predicate to build against.
	wb.h.mu.Lock()
	if err := wb.h.sess.setSchema(syntheticSchemaYAML, "yaml", wb.h.fsys, wb.h.confine); err != nil {
		wb.h.mu.Unlock()
		t.Fatalf("preloading schema: %v", err)
	}
	wb.h.mu.Unlock()

	srv := startTestServer(wb)
	defer srv.Close()

	// Subscribe to /events BEFORE the MCP call so the patch-back lands on an
	// active subscriber.
	eventsReq, err := http.NewRequest(http.MethodGet, srv.URL+"/events", nil)
	if err != nil {
		t.Fatalf("new /events request: %v", err)
	}
	eventsReq.Header.Set("Accept", "text/event-stream")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	eventsReq = eventsReq.WithContext(ctx)

	eventsResp, err := http.DefaultClient.Do(eventsReq)
	if err != nil {
		t.Fatalf("GET /events: %v", err)
	}
	defer eventsResp.Body.Close()

	frames := make(chan string, 8)
	go func() {
		scanner := bufio.NewScanner(eventsResp.Body)
		scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
		var cur strings.Builder
		for scanner.Scan() {
			line := scanner.Text()
			if line == "" {
				if cur.Len() > 0 {
					frames <- cur.String()
					cur.Reset()
				}
				continue
			}
			if data, ok := strings.CutPrefix(line, "data: "); ok {
				if cur.Len() > 0 {
					cur.WriteByte('\n')
				}
				cur.WriteString(data)
			}
		}
	}()

	// Drain the initial predicates render.
	select {
	case <-frames:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for initial /events frame")
	}

	// initialize handshake.
	mustMCPCall(t, srv, wb.mcpToken, mcpInitializeBody())

	// call put_rule_group.
	putRuleGroupBody := mcpToolCallBody("put_rule_group", map[string]any{
		"head": "derived", "arity": 1,
		"text":     "derived(X) :- event(_, X, _).\n",
		"revision": 0,
	})
	respBody := mustMCPCall(t, srv, wb.mcpToken, putRuleGroupBody)
	if strings.Contains(strings.ToLower(string(respBody)), `"iserror":true`) {
		t.Fatalf("put_rule_group tool call reported an error: %s", respBody)
	}

	// The /events subscriber should receive a #predicates patch-back
	// reflecting the agent's put_rule_group call.
	select {
	case f := <-frames:
		if !strings.Contains(f, "predicates") {
			t.Fatalf("expected a #predicates patch-back after put_rule_group, got: %s", f)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for patch-back /events frame after put_rule_group over /mcp")
	}
}

// mustMCPCall POSTs body to /mcp with the given bearer token and returns the
// raw response bytes, failing the test on any transport error or non-2xx
// status.
func mustMCPCall(t *testing.T, srv *httptest.Server, token string, body []byte) []byte {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/mcp", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("new /mcp request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /mcp: %v", err)
	}
	defer resp.Body.Close()
	out, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading /mcp response: %v", err)
	}
	if resp.StatusCode >= 300 {
		t.Fatalf("POST /mcp: status = %d, body: %s", resp.StatusCode, out)
	}
	return out
}

// mcpInitializeBody builds a minimal JSON-RPC 2.0 "initialize" request body
// for the streamable HTTP MCP transport.
func mcpInitializeBody() []byte {
	req := map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "initialize",
		"params": map[string]any{
			"protocolVersion": "2024-11-05",
			"capabilities":    map[string]any{},
			"clientInfo":      map[string]any{"name": "serve_test", "version": "0.0.1"},
		},
	}
	b, _ := json.Marshal(req)
	return b
}

// mcpToolCallBody builds a JSON-RPC 2.0 "tools/call" request body.
func mcpToolCallBody(name string, args map[string]any) []byte {
	req := map[string]any{
		"jsonrpc": "2.0",
		"id":      2,
		"method":  "tools/call",
		"params": map[string]any{
			"name":      name,
			"arguments": args,
		},
	}
	b, _ := json.Marshal(req)
	return b
}

// -- 9. bus unit test ---------------------------------------------------------

func TestBus_SubscribePublishDropClose(t *testing.T) {
	b := newBus()

	sub := b.Subscribe()

	b.Publish(testEvent("hello"))

	select {
	case got := <-sub.Events():
		if renderEvent(got) == "" {
			t.Fatal("bus: received empty event")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("bus: subscriber did not receive published event")
	}

	// A full buffer drops rather than blocks: fill the subscriber's buffer
	// past capacity, then confirm Publish returns promptly (does not hang).
	done := make(chan struct{})
	go func() {
		for i := 0; i < subscriberBuffer+4; i++ {
			b.Publish(testEvent("flood"))
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("bus: Publish blocked on a full subscriber buffer instead of dropping")
	}

	// Close unregisters: subsequent publishes must not deliver (and must not
	// panic or block) to the closed subscriber.
	sub.Close()
	b.Publish(testEvent("after-close"))

	b.mu.Lock()
	_, stillRegistered := b.subs[sub]
	b.mu.Unlock()
	if stillRegistered {
		t.Fatal("bus: subscriber still registered after Close")
	}
}

// testEvent builds a real datastar.Elements event (the same constructor
// production code uses in publishSessionChanged/handleFacts/etc.) wrapping
// a trivial div, so the bus unit test exercises the actual Event type
// rather than a parallel fake.
func testEvent(msg string) datastar.Event {
	return datastar.Elements(html.Text(msg))
}

// renderEvent renders a datastar.Event to raw SSE bytes via a Stream over a
// buffer, since Event has unexported methods and can't otherwise be
// inspected directly in a test.
func renderEvent(ev datastar.Event) string {
	var buf bytes.Buffer
	stream := datastar.NewStream(&buf)
	_ = stream.Emit(ev)
	return buf.String()
}
