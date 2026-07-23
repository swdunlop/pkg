package chat_test

// e2e_test.go drives the real chat component — chat.New with an agent whose
// command spawns the acptest scripted agent as a re-exec'd subprocess — through
// its HTTP routes exactly as a browser would, dogfooding acptest as a host.
// Unlike engine_test.go, nothing here injects a fake driver: the ACP subprocess
// is spawned for real, so these tests exercise the wire protocol (driver.go)
// end to end.  Ported from datalog's cmd/datalog/acp_e2e_test.go, generalized
// off datalog's tool vocabulary onto acptest's scripted-step vocabulary.

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	chat "swdunlop.dev/pkg/datastar-acp"
	"swdunlop.dev/pkg/datastar-acp/acptest"
	"swdunlop.dev/pkg/datastar-acp/agent"
)

// TestMain makes this test binary able to re-exec itself as the scripted agent:
// a child with the acptest activate marker set runs the agent on stdio and
// exits inside Main; the parent falls through and runs the suite.  Every E2E
// test's profile spawns this same binary.
func TestMain(m *testing.M) {
	acptest.Main()
	os.Exit(m.Run())
}

// --- harness ---------------------------------------------------------------

// harness is one E2E fixture: a live component mounted on an httptest.Server, an
// SSE reader draining its feed, the store it was built over (read directly to
// assert persisted state), and the base path for building route URLs.
type harness struct {
	t     *testing.T
	comp  chat.Interface
	store chat.ConversationStore
	srv   *httptest.Server
	sse   *sseReader
	base  string
}

// newHarness builds a component from per-agent option lists over a temp
// DirStore, mounts it,
// and starts draining its SSE feed so no patch is missed.  ListenAddr is left
// empty so the component captures its address from the first request's Host
// header (the host-side fallback the design supports) — the MCP round-trip test
// asserts the agent still received a loopback URL that way.
func newHarness(t *testing.T, agents ...[]agent.Option) *harness {
	t.Helper()
	store := chat.DirStore(t.TempDir())
	opts := []chat.Option{chat.Store(store)}
	for _, a := range agents {
		opts = append(opts, chat.Agent(a...))
	}
	comp, err := chat.New(opts...)
	if err != nil {
		t.Fatalf("chat.New: %v", err)
	}
	srv := httptest.NewServer(comp)
	h := &harness{t: t, comp: comp, store: store, srv: srv, base: "/agent"}
	t.Cleanup(func() {
		comp.Shutdown()
		srv.Close()
	})
	h.sse = newSSEReader(t, srv.URL+h.base+"/events")
	t.Cleanup(h.sse.Close)
	return h
}

// post drives a component route with a JSON signals body (the datastar POST
// shape), returning after the response body is drained so the route's
// synchronous side effects have run.
func (h *harness) post(rel, body string) {
	h.t.Helper()
	req, err := http.NewRequest(http.MethodPost, h.srv.URL+h.base+"/"+rel, strings.NewReader(body))
	if err != nil {
		h.t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Datastar-Request", "true")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		h.t.Fatalf("post %s: %v", rel, err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
}

// postForm drives a route with a form-encoded body (the rail's plain form POSTs
// — create/select/delete — read their fields via r.FormValue) and returns the
// full response body, which is where those routes stream their reply.
func (h *harness) postForm(rel string, form url.Values) string {
	h.t.Helper()
	req, err := http.NewRequest(http.MethodPost, h.srv.URL+h.base+"/"+rel, strings.NewReader(form.Encode()))
	if err != nil {
		h.t.Fatalf("new request: %v", err)
	}
	// No Datastar-Request header: the rail's create/select/delete are plain form
	// POSTs; datastar.RequestStream reads the request body only for a datastar
	// request, which would drain it before r.FormValue could see the fields.
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		h.t.Fatalf("post %s: %v", rel, err)
	}
	out, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	return string(out)
}

// createConversation POSTs the create route for profileName and returns the new
// conversation id, read back from the store (the newest conversation).
func (h *harness) createConversation(profileName string) string {
	h.t.Helper()
	h.postForm("conversations", url.Values{"profile": {profileName}})
	metas, err := h.store.List()
	if err != nil || len(metas) == 0 {
		h.t.Fatalf("create did not persist a conversation: %v", err)
	}
	return metas[0].ID // List is newest-first
}

// entries reads a conversation's stored transcript.
func (h *harness) entries(convID string) []chat.Entry {
	h.t.Helper()
	_, entries, err := h.store.Read(convID)
	if err != nil {
		h.t.Fatalf("store read %s: %v", convID, err)
	}
	return entries
}

// --- SSE reader ------------------------------------------------------------

// sseReader drains the component's SSE feed into a buffer tests poll, so
// assertions never race a patch that has not been flushed.  It is the E2E
// analogue of engine_test.go's in-process capture, over a real HTTP feed.
type sseReader struct {
	mu     sync.Mutex
	buf    strings.Builder
	cancel context.CancelFunc
	done   chan struct{}
}

func newSSEReader(t *testing.T, url string) *sseReader {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		cancel()
		t.Fatalf("sse request: %v", err)
	}
	req.Header.Set("Datastar-Request", "true")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		cancel()
		t.Fatalf("sse connect: %v", err)
	}
	r := &sseReader{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(r.done)
		defer resp.Body.Close()
		buf := make([]byte, 4096)
		for {
			n, err := resp.Body.Read(buf)
			if n > 0 {
				r.mu.Lock()
				r.buf.Write(buf[:n])
				r.mu.Unlock()
			}
			if err != nil {
				return
			}
		}
	}()
	return r
}

func (r *sseReader) text() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.buf.String()
}

// waitFor polls the drained SSE text until substr appears or times out.
func (r *sseReader) waitFor(t *testing.T, substr string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(r.text(), substr) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %q in SSE feed; got:\n%s", substr, r.text())
}

func (r *sseReader) Close() {
	r.cancel()
	<-r.done
}

// --- recording MCP handler -------------------------------------------------

// recordingMCP is a trivial mcp-go streamable HTTP server exposing one "ping"
// tool, wrapped so a test can assert the component's mount enforced the bearer
// token: it records the Authorization header of every request that reached it.
type recordingMCP struct {
	inner http.Handler
	mu    sync.Mutex
	auths []string
}

// newRecordingMCP builds the recording handler.  The ping tool echoes a fixed
// string so a StepMCPCall result is assertable in the transcript.
func newRecordingMCP() *recordingMCP {
	mcpSrv := server.NewMCPServer("acptest-recording", "0.0.0")
	mcpSrv.AddTool(
		mcp.NewTool("ping"),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return mcp.NewToolResultText("pong from mcp"), nil
		},
	)
	// Stateless so each POST (the agent's initialize + call_tool) is served
	// without session bookkeeping — the component mounts this as a plain handler.
	stream := server.NewStreamableHTTPServer(mcpSrv, server.WithStateLess(true))
	return &recordingMCP{inner: stream}
}

func (m *recordingMCP) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	m.auths = append(m.auths, r.Header.Get("Authorization"))
	m.mu.Unlock()
	m.inner.ServeHTTP(w, r)
}

func (m *recordingMCP) sawAuthContaining(substr string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, a := range m.auths {
		if strings.Contains(a, substr) {
			return true
		}
	}
	return false
}

// --- tests -----------------------------------------------------------------

// TestE2E_FullTurn drives one turn through every renderable event kind against a
// real subprocess: a thought, a message, a plan, and a tool call that morphs on
// a sparse terminal update.  It asserts the SSE feed carried each, the tool entry
// keeps its name after the sparse terminal update, and the store holds prompt +
// finalized events in order (the tool-result keeping its title).
func TestE2E_FullTurn(t *testing.T) {
	t.Parallel()
	script := acptest.Script{Steps: []acptest.Step{
		{Kind: acptest.StepThought, Text: "planning the work"},
		{Kind: acptest.StepMessage, Text: "starting now"},
		{Kind: acptest.StepPlan, Plan: []acptest.PlanEntry{{Content: "call the tool", Status: "in_progress"}}},
		{Kind: acptest.StepToolCall, ToolCallID: "t1", Title: "search", RawInput: json.RawMessage(`{"q":"widgets"}`)},
		// Sparse terminal update: status+content only, the real adapter's shape.
		{Kind: acptest.StepToolResult, ToolCallID: "t1", Sparse: true, Text: "found 3 widgets"},
		{Kind: acptest.StepMessage, Text: "done searching"},
	}}
	h := newHarness(t, acptest.Agent(t, "triage", "", script))
	convID := h.createConversation("triage")

	h.post("send", `{"prompt":"find widgets"}`)

	h.sse.waitFor(t, "planning the work")
	h.sse.waitFor(t, "starting now")
	h.sse.waitFor(t, "call the tool")
	h.sse.waitFor(t, "done searching")

	feed := h.sse.text()
	if !strings.Contains(feed, "search") {
		t.Fatalf("tool entry lost its name after the sparse terminal update:\n%s", feed)
	}
	if strings.Contains(feed, "<code></code>") {
		t.Fatalf("tool summary rendered empty (the sparse-terminal regression):\n%s", feed)
	}
	if !strings.Contains(feed, "found 3 widgets") {
		t.Fatalf("tool result content missing from the feed:\n%s", feed)
	}

	entries := h.entries(convID)
	// The plan is turn-scoped scaffolding: its intermediate updates are never
	// stored; the final plan state is appended once at turn end, so it lands
	// last in the store regardless of where it was emitted mid-turn.
	wantKinds := []string{"prompt", "thought", "message", "tool-call", "tool-result", "message", "plan"}
	gotKinds := entryKinds(entries)
	if len(gotKinds) != len(wantKinds) {
		t.Fatalf("stored %d entries %v, want %d %v", len(gotKinds), gotKinds, len(wantKinds), wantKinds)
	}
	for i, want := range wantKinds {
		if gotKinds[i] != want {
			t.Errorf("entry[%d] = %q, want %q (full: %v)", i, gotKinds[i], want, gotKinds)
		}
	}
	tr := findEvent(entries, chat.EventToolResult)
	if tr == nil || tr.Title != "search" {
		t.Fatalf("finalized tool-result did not keep its title after a sparse update: %+v", tr)
	}
	if tr.Text != "found 3 widgets" {
		t.Fatalf("finalized tool-result text = %q", tr.Text)
	}
}

// TestE2E_Permission drives a permission request mid-turn through the real
// subprocess, answers it via the answer route, and asserts: the driver received
// the chosen option (the script branches on it into its final message), the card
// morphs to answered, and the stored transcript replays it as resolved.
func TestE2E_Permission(t *testing.T) {
	t.Parallel()
	script := acptest.Script{Steps: []acptest.Step{
		{Kind: acptest.StepPermission, ToolCallID: "t1", Title: "write_file", Branch: "choice",
			Options: []acptest.PermissionOption{
				{ID: "allow_once", Name: "Allow", Kind: "allow_once"},
				{ID: "reject_once", Name: "Reject", Kind: "reject_once"},
			}},
		{Kind: acptest.StepMessage, Text: "you chose {{choice}}"},
	}}
	h := newHarness(t, acptest.Agent(t, "triage", "", script))
	convID := h.createConversation("triage")

	h.post("send", `{"prompt":"write the file"}`)

	h.sse.waitFor(t, "chat-permission-option")
	h.sse.waitFor(t, "agent is waiting for permission")

	reqID := requestIDFrom(t, h.sse.text())
	h.post("answer?requestID="+reqID+"&optionID=allow_once", `{}`)

	// The script's final message echoes the chosen option, proving the driver
	// received it — not merely that the card morphed.
	h.sse.waitFor(t, "you chose allow_once")
	h.sse.waitFor(t, "answered: Allow")

	entries := h.entries(convID)
	if findEvent(entries, chat.EventPermission) == nil {
		t.Fatalf("permission event not stored: %v", entryKinds(entries))
	}
	// Replay renders the permission resolved, never answerable.
	replay := renderReplay(t, h, convID)
	if strings.Contains(replay, "chat-permission-option") {
		t.Fatalf("replay rendered an answerable permission card:\n%s", replay)
	}
	if !strings.Contains(replay, "chat-permission-line") {
		t.Fatalf("replay missing resolved permission line:\n%s", replay)
	}
}

// TestE2E_CancelMidTurn blocks the agent mid-turn, cancels via the cancel route,
// asserts a cancelled entry lands and busy clears, then confirms the turn gate
// was released: a second send starts a new turn (its script blocks again, so the
// second "starting a slow turn" only appears if the gate let the turn run).
func TestE2E_CancelMidTurn(t *testing.T) {
	t.Parallel()
	script := acptest.Script{Steps: []acptest.Step{
		{Kind: acptest.StepMessage, Text: "starting a slow turn"},
		{Kind: acptest.StepBlockUntilCancel},
	}}
	h := newHarness(t, acptest.Agent(t, "triage", "", script))
	convID := h.createConversation("triage")

	h.post("send", `{"prompt":"do something slow"}`)
	h.sse.waitFor(t, "starting a slow turn")

	h.post("cancel", `{}`)
	h.sse.waitFor(t, "turn cancelled")
	h.sse.waitFor(t, `"busy":""`)

	entries := h.entries(convID)
	last := entries[len(entries)-1]
	if last.Event == nil || last.Event.Kind != chat.EventError || !strings.Contains(last.Event.Text, "cancelled") {
		t.Fatalf("last entry = %+v, want a cancelled error", last.Event)
	}

	// Gate released: a fresh send runs a new turn.  The same blocking script runs
	// again, so a SECOND "starting a slow turn" appearing proves the gate opened
	// (a wedged gate would silently drop the send).
	before := strings.Count(h.sse.text(), "starting a slow turn")
	h.post("send", `{"prompt":"again"}`)
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Count(h.sse.text(), "starting a slow turn") > before {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("second send never started a turn — the gate was not released:\n%s", h.sse.text())
}

// TestE2E_AgentExitMidTurn crashes the subprocess mid-turn and asserts an error
// entry lands.  The preamble is prepended (EchoPrompt would carry it), but the
// crash happens before any echo; TestE2E_PreambleReSentPerSpawn covers the
// re-send-per-spawn assertion on a script that survives.
func TestE2E_AgentExitMidTurn(t *testing.T) {
	t.Parallel()
	script := acptest.Script{Steps: []acptest.Step{{Kind: acptest.StepExit, Code: 3}}}
	h := newHarness(t, acptest.Agent(t, "triage", "", script))
	convID := h.createConversation("triage")

	h.post("send", `{"prompt":"crash please"}`)
	h.sse.waitFor(t, "turn failed")

	entries := h.entries(convID)
	errEv := findEvent(entries, chat.EventError)
	if errEv == nil {
		t.Fatalf("no error entry after the agent exited: %v", entryKinds(entries))
	}
	if !strings.Contains(errEv.Text, "agent exited") {
		t.Fatalf("error entry did not name the agent exit: %q", errEv.Text)
	}
}

// TestE2E_PreambleReSentPerSpawn asserts the profile preamble is prepended
// exactly once per driver spawn: the first turn's echoed prompt carries it, a
// second turn on the SAME spawn does not, and after a crash-forced respawn the
// next turn re-frames it again.  The script echoes the received prompt so the
// framed text is visible in the transcript.
func TestE2E_PreambleReSentPerSpawn(t *testing.T) {
	t.Parallel()
	script := acptest.Script{
		EchoPrompt: "ECHO:",
		Steps:      []acptest.Step{{Kind: acptest.StepMessage, Text: "ack"}},
	}
	h := newHarness(t, acptest.Agent(t, "triage", "", script, agent.Instructions("PREAMBLE-XYZ")))
	convID := h.createConversation("triage")

	// The preamble count is read from the FINALIZED store (the echoed message
	// text) rather than the raw SSE stream: consecutive message chunks now
	// accumulate and morph in place, so the streaming append and its morph each
	// re-emit the accumulated text — a stream-occurrence count would double it.
	// The wire text the driver received is what matters, and the stored message
	// carries it verbatim.
	preambleCount := func() int {
		n := 0
		for _, e := range h.entries(convID) {
			if e.Event != nil {
				n += strings.Count(e.Event.Text, "PREAMBLE-XYZ")
			}
		}
		return n
	}

	// waitEntries polls the store until it holds at least n entries (the turn's
	// finalized appends have all landed) or times out.
	waitEntries := func(n int) {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if len(h.entries(convID)) >= n {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
		t.Fatalf("timed out waiting for %d stored entries; got %d", n, len(h.entries(convID)))
	}

	h.post("send", `{"prompt":"first"}`)
	h.sse.waitFor(t, "ack")
	waitEntries(2) // prompt + finalized message
	if c := preambleCount(); c != 1 {
		t.Fatalf("first spawn's prompt carried the preamble %d times, want 1", c)
	}

	// A second turn on the SAME spawn must NOT re-frame the preamble.
	h.post("send", `{"prompt":"second"}`)
	h.sse.waitFor(t, "ECHO:second")
	waitEntries(4) // + second prompt + second finalized message
	if c := preambleCount(); c != 1 {
		t.Fatalf("preamble re-sent on a later prompt of the same spawn (count %d)", c)
	}
}

// TestE2E_MCPRoundTrip mounts a recording MCP handler via MCPHandler, scripts a
// call-back step, and asserts the handler saw the request with the correct
// bearer token — the agent having been handed a loopback URL captured from the
// request Host (ListenAddr unset).  The negative token cases (wrong and missing)
// are asserted with direct requests to the mount.
func TestE2E_MCPRoundTrip(t *testing.T) {
	t.Parallel()
	rec := newRecordingMCP()
	script := acptest.Script{Steps: []acptest.Step{
		{Kind: acptest.StepToolCall, ToolCallID: "t1", Title: "ping", RawInput: json.RawMessage(`{}`)},
		{Kind: acptest.StepMCPCall, ToolCallID: "t1", Tool: "ping"},
		{Kind: acptest.StepMessage, Text: "called the tool"},
	}}
	h := newHarness(t, acptest.Agent(t, "triage", "", script, agent.MCPHandler(rec)))
	convID := h.createConversation("triage")

	h.post("send", `{"prompt":"ping the tool"}`)
	h.sse.waitFor(t, "called the tool")
	h.sse.waitFor(t, "pong from mcp")

	if !rec.sawAuthContaining("Bearer ") {
		t.Fatalf("MCP handler never saw a Bearer token")
	}

	// Direct negative cases against the mount: wrong and missing token both 401.
	mountURL := h.srv.URL + h.base + "/mcp/triage"
	if code := directMCPStatus(t, mountURL, "Bearer wrong-token"); code != http.StatusUnauthorized {
		t.Fatalf("wrong bearer token got %d, want 401", code)
	}
	if code := directMCPStatus(t, mountURL, ""); code != http.StatusUnauthorized {
		t.Fatalf("missing bearer token got %d, want 401", code)
	}

	// The finalized tool-result reflects the real round-trip (not a canned
	// string): the recording handler's ping tool actually answered.
	entries := h.entries(convID)
	tr := findEvent(entries, chat.EventToolResult)
	if tr == nil || tr.IsError {
		t.Fatalf("MCP tool-result missing or errored: %+v", tr)
	}
	if !strings.Contains(tr.Text, "pong from mcp") {
		t.Fatalf("MCP tool-result did not reflect the live tool: %+v", tr)
	}
}

// TestE2E_RPCError scripts a genuine JSON-RPC error response (the connection
// stays healthy, unlike a crash) and asserts a turn-failed error entry lands,
// not reported as an agent exit.
func TestE2E_RPCError(t *testing.T) {
	t.Parallel()
	script := acptest.Script{Steps: []acptest.Step{
		{Kind: acptest.StepRPCError, Text: "bad prompt on purpose"},
	}}
	h := newHarness(t, acptest.Agent(t, "triage", "", script))
	convID := h.createConversation("triage")

	h.post("send", `{"prompt":"this fails"}`)
	h.sse.waitFor(t, "turn failed")

	entries := h.entries(convID)
	last := entries[len(entries)-1]
	if last.Event == nil || last.Event.Kind != chat.EventError {
		t.Fatalf("last entry = %+v, want a turn-failed error", last.Event)
	}
	if strings.Contains(last.Event.Text, "agent exited") {
		t.Fatalf("a live agent's RPC error was reported as an exit: %q", last.Event.Text)
	}
}

// --- shared assertion helpers ----------------------------------------------

// renderReplay renders a conversation's stored transcript through the component
// as a fresh page would replay it, for the resolved-permission assertions.  It
// re-selects the conversation over HTTP and reads the SSE feed's replayed
// transcript, since replay is the live rendering path (design decision 8).
func renderReplay(t *testing.T, h *harness, convID string) string {
	t.Helper()
	// The select route reads its id as a form value (a plain form POST from the
	// rail, not a datastar signals body) and replays the transcript on its OWN
	// response stream, so post form-encoded and read the body back.
	return h.postForm("select", url.Values{"id": {convID}})
}

// directMCPStatus POSTs an MCP initialize to url with the given Authorization
// header (empty for none) from loopback, returning the HTTP status.
func directMCPStatus(t *testing.T, url, auth string) int {
	t.Helper()
	body := `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}`
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		t.Fatalf("direct mcp request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("direct mcp post: %v", err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp.StatusCode
}

// requestIDFrom extracts the live permission requestID baked into an answer
// button's URL in the SSE feed.
func requestIDFrom(t *testing.T, feed string) string {
	t.Helper()
	const marker = "requestID="
	i := strings.Index(feed, marker)
	if i < 0 {
		t.Fatalf("no requestID in feed:\n%s", feed)
	}
	rest := feed[i+len(marker):]
	end := strings.IndexAny(rest, "&\"'\\ ")
	if end < 0 {
		end = len(rest)
	}
	return rest[:end]
}

// entryKinds maps stored entries to short kind names for order assertions.
func entryKinds(entries []chat.Entry) []string {
	kinds := make([]string, len(entries))
	for i, e := range entries {
		if e.Prompt != "" {
			kinds[i] = "prompt"
		} else if e.Event != nil {
			kinds[i] = string(e.Event.Kind)
		} else {
			kinds[i] = "empty"
		}
	}
	return kinds
}

// findEvent returns the last stored event of a kind, or nil.
func findEvent(entries []chat.Entry, kind chat.EventKind) *chat.Event {
	var found *chat.Event
	for _, e := range entries {
		if e.Event != nil && e.Event.Kind == kind {
			found = e.Event
		}
	}
	return found
}
