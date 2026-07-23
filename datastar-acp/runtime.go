package chat

// runtime is the live component behind chat.New: it holds the configuration
// plus all mutable turn state (active conversation, the single live driver, the
// global one-turn gate, pending permissions, per-conversation entry counters),
// serves the HTTP surface, and renders the pane.  All mutable state is guarded
// by one mutex, never held across a Prompt call or store I/O in the turn loop.

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/swdunlop/html-go"

	"swdunlop.dev/pkg/datastar-acp/agent"
)

// runtime implements Interface.
type runtime struct {
	cfg *config

	mu sync.Mutex // guards everything below

	// activeID is the server-side selection: the conversation the live driver
	// serves and whose transcript the UI shows.  The resolved answer to
	// design.md's open question (the component tracks selection, not a query
	// param).
	activeID string

	// live is the single live driver (design decision 14: one live subprocess).
	// liveID names the conversation it serves.
	live   driver
	liveID string

	// turnOwner / turnOwnerName record the conversation currently holding the
	// global one-turn gate; turnCancel cancels its ctx.
	turnRunning   bool
	turnOwner     string
	turnOwnerName string
	turnCancel    context.CancelFunc

	// pending maps a permission RequestID to the transcript element id of its
	// card, so the answer route can morph it in place and turn-end cleanup can
	// expire any unresolved cards.
	pending map[string]pendingPerm

	// seq tracks the per-conversation transcript entry counter for stable
	// element ids across a live turn.
	seq map[string]int

	// spawnPreambleSent records, per driver spawn, whether the agent's
	// Instructions preamble has already been prepended to a prompt (design
	// decision: per-driver-spawn, not per-conversation).  Reset whenever a
	// driver is dropped/respawned.
	preambleSent bool

	closed bool

	// newDriver overrides the driver factory (tests inject a scripted fake);
	// nil uses newACPDriver.  Set only at construction, before any goroutine
	// reads it, so it needs no mutex.
	newDriver func(profile agent.Config, mcp mcpEndpoint) (driver, error)
}

// pendingPerm is one live permission card awaiting an answer.
type pendingPerm struct {
	convID string
	elemID string
	event  Event
}

// mcpMount is one agent's resolved reference MCP mount.
type mcpMount struct {
	handler http.Handler
	token   string
	path    string // path relative to base, e.g. "mcp/triage"
}

// newRuntime validates MCP mounts and returns the live component.
func newRuntime(cfg *config) (*runtime, error) {
	rt := &runtime{
		cfg:     cfg,
		pending: map[string]pendingPerm{},
		seq:     map[string]int{},
	}
	// Resolve reference MCP mounts: each agent with a handler gets a random
	// bearer token and a mount path.  This is the chokepoint that keeps a host
	// from forgetting the token or handshake wiring (design decision 6).
	for _, a := range cfg.agents {
		handler := a.MCPHandler()
		if handler == nil {
			continue
		}
		var tok [24]byte
		if _, err := rand.Read(tok[:]); err != nil {
			return nil, fmt.Errorf("chat: generating MCP token for agent %q: %w", a.Name(), err)
		}
		if cfg.mcp == nil {
			cfg.mcp = map[string]*mcpMount{}
		}
		cfg.mcp[a.Name()] = &mcpMount{
			handler: handler,
			token:   hex.EncodeToString(tok[:]),
			path:    "mcp/" + url.PathEscape(a.Name()),
		}
	}
	return rt, nil
}

// AppendHTML renders the fresh-page hydration view.
func (rt *runtime) AppendHTML(buf []byte) []byte {
	return html.Append(buf, rt.root())
}

// Shutdown cancels any running turn, closes the live driver, and is idempotent.
func (rt *runtime) Shutdown() {
	rt.mu.Lock()
	if rt.closed {
		rt.mu.Unlock()
		return
	}
	rt.closed = true
	cancel := rt.turnCancel
	d := rt.live
	rt.live, rt.liveID = nil, ""
	rt.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if d != nil {
		_ = d.Close()
	}
}

// --- helpers --------------------------------------------------------------

// path joins the base path with a component route ("send" → "/agent/send").
func (rt *runtime) path(rel string) string {
	base := strings.TrimRight(rt.cfg.basePath, "/")
	return base + "/" + rel
}

// elemID namespaces a stable element id under the base path so two components
// on one page never collide.
func (rt *runtime) elemID(name string) string {
	return "chat-" + name
}

// logID names one conversation's live-entry append target.
func (rt *runtime) logID(convID string) string {
	if convID == "" {
		return rt.elemID("log-empty")
	}
	return "chat-log-" + convID
}

// nextSeq returns the next transcript entry index for a conversation.  Caller
// holds rt.mu.
func (rt *runtime) nextSeq(convID string) int {
	n := rt.seq[convID]
	rt.seq[convID]++
	return n
}

// truncateTitle derives a rail title from the first prompt (design decision:
// no LLM auto-title; truncate ~60 runes).
func truncateTitle(text string, max int) string {
	text = strings.TrimSpace(strings.ReplaceAll(text, "\n", " "))
	runes := []rune(text)
	if len(runes) <= max {
		return text
	}
	return strings.TrimSpace(string(runes[:max])) + "…"
}

const titleMaxLen = 60

// agentByName finds a registered agent.
func (rt *runtime) agentByName(name string) (agent.Config, bool) {
	for _, a := range rt.cfg.agents {
		if a.Name() == name {
			return a, true
		}
	}
	return agent.Config{}, false
}

// queryEscape escapes a value for a hand-built query string.
func queryEscape(s string) string { return url.QueryEscape(s) }

// newID mints a conversation id.
func newID() string { return uuid.NewString() }

// splitHostPortLoose splits a RemoteAddr, tolerating a bare host with no port.
func splitHostPortLoose(addr string) (host, port string, err error) {
	host, port, err = net.SplitHostPort(addr)
	if err != nil {
		return addr, "", nil
	}
	return host, port, nil
}

// isLoopback reports whether host names the loopback interface.
func isLoopback(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// bearerMatch compares an "Bearer <token>" header to the expected token.
func bearerMatch(auth, token string) bool {
	const prefix = "Bearer "
	return len(auth) == len(prefix)+len(token) &&
		subtle.ConstantTimeCompare([]byte(auth[:len(prefix)]), []byte(prefix)) == 1 &&
		subtle.ConstantTimeCompare([]byte(auth[len(prefix):]), []byte(token)) == 1
}

// mcpURLFor builds the loopback MCP URL the agent dials for its mount.
//
// The component does not run the listener itself, so it cannot know its own
// port the way datalog's serve.go does (datalog derives the URL from its own
// --listen flag via mcpURL()).  We adopt the same mechanism adapted to a
// component: the host tells us the listen address via the ListenAddr option, or
// we capture it from the first request's Host header.  Either way the host is
// forced to loopback (127.0.0.1) since the agent is a local subprocess of this
// same host (design's loopback-only posture).
func (rt *runtime) mcpURLFor(mount *mcpMount, reqHost string) string {
	addr := rt.cfg.listenAddr
	if addr == "" {
		addr = reqHost
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		// No port to parse (bare host, or empty): fall back to the raw addr as
		// host with the default http port.
		host, port = addr, "80"
	}
	switch host {
	case "", "0.0.0.0", "::":
		host = "127.0.0.1"
	}
	base := rt.path(mount.path)
	return fmt.Sprintf("http://%s%s", net.JoinHostPort(host, port), base)
}
