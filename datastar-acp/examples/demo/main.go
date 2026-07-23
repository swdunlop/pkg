// Command demo serves the datastar-acp chat component on 127.0.0.1 with two
// agent profiles wrapping an operator-supplied ACP agent: "plain" runs it
// bare, and "kv" runs it behind a toy key-value MCP tool the agent is
// instructed to use.  It is intentionally small — it is documentation for how
// a host embeds the component.
//
// The ACP agent command comes from the demo's own arguments:
//
//	go run ./examples/demo -- npx -y @agentclientprotocol/claude-agent-acp
//	go run ./examples/demo -- gemini --experimental-acp
//
// The agent subprocess inherits this process's environment, so adapter
// settings like CLAUDE_CODE_EXECUTABLE just need to be set in the shell.
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"github.com/swdunlop/html-go"
	"github.com/swdunlop/html-go/tag"
	chat "swdunlop.dev/pkg/datastar-acp"
)

// listenAddr is where the demo serves; loopback only, since the component
// spawns the agent as a local subprocess and hands it a loopback MCP URL.
const listenAddr = "127.0.0.1:8765"

func main() {
	args := os.Args[1:]
	if len(args) > 0 && args[0] == "--" {
		args = args[1:]
	}
	if len(args) == 0 {
		fmt.Fprintf(os.Stderr, "usage: %s <acp-agent-command> [args...]\n", os.Args[0])
		fmt.Fprintln(os.Stderr, "e.g.:  demo npx -y @agentclientprotocol/claude-agent-acp")
		os.Exit(2)
	}
	if err := run(args[0], args[1:]); err != nil {
		log.Fatal(err)
	}
}

func run(agentCommand string, agentArgs []string) error {
	store := chat.DirStore(conversationsDir())

	// The toy MCP server: one profile hands the agent a scratch key-value store
	// over the component's reference mount.  MCPHandler is the chokepoint — the
	// component mints the bearer token, enforces loopback, and hands the URL +
	// token to the agent at session/new; the demo never touches the token.
	kv := newKVServer()

	component, err := chat.New(
		// ListenAddr lets the component build the loopback MCP URL it hands the
		// agent without waiting for a first request's Host header.
		chat.ListenAddr(listenAddr),
		chat.Store(store),
		chat.Profile(chat.AgentProfile{
			Name:         "plain",
			Command:      agentCommand,
			Args:         agentArgs,
			Instructions: "You are a helpful assistant embedded in a demo app.",
		}),
		chat.Profile(chat.AgentProfile{
			Name:         "kv",
			Command:      agentCommand,
			Args:         agentArgs,
			Instructions: "You have a key-value store MCP tool. Use kv_set to store values and kv_get to retrieve them when the user asks.",
			MCP:          chat.MCPHandler(kv),
		}),
	)
	if err != nil {
		return fmt.Errorf("building chat component: %w", err)
	}
	defer component.Shutdown()

	mux := http.NewServeMux()
	mux.Handle("/agent/", component) // the component's HTTP surface
	mux.HandleFunc("/chat.css", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/css; charset=utf-8")
		io.WriteString(w, chat.CSS)
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(page(component).AppendHTML(nil))
	})

	log.Printf("demo serving on http://%s", listenAddr)
	return http.ListenAndServe(listenAddr, mux)
}

// page wraps the component's pane (an html.Content) in a minimal shell.  The
// component initializes its own signals and opens its SSE feed from its root
// tag, so the shell only needs a datastar script tag and a container.
func page(component chat.Interface) html.Content {
	return html.Group{
		html.HTML("<!doctype html>"),
		tag.New("html").Add(
			tag.New("head").Add(
				tag.New("meta[charset=utf-8]"),
				tag.New("title", html.Text("datastar-acp demo")),
				// Datastar's browser runtime; the component's data-* attributes
				// need it to drive the SSE feed and signals.
				tag.New("script[type=module]").Set("src",
					"https://cdn.jsdelivr.net/gh/starfederation/datastar@v1.0.0/bundles/datastar.js"),
				// The component's optional stylesheet (chat.CSS), plus the one
				// obligation it leaves the page: give .chat a bounded height so
				// the transcript scrolls instead of growing the page.
				tag.New("link[rel=stylesheet][href=/chat.css]"),
				tag.New("style", html.Text(shellCSS)),
			),
			tag.New("body").Add(
				tag.New("h1", html.Text("datastar-acp demo")),
				component, // the chat pane
			),
		),
	}
}

// shellCSS is the page's own (tiny) stylesheet: fill the viewport, hand the
// chat pane the height left under the heading, and match the page chrome to
// the component's darkberg tokens so the frame doesn't clash with the pane.
const shellCSS = `
html, body { height: 100%; margin: 0; }
body {
	display: flex; flex-direction: column;
	color-scheme: light dark;
	background: light-dark(#e9e9ed, #020202);
	color: light-dark(#33374d, #c7c9d1);
	font-family: system-ui, sans-serif;
}
h1 { flex: none; margin: 0; padding: 0.5rem 1rem; font-size: 1rem; }
.chat { flex: 1 1 auto; min-height: 0; border-top: 1px solid light-dark(#bec0ca, #2b2f3f); }
`

// --- toy key-value MCP server ----------------------------------------------

// newKVServer builds a stateless streamable-HTTP MCP server exposing kv_get and
// kv_set over an in-memory map — the toy tool the "kv" profile's agent is told
// to use.
func newKVServer() http.Handler {
	store := &kvStore{data: map[string]string{}}
	srv := server.NewMCPServer("demo-kv", "0.1.0")

	srv.AddTool(
		mcp.NewTool("kv_set",
			mcp.WithDescription("Store a value under a key."),
			mcp.WithString("key", mcp.Required()),
			mcp.WithString("value", mcp.Required()),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			key, err := req.RequireString("key")
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			value, err := req.RequireString("value")
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			store.set(key, value)
			return mcp.NewToolResultText(fmt.Sprintf("stored %q = %q", key, value)), nil
		},
	)
	srv.AddTool(
		mcp.NewTool("kv_get",
			mcp.WithDescription("Retrieve the value stored under a key."),
			mcp.WithString("key", mcp.Required()),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			key, err := req.RequireString("key")
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			value, ok := store.get(key)
			if !ok {
				return mcp.NewToolResultText(fmt.Sprintf("no value stored under %q", key)), nil
			}
			return mcp.NewToolResultText(value), nil
		},
	)

	// Stateless: each POST is served without session bookkeeping, so the demo
	// mounts it as a plain http.Handler under the component's reference mount.
	return server.NewStreamableHTTPServer(srv, server.WithStateLess(true))
}

// kvStore is the toy tool's in-memory backing map.
type kvStore struct {
	mu   sync.Mutex
	data map[string]string
}

func (s *kvStore) set(k, v string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[k] = v
}

func (s *kvStore) get(k string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.data[k]
	return v, ok
}

// --- small helpers ---------------------------------------------------------

// conversationsDir is where the demo persists transcripts; overridable via
// DEMO_CONV_DIR, defaulting to a local directory.
func conversationsDir() string {
	if dir := os.Getenv("DEMO_CONV_DIR"); dir != "" {
		return dir
	}
	return "./conversations"
}
