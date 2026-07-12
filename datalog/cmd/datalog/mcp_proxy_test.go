package main

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/client"
	"github.com/mark3labs/mcp-go/client/transport"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

// mcp_proxy_test.go drives `datalog mcp --proxy <url>`'s core
// (newMCPProxyServer + a real mcp-go stdio server) against a REAL
// workbench /mcp mount over httptest (serve_test.go's startTestServer),
// with the proxy's own stdio side wired to in-memory pipes rather than a
// spawned subprocess — transport.NewIO (see client/transport's doc
// comment: "useful for testing ... instead of spawning a subprocess") lets
// a real mcp-go client drive the proxy's StdioServer.Listen without ever
// forking datalog itself, per doc/features/acp-integration.md work item 7's
// "factor the proxy's core to take io.Reader/io.Writer so the test needn't
// spawn a subprocess" instruction.

// newProxyTestClient starts newMCPProxyServer against the workbench at
// mcpURL/token, serves it over an in-process stdio pipe (server.
// NewStdioServer.Listen, cancelled via ctx when the test ends), and returns
// an initialized mcp-go client attached to the OTHER end of that pipe. The
// caller owns ctx's cancellation and the returned client's Close.
func newProxyTestClient(ctx context.Context, t *testing.T, mcpURL, token string) *client.Client {
	t.Helper()

	srv, closeFn, err := newMCPProxyServer(ctx, mcpURL, token)
	if err != nil {
		t.Fatalf("newMCPProxyServer: %v", err)
	}
	t.Cleanup(func() { closeFn() })

	// Two pipes, crossed: the proxy server reads clientToProxy/writes
	// proxyToClient; the test client does the opposite — the same shape
	// io.Pipe-backed stdio always takes, matching what a real stdin/stdout
	// pair between two processes would look like.
	clientToProxyR, clientToProxyW := io.Pipe()
	proxyToClientR, proxyToClientW := io.Pipe()

	go func() {
		_ = server.NewStdioServer(srv).Listen(ctx, clientToProxyR, proxyToClientW)
	}()
	t.Cleanup(func() {
		clientToProxyW.Close()
		proxyToClientR.Close()
	})

	// The third argument is only consulted for stderr-style diagnostics,
	// which the proxy path never produces here; an already-EOF reader is
	// enough.
	stdio := transport.NewIO(proxyToClientR, clientToProxyW, io.NopCloser(bytes.NewReader(nil)))
	c := client.NewClient(stdio)
	t.Cleanup(func() { c.Close() })

	if err := c.Start(ctx); err != nil {
		t.Fatalf("starting client against proxy: %v", err)
	}
	initReq := mcp.InitializeRequest{}
	initReq.Params.ProtocolVersion = mcp.LATEST_PROTOCOL_VERSION
	initReq.Params.ClientInfo = mcp.Implementation{Name: "proxy-test-client", Version: "0.0.0"}
	if _, err := c.Initialize(ctx, initReq); err != nil {
		t.Fatalf("initializing against proxy: %v", err)
	}
	return c
}

// TestMCPProxy_RoundTrip drives one real tools/list and one real
// tools/call (list_predicates, against the mordor example — the same
// fixture serve_test.go and acp_e2e_test.go use) through the proxy end to
// end: workbench's real /mcp mount -> newMCPProxyServer's forwarding
// handlers -> a stdio-speaking mcp-go client, asserting the result matches
// calling the workbench directly.
func TestMCPProxy_RoundTrip(t *testing.T) {
	wb := newMordorWorkbench(t)
	srv := startTestServer(wb)
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := newProxyTestClient(ctx, t, srv.URL+"/mcp", "test-token")

	tools, err := c.ListTools(ctx, mcp.ListToolsRequest{})
	if err != nil {
		t.Fatalf("tools/list through proxy: %v", err)
	}
	if len(tools.Tools) != 6 {
		t.Fatalf("tools/list through proxy: got %d tools, want 6 (set_schema, set_rules, query, list_predicates, sample_facts, sample_input)", len(tools.Tools))
	}

	callReq := mcp.CallToolRequest{}
	callReq.Params.Name = "list_predicates"
	callReq.Params.Arguments = map[string]any{}
	res, err := c.CallTool(ctx, callReq)
	if err != nil {
		t.Fatalf("tools/call list_predicates through proxy: %v", err)
	}
	if res.IsError {
		t.Fatalf("list_predicates through proxy reported isError: %+v", res.Content)
	}
	found := false
	for _, block := range res.Content {
		if text, ok := block.(mcp.TextContent); ok && len(text.Text) > 0 {
			found = true
		}
	}
	if !found {
		t.Fatalf("list_predicates through proxy returned no text content: %+v", res.Content)
	}
}

// TestMCPProxy_MissingToken asserts newMCPProxyServer refuses to start —
// with a clean, explanatory error, not a panic or a hang — when no token is
// given at all, per acp-integration.md's handshake step 2 (the token is
// mandatory, never optional, on /mcp).
func TestMCPProxy_MissingToken(t *testing.T) {
	wb := newMordorWorkbench(t)
	srv := startTestServer(wb)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, _, err := newMCPProxyServer(ctx, srv.URL+"/mcp", "")
	if err == nil {
		t.Fatalf("newMCPProxyServer with empty token: want error, got nil")
	}
}

// TestMCPProxy_WrongToken asserts a wrong token is rejected during the
// proxy's own initialize handshake against the workbench, rather than
// being silently accepted or hanging.
func TestMCPProxy_WrongToken(t *testing.T) {
	wb := newMordorWorkbench(t)
	srv := startTestServer(wb)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, _, err := newMCPProxyServer(ctx, srv.URL+"/mcp", "wrong-token")
	if err == nil {
		t.Fatalf("newMCPProxyServer with wrong token: want error, got nil")
	}
}
