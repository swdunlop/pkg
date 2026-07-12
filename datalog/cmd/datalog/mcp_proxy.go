package main

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/mark3labs/mcp-go/client"
	"github.com/mark3labs/mcp-go/client/transport"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

// runMCPProxy implements `datalog mcp --proxy <url>` (doc/features/
// acp-integration.md work item 7): a stdio<->HTTP bridge so any MCP client
// that only speaks stdio (the baseline every ACP agent is required to
// support — HTTP MCP is merely an optional capability) can still reach a
// live `datalog serve` session's /mcp mount. token is read from
// DATALOG_MCP_TOKEN by the caller, never argv: argv is visible to every
// other process on the host via /proc or `ps`, and the acp handshake
// (acp-integration.md step 2) already threads the token through mcpServers'
// env for exactly this reason.
//
// Framing, not bytes: stdio MCP is newline-delimited JSON-RPC and
// streamable HTTP is per-request POSTs, so a byte-level pipe can't bridge
// them — this instead runs a real mcp-go stdio server backed by a real
// mcp-go streamable-HTTP client, joined by proxyTools below. It exits
// nonzero with a clear stderr message if the token is missing or the
// remote endpoint rejects it, rather than silently serving an empty tool
// list.
func runMCPProxy(url, token string, stdin io.Reader, stdout io.Writer) {
	ctx := context.Background()
	srv, closeFn, err := newMCPProxyServer(ctx, url, token)
	if err != nil {
		fmt.Fprintf(os.Stderr, "datalog mcp --proxy: %v\n", err)
		os.Exit(1)
	}
	defer closeFn()

	if err := server.NewStdioServer(srv).Listen(ctx, stdin, stdout); err != nil && err != io.EOF {
		fmt.Fprintf(os.Stderr, "datalog mcp --proxy: %v\n", err)
		os.Exit(1)
	}
}

// newMCPProxyServer builds the stdio-facing *server.MCPServer for the
// proxy: connects a streamable-HTTP client to url (bearer token attached
// to every request), initializes it, lists the remote's tools, and
// registers one forwarding handler per tool that calls straight through to
// the remote via CallTool. Factored out from runMCPProxy so the round-trip
// test can drive it against io.Pipe-backed stdio without spawning a
// subprocess.
//
// Only tools are forwarded — the six datalog mcp tools are all the
// workbench's /mcp mount exposes today (mountMCP registers nothing else),
// and ACP's mcpServers config is tool-shaped in the same way (an agent
// reaches the session purely through tool calls); resources and prompts
// have no analog here yet, so proxying them is unimplemented rather than
// silently dropped — a proxy is meant to be a faithful mirror of what it
// is given, and the remote currently gives it nothing else to mirror.
func newMCPProxyServer(ctx context.Context, url, token string) (*server.MCPServer, func() error, error) {
	if token == "" {
		return nil, nil, fmt.Errorf("DATALOG_MCP_TOKEN is not set (the proxy has no bearer token to authenticate with %s)", url)
	}

	c, err := client.NewStreamableHttpClient(url,
		transport.WithHTTPHeaders(map[string]string{"Authorization": "Bearer " + token}))
	if err != nil {
		return nil, nil, fmt.Errorf("connecting to %s: %w", url, err)
	}

	if err := c.Start(ctx); err != nil {
		c.Close()
		return nil, nil, fmt.Errorf("starting MCP client for %s: %w", url, err)
	}

	initReq := mcp.InitializeRequest{}
	initReq.Params.ProtocolVersion = mcp.LATEST_PROTOCOL_VERSION
	initReq.Params.ClientInfo = mcp.Implementation{Name: "datalog-mcp-proxy", Version: "0.1.0"}
	if _, err := c.Initialize(ctx, initReq); err != nil {
		c.Close()
		return nil, nil, fmt.Errorf("initializing MCP session with %s: %w (check DATALOG_MCP_TOKEN)", url, err)
	}

	tools, err := c.ListTools(ctx, mcp.ListToolsRequest{})
	if err != nil {
		c.Close()
		return nil, nil, fmt.Errorf("listing tools from %s: %w", url, err)
	}

	srv := server.NewMCPServer("datalog-mcp-proxy", "0.1.0",
		server.WithInstructions(mcpServerInstructions),
	)
	for _, tool := range tools.Tools {
		srv.AddTool(tool, proxyToolHandler(c, tool.Name))
	}

	return srv, c.Close, nil
}

// proxyToolHandler returns a ToolHandlerFunc that forwards a single
// tools/call through to the remote client c, verbatim: the incoming
// request's arguments pass through as-is, and the remote's CallToolResult
// (content blocks, isError, structured content) is returned as-is. Tool
// results already carry their own isError flag per MCP's convention (see
// mcpHandlers' tools), so a remote-reported tool error is not turned into a
// transport-level error here — only a genuine RPC failure (the remote
// unreachable, the session dead) is.
func proxyToolHandler(c *client.Client, name string) server.ToolHandlerFunc {
	return func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		fwd := mcp.CallToolRequest{}
		fwd.Params.Name = name
		fwd.Params.Arguments = req.GetArguments()
		return c.CallTool(ctx, fwd)
	}
}
