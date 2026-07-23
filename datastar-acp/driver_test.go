package chat

import (
	"testing"

	acp "github.com/coder/acp-go-sdk"
)

// TestMCPServerConfigStdio verifies a command endpoint maps to ACP's baseline
// stdio transport regardless of the agent's HTTP capability, with "KEY=VALUE"
// env entries split into ACP's name/value pairs.
func TestMCPServerConfigStdio(t *testing.T) {
	for _, httpCap := range []bool{false, true} {
		d := &acpDriver{
			mcp: mcpEndpoint{
				name:    "triage",
				command: "/usr/bin/mcp-tool",
				args:    []string{"--flag", "v"},
				env:     []string{"KEY=VALUE", "BARE"},
			},
			agentCaps: acp.AgentCapabilities{McpCapabilities: acp.McpCapabilities{Http: httpCap}},
		}
		srv, err := d.mcpServerConfig()
		if err != nil {
			t.Fatalf("httpCap=%v: %v", httpCap, err)
		}
		if srv.Stdio == nil {
			t.Fatalf("httpCap=%v: expected stdio config, got %+v", httpCap, srv)
		}
		if srv.Stdio.Name != "triage" || srv.Stdio.Command != "/usr/bin/mcp-tool" {
			t.Errorf("stdio name/command = %q/%q", srv.Stdio.Name, srv.Stdio.Command)
		}
		if len(srv.Stdio.Args) != 2 || srv.Stdio.Args[0] != "--flag" || srv.Stdio.Args[1] != "v" {
			t.Errorf("stdio args = %v", srv.Stdio.Args)
		}
		if len(srv.Stdio.Env) != 2 ||
			srv.Stdio.Env[0].Name != "KEY" || srv.Stdio.Env[0].Value != "VALUE" ||
			srv.Stdio.Env[1].Name != "BARE" || srv.Stdio.Env[1].Value != "" {
			t.Errorf("stdio env = %v", srv.Stdio.Env)
		}
	}
}

// TestMCPServerConfigHTTP verifies the HTTP endpoint still requires the agent
// capability: bearer header when declared, error when not.
func TestMCPServerConfigHTTP(t *testing.T) {
	d := &acpDriver{
		mcp:       mcpEndpoint{name: "triage", url: "http://127.0.0.1:1/mcp", token: "tok"},
		agentCaps: acp.AgentCapabilities{McpCapabilities: acp.McpCapabilities{Http: true}},
	}
	srv, err := d.mcpServerConfig()
	if err != nil {
		t.Fatal(err)
	}
	if srv.Http == nil || srv.Http.Url != "http://127.0.0.1:1/mcp" {
		t.Fatalf("expected http config, got %+v", srv)
	}
	if len(srv.Http.Headers) != 1 || srv.Http.Headers[0].Value != "Bearer tok" {
		t.Errorf("headers = %v", srv.Http.Headers)
	}

	d.agentCaps = acp.AgentCapabilities{}
	if _, err := d.mcpServerConfig(); err == nil {
		t.Fatal("expected error for HTTP endpoint against stdio-only agent")
	}
}

// TestMCPCommandEnv verifies the MCPCommand/Env constructor chain carries
// through and Env does not mutate its receiver's backing array.
func TestMCPCommandEnv(t *testing.T) {
	base := MCPCommand("tool", "a").Env("X=1")
	other := base.Env("Y=2")
	third := base.Env("Z=3")
	if len(base.env) != 1 || base.env[0] != "X=1" {
		t.Errorf("base env = %v", base.env)
	}
	if len(other.env) != 2 || other.env[1] != "Y=2" {
		t.Errorf("other env = %v", other.env)
	}
	if len(third.env) != 2 || third.env[1] != "Z=3" {
		t.Errorf("third env = %v (clobbered by sibling append?)", third.env)
	}
	if base.command != "tool" || len(base.args) != 1 || base.args[0] != "a" {
		t.Errorf("command/args = %q/%v", base.command, base.args)
	}
}
