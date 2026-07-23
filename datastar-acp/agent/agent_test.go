package agent

import (
	"strings"
	"testing"
)

// TestNewValidation covers the invariants New enforces: required name and
// command, a single MCP variant, and MCPEnv only alongside MCPCommand.
func TestNewValidation(t *testing.T) {
	cases := []struct {
		name    string
		options []Option
		wantErr string
	}{
		{"missing name", []Option{Command("x")}, "name is required"},
		{"missing command", []Option{Name("triage")}, "no command"},
		{"two mcp variants", []Option{Name("triage"), Command("x"),
			MCPEndpoint("http://127.0.0.1:1/mcp", "tok"), MCPCommand("tool")}, "more than one MCP server"},
		{"mcp env without command", []Option{Name("triage"), Command("x"),
			MCPEnv("X=1")}, "MCPEnv without MCPCommand"},
		{"minimal ok", []Option{Name("triage"), Command("x")}, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := New(c.options...)
			switch {
			case c.wantErr == "" && err != nil:
				t.Fatalf("New: %v", err)
			case c.wantErr != "" && err == nil:
				t.Fatalf("expected error containing %q", c.wantErr)
			case c.wantErr != "" && !strings.Contains(err.Error(), c.wantErr):
				t.Fatalf("error %q does not contain %q", err, c.wantErr)
			}
		})
	}
}

// TestOptionsCarryThrough verifies every option lands in the Config's
// accessors and that Env/MCPEnv accumulate across repeated use.
func TestOptionsCarryThrough(t *testing.T) {
	cfg, err := New(
		Name("kv"),
		Command("acp-agent", "--flag"),
		Env("A=1"), Env("B=2"),
		Dir("/work"),
		Instructions("preamble"),
		MCPCommand("tool", "a"),
		MCPEnv("X=1"), MCPEnv("Y=2"),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if cfg.Name() != "kv" || cfg.Dir() != "/work" || cfg.Instructions() != "preamble" {
		t.Errorf("name/dir/instructions = %q/%q/%q", cfg.Name(), cfg.Dir(), cfg.Instructions())
	}
	if cmd, args := cfg.Command(); cmd != "acp-agent" || len(args) != 1 || args[0] != "--flag" {
		t.Errorf("command = %q %v", cmd, args)
	}
	if env := cfg.Env(); len(env) != 2 || env[0] != "A=1" || env[1] != "B=2" {
		t.Errorf("env = %v", env)
	}
	cmd, args, env, ok := cfg.MCPCommand()
	if !ok || cmd != "tool" || len(args) != 1 || args[0] != "a" {
		t.Errorf("mcp command = %q %v (ok=%v)", cmd, args, ok)
	}
	if len(env) != 2 || env[0] != "X=1" || env[1] != "Y=2" {
		t.Errorf("mcp env = %v", env)
	}
	if h := cfg.MCPHandler(); h != nil {
		t.Errorf("unexpected mcp handler %v", h)
	}
	if _, _, ok := cfg.MCPEndpoint(); ok {
		t.Error("unexpected mcp endpoint")
	}
}
