// Package agent describes how the chat component spawns and frames one ACP
// agent: the subprocess command, its environment and working directory, the
// instructions preamble, and the MCP server handed to it at session/new.
// Hosts build a Config with New (usually indirectly, via chat.Agent) from the
// functional options here; the zero Config is not usable.
package agent

import (
	"fmt"
	"net/http"
)

// Config is one registered agent: the recipe the chat component uses to spawn
// the ACP subprocess and frame its sessions.  A conversation records the name
// of the Config it was created with, so hosts register one Config per role
// their application needs.  Construct with New; fields are read through the
// accessor methods below.
type Config struct {
	name         string
	command      string
	args         []string
	env          []string
	dir          string
	instructions string

	mcpHandler http.Handler
	mcpURL     string
	mcpToken   string
	mcpCommand string
	mcpArgs    []string
	mcpEnv     []string
}

// Option configures one aspect of an agent Config; see New.
type Option func(*Config)

// New builds a validated Config: Name and Command are required, and at most
// one of MCPHandler, MCPEndpoint, or MCPCommand may be given.
func New(options ...Option) (Config, error) {
	var cfg Config
	for _, opt := range options {
		opt(&cfg)
	}
	if cfg.name == `` {
		return Config{}, fmt.Errorf(`agent: a name is required; see agent.Name`)
	}
	if cfg.command == `` {
		return Config{}, fmt.Errorf(`agent: agent %q has no command; see agent.Command`, cfg.name)
	}
	variants := 0
	for _, set := range []bool{cfg.mcpHandler != nil, cfg.mcpURL != ``, cfg.mcpCommand != ``} {
		if set {
			variants++
		}
	}
	if variants > 1 {
		return Config{}, fmt.Errorf(`agent: agent %q has more than one MCP server; use one of MCPHandler, MCPEndpoint, or MCPCommand`, cfg.name)
	}
	if len(cfg.mcpEnv) > 0 && cfg.mcpCommand == `` {
		return Config{}, fmt.Errorf(`agent: agent %q sets MCPEnv without MCPCommand`, cfg.name)
	}
	return cfg, nil
}

// Name identifies the agent in the new-conversation UI and in stored
// conversation metadata; it is required and must be unique among the agents
// registered with one component.
func Name(name string) Option {
	return func(cfg *Config) { cfg.name = name }
}

// Command names the ACP agent subprocess to spawn, speaking JSON-RPC 2.0 over
// its stdio; required.
func Command(command string, args ...string) Option {
	return func(cfg *Config) { cfg.command, cfg.args = command, args }
}

// Env appends "KEY=VALUE" entries to the parent environment for the spawned
// agent subprocess (e.g. CLAUDE_CODE_EXECUTABLE for the claude-agent-acp
// adapter).  Repeated use accumulates.
func Env(env ...string) Option {
	return func(cfg *Config) { cfg.env = append(cfg.env, env...) }
}

// Dir sets the subprocess working directory.  Agents that read CLAUDE.md or
// AGENTS.md pick them up from here, which is the alternative to Instructions
// for hosts that prefer instructions out of the transcript.
func Dir(dir string) Option {
	return func(cfg *Config) { cfg.dir = dir }
}

// Instructions frames the first prompt of each session — ACP has no
// system-prompt field, and the preamble keeps the specialization visible in
// the stored transcript.  It is re-injected when a persisted conversation
// resumes cold after a restart.
func Instructions(text string) Option {
	return func(cfg *Config) { cfg.instructions = text }
}

// MCPHandler mounts h (typically an mcp-go streamable HTTP server) under the
// chat component's base path as the reference MCP mount: the component
// generates a bearer token, enforces loopback with constant-time token
// comparison, and hands the URL and token to the agent at session/new.  This
// is the chokepoint that keeps hosts from forgetting the token or the
// handshake wiring.
func MCPHandler(h http.Handler) Option {
	return func(cfg *Config) { cfg.mcpHandler = h }
}

// MCPEndpoint passes an externally hosted MCP server through the same
// session/new handshake, for hosts that already mount and guard their own
// endpoint.
func MCPEndpoint(url, token string) Option {
	return func(cfg *Config) { cfg.mcpURL, cfg.mcpToken = url, token }
}

// MCPCommand hands the agent a local MCP server it spawns itself and speaks
// to over stdio — ACP's baseline transport, which every agent must support
// (unlike HTTP, which is capability-gated).  The command is not run by the
// chat component: it travels through session/new and the agent launches it in
// its own working directory, so command should be an absolute path or on the
// agent's PATH.  There is no bearer token — process spawning is the trust
// boundary.
func MCPCommand(command string, args ...string) Option {
	return func(cfg *Config) { cfg.mcpCommand, cfg.mcpArgs = command, args }
}

// MCPEnv appends "KEY=VALUE" environment entries for the MCP server an
// MCPCommand agent spawns, mirroring Env's form; valid only alongside
// MCPCommand.  Repeated use accumulates.
func MCPEnv(env ...string) Option {
	return func(cfg *Config) { cfg.mcpEnv = append(cfg.mcpEnv, env...) }
}

// Name returns the agent's registered name.
func (c Config) Name() string { return c.name }

// Command returns the subprocess command and arguments.
func (c Config) Command() (command string, args []string) { return c.command, c.args }

// Env returns the "KEY=VALUE" entries appended to the subprocess environment.
func (c Config) Env() []string { return c.env }

// Dir returns the subprocess working directory, empty for the host's cwd.
func (c Config) Dir() string { return c.dir }

// Instructions returns the session-framing preamble, empty for none.
func (c Config) Instructions() string { return c.instructions }

// MCPHandler returns the reference-mount handler, nil for none.
func (c Config) MCPHandler() http.Handler { return c.mcpHandler }

// MCPEndpoint returns the external MCP endpoint; ok is false for none.
func (c Config) MCPEndpoint() (url, token string, ok bool) {
	return c.mcpURL, c.mcpToken, c.mcpURL != ``
}

// MCPCommand returns the stdio MCP server the agent spawns itself; ok is
// false for none.
func (c Config) MCPCommand() (command string, args, env []string, ok bool) {
	return c.mcpCommand, c.mcpArgs, c.mcpEnv, c.mcpCommand != ``
}
