package chat

import "net/http"

// AgentProfile is the unit of specialization: a named way to spawn an agent with particular tools and
// instructions.  Hosts register one profile per role their application needs; each conversation records the
// profile it was created with.
type AgentProfile struct {
	// Name identifies the profile in the new-conversation UI and in stored conversation metadata.
	Name string

	// Command, Args, and Env spawn the ACP agent subprocess, speaking JSON-RPC 2.0 over its stdio.  Env
	// appends to the parent environment (e.g. CLAUDE_CODE_EXECUTABLE for the claude-agent-acp adapter).
	Command string
	Args    []string
	Env     []string

	// Dir is the subprocess working directory.  Agents that read CLAUDE.md or AGENTS.md pick them up from
	// here, which is the alternative to Instructions for hosts that prefer instructions out of the transcript.
	Dir string

	// Instructions, when set, frames the first prompt of each session — ACP has no system-prompt field, and
	// the preamble keeps the specialization visible in the stored transcript.  It is re-injected when a
	// persisted conversation resumes cold after a restart.
	Instructions string

	// MCP names the tool server handed to the agent at session/new; see MCPHandler, MCPEndpoint, and
	// MCPCommand.  Empty means the agent gets no MCP server.
	MCP MCPConfig
}

// MCPConfig describes the MCP server a profile hands its agent; construct with MCPHandler, MCPEndpoint, or
// MCPCommand.
type MCPConfig struct {
	handler  http.Handler
	url      string
	token    string
	external bool
	command  string
	args     []string
	env      []string
}

// MCPHandler mounts h (typically an mcp-go streamable HTTP server) under the component's base path as the
// reference MCP mount: the component generates a bearer token, enforces loopback with constant-time token
// comparison, and hands the URL and token to the agent at session/new.  This is the chokepoint that keeps hosts
// from forgetting the token or the handshake wiring.
func MCPHandler(h http.Handler) MCPConfig {
	return MCPConfig{handler: h}
}

// MCPEndpoint passes an externally hosted MCP server through the same session/new handshake, for hosts that
// already mount and guard their own endpoint.
func MCPEndpoint(url, token string) MCPConfig {
	return MCPConfig{url: url, token: token, external: true}
}

// MCPCommand hands the agent a local MCP server it spawns itself and speaks to over stdio — ACP's baseline
// transport, which every agent must support (unlike HTTP, which is capability-gated).  The command is not run
// by this component: it travels through session/new and the agent launches it in its own working directory,
// so command should be an absolute path or on the agent's PATH.  There is no bearer token — process spawning
// is the trust boundary.
func MCPCommand(command string, args ...string) MCPConfig {
	return MCPConfig{command: command, args: args}
}

// Env returns a copy of the config with "KEY=VALUE" environment entries set for the spawned MCP server —
// meaningful only for MCPCommand configs, mirroring AgentProfile.Env's form.  Entries without '=' get an
// empty value.
func (c MCPConfig) Env(env ...string) MCPConfig {
	c.env = append(c.env[:len(c.env):len(c.env)], env...)
	return c
}
