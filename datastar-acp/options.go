package chat

import (
	"fmt"

	"github.com/swdunlop/html-go"

	"swdunlop.dev/pkg/datastar-acp/agent"
)

// BasePath overrides the path prefix the HTTP handler serves under, defaults to /agent.
func BasePath(prefix string) Option {
	return func(cfg *config) { cfg.basePath = prefix }
}

// Store overrides the conversation store, which defaults to an in-memory store that is lost on shutdown.  Most
// applications want DirStore so conversations survive restarts.
func Store(store ConversationStore) Option {
	return func(cfg *config) { cfg.store = store }
}

// Bus provides the datastar event bus shared with the host page.  A datastar page has exactly one SSE feed; hosts
// that already publish their own patches should provide their bus here.  When omitted, the component owns an
// internal bus and serves its feed under the base path.
func Bus(bus EventBus) Option {
	return func(cfg *config) { cfg.bus = bus }
}

// Agent registers a named agent built from the agent subpackage's options; at least one is required.  The
// new-conversation UI offers one choice per registered agent, and each conversation records the name of the
// agent it was created with.
func Agent(options ...agent.Option) Option {
	return func(cfg *config) {
		a, err := agent.New(options...)
		if err != nil {
			cfg.err = err
			return
		}
		for _, other := range cfg.agents {
			if other.Name() == a.Name() {
				cfg.err = fmt.Errorf(`chat: duplicate agent %q`, a.Name())
				return
			}
		}
		cfg.agents = append(cfg.agents, a)
	}
}

// AgentDriver registers a named agent served by a host-supplied Driver
// factory instead of a spawned ACP subprocess — the seam for embedding an
// in-process agent (an LLM library wired straight to the host's tools) behind
// the same pane, transcript, and turn engine as an ACP agent.  The factory is
// called lazily on the first prompt, and again after any fatal turn error or
// conversation switch (the component keeps one live Driver and rebuilds on
// demand; see Driver's contract).
//
// options may add Instructions; agent.Command and the MCP options do not
// apply — the host wires the driver's tools itself, in process — and setting
// any of them is a configuration error.
func AgentDriver(name string, factory func() (Driver, error), options ...agent.Option) Option {
	return func(cfg *config) {
		if factory == nil {
			cfg.err = fmt.Errorf(`chat: agent %q has a nil Driver factory`, name)
			return
		}
		Agent(append([]agent.Option{agent.Name(name)}, options...)...)(cfg)
		if cfg.err != nil {
			return
		}
		a := cfg.agents[len(cfg.agents)-1]
		command, _ := a.Command()
		_, _, hasEndpoint := a.MCPEndpoint()
		_, _, _, hasCommand := a.MCPCommand()
		switch {
		case command != ``:
			cfg.err = fmt.Errorf(`chat: agent %q sets both a Driver factory and agent.Command; use one`, name)
		case a.MCPHandler() != nil || hasEndpoint || hasCommand:
			cfg.err = fmt.Errorf(`chat: agent %q sets both a Driver factory and an MCP option; a Driver wires its own tools`, name)
		}
		if cfg.err != nil {
			return
		}
		if cfg.drivers == nil {
			cfg.drivers = map[string]func() (Driver, error){}
		}
		cfg.drivers[name] = factory
	}
}

// ListenAddr tells the component the host:port the host serves it on, so it
// can build the loopback MCP URL it hands the agent at session/new.  The
// component does not run the listener itself (unlike datalog's serve, which
// derives the URL from its own --listen flag), so a host using the reference
// MCP mount must supply this — or leave it empty to let the component capture
// the address from the first request's Host header.  The host is always forced
// to loopback (127.0.0.1) since the agent is a local subprocess.
func ListenAddr(addr string) Option {
	return func(cfg *config) { cfg.listenAddr = addr }
}

// PermissionRenderer overrides the rendering of permission request cards, letting hosts substitute richer
// content (such as diff cards derived from the tool call's raw input) over the same answer plumbing.  The
// returned content replaces only the card body; the option buttons and answer routing remain the component's.
func PermissionRenderer(render func(Event) html.Content) Option {
	return func(cfg *config) { cfg.renderPermission = render }
}

// Signals overrides the datastar signal names used by the component; zero-valued fields keep their defaults.
// Signals are initialized via data-signals on the component's root tag, but $busy is deliberately shared: a host
// with its own long-running jobs may use it as the page-wide mutex.
func Signals(names SignalNames) Option {
	return func(cfg *config) {
		if names.Busy != `` {
			cfg.signals.Busy = names.Busy
		}
		if names.BusyConv != `` {
			cfg.signals.BusyConv = names.BusyConv
		}
		if names.BusyConvName != `` {
			cfg.signals.BusyConvName = names.BusyConvName
		}
		if names.Prompt != `` {
			cfg.signals.Prompt = names.Prompt
		}
	}
}

// SignalNames names the datastar signals the component binds; see Signals.
type SignalNames struct {
	Busy         string // page-wide mutex naming the running job, default "busy"
	BusyConv     string // id of the conversation that owns the running turn, default "busyConv"
	BusyConvName string // display name of that conversation, default "busyConvName"
	Prompt       string // composer textarea binding, default "prompt"
}

// defaultSignals returns the datalog workbench signal vocabulary.
func defaultSignals() SignalNames {
	return SignalNames{Busy: `busy`, BusyConv: `busyConv`, BusyConvName: `busyConvName`, Prompt: `prompt`}
}
