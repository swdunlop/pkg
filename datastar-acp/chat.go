package chat

import (
	"fmt"
	"net/http"

	"github.com/swdunlop/html-go"

	"swdunlop.dev/pkg/datastar-acp/agent"
)

func New(options ...Option) (Interface, error) {
	var cfg config
	cfg.basePath = "/agent"
	cfg.signals = defaultSignals()
	for _, opt := range options {
		opt(&cfg)
		if cfg.err != nil {
			return nil, cfg.err
		}
	}
	err := cfg.validate()
	if err != nil {
		return nil, err
	}
	return newRuntime(&cfg)
}

type Option func(*config)

// Interface describes the configured ACP chat interface.  This can handle HTTP requests, and can be provided as
// HTML content to clients.
type Interface interface {
	// Shutdown shuts down the ACP interface.
	Shutdown()

	html.Content
	http.Handler
}

type config struct {
	// basePath is the path prefix for the HTTP handler, defaults to /agent
	basePath string

	// agents are the registered agent configs; at least one is required.
	agents []agent.Config

	// store persists conversations; nil means an in-memory store lost on shutdown.
	store ConversationStore

	// bus carries datastar patches to connected pages; nil means the component owns a NewBus and serves its
	// feed under basePath.
	bus EventBus

	// ownBus notes that bus is component-owned, so ServeHTTP must mount the SSE feed for it.
	ownBus bool

	// renderPermission optionally overrides the body of permission request cards.
	renderPermission func(Event) html.Content

	// signals names the datastar signals the component binds.
	signals SignalNames

	// mcp maps agent name → resolved MCP mount info (token, mount path) for
	// agents configured with agent.MCPHandler; populated by newRuntime.
	mcp map[string]*mcpMount

	// listenAddr, when set via ListenAddr, is the host:port the host serves the
	// component on — needed to build the loopback MCP URL handed to the agent
	// (see newRuntime and the ListenAddr option).
	listenAddr string

	// err holds any configuration errors from options
	err error
}

// validate checks the configuration for errors and applies defaults that require construction.
func (cfg *config) validate() error {
	if len(cfg.agents) == 0 {
		return fmt.Errorf(`chat: at least one agent is required; see chat.Agent`)
	}
	if cfg.bus == nil {
		cfg.bus, cfg.ownBus = NewBus(), true
	}
	if cfg.store == nil {
		cfg.store = newMemoryStore()
	}
	return nil
}

var _ Interface = (*runtime)(nil)
