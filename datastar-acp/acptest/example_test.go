package acptest_test

import (
	"os"
	"testing"

	chat "swdunlop.dev/pkg/datastar-acp"
	"swdunlop.dev/pkg/datastar-acp/acptest"
)

// Example shows a host wiring the scripted agent: TestMain calls acptest.Main
// first, so a re-exec'd child becomes the agent instead of re-running the suite,
// then a test registers a profile whose Command re-execs this same binary and
// replays a Script.  A real host mounts the returned component in an
// httptest.Server and drives its HTTP routes.
func Example() {
	// In TestMain:
	//
	//	func TestMain(m *testing.M) {
	//		acptest.Main()      // a re-exec'd child runs the agent and exits here
	//		os.Exit(m.Run())    // the parent runs the suite as usual
	//	}
	//
	// In a test, build a profile that replays a two-step script:
	var t *testing.T // provided by the test
	script := acptest.Script{Steps: []acptest.Step{
		{Kind: acptest.StepThought, Text: "considering"},
		{Kind: acptest.StepMessage, Text: "hello from the scripted agent"},
	}}
	profile := acptest.Profile(t, "demo", "", script, chat.MCPConfig{})

	component, err := chat.New(chat.Profile(profile))
	if err != nil {
		panic(err)
	}
	defer component.Shutdown()
	// mux.Handle("/agent/", component); drive its routes over HTTP.
}

// TestMain makes this package's own test binary able to run as the scripted
// agent (needed by the example above and by any host that re-execs it), then
// runs the suite normally.
func TestMain(m *testing.M) {
	acptest.Main()
	os.Exit(m.Run())
}
