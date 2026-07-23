package acptest

import (
	"os"
	"testing"

	chat "swdunlop.dev/pkg/datastar-acp"
)

// Command builds the (command, args) a chat.AgentProfile needs to re-exec the
// running test binary into Main as the scripted agent — the reusable form of
// datalog's fakeACPAgentCommand.  The returned command is os.Executable(),
// pinned with -test.run to a helper test that only calls Main, and -test.v=false
// so the test framework's own output never pollutes the JSON-RPC stdio channel.
//
// helperTest names a Test function in the host's package whose whole body is
// acptest.Main() — that is the process's entry point when it is re-exec'd; under
// a normal run its Main is a no-op because the activate marker is unset.  Most
// hosts pass "" and instead call Main() from TestMain (see Example), in which
// case no -test.run pin is needed because TestMain runs before any test.
func Command(t *testing.T, helperTest string) (string, []string) {
	t.Helper()
	self, err := os.Executable()
	if err != nil {
		t.Fatalf("acptest: os.Executable: %v", err)
	}
	args := []string{"-test.v=false"}
	if helperTest != "" {
		args = append(args, "-test.run=^"+helperTest+"$")
	}
	return self, args
}

// Env builds the environment entries a chat.AgentProfile must carry so a
// re-exec'd child runs script as the scripted agent: the activate marker plus
// the JSON-encoded script.  These append to the child's inherited environment
// (chat spawns with the parent environment plus the profile's Env), so the
// child sees both.  A bad script fails the test here rather than in the child.
func Env(t *testing.T, script Script) []string {
	t.Helper()
	encoded, err := script.Encode()
	if err != nil {
		t.Fatalf("acptest: encoding script: %v", err)
	}
	return []string{activateEnv + "=1", ScriptEnv + "=" + encoded}
}

// Profile builds a chat.AgentProfile named name that spawns the running test
// binary as the scripted agent replaying script, with mcp attached.  It is the
// one call a host needs to dogfood a scenario through the real chat HTTP surface:
// pass the result to chat.Profile.  helperTest is the -test.run pin (see
// Command); pass "" when Main runs from TestMain.
func Profile(t *testing.T, name, helperTest string, script Script, mcp chat.MCPConfig) chat.AgentProfile {
	t.Helper()
	cmd, args := Command(t, helperTest)
	return chat.AgentProfile{
		Name:    name,
		Command: cmd,
		Args:    args,
		Env:     Env(t, script),
		MCP:     mcp,
	}
}
