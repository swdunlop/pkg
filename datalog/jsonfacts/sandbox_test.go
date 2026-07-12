package jsonfacts_test

// These tests pin the security posture of the expr-lang sandbox that
// jsonfacts.Config.LoadDir/LoadFS compiles mapping and filter expressions
// into (see loader.go). Once configs can be submitted by an LLM over MCP
// (doc/features/mcp-server.md, design constraint 4, work item 3), the expr
// environment must expose nothing beyond:
//
//   - the `value` variable (the decoded JSON line)
//   - the loader's own functions: fresh_id, assert, match_contains,
//     match_starts_with, match_ends_with, match_regex
//   - expr-lang's builtin functions/operators (map, filter, len, type, ...)
//
// There is no filesystem, network, process, or reflection escape: the env
// passed to expr.Compile is always a fresh map[string]any{"value": ...} (see
// compileMappings and compileImperative in loader.go), and expr.Compile with
// expr.Env(env) resolves identifiers against that map at compile time, so an
// undefined name is a compile error, not a runtime one. There is no
// expr.AllowUndefinedVariables() anywhere in this package.

import (
	"strings"
	"testing"

	"swdunlop.dev/pkg/datalog/jsonfacts"
)

// undefinedIdentifierSchema builds a one-source schema whose only mapping
// uses the given expr snippet in the given mode ("args", "filter", or
// "expr" for imperative mode).
func schemaWithExpr(mode, exprSrc string) map[string]any {
	mapping := map[string]any{}
	switch mode {
	case "args":
		mapping["predicate"] = "p"
		mapping["args"] = []string{exprSrc}
	case "filter":
		mapping["predicate"] = "p"
		mapping["args"] = []string{"value.x"}
		mapping["filter"] = exprSrc
	case "expr":
		mapping["expr"] = exprSrc
	default:
		panic("unknown mode " + mode)
	}
	return map[string]any{
		"sources": []any{
			map[string]any{
				"file":     "data.jsonl",
				"mappings": []any{mapping},
			},
		},
	}
}

// TestSandboxRejectsUndefinedIdentifiers pins that any identifier outside
// {value, fresh_id, assert, match_contains, match_starts_with,
// match_ends_with, match_regex, expr-lang builtins} fails to compile, in all
// three mapping modes: simple args, simple filter, and imperative expr. This
// is the load-bearing guarantee for constraint 4: an untrusted config cannot
// reach anything beyond the declared env just by naming an identifier.
//
// Finding: expr.Compile for mapping/filter/expr programs happens lazily,
// inside loadSource (called from LoadDir/LoadFS), not during
// LoadSchemaDir/LoadSchemaFS's JSON parse + validate() pass. LoadSchemaDir
// happily accepts a schema referencing "os" or "exec" — validate() only
// checks mapping shape (expr xor predicate/args/filter), never compiles the
// expr source. The bad identifier is only caught once the data file is
// loaded via LoadDir/LoadFS. This is not a sandbox escape (the identifier
// still never resolves — Compile always errors before Run), but it means a
// caller that treats a successful LoadSchemaDir as "the config is valid"
// (as an MCP set_schema handler plausibly would, per mcp-server.md's
// description of set_schema validating and loading data) will not see the
// compile error until LoadDir runs against real data. The tests below pin
// this by calling LoadDir (which does trigger compilation) rather than
// asserting on LoadSchemaDir alone.
func TestSandboxRejectsUndefinedIdentifiers(t *testing.T) {
	identifiers := []string{
		`os`,
		`os.Getenv("HOME")`,
		`exec`,
		`exec.Command("ls")`,
		`open("/etc/passwd")`,
		`env`,
		`getenv("HOME")`,
		`syscall`,
		`import`,
		`file`,
		`ioutil`,
		`net`,
		`http`,
	}

	for _, mode := range []string{"args", "filter", "expr"} {
		for _, ident := range identifiers {
			t.Run(mode+"/"+ident, func(t *testing.T) {
				dir := t.TempDir()
				writeFile(t, dir, "data.jsonl", `{"x":"a"}`)
				writeSchema(t, dir, schemaWithExpr(mode, ident))

				var cfg jsonfacts.Config
				if err := cfg.LoadSchemaDir(dir); err != nil {
					t.Fatalf("LoadSchemaDir: unexpected error before compilation: %v", err)
				}

				_, err := cfg.LoadDir(dir)
				if err == nil {
					t.Fatalf("expected LoadDir compile error for identifier %q in %s mode, got nil", ident, mode)
				}
				if !strings.Contains(err.Error(), "unknown name") {
					t.Errorf("expected an %q compile error for %q, got: %v", "unknown name", ident, err)
				}
			})
		}
	}
}

// TestSandboxKnownGoodFunctionsCompile pins that the documented loader
// function set (fresh_id, assert, and each match_* helper) still compiles
// and loads successfully, so the rejection test above isn't accidentally
// passing because the sandbox rejects everything.
func TestSandboxKnownGoodFunctionsCompile(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "data.jsonl", `{"id":"a1","cmd":"certutil -urlcache"}
{"id":"a2","cmd":"notepad.exe"}
`)
	writeSchema(t, dir, map[string]any{
		"sources": []any{
			map[string]any{
				"file": "data.jsonl",
				"mappings": []any{
					map[string]any{
						"expr": `let id = fresh_id();
assert("proc", [id, value.cmd]);
match_contains("cmd_contains", id, value.cmd, ["certutil", "bitsadmin"])`,
					},
				},
			},
		},
	})

	var cfg jsonfacts.Config
	if err := cfg.LoadSchemaDir(dir); err != nil {
		t.Fatalf("expected known-good function set to compile, got: %v", err)
	}

	db, err := cfg.LoadDir(dir)
	if err != nil {
		t.Fatalf("expected known-good function set to load, got: %v", err)
	}

	procCount := 0
	for range db.Facts("proc", 2) {
		procCount++
	}
	if procCount != 2 {
		t.Errorf("expected 2 proc facts, got %d", procCount)
	}

	matchCount := 0
	for range db.Facts("cmd_contains", 2) {
		matchCount++
	}
	if matchCount != 1 {
		t.Errorf("expected 1 cmd_contains fact, got %d", matchCount)
	}
}
