package main

import "testing"

// TestReadOnlyToolName covers the auto-allow policy's matcher
// (doc/features/acp-integration.md's "Permission requests" bullet):
// readOnlyToolName must recognize the workbench's own read-only MCP tools
// (query, sample_facts, list_predicates) under every title shape an ACP
// adapter has been observed to use, and reject everything else — including
// near-misses where a false positive would silently grant permission.
func TestReadOnlyToolName(t *testing.T) {
	cases := []struct {
		title string
		want  string
		ok    bool
	}{
		// -- shapes that must match ------------------------------------------
		{"query", "query", true},
		{"sample_facts", "sample_facts", true},
		{"list_predicates", "list_predicates", true},
		{"mcp__datalog__query", "query", true},
		{"datalog__query", "query", true},
		{"datalog__sample_facts", "sample_facts", true},
		{"datalog:query", "query", true},
		{"datalog - query (MCP)", "query", true},
		{"QUERY", "query", true},     // case normalized
		{"  query  ", "query", true}, // surrounding whitespace
		{"Datalog: List_Predicates", "list_predicates", true},

		// -- near-misses that must NOT match ---------------------------------
		{"set_rules", "", false},
		{"datalog__set_schema", "", false},
		{"queryX", "", false},
		{"Bash: psql query", "", false}, // contains "query" but isn't naming the tool
		{"", "", false},
		{"sample_input", "", false}, // mutating tool, not read-only
		{"set_rules (MCP)", "", false},
		{"mcp__datalog__set_rules", "", false},
		{"some other tool", "", false},
		{"query_history", "", false}, // trailing-token match only, not a prefix/substring one
		{"db-query", "", false},      // single-token dash-joined prefix, but not a known namespace shape
		{"psql-query", "", false},
		{"web-query", "", false},
		{"foo:query", "", false},
		{"foo__query", "", false},
	}

	for _, c := range cases {
		got, ok := readOnlyToolName(c.title)
		if ok != c.ok || got != c.want {
			t.Errorf("readOnlyToolName(%q) = (%q, %v), want (%q, %v)", c.title, got, ok, c.want, c.ok)
		}
	}
}

// TestAutoAllowOption covers the option-selection half of the auto-allow
// policy: prefer allow_once, fall back to any allow_* kind, and report
// ok=false when no allow option exists at all so the caller falls through
// to the normal buttons rather than guessing.
func TestAutoAllowOption(t *testing.T) {
	allowOnce := agentOption{ID: "a1", Kind: "allow_once"}
	allowAlways := agentOption{ID: "a2", Kind: "allow_always"}
	rejectOnce := agentOption{ID: "r1", Kind: "reject_once"}
	rejectAlways := agentOption{ID: "r2", Kind: "reject_always"}

	cases := []struct {
		name string
		opts []agentOption
		want agentOption
		ok   bool
	}{
		{"prefers allow_once even when allow_always also present",
			[]agentOption{rejectOnce, allowAlways, allowOnce}, allowOnce, true},
		{"falls back to allow_always when no allow_once",
			[]agentOption{rejectOnce, allowAlways, rejectAlways}, allowAlways, true},
		{"no allow option at all",
			[]agentOption{rejectOnce, rejectAlways}, agentOption{}, false},
		{"empty options",
			nil, agentOption{}, false},
	}

	for _, c := range cases {
		got, ok := autoAllowOption(c.opts)
		if ok != c.ok || got.ID != c.want.ID {
			t.Errorf("%s: autoAllowOption(...) = (%+v, %v), want (%+v, %v)", c.name, got, ok, c.want, c.ok)
		}
	}
}
