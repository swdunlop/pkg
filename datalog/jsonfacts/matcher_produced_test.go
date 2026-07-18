package jsonfacts

import (
	"encoding/base64"
	"reflect"
	"testing"

	"swdunlop.dev/pkg/datalog"
)

// TestProducedPredicates pins Matcher.ProducedPredicates as the single
// source of truth for matcher output naming, including the per-kind
// quirks: contains/starts_with take both ci_ and wd_ prefixes, ends_with
// and regex_match never take wd_, and base64/base64_utf16le/cidr use
// fixed names with no prefixes at all. cmd/datalog's predicate_deps and
// explain_fact depend on these names matching what applyMatchers emits —
// see TestProducedPredicatesMatchEmittedFacts for that end-to-end pin.
func TestProducedPredicates(t *testing.T) {
	cases := []struct {
		name string
		m    Matcher
		want []string
	}{
		{
			name: "contains plain",
			m:    Matcher{Predicate: "event", Term: 2, Contains: []string{"x"}},
			want: []string{"contains"},
		},
		{
			name: "contains ci+windash",
			m:    Matcher{Predicate: "event", Term: 2, CaseInsensitive: true, Windash: true, Contains: []string{"x"}},
			want: []string{"ci_wd_contains"},
		},
		{
			name: "ends_with and regex_match never take windash",
			m:    Matcher{Predicate: "event", Term: 0, Windash: true, EndsWith: []string{"x"}, RegexMatch: []string{"y"}},
			want: []string{"ends_with", "regex_match"},
		},
		{
			name: "base64 and cidr are fixed names, no prefixes",
			m: Matcher{
				Predicate: "event", Term: 0, CaseInsensitive: true, Windash: true,
				Base64: []string{"x"}, Base64UTF16: []string{"y"}, CIDR: []string{"10.0.0.0/8"},
			},
			want: []string{"base64_contains", "base64_utf16le_contains", "cidr_match"},
		},
		{
			name: "multi-kind matcher produces one predicate per kind",
			m: Matcher{
				Predicate: "event", Term: 1, CaseInsensitive: true,
				Contains: []string{"a"}, StartsWith: []string{"b"},
			},
			want: []string{"ci_contains", "ci_starts_with"},
		},
		{
			name: "unresolved _from fields count as populated",
			m:    Matcher{Predicate: "event", Term: 0, ContainsFrom: "patterns.txt"},
			want: []string{"contains"},
		},
		{
			name: "no kinds, no products",
			m:    Matcher{Predicate: "event", Term: 0},
			want: nil,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.m.ProducedPredicates(); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("ProducedPredicates() = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestProducedPredicatesMatchEmittedFacts is the anti-drift pin: every
// predicate applyMatchers actually emits must appear in the matcher's
// ProducedPredicates. If the emit-time naming ever changes without the
// static helper following, this fails.
func TestProducedPredicatesMatchEmittedFacts(t *testing.T) {
	m := Matcher{
		Predicate: "event", Term: 0, CaseInsensitive: true, Windash: true,
		Contains:   []string{"-flag"},
		StartsWith: []string{"cmd"},
		EndsWith:   []string{".exe"},
		RegexMatch: []string{`\d+`},
		Base64:     []string{"payload"},
		CIDR:       []string{"10.0.0.0/8"},
	}
	strFact := func(name, s string) datalog.Fact {
		return datalog.Fact{Name: name, Terms: []datalog.Constant{datalog.String(s)}}
	}
	facts := []datalog.Fact{
		strFact("event", "CMD /flag payload9 "+base64.StdEncoding.EncodeToString([]byte("payload"))+" TOOL.EXE"),
		strFact("event", "10.1.2.3"),
	}
	out, err := applyMatchers(facts, []Matcher{m}, nil)
	if err != nil {
		t.Fatalf("applyMatchers: %v", err)
	}
	if len(out) == 0 {
		t.Fatal("applyMatchers emitted nothing; fixture no longer exercises every kind")
	}
	produced := map[string]bool{}
	for _, p := range m.ProducedPredicates() {
		produced[p] = true
	}
	seen := map[string]bool{}
	for _, f := range out {
		seen[f.Name] = true
		if !produced[f.Name] {
			t.Errorf("applyMatchers emitted %q, which ProducedPredicates() does not report (%v)", f.Name, m.ProducedPredicates())
		}
		if len(f.Terms) != 2 {
			t.Errorf("applyMatchers emitted %q with arity %d, want 2", f.Name, len(f.Terms))
		}
	}
	// Both directions: the fixture is built so every kind fires, so every
	// statically reported predicate must also have been emitted.
	for p := range produced {
		if !seen[p] {
			t.Errorf("ProducedPredicates() reports %q but the fixture never emitted it — fixture no longer exercises that kind", p)
		}
	}
}
