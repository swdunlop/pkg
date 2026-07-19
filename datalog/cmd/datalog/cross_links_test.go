package main

import (
	"net/http"
	"net/url"
	"strings"
	"testing"

	html "github.com/swdunlop/html-go"
)

// -- phase 4: cross-links + provenance polish --------------------------------

// TestSchemaPanelDeepLinks pins phase 4's dependency navigation in the
// Schema tab: predicate links don't just flip to the Facts tab, they load
// the predicate's facts (via its /facts endpoint) and scroll to them, with
// the matcher's read arity resolved from the config's own mappings.
func TestSchemaPanelDeepLinks(t *testing.T) {
	wb := newMordorWorkbench(t)

	wb.h.mu.Lock()
	out := string(html.Append(nil, renderSchemaPanel(wb.h.sess.authoringCfg)))
	wb.h.mu.Unlock()

	// A mapping's predicate deep-links with the mapping's own arity.
	if !strings.Contains(out, "@get(&apos;/facts/net_conn/8&apos;)") {
		t.Fatalf("mapping predicate must deep-link its facts:\n%s", out)
	}
	if !strings.Contains(out, "scrollIntoView") {
		t.Fatalf("facts deep links must scroll the target into view:\n%s", out)
	}
	// A matcher's produced predicate deep-links at arity 2.
	if !strings.Contains(out, "@get(&apos;/facts/contains/2&apos;)") {
		t.Fatalf("matcher produced predicates must deep-link their facts:\n%s", out)
	}
}

// TestRuleGroupDetailDeepLinks pins the Rules tab's dependency navigation:
// a base body predicate deep-links into the Facts tab (not just a tab
// flip), and a consuming group's link flips to the Rules tab before
// loading its detail — required now that these links also render outside
// the Rules tab (the why? tree, transcript entries).
func TestRuleGroupDetailDeepLinks(t *testing.T) {
	wb := newRulesDirWorkbench(t)
	srv := startTestServer(wb)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/rules/smb_conn/3")
	if err != nil {
		t.Fatalf("GET /rules/smb_conn/3: %v", err)
	}
	out := readAll(t, resp)

	if !strings.Contains(out, "@get(&apos;/facts/net_conn/8&apos;)") {
		t.Fatalf("base body predicates must deep-link their facts:\n%s", out)
	}
	if !strings.Contains(out, "$_browserTab = &apos;rules&apos;; @get(&apos;/rules/lateral_movement/4&apos;)") {
		t.Fatalf("group links must flip to the Rules tab before loading the detail:\n%s", out)
	}
}

// TestHandleWhy_StructuralTree drives the phase-4 why? expansion end to
// end over a rules-directory workbench: POST /why must publish a
// structural derivation tree — nested why-node <details>, the deriving
// rule highlighted with a link to its rule group, and base facts as
// leaves that deep-link their predicate's facts.
func TestHandleWhy_StructuralTree(t *testing.T) {
	wb := newRulesDirWorkbench(t)
	srv := startTestServer(wb)
	defer srv.Close()

	// Find a real smb_conn/3 fact in the evaluation to explain.
	wb.h.mu.Lock()
	db, dbErr := wb.h.sess.evaluatedDB()
	wb.h.mu.Unlock()
	if dbErr != nil {
		t.Fatalf("evaluatedDB: %v", dbErr)
	}
	var lit string
	for row := range db.Facts("smb_conn", 3) {
		if l, ok := factLiteral("smb_conn", row); ok {
			lit = l
			break
		}
	}
	if lit == "" {
		t.Fatal("no round-trippable smb_conn/3 fact in the mordor evaluation")
	}

	sub := wb.bus.Subscribe()
	defer sub.Close()

	req, err := http.NewRequest(http.MethodPost, srv.URL+"/why/smb_conn/3?fact="+url.QueryEscape(lit), nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Accept", "text/event-stream")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /why: %v", err)
	}
	readAll(t, resp)

	out := collectWhyOutput(t, sub)
	if !strings.Contains(out, "why-node") || !strings.Contains(out, "why-tree") {
		t.Fatalf("why? must render a structural tree, not <pre> text:\n%s", out)
	}
	// The deriving rule renders highlighted, with its group linked.
	if !strings.Contains(out, "dl-pred") {
		t.Fatalf("the deriving rule must be syntax highlighted:\n%s", out)
	}
	if !strings.Contains(out, ">open rule group</a>") ||
		!strings.Contains(out, "@get(&apos;/rules/smb_conn/3&apos;)") {
		t.Fatalf("a derived step must link its rule group:\n%s", out)
	}
	// Base facts render as leaves with a badge and a Facts-tab deep link.
	if !strings.Contains(out, ">base</span>") {
		t.Fatalf("base facts must carry the base badge:\n%s", out)
	}
	if !strings.Contains(out, "@get(&apos;/facts/net_conn/8&apos;)") {
		t.Fatalf("base facts must deep-link their predicate:\n%s", out)
	}
}

// TestQueryPredicateLinks pins the transcript query cross-links: linkable
// body predicates become Facts-tab deep links, while comparison and
// is-atoms — whose "predicates" are not identifiers and must never be
// spliced into a click expression — render nothing.
func TestQueryPredicateLinks(t *testing.T) {
	out := string(html.Append(nil, queryPredicateLinks(`smb_conn(S, D, U), S != "x"?`)))
	if !strings.Contains(out, "@get(&apos;/facts/smb_conn/3&apos;)") {
		t.Fatalf("query predicates must deep-link:\n%s", out)
	}
	if strings.Contains(out, "!=") {
		t.Fatalf("comparison atoms must not render links:\n%s", out)
	}

	if out := string(html.Append(nil, queryPredicateLinks("not a query"))); out != "" {
		t.Fatalf("unparseable text must render nothing:\n%s", out)
	}
}

// TestToolBrowserLink pins the transcript tool-entry cross-links: rule
// group CRUD links its Rules-tab detail, schema CRUD flips to the Schema
// tab, explain links the explained fact's predicate, and hostile or
// malformed arguments render nothing at all.
func TestToolBrowserLink(t *testing.T) {
	out := string(html.Append(nil, toolBrowserLink("put_rule_group",
		`{"head":"smb_conn","arity":3,"text":"..."}`)))
	if !strings.Contains(out, "$_browserTab = &apos;rules&apos;; @get(&apos;/rules/smb_conn/3&apos;)") {
		t.Fatalf("put_rule_group must link its group detail:\n%s", out)
	}
	if !strings.Contains(out, ">smb_conn/3</a>") {
		t.Fatalf("the rule group link must be labeled head/arity:\n%s", out)
	}

	out = string(html.Append(nil, toolBrowserLink("put_matcher", `{"predicate":"proc"}`)))
	if !strings.Contains(out, "$_browserTab = &apos;schema&apos;") {
		t.Fatalf("schema CRUD must flip to the Schema tab:\n%s", out)
	}

	out = string(html.Append(nil, toolBrowserLink("explain", `{"fact":"smb_conn(\"a\", \"b\", \"c\")"}`)))
	if !strings.Contains(out, "@get(&apos;/facts/smb_conn/3&apos;)") {
		t.Fatalf("explain must link the explained fact's predicate:\n%s", out)
	}

	// A head that is not an identifier must never reach a click expression.
	if out := string(html.Append(nil, toolBrowserLink("put_rule_group",
		`{"head":"x'); alert(1); ('","arity":1}`))); out != "" {
		t.Fatalf("a non-identifier head must render nothing:\n%s", out)
	}
	if out := string(html.Append(nil, toolBrowserLink("query", `{"query":"x?"}`))); out != "" {
		t.Fatalf("tools without a browser referent must render nothing:\n%s", out)
	}
}

// readAll drains and closes an HTTP response body, returning it as text.
func readAll(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer resp.Body.Close()
	var b strings.Builder
	buf := make([]byte, 4096)
	for {
		n, err := resp.Body.Read(buf)
		b.Write(buf[:n])
		if err != nil {
			return b.String()
		}
	}
}
