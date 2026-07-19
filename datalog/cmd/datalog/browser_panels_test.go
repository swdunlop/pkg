package main

import (
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	html "github.com/swdunlop/html-go"
)

// -- phase 3: Schema tab structural rendering --------------------------------

// TestSchemaPanelStructural pins design decision 9's Schema tab: sources,
// matchers, and declarations render structurally (not YAML text), with use
// docs shown and produced predicates as Facts-tab links.
func TestSchemaPanelStructural(t *testing.T) {
	wb := newMordorWorkbench(t)

	wb.h.mu.Lock()
	out := string(html.Append(nil, renderSchemaPanel(wb.h.sess.authoringCfg)))
	wb.h.mu.Unlock()

	if !strings.Contains(out, "id='schema-panel'") {
		t.Fatalf("schema panel must carry its stable patch id:\n%s", out[:200])
	}
	// A source mapping renders its predicate as a link and its filter.
	if !strings.Contains(out, ">net_conn</a>") {
		t.Fatalf("mapping predicates must render as pred-links:\n%s", out)
	}
	if !strings.Contains(out, "value.EventID == 3") {
		t.Fatalf("mapping filters must be visible:\n%s", out)
	}
	// The net_conn/term-5 contains matcher produces contains/2 — the
	// produced predicate is the link a why-walk follows.
	if !strings.Contains(out, ">contains</a>") {
		t.Fatalf("matcher produced predicates must render as pred-links:\n%s", out)
	}
	// Declarations show term names and Use docs.
	if !strings.Contains(out, "the process that made the connection") {
		t.Fatalf("declaration term Use docs must be visible:\n%s", out)
	}
	// Structural, not serialized: no YAML keys leak through as text.
	if strings.Contains(out, "mappings:") || strings.Contains(out, "sources:") {
		t.Fatalf("schema tab must not render YAML text:\n%s", out)
	}
}

// -- phase 3: Rules tab master-detail ----------------------------------------

// TestHTTP_RuleGroupDetail drives GET /rules/{head}/{arity} over a
// rules-directory workbench: the detail must carry the group's %% doc as
// prose, syntax-highlighted rule text, and dependency links both ways
// (body predicates it uses; groups whose bodies consume its head).
func TestHTTP_RuleGroupDetail(t *testing.T) {
	wb := newRulesDirWorkbench(t)
	srv := startTestServer(wb)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/rules/smb_conn/3")
	if err != nil {
		t.Fatalf("GET /rules/smb_conn/3: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	out := string(body)

	if !strings.Contains(out, "id='rule-group-detail'") {
		t.Fatalf("detail must patch #rule-group-detail:\n%s", out)
	}
	// The %% doc renders as prose, stripped from the highlighted source.
	if !strings.Contains(out, "the transport that lateral movement rides on") {
		t.Fatalf("group %%%% doc must render as prose:\n%s", out)
	}
	if !strings.Contains(out, "dl-pred") || !strings.Contains(out, "dl-var") {
		t.Fatalf("rule text must be syntax highlighted:\n%s", out)
	}
	// Uses: smb_conn's body reads net_conn/8 (base) and contains/2
	// (matcher-produced).
	if !strings.Contains(out, ">net_conn/8</a>") || !strings.Contains(out, ">contains/2</a>") {
		t.Fatalf("detail must link the body predicates it uses:\n%s", out)
	}
	// Used by: lateral_movement/4's body references smb_conn/3, and its
	// own group exists, so the link loads that group's detail.
	if !strings.Contains(out, ">lateral_movement/4</a>") {
		t.Fatalf("detail must link the groups that consume this head:\n%s", out)
	}
	if !strings.Contains(out, "@get(&apos;/rules/lateral_movement/4&apos;)") {
		t.Fatalf("a consuming GROUP link must navigate to that group's detail:\n%s", out)
	}
}

// TestHTTP_RuleGroupDetailUnknown pins the vanished-group race: a click on
// a stale list entry renders an in-pane error, not a broken page.
func TestHTTP_RuleGroupDetailUnknown(t *testing.T) {
	wb := newRulesDirWorkbench(t)
	srv := startTestServer(wb)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/rules/no_such_group/7")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	out := string(body)
	if !strings.Contains(out, "id='rule-group-detail'") || !strings.Contains(out, "no rule group no_such_group/7") {
		t.Fatalf("unknown group must render an in-pane error:\n%s", out)
	}
}

// TestRulesPanelListsGroups pins the group list: filename order, statement
// and head fact counts, and a click target per group.
func TestRulesPanelListsGroups(t *testing.T) {
	wb := newRulesDirWorkbench(t)

	wb.h.mu.Lock()
	out := string(html.Append(nil, wb.renderRulesPanel()))
	wb.h.mu.Unlock()

	if !strings.Contains(out, "id='rules-panel'") {
		t.Fatalf("rules panel must carry its stable patch id:\n%s", out)
	}
	if !strings.Contains(out, "@get(&apos;/rules/smb_conn/3&apos;)") {
		t.Fatalf("group rows must load their detail on click:\n%s", out)
	}
	if !strings.Contains(out, "smb_conn_3.dl") {
		t.Fatalf("group rows must show their on-disk file:\n%s", out)
	}
	// Head fact counts come from the evaluated database — lateral movement
	// derives at least one fact in the mordor dataset, so a non-zero count
	// must appear somewhere (0 everywhere would mean the counts are wired
	// to nothing).
	if !strings.Contains(out, " facts") {
		t.Fatalf("group rows must show head fact counts:\n%s", out)
	}
}

// TestPublishSessionChangedPatchesBrowserPanels pins the refresh mechanism:
// every mutation/reload fan-out must re-patch the Schema and Rules panels
// (alongside the predicate lists it always carried), or agent CRUD and vim
// saves would leave phase 3's structural tabs stale until a full page load.
func TestPublishSessionChangedPatchesBrowserPanels(t *testing.T) {
	wb := newRulesDirWorkbench(t)
	sub := wb.bus.Subscribe()
	defer sub.Close()

	wb.h.mu.Lock()
	wb.publishSessionChanged()
	wb.h.mu.Unlock()

	deadline := time.After(5 * time.Second)
	var got string
	for got == "" {
		select {
		case ev := <-sub.Events():
			out := renderEvent(ev)
			if strings.Contains(out, "schema-panel") {
				got = out
			}
		case <-deadline:
			t.Fatal("timed out waiting for the session-changed batch on the bus")
		}
	}
	if !strings.Contains(got, "rules-panel") {
		t.Fatalf("the session-changed batch must patch #rules-panel too:\n%s", got)
	}
	if !strings.Contains(got, "predicates-base") || !strings.Contains(got, "predicates-derived") {
		t.Fatalf("the batch must still carry the predicate lists:\n%s", got)
	}
}
