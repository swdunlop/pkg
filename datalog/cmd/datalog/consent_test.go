package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"swdunlop.dev/pkg/datalog/syntax"
)

// newRulesDirWorkbench builds a workbench over the mordor example with its
// ruleset imported into a rules/ directory store — the shape rule-group
// CRUD requires (h.rules non-nil).
func newRulesDirWorkbench(t *testing.T) *workbench {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "examples", "mordor", "rules.dl"))
	if err != nil {
		t.Fatalf("reading mordor rules.dl: %v", err)
	}
	rs, err := syntax.ParseAll(string(data))
	if err != nil {
		t.Fatalf("parsing mordor rules.dl: %v", err)
	}
	rulesDir := filepath.Join(t.TempDir(), "rules")
	if _, err := importRuleset(rs, rulesDir); err != nil {
		t.Fatalf("importRuleset: %v", err)
	}
	zipPath := filepath.Join("..", "..", "examples", "mordor", "covenant_copy_smb.zip")
	schemaPath := filepath.Join("..", "..", "examples", "mordor", "mordor.yaml")
	return newTestWorkbenchRulesDir(t, zipPath, schemaPath, rulesDir, "test-token")
}

// pendingConsentID polls until exactly one pending permission is tracked
// and returns its requestID.
func pendingConsentID(t *testing.T, wb *workbench) string {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		wb.permMu.Lock()
		if len(wb.pendingPerm) == 1 {
			for id := range wb.pendingPerm {
				wb.permMu.Unlock()
				return id
			}
		}
		wb.permMu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("no pending consent card appeared")
	return ""
}

// TestConsentedRuleGroupWrites pins design decision 5's consent rule at
// the mechanism (consent.go's consented* methods): adds apply immediately
// with no card, edits/deletes of an existing group block on the diff card,
// Deny returns the "denied by user" tool result without writing, Approve
// lands the write, and a cancelled turn context abandons the card.
func TestConsentedRuleGroupWrites(t *testing.T) {
	wb := newRulesDirWorkbench(t)
	gate := newConsentGate(wb, "conv1")
	driver := &kitDriver{consent: gate} // Answer's dispatch path, as the /answer handler uses it
	ctx := context.Background()

	// 1. ADD: a new group applies immediately — no card, no block.
	out, err := wb.h.consentedPutRuleGroup(ctx, gate, putRuleGroupInput{
		Head: "consent_probe", Arity: 1, Text: `consent_probe("a").`, Revision: 0,
	})
	if err != nil {
		t.Fatalf("adding a new group must not be gated: %v", err)
	}
	if out.IsStale {
		t.Fatalf("unexpected stale on add: %+v", out)
	}
	if log := renderLog(wb, "conv1"); strings.Contains(log, "waiting for approval") {
		t.Fatalf("add rendered a consent card: %s", log)
	}

	// 2. EDIT + DENY: card appears, Deny returns denied-by-user, text unchanged.
	type result struct {
		out putRuleGroupOutput
		err error
	}
	resCh := make(chan result, 1)
	go func() {
		o, e := wb.h.consentedPutRuleGroup(ctx, gate, putRuleGroupInput{
			Head: "consent_probe", Arity: 1, Text: `consent_probe("b").`, Revision: out.Revision,
		})
		resCh <- result{o, e}
	}()
	reqID := pendingConsentID(t, wb)
	log := renderLog(wb, "conv1")
	if !strings.Contains(log, "agent is waiting for approval: edit rule group consent_probe/1") {
		t.Fatalf("consent card missing or mislabelled: %s", log)
	}
	if !strings.Contains(log, `- consent_probe(&quot;a&quot;)`) && !strings.Contains(log, `- consent_probe("a")`) {
		t.Fatalf("diff card missing the removed line: %s", log)
	}
	// Deny through the driver's Answer — the exact path handleAnswer takes.
	if err := driver.Answer(reqID, "deny"); err != nil {
		t.Fatalf("Answer(deny): %v", err)
	}
	res := <-resCh
	if !errors.Is(res.err, errConsentDenied) {
		t.Fatalf("denied edit returned %v, want errConsentDenied", res.err)
	}
	cur, err := func() (getRuleGroupOutput, error) {
		wb.h.mu.Lock()
		defer wb.h.mu.Unlock()
		return wb.h.getRuleGroup(getRuleGroupInput{Head: "consent_probe", Arity: 1})
	}()
	if err != nil || !strings.Contains(cur.Text, `"a"`) {
		t.Fatalf("denied edit still changed the group: %q, %v", cur.Text, err)
	}

	// 3. EDIT + APPROVE: the write lands.
	go func() {
		o, e := wb.h.consentedPutRuleGroup(ctx, gate, putRuleGroupInput{
			Head: "consent_probe", Arity: 1, Text: `consent_probe("b").`, Revision: cur.Revision,
		})
		resCh <- result{o, e}
	}()
	reqID = pendingConsentID(t, wb)
	if err := driver.Answer(reqID, "approve"); err != nil {
		t.Fatalf("Answer(approve): %v", err)
	}
	res = <-resCh
	if res.err != nil || res.out.IsStale {
		t.Fatalf("approved edit failed: %+v, %v", res.out, res.err)
	}

	// 4. DELETE + APPROVE: gated, then lands.
	delCh := make(chan error, 1)
	go func() {
		_, e := wb.h.consentedDeleteRuleGroup(ctx, gate, deleteRuleGroupInput{
			Head: "consent_probe", Arity: 1, Revision: res.out.Revision,
		})
		delCh <- e
	}()
	reqID = pendingConsentID(t, wb)
	if !strings.Contains(renderLog(wb, "conv1"), "delete rule group consent_probe/1") {
		t.Fatalf("delete card missing: %s", renderLog(wb, "conv1"))
	}
	if err := driver.Answer(reqID, "approve"); err != nil {
		t.Fatalf("Answer(approve delete): %v", err)
	}
	if err := <-delCh; err != nil {
		t.Fatalf("approved delete failed: %v", err)
	}

	// 5. CANCELLED TURN: the card is abandoned with the context's error.
	cancelCtx, cancel := context.WithCancel(context.Background())
	go func() {
		_, e := wb.h.consentedPutRuleGroup(cancelCtx, gate, putRuleGroupInput{
			Head: "lateral_movement", Arity: 4, Text: `lateral_movement("x","y","z","w").`, Revision: 1,
		})
		delCh <- e
	}()
	pendingConsentID(t, wb)
	cancel()
	if err := <-delCh; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled consent returned %v, want context.Canceled", err)
	}

	// A denied/cancelled card must not leak driver Answer state: an unknown
	// request errors rather than resolving anything.
	if err := driver.Answer("consent-999", "approve"); err == nil {
		t.Fatal("Answer on an unknown requestID must error")
	}
}

// TestConsentedSchemaDeleteDenied covers the schema half's gate shape with
// the deny path: deleting an existing source renders a card, Deny returns
// denied-by-user, and the source survives.
func TestConsentedSchemaDeleteDenied(t *testing.T) {
	wb := newMordorWorkbench(t)
	gate := newConsentGate(wb, "conv1")

	wb.h.mu.Lock()
	if len(wb.h.sess.authoringCfg.Sources) == 0 {
		wb.h.mu.Unlock()
		t.Fatal("mordor schema has no sources to probe")
	}
	file := wb.h.sess.authoringCfg.Sources[0].File
	nSources := len(wb.h.sess.authoringCfg.Sources)
	wb.h.mu.Unlock()

	errCh := make(chan error, 1)
	go func() {
		_, e := wb.h.consentedDeleteSource(context.Background(), gate, deleteSourceInput{File: file, Revision: 1})
		errCh <- e
	}()
	reqID := pendingConsentID(t, wb)
	if !strings.Contains(renderLog(wb, "conv1"), "delete source "+file) {
		t.Fatalf("delete-source card missing: %s", renderLog(wb, "conv1"))
	}
	if !gate.Resolve(reqID, "deny") {
		t.Fatal("gate did not recognize its own requestID")
	}
	if err := <-errCh; !errors.Is(err, errConsentDenied) {
		t.Fatalf("denied delete returned %v, want errConsentDenied", err)
	}

	wb.h.mu.Lock()
	defer wb.h.mu.Unlock()
	if len(wb.h.sess.authoringCfg.Sources) != nSources {
		t.Fatal("denied delete still removed the source")
	}
}

// TestDiffLines pins the consent card's line diff: context lines pass
// through, removals and additions are marked, and a pure delete renders
// all-minus.
func TestDiffLines(t *testing.T) {
	diff := diffLines("a\nb\nc\n", "a\nx\nc\n")
	var got []string
	for _, ln := range diff {
		got = append(got, ln.Kind+ln.Text)
	}
	want := []string{" a", "-b", "+x", " c"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("diff = %v, want %v", got, want)
	}

	del := diffLines("a\nb\n", "")
	if len(del) != 2 || del[0].Kind != "-" || del[1].Kind != "-" {
		t.Fatalf("pure delete diff = %v", del)
	}
}
