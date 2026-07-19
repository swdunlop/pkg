package main

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"

	"gopkg.in/yaml.v3"
	"swdunlop.dev/pkg/datalog/cmd/datalog/view"
	"swdunlop.dev/pkg/datalog/jsonfacts"
)

// This file implements phase 2c of doc/features/workbench-v2.md (design
// decision 5's consent rule): for a KIT conversation, editing or deleting
// an EXISTING rule group or schema item renders a diff card in the
// transcript with Approve/Deny, and the tool call blocks until the human
// answers. Adding something new applies immediately (additive, visible,
// revertable). The gate exists because kit has no permission flow of its
// own (agentDriver's doc comment): an ACP agent's writes ride ACP's native
// request_permission instead, and the /mcp mount and stdio server register
// with a nil gate — an external agent there is the operator's own trusted
// tool, holding the bearer token the operator handed it.
//
// The gate rides the SAME pendingPerm/handleAnswer machinery the ACP
// permission flow uses: the diff card is a "permission"-kind transcript
// entry whose buttons post /answer?requestID=..., handleAnswer resolves
// through kitDriver.Answer (which recognizes the gate's requestIDs), and
// runAgentTurn's turn-end cleanup morphs an unanswered card to cancelled
// exactly like any other pending permission.

// errConsentDenied is the tool result a denied write returns (design
// decision 5: "Deny returns a 'denied by user' tool result and the turn
// continues").
var errConsentDenied = errors.New("denied by user")

// consentGate serializes consent requests for one conversation's kit
// agent. It is constructed per conversation (conversationDriver) beside
// the kit instance whose tool registration it gates.
type consentGate struct {
	wb     *workbench
	convID string

	mu      sync.Mutex
	seq     uint64
	pending map[string]chan bool
}

func newConsentGate(wb *workbench, convID string) *consentGate {
	return &consentGate{wb: wb, convID: convID, pending: map[string]chan bool{}}
}

// Request renders a diff card for action (e.g. "edit rule group risky/2")
// into the conversation's transcript and blocks until the human approves
// (nil), denies (errConsentDenied), or the turn's context ends (ctx.Err()).
// oldText/newText are the current and proposed content; newText empty
// renders a deletion card.
func (g *consentGate) Request(ctx context.Context, action, oldText, newText string) error {
	g.mu.Lock()
	g.seq++
	requestID := "consent-" + strconv.FormatUint(g.seq, 10)
	ch := make(chan bool, 1)
	g.pending[requestID] = ch
	g.mu.Unlock()
	defer func() {
		g.mu.Lock()
		delete(g.pending, requestID)
		g.mu.Unlock()
		// Drop the workbench's pending entry too: handleAnswer already
		// deletes it on the click path, but a request resolved another way
		// (turn context cancelled, a driver Answer that bypassed the HTTP
		// handler) must not leave a stale requestID for the turn-end sweep
		// to misreport as "cancelled before the agent received an answer."
		g.wb.permMu.Lock()
		delete(g.wb.pendingPerm, requestID)
		g.wb.permMu.Unlock()
	}()

	// The card is a "permission" entry so handleAnswer's morph-to-resolved
	// and runAgentTurn's cancel cleanup both apply unchanged. The event's
	// ToolName carries the human-readable action for permissionSummary.
	ev := agentEvent{
		Kind:      "permission",
		RequestID: requestID,
		ToolName:  action,
		Options: []agentOption{
			{ID: "approve", Name: "Approve", Kind: "allow_once"},
			{ID: "deny", Name: "Deny", Kind: "reject_once"},
		},
	}
	entryID := g.wb.consoleAppend(g.convID, "permission",
		view.ConsentCard(action, diffLines(oldText, newText), consentButtons(ev, g.convID)))
	g.wb.permMu.Lock()
	g.wb.pendingPerm[ev.RequestID] = pendingPermission{entryID: entryID, event: ev, tab: g.convID}
	g.wb.permMu.Unlock()

	select {
	case approved := <-ch:
		if !approved {
			return errConsentDenied
		}
		return nil
	case <-ctx.Done():
		// runAgentTurn's turn-end cleanup (or handleAnswer racing it) owns
		// the entry morph; this side just stops waiting.
		return ctx.Err()
	}
}

// Resolve delivers the human's answer for requestID, reporting whether the
// id belongs to this gate — kitDriver.Answer's dispatch test.
func (g *consentGate) Resolve(requestID, optionID string) bool {
	g.mu.Lock()
	ch, ok := g.pending[requestID]
	if ok {
		delete(g.pending, requestID)
	}
	g.mu.Unlock()
	if !ok {
		return false
	}
	ch <- (optionID == "approve")
	return true
}

// consentButtons renders the card's Approve/Deny row — permissionEntry's
// button idiom, reused so the /answer wiring and styling stay identical.
func consentButtons(ev agentEvent, tab string) []view.ConsentOption {
	out := make([]view.ConsentOption, len(ev.Options))
	for i, opt := range ev.Options {
		out[i] = view.ConsentOption{
			RequestID: ev.RequestID,
			OptionID:  opt.ID,
			Label:     opt.Name,
			Reject:    opt.Kind == "reject_once",
			Tab:       tab,
		}
	}
	return out
}

// diffLines computes a line-level unified-ish diff (context, -, +) between
// old and new via a longest-common-subsequence walk — rule groups and
// schema items are small, so the O(n·m) table is nothing. It exists so the
// consent card shows the CHANGE, not two blobs the human must eyeball.
func diffLines(oldText, newText string) []view.DiffLine {
	a := splitLines(oldText)
	b := splitLines(newText)

	// LCS table.
	m, n := len(a), len(b)
	lcs := make([][]int, m+1)
	for i := range lcs {
		lcs[i] = make([]int, n+1)
	}
	for i := m - 1; i >= 0; i-- {
		for j := n - 1; j >= 0; j-- {
			if a[i] == b[j] {
				lcs[i][j] = lcs[i+1][j+1] + 1
			} else {
				lcs[i][j] = max(lcs[i+1][j], lcs[i][j+1])
			}
		}
	}

	var out []view.DiffLine
	i, j := 0, 0
	for i < m && j < n {
		switch {
		case a[i] == b[j]:
			out = append(out, view.DiffLine{Kind: " ", Text: a[i]})
			i++
			j++
		case lcs[i+1][j] >= lcs[i][j+1]:
			out = append(out, view.DiffLine{Kind: "-", Text: a[i]})
			i++
		default:
			out = append(out, view.DiffLine{Kind: "+", Text: b[j]})
			j++
		}
	}
	for ; i < m; i++ {
		out = append(out, view.DiffLine{Kind: "-", Text: a[i]})
	}
	for ; j < n; j++ {
		out = append(out, view.DiffLine{Kind: "+", Text: b[j]})
	}
	return out
}

func splitLines(s string) []string {
	if s == "" {
		return nil
	}
	var out []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		out = append(out, s[start:])
	}
	return out
}

// consentYAML renders a schema item for the diff card the way the human
// would meet it in the schema file — YAML, not Go syntax or JSON.
func consentYAML(v any) string {
	b, err := yaml.Marshal(v)
	if err != nil {
		return fmt.Sprintf("%+v", v)
	}
	return string(b)
}

// -- consent-gated write methods ---------------------------------------------
//
// These wrap the typed CRUD handlers with the design-decision-5 consent
// rule: with a nil gate they are exactly the ungated write (stdio, /mcp);
// with a gate, an edit or delete of an EXISTING item first renders the
// diff card and blocks for the human's answer, while creating something
// new passes straight through. The gate runs OUTSIDE h.mu — a card can sit
// unanswered for minutes, and holding the session mutex across it would
// freeze every pane and tool — so the existence probe is advisory: the
// write's own revision check under the lock still rejects anything that
// changed while the card was up (stale rejection with current content, the
// same contract vim races get).

func (h *mcpHandlers) consentedPutRuleGroup(ctx context.Context, consent *consentGate, in putRuleGroupInput) (putRuleGroupOutput, error) {
	if consent != nil {
		h.mu.Lock()
		cur, err := h.getRuleGroup(getRuleGroupInput{Head: in.Head, Arity: in.Arity})
		h.mu.Unlock()
		if err == nil { // group exists: this is an edit, consent-gated
			if err := consent.Request(ctx,
				fmt.Sprintf("edit rule group %s/%d", in.Head, in.Arity),
				cur.Text, in.Text); err != nil {
				return putRuleGroupOutput{}, err
			}
		}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.putRuleGroup(in)
}

func (h *mcpHandlers) consentedDeleteRuleGroup(ctx context.Context, consent *consentGate, in deleteRuleGroupInput) (deleteRuleGroupOutput, error) {
	if consent != nil {
		h.mu.Lock()
		cur, err := h.getRuleGroup(getRuleGroupInput{Head: in.Head, Arity: in.Arity})
		h.mu.Unlock()
		if err == nil { // deleting nothing needs no card; the write reports stale
			if err := consent.Request(ctx,
				fmt.Sprintf("delete rule group %s/%d", in.Head, in.Arity),
				cur.Text, ""); err != nil {
				return deleteRuleGroupOutput{}, err
			}
		}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.deleteRuleGroup(in)
}

// existingSchemaItem probes for a current schema item under the lock,
// returning its YAML rendering and whether it exists — the shared first
// half of the six schema gates below.
func (h *mcpHandlers) existingSchemaItem(find func() (any, bool)) (string, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	item, ok := find()
	if !ok {
		return "", false
	}
	return consentYAML(item), true
}

func (h *mcpHandlers) consentedPutSource(ctx context.Context, consent *consentGate, in putSourceInput) (putSourceOutput, error) {
	if consent != nil {
		if old, ok := h.existingSchemaItem(func() (any, bool) {
			s, ok := findSource(h.sess.authoringCfg.Sources, in.File)
			return s, ok
		}); ok {
			proposed := jsonfacts.Source{File: in.File, Mappings: in.Mappings}
			if err := consent.Request(ctx, "edit source "+in.File,
				old, consentYAML(proposed)); err != nil {
				return putSourceOutput{}, err
			}
		}
	}
	return h.putSource(in)
}

func (h *mcpHandlers) consentedDeleteSource(ctx context.Context, consent *consentGate, in deleteSourceInput) (deleteSourceOutput, error) {
	if consent != nil {
		if old, ok := h.existingSchemaItem(func() (any, bool) {
			s, ok := findSource(h.sess.authoringCfg.Sources, in.File)
			return s, ok
		}); ok {
			if err := consent.Request(ctx, "delete source "+in.File, old, ""); err != nil {
				return deleteSourceOutput{}, err
			}
		}
	}
	return h.deleteSource(in)
}

func (h *mcpHandlers) consentedPutMatcher(ctx context.Context, consent *consentGate, in putMatcherInput) (putMatcherOutput, error) {
	if consent != nil {
		key := matcherKeyOf(in.Matcher)
		if old, ok := h.existingSchemaItem(func() (any, bool) {
			m, ok := findMatcher(h.sess.authoringCfg.Matchers, key)
			return m, ok
		}); ok {
			if err := consent.Request(ctx,
				fmt.Sprintf("edit matcher %s[%d]", key.Predicate, key.Term),
				old, consentYAML(in.Matcher)); err != nil {
				return putMatcherOutput{}, err
			}
		}
	}
	return h.putMatcher(in)
}

func (h *mcpHandlers) consentedDeleteMatcher(ctx context.Context, consent *consentGate, in deleteMatcherInput) (deleteMatcherOutput, error) {
	if consent != nil {
		key := matcherKey{Predicate: in.Predicate, Term: in.Term,
			CaseInsensitive: in.CaseInsensitive, Windash: in.Windash}
		if old, ok := h.existingSchemaItem(func() (any, bool) {
			m, ok := findMatcher(h.sess.authoringCfg.Matchers, key)
			return m, ok
		}); ok {
			if err := consent.Request(ctx,
				fmt.Sprintf("delete matcher %s[%d]", key.Predicate, key.Term),
				old, ""); err != nil {
				return deleteMatcherOutput{}, err
			}
		}
	}
	return h.deleteMatcher(in)
}

func (h *mcpHandlers) consentedPutDeclaration(ctx context.Context, consent *consentGate, in putDeclarationInput) (putDeclarationOutput, error) {
	if consent != nil {
		key := declarationKeyOf(in.Declaration)
		if old, ok := h.existingSchemaItem(func() (any, bool) {
			d, ok := findDeclaration(h.sess.authoringCfg.Declarations, key)
			return d, ok
		}); ok {
			if err := consent.Request(ctx,
				fmt.Sprintf("edit declaration %s/%d", key.Name, key.Arity),
				old, consentYAML(in.Declaration)); err != nil {
				return putDeclarationOutput{}, err
			}
		}
	}
	return h.putDeclaration(in)
}

func (h *mcpHandlers) consentedDeleteDeclaration(ctx context.Context, consent *consentGate, in deleteDeclarationInput) (deleteDeclarationOutput, error) {
	if consent != nil {
		key := declarationKey{Name: in.Name, Arity: in.Arity}
		if old, ok := h.existingSchemaItem(func() (any, bool) {
			d, ok := findDeclaration(h.sess.authoringCfg.Declarations, key)
			return d, ok
		}); ok {
			if err := consent.Request(ctx,
				fmt.Sprintf("delete declaration %s/%d", key.Name, key.Arity),
				old, ""); err != nil {
				return deleteDeclarationOutput{}, err
			}
		}
	}
	return h.deleteDeclaration(in)
}
