package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/mark3labs/kit/pkg/kit"
	html "github.com/swdunlop/html-go"
	"github.com/swdunlop/html-go/tag"
	"swdunlop.dev/pkg/datalog/cmd/datalog/view"
	"swdunlop.dev/pkg/datalog/jsonfacts"
)

// This file implements the composer's `?` and `!` commands (doc/features/
// workbench-v2.md design decision 8): a leading `?` runs a datalog query
// (all modes); a leading `!` evaluates an expr against the record
// currently selected in the Data tab (Facts Mode). The command and its
// result render in the transcript and persist with the conversation
// (extension-data entries); command/result pairs since the agent's last
// turn are prepended to the next prompt, clearly framed as "the user ran
// these" — but a command never grants the agent a turn.

// commandExtType is the extension-data type name command/result pairs
// persist under, beside conversationExtType in the session file.
const commandExtType = "datalog-command"

// commandRecord is one persisted (and preamble-pending) command/result
// pair. Result is the plain-text summary the agent reads — the transcript
// renders richer HTML, but the preamble and the persisted record speak
// text.
type commandRecord struct {
	Kind   string `json:"kind"` // "query" | "expr"
	Input  string `json:"input"`
	Result string `json:"result"`
}

// composerCommand recognizes a command prompt: a leading "?" or "!"
// (design decision 8). Returns the kind ("query"/"expr"), the trimmed
// remainder, and whether it was a command at all.
func composerCommand(text string) (kind, rest string, ok bool) {
	switch {
	case strings.HasPrefix(text, "?"):
		return "query", strings.TrimSpace(text[1:]), true
	case strings.HasPrefix(text, "!"):
		return "expr", strings.TrimSpace(text[1:]), true
	default:
		return "", "", false
	}
}

// runComposerCommand executes one `?`/`!` command for a conversation:
// renders the outcome into the transcript, persists the pair into the
// session file, and queues it for the next turn's preamble. It never
// starts a turn.
func (wb *workbench) runComposerCommand(convID string, mode conversationMode, kind, input string) {
	var rendered html.Content
	var result string
	switch kind {
	case "query":
		rendered, result = wb.runComposerQuery(input)
	case "expr":
		rendered, result = wb.runComposerExpr(mode, input)
	}
	wb.consoleAppend(convID, "query", rendered)

	rec := commandRecord{Kind: kind, Input: input, Result: result}
	wb.persistCommand(convID, rec)
	wb.cmdMu.Lock()
	wb.pendingCmds[convID] = append(wb.pendingCmds[convID], rec)
	wb.cmdMu.Unlock()
}

// runComposerQuery runs one `?` command through the same pipeline the
// query tool uses (lockedSnapshot → runQuery under wb.h.evalContext's
// resolved --eval-timeout, outcomes ordered by classifyQueryOutcome),
// returning the transcript rendering and the plain-text summary for
// persistence/preamble.
func (wb *workbench) runComposerQuery(input string) (html.Content, string) {
	echo := queryEcho("? " + input)
	if input == "" {
		return html.Group{echo, commandError("no query given")}, "error: no query given"
	}
	// The composer input is a query by convention; add the terminator so
	// the user doesn't type it every probe. A `.` terminator is left alone
	// so pasted rules parse as rules and hit the queries-only rejection.
	text := input
	if !strings.HasSuffix(text, "?") && !strings.HasSuffix(text, ".") {
		text += "?"
	}

	ruleset, err := parseUserProgram(text)
	if err != nil {
		return html.Group{echo, commandError(err.Error())}, "error: " + err.Error()
	}
	if len(ruleset.Rules) > 0 || len(ruleset.AggRules) > 0 {
		const msg = "the ? command runs queries only — author rules through a Rules conversation or vim"
		return html.Group{echo, commandError(msg)}, "error: " + msg
	}
	if len(ruleset.Queries) == 0 {
		return html.Group{echo, commandError("no query found")}, "error: no query found"
	}

	// One job key gates concurrent commands; Global Cancel reaches it like
	// everything else. The composer disables while $busy anyway, so a
	// contended Begin means a direct POST race — refuse rather than queue.
	jobCtx, done := wb.jobs.Begin(context.Background(), composerQueryJobKey)
	if jobCtx == nil {
		return html.Group{echo, commandError("another command is already running")},
			"error: another command is already running"
	}
	defer done()
	wb.publishBusy("query")
	defer wb.publishBusy("")

	ctx, cancel := wb.h.evalContext(jobCtx)
	defer cancel()

	parts := html.Group{echo}
	var summary strings.Builder
	for i := range ruleset.Queries {
		q := ruleset.Queries[i]
		var blk queryResultBlock
		qErr := <-runRecovered(func() error {
			snap, err := wb.h.lockedSnapshot()
			if err != nil {
				return err
			}
			rows, vars, _, err := snap.runQuery(ctx, &q)
			if err == nil {
				blk = renderQueryResult(q.String(), vars, rows)
			}
			return err
		})
		outcome := classifyQueryOutcome(ctx, q.String(), blk, qErr, "query stopped")
		if outcome.RenderBlock {
			if outcome.Block.Err != "" {
				parts = append(parts, commandError(outcome.Block.Err))
				fmt.Fprintf(&summary, "%s\nerror: %s\n", q.String(), outcome.Block.Err)
			} else {
				parts = append(parts, resultBlock(outcome.Block),
					linkRow("predicates", atomPredicateLinks(q.Body)))
				summarizeQueryBlock(&summary, outcome.Block)
			}
		}
		if outcome.Halt != "" {
			parts = append(parts, commandError(outcome.Halt))
			fmt.Fprintf(&summary, "%s\n%s\n", q.String(), outcome.Halt)
		}
		if !outcome.Continue {
			break
		}
	}
	return parts, strings.TrimRight(summary.String(), "\n")
}

// summarizeQueryBlock writes one query's outcome as the plain text the
// preamble carries: the query, the variable header, and every row as a
// tab-joined tuple (already bounded by the query pipeline's own limits).
func summarizeQueryBlock(w *strings.Builder, blk queryResultBlock) {
	fmt.Fprintf(w, "%s\n", blk.Query)
	if len(blk.Rows) == 0 {
		w.WriteString("no results\n")
		return
	}
	fmt.Fprintf(w, "%s\n", strings.Join(blk.Vars, "\t"))
	for _, row := range blk.Rows {
		fmt.Fprintf(w, "%s\n", strings.Join(row, "\t"))
	}
	if blk.Truncated {
		fmt.Fprintf(w, "(%d total, truncated)\n", blk.Total)
	}
}

// runComposerExpr runs one `!` command: an expr evaluated against the Data
// tab's selected record (jsonfacts.EvalExpr — the same environment and
// builtins a mapping expr gets). Facts conversations only; no selection is
// an inline error (design decision 8).
func (wb *workbench) runComposerExpr(mode conversationMode, input string) (html.Content, string) {
	echo := queryEcho("! " + input)
	fail := func(msg string) (html.Content, string) {
		return html.Group{echo, commandError(msg)}, "error: " + msg
	}
	if mode != conversationModeFacts {
		return fail("the ! command evaluates extraction exprs and is available in Facts conversations only")
	}
	if input == "" {
		return fail("no expr given")
	}

	wb.selMu.Lock()
	file, raw, valid := wb.selFile, wb.selRecord, wb.selValid
	wb.selMu.Unlock()
	if !valid {
		return fail("no record selected — pick a row in the Data tab first")
	}

	var record any
	if err := json.Unmarshal([]byte(raw), &record); err != nil {
		return fail(fmt.Sprintf("selected record in %s is not valid JSON: %v", file, err))
	}

	result, asserted, err := jsonfacts.EvalExpr(input, record)
	if err != nil {
		return fail(err.Error())
	}

	var summary strings.Builder
	fmt.Fprintf(&summary, "against %s: %s\n", file, exprValueText(result))
	parts := html.Group{echo,
		tag.New("p.expr-result", html.Text("⇒ "+exprValueText(result)))}
	if len(asserted) > 0 {
		lines := make(html.Group, 0, len(asserted))
		seen := map[string]bool{}
		var links []html.Content
		for _, f := range asserted {
			text := assertedFactText(f)
			lines = append(lines, tag.New("li", html.Text(text)))
			fmt.Fprintf(&summary, "asserted %s\n", text)
			key := fmt.Sprintf("%s/%d", f.Predicate, len(f.Args))
			if !seen[key] && isPredicateIdent(f.Predicate) {
				seen[key] = true
				links = append(links, view.FactsLink(f.Predicate, len(f.Args), f.Predicate))
			}
		}
		parts = append(parts, tag.New("ul.asserted-facts", lines),
			linkRow("predicates", links))
	}
	return parts, strings.TrimRight(summary.String(), "\n")
}

// exprValueText renders an expr result value for display: JSON where it
// can, %v where it can't.
func exprValueText(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprint(v)
	}
	return string(b)
}

// assertedFactText renders one asserted fact as predicate(args...), with
// args in their JSON form — a probe display, not datalog source (the
// loader's own normalizeToConstant rules apply only when a mapping
// actually loads facts).
func assertedFactText(f jsonfacts.AssertedFact) string {
	args := make([]string, len(f.Args))
	for i, a := range f.Args {
		args[i] = exprValueText(a)
	}
	return fmt.Sprintf("%s(%s)", f.Predicate, strings.Join(args, ", "))
}

// commandError renders a command failure inline in its transcript entry.
func commandError(msg string) html.Content {
	return tag.New("ul.errors", tag.New("li", html.Text(msg)))
}

// composerQueryJobKey gates `?` commands on the jobs set: one at a time,
// Global Cancel reaches it.
const composerQueryJobKey = "composer-command"

// persistCommand appends the command/result pair to the conversation's
// session file. When the conversation's kit agent is live, the append MUST
// go through it (a second TreeManager on the same open file would fork the
// tree: kit's in-memory leaf would not see the external entry, and the
// next message would parent past it); otherwise a short-lived open is
// safe. Best-effort: an ACP conversation (no kit session) or a failed
// append loses persistence, not the command itself.
func (wb *workbench) persistCommand(convID string, rec commandRecord) {
	data, err := json.Marshal(rec)
	if err != nil {
		return
	}

	wb.agentMu.Lock()
	driver := wb.agent
	ownsConv := wb.agentConvID == convID
	wb.agentMu.Unlock()
	if ownsConv {
		if kd, ok := driver.(*kitDriver); ok && kd != nil {
			_, _ = kd.k.GetSessionManager().AppendExtensionData(commandExtType, string(data))
			return
		}
	}
	if wb.conversations == nil {
		return
	}
	tm, err := kit.OpenTreeSession(wb.conversations.sessionPath(convID))
	if err != nil {
		return
	}
	defer tm.Close()
	_, _ = kit.NewTreeManagerAdapter(tm).AppendExtensionData(commandExtType, string(data))
}

// consumePreamble builds the next turn's preamble for convID (design
// decision 8): the command/result pairs run since the agent's last turn,
// framed as the user's own actions, plus a disk-change notice when
// fsnotify reloaded project files in the gap. Returns "" when there is
// nothing to say. Consuming clears the pending queue and advances the
// conversation's reload watermark.
func (wb *workbench) consumePreamble(convID string) string {
	wb.reloadMu.Lock()
	reloadSeq := wb.reloadSeq
	last := wb.lastReload
	wb.reloadMu.Unlock()

	wb.cmdMu.Lock()
	cmds := wb.pendingCmds[convID]
	delete(wb.pendingCmds, convID)
	seen, hasBaseline := wb.reloadSeen[convID]
	wb.reloadSeen[convID] = reloadSeq
	wb.cmdMu.Unlock()

	var b strings.Builder
	if len(cmds) > 0 {
		b.WriteString("The user ran these workbench commands since your last turn (you are seeing their results, not being asked to react):\n")
		for _, c := range cmds {
			marker := "?"
			if c.Kind == "expr" {
				marker = "!"
			}
			fmt.Fprintf(&b, "%s %s\n%s\n", marker, c.Input, indentLines(c.Result))
		}
	}
	// Disk-change notices only report reloads that happened BETWEEN this
	// conversation's turns: the first turn establishes the watermark, so a
	// conversation started after a reload is not told about ancient
	// history.
	if hasBaseline && reloadSeq > seen {
		if last.Err != "" {
			fmt.Fprintf(&b, "Project files changed on disk since your last turn, but the reload FAILED (last good state kept): %s\n", last.Err)
		} else if len(last.Changed) > 0 {
			fmt.Fprintf(&b, "Project files changed on disk since your last turn: %s. Re-read anything you plan to edit — your revisions may be stale.\n", strings.Join(last.Changed, ", "))
		}
	}
	return strings.TrimRight(b.String(), "\n")
}

// indentLines indents a command result under its command line.
func indentLines(s string) string {
	if s == "" {
		return "  (no output)"
	}
	lines := strings.Split(s, "\n")
	for i, ln := range lines {
		lines[i] = "  " + ln
	}
	return strings.Join(lines, "\n")
}

// framePromptWithPreamble prepends a non-empty preamble to the user's
// message, clearly delimited so the model can tell the workbench's framing
// from the human's prose.
func framePromptWithPreamble(preamble, text string) string {
	if preamble == "" {
		return text
	}
	return "[workbench context]\n" + preamble + "\n[end workbench context]\n\n" + text
}

// frameModePreamble prepends the conversation's mode instructions to the
// FIRST prompt sent through a driver that needs them in-band (design
// decision 7: ACP conversations get the same mode choice, with "mode
// instructions ride the first prompt as preamble" — the kit driver
// carries them as its system prompt instead and never matches here).
// Tracked in memory per conversation: an ACP session is not resumable
// across restarts anyway (decision 7), so a restart re-sending the
// instructions on the next first prompt is correct, not a bug.
func (wb *workbench) frameModePreamble(driver agentDriver, convID string, mode conversationMode, text string) string {
	np, ok := driver.(interface{ NeedsModePreamble() bool })
	if !ok || !np.NeedsModePreamble() {
		return text
	}
	wb.cmdMu.Lock()
	sent := wb.modePreambled[convID]
	wb.modePreambled[convID] = true
	wb.cmdMu.Unlock()
	if sent {
		return text
	}
	return "[conversation mode]\n" + conversationSystemPrompt(mode) +
		"\nUse only the tools this mode describes, even if others are reachable.\n[end conversation mode]\n\n" + text
}
