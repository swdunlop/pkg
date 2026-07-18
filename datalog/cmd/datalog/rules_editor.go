package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	html "github.com/swdunlop/html-go"
	"github.com/swdunlop/html-go/datastar"
	"github.com/swdunlop/html-go/tag"
	"swdunlop.dev/pkg/datalog"
	"swdunlop.dev/pkg/datalog/cmd/datalog/view"
	"swdunlop.dev/pkg/datalog/seminaive"
	"swdunlop.dev/pkg/datalog/syntax"
)

// rulesSignals mirrors the Datalog Editor's textarea signal (data-bind:rules-text
// in view.RulesEditor), used by both /rules/check and /rules/run.
type rulesSignals struct {
	RulesText string `json:"rulesText"`
}

// handleRulesCheck is the debounced parse/compile-only endpoint
// (POST /rules/check). Per doc/features/web-ui.md observation 5, keystrokes
// buy errors cheaply: this runs syntax.ParseAll plus a trial
// seminaive.Compile with no session mutation and no timeout — parse+compile
// costs microseconds. Embedded `?` queries are allowed here (the document
// convention matches the REPL: a pasted .dl file may carry queries), so this
// does not call session.setRules (which rejects them) and never mutates the
// session; it only refreshes the error list.
func (wb *workbench) handleRulesCheck(w http.ResponseWriter, r *http.Request) {
	// Decode signals BEFORE opening the SSE stream: RequestStream/startSSE
	// writes response headers (and, empirically, leaves r.Body unreadable
	// afterward), so the request body must be consumed first.
	var sig rulesSignals
	decodeErr := datastar.Decode(&sig, r)

	stream, err := datastar.RequestStream(w, r)
	if err != nil {
		return
	}

	if decodeErr != nil {
		_ = stream.Emit(datastar.Elements(errorList([]string{decodeErr.Error()})))
		return
	}

	ruleset, err := parseUserProgram(sig.RulesText)
	if err != nil {
		_ = stream.Emit(datastar.Elements(errorList([]string{err.Error()})))
		return
	}

	wb.h.mu.Lock()
	opts := wb.h.sess.engineOpts
	wb.h.mu.Unlock()

	if _, err := seminaive.New(opts...).Compile(ruleset); err != nil {
		_ = stream.Emit(datastar.Elements(errorList([]string{err.Error()})))
		return
	}

	// A clean parse/compile still surfaces ruleset.Warnings -- detached
	// '%%' doc blocks the author almost certainly meant to attach. This is
	// the round-trip editor's data-loss tell: the workbench rules document
	// round-trips through parse->String on every edit, so a dropped doc is
	// user data loss the author needs to see rather than a silent
	// "no errors" that hides it. Rendered in the same #rules-error fragment,
	// prefixed "warning:" so they read distinctly from hard errors.
	_ = stream.Emit(datastar.Elements(errorList(warningPrefixed(ruleset.Warnings))))
}

// warningPrefixed prefixes each parse warning with "warning: " so the
// shared #rules-error list distinguishes non-fatal diagnostics (dropped
// detached doc blocks) from hard parse/compile errors. Returns nil for no
// warnings, so the list renders empty (and is hidden by the :empty CSS
// rule) exactly as on a fully clean check.
func warningPrefixed(warnings []string) []string {
	if len(warnings) == 0 {
		return nil
	}
	out := make([]string, len(warnings))
	for i, w := range warnings {
		out[i] = "warning: " + w
	}
	return out
}

// errorList renders the #rules-error fragment: an empty list (hidden by the
// :empty CSS rule) on success, or one <li> per error message, verbatim from
// the parser/compiler (already line:col prefixed).
func errorList(msgs []string) html.Content {
	items := make([]html.Content, len(msgs))
	for i, m := range msgs {
		items[i] = tag.New("li", html.Text(m))
	}
	return view.ErrorList.Set("id", "rules-error").Add(items...)
}

// statusFragment renders the #status fragment (doc/notes/datastar.md §9):
// empty clears it (hidden by :empty CSS), non-empty shows progress or a
// final report.
func statusFragment(msg string) html.Content {
	d := view.StatusDiv.Set("id", "status")
	if msg == "" {
		return d
	}
	return d.Add(html.Text(msg))
}

// rulesRunJobKey is the Jobs key for the Datalog Editor's Run action — the
// sandbox's Global Cancel button targets every registered job indifferently,
// but Begin's per-key busy check needs a stable name for "a Run is already
// in flight."
const rulesRunJobKey = "run"

// handleRulesRun is the Run action (POST /rules/run): the explicit,
// timeout-bounded, job-gated counterpart to handleRulesCheck. It applies the
// full document (rules plus any embedded queries) to the session via the
// new session.setRulesWithQueries method, executes each embedded query in
// turn, and streams #status progress throughout (doc/notes/datastar.md §9).
func (wb *workbench) handleRulesRun(w http.ResponseWriter, r *http.Request) {
	// Decode signals BEFORE opening the SSE stream — see handleRulesCheck's
	// comment: RequestStream leaves r.Body unreadable afterward.
	var sig rulesSignals
	decodeErr := datastar.Decode(&sig, r)

	stream, err := datastar.RequestStream(w, r)
	if err != nil {
		return
	}

	jobCtx, done := wb.jobs.Begin(r.Context(), rulesRunJobKey)
	if jobCtx == nil {
		_ = stream.Emit(datastar.Elements(statusFragment("already running")))
		return
	}
	defer done()
	// rulesRunJobKey ("run") doubles as the UI's $busy vocabulary here —
	// see publishBusy's doc; jsonfacts passes "apply" explicitly instead.
	wb.publishBusy(rulesRunJobKey)
	defer wb.publishBusy("")

	if decodeErr != nil {
		_ = stream.Emit(datastar.Elements(statusFragment("")), datastar.Elements(errorList([]string{decodeErr.Error()})))
		return
	}

	ctx, cancel := context.WithTimeout(jobCtx, evalTimeout)
	defer cancel()

	_ = stream.Emit(datastar.Elements(statusFragment("compiling…")))

	// Apply the document (atomic replace of rules/aggRules/rulesText, plus
	// the embedded queries the workbench — unlike set_rules — accepts).
	// runApplyRulesDocument checks ctx AFTER acquiring wb.h.mu and BEFORE
	// mutating (mirroring runApplySchema's late-arrival guard,
	// jsonfacts_editor.go): a Stop or expired deadline racing the lock must
	// never land a mutation this handler is about to report as "not
	// applied."
	res := <-runApplyRulesDocument(ctx, wb, sig.RulesText)
	queries := res.queries

	if ctx.Err() != nil {
		wb.h.mu.Lock()
		wb.publishSessionChanged()
		wb.h.mu.Unlock()
		_ = stream.Emit(datastar.Elements(statusFragment(evalHaltStatus(ctx, "run stopped"))))
		return
	}
	if res.err != nil {
		_ = stream.Emit(datastar.Elements(statusFragment("")), datastar.Elements(errorList([]string{res.err.Error()})))
		return
	}

	// Evaluate the applied ruleset once, unconditionally, and cache the
	// result on the session (session.derivedDB) — this is what the Fact
	// Browser's "derived" column reads (doc/features/web-ui.md design
	// constraint 2). Previously nothing populated this: setRulesWithQueries
	// only parses/compiles, and Transform only ever ran inside runQuery's
	// per-query synthetic-rule evaluation, whose output was discarded after
	// extracting that one query's rows — so the Fact Browser always showed
	// derived predicates as empty regardless of what Run just computed. A
	// failure here doesn't abort the request; queries below can still
	// succeed or fail independently, exactly as before this change.
	//
	// The snapshot/Transform/swap-back split mirrors set_schema's
	// prepareSchema/applySchemaLocked split (mcp.go, session.go's
	// snapshotForEvaluate doc comment): wb.h.mu is held only to capture the
	// inputs and, afterward, to commit the result — the Transform itself
	// (which can run for up to evalTimeout) runs lock-free, so Run no
	// longer freezes every page load, SSE connect, and MCP call for its
	// duration. The over-cap check happens BEFORE caching or publishing,
	// matching the startup evaluation's order (serve.go): a rule that blows
	// the cap must never be cached or shown as this run's "derived" data,
	// even momentarily.
	wb.h.mu.Lock()
	ruleset, engineOpts, db, snapGen, buildErr := wb.h.sess.snapshotForEvaluate()
	prov := wb.h.sess.newEvalProvenance()
	wb.h.mu.Unlock()

	var evaluated datalog.Database
	evalErr := buildErr
	if buildErr == nil {
		evalErr = <-runRecovered(func() error {
			var err error
			evaluated, err = evaluateSnapshot(ctx, ruleset, engineOpts, db, prov)
			return err
		})
	}

	var capErr error
	if evalErr == nil {
		capErr = checkFactCap(evaluated)
	}

	wb.h.mu.Lock()
	if ctx.Err() == nil && evalErr == nil && capErr == nil && wb.h.sess.gen == snapGen {
		// prov, cached beside evaluated in the same critical section under
		// the same snapGen guard, is the recorder for THIS Transform — see
		// doc/features/provenance.md "Session cache interaction" and
		// session.derivedProv's doc comment.
		wb.h.sess.derivedDB = evaluated
		wb.h.sess.derivedProv = prov
	}
	wb.h.mu.Unlock()

	if ctx.Err() != nil {
		wb.h.mu.Lock()
		wb.publishSessionChanged()
		wb.h.mu.Unlock()
		_ = stream.Emit(datastar.Elements(statusFragment(evalHaltStatus(ctx, "run stopped"))))
		return
	}

	if len(queries) == 0 {
		var blocks []queryResultBlock
		if evalErr != nil {
			blocks = append(blocks, queryResultBlock{Err: evalErr.Error()})
		} else if capErr != nil {
			blocks = append(blocks, queryResultBlock{Err: capErr.Error()})
		}
		wb.h.mu.Lock()
		wb.publishSessionChanged()
		wb.h.mu.Unlock()
		_ = stream.Emit(datastar.Elements(statusFragment("")), datastar.Elements(resultsFragment(blocks)))
		return
	}

	blocks := make([]queryResultBlock, 0, len(queries))
	timedOut := false
	for i := range queries {
		if ctx.Err() != nil {
			timedOut = true
			break
		}
		_ = stream.Emit(datastar.Elements(statusFragment(fmt.Sprintf("running query %d of %d…", i+1, len(queries)))))

		q := queries[i]
		var rows [][]datalog.Constant
		var vars []string
		qErr := <-runRecovered(func() error {
			snap, err := wb.h.lockedSnapshot()
			if err != nil {
				return err
			}
			rows, vars, _, err = snap.runQuery(ctx, &q)
			return err
		})

		var blk queryResultBlock
		if qErr == nil {
			blk = renderQueryResult(q.String(), vars, rows)
		}
		outcome := classifyQueryOutcome(ctx, q.String(), blk, qErr, "run stopped")
		if outcome.RenderBlock {
			blocks = append(blocks, outcome.Block)
		}
		if outcome.Halt != "" {
			timedOut = true
		}
		if !outcome.Continue {
			break
		}
	}

	if evalErr != nil {
		blocks = append(blocks, queryResultBlock{Err: evalErr.Error()})
	} else if capErr != nil {
		blocks = append(blocks, queryResultBlock{Err: capErr.Error()})
	}
	wb.h.mu.Lock()
	wb.publishSessionChanged()
	wb.h.mu.Unlock()

	status := ""
	if timedOut {
		status = evalHaltStatus(ctx, "run stopped")
	}
	_ = stream.Emit(datastar.Elements(statusFragment(status)), datastar.Elements(resultsFragment(blocks)))
}

// applyRulesResult is runApplyRulesDocument's result payload.
type applyRulesResult struct {
	queries []syntax.Query
	err     error
}

// runApplyRulesDocument runs setRulesWithQueries under wb.h.mu, via
// runRecovered for panic recovery, mirroring runApplySchema's ctx-after-lock
// guard (jsonfacts_editor.go): ctx.Err() is checked AFTER acquiring wb.h.mu
// and BEFORE calling setRulesWithQueries, so a Stop or expired deadline
// racing the lock can never land a rules mutation the handler is about to
// report as "not applied" (run stopped). Before this fix, nothing gated the
// call at all — a stopped run could still replace the session's rules while
// reporting "run stopped." Parse/compile (setRulesWithQueries' own cost) is
// cheap relative to evalTimeout, so — unlike setSchema's data reload —
// holding wb.h.mu across the whole call is fine; there is no lock-free
// "prepare" half worth splitting out here.
//
// KNOWN DIVERGENCE under a --rules (rules/ directory store) session: this
// path calls session.setRulesWithQueries, which only ever mutates SESSION
// MEMORY (s.rules/s.aggRules/s.rulesText) — it has no idea a *ruleStore
// exists at all and never touches h.rules or any file on disk. The v1
// workbench's Datalog Editor Run action therefore still edits the
// monolithic in-memory document even when the session was started with
// --rules: pasting a whole ruleset into the editor and clicking Run applies
// it to the session but does NOT update the rules/ directory's group files,
// so a subsequent put_rule_group/delete_rule_group call (which reloads
// FROM the store via session.loadRuleStore) would silently discard whatever
// Run just applied, and the rules/ directory and the session's in-memory
// ruleset can disagree indefinitely. This is accepted, not fixed, for phase
// 1b (doc/features/workbench-v2.md work item 2, "Old UI keeps working until
// phase 2 replaces it," is what retires this whole editor/Run affordance in
// favor of the new browser's read-only Rules tab plus the CRUD tools) — see
// this task's brief, which calls this out explicitly as a known, deferred
// divergence rather than a bug to fix now.
func runApplyRulesDocument(ctx context.Context, wb *workbench, rulesText string) <-chan applyRulesResult {
	out := make(chan applyRulesResult, 1)
	var queries []syntax.Query
	done := runRecovered(func() error {
		wb.h.mu.Lock()
		defer wb.h.mu.Unlock()
		if err := ctx.Err(); err != nil {
			return err
		}
		qs, err := wb.h.sess.setRulesWithQueries(rulesText)
		queries = qs
		return err
	})
	go func() {
		err := <-done
		out <- applyRulesResult{queries: queries, err: err}
	}()
	return out
}

// evalHaltStatus words the status line for an evaluation ctx that ended
// early: a user Stop (context.Canceled via /cancel or a closed page) reads
// differently from the evalTimeout deadline expiring. stopped is the
// surface-specific wording for the user-cancel case ("run stopped",
// "query stopped", ...); the timeout wording is shared. This is the single
// place that classifies ctx.Err() for user-facing halt messages -- new
// handlers must call it rather than re-deriving the branch inline.
func evalHaltStatus(ctx context.Context, stopped string) string {
	if ctx.Err() == context.Canceled {
		return stopped
	}
	return "evaluation timed out, results may be incomplete"
}

// perQueryOutcome is the result of running one query in a multi-query loop
// (handleConsoleQuery, handleRulesRun): the mechanism both call sites use to
// decide what to render and whether to keep looping. A round-two review
// found both loops checking ctx.Err() BEFORE looking at whether runQuery had
// already produced a result, so a deadline landing in the gap after a
// successful runQuery returned discarded the finished rows and showed only
// halt-status wording — with multi-query input, one query consuming the
// budget silently ate the next query's completed results. The fix is this
// ordering rule, applied once here rather than at each call site: whenever
// runQuery produced an outcome of its own — a completed result (qErr nil) or
// the query's OWN failure, one unrelated to ctx ending — that outcome is
// ALWAYS rendered (RenderBlock true), regardless of what ctx.Err() reports
// afterward. The one case Block is suppressed is qErr being the ctx
// cancellation/deadline itself (errors.Is(qErr, ctx.Err()) — semi-naive's own
// mid-Transform ctx check unwound the query, so qErr carries no information
// beyond "ctx ended" already): showing a raw "context canceled" block right
// next to the halt message would just duplicate the same event twice. Either
// way, ctx.Err() alone controls whether a trailing halt-status message
// follows (Halt) and whether the caller's loop should keep going (Continue
// false stops it).
type perQueryOutcome struct {
	RenderBlock bool             // true: Block holds a result or the query's own error, always render it
	Block       queryResultBlock // valid when RenderBlock is true
	Halt        string           // non-empty: append this halt-status message (after Block, if rendered)
	Continue    bool             // false: the caller's multi-query loop must stop here
}

// classifyQueryOutcome applies the ordering rule described on
// perQueryOutcome to one query's (blk, qErr, ctx) triple. stopped is the
// surface-specific wording evalHaltStatus should use for the halt message
// when ctx ended in a user cancel ("query stopped", "run stopped", ...).
func classifyQueryOutcome(ctx context.Context, queryText string, blk queryResultBlock, qErr error, stopped string) perQueryOutcome {
	out := perQueryOutcome{Continue: ctx.Err() == nil}
	switch {
	case qErr == nil:
		// runQuery succeeded and blk is a real result — render it
		// unconditionally. ctx.Err() only decides the trailing halt message
		// and whether the loop continues.
		out.RenderBlock = true
		out.Block = blk
	case ctx.Err() != nil && errors.Is(qErr, ctx.Err()):
		// qErr IS the ctx ending (semi-naive's own mid-Transform ctx check
		// unwound the query) — nothing beyond "ctx ended" to show, so skip
		// the redundant raw error block and let Halt alone speak for it.
	default:
		// A genuine query-level failure (bad query, broken snapshot, ...),
		// independent of ctx — always rendered, never treated as a halt.
		out.RenderBlock = true
		out.Block = queryResultBlock{Query: queryText, Err: qErr.Error()}
	}
	if ctx.Err() != nil {
		out.Halt = evalHaltStatus(ctx, stopped)
	}
	return out
}

// queryResultBlock is one embedded query's rendered outcome: either an
// error, or a variable-named table of (possibly truncated) rows.
type queryResultBlock struct {
	Query     string
	Vars      []string
	Rows      [][]string
	Total     int
	Truncated bool
	Err       string
}

// renderQueryResult caps rows at defaultQueryLimit (the same 100-row default
// the MCP query tool uses) and converts constants to display strings via
// constantToJSON's conventions.
func renderQueryResult(queryText string, vars []string, rows [][]datalog.Constant) queryResultBlock {
	total := len(rows)
	limit := defaultQueryLimit
	truncated := total > limit
	if truncated {
		rows = rows[:limit]
	}
	out := make([][]string, len(rows))
	for i, row := range rows {
		cells := make([]string, len(row))
		for j, c := range row {
			cells[j] = fmt.Sprint(constantToJSON(c))
		}
		out[i] = cells
	}
	return queryResultBlock{
		Query:     queryText,
		Vars:      vars,
		Rows:      out,
		Total:     total,
		Truncated: truncated,
	}
}

// resultsFragment renders the #rules-results fragment: one block per
// executed query, each showing the query text, a variable-named table, and
// a truncation note if applicable.
func resultsFragment(blocks []queryResultBlock) html.Content {
	children := make([]html.Content, len(blocks))
	for i, b := range blocks {
		children[i] = resultBlock(b)
	}
	return tag.New("div#rules-results").Add(children...)
}

func resultBlock(b queryResultBlock) html.Content {
	heading := tag.New("p.query-text", html.Text(strings.TrimSpace(b.Query)))
	if b.Err != "" {
		return tag.New("div.result-block", heading, tag.New("ul.errors", tag.New("li", html.Text(b.Err))))
	}
	return tag.New("div.result-block", heading, resultTable(b))
}

// resultTable renders b's variable-named table and truncation note — shared
// between the editor's result blocks and the agent transcript's query
// entries, which supply their own headings.
func resultTable(b queryResultBlock) html.Content {
	header := make([]html.Content, len(b.Vars))
	for i, v := range b.Vars {
		header[i] = tag.New("th", html.Text(v))
	}
	rows := make([]html.Content, len(b.Rows))
	for i, row := range b.Rows {
		cells := make([]html.Content, len(row))
		for j, c := range row {
			cells[j] = tag.New("td", html.Text(c))
		}
		rows[i] = tag.New("tr", cells...)
	}

	var note html.Content = html.Group{}
	if b.Truncated {
		note = tag.New("p.truncated", html.Text(fmt.Sprintf("showing %d of %d rows", len(b.Rows), b.Total)))
	}

	return html.Group{
		tag.New("table",
			tag.New("thead", tag.New("tr", header...)),
			tag.New("tbody", rows...),
		),
		note,
	}
}
