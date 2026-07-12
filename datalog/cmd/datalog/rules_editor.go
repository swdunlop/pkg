package main

import (
	"context"
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

	ruleset, err := syntax.ParseAll(sig.RulesText)
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

	_ = stream.Emit(datastar.Elements(errorList(nil)))
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

	token := wb.gen.Next()

	if decodeErr != nil {
		_ = stream.Emit(datastar.Elements(statusFragment("")), datastar.Elements(errorList([]string{decodeErr.Error()})))
		return
	}

	ctx, cancel := context.WithTimeout(jobCtx, evalTimeout)
	defer cancel()

	_ = stream.Emit(datastar.Elements(statusFragment("compiling…")))

	// Apply the document (atomic replace of rules/aggRules/rulesText, plus
	// the embedded queries the workbench — unlike set_rules — accepts).
	var queries []syntax.Query
	applyErr := <-runRecovered(func() error {
		wb.h.mu.Lock()
		defer wb.h.mu.Unlock()
		qs, err := wb.h.sess.setRulesWithQueries(sig.RulesText)
		queries = qs
		return err
	})

	if ctx.Err() != nil {
		_ = stream.Emit(datastar.Elements(statusFragment(evalHaltStatus(ctx))))
		return
	}
	if applyErr != nil {
		_ = stream.Emit(datastar.Elements(statusFragment("")), datastar.Elements(errorList([]string{applyErr.Error()})))
		return
	}
	if wb.gen.Stale(token) {
		return // a newer Run superseded this one; discard our result
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
	var evaluated datalog.Database
	var evalErr error
	evalErr = <-runRecovered(func() error {
		wb.h.mu.Lock()
		defer wb.h.mu.Unlock()
		var err error
		evaluated, err = wb.h.sess.evaluate(ctx)
		if err == nil {
			wb.h.sess.derivedDB = evaluated
		}
		return err
	})
	if ctx.Err() != nil {
		_ = stream.Emit(datastar.Elements(statusFragment(evalHaltStatus(ctx))))
		return
	}
	if wb.gen.Stale(token) {
		return
	}

	if len(queries) == 0 {
		var blocks []queryResultBlock
		if evalErr != nil {
			blocks = append(blocks, queryResultBlock{Err: evalErr.Error()})
		} else if capErr := checkFactCap(evaluated); capErr != nil {
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
			// Hold h.mu only for the snapshot; the Transform below can
			// run up to the eval timeout and must not freeze the other
			// panes or the MCP tools sharing this mutex.
			wb.h.mu.Lock()
			snap, err := wb.h.sess.snapshotForQuery()
			wb.h.mu.Unlock()
			if err != nil {
				return err
			}
			rows, vars, _, err = snap.runQuery(ctx, &q)
			return err
		})

		if ctx.Err() != nil {
			timedOut = true
			break
		}
		if qErr != nil {
			blocks = append(blocks, queryResultBlock{Query: q.String(), Err: qErr.Error()})
			continue
		}
		blocks = append(blocks, renderQueryResult(q.String(), vars, rows))
	}

	if evalErr != nil {
		blocks = append(blocks, queryResultBlock{Err: evalErr.Error()})
	} else if capErr := checkFactCap(evaluated); capErr != nil {
		blocks = append(blocks, queryResultBlock{Err: capErr.Error()})
	}
	wb.h.mu.Lock()
	wb.publishSessionChanged()
	wb.h.mu.Unlock()

	if wb.gen.Stale(token) {
		return
	}

	status := ""
	if timedOut {
		status = evalHaltStatus(ctx)
	}
	_ = stream.Emit(datastar.Elements(statusFragment(status)), datastar.Elements(resultsFragment(blocks)))
}

// evalHaltStatus words the status line for an evaluation ctx that ended
// early: a user Stop (context.Canceled via /cancel or a closed page) reads
// differently from the evalTimeout deadline expiring.
func evalHaltStatus(ctx context.Context) string {
	if ctx.Err() == context.Canceled {
		return "run stopped"
	}
	return "evaluation timed out, results may be incomplete"
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
