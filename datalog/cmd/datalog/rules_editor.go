package main

import (
	"context"
	"errors"
	"fmt"
	"strings"

	html "github.com/swdunlop/html-go"
	"github.com/swdunlop/html-go/tag"
	"swdunlop.dev/pkg/datalog"
)

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
