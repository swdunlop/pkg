package view

import (
	"encoding/json"
	"fmt"
	"net/url"
	"sort"

	html "github.com/swdunlop/html-go"
	"github.com/swdunlop/html-go/tag"
)

// DataBrowser renders the Data tab's master-detail shell (doc/features/
// workbench-v2.md design decision 9): a file picker and substring filter up
// top, then two columns — a compact record list (line number + truncated
// preview) on the left, and the selected record pretty-printed with
// collapsible nesting on the right. Selection doubles as the `!` composer
// command's evaluation target.
//
//   - #data-files      — file list, patched by GET /data
//   - #data-table-body — paginated row list, patched (append mode) by
//     GET /data/{file}?offset=N&filter=...
//   - #data-detail     — detail pane, patched by GET /data/select/{file}/{row}
//   - #data-error      — in-pane error surface
//
// $_dataFilter is chrome state (underscore-prefixed, doc/notes/datastar.md's
// convention) — the server reads it from the URL the client builds, never
// from a signal payload. The input writes the signal itself in its own
// handler rather than via data-bind, so the underscore name never has to
// round-trip datastar's attribute-name casing rules.
func DataBrowser() html.Content {
	filterAction := "@get('/data/' + encodeURIComponent($dataFile) + '?filter=' + encodeURIComponent($_dataFilter))"
	return PaneSection.Set("id", "pane-data-browser").
		Set("data-signals", `{_dataFilter: ''}`).
		Add(
			PaneHeading.Add(html.Text("Data Browser")),
			ErrorList.Set("id", "data-error"),
			tag.New("div.data-controls").Add(
				// data-init fetches the configured file list as soon as the
				// pane mounts — without it the pane renders empty until
				// something else happens to request /data, which nothing does.
				tag.New("div#data-files").Set("data-init", "@get('/data')"),
				tag.New("input#data-filter").
					Set("type", "search").
					Set("placeholder", "filter records…").
					Set("data-on:input__debounce.300ms", "$_dataFilter = evt.target.value; "+filterAction),
			),
			tag.New("div.data-columns").Add(
				tag.New("div.data-list").Add(
					tag.New("ul.unstyled#data-table-body"),
					// The Load More control is a SIBLING of the row list, never a
					// child: the list is an append target, so anything rendered
					// inside it gets buried under appended pages (the
					// stranded-mid-list bug). Mirrors fact_browser.go's
					// tbody/LoadMoreControl split.
					tag.New("div").Set("id", dataLoadMoreID),
				),
				DataDetail("", 0, DataDetailEmpty()),
			),
		)
}

// DataDetailEmpty is the detail pane's unselected placeholder.
func DataDetailEmpty() html.Content {
	return tag.New("p.text-light", html.Text("select a record to inspect it"))
}

// DataDetail renders the #data-detail fragment: the selected record's
// source position and its pretty-printed body (JSONTree on parse success, a
// raw <pre> fallback otherwise — package main decides which). An empty file
// renders the placeholder shell DataBrowser mounts.
func DataDetail(file string, row int, body html.Content) html.Content {
	div := tag.New("div#data-detail")
	if file == "" {
		return div.Add(body)
	}
	return div.Add(
		tag.New("p.detail-heading", html.Text(fmt.Sprintf("%s · record %d", file, row))),
		body,
	)
}

// JSONTree renders a decoded JSON value with collapsible nesting: objects
// and arrays become open <details> nodes (summary = key + size), scalars
// become key/value leaf lines. Values render via json.Marshal so strings
// stay quoted and json.Number (the caller should decode with UseNumber)
// keeps its exact source digits.
func JSONTree(v any) html.Content {
	return tag.New("div.json-tree").Add(jsonNode("", v))
}

func jsonNode(key string, v any) html.Content {
	label := html.Group{}
	if key != "" {
		label = html.Group{tag.New("span.json-key", html.Text(key)), html.Text(" ")}
	}
	switch val := v.(type) {
	case map[string]any:
		keys := make([]string, 0, len(val))
		for k := range val {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		children := make([]html.Content, len(keys))
		for i, k := range keys {
			children[i] = jsonNode(k, val[k])
		}
		return tag.New("details.json-node").Set("open", "").Add(
			tag.New("summary").Add(label, tag.New("span.json-size", html.Text(fmt.Sprintf("{%d}", len(val))))),
			tag.New("div.json-children").Add(children...),
		)
	case []any:
		children := make([]html.Content, len(val))
		for i, item := range val {
			children[i] = jsonNode(fmt.Sprintf("[%d]", i), item)
		}
		return tag.New("details.json-node").Set("open", "").Add(
			tag.New("summary").Add(label, tag.New("span.json-size", html.Text(fmt.Sprintf("[%d]", len(val))))),
			tag.New("div.json-children").Add(children...),
		)
	default:
		text, err := json.Marshal(v)
		if err != nil {
			text = []byte(fmt.Sprint(v))
		}
		return tag.New("div.json-leaf").Add(label, tag.New("span.json-value", html.Text(string(text))))
	}
}

// DataFileList renders the #data-files fragment: a <select> with one
// <option> per configured source file. Changing the selection fetches that
// file's first page of rows into #data-table-body; data-bind:data-file
// gives the change handler's action a $dataFile signal to read (see
// doc/notes/datastar.md §"data-bind" — the select's own selected option is
// canonical, so binding it is fine even though the value is server-owned).
// The browser defaults to the first <option> (files[0]), which the caller
// (handleDataList) also auto-loads so the pane isn't left empty.
//
// encodeURIComponent($dataFile) is the JS-side counterpart of pathEscape
// (used below for the row-level /data/{file}/... links, and by
// handleJSONFactsTest's confine call): GET /data/{file} is a single-segment
// route (serve.go), and a source file naming a subdirectory (e.g.
// "logs/events.jsonl") would otherwise widen the request into an extra path
// segment the route never matches, 404ing silently. Escaping keeps it one
// segment; ServeMux matches against the still-escaped path (each segment
// unescaped only for the comparison, per net/http's pattern matching) and
// r.PathValue("file") returns the decoded "logs/events.jsonl" — a "/"
// escapes to "%2F" the same way url.PathEscape and encodeURIComponent both
// treat it, so this reaches the handler intact.
// The change handler carries the current substring filter along
// ($_dataFilter, DataBrowser's chrome signal) so switching files keeps the
// filter applied, matching what the filter input itself would fetch.
func DataFileList(files []string) html.Content {
	return tag.New("div#data-files").Add(
		tag.New("select").
			Set("data-bind:data-file").
			Set("data-on:change", "@get('/data/' + encodeURIComponent($dataFile) + '?filter=' + encodeURIComponent($_dataFilter))").
			Add(
				html.Map(files, func(f string) html.Content {
					return tag.New("option").Set("value", f).Add(html.Text(f))
				}),
			),
	)
}

// DataErrors renders the #data-error fragment: an in-form error list,
// verbatim, per doc/notes/datastar.md §4.
func DataErrors(errs []string) html.Content {
	return ErrorList.Set("id", "data-error").Add(
		html.Map(errs, func(e string) html.Content {
			return tag.New("li", html.Text(e))
		})...,
	)
}

// DataRowInfo is one raw record's index and source line, passed to
// DataTableBody.
type DataRowInfo struct {
	Index int
	Raw   string
}

// dataPreviewLen is how many characters of the raw line show in the record
// list (design decision 9: "a compact record list (line number + first ~100
// chars)"); the full record lives in the detail pane.
const dataPreviewLen = 100

// DataRow renders one <li> for a raw JSONL record: the line number and a
// truncated preview of the raw line. Clicking the row selects it — loading
// the detail pane and marking it as the `!` composer command's evaluation
// target. The row carries a stable #data-row-N id so handleDataSelect can
// patch it (and any previously-selected row) in place.
func DataRow(file string, row int, raw string, selected bool) html.Content {
	preview := raw
	if runes := []rune(preview); len(runes) > dataPreviewLen {
		preview = string(runes[:dataPreviewLen]) + "…"
	}
	li := tag.New("li").
		Set("id", fmt.Sprintf("data-row-%d", row)).
		Set("data-on:click", fmt.Sprintf("@get('/data/select/%s/%d')", pathEscape(file), row)).
		Add(
			tag.New("span.line-no", html.Text(fmt.Sprintf("%d", row))),
			html.Text(preview),
		)
	if selected {
		li = li.Class("selected")
	}
	return li
}

// dataLoadMoreID is the Load More control's fixed element id. Unlike the
// Fact Browser's per predicate/arity LoadMoreID (fact_browser.go), the Data
// Browser shows exactly one file's rows at a time, so a single static id is
// enough.
const dataLoadMoreID = "data-load-more"

// DataLoadMore renders the Load More control for file's next chunk, or an
// empty (id-carrying) placeholder once hasMore is false — kept non-nil and
// id-bearing so a later patch targeting dataLoadMoreID always has an element
// to morph, even after exhaustion (mirrors fact_browser.go's
// loadMoreControl). filter is the substring filter this page was fetched
// under: the next chunk's offset counts MATCHING rows, so the control must
// re-submit the same filter or the offset would address the wrong rows.
func DataLoadMore(file string, nextOffset int, hasMore bool, filter string) html.Content {
	div := tag.New("div").Set("id", dataLoadMoreID)
	if !hasMore {
		return div
	}
	return div.Add(
		ActionButton.
			Set("data-on:click", fmt.Sprintf("@get('/data/%s?offset=%d&filter=%s')",
				pathEscape(file), nextOffset, url.QueryEscape(filter))).
			Add(html.Text("Load More")),
	)
}

// DataRows renders just the <li> row elements for an appended page
// (offset > 0). The handler patches these into the existing #data-table-body
// list via datastar.Mode("append") rather than replacing it, and separately
// patches dataLoadMoreID's control with a fresh one via DataLoadMore
// (mirrors fact_browser.go's FactRows/TbodyID/LoadMoreControl split).
// Appending the whole DataTableBody fragment, as this used to do, nested a
// duplicate #data-table-body <ul> inside itself (its own root element
// carries the same id) and left the previous, now stale-offset, Load More
// button live alongside the new one.
func DataRows(file string, rows []DataRowInfo, selFile string, selRow int, selValid bool) html.Content {
	children := make([]html.Content, len(rows))
	for i, r := range rows {
		selected := selValid && selFile == file && selRow == r.Index
		children[i] = DataRow(file, r.Index, r.Raw, selected)
	}
	return html.Group(children)
}

// DataTableBody renders the #data-table-body fragment for a chunk of rows —
// rows ONLY; the Load More control is a sibling element the handler patches
// separately via DataLoadMore (rendering it inside this ul would bury it
// under appended pages, since the ul is the append target). The handler
// chooses the Mode (replace for offset==0, append for offset>0) at the
// SSE-event level; this function only builds the HTML. selFile/selRow/
// selValid are the jsonfacts Editor's current evaluation target
// (wb.selFile/selRow/selValid) so the tested row renders highlighted even
// across a file reload or Load More.
func DataTableBody(file string, rows []DataRowInfo, selFile string, selRow int, selValid bool) html.Content {
	children := make([]html.Content, 0, len(rows))
	for _, r := range rows {
		selected := selValid && selFile == file && selRow == r.Index
		children = append(children, DataRow(file, r.Index, r.Raw, selected))
	}
	return tag.New("ul.unstyled#data-table-body").Add(children...)
}

// pathEscape escapes a file reference for embedding in a URL path segment
// within a Datastar action string. Source file names are simple relative
// paths (no query/fragment chars expected), but escaping is cheap insurance
// against any file name containing characters that would otherwise break
// the action string or the resulting request path.
func pathEscape(s string) string {
	return url.PathEscape(s)
}
