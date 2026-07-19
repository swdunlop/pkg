package main

import (
	"encoding/json"
	"strconv"

	html "github.com/swdunlop/html-go"
	"github.com/swdunlop/html-go/tag"
	"swdunlop.dev/pkg/datalog/cmd/datalog/view"
	"swdunlop.dev/pkg/datalog/syntax"
)

// This file builds transcript→browser cross-links (doc/features/
// workbench-v2.md phase 4, design decision 2: "the conversation is the
// index into the browser"): the predicates a query read, the rule group an
// agent edit touched, the fact an explain call resolved — each rendered as
// a view.FactsLink/RulesLink row under the transcript entry it annotates.
// Everything here is best-effort display: a parse failure or an unexpected
// argument shape renders nothing, never an error, because the entry it
// decorates already carries the authoritative text.

// linkRow wraps a labeled series of links as one .result-links line, the
// shared shape for every transcript link row. Returns an empty group when
// there are no links, so call sites can append unconditionally.
func linkRow(label string, links []html.Content) html.Content {
	if len(links) == 0 {
		return html.Group{}
	}
	row := tag.New("div.result-links").Add(
		tag.New("span.result-links-label", html.Text(label+": ")))
	for i, l := range links {
		if i > 0 {
			row = row.Add(html.Text(", "))
		}
		row = row.Add(l)
	}
	return row
}

// isPredicateIdent reports whether name is a plain datalog identifier —
// the load-bearing gate for splicing parsed user text into a link's click
// expression (view.FactsLink's splice-safety contract). Comparison atoms
// ("<", "=="), is-expressions, and anything else the parser admits as an
// atom head that isn't an identifier fail here and get no link.
func isPredicateIdent(name string) bool {
	if name == "" {
		return false
	}
	for i, r := range name {
		switch {
		case r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r == '_':
		case r >= '0' && r <= '9':
			if i == 0 {
				return false
			}
		default:
			return false
		}
	}
	return true
}

// atomPredicateLinks collects the distinct linkable predicates of a query
// body, in first-seen order, as Facts-tab deep links.
func atomPredicateLinks(atoms []syntax.Atom) []html.Content {
	type key struct {
		name  string
		arity int
	}
	seen := map[key]bool{}
	var out []html.Content
	for _, a := range atoms {
		if a.Pred == "is" || a.Expr != nil || !isPredicateIdent(a.Pred) {
			continue
		}
		k := key{a.Pred, a.Arity()}
		if seen[k] {
			continue
		}
		seen[k] = true
		out = append(out, view.FactsLink(a.Pred, a.Arity(), a.Pred))
	}
	return out
}

// queryPredicateLinks parses queryText best-effort and renders the
// predicates its queries read as a Facts-tab link row — used where only the
// query's text survives (an agent query tool call's arguments); the
// composer's `?` command links from its already-parsed queries instead.
func queryPredicateLinks(queryText string) html.Content {
	ruleset, err := parseUserProgram(queryText)
	if err != nil {
		return html.Group{}
	}
	var atoms []syntax.Atom
	for _, q := range ruleset.Queries {
		atoms = append(atoms, q.Body...)
	}
	return linkRow("predicates", atomPredicateLinks(atoms))
}

// toolBrowserLink renders the browser cross-link for one agent tool call,
// keyed by tool name: rule-group CRUD links its group's Rules-tab detail,
// schema CRUD flips to the Schema tab, explain links the explained fact's
// predicate. Tools with no browser-side referent (and unparseable
// arguments) render nothing. Used by both live tool entries (toolEntry) and
// permission cards (permissionEntry/permissionResolvedEntry), so an
// approved edit and its transcript record link the same way.
func toolBrowserLink(name, args string) html.Content {
	switch name {
	case "put_rule_group", "get_rule_group":
		var in struct {
			Head  string `json:"head"`
			Arity int    `json:"arity"`
		}
		if json.Unmarshal([]byte(args), &in) != nil ||
			!isPredicateIdent(in.Head) || in.Arity < 0 {
			return html.Group{}
		}
		return linkRow("rule group", []html.Content{
			view.RulesLink(in.Head, in.Arity, ruleGroupLabel(in.Head, in.Arity)),
		})
	case "put_source", "delete_source", "put_matcher", "delete_matcher",
		"put_declaration", "delete_declaration":
		return linkRow("schema", []html.Content{view.SchemaLink("view schema")})
	case "explain", "explain_fact":
		var in struct {
			Fact string `json:"fact"`
		}
		if json.Unmarshal([]byte(args), &in) != nil {
			return html.Group{}
		}
		fact, err := parseFactStatement(in.Fact)
		if err != nil || !isPredicateIdent(fact.Name) {
			return html.Group{}
		}
		return linkRow("explains", []html.Content{
			view.FactsLink(fact.Name, len(fact.Terms), fact.Name),
		})
	}
	return html.Group{}
}

func ruleGroupLabel(head string, arity int) string {
	return head + "/" + strconv.Itoa(arity)
}
