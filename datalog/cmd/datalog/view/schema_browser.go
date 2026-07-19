package view

import (
	"fmt"
	"strings"

	html "github.com/swdunlop/html-go"
	"github.com/swdunlop/html-go/tag"
)

// This file renders the Schema tab (doc/features/workbench-v2.md design
// decision 9): sources, matchers, and declarations structurally — no YAML —
// with `use` docs shown and the predicates each item produces rendered as
// links into the Facts tab. Package main converts engine types
// (jsonfacts.Config, datalog.Declaration) into the plain Info structs here,
// keeping view engine-type-agnostic like the rest of this package.

// SchemaMappingInfo is one source mapping: either simple mode
// (Predicate/Args/Filter) or imperative mode (Expr).
type SchemaMappingInfo struct {
	Predicate string
	Args      []string
	Filter    string
	Expr      string
}

// SchemaSourceInfo is one schema source: a JSONL file and its mappings.
type SchemaSourceInfo struct {
	File     string
	Mappings []SchemaMappingInfo
}

// SchemaPatternsInfo is one matcher pattern kind (e.g. "contains") with its
// patterns and the arity-2 predicate that kind emits.
type SchemaPatternsInfo struct {
	Kind     string
	Produces string // the arity-2 match-kind predicate, e.g. "contains"
	Patterns []string
}

// SchemaMatcherInfo is one matcher: the (predicate, term) it reads, its
// flags, and its pattern kinds. Arity is the read predicate's arity when
// package main could resolve it from the config's own mappings/declarations
// (the matcher itself doesn't carry one), or 0 when unknown/ambiguous — the
// read link then falls back to a plain Facts-tab flip.
type SchemaMatcherInfo struct {
	Predicate string
	Term      int
	Arity     int
	Flags     []string // e.g. "case-insensitive", "windash"
	Patterns  []SchemaPatternsInfo
}

// SchemaDeclTerm is one declared term: its display label (name plus type
// constraint, "_" when anonymous) and its own Use doc, shown as a tooltip.
type SchemaDeclTerm struct {
	Label string
	Use   string
}

// SchemaDeclInfo is one declaration: predicate name, terms, and the Use doc.
type SchemaDeclInfo struct {
	Name  string
	Terms []SchemaDeclTerm
	Use   string
}

// SchemaPanel renders the #schema-panel fragment: the whole authoring
// schema, structurally. It carries a stable id so publishSessionChanged can
// re-patch it after agent CRUD or an fsnotify reload.
func SchemaPanel(sources []SchemaSourceInfo, matchers []SchemaMatcherInfo, decls []SchemaDeclInfo) html.Content {
	root := tag.New("div#schema-panel")
	if len(sources) == 0 && len(matchers) == 0 && len(decls) == 0 {
		return root.Add(tag.New("p.text-light", html.Text("no schema loaded")))
	}
	return root.Add(
		When(len(sources) > 0, schemaSourcesSection(sources)),
		When(len(matchers) > 0, schemaMatchersSection(matchers)),
		When(len(decls) > 0, schemaDeclsSection(decls)),
	)
}

func schemaSourcesSection(sources []SchemaSourceInfo) html.Content {
	return tag.New("section.schema-section").Add(
		tag.New("h3", html.Text("Sources")),
		html.Map(sources, func(s SchemaSourceInfo) html.Content {
			return tag.New("div.schema-item").Add(
				tag.New("div.schema-item-head").Add(tag.New("code", html.Text(s.File))),
				tag.New("ul.schema-mappings").Add(html.Map(s.Mappings, schemaMapping)...),
			)
		}),
	)
}

func schemaMapping(m SchemaMappingInfo) html.Content {
	if m.Expr != "" {
		return tag.New("li.schema-mapping").Add(
			tag.New("span.schema-arrow", html.Text("expr ")),
			tag.New("code", html.Text(m.Expr)),
		)
	}
	li := tag.New("li.schema-mapping").Add(
		tag.New("span.schema-arrow", html.Text("→ ")),
		FactsLink(m.Predicate, len(m.Args), m.Predicate),
		tag.New("code", html.Text("("+strings.Join(m.Args, ", ")+")")),
	)
	if m.Filter != "" {
		li = li.Add(
			tag.New("span.schema-filter-label", html.Text(" when ")),
			tag.New("code", html.Text(m.Filter)),
		)
	}
	return li
}

func schemaMatchersSection(matchers []SchemaMatcherInfo) html.Content {
	return tag.New("section.schema-section").Add(
		tag.New("h3", html.Text("Matchers")),
		html.Map(matchers, schemaMatcher),
	)
}

// schemaPatternInlineMax is how many patterns of one kind render inline;
// longer lists collapse into a <details> so an IOC feed with hundreds of
// patterns doesn't swamp the tab.
const schemaPatternInlineMax = 8

func schemaMatcher(m SchemaMatcherInfo) html.Content {
	readLink := PredicateLink(m.Predicate)
	if m.Arity > 0 {
		readLink = FactsLink(m.Predicate, m.Arity, m.Predicate)
	}
	head := tag.New("div.schema-item-head").Add(
		html.Text("reads "),
		readLink,
		tag.New("code", html.Text(fmt.Sprintf(" term %d", m.Term))),
	)
	for _, f := range m.Flags {
		head = head.Add(tag.New("span.badge", html.Text(f)))
	}
	item := tag.New("div.schema-item").Add(head)
	for _, p := range m.Patterns {
		line := tag.New("div.schema-patterns").Add(
			tag.New("span.schema-arrow", html.Text("→ ")),
			FactsLink(p.Produces, 2, p.Produces),
			html.Text(fmt.Sprintf("/2 · %s (%d): ", p.Kind, len(p.Patterns))),
		)
		if len(p.Patterns) <= schemaPatternInlineMax {
			line = line.Add(patternCodes(p.Patterns))
		} else {
			line = line.Add(tag.New("details").Add(
				tag.New("summary", html.Text("show patterns")),
				patternCodes(p.Patterns),
			))
		}
		item = item.Add(line)
	}
	return item
}

func patternCodes(patterns []string) html.Content {
	out := make([]html.Content, 0, len(patterns)*2)
	for i, p := range patterns {
		if i > 0 {
			out = append(out, html.Text(" "))
		}
		out = append(out, tag.New("code.schema-pattern", html.Text(p)))
	}
	return html.Group(out)
}

func schemaDeclsSection(decls []SchemaDeclInfo) html.Content {
	return tag.New("section.schema-section").Add(
		tag.New("h3", html.Text("Declarations")),
		html.Map(decls, func(d SchemaDeclInfo) html.Content {
			terms := make([]html.Content, 0, len(d.Terms)*2+2)
			terms = append(terms, html.Text("("))
			for i, t := range d.Terms {
				if i > 0 {
					terms = append(terms, html.Text(", "))
				}
				span := tag.New("span", html.Text(t.Label))
				if t.Use != "" {
					span = span.Set("title", t.Use)
				}
				terms = append(terms, span)
			}
			terms = append(terms, html.Text(")"))
			item := tag.New("div.schema-item").Add(
				tag.New("div.schema-item-head").Add(
					FactsLink(d.Name, len(d.Terms), d.Name),
					tag.New("code").Add(terms...),
				),
			)
			if d.Use != "" {
				item = item.Add(tag.New("p.predicate-use", html.Text(d.Use)))
			}
			return item
		}),
	)
}

// PredicateLink renders a predicate name as a plain Facts-tab flip — the
// arity-unknown fallback for surfaces that cannot resolve one (a matcher
// reading a predicate no mapping or declaration names). Everything that
// knows the arity uses FactsLink (cross_links.go) instead, which also loads
// and scrolls to the predicate's facts.
func PredicateLink(name string) html.Content {
	return tag.New("a.pred-link").
		Set("data-on:click", "$_browserTab = 'facts'").
		Add(html.Text(name))
}
