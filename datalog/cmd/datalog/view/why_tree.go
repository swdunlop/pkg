package view

import (
	"fmt"
	"strings"

	html "github.com/swdunlop/html-go"
	"github.com/swdunlop/html-go/tag"
)

// This file renders the Facts tab's why? expansion structurally
// (doc/features/workbench-v2.md phase 4, design decision 9: "a
// per-derived-fact 'why?' expansion backed by provenance/explain_fact").
// Package main converts seminaive.Derivation into WhyNode (view stays
// engine-type-agnostic, like every other Info struct here); this replaces
// the phase-3 <pre> box-drawing text with nested <details> the reader can
// collapse, rule text highlighted like the Rules tab, and dependency links:
// each derived step links to its rule group, each base fact to its
// predicate's facts.

// WhyNode is one step of a derivation tree.
type WhyNode struct {
	Fact  string // the resolved fact rendered as a literal, e.g. concern("ws01", 87)
	Pred  string
	Arity int

	Base      bool // no witness: asserted or loaded, nothing further to explain
	Repeated  bool // already rendered fully earlier in this same tree
	Truncated bool // depth/node cap reached; the derivation continues unseen

	Aggregate  bool // an aggregate head; Body holds sampled contributor tuples
	GroupCount int
	Sampled    bool

	Rule        string // the deriving rule's text, doc excluded (empty for base/repeated)
	Doc         string // the deriving rule's %% doc
	RuleIsGroup bool   // a rule group defines Pred/Arity — link to its Rules-tab detail

	Detail []string // ground constraint/builtin/negation lines
	Body   []WhyNode
}

// WhyTree renders a why? result for WhyOutput: the queried fact as a
// heading and the full derivation as nested open <details> nodes.
func WhyTree(fact string, root WhyNode) html.Content {
	return html.Group{
		tag.New("p.query-text", html.Text("why: "+fact)),
		tag.New("div.why-tree", whyNode(root)),
	}
}

func whyNode(n WhyNode) html.Content {
	if n.Base || n.Repeated || n.Truncated || (n.Rule == "" && !n.Aggregate) {
		return whyLeaf(n)
	}

	body := tag.New("div.why-body")
	if n.Doc != "" {
		body = body.Add(tag.New("p.rule-doc", html.Text(n.Doc)))
	}
	if n.Rule != "" {
		ruleLine := tag.New("div.why-rule").Add(tag.New("pre.rule-text").Add(HighlightDatalog(n.Rule)))
		if n.RuleIsGroup {
			ruleLine = ruleLine.Add(RulesLink(n.Pred, n.Arity, "open rule group"))
		}
		body = body.Add(ruleLine)
	}
	body = body.Add(whyDetailLines(n.Detail))
	if n.Aggregate {
		for i, s := range n.Body {
			sample := tag.New("div.why-sample").Add(
				tag.New("span.why-sample-label", html.Text(fmt.Sprintf("contributor %d:", i+1))),
				whyDetailLines(s.Detail),
			)
			for _, b := range s.Body {
				sample = sample.Add(whyNode(b))
			}
			body = body.Add(sample)
		}
	} else {
		for _, b := range n.Body {
			body = body.Add(whyNode(b))
		}
	}

	return tag.New("details.why-node").Set("open", "").Add(
		tag.New("summary").Add(
			tag.New("code", html.Text(n.Fact)),
			whyBadge(n),
		),
		body,
	)
}

// whyLeaf renders a node with nothing beneath it: base facts, repeated
// back-references, truncation markers, and (defensively) any node with no
// rule. The predicate name is a Facts-tab deep link — the tree's own
// dependency navigation down to the evidence.
func whyLeaf(n WhyNode) html.Content {
	line := tag.New("div.why-leaf")
	if rest, ok := strings.CutPrefix(n.Fact, n.Pred); ok && n.Pred != "" {
		line = line.Add(
			FactsLink(n.Pred, n.Arity, n.Pred),
			tag.New("code", html.Text(rest)),
		)
	} else {
		line = line.Add(tag.New("code", html.Text(n.Fact)))
	}
	return line.Add(whyBadge(n))
}

func whyBadge(n WhyNode) html.Content {
	switch {
	case n.Base:
		return tag.New("span.badge", html.Text("base"))
	case n.Repeated:
		return tag.New("span.badge", html.Text("shown above"))
	case n.Truncated:
		return tag.New("span.badge", html.Text("truncated"))
	case n.Aggregate:
		text := fmt.Sprintf("aggregated over %d solutions", n.GroupCount)
		if n.Sampled {
			text += ", sampled"
		}
		return tag.New("span.badge", html.Text(text))
	}
	return html.Group{}
}

func whyDetailLines(detail []string) html.Content {
	if len(detail) == 0 {
		return html.Group{}
	}
	out := make(html.Group, 0, len(detail))
	for _, d := range detail {
		out = append(out, tag.New("div.why-detail", tag.New("code", html.Text(d))))
	}
	return out
}
