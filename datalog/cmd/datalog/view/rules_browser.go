package view

import (
	"fmt"
	"strings"

	html "github.com/swdunlop/html-go"
	"github.com/swdunlop/html-go/tag"
)

// This file renders the Rules tab (doc/features/workbench-v2.md design
// decision 9): a rule-group list on the left and a group detail on the
// right — syntax-highlighted rule text, rendered %% docs, head fact count,
// and dependency links in both directions. Package main supplies the Info
// structs (engine types stay out of view, as everywhere in this package).

// RuleGroupItem is one row of the group list.
type RuleGroupItem struct {
	Head       string
	Arity      int
	File       string
	Statements int
	Facts      int
}

// RulesPanel renders the #rules-panel fragment: the group list and the
// (initially empty) detail pane. It carries a stable id so
// publishSessionChanged can re-patch it after agent CRUD or an fsnotify
// reload; the patch resets the detail pane too, which is correct — a
// mutation may have changed or deleted the group it showed.
func RulesPanel(groups []RuleGroupItem) html.Content {
	root := tag.New("div#rules-panel")
	if len(groups) == 0 {
		return root.Add(tag.New("p.text-light", html.Text("no rule groups loaded")))
	}
	return root.Add(
		tag.New("div.rules-columns").Add(
			tag.New("div#rule-groups").Add(html.Map(groups, ruleGroupRow)...),
			tag.New("div#rule-group-detail").Add(
				tag.New("p.text-light", html.Text("select a rule group")),
			),
		),
	)
}

func ruleGroupRow(g RuleGroupItem) html.Content {
	return tag.New("div.predicate-row").
		Set("data-on:click", fmt.Sprintf("@get('/rules/%s/%d')", g.Head, g.Arity)).
		Add(
			html.Text(fmt.Sprintf("%s/%d", g.Head, g.Arity)),
			tag.New("span.count", html.Text(fmt.Sprintf(" %d stmt · %d facts", g.Statements, g.Facts))),
			tag.New("div.rule-group-file", tag.New("code", html.Text(g.File))),
		)
}

// RuleStatementInfo is one statement in a group's detail: its %% doc (may
// be empty) and its rule text WITHOUT the doc lines — the doc renders as
// prose above the highlighted source.
type RuleStatementInfo struct {
	Doc  string
	Text string
}

// RuleDepLink is one dependency edge in a group detail. IsGroup means the
// target predicate has its own rule group — the link loads that group's
// detail in place; otherwise it flips to the Facts tab (a base predicate).
type RuleDepLink struct {
	Head    string
	Arity   int
	IsGroup bool
}

// RuleGroupDetailInfo is everything one group's detail pane shows.
type RuleGroupDetailInfo struct {
	Head           string
	Arity          int
	File           string
	Revision       int
	Facts          int
	Statements     []RuleStatementInfo
	Uses           []RuleDepLink // body predicates of this group's own statements
	UsedBy         []RuleDepLink // groups whose bodies reference this head
	ReadByMatchers []string      // e.g. "matcher on term 1" — schema-side consumers
}

// RuleGroupDetail renders the #rule-group-detail fragment.
func RuleGroupDetail(info RuleGroupDetailInfo) html.Content {
	root := tag.New("div#rule-group-detail").Add(
		tag.New("div.rule-detail-head").Add(
			tag.New("h3", html.Text(fmt.Sprintf("%s/%d", info.Head, info.Arity))),
			tag.New("span.count", html.Text(fmt.Sprintf("%d facts", info.Facts))),
			tag.New("code.rule-detail-file", html.Text(fmt.Sprintf("%s · rev %d", info.File, info.Revision))),
		),
	)
	for _, s := range info.Statements {
		stmt := tag.New("div.rule-statement")
		if s.Doc != "" {
			stmt = stmt.Add(tag.New("p.rule-doc", html.Text(s.Doc)))
		}
		stmt = stmt.Add(tag.New("pre.rule-text").Add(HighlightDatalog(s.Text)))
		root = root.Add(stmt)
	}
	root = root.Add(
		ruleDepSection("uses", info.Uses, nil),
		ruleDepSection("used by", info.UsedBy, info.ReadByMatchers),
	)
	return root
}

// RuleGroupDetailError renders a detail-pane error (group vanished between
// the list render and the click — a reload or delete won the race).
func RuleGroupDetailError(msg string) html.Content {
	return tag.New("div#rule-group-detail").Add(
		tag.New("ul.errors", tag.New("li", html.Text(msg))),
	)
}

func ruleDepSection(label string, links []RuleDepLink, matchers []string) html.Content {
	if len(links) == 0 && len(matchers) == 0 {
		return html.Group{}
	}
	sec := tag.New("div.rule-deps").Add(tag.New("span.rule-deps-label", html.Text(label+": ")))
	first := true
	for _, l := range links {
		if !first {
			sec = sec.Add(html.Text(", "))
		}
		first = false
		sec = sec.Add(ruleDepLink(l))
	}
	for _, m := range matchers {
		if !first {
			sec = sec.Add(html.Text(", "))
		}
		first = false
		sec = sec.Add(SchemaLink(m))
	}
	return sec
}

func ruleDepLink(l RuleDepLink) html.Content {
	label := fmt.Sprintf("%s/%d", l.Head, l.Arity)
	if l.IsGroup {
		return RulesLink(l.Head, l.Arity, label)
	}
	return FactsLink(l.Head, l.Arity, label)
}

// HighlightDatalog renders one rule statement's source text with light
// syntax highlighting: comments, strings, numbers, variables (capitalized
// identifiers), predicate names (identifier followed by an open paren), and
// the rule operator. It is a display-only lexer over text — it must never
// fail, so anything it doesn't recognize passes through as plain text.
func HighlightDatalog(text string) html.Content {
	var out []html.Content
	plain := strings.Builder{}
	flush := func() {
		if plain.Len() > 0 {
			out = append(out, html.Text(plain.String()))
			plain.Reset()
		}
	}
	span := func(class, s string) {
		flush()
		out = append(out, tag.New("span."+class, html.Text(s)))
	}

	runes := []rune(text)
	for i := 0; i < len(runes); {
		r := runes[i]
		switch {
		case r == '%': // comment (or %% doc) to end of line
			j := i
			for j < len(runes) && runes[j] != '\n' {
				j++
			}
			span("dl-comment", string(runes[i:j]))
			i = j
		case r == '"':
			j := i + 1
			for j < len(runes) {
				if runes[j] == '\\' && j+1 < len(runes) {
					j += 2
					continue
				}
				if runes[j] == '"' {
					j++
					break
				}
				j++
			}
			span("dl-str", string(runes[i:j]))
			i = j
		case r == ':' && i+1 < len(runes) && runes[i+1] == '-':
			span("dl-op", ":-")
			i += 2
		case r >= '0' && r <= '9':
			j := i
			for j < len(runes) && (runes[j] >= '0' && runes[j] <= '9' || runes[j] == '.' || runes[j] == 'e' || runes[j] == 'E' || runes[j] == '-' && j > i) {
				j++
			}
			span("dl-num", string(runes[i:j]))
			i = j
		case isIdentStart(r):
			j := i
			for j < len(runes) && isIdentRune(runes[j]) {
				j++
			}
			word := string(runes[i:j])
			switch {
			case r >= 'A' && r <= 'Z' || r == '_':
				span("dl-var", word)
			case word == "not":
				span("dl-kw", word)
			default:
				// Predicate position: identifier followed (past spaces) by '('.
				k := j
				for k < len(runes) && runes[k] == ' ' {
					k++
				}
				if k < len(runes) && runes[k] == '(' {
					span("dl-pred", word)
				} else {
					plain.WriteString(word)
				}
			}
			i = j
		default:
			plain.WriteRune(r)
			i++
		}
	}
	flush()
	return html.Group(out)
}

func isIdentStart(r rune) bool {
	return r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r == '_'
}

func isIdentRune(r rune) bool {
	return isIdentStart(r) || r >= '0' && r <= '9'
}
