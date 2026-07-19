package main

import (
	"fmt"
	"net/http"
	"strconv"

	html "github.com/swdunlop/html-go"
	"github.com/swdunlop/html-go/datastar"
	"github.com/swdunlop/html-go/tag"
	"swdunlop.dev/pkg/datalog"
	"swdunlop.dev/pkg/datalog/cmd/datalog/view"
	"swdunlop.dev/pkg/datalog/jsonfacts"
	"swdunlop.dev/pkg/datalog/memory"
)

// This file builds the Schema and Rules tab panels (doc/features/
// workbench-v2.md phase 3, design decision 9): package main converts engine
// state (jsonfacts.Config, the rule store) into view's plain Info structs.
// Both panels render from session state under wb.h.mu — at page load
// (renderConversationPage) and again on every mutation/reload
// (publishSessionChanged patches the same stable ids).

// renderSchemaPanel builds the #schema-panel fragment from the AUTHORING
// config — the same form and ordering get_config returns
// (sortConfigForSerialization), so the tab always shows what a re-read of
// the schema file would. Callers must hold wb.h.mu.
func renderSchemaPanel(cfg jsonfacts.Config) html.Content {
	sorted := sortConfigForSerialization(cfg)

	sources := make([]view.SchemaSourceInfo, len(sorted.Sources))
	for i, s := range sorted.Sources {
		mappings := make([]view.SchemaMappingInfo, len(s.Mappings))
		for j, m := range s.Mappings {
			mappings[j] = view.SchemaMappingInfo{
				Predicate: m.Predicate, Args: m.Args, Filter: m.Filter, Expr: m.Expr,
			}
		}
		sources[i] = view.SchemaSourceInfo{File: s.File, Mappings: mappings}
	}

	matchers := make([]view.SchemaMatcherInfo, len(sorted.Matchers))
	for i, m := range sorted.Matchers {
		matchers[i] = schemaMatcherInfo(m)
	}

	decls := make([]view.SchemaDeclInfo, len(sorted.Declarations))
	for i, d := range sorted.Declarations {
		decls[i] = view.SchemaDeclInfo{Name: d.Name, Terms: declTermNames(d), Use: d.Use}
	}

	return view.SchemaPanel(sources, matchers, decls)
}

// schemaMatcherInfo converts one matcher, pairing each present pattern kind
// with the arity-2 predicate it emits. The kind order here MUST mirror
// jsonfacts.Matcher.ProducedPredicates — that method returns one produced
// name per present kind, in this same fixed order, and the zip below leans
// on that alignment. An unresolved *_from reference (possible in the
// authoring form) renders as a "(from <file>)" pseudo-pattern so the tab
// still shows where the patterns come from.
func schemaMatcherInfo(m jsonfacts.Matcher) view.SchemaMatcherInfo {
	info := view.SchemaMatcherInfo{Predicate: m.Predicate, Term: m.Term}
	if m.CaseInsensitive {
		info.Flags = append(info.Flags, "case-insensitive")
	}
	if m.Windash {
		info.Flags = append(info.Flags, "windash")
	}

	produced := m.ProducedPredicates()
	idx := 0
	addKind := func(kind string, patterns []string, from string) {
		if len(patterns) == 0 && from == "" {
			return
		}
		if idx >= len(produced) { // defensive: alignment drift must not panic the page
			return
		}
		p := view.SchemaPatternsInfo{Kind: kind, Produces: produced[idx], Patterns: patterns}
		if from != "" {
			p.Patterns = append([]string{"(from " + from + ")"}, patterns...)
		}
		info.Patterns = append(info.Patterns, p)
		idx++
	}
	addKind("contains", m.Contains, m.ContainsFrom)
	addKind("starts_with", m.StartsWith, m.StartsWithFrom)
	addKind("ends_with", m.EndsWith, m.EndsWithFrom)
	addKind("regex_match", m.RegexMatch, m.RegexMatchFrom)
	addKind("base64", m.Base64, m.Base64From)
	addKind("base64_utf16le", m.Base64UTF16, m.Base64UTF16From)
	addKind("cidr", m.CIDR, m.CIDRFrom)
	return info
}

// declTermNames renders a declaration's term list for display: the declared
// name (with its type constraint when one is set), "_" for anonymous terms,
// each carrying its own Use doc for the tooltip.
func declTermNames(d datalog.Declaration) []view.SchemaDeclTerm {
	out := make([]view.SchemaDeclTerm, len(d.Terms))
	for i, t := range d.Terms {
		name := t.Name
		if name == "" {
			name = "_"
		}
		if t.Type != datalog.TermAny {
			name += ":" + string(t.Type)
		}
		out[i] = view.SchemaDeclTerm{Label: name, Use: t.Use}
	}
	return out
}

// renderBrowserPanels builds the Schema and Rules tab contents from current
// session state — the one place both the full-page render
// (renderConversationPage) and the change fan-out (publishSessionChanged)
// get them, so the two can never drift. Callers must hold wb.h.mu.
func (wb *workbench) renderBrowserPanels() (schema, rules html.Content) {
	schema = renderSchemaPanel(wb.h.sess.authoringCfg)
	if wb.h.rules == nil {
		rules = legacyRulesPanel(wb.h.sess.rulesText)
	} else {
		rules = wb.renderRulesPanel()
	}
	return schema, rules
}

// legacyRulesPanel is the Rules tab for a session without a rules/
// directory store (legacy positional files): the concatenated ruleset text
// verbatim, as phase 2 showed it — the group master-detail needs the
// store's per-group split.
func legacyRulesPanel(rulesText string) html.Content {
	root := tag.New("div#rules-panel")
	if rulesText == "" {
		return root.Add(tag.New("p.text-light", html.Text("no rules loaded")))
	}
	return root.Add(tag.New("pre.doc", html.Text(rulesText)))
}

// renderRulesPanel builds the #rules-panel group list from the rule store,
// in filename order (the same ordering list_rule_groups returns). Callers
// must hold wb.h.mu and have checked wb.h.rules != nil.
func (wb *workbench) renderRulesPanel() html.Content {
	counts := predicateFactCounts(wb.h.sess)
	items := make([]view.RuleGroupItem, 0, len(wb.h.rules.Order))
	for _, k := range wb.h.rules.Order {
		g := wb.h.rules.Groups[k]
		items = append(items, view.RuleGroupItem{
			Head:       k.Head,
			Arity:      k.Arity,
			File:       g.File,
			Statements: len(g.Rules) + len(g.AggRules),
			Facts:      counts[predArity{k.Head, k.Arity}],
		})
	}
	return view.RulesPanel(items)
}

// predArity keys predicateFactCounts.
type predArity struct {
	name  string
	arity int
}

// predicateFactCounts returns the evaluated database's per-predicate fact
// counts, or an empty map when evaluation fails (counts then show as 0 —
// the Rules tab must render even over a broken ruleset). Callers must hold
// wb.h.mu.
func predicateFactCounts(sess *session) map[predArity]int {
	counts := map[predArity]int{}
	db, err := sess.evaluatedDB()
	if err != nil {
		return counts
	}
	if mdb, ok := db.(*memory.Database); ok {
		for pa, n := range mdb.PredicateCounts() {
			counts[predArity{pa.Name, pa.Arity}] = n
		}
	}
	return counts
}

// handleRuleGroup (GET /rules/{head}/{arity}) loads one group's detail into
// the Rules tab's #rule-group-detail pane: header (file, revision, head
// fact count), each statement's %% doc rendered as prose above its
// syntax-highlighted source, and dependency links both directions — body
// predicates this group uses, and the groups/matchers that consume its
// head (predicateDeps' DependedOnBy edges, given a face per design
// decision 9).
func (wb *workbench) handleRuleGroup(w http.ResponseWriter, r *http.Request) {
	stream, err := datastar.RequestStream(w, r)
	if err != nil {
		return
	}
	head := r.PathValue("head")
	arity, arityErr := strconv.Atoi(r.PathValue("arity"))
	if arityErr != nil || arity < 0 {
		_ = stream.Emit(datastar.Elements(view.RuleGroupDetailError("invalid arity in request path")))
		return
	}

	// predicateDeps takes h.mu itself; call it before the lock below. The
	// group could change between the two acquisitions, but both reads are
	// of current state and the pane is read-only — worst case a reload
	// racing this click renders one section a hair fresher than the other.
	deps, depsErr := wb.h.predicateDeps(predicateDepsInput{Predicate: head, Arity: arity})

	wb.h.mu.Lock()
	if wb.h.rules == nil {
		wb.h.mu.Unlock()
		_ = stream.Emit(datastar.Elements(view.RuleGroupDetailError("this session has no rules directory store")))
		return
	}
	g, ok := wb.h.rules.Groups[groupKey{Head: head, Arity: arity}]
	if !ok {
		wb.h.mu.Unlock()
		_ = stream.Emit(datastar.Elements(view.RuleGroupDetailError(fmt.Sprintf(
			"no rule group %s/%d — it may have been deleted or renamed; the list refreshes on the next change", head, arity))))
		return
	}

	info := view.RuleGroupDetailInfo{
		Head:     head,
		Arity:    arity,
		File:     g.File,
		Revision: g.Revision,
		Facts:    predicateFactCounts(wb.h.sess)[predArity{head, arity}],
	}
	for _, rule := range g.Rules {
		stmt := rule // strip the doc from the source text; it renders as prose
		stmt.Doc = ""
		info.Statements = append(info.Statements, view.RuleStatementInfo{Doc: rule.Doc, Text: stmt.String()})
	}
	for _, ar := range g.AggRules {
		stmt := ar
		stmt.Doc = ""
		info.Statements = append(info.Statements, view.RuleStatementInfo{Doc: ar.Doc, Text: stmt.String()})
	}
	info.Uses = wb.ruleGroupUses(g)
	if depsErr == nil {
		for _, addr := range deps.DependedOnBy {
			_, isGroup := wb.h.rules.Groups[groupKey{Head: addr.Head, Arity: addr.Arity}]
			info.UsedBy = append(info.UsedBy, view.RuleDepLink{Head: addr.Head, Arity: addr.Arity, IsGroup: isGroup})
		}
		for _, m := range deps.DependedOnByMatchers {
			info.ReadByMatchers = append(info.ReadByMatchers, fmt.Sprintf("matcher on term %d", m.Term))
		}
	}
	wb.h.mu.Unlock()

	_ = stream.Emit(datastar.Elements(view.RuleGroupDetail(info)))
}

// ruleGroupUses collects the distinct body predicates of g's statements —
// the "uses" direction of the detail's dependency links — in first-seen
// order, each marked IsGroup when a rule group defines it (the link then
// navigates to that group instead of the Facts tab). Callers must hold
// wb.h.mu.
func (wb *workbench) ruleGroupUses(g *ruleStoreGroup) []view.RuleDepLink {
	var out []view.RuleDepLink
	seen := map[groupKey]bool{}
	add := func(pred string, arity int) {
		k := groupKey{Head: pred, Arity: arity}
		if seen[k] {
			return
		}
		seen[k] = true
		_, isGroup := wb.h.rules.Groups[k]
		out = append(out, view.RuleDepLink{Head: pred, Arity: arity, IsGroup: isGroup})
	}
	for _, rule := range g.Rules {
		for _, atom := range rule.Body {
			add(atom.Pred, len(atom.Terms))
		}
	}
	for _, ar := range g.AggRules {
		for _, atom := range ar.Body {
			add(atom.Pred, len(atom.Terms))
		}
	}
	return out
}
