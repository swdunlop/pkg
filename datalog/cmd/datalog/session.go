package main

import (
	"archive/zip"
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
	"swdunlop.dev/pkg/datalog"
	"swdunlop.dev/pkg/datalog/jsonfacts"
	"swdunlop.dev/pkg/datalog/memory"
	"swdunlop.dev/pkg/datalog/seminaive"
	"swdunlop.dev/pkg/datalog/syntax"
)

// session holds one evaluation context: a data source, loaded base facts,
// and accumulated rules. It is the engine-facing core shared by the REPL
// and the MCP server; it never touches a terminal. Methods return errors
// and data, never write to an io.Writer.
type session struct {
	facts    []datalog.Fact
	rules    []syntax.Rule
	aggRules []syntax.AggregateRule

	// engineOpts is threaded into every seminaive.Engine this session's
	// methods build (setRules/setRulesWithQueries's trial Compile,
	// evaluate/evaluateSnapshot, runQuery's two stages). newMCPHandlers
	// (mcp.go) sets it once at construction to
	// []seminaive.Option{seminaive.WithFactLimit(factCap)} (sandbox.go), so
	// every Transform this session ever runs — including both of a query's
	// stages — halts mid-evaluation once it derives more than factCap new
	// facts, rather than materializing an unbounded result first.
	engineOpts []seminaive.Option

	// Data source configured via setDataSource.
	configPath string
	dataDir    string
	cfg        jsonfacts.Config
	dataDB     *memory.Database // facts loaded from data source (replaced on reload)

	// authoringCfg is the AUTHORING-form config: parsed from the same schema
	// text as cfg but NOT run through ResolveFromFS, so every matcher's
	// *_from field still names its pattern file instead of having that
	// file's patterns merged inline and the field cleared (resolveFromFS
	// does both, mutating the matcher in place). cfg above is the RESOLVED
	// runtime form — the one LoadFS and single-row extraction
	// (jsonfacts_editor.go's extractRecord) must use, since matching runs
	// against resolved pattern lists.
	//
	// The split exists because the schema CRUD surface (schema_crud.go,
	// schema_reads.go) writes the config BACK to disk: building a
	// prospective write (or a get_config/current-item handback) from the
	// resolved form would serialize the baked-inline patterns and DROP the
	// *_from indirection on the very first agent write — a silent, persisted
	// behavior change (future edits to the pattern file would stop affecting
	// extraction, with nothing in the YAML hinting why). So the rule is:
	// every surface that ROUND-TRIPS the config (CRUD staleness lookups,
	// prospective building, current-item handbacks, get_config,
	// predicate_deps' matcher/declaration listings, explain_fact's
	// schema-side addresses) reads authoringCfg; only loading/matching reads
	// cfg. Item keys (source File, matcher predicate/term/flags, declaration
	// name/arity) are identical between the two forms — resolution never
	// touches them — so revision bookkeeping keyed off either form agrees.
	// Both fields are always assigned together, from the same source text,
	// by the same mutators (setSchema, loadData, applySchemaLocked).
	authoringCfg jsonfacts.Config

	// Canonical document texts. The session owns the documents, not just
	// their compiled artifacts: the web workbench renders and patches
	// these, and set_schema/set_rules update them, so a human typing in
	// an editor and an agent submitting a document are the same operation.
	schemaText string
	rulesText  string

	// derivedDB caches the last full Transform's output (doc/features/web-ui.md
	// design constraint 2's snapshot pointer): base facts plus every
	// rule-derived predicate, populated by evaluate() and consumed by
	// evaluatedDB(). Transform returns the datalog.Database interface (not
	// concretely *memory.Database), so this field is typed to match. nil
	// means "no successful evaluation since the last rules/schema/fact
	// change" — evaluatedDB() falls back to buildDB's EDB-only snapshot in
	// that case. Every mutator that can change what evaluate() would
	// produce (setSchema, setRules, setRulesWithQueries, loadProgram,
	// loadData) clears it, so a stale Run's derived facts never survive an
	// unapplied edit.
	derivedDB datalog.Database

	// derivedProv is the witness recorder for the Transform that produced
	// derivedDB, cached BESIDE it (doc/features/provenance.md "Session cache
	// interaction"): every call site that assigns derivedDB under the
	// generation guard assigns derivedProv in the same critical section, from
	// the SAME Transform call, so an explain against a cache hit always
	// resolves against the recorder that actually produced the cached
	// database — never a later (or earlier) run's recorder. nil whenever
	// derivedDB is nil (cleared by the same five mutators) OR when
	// provenanceEnabled is false. See newEvalProvenance/explainAgainstSession.
	derivedProv *seminaive.Provenance

	// provenanceEnabled turns on witness recording for every Transform this
	// session runs whose result gets cached into derivedDB — the session
	// policy half of doc/features/provenance.md's "Session policy" section
	// (cmd/datalog sessions default to provenance on; the library default
	// stays off, per seminaive.WithProvenance's own doc comment). Set once at
	// construction (newMCPHandlers, newREPL) and never toggled afterward;
	// left false on a bare &session{} (most tests, and any future caller that
	// constructs one directly) so provenance stays strictly opt-in outside
	// the two cmd/datalog entry points that turn it on.
	provenanceEnabled bool

	// gen counts invalidations of derivedDB: incremented by every mutator
	// that changes what evaluate() would produce (setSchema, setRules,
	// setRulesWithQueries, loadProgram, loadData — the same five call sites
	// that already nil derivedDB out, below). querySnapshot captures gen
	// alongside derivedDB (see snapshotForQuery) so a caller that computes a
	// fresh fixpoint lock-free (querySnapshot.runQuery's cold path) can
	// later write it back into derivedDB only if gen is unchanged since —
	// i.e. nothing invalidated the session while the Transform was running
	// without the lock held. See mcp.go's query handler, the one writer of
	// this cache outside the mutators themselves.
	gen uint64
}

// reservedQueryPred is the synthetic predicate name runQuery mixes into
// every query's evaluation (see runQuery's doc comment: "Build synthetic
// rule: _q_(Var1, ..., VarN) :- body."). It is reserved across the whole
// user-facing predicate namespace — fact names, rule/aggregate-rule heads,
// AND body atoms — because a user-asserted `_q_(...)` fact would sit in the
// same predicate slot runQuery's synthetic rule writes its answer rows into
// (session.go's runQuery/querySnapshot.runQuery, output.Facts("_q_", ...)),
// silently mixing unrelated user data into every query's result set with no
// error at all. parseUserProgram is the single funnel every entry point
// that accepts user program text (facts/rules/queries) must call instead of
// syntax.ParseAll directly, so this reservation cannot be forgotten by a
// future call site.
const reservedQueryPred = "_q_"

// parseUserProgram parses source with syntax.ParseAll and then rejects any
// use of the reserved query predicate (reservedQueryPred) as a fact name,
// rule head, aggregate-rule head, or body atom — the single chokepoint for
// BUG #2 (silent wrong answer): before this check existed, a program
// containing `_q_("boom").` parsed and compiled cleanly, and its fact
// silently rode along in every later query's result rows (runQuery reads
// exactly that predicate out of the post-query fixpoint). Every session
// entry point that accepts raw user program text (setRules,
// setRulesWithQueries, loadProgram) — and the workbench/console call sites
// that pre-parse the same text for check-only or query-only purposes
// (rules_editor.go's handleRulesCheck, console.go's query handler) — must
// call this instead of syntax.ParseAll, so the reservation cannot be
// bypassed by adding a new entry point that forgets it.
func parseUserProgram(source string) (syntax.Ruleset, error) {
	ruleset, err := syntax.ParseAll(source)
	if err != nil {
		return syntax.Ruleset{}, err
	}
	if err := validateNoReservedPred(ruleset); err != nil {
		return syntax.Ruleset{}, err
	}
	return ruleset, nil
}

// validateNoReservedPred rejects a parsed Ruleset that names
// reservedQueryPred anywhere a user predicate can appear: fact/rule heads,
// aggregate-rule heads, and body atoms of rules, aggregate rules, and
// queries alike (the query body case matters too — `_q_(X)?` would read
// back whatever the last query happened to leave in that predicate, an
// equally confusing silent-wrong-answer surface). See parseUserProgram's
// doc comment for why this lives as one shared check rather than being
// duplicated per call site.
func validateNoReservedPred(rs syntax.Ruleset) error {
	is := func(pred string) bool { return pred == reservedQueryPred }
	for _, r := range rs.Rules {
		if is(r.Head.Pred) {
			return fmt.Errorf("predicate %q is reserved for internal query evaluation and cannot be used as a fact or rule name", reservedQueryPred)
		}
		for _, atom := range r.Body {
			if is(atom.Pred) {
				return fmt.Errorf("predicate %q is reserved for internal query evaluation and cannot be used in a rule body", reservedQueryPred)
			}
		}
	}
	for _, ar := range rs.AggRules {
		if is(ar.Head.Pred) {
			return fmt.Errorf("predicate %q is reserved for internal query evaluation and cannot be used as an aggregate rule name", reservedQueryPred)
		}
		for _, atom := range ar.Body {
			if is(atom.Pred) {
				return fmt.Errorf("predicate %q is reserved for internal query evaluation and cannot be used in a rule body", reservedQueryPred)
			}
		}
	}
	for _, q := range rs.Queries {
		for _, atom := range q.Body {
			if is(atom.Pred) {
				return fmt.Errorf("predicate %q is reserved for internal query evaluation and cannot be used in a query", reservedQueryPred)
			}
		}
	}
	return nil
}

// validateStatementNoReservedPred applies the reservedQueryPred reservation to
// a single statement parsed via syntax.ParseStatement -- the ingest surfaces
// that take one statement at a time rather than a whole program (the
// interactive REPL and the MCP query handler). It wraps the statement in a
// one-element Ruleset and defers to validateNoReservedPred so all program
// ingest, whether via ParseAll or ParseStatement, enforces the reservation
// through the same shared check and no ParseStatement caller can silently
// reintroduce the _q_ leak.
func validateStatementNoReservedPred(stmt any) error {
	switch v := stmt.(type) {
	case *syntax.Rule:
		return validateNoReservedPred(syntax.Ruleset{Rules: []syntax.Rule{*v}})
	case *syntax.AggregateRule:
		return validateNoReservedPred(syntax.Ruleset{AggRules: []syntax.AggregateRule{*v}})
	case *syntax.Query:
		return validateNoReservedPred(syntax.Ruleset{Queries: []syntax.Query{*v}})
	}
	return nil
}

// setDataSource configures the session to load facts from a jsonfacts config.
func (s *session) setDataSource(configPath, dataDir string) {
	s.configPath = configPath
	s.dataDir = dataDir
}

// loadData loads (or reloads) facts from the configured data source.
func (s *session) loadData() error {
	if s.configPath == "" {
		return fmt.Errorf("no data source configured (use -c flag)")
	}

	data, err := os.ReadFile(s.configPath)
	if err != nil {
		return fmt.Errorf("config %s: %w", s.configPath, err)
	}
	cfg, err := parseConfig(data, s.configPath)
	if err != nil {
		return fmt.Errorf("config %s: %w", s.configPath, err)
	}
	// authoring is a second, independent parse of the same bytes — the
	// unresolved AUTHORING form kept beside the resolved runtime cfg below;
	// see prepareSchema's doc comment for why parse-twice is the chosen
	// deep-copy (ResolveFromFS mutates matchers in place) and
	// session.authoringCfg's for why both forms must exist.
	authoring, err := parseConfig(data, s.configPath)
	if err != nil {
		return fmt.Errorf("config %s: %w", s.configPath, err)
	}

	var db *memory.Database
	if strings.HasSuffix(s.dataDir, ".zip") {
		db, err = loadFromZip(&cfg, s.dataDir)
	} else {
		// Resolve *_from pattern files against the same directory LoadDir
		// will read from, mirroring prepareSchema's fsys/ResolveFromFS
		// pairing (mcp.go's set_schema path). LoadDir/LoadFS do not resolve
		// _from fields themselves (see ResolveFromFS's doc comment) — before
		// this call, a matcher like `contains_from: iocs.txt` validated but
		// silently matched against an empty inline list, so compileMatchers
		// emitted zero facts with no warning. Without this, the CLI -c/
		// .reload path and the MCP set_schema path disagreed on the same
		// schema document.
		fsys := os.DirFS(s.dataDir)
		if err = cfg.ResolveFromFS(fsys); err != nil {
			return fmt.Errorf("resolving *_from patterns in %s: %w", s.dataDir, err)
		}
		db, err = cfg.LoadFS(fsys)
	}
	if err != nil {
		return fmt.Errorf("loading data from %s: %w", s.dataDir, err)
	}

	s.cfg = cfg
	s.authoringCfg = authoring
	s.schemaText = string(data)
	s.dataDB = db
	s.derivedDB = nil
	s.derivedProv = nil
	s.gen++
	return nil
}

// loadFromZip opens a zip file and loads JSONL data using it as an fs.FS.
// Like the plain-directory path in loadData, it resolves *_from pattern
// files against the same fs.FS (the zip contents) before loading, so a
// zip-packaged config's matchers behave the same as a directory-packaged
// one — see loadData's comment for why this matters (BUG #3).
func loadFromZip(cfg *jsonfacts.Config, path string) (*memory.Database, error) {
	r, err := zip.OpenReader(path)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	if err := cfg.ResolveFromFS(&r.Reader); err != nil {
		return nil, fmt.Errorf("resolving *_from patterns in %s: %w", path, err)
	}
	return cfg.LoadFS(&r.Reader)
}

// parseConfig decodes a jsonfacts.Config from JSON or YAML source; path is
// used only to detect YAML by extension.
func parseConfig(data []byte, path string) (jsonfacts.Config, error) {
	ext := filepath.Ext(path)
	format := "json"
	if ext == ".yaml" || ext == ".yml" {
		format = "yaml"
	}
	return parseConfigFormat(data, format)
}

// parseConfigFormat decodes a jsonfacts.Config from raw bytes given an
// explicit format hint ("json" or "yaml"), rather than sniffing a file
// extension. This is the variant set_schema uses: an MCP submission is a
// string in memory with no path to sniff. An empty format defaults to
// "yaml", matching the schema's typical authoring format.
func parseConfigFormat(data []byte, format string) (jsonfacts.Config, error) {
	switch format {
	case "", "yaml":
		var raw any
		if err := yaml.Unmarshal(data, &raw); err != nil {
			return jsonfacts.Config{}, fmt.Errorf("parsing YAML: %w", err)
		}
		var err error
		data, err = json.Marshal(raw)
		if err != nil {
			return jsonfacts.Config{}, fmt.Errorf("converting YAML to JSON: %w", err)
		}
	case "json":
		// no conversion needed
	default:
		return jsonfacts.Config{}, fmt.Errorf("unknown config format %q (want %q or %q)", format, "yaml", "json")
	}

	var cfg jsonfacts.Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return jsonfacts.Config{}, fmt.Errorf("parsing config: %w", err)
	}
	if err := validateConfigKeyUniqueness(cfg); err != nil {
		return jsonfacts.Config{}, err
	}
	return cfg, nil
}

// confineRef validates a single file reference against a session's
// confinement boundary. MCP wires this to dataRoot.Confine (directory data
// source) or a zip-based fs.ValidPath check (zip data source); the REPL
// never sets it, so setSchema outside MCP behaves as before (no
// confinement, since the operator authors the config themselves).
type confineRef func(ref string) (string, error)

// prepareSchema performs setSchema's expensive, session-independent work:
// parsing the config, validating every file reference through confine,
// resolving *_from pattern files, and loading the data via fsys. It touches
// no session state at all, so a caller that shares the session across
// goroutines (the schema CRUD writes in schema_crud.go) can run this
// WITHOUT holding its lock, taking the lock only for the cheap field swap
// setSchema (or the caller itself) performs afterward — the fix for
// set_schema holding h.mu across a full data reload. On any error, nothing
// has been touched yet, so there is nothing for a caller to unwind.
//
// It returns TWO configs (see session.authoringCfg's doc comment for the
// full rationale): authoring is the parsed config exactly as written, with
// every matcher's *_from field intact; runtime is a resolved copy —
// ResolveFromFS merges each *_from pattern file into the inline lists and
// clears the field, mutating matchers in place — which is what LoadFS
// matches against. The deep copy is obtained by simply parsing text a
// SECOND time rather than by a reflective/manual clone: the text is already
// in hand, parseConfigFormat is cheap next to the LoadFS that follows, and
// a second parse is guaranteed to share no slice backing arrays with the
// authoring value — the exact property the in-place resolution would
// otherwise violate — with no clone helper to keep in sync as Config grows
// fields.
func prepareSchema(text string, format string, fsys fs.FS, confine confineRef) (authoring, runtime jsonfacts.Config, db *memory.Database, err error) {
	authoring, err = parseConfigFormat([]byte(text), format)
	if err != nil {
		return jsonfacts.Config{}, jsonfacts.Config{}, nil, err
	}
	runtime, err = parseConfigFormat([]byte(text), format)
	if err != nil {
		return jsonfacts.Config{}, jsonfacts.Config{}, nil, err
	}

	// Confinement checks read the AUTHORING form: the *_from fields being
	// validated here are exactly the ones resolution would clear.
	if confine != nil {
		for i, src := range authoring.Sources {
			if _, err := confine(src.File); err != nil {
				return jsonfacts.Config{}, jsonfacts.Config{}, nil, fmt.Errorf("source %d: file %q: %w", i, src.File, err)
			}
		}
		for i := range authoring.Matchers {
			m := &authoring.Matchers[i]
			for _, ref := range []string{
				m.ContainsFrom, m.StartsWithFrom, m.EndsWithFrom,
				m.RegexMatchFrom, m.Base64From, m.Base64UTF16From, m.CIDRFrom,
			} {
				if ref == "" {
					continue
				}
				if _, err := confine(ref); err != nil {
					return jsonfacts.Config{}, jsonfacts.Config{}, nil, fmt.Errorf("matcher %d: file %q: %w", i, ref, err)
				}
			}
		}
	}

	// Resolve *_from pattern files on the RUNTIME copy only: LoadFS (unlike
	// LoadSchemaFS) does not do this, and an MCP-submitted config is not a
	// schema-dir file, so nothing else will resolve them before matching
	// runs.
	if err := runtime.ResolveFromFS(fsys); err != nil {
		return jsonfacts.Config{}, jsonfacts.Config{}, nil, err
	}

	db, err = runtime.LoadFS(fsys)
	if err != nil {
		return jsonfacts.Config{}, jsonfacts.Config{}, nil, fmt.Errorf("loading data: %w", err)
	}
	return authoring, runtime, db, nil
}

// setSchema replaces the session's jsonfacts configuration atomically: it
// delegates the parsing/confinement/loading work to prepareSchema, then
// commits the result to session state in one shot. On any error, the
// session is left exactly as it was before the call — the model's
// replacement schema either fully replaces the old one or not at all, per
// the design's atomic-replacement requirement (mcp-server.md). Callers that
// need prepareSchema's expensive part to run without holding a shared lock
// (mcpHandlers.setSchema) should call prepareSchema directly instead and
// commit the four fields below together under their own lock — see that
// method's doc comment for the TOCTOU reasoning.
func (s *session) setSchema(text string, format string, fsys fs.FS, confine confineRef) error {
	authoring, runtime, db, err := prepareSchema(text, format, fsys, confine)
	if err != nil {
		return err
	}
	s.cfg = runtime
	s.authoringCfg = authoring
	s.schemaText = text
	s.dataDB = db
	s.derivedDB = nil
	s.derivedProv = nil
	s.gen++
	return nil
}

// setRules replaces the session's Datalog program atomically: it parses
// source, rejects embedded `?` queries (the model should use the query
// tool instead of embedding one in the document), runs a trial Compile so
// stratification/arity errors attach to this submission rather than to a
// later, innocent query, and only then replaces s.rules/s.aggRules/
// s.rulesText wholesale. Unlike loadProgram (which appends, for the REPL's
// .load), setRules always replaces the whole document.
//
// This method is NOT part of the MCP tool surface (phase 1b removed the
// whole-document set_rules MCP tool — doc/features/workbench-v2.md design
// decision 4 — in favor of the rule-group CRUD tools in rules_crud.go). It
// survives as a plain session-level helper because many pre-existing unit
// tests (session_test.go, describe_test.go, commands_test.go) use it
// directly as test setup, unrelated to MCP; those tests are out of scope
// for the CRUD migration and this method's behavior for them must not
// change. Do not wire this back onto an MCP tool.
//
// The returned warnings are ruleset.Warnings (detached '%%' doc-comment
// blocks that will be silently dropped from the round-tripped document).
// They are surfaced -- not swallowed -- so a caller writing documented rules
// learns a doc block it wrote failed to attach; see setSchema, whose output
// carries the same Warnings channel.
func (s *session) setRules(source string) ([]string, error) {
	ruleset, err := parseUserProgram(source)
	if err != nil {
		return nil, err
	}
	if len(ruleset.Queries) > 0 {
		return nil, fmt.Errorf("set_rules: source contains %d embedded query statement(s) ('?'); "+
			"remove them and use the query tool to run queries", len(ruleset.Queries))
	}

	if _, err := seminaive.New(s.engineOpts...).Compile(ruleset); err != nil {
		return nil, err
	}

	s.rules = ruleset.Rules
	s.aggRules = ruleset.AggRules
	s.rulesText = source
	s.derivedDB = nil
	s.derivedProv = nil
	s.gen++
	return ruleset.Warnings, nil
}

// setRulesWithQueries replaces the session's Datalog program atomically,
// exactly like setRules (trial Compile of the parsed rules; rules/aggRules/
// rulesText are replaced wholesale only if that Compile succeeds, and
// rulesText captures the full original source — including any embedded
// queries — since the workbench editor's content is the canonical document),
// but returns the embedded `?` queries to the caller instead of rejecting
// them. setRules above keeps the stricter no-embedded-queries rule (matching
// what the now-removed whole-document set_rules MCP tool used to enforce).
// The workbench's Datalog Editor Run action (rules_editor.go) is this
// method's one remaining caller: it accepts embedded queries because the
// editor follows the REPL's `.`/`?` convention, so a pasted `.dl` file with
// trailing queries should just work. Under a --rules session, this path
// still only mutates SESSION memory and can diverge from the rules/
// directory store on disk until a later phase retires it — see
// runApplyRulesDocument's doc comment in rules_editor.go. On any error,
// session state is unchanged.
func (s *session) setRulesWithQueries(source string) ([]syntax.Query, error) {
	ruleset, err := parseUserProgram(source)
	if err != nil {
		return nil, err
	}

	if _, err := seminaive.New(s.engineOpts...).Compile(ruleset); err != nil {
		return nil, err
	}

	s.rules = ruleset.Rules
	s.aggRules = ruleset.AggRules
	s.rulesText = source
	s.derivedDB = nil
	s.derivedProv = nil
	s.gen++
	return ruleset.Queries, nil
}

// loadRuleStore is the ONE chokepoint that rebuilds a session's rules from a
// *ruleStore (doc/features/workbench-v2.md work item 1's design decision 3,
// "One rebuild chokepoint on the session"): it resets s.rules/s.aggRules/
// s.facts, reloads EVERY group in store through s.loadProgram (the same
// fact-routing path newMCPHandlers used to run inline — see that function's
// git history), sets s.rulesText to store.export(), invalidates
// derivedDB/derivedProv, and bumps gen. Every caller that (re)builds a
// session's rules from a rules/ directory — newMCPHandlers at startup, and
// every put_rule_group/delete_rule_group CRUD write after this task —
// funnels through here, so the fsnotify watcher (phase 1d) can reuse this
// exact method for a disk-triggered reload with no separate code path to
// keep in sync.
//
// s.facts is reset here (unlike loadProgram's own append-only contract)
// because this method represents a FULL rebuild from the store's current
// on-disk content, not an incremental addition — a group that used to assert
// a fact and no longer does must not leave that fact behind. Any facts
// asserted through some OTHER path before this call (there are none in the
// current cmd/datalog surface, since there is no assert-fact tool per
// doc/features/workbench-v2.md's "no assert-fact tool" line) would also be
// dropped by this reset; that is intentional, not a gap, since the rules/
// directory store's groups are meant to be the session's only source of
// rule-derived facts (body-less rules) in this mode.
//
// On any per-group loadProgram error, the error is returned immediately and
// s is left PARTIALLY rebuilt (some groups loaded, some not) — callers that
// need "all-or-nothing including on-session-state" must validate the WHOLE
// prospective ruleset via a trial Compile (see rules_crud.go's put_rule_group,
// which does exactly this) BEFORE ever calling loadRuleStore. Note that this
// method itself never compiles: newMCPHandlers's startup path calls it with
// no trial Compile, so a directory whose groups individually parse but
// collectively fail stratification loads "successfully" here and errors
// LOUDLY at the first evaluation instead — the same query-time failure a
// malformed monolithic rules file always produced, never a silent drop.
func (s *session) loadRuleStore(store *ruleStore) error {
	s.facts = nil
	s.rules = nil
	s.aggRules = nil
	for _, k := range store.Order {
		g := store.Groups[k]
		if _, err := s.loadProgram(g.Text); err != nil {
			return fmt.Errorf("%s: %w", g.File, err)
		}
	}
	s.rulesText = store.export()
	s.derivedDB = nil
	s.derivedProv = nil
	s.gen++
	return nil
}

// loadProgram parses a Datalog source string containing multiple statements,
// adding facts and rules to the session. Queries found in the source are
// returned for the caller to execute and present.
func (s *session) loadProgram(src string) ([]syntax.Query, error) {
	ruleset, err := parseUserProgram(src)
	if err != nil {
		return nil, err
	}
	for _, rule := range ruleset.Rules {
		if rule.IsFact() {
			s.facts = append(s.facts, rule.ToFact())
		} else {
			s.rules = append(s.rules, rule)
		}
	}
	s.aggRules = append(s.aggRules, ruleset.AggRules...)
	s.derivedDB = nil
	s.derivedProv = nil
	s.gen++
	return ruleset.Queries, nil
}

// buildDB returns the database to evaluate against: the loaded data source
// extended with interactively asserted facts.
func (s *session) buildDB() (*memory.Database, error) {
	if s.dataDB != nil {
		if len(s.facts) == 0 {
			return s.dataDB, nil
		}
		return s.dataDB.Extend(s.facts...)
	}
	b := memory.NewBuilder()
	for _, f := range s.facts {
		if err := b.AddFact(f); err != nil {
			return nil, err
		}
	}
	return b.Build(), nil
}

// evaluate compiles the session's current ruleset and runs a full seminaive
// Transform against buildDB()'s EDB, returning the resulting database: base
// facts plus every rule-derived predicate. This is the same computation
// runQuery performs per query (with its extra synthetic _q_ rule mixed in)
// minus the synthetic head — callers that want a snapshot of "what does the
// current ruleset actually derive," not just one query's answer, call this
// instead and cache the result themselves (see derivedDB, evaluatedDB).
// Callers that must not hold the session's lock across the Transform (Run —
// see snapshotForEvaluate's doc comment) should call snapshotForEvaluate and
// evaluateSnapshot directly instead of this method.
//
// prov is populated with this Transform's witnesses when the session has
// provenance enabled (nil otherwise) — see newEvalProvenance and
// evaluateSnapshot's doc comment. Callers that cache the returned database
// into derivedDB must cache prov into derivedProv in the SAME critical
// section (doc/features/provenance.md "Session cache interaction"); this
// method itself does not touch derivedDB/derivedProv, matching
// evaluateSnapshot's lock-free contract.
func (s *session) evaluate(ctx context.Context) (db datalog.Database, prov *seminaive.Provenance, err error) {
	ruleset, engineOpts, edb, _, err := s.snapshotForEvaluate()
	if err != nil {
		return nil, nil, err
	}
	prov = s.newEvalProvenance()
	db, err = evaluateSnapshot(ctx, ruleset, engineOpts, edb, prov)
	return db, prov, err
}

// newEvalProvenance returns a fresh *seminaive.Provenance for a Transform
// whose result this session might cache, or nil when provenanceEnabled is
// false. A Provenance is most-recent-run-only (seminaive.Provenance's doc
// comment), so every Transform that could be cached into derivedDB must get
// its OWN fresh recorder rather than sharing one across calls — sharing one
// would leave an earlier cached derivedDB's explanations silently pointing
// at a LATER run's witnesses (or a Provenance mid-repopulation from a
// concurrent Transform) the moment a second Transform completed.
func (s *session) newEvalProvenance() *seminaive.Provenance {
	if !s.provenanceEnabled {
		return nil
	}
	return seminaive.NewProvenance()
}

// snapshotForEvaluate captures the state evaluate() needs — the ruleset,
// engine options, the EDB snapshot, and the generation counter — so a caller
// can run the expensive Compile+Transform (evaluateSnapshot) with no lock
// held, mirroring prepareSchema/setSchema's split (mcp.go's setSchema: the
// fix for set_schema previously holding h.mu across a full data reload).
// Run (rules_editor.go's handleRulesRun) is this pattern's other caller: it
// previously held wb.h.mu across evaluate's entire Transform, freezing every
// page load, SSE connect, and MCP call for up to evalTimeout. gen is
// session.gen as of this snapshot; a caller that wants to write a freshly
// computed result back into derivedDB should only do so if the session's
// CURRENT gen still equals the gen returned here — otherwise a set_schema/
// set_rules/loadData landed while the Transform ran lock-free, and the
// result reflects a ruleset/schema/data that no longer exists (same
// reasoning as querySnapshot's gen field). Callers that share the session
// across goroutines must hold their lock around this call only.
func (s *session) snapshotForEvaluate() (ruleset syntax.Ruleset, engineOpts []seminaive.Option, db *memory.Database, gen uint64, err error) {
	db, err = s.buildDB()
	if err != nil {
		return syntax.Ruleset{}, nil, nil, 0, err
	}
	return syntax.Ruleset{Rules: s.rules, AggRules: s.aggRules}, s.engineOpts, db, s.gen, nil
}

// evaluateSnapshot is evaluate's lock-free half: Compile+Transform against
// an already-captured snapshot (snapshotForEvaluate), safe to run with no
// session lock held at all. engineOpts carries seminaive.WithFactLimit(factCap)
// (set once in newMCPHandlers — see session.engineOpts), so Transform halts
// mid-evaluation with a seminaive.FactLimitError if the ruleset derives too
// many facts; translateFactLimit reworks that into checkFactCap's familiar
// "rule too broad" wording before it reaches a caller.
//
// prov, when non-nil (see newEvalProvenance), is threaded in as an extra
// seminaive.WithProvenance option for THIS Transform only — engineOpts
// itself never carries a Provenance (it is shared/reused across many
// Transforms via session.engineOpts, while a Provenance must be fresh per
// cached Transform, see newEvalProvenance's doc comment). A caller passing
// prov == nil gets exactly the pre-provenance behavior: no extra option, no
// witness recording.
func evaluateSnapshot(ctx context.Context, ruleset syntax.Ruleset, engineOpts []seminaive.Option, db *memory.Database, prov *seminaive.Provenance) (datalog.Database, error) {
	opts := engineOpts
	if prov != nil {
		opts = append(engineOpts[:len(engineOpts):len(engineOpts)], seminaive.WithProvenance(prov))
	}
	t, err := seminaive.New(opts...).Compile(ruleset)
	if err != nil {
		return nil, err
	}
	out, err := t.Transform(ctx, db)
	if err != nil {
		return nil, translateFactLimit(err)
	}
	return out, nil
}

// evaluatedDB returns derivedDB if a successful evaluate() has populated it
// since the last rules/schema/fact change, otherwise falls back to
// buildDB's EDB-only snapshot. Fact-browsing callers (the workbench's Fact
// Browser) use this instead of buildDB directly so a predicate's "derived"
// facts reflect the last Run rather than always reading as empty
// (buildDB alone never runs rule evaluation — see doc/features/web-ui.md
// design constraint 2's snapshot-pointer intent). The MCP fact-listing
// tools (list_predicates, sample_facts) read this too, so the agent and
// the Fact Browser always agree on counts; REPL paths still call buildDB
// directly and are unaffected by this cache.
func (s *session) evaluatedDB() (datalog.Database, error) {
	if s.derivedDB != nil {
		return s.derivedDB, nil
	}
	return s.buildDB()
}

// querySnapshot captures the session state one query needs, so the
// expensive Compile+Transform can run without any lock held. All fields
// are safe to read after the owning lock is released: rules/aggRules/
// dataDB are only ever replaced wholesale by session mutators (loadProgram
// appends, which never rewrites elements below the snapshotted length),
// engineOpts is fixed at construction, and a built memory.Database is
// never mutated in place (Extend returns a new one). A query therefore
// sees the session as of snapshot time; mutations landing mid-query apply
// to the next query, which is the only coherent ordering for the race
// anyway.
//
// derived and gen exist so runQuery can skip re-deriving every IDB
// predicate on each call (doc/features/mcp-server.md review item 6): derived
// is session.derivedDB as of snapshot time (nil if the session has no valid
// cached fixpoint — cleared by every mutator that invalidates it), and gen
// is session.gen at the same moment. runQuery's cold path (derived == nil)
// computes the ruleset's fixpoint once and stores it back into THIS
// querySnapshot's derived field (see its pointer receiver); a caller that
// wants to publish that fresh fixpoint back to the session for later
// queries to reuse (mcp.go's query handler) may do so after the call
// returns, but only if the session's CURRENT gen still equals the gen
// captured here — otherwise something invalidated the session while this
// query's Transform ran lock-free, and writing back would resurrect a
// fixpoint computed against a ruleset/schema/data that no longer exists.
//
// derivedProv rides beside derived exactly like session.derivedProv rides
// beside session.derivedDB (doc/features/provenance.md "Session cache
// interaction"): it is the recorder for the SAME Transform that produced
// derived, nil whenever derived is nil or provEnabled is false, and
// runQuery's cold path fills it in alongside base the same way it fills in
// derived. A caller writing derived back to session.derivedDB must write
// derivedProv back to session.derivedProv in the same critical section.
// provEnabled mirrors session.provenanceEnabled as of snapshot time, so the
// cold path knows whether to mint a fresh Provenance for the base stage at
// all — the synthetic _q_ stage below never gets one regardless (explaining
// _q_ heads is not a goal; see runQuery's doc comment).
type querySnapshot struct {
	rules       []syntax.Rule
	aggRules    []syntax.AggregateRule
	engineOpts  []seminaive.Option
	db          *memory.Database
	derived     datalog.Database
	derivedProv *seminaive.Provenance
	provEnabled bool
	gen         uint64
}

// snapshotForQuery captures the state runQuery reads. Callers that share
// the session across goroutines must hold their lock around this call
// (and only this call); single-threaded callers may use session.runQuery
// directly.
func (s *session) snapshotForQuery() (querySnapshot, error) {
	db, err := s.buildDB()
	if err != nil {
		return querySnapshot{}, err
	}
	return querySnapshot{
		rules:       s.rules,
		aggRules:    s.aggRules,
		engineOpts:  s.engineOpts,
		db:          db,
		derived:     s.derivedDB,
		derivedProv: s.derivedProv,
		provEnabled: s.provenanceEnabled,
		gen:         s.gen,
	}, nil
}

// runQuery compiles the session's rules plus a synthetic _q_ rule for q,
// evaluates against the current database, and returns the result rows, the
// query's variable names, and per-stratum evaluation stats. Sorting and
// presentation are left to the caller. Stats are non-nil only when the
// Transform ran to completion.
func (s *session) runQuery(ctx context.Context, q *syntax.Query) (rows [][]datalog.Constant, vars []string, stats []seminaive.StratumStats, err error) {
	snap, err := s.snapshotForQuery()
	if err != nil {
		return nil, extractNamedVars(q.Body), nil, err
	}
	return snap.runQuery(ctx, q)
}

// runQuery is runQuery's evaluation half: everything after the snapshot,
// safe to run with no lock held. It has a pointer receiver — not because it
// needs one for the query itself, but so its cold path (see below) can
// leave the freshly-derived fixpoint in qs.derived for the caller to
// inspect afterward (qs is always an addressable local at every call site,
// so this is source-compatible with every existing caller).
//
// Evaluation is split into two stages so a query never re-derives every IDB
// predicate when the session already has (doc/features/mcp-server.md review
// item 6): first, ensure qs.derived holds the ruleset's full fixpoint —
// reusing it as-is if the caller's snapshot already had one (a Run/Apply
// already computed it, or an earlier query on this same generation cached
// it), otherwise compiling and evaluating qs.rules/qs.aggRules against
// qs.db exactly once here. Second, compile and evaluate ONLY the synthetic
// _q_ rule against that fixpoint — cheap, since every predicate the query
// body references is already materialized. A session with no rules at all
// skips stage one entirely (qs.db is already the whole fixpoint), matching
// the pre-fix single-Transform cost exactly for that common case.
//
// Both stages' engine options come from qs.engineOpts, which carries
// seminaive.WithFactLimit(factCap) by default (set once in newMCPHandlers —
// see session.engineOpts' doc comment): each stage's own Transform halts
// mid-evaluation with a seminaive.FactLimitError if IT derives too many new
// facts, translated to the familiar "rule too broad" wording below. This
// covers the synthetic _q_ stage too, which previously had no cap of its
// own at all — a zero-rule session (stage one skipped, base = qs.db
// verbatim) left the query's own cross product completely uncapped.
func (qs *querySnapshot) runQuery(ctx context.Context, q *syntax.Query) (rows [][]datalog.Constant, vars []string, stats []seminaive.StratumStats, err error) {
	vars = extractNamedVars(q.Body)

	// Build synthetic rule: _q_(Var1, ..., VarN) :- body.
	headTerms := make([]datalog.Term, len(vars))
	for i, v := range vars {
		headTerms[i] = datalog.Variable(v)
	}
	synth := syntax.Rule{
		Head: syntax.Atom{Pred: "_q_", Terms: headTerms},
		Body: q.Body,
	}

	var baseStats []seminaive.StratumStats
	base := qs.derived
	if base == nil {
		// A fresh Provenance per base-stage Transform, never shared across
		// calls (seminaive.Provenance is most-recent-run-only — see
		// session.newEvalProvenance's doc comment). Minted even in the
		// zero-rule branch below: Provenance.install still runs for a
		// Transform with no rules (transformer.Transform, unconditional on
		// t.provenance != nil), and without it a zero-rule ruleset's base
		// facts would have no Provenance to Explain them as base facts at
		// all — better to pay one cheap pass-through Transform than to leave
		// that case unexplainable.
		var prov *seminaive.Provenance
		if qs.provEnabled {
			prov = seminaive.NewProvenance()
		}
		switch {
		case len(qs.rules) == 0 && len(qs.aggRules) == 0 && prov == nil:
			// Nothing to derive and no provenance to record: the EDB
			// snapshot IS the whole fixpoint, so skip the Transform entirely
			// (the pre-provenance fast path, preserved when provenance is
			// off).
			base = qs.db
		default:
			baseRuleset := syntax.Ruleset{Rules: qs.rules, AggRules: qs.aggRules}
			baseOpts := append(qs.engineOpts[:len(qs.engineOpts):len(qs.engineOpts)],
				seminaive.WithProfile(func(ss []seminaive.StratumStats) { baseStats = ss }))
			if prov != nil {
				baseOpts = append(baseOpts, seminaive.WithProvenance(prov))
			}
			bt, cerr := seminaive.New(baseOpts...).Compile(baseRuleset)
			if cerr != nil {
				return nil, vars, nil, cerr
			}
			base, err = bt.Transform(ctx, qs.db)
			if err != nil {
				// WithFactLimit (baseOpts, from qs.engineOpts) is enforced
				// right where a rule can actually blow the working set up —
				// the ruleset's OWN derivation, counted from zero for this
				// Transform call — not against the query's raw input
				// snapshot: a query against a legitimately large
				// already-loaded dataset must still run to completion
				// (review item 1's fix must not reject item 1's own
				// regression test, a 1500-fact base read with zero rules).
				return nil, vars, nil, translateFactLimit(err)
			}
		}
		// Leave the freshly-derived fixpoint (and its recorder, beside it)
		// where the caller (if it holds the session's lock, e.g. mcp.go's
		// query handler) can find them and cache them back into
		// session.derivedDB/derivedProv for later queries — see this
		// method's doc comment and querySnapshot.derived's.
		qs.derived = base
		qs.derivedProv = prov
	}

	opts := append(qs.engineOpts[:len(qs.engineOpts):len(qs.engineOpts)],
		seminaive.WithProfile(func(ss []seminaive.StratumStats) { stats = ss }))
	t, err := seminaive.New(opts...).Compile(syntax.Ruleset{Rules: []syntax.Rule{synth}})
	if err != nil {
		return nil, vars, nil, err
	}

	output, err := t.Transform(ctx, base)
	stats = append(baseStats, stats...)
	if err != nil {
		// WithFactLimit applies here too (opts, from qs.engineOpts): this is
		// the fix for the previously-uncapped hole — a zero-rule session
		// skips the base stage above entirely, so this synthetic _q_
		// Transform is the ONLY evaluation a cross-product query like
		// `event(A,B,C), event(D,E,F), event(G,H,I)?` ever runs through.
		return nil, vars, stats, translateFactLimit(err)
	}

	for row := range output.Facts("_q_", len(vars)) {
		rows = append(rows, row)
	}
	return rows, vars, stats, nil
}

// explainProvenance returns the *seminaive.Provenance an Explain/ExplainTree
// call should resolve fact against: the recorder cached beside the current
// derivedDB when one exists and provenance is enabled (the common case — a
// query or Run has already populated it), or a freshly computed one
// otherwise (a session that has never evaluated since its last mutation, but
// whose caller still wants to explain a fact against the CURRENT ruleset).
// ok is false when provenance is disabled for this session at all — callers
// should report that distinctly from "fact not found" (see mcp.go's explain
// tool and repl.go's .why).
//
// The freshly-computed path deliberately reuses evaluate()/evaluateSnapshot
// rather than duplicating Compile+Transform here, and — mirroring every
// other derivedDB writer (setRules et al., rules_editor.go's Run,
// mcp.go's cacheDerivedQuery) — caches its result back into
// derivedDB/derivedProv under the SAME generation guard, so a caller with no
// cache yet pays for exactly one Transform and every later explain/query on
// the same generation reuses it.
func (s *session) explainProvenance(ctx context.Context) (*seminaive.Provenance, bool, error) {
	if !s.provenanceEnabled {
		return nil, false, nil
	}
	if s.derivedDB != nil && s.derivedProv != nil {
		return s.derivedProv, true, nil
	}

	ruleset, engineOpts, db, snapGen, err := s.snapshotForEvaluate()
	if err != nil {
		return nil, false, err
	}
	prov := seminaive.NewProvenance()
	out, err := evaluateSnapshot(ctx, ruleset, engineOpts, db, prov)
	if err != nil {
		return nil, false, err
	}
	// Cache beside the generation it was computed against, exactly like
	// Run/query's cold path — a mutation landing while this Transform ran
	// must not resurrect a stale ruleset's derivedDB/derivedProv pair.
	if s.gen == snapGen {
		s.derivedDB = out
		s.derivedProv = prov
	}
	return prov, true, nil
}

// parseFactStatement parses text as one ground Datalog fact — a("b", 3),
// with or without the trailing "." a full statement requires — for the two
// surfaces that accept a fact by name rather than by JSON structure: the MCP
// explain tool (mcp.go's explainInput) and the REPL's .why (repl.go). It
// reuses syntax.ParseStatement (via syntax.ParseAll's underlying parser)
// rather than hand-rolling atom parsing, so a fact typed here parses exactly
// as the same text would inside a rules document or the console — no
// second, subtly different grammar to keep in sync. Rejects a body (":-
// ..."), a query ("?"), an aggregate rule, or a head with any unbound
// variable — explain only makes sense for one fully ground fact.
func parseFactStatement(text string) (datalog.Fact, error) {
	text = strings.TrimSpace(text)
	if text == "" {
		return datalog.Fact{}, fmt.Errorf("explain: empty fact")
	}
	if !strings.HasSuffix(text, ".") {
		text += "."
	}
	stmt, err := syntax.ParseStatement(text)
	if err != nil {
		return datalog.Fact{}, err
	}
	rule, ok := stmt.(*syntax.Rule)
	if !ok {
		return datalog.Fact{}, fmt.Errorf("explain: expected a single ground fact like pred(\"a\", 1), got %T", stmt)
	}
	if len(rule.Body) > 0 {
		return datalog.Fact{}, fmt.Errorf("explain: expected a single ground fact with no body, got a rule: %s", rule.String())
	}
	if !rule.IsFact() {
		return datalog.Fact{}, fmt.Errorf("explain: %s is not fully ground (every term must be a constant, not a variable)", rule.String())
	}
	return rule.ToFact(), nil
}

// explainTree resolves fact's full derivation tree against this session's
// current evaluation, computing (and caching, per explainProvenance's doc
// comment) one if none is cached yet. ok is false when provenance is
// disabled for this session; found is false when provenance is enabled but
// fact is not one this session's most recent evaluation produced at all
// (seminaive.Provenance.ExplainTree's own not-found case — an unknown fact,
// not a base fact, which reports found=true with Derivation.Base set). opts
// forwards directly to seminaive.ExplainTree (MaxDepth/MaxNodes).
func (s *session) explainTree(ctx context.Context, fact datalog.Fact, opts ...seminaive.TreeOption) (d seminaive.Derivation, ok bool, found bool, err error) {
	prov, ok, err := s.explainProvenance(ctx)
	if err != nil || !ok {
		return seminaive.Derivation{}, ok, false, err
	}
	d, found = prov.ExplainTree(fact, opts...)
	return d, true, found, nil
}

func (s *session) allPredicateNames() []string {
	seen := map[string]bool{}
	if s.dataDB != nil {
		for pred := range s.dataDB.Predicates() {
			seen[pred] = true
		}
	}
	for _, f := range s.facts {
		seen[f.Name] = true
	}
	for _, rule := range s.rules {
		seen[rule.Head.Pred] = true
		for _, atom := range rule.Body {
			seen[atom.Pred] = true
		}
	}
	for _, ar := range s.aggRules {
		seen[ar.Head.Pred] = true
	}

	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// extractNamedVars collects unique named variables from query body atoms,
// preserving order of first occurrence. Underscore-prefixed variables and
// parser-generated anonymous ones (?N, from '?' and bare '_') are excluded:
// they are don't-cares, not requested columns — and excluding them from the
// synthetic head is what lets a negated query atom use anonymous variables
// (the engine's safety check skips anonymous vars in negated atoms, but a
// head variable must always be bound).
func extractNamedVars(body []syntax.Atom) []string {
	var vars []string
	seen := map[string]bool{}
	for _, atom := range body {
		for _, t := range atom.Terms {
			if v, ok := t.(datalog.Variable); ok {
				name := string(v)
				if !seen[name] && !strings.HasPrefix(name, "_") && !strings.HasPrefix(name, "?") {
					vars = append(vars, name)
					seen[name] = true
				}
			}
		}
		if atom.Pred == "is" && atom.Expr != nil {
			extractExprVars(atom.Expr, &vars, seen)
		}
	}
	return vars
}

func extractExprVars(expr syntax.Expr, vars *[]string, seen map[string]bool) {
	switch e := expr.(type) {
	case syntax.TermExpr:
		if v, ok := e.Term.(datalog.Variable); ok {
			name := string(v)
			if !seen[name] && !strings.HasPrefix(name, "_") && !strings.HasPrefix(name, "?") {
				*vars = append(*vars, name)
				seen[name] = true
			}
		}
	case syntax.BinExpr:
		extractExprVars(e.Left, vars, seen)
		extractExprVars(e.Right, vars, seen)
	}
}
