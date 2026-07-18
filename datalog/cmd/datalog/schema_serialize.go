package main

import (
	"encoding/json"
	"fmt"
	"sort"

	"gopkg.in/yaml.v3"
	"swdunlop.dev/pkg/datalog"
	"swdunlop.dev/pkg/datalog/jsonfacts"
)

// serializeConfigYAML renders cfg as YAML in the ONE canonical form every
// schema CRUD write produces (doc/features/workbench-v2.md design decision
// 4's "Deterministic serialization for agent writes... matchers/
// declarations sorted by key"): sources sorted by File, matchers sorted by
// the (predicate, term, case_insensitive, windash) key tuple (the same
// tuple design decision 1 keys put_matcher/delete_matcher by), and
// declarations sorted by (name, arity) — arity, not bare name, because
// datalog.NewDeclarationSet itself keys a DeclarationSet by (Name,
// len(Terms)) (datalog.go), so two declarations sharing a Name at
// different Terms lengths are legal, distinct entries the engine already
// disambiguates this way; sorting declarations by name alone would leave
// their relative order among same-named-different-arity entries
// unspecified, breaking the "serialize twice, byte-identical" contract this
// function exists for.
//
// A caller must call this on every value written by an agent (put_source,
// put_matcher, put_declaration, and their delete_ counterparts): the WHOLE
// file is rewritten in this canonical form on every write, per the spec's
// explicit trade-off. This means a human's hand-authored comments and
// key ordering in the schema YAML are LOST the first time an agent touches
// the file — deliberate, not a bug: the schema is jsonfacts' structured
// config, already fully described by Config's fields, unlike the rules/
// directory's per-group .dl text (which preserves an agent's or operator's
// exact formatting verbatim, since Datalog rule text has meaningful
// comments/docs that JSON/YAML struct fields cannot carry). An operator who
// wants hand-formatted schema YAML to survive should not let an agent write
// to that file — a fair comparison to vim's own "last writer wins" file
// semantics, just applied to structure instead of bytes.
//
// The implementation goes through the exact inverse of parseConfigFormat's
// read path (yaml.Unmarshal -> json.Marshal there; json.Marshal ->
// yaml.Marshal here, both via an untyped `any` intermediate) rather than a
// direct yaml.Marshal(cfg): Config's struct tags are `json:"..."` only (no
// `yaml:"..."` tags), and gopkg.in/yaml.v3 does not fall back to json tags
// on its own, so a direct yaml.Marshal would silently emit Go's default
// lower-cased field names (e.g. "casesensitive" or "CaseInsensitive"
// depending on version) instead of the schema's actual documented keys
// (case_insensitive, contains_from, ...). Routing through json.Marshal
// first guarantees the emitted YAML keys are byte-identical to what
// parseConfigFormat's "yaml" branch expects to read back.
func serializeConfigYAML(cfg jsonfacts.Config) (string, error) {
	sorted := sortConfigForSerialization(cfg)

	data, err := json.Marshal(sorted)
	if err != nil {
		return "", fmt.Errorf("serializing config: %w", err)
	}
	var raw any
	if err := json.Unmarshal(data, &raw); err != nil {
		return "", fmt.Errorf("serializing config: %w", err)
	}
	out, err := yaml.Marshal(raw)
	if err != nil {
		return "", fmt.Errorf("serializing config: %w", err)
	}
	return string(out), nil
}

// sortConfigForSerialization returns a copy of cfg (the slice headers are
// copied and re-sorted; Source/Matcher/Declaration values themselves are
// not deep-copied, but sort.Slice/sort.SliceStable never mutate a value in
// place beyond reordering, so this is safe against a caller's cfg) with
// Sources, Matchers, and Declarations sorted by their respective keys.
// Split out from serializeConfigYAML so a test can assert on the sorted
// Config directly, without going through a YAML round trip.
func sortConfigForSerialization(cfg jsonfacts.Config) jsonfacts.Config {
	sources := append([]jsonfacts.Source{}, cfg.Sources...)
	sort.Slice(sources, func(i, j int) bool { return sources[i].File < sources[j].File })

	matchers := append([]jsonfacts.Matcher{}, cfg.Matchers...)
	sort.Slice(matchers, func(i, j int) bool { return matcherKeyLess(matchers[i], matchers[j]) })

	decls := append([]datalog.Declaration{}, cfg.Declarations...)
	sort.Slice(decls, func(i, j int) bool { return declarationKeyLess(decls[i], decls[j]) })

	return jsonfacts.Config{Sources: sources, Matchers: matchers, Declarations: decls}
}

// matcherKey is the tuple design decision 1 keys put_matcher/delete_matcher
// by: (predicate, term, case_insensitive, windash). Two matchers sharing
// this whole tuple are indistinguishable to the CRUD surface (there is no
// further field to disambiguate them) — see putMatcher's doc comment for
// the collision rejection this implies for a CREATE.
type matcherKey struct {
	Predicate       string
	Term            int
	CaseInsensitive bool
	Windash         bool
}

func matcherKeyOf(m jsonfacts.Matcher) matcherKey {
	return matcherKey{Predicate: m.Predicate, Term: m.Term, CaseInsensitive: m.CaseInsensitive, Windash: m.Windash}
}

// matcherKeyLess orders two matchers by their key tuple, field by field.
func matcherKeyLess(a, b jsonfacts.Matcher) bool {
	ka, kb := matcherKeyOf(a), matcherKeyOf(b)
	if ka.Predicate != kb.Predicate {
		return ka.Predicate < kb.Predicate
	}
	if ka.Term != kb.Term {
		return ka.Term < kb.Term
	}
	if ka.CaseInsensitive != kb.CaseInsensitive {
		return !ka.CaseInsensitive // false < true
	}
	return !ka.Windash && kb.Windash // false < true
}

// declarationKey is (name, arity): see serializeConfigYAML's doc comment
// for why arity, not bare name, disambiguates two declarations that share a
// Name.
type declarationKey struct {
	Name  string
	Arity int
}

func declarationKeyOf(d datalog.Declaration) declarationKey {
	return declarationKey{Name: d.Name, Arity: len(d.Terms)}
}

// declarationKeyLess orders two declarations by (name, arity).
func declarationKeyLess(a, b datalog.Declaration) bool {
	ka, kb := declarationKeyOf(a), declarationKeyOf(b)
	if ka.Name != kb.Name {
		return ka.Name < kb.Name
	}
	return ka.Arity < kb.Arity
}

// validateConfigKeyUniqueness rejects a jsonfacts.Config in which two
// sources share a File key, two matchers share a (predicate, term,
// case_insensitive, windash) key, or two declarations share a (name, arity)
// key — the CRUD keys schema_crud.go's put_source/put_matcher/
// put_declaration address one item by (matcherKey/declarationKey above;
// Source.File is already its own key). This is the analogue of
// validateDeclarationTermNames (jsonfacts/config.go:187), which rejects a
// duplicate term-name key WITHIN one declaration; this rejects a duplicate
// CRUD key ACROSS a Config's own Sources/Matchers/Declarations slices, a
// case jsonfacts.Config.validate() does not check at all (a Config may
// legitimately hold two Source/Matcher/Declaration values whose OTHER
// fields differ, and nothing in that package's loader ever needed to
// address one of them individually — only this package's keyed CRUD
// surface does).
//
// Without this check, a config that already has a colliding pair (hand-
// authored, or produced before this validation existed) makes every keyed
// write on that key silently corrupt data: replaceMatcher/replaceSource/
// replaceDeclaration (schema_crud.go) match by key and replace EVERY
// colliding entry, so a single put_matcher/put_source/put_declaration
// against a duplicated key produces two IDENTICAL copies of the new content
// and silently drops both originals; the delete_ counterparts remove every
// colliding entry at once. See TODO.md's 2026-07-18 phase 1c/1d validation
// entry for the full repro.
//
// Called from the ONE chokepoint both config-load paths share:
// parseConfigFormat (used directly by both loadData's parseConfig call and
// prepareSchema, which is itself shared by startup/reload/every schema CRUD
// write's prepareSchemaWrite) — so neither entry point can forget it. A
// pre-existing config with a genuine collision (see this repo's
// examples/mordor.yaml and examples/finbench.yaml, both checked clean) would
// now fail to load; the operator's fix is to merge the colliding entries
// into one (a single Matcher already carries contains+regex+cidr+... at
// once, so the union is always expressible), never to widen a key or patch
// the replace/remove helpers to touch only one of the colliding entries
// (see schema_crud.go's putMatcher doc comment for why key-widening breaks
// "edit the matcher for predicate/term" addressing).
func validateConfigKeyUniqueness(cfg jsonfacts.Config) error {
	seenSources := make(map[string]bool, len(cfg.Sources))
	for _, s := range cfg.Sources {
		if seenSources[s.File] {
			return fmt.Errorf("config: duplicate source file %q: two sources share this key; merge their mappings into one source", s.File)
		}
		seenSources[s.File] = true
	}

	seenMatchers := make(map[matcherKey]bool, len(cfg.Matchers))
	for _, m := range cfg.Matchers {
		key := matcherKeyOf(m)
		if seenMatchers[key] {
			return fmt.Errorf("config: duplicate matcher for predicate %q term %d (case_insensitive=%v windash=%v): "+
				"two matchers share this key; merge their pattern lists into one matcher",
				key.Predicate, key.Term, key.CaseInsensitive, key.Windash)
		}
		seenMatchers[key] = true
	}

	seenDeclarations := make(map[declarationKey]bool, len(cfg.Declarations))
	for _, d := range cfg.Declarations {
		key := declarationKeyOf(d)
		if seenDeclarations[key] {
			return fmt.Errorf("config: duplicate declaration for predicate %q arity %d: "+
				"two declarations share this key; merge them into one declaration", key.Name, key.Arity)
		}
		seenDeclarations[key] = true
	}

	return nil
}
