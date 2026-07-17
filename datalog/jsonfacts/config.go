package jsonfacts

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"net/netip"
	"os"
	"regexp"
	"strconv"

	"swdunlop.dev/pkg/datalog"
)

// Config holds the combined schema for loading JSONL files into a Datalog database.
// It accumulates sources, matchers, and declarations from one or more schema files.
// Config is JSON-serializable so a merged configuration can be saved and reloaded.
type Config struct {
	Sources      []Source              `json:"sources,omitempty"`
	Matchers     []Matcher             `json:"matchers,omitempty"`
	Declarations []datalog.Declaration `json:"declarations,omitempty"`

	// OnTypeError is called when a loaded fact's terms don't match the declared types.
	// When nil, no type checking is performed during loading.
	OnTypeError func(error) `json:"-"`

	// OnMappingError is called when a mapping's filter or arg expression
	// behaves unexpectedly for a JSONL record (the same way OnTypeError
	// surfaces facts that loaded but don't match the schema, this surfaces
	// records a mapping quietly dropped or produced a suspicious term for).
	// It fires for three distinct situations, all reported as plain errors
	// naming the expression, the record, and (for the filter case) the
	// actual result:
	//   - a filter, arg, or imperative (expr:) mapping expression that fails
	//     to evaluate outright (a genuine expr runtime error), which drops
	//     the mapping's fact (or, for an imperative mapping, that record's
	//     assert() calls for that mapping) for the record -- both mapping
	//     modes agree on this fatal-vs-skip boundary: only a compile-time
	//     error is fatal, a per-record runtime error is not;
	//   - a filter that evaluates to anything other than a literal true/false
	//     (including nil), which is treated as "no match" and also drops the
	//     fact -- notably, expr-lang does not error on a map field access
	//     that misses (e.g. value.usrname against a record with only
	//     "username"), it simply yields nil, so a field-name typo in a
	//     filter would otherwise silently and permanently exclude matching
	//     records with no indication why;
	//   - an arg that evaluates to nil, for the same field-name-typo reason;
	//     the resulting fact still gets a Null term for that arg (unchanged
	//     behavior), this only adds observability.
	// When nil, all of the above are ignored and the record/arg is handled
	// exactly as if a caller had wired an OnMappingError that does nothing,
	// i.e. as before this hook existed.
	OnMappingError func(error) `json:"-"`

	// OnMatcherWarning is called when a matcher's combined pre-filter gate
	// regex fails to compile (e.g. a very large contains_from pattern list
	// exceeding regexp's internal program-size limit) and matching falls
	// back to checking every fact without that speedup. The fallback is
	// always correct (the gate is only a prefilter), so this is a warning,
	// not an error; when nil, such fallbacks are silent.
	OnMatcherWarning func(error) `json:"-"`
}

// Source describes a single JSONL file and how to map its lines to facts.
type Source struct {
	File     string    `json:"file"`
	Mappings []Mapping `json:"mappings"`
}

// Mapping maps a JSONL line to predicate arguments.
// Either Predicate/Args/Filter (simple mode) or Expr (imperative mode) must be set, not both.
type Mapping struct {
	// Simple mode: each Args expression yields one term.
	Predicate string   `json:"predicate,omitempty"`
	Args      []string `json:"args,omitempty"`
	Filter    string   `json:"filter,omitempty"`

	// Imperative mode: expr program with assert()/fresh_id() calls.
	Expr string `json:"expr,omitempty"`
}

// Matcher declares string-matching patterns to apply to a predicate's term.
// After JSONL facts are loaded, matchers scan facts of the named predicate,
// extract the string at the given term index, and emit match facts
// (contains/2, starts_with/2, ends_with/2, regex_match/2, etc.).
type Matcher struct {
	Predicate       string `json:"predicate"`
	Term            int    `json:"term"`
	CaseInsensitive bool   `json:"case_insensitive,omitempty"`
	Windash         bool   `json:"windash,omitempty"`

	Contains        []string `json:"contains,omitempty"`
	ContainsFrom    string   `json:"contains_from,omitempty"`
	StartsWith      []string `json:"starts_with,omitempty"`
	StartsWithFrom  string   `json:"starts_with_from,omitempty"`
	EndsWith        []string `json:"ends_with,omitempty"`
	EndsWithFrom    string   `json:"ends_with_from,omitempty"`
	RegexMatch      []string `json:"regex_match,omitempty"`
	RegexMatchFrom  string   `json:"regex_match_from,omitempty"`
	Base64          []string `json:"base64,omitempty"`
	Base64From      string   `json:"base64_from,omitempty"`
	Base64UTF16     []string `json:"base64_utf16le,omitempty"`
	Base64UTF16From string   `json:"base64_utf16le_from,omitempty"`
	CIDR            []string `json:"cidr,omitempty"`
	CIDRFrom        string   `json:"cidr_from,omitempty"`
}

// LoadSchemaDir loads all JSON schema files from dir and merges them into
// the Config. Pattern files referenced by _from fields are resolved relative
// to dir at load time, making the Config self-contained.
func (cfg *Config) LoadSchemaDir(dir string) error {
	return cfg.LoadSchemaFS(os.DirFS(dir))
}

// LoadSchemaFS loads all JSON schema files from fsys and merges them into
// the Config. Pattern files referenced by _from fields are resolved from
// fsys at load time.
func (cfg *Config) LoadSchemaFS(fsys fs.FS) error {
	entries, err := fs.Glob(fsys, "*.json")
	if err != nil {
		return fmt.Errorf("listing schemas: %w", err)
	}
	for _, name := range entries {
		if err := cfg.loadSchemaFile(fsys, name); err != nil {
			return fmt.Errorf("schema %s: %w", name, err)
		}
	}
	return nil
}

func (cfg *Config) loadSchemaFile(fsys fs.FS, name string) error {
	data, err := fs.ReadFile(fsys, name)
	if err != nil {
		return err
	}
	var s Config
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("parsing: %w", err)
	}
	for i := range s.Matchers {
		if err := s.Matchers[i].resolveFromFS(fsys); err != nil {
			return fmt.Errorf("matcher %d: %w", i, err)
		}
	}
	if err := s.validate(); err != nil {
		return err
	}
	cfg.Sources = append(cfg.Sources, s.Sources...)
	cfg.Matchers = append(cfg.Matchers, s.Matchers...)
	cfg.Declarations = append(cfg.Declarations, s.Declarations...)
	return nil
}

func (cfg *Config) validate() error {
	for si, src := range cfg.Sources {
		for mi, m := range src.Mappings {
			if err := m.validate(); err != nil {
				return fmt.Errorf("source %d mapping %d: %w", si, mi, err)
			}
		}
	}
	for mi, mc := range cfg.Matchers {
		if err := mc.validate(); err != nil {
			return fmt.Errorf("matcher %d: %w", mi, err)
		}
	}
	for di, d := range cfg.Declarations {
		if err := validateDeclarationTermNames(d); err != nil {
			return fmt.Errorf("declaration %d (%s): %w", di, d.Name, err)
		}
	}
	return nil
}

// validateDeclarationTermNames rejects a declaration whose Terms contain a
// duplicate name, or a named term that collides with the positional key
// (Encoder's termKey uses strconv.Itoa(i) for any term without a name)
// another term in the same declaration would fall back to if it were
// unnamed. Both cases would make Encoder.Encode -- which builds the
// per-fact JSON object by writing obj[key] = ... for each term in order --
// silently overwrite an earlier term's value with a later one's (a plain Go
// map key collision, not an error), dropping a value with no indication
// anything went wrong. Catching this at declaration load time, before any
// fact is ever encoded, is the earliest chokepoint: it rejects the bad
// schema outright instead of letting every fact encoded under it silently
// lose data.
func validateDeclarationTermNames(d datalog.Declaration) error {
	seen := make(map[string]int, len(d.Terms))
	for i, t := range d.Terms {
		key := t.Name
		if key == "" {
			key = strconv.Itoa(i)
		}
		if prev, ok := seen[key]; ok {
			if t.Name == "" {
				return fmt.Errorf("term %d has no name and collides with the positional key %q already used by term %d", i, key, prev)
			}
			return fmt.Errorf("duplicate term name %q used by terms %d and %d", t.Name, prev, i)
		}
		seen[key] = i
	}
	// A single pass suffices for the positional-key collision too (e.g. a
	// term literally named "1" and an unnamed term at index 1): seen is
	// keyed by each term's *resolved* key (its own name if set, else its
	// index as a string), so a later term's resolved key colliding with an
	// earlier one's is caught above regardless of which one is named and
	// which is positional, and regardless of declaration order.
	return nil
}

func (m Mapping) validate() error {
	if m.Expr != "" && (m.Predicate != "" || len(m.Args) > 0 || m.Filter != "") {
		return fmt.Errorf("mapping has both 'expr' and 'predicate'/'args'/'filter'; these are mutually exclusive")
	}
	if m.Expr == "" && m.Predicate == "" {
		return fmt.Errorf("mapping must have either 'expr' or 'predicate'")
	}
	return nil
}

func (mc Matcher) validate() error {
	if mc.Predicate == "" {
		return fmt.Errorf("matcher must have a predicate")
	}
	if mc.Term < 0 {
		return fmt.Errorf("matcher term must be >= 0")
	}
	hasPatterns := len(mc.Contains) > 0 || len(mc.StartsWith) > 0 || len(mc.EndsWith) > 0 ||
		len(mc.RegexMatch) > 0 || len(mc.Base64) > 0 || len(mc.Base64UTF16) > 0 || len(mc.CIDR) > 0
	if !hasPatterns && !mc.hasFromFiles() {
		return fmt.Errorf("matcher must have at least one pattern list or _from file")
	}
	for _, p := range mc.RegexMatch {
		if _, err := regexp.Compile(p); err != nil {
			return fmt.Errorf("invalid regex %q: %w", p, err)
		}
	}
	for _, c := range mc.CIDR {
		if _, err := netip.ParsePrefix(c); err != nil {
			return fmt.Errorf("invalid CIDR %q: %w", c, err)
		}
	}
	return nil
}

// hasFromFiles reports whether any *_from field is still set, i.e. names a
// pattern file that has not yet been resolved into its corresponding inline
// list (resolveFromFS clears each *_from field to "" once it has merged
// that file's patterns into the inline slice and is the only thing that
// clears them). A Matcher that validates successfully (validate only checks
// that *some* pattern source, inline or _from, is present) can still have
// hasFromFiles true forever if ResolveFromFS/LoadSchemaFS's automatic
// resolution was never called -- see checkResolved, which turns that state
// into a load-time error instead of the silent "zero derived facts" it
// otherwise causes.
func (mc Matcher) hasFromFiles() bool {
	return mc.ContainsFrom != "" || mc.StartsWithFrom != "" || mc.EndsWithFrom != "" ||
		mc.RegexMatchFrom != "" || mc.Base64From != "" || mc.Base64UTF16From != "" || mc.CIDRFrom != ""
}

// checkResolved rejects a Matcher that still has an unresolved *_from field
// at the point matching is about to run (compileMatchers, the mechanism
// both applyMatchers/LoadFS and any other caller of compileMatchers go
// through). Without this, a matcher like {"contains_from": "iocs.txt"}
// passes validate (which only requires *some* pattern source, inline or
// _from) but, if ResolveFromFS was never called to turn that file into an
// inline list, compileMatchers silently reads zero patterns from the
// (still-empty) inline Contains slice and the matcher derives no facts at
// all -- a total, silent loss of every match this matcher was meant to
// produce, with no warning distinguishable from "the file legitimately
// contained no patterns". This check makes that distinction explicit:
// "never resolved" is now a clear error; "resolved to an empty file" (the
// *_from field is cleared to "" by resolveFromFS regardless of how many
// patterns it found) is not, and continues to load with zero derived facts
// for that matcher as before.
func (mc Matcher) checkResolved() error {
	if mc.hasFromFiles() {
		return fmt.Errorf("matcher has an unresolved _from field; call Config.ResolveFromFS (or LoadSchemaDir/LoadSchemaFS, which do this automatically) before LoadFS/LoadDir")
	}
	return nil
}
