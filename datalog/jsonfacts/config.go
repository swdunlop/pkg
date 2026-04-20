package jsonfacts

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"net/netip"
	"os"
	"regexp"

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
	hasFromFiles := mc.ContainsFrom != "" || mc.StartsWithFrom != "" || mc.EndsWithFrom != "" ||
		mc.RegexMatchFrom != "" || mc.Base64From != "" || mc.Base64UTF16From != "" || mc.CIDRFrom != ""
	if !hasPatterns && !hasFromFiles {
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
