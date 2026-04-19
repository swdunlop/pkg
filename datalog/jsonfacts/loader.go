package jsonfacts

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"regexp"
	"strings"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
	"swdunlop.dev/pkg/datalog"
	"swdunlop.dev/pkg/datalog/memory"
)

// LoadDir loads JSONL files from dir according to the Config, applies matchers,
// and returns a memory.Database containing all loaded and derived facts.
func (cfg *Config) LoadDir(dir string) (*memory.Database, error) {
	return cfg.LoadFS(os.DirFS(dir))
}

// LoadFS loads JSONL files from fsys according to the Config, applies matchers,
// and returns a memory.Database containing all loaded and derived facts.
func (cfg *Config) LoadFS(fsys fs.FS) (*memory.Database, error) {
	var facts []datalog.Fact
	counter := &idCounter{}
	for _, src := range cfg.Sources {
		srcFacts, err := loadSource(src, fsys, counter)
		if err != nil {
			return nil, fmt.Errorf("source %s: %w", src.File, err)
		}
		facts = append(facts, srcFacts...)
	}

	if len(cfg.Matchers) > 0 {
		derived, err := applyMatchers(facts, cfg.Matchers)
		if err != nil {
			return nil, fmt.Errorf("matchers: %w", err)
		}
		facts = append(facts, derived...)
	}

	builder := memory.NewBuilder()
	for _, d := range cfg.Declarations {
		builder.AddDeclaration(d)
	}
	for _, f := range facts {
		builder.AddFact(f)
	}
	return builder.Build(), nil
}

// idCounter generates monotonic synthetic IDs for fresh_id() calls.
type idCounter struct {
	next uint64
}

func (c *idCounter) freshID() datalog.ID {
	id := datalog.ID(c.next)
	c.next++
	return id
}

// compiledMapping holds pre-compiled expr programs for a mapping.
type compiledMapping struct {
	predicate  string
	args       []*vm.Program
	filter     *vm.Program // nil if no filter
	imperative *vm.Program // non-nil for imperative mode
}

// loadSource reads a single JSONL source from fsys and returns all generated facts.
func loadSource(src Source, fsys fs.FS, counter *idCounter) ([]datalog.Fact, error) {
	var facts []datalog.Fact

	assertFn := func(pred string, args []any) {
		terms := make([]datalog.Constant, len(args))
		for j, a := range args {
			terms[j] = normalizeToConstant(a)
		}
		facts = append(facts, datalog.Fact{Name: pred, Terms: terms})
	}

	compiled, err := compileMappings(src.Mappings, counter, assertFn)
	if err != nil {
		return nil, err
	}

	f, err := fsys.Open(src.File)
	if err != nil {
		return nil, fmt.Errorf("opening %s: %w", src.File, err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 1024*1024), 10*1024*1024)
	lineNum := 0

	for scanner.Scan() {
		lineNum++
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var obj map[string]any
		if err := json.Unmarshal(line, &obj); err != nil {
			return nil, fmt.Errorf("line %d: %w", lineNum, err)
		}

		runEnv := map[string]any{"value": obj}

		for _, cm := range compiled {
			if cm.imperative != nil {
				if _, err := expr.Run(cm.imperative, runEnv); err != nil {
					continue
				}
				continue
			}

			if cm.filter != nil {
				result, err := expr.Run(cm.filter, runEnv)
				if err != nil || result != true {
					continue
				}
			}

			terms := make([]datalog.Constant, len(cm.args))
			skip := false
			for j, prog := range cm.args {
				result, err := expr.Run(prog, runEnv)
				if err != nil {
					skip = true
					break
				}
				terms[j] = normalizeToConstant(result)
			}
			if skip {
				continue
			}

			facts = append(facts, datalog.Fact{Name: cm.predicate, Terms: terms})
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("reading %s: %w", src.File, err)
	}

	return facts, nil
}

// compileMappings compiles all mappings for a source.
func compileMappings(mappings []Mapping, counter *idCounter, assertFn func(string, []any)) ([]compiledMapping, error) {
	env := map[string]any{"value": map[string]any{}}

	compiled := make([]compiledMapping, len(mappings))
	for i, m := range mappings {
		if m.Expr != "" {
			prog, err := compileImperative(m.Expr, env, counter, assertFn)
			if err != nil {
				return nil, fmt.Errorf("compiling expr for mapping %d: %w", i, err)
			}
			compiled[i] = compiledMapping{imperative: prog}
			continue
		}
		cm := compiledMapping{predicate: m.Predicate}
		for _, argExpr := range m.Args {
			prog, err := expr.Compile(argExpr, expr.Env(env))
			if err != nil {
				return nil, fmt.Errorf("compiling arg expr %q for %s: %w", argExpr, m.Predicate, err)
			}
			cm.args = append(cm.args, prog)
		}
		if m.Filter != "" {
			prog, err := expr.Compile(m.Filter, expr.Env(env), expr.AsBool())
			if err != nil {
				return nil, fmt.Errorf("compiling filter %q for %s: %w", m.Filter, m.Predicate, err)
			}
			cm.filter = prog
		}
		compiled[i] = cm
	}
	return compiled, nil
}

// compileImperative compiles an imperative expr program with fresh_id(),
// assert(), and match_* builtins registered.
func compileImperative(exprSrc string, env map[string]any, counter *idCounter, assertFn func(string, []any)) (*vm.Program, error) {
	freshIDOpt := expr.Function("fresh_id", func(params ...any) (any, error) {
		return counter.freshID(), nil
	})
	assertOpt := expr.Function("assert", func(params ...any) (any, error) {
		if len(params) != 2 {
			return nil, fmt.Errorf("assert: expected 2 arguments (predicate, args), got %d", len(params))
		}
		pred, ok := params[0].(string)
		if !ok {
			return nil, fmt.Errorf("assert: first argument must be a string predicate name, got %T", params[0])
		}
		args, ok := params[1].([]any)
		if !ok {
			return nil, fmt.Errorf("assert: second argument must be an array, got %T", params[1])
		}
		assertFn(pred, args)
		return true, nil
	})
	matchContainsOpt := expr.Function("match_contains",
		makeMatchFn("match_contains", strings.Contains, assertFn))
	matchStartsWithOpt := expr.Function("match_starts_with",
		makeMatchFn("match_starts_with", strings.HasPrefix, assertFn))
	matchEndsWithOpt := expr.Function("match_ends_with",
		makeMatchFn("match_ends_with", strings.HasSuffix, assertFn))
	matchRegexOpt := expr.Function("match_regex",
		makeMatchRegexFn(assertFn))
	return expr.Compile(exprSrc, expr.Env(env), freshIDOpt, assertOpt,
		matchContainsOpt, matchStartsWithOpt, matchEndsWithOpt, matchRegexOpt)
}

// makeMatchFn returns an expr function that checks a haystack against each
// pattern in an array, asserting pred(key, pattern) for each match.
func makeMatchFn(name string, match func(string, string) bool, assertFn func(string, []any)) func(params ...any) (any, error) {
	return func(params ...any) (any, error) {
		if len(params) != 4 {
			return nil, fmt.Errorf("%s: expected 4 arguments (pred, key, haystack, patterns), got %d", name, len(params))
		}
		pred, ok := params[0].(string)
		if !ok {
			return nil, fmt.Errorf("%s: first argument must be a string predicate name", name)
		}
		key := params[1]
		hs, ok := params[2].(string)
		if !ok {
			return 0, nil
		}
		patterns, ok := params[3].([]any)
		if !ok {
			return nil, fmt.Errorf("%s: fourth argument must be an array", name)
		}
		count := 0
		for _, p := range patterns {
			ps, ok := p.(string)
			if !ok {
				continue
			}
			if match(hs, ps) {
				assertFn(pred, []any{key, ps})
				count++
			}
		}
		return count, nil
	}
}

// makeMatchRegexFn returns an expr function like makeMatchFn but using
// regexp matching. Compiled patterns are cached across calls.
func makeMatchRegexFn(assertFn func(string, []any)) func(params ...any) (any, error) {
	cache := make(map[string]*regexp.Regexp)
	return func(params ...any) (any, error) {
		if len(params) != 4 {
			return nil, fmt.Errorf("match_regex: expected 4 arguments (pred, key, haystack, patterns), got %d", len(params))
		}
		pred, ok := params[0].(string)
		if !ok {
			return nil, fmt.Errorf("match_regex: first argument must be a string predicate name")
		}
		key := params[1]
		hs, ok := params[2].(string)
		if !ok {
			return 0, nil
		}
		patterns, ok := params[3].([]any)
		if !ok {
			return nil, fmt.Errorf("match_regex: fourth argument must be an array")
		}
		count := 0
		for _, p := range patterns {
			ps, ok := p.(string)
			if !ok {
				continue
			}
			re, exists := cache[ps]
			if !exists {
				var err error
				re, err = regexp.Compile(ps)
				if err != nil {
					continue
				}
				cache[ps] = re
			}
			if re.MatchString(hs) {
				assertFn(pred, []any{key, ps})
				count++
			}
		}
		return count, nil
	}
}

// normalizeToConstant converts an expr output value to a typed datalog.Constant.
func normalizeToConstant(v any) datalog.Constant {
	switch val := v.(type) {
	case datalog.ID:
		return val
	case datalog.Constant:
		return val
	case string:
		return datalog.String(val)
	case int:
		return datalog.Integer(int64(val))
	case int64:
		return datalog.Integer(val)
	case float64:
		i := int64(val)
		if float64(i) == val {
			return datalog.Integer(i)
		}
		return datalog.Float(val)
	case float32:
		return datalog.Float(float64(val))
	case bool:
		if val {
			return datalog.Integer(1)
		}
		return datalog.Integer(0)
	case json.Number:
		if i, err := val.Int64(); err == nil {
			return datalog.Integer(i)
		}
		if f, err := val.Float64(); err == nil {
			return datalog.Float(f)
		}
		return datalog.String(val.String())
	default:
		return datalog.String(fmt.Sprintf("%v", val))
	}
}
