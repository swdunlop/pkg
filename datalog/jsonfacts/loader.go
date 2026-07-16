package jsonfacts

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
	"swdunlop.dev/pkg/datalog"
	"swdunlop.dev/pkg/datalog/internal/interned"
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
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	var facts []datalog.Fact
	counter := &idCounter{}
	for _, src := range cfg.Sources {
		srcFacts, err := loadSource(src, fsys, counter, cfg.OnMappingError)
		if err != nil {
			return nil, fmt.Errorf("source %s: %w", src.File, err)
		}
		facts = append(facts, srcFacts...)
	}

	if len(cfg.Matchers) > 0 {
		derived, err := applyMatchers(facts, cfg.Matchers, cfg.OnMatcherWarning)
		if err != nil {
			return nil, fmt.Errorf("matchers: %w", err)
		}
		facts = append(facts, derived...)
	}

	if cfg.OnTypeError != nil && len(cfg.Declarations) > 0 {
		ds := datalog.NewDeclarationSet(func(yield func(datalog.Declaration) bool) {
			for _, d := range cfg.Declarations {
				if !yield(d) {
					return
				}
			}
		})
		for _, f := range facts {
			if err := ds.CheckFact(f); err != nil {
				cfg.OnTypeError(err)
			}
		}
	}

	builder := memory.NewBuilder()
	for _, d := range cfg.Declarations {
		builder.AddDeclaration(d)
	}
	for _, f := range facts {
		if err := builder.AddFact(f); err != nil {
			return nil, err
		}
	}
	return builder.Build(), nil
}

// utf8BOM is the UTF-8 encoding of U+FEFF (byte order mark), which some
// JSONL producers prepend to a file. See its use in loadSource.
var utf8BOM = []byte{0xEF, 0xBB, 0xBF}

// idCounter generates monotonic synthetic IDs for fresh_id() calls.
type idCounter struct {
	next uint64
}

func (c *idCounter) freshID() datalog.ID {
	id := datalog.ID(c.next)
	c.next++
	return id
}

// compiledMapping holds pre-compiled expr programs for a mapping. argSrcs and
// filterSrc retain the original expression text solely so OnMappingError
// messages can name the offending expression (the compiled *vm.Program does
// not retain it).
type compiledMapping struct {
	predicate  string
	args       []*vm.Program
	argSrcs    []string
	filter     *vm.Program // nil if no filter
	filterSrc  string
	imperative *vm.Program // non-nil for imperative mode
}

// loadSource reads a single JSONL source from fsys and returns all generated
// facts. onMappingError, if non-nil, is called for each record where a
// mapping's filter or arg expression:
//   - fails to evaluate outright (a genuine expr runtime error, e.g. indexing
//     past the end of a string), which drops that mapping's fact for the
//     record;
//   - is a filter that evaluates to something other than a literal bool
//     (including nil), which is treated as "don't match" and also drops the
//     fact -- expr-lang does not error when a map field access like
//     value.foo misses, it simply yields nil, so a field-name typo in a
//     filter would otherwise silently and permanently exclude every record
//     with no indication why;
//   - is an arg that evaluates to nil, which still emits a Null term for
//     that arg (unchanged behavior -- nothing in this package relies on a
//     *missing* field's arg evaluating to nil as an intentional
//     "optional field" mechanism; see doc.go), but is now reported since it
//     is otherwise indistinguishable from a genuine field-name typo.

func loadSource(src Source, fsys fs.FS, counter *idCounter, onMappingError func(error)) ([]datalog.Fact, error) {
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
		// Strip a leading UTF-8 BOM (U+FEFF encodes as EF BB BF), which
		// bytes.TrimSpace does not treat as whitespace since it is not one.
		// Stripping it from every line (not just the first) handles both a
		// BOM at the very start of the file, preceding a valid JSON object
		// on the same line, and the degenerate case of a line containing
		// only a BOM, which without this falls through to the JSON decoder
		// below and aborts the whole load with a decode error instead of
		// being skipped like any other blank line.
		line = bytes.TrimPrefix(line, utf8BOM)
		if len(bytes.TrimSpace(line)) == 0 {
			// Empty or whitespace-only lines are not data; json.Decoder
			// would report a bare "EOF" for one (there is no JSON value to
			// decode), which previously aborted the entire load instead of
			// just skipping the blank line, as blank lines between JSONL
			// records are normally expected to be harmless.
			continue
		}

		dec := json.NewDecoder(bytes.NewReader(line))
		dec.UseNumber()
		var obj map[string]any
		if err := dec.Decode(&obj); err != nil {
			return nil, fmt.Errorf("line %d: %w", lineNum, err)
		}
		// A JSONL line must contain exactly one JSON value: confirm nothing
		// follows it (other than whitespace, which dec.Token skips), the
		// same way canon.ParseComposite checks for trailing data after
		// decoding a single composite. Without this, "{...} GARBAGE" quietly
		// loads only the leading object and drops the trailing garbage with
		// no indication the line was malformed.
		if _, err := dec.Token(); err != io.EOF {
			return nil, fmt.Errorf("line %d: unexpected data after JSON value", lineNum)
		}
		resolveNumbers(obj)

		runEnv := map[string]any{"value": obj}

		for mi, cm := range compiled {
			if cm.imperative != nil {
				if _, err := expr.Run(cm.imperative, runEnv); err != nil {
					return nil, fmt.Errorf("%s: line %d: mapping %d: %w", src.File, lineNum, mi, err)
				}
				continue
			}

			if cm.filter != nil {
				result, err := expr.Run(cm.filter, runEnv)
				if err != nil {
					if onMappingError != nil {
						onMappingError(fmt.Errorf("%s: line %d: mapping %d predicate %q: filter %q: %w", src.File, lineNum, mi, cm.predicate, cm.filterSrc, err))
					}
					continue
				}
				b, ok := result.(bool)
				if !ok {
					// expr-lang does not error on a map field access that
					// misses (e.g. value.usrname against a record with only
					// "username") -- it yields nil, which combined with a
					// comparison the schema author never intended can look
					// like "no match" for reasons that have nothing to do
					// with the record's data. Any non-bool result (nil or
					// otherwise) is reported rather than silently treated as
					// false, since a filter is defined to yield a bool.
					if onMappingError != nil {
						onMappingError(fmt.Errorf("%s: line %d: mapping %d predicate %q: filter %q evaluated to non-bool result %#v (%T) instead of true/false for record %v; record skipped", src.File, lineNum, mi, cm.predicate, cm.filterSrc, result, result, obj))
					}
					continue
				}
				if !b {
					continue
				}
			}

			terms := make([]datalog.Constant, len(cm.args))
			skip := false
			for j, prog := range cm.args {
				result, err := expr.Run(prog, runEnv)
				if err != nil {
					if onMappingError != nil {
						onMappingError(fmt.Errorf("%s: line %d: mapping %d predicate %q: arg %d %q: %w", src.File, lineNum, mi, cm.predicate, j, cm.argSrcs[j], err))
					}
					skip = true
					break
				}
				if result == nil {
					// Same field-name-typo hazard as the filter case above:
					// a missing map key evaluates to nil without an expr
					// error, so this is otherwise indistinguishable from a
					// genuine typo. Unlike the filter case, there is no
					// evidence anything relies on a missing field's arg
					// intentionally producing a Null term (see doc.go), so
					// this only adds observability -- the Null term below is
					// still emitted, matching prior behavior.
					if onMappingError != nil {
						onMappingError(fmt.Errorf("%s: line %d: mapping %d predicate %q: arg %d %q evaluated to nil for record %v; emitting a Null term", src.File, lineNum, mi, cm.predicate, j, cm.argSrcs[j], obj))
					}
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

// resolveNumbers walks a decoded JSON value in place, replacing every
// json.Number leaf with an int64 (when the literal is exact and fits) or a
// float64 otherwise. The JSONL decoder runs with UseNumber() so that large
// integers (beyond float64's 2^53 exact range, e.g. IDs, hashes, and
// nanosecond timestamps) survive decoding without precision loss; this walk
// then converts those numbers into plain Go numeric types so expr mapping
// and filter expressions (which do not support arithmetic or comparisons on
// json.Number) keep working exactly as they did before UseNumber() was
// introduced, while retaining int64 exactness that plain json.Unmarshal into
// float64 would have destroyed.
func resolveNumbers(v any) any {
	switch val := v.(type) {
	case json.Number:
		return numberToConstantValue(val)
	case map[string]any:
		for k, elem := range val {
			val[k] = resolveNumbers(elem)
		}
		return val
	case []any:
		for i, elem := range val {
			val[i] = resolveNumbers(elem)
		}
		return val
	default:
		return v
	}
}

// numberToConstantValue converts a json.Number into an int64 when the
// literal has no fraction or exponent and fits in 64 bits, else a float64.
func numberToConstantValue(n json.Number) any {
	if i, err := strconv.ParseInt(n.String(), 10, 64); err == nil {
		return i
	}
	f, err := n.Float64()
	if err != nil {
		// Should not happen for valid JSON numbers; fall back to the
		// original string representation via normalizeToConstant.
		return n
	}
	return f
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
			cm.argSrcs = append(cm.argSrcs, argExpr)
		}
		if m.Filter != "" {
			// Deliberately not compiled with expr.AsBool(): since env's
			// "value" field types are all `any` (the JSONL record is decoded
			// into map[string]any), expr's compile-time checker cannot
			// statically reject a dynamically-typed filter, so AsBool()'s
			// only real effect here is at runtime -- and there, when the
			// expression's actual result is nil (e.g. a missing-field access
			// with no comparison around it), the VM silently coerces it to
			// the zero value for bool (false) rather than surfacing it,
			// which is exactly the silent-typo hazard this package's
			// OnMappingError hook exists to catch. Compiling without
			// AsBool() gets the raw result back (nil is nil, not false) so
			// the runtime check below can tell a genuine `false` from a
			// missing/malformed result.
			prog, err := expr.Compile(m.Filter, expr.Env(env))
			if err != nil {
				return nil, fmt.Errorf("compiling filter %q for %s: %w", m.Filter, m.Predicate, err)
			}
			cm.filter = prog
			cm.filterSrc = m.Filter
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
// Objects and arrays become atomic Composite terms, so a mapping may assert
// value (or any sub-object expression result) whole and let rules destructure
// it lazily.
func normalizeToConstant(v any) datalog.Constant {
	switch val := v.(type) {
	case nil:
		return datalog.Null{}
	case datalog.ID:
		return val
	case datalog.Constant:
		return val
	case map[string]any, []any:
		c, err := datalog.NewComposite(val)
		if err != nil {
			// NaN or unsupported nested values: fall back to stringification.
			return datalog.String(fmt.Sprintf("%v", val))
		}
		return c
	case string:
		return datalog.String(val)
	case int:
		return datalog.Integer(int64(val))
	case int64:
		return datalog.Integer(val)
	case float64:
		// Route through NormalizeNumeric rather than doing our own
		// int64(val)/float64(i) round-trip: the round-trip guard is
		// unsound for out-of-range floats because Go's float64->int64
		// conversion is implementation-defined. On arm64, FCVTZS
		// saturates 2^63 to MaxInt64 and float64(MaxInt64) rounds back
		// up to exactly 2^63, so the round-trip passes and the value
		// silently becomes Integer(MaxInt64) -- one off and divergent
		// from amd64, which keeps it a Float. NormalizeNumeric
		// range-checks before converting, so 2^63 stays a Float on
		// every platform. The value is a typed datalog.Integer before
		// it reaches the dict, so the dict's own NormalizeNumeric
		// (which only touches float64) cannot catch it; this must land
		// here.
		switch n := interned.NormalizeNumeric(val).(type) {
		case int64:
			return datalog.Integer(n)
		case float64:
			return datalog.Float(n)
		default:
			return datalog.Float(val)
		}
	case float32:
		return datalog.Float(float64(val))
	case bool:
		return datalog.Bool(val)
	case json.Number:
		switch n := numberToConstantValue(val).(type) {
		case int64:
			return datalog.Integer(n)
		case float64:
			return datalog.Float(n)
		default:
			return datalog.String(val.String())
		}
	default:
		return datalog.String(fmt.Sprintf("%v", val))
	}
}
