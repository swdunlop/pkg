package jsonfacts

import (
	"fmt"
	"net/netip"
	"regexp"
	"strings"

	"swdunlop.dev/pkg/datalog"
)

// compiledMatcher holds pre-compiled regex gates and pattern lists for a single Matcher.
type compiledMatcher struct {
	predicate       string
	term            int
	caseInsensitive bool

	containsPred   string
	startsWithPred string
	endsWithPred   string
	regexMatchPred string

	containsGate     *regexp.Regexp
	containsPatterns []string
	containsLower    []string
	containsWindash  []windashEntry

	startsWithGate     *regexp.Regexp
	startsWithPatterns []string
	startsWithLower    []string
	startsWithWindash  []windashEntry

	endsWithGate     *regexp.Regexp
	endsWithPatterns []string
	endsWithLower    []string

	regexMatchGate     *regexp.Regexp
	regexMatchPatterns []string
	regexMatchCompiled []*regexp.Regexp

	base64Gate     *regexp.Regexp
	base64Variants []base64Variant

	cidrNetworks    []netip.Prefix
	cidrNetworkStrs []string
	cidrIPGate      *regexp.Regexp
}

// windashEntry maps a match variant back to its original pattern for
// emission. matchLower is precomputed at compile time (rather than
// lower-cased per fact scanned) since a case-insensitive windash matcher
// otherwise re-lowers every pattern for every fact it checks.
type windashEntry struct {
	match      string
	matchLower string
	original   string
}

// matchPred builds a predicate name with modifier prefixes.
func matchPred(base string, ci, wd bool) string {
	prefix := ""
	if ci {
		prefix += "ci_"
	}
	if wd {
		prefix += "wd_"
	}
	return prefix + base
}

func expandWindash(pattern string) (string, bool) {
	if len(pattern) == 0 {
		return "", false
	}
	switch pattern[0] {
	case '-':
		return "/" + pattern[1:], true
	case '/':
		return "-" + pattern[1:], true
	}
	return "", false
}

func buildWindashEntries(patterns []string) []windashEntry {
	var entries []windashEntry
	for _, p := range patterns {
		entries = append(entries, windashEntry{match: p, matchLower: strings.ToLower(p), original: p})
		if alt, ok := expandWindash(p); ok {
			entries = append(entries, windashEntry{match: alt, matchLower: strings.ToLower(alt), original: p})
		}
	}
	return entries
}

func windashMatchPatterns(entries []windashEntry) []string {
	out := make([]string, len(entries))
	for i, e := range entries {
		out[i] = e.match
	}
	return out
}

// compileMatchers compiles matcher configs into their runtime form. onWarning,
// if non-nil, is called for each non-fatal issue encountered while compiling
// pre-filter gates -- currently, a combined gate regex that fails to compile
// (e.g. because it exceeds regexp's internal program-size limit for a very
// large pattern list). A gate is only a prefilter: matching still runs the
// real per-pattern checks (strings.Contains, strings.HasPrefix, ...), so a
// gate that fails to build must fall back to "no prefilter" (checking every
// fact) rather than either silently matching nothing or aborting the whole
// load, both of which would be wrong for a component whose only job is an
// optional speedup.
func compileMatchers(matchers []Matcher, onWarning func(error)) ([]compiledMatcher, error) {
	compiled := make([]compiledMatcher, len(matchers))
	for i, mc := range matchers {
		if err := mc.checkResolved(); err != nil {
			return nil, fmt.Errorf("matcher %d (%s): %w", i, mc.Predicate, err)
		}
		cm := compiledMatcher{
			predicate:       mc.Predicate,
			term:            mc.Term,
			caseInsensitive: mc.CaseInsensitive,
			containsPred:    matchPred("contains", mc.CaseInsensitive, mc.Windash),
			startsWithPred:  matchPred("starts_with", mc.CaseInsensitive, mc.Windash),
			endsWithPred:    matchPred("ends_with", mc.CaseInsensitive, false),
			regexMatchPred:  matchPred("regex_match", mc.CaseInsensitive, false),
		}

		ci := mc.CaseInsensitive

		warnGate := func(kind string, err error) {
			if onWarning == nil {
				return
			}
			onWarning(fmt.Errorf("matcher %d (%s): %s pre-filter regex failed to compile, falling back to unfiltered matching: %w", i, mc.Predicate, kind, err))
		}

		if len(mc.Contains) > 0 {
			cm.containsPatterns = mc.Contains
			var patterns []string
			if mc.Windash {
				cm.containsWindash = buildWindashEntries(mc.Contains)
				patterns = windashMatchPatterns(cm.containsWindash)
			} else {
				patterns = mc.Contains
			}
			gate, err := buildGateRegex(patterns, "", "", ci)
			if err != nil {
				warnGate("contains", err)
			} else {
				cm.containsGate = gate
			}
			if ci {
				cm.containsLower = lowerAll(mc.Contains)
			}
		}
		if len(mc.StartsWith) > 0 {
			cm.startsWithPatterns = mc.StartsWith
			var patterns []string
			if mc.Windash {
				cm.startsWithWindash = buildWindashEntries(mc.StartsWith)
				patterns = windashMatchPatterns(cm.startsWithWindash)
			} else {
				patterns = mc.StartsWith
			}
			gate, err := buildGateRegex(patterns, "^(?:", ")", ci)
			if err != nil {
				warnGate("starts_with", err)
			} else {
				cm.startsWithGate = gate
			}
			if ci {
				cm.startsWithLower = lowerAll(mc.StartsWith)
			}
		}
		if len(mc.EndsWith) > 0 {
			cm.endsWithPatterns = mc.EndsWith
			gate, err := buildGateRegex(mc.EndsWith, "(?:", ")$", ci)
			if err != nil {
				warnGate("ends_with", err)
			} else {
				cm.endsWithGate = gate
			}
			if ci {
				cm.endsWithLower = lowerAll(mc.EndsWith)
			}
		}
		if len(mc.RegexMatch) > 0 {
			cm.regexMatchPatterns = mc.RegexMatch
			cm.regexMatchCompiled = make([]*regexp.Regexp, len(mc.RegexMatch))
			for j, p := range mc.RegexMatch {
				if ci {
					p = "(?i)" + p
				}
				re, err := regexp.Compile(p)
				if err != nil {
					return nil, err
				}
				cm.regexMatchCompiled[j] = re
			}
			// Each alternate is wrapped in its own non-capturing group
			// before joining: without this, an inline flag such as
			// "(?-i)" in one pattern is not scoped to that pattern -- it
			// changes how every later alternate in the combined
			// alternation parses (Go's regexp/syntax, like Perl, scopes an
			// inline flag to the rest of its *enclosing group*, which
			// without wrapping is the whole combined expression). A
			// prefilter gate must never reject a string that the real,
			// individually-compiled pattern (regexMatchCompiled, compiled
			// and applied to each pattern separately above) would accept.
			wrapped := make([]string, len(mc.RegexMatch))
			for j, p := range mc.RegexMatch {
				wrapped[j] = "(?:" + p + ")"
			}
			combined := strings.Join(wrapped, "|")
			if ci {
				combined = "(?i)" + combined
			}
			re, err := regexp.Compile(combined)
			if err != nil {
				warnGate("regex_match", err)
			} else {
				cm.regexMatchGate = re
			}
		}

		var allBase64 []base64Variant
		if len(mc.Base64) > 0 {
			allBase64 = append(allBase64, compileBase64Patterns(mc.Base64, false, "base64_contains")...)
		}
		if len(mc.Base64UTF16) > 0 {
			allBase64 = append(allBase64, compileBase64Patterns(mc.Base64UTF16, true, "base64_utf16le_contains")...)
		}
		if len(allBase64) > 0 {
			cm.base64Variants = allBase64
			var allSearch []string
			for _, bv := range allBase64 {
				allSearch = append(allSearch, bv.searchStrings...)
			}
			gate, err := buildGateRegex(allSearch, "", "", false)
			if err != nil {
				warnGate("base64", err)
			} else {
				cm.base64Gate = gate
			}
		}

		if len(mc.CIDR) > 0 {
			cm.cidrNetworks = make([]netip.Prefix, len(mc.CIDR))
			cm.cidrNetworkStrs = mc.CIDR
			for j, c := range mc.CIDR {
				prefix, err := netip.ParsePrefix(c)
				if err != nil {
					return nil, err
				}
				cm.cidrNetworks[j] = prefix
			}
			cm.cidrIPGate, _ = regexp.Compile(`\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}|:`)
		}

		compiled[i] = cm
	}
	return compiled, nil
}

// buildGateRegex builds a combined pre-filter regex matching any of patterns.
// Every pattern is regexp.QuoteMeta'd, so none of them can contribute an
// unescaped '(', '|', or '?' that would let one alternate's syntax bleed
// into another's -- unlike the raw (non-literal) patterns joined for the
// regex_match gate, this join is safe without per-alternate grouping. The
// error return must not be discarded by the caller: a gate is only a
// prefilter, so a compile failure (e.g. exceeding regexp's internal
// program-size limit for a very large pattern list) should fall back to no
// prefilter rather than silently matching nothing.
func buildGateRegex(patterns []string, prefix, suffix string, caseInsensitive bool) (*regexp.Regexp, error) {
	quoted := make([]string, len(patterns))
	for i, p := range patterns {
		quoted[i] = regexp.QuoteMeta(p)
	}
	e := prefix + strings.Join(quoted, "|") + suffix
	if caseInsensitive {
		e = "(?i)" + e
	}
	return regexp.Compile(e)
}

func lowerAll(ss []string) []string {
	out := make([]string, len(ss))
	for i, s := range ss {
		out[i] = strings.ToLower(s)
	}
	return out
}

// applyMatchers scans facts for matching predicates and emits derived match
// facts. onWarning, if non-nil, receives non-fatal issues encountered while
// compiling matcher pre-filter gates; see compileMatchers.
func applyMatchers(facts []datalog.Fact, matchers []Matcher, onWarning func(error)) ([]datalog.Fact, error) {
	compiled, err := compileMatchers(matchers, onWarning)
	if err != nil {
		return nil, err
	}

	byPred := make(map[string][]int)
	for i, cm := range compiled {
		byPred[cm.predicate] = append(byPred[cm.predicate], i)
	}

	type matchKey struct {
		pred, value, pattern string
	}
	seen := make(map[matchKey]struct{})
	var result []datalog.Fact

	emit := func(pred, value, pattern string) {
		mk := matchKey{pred, value, pattern}
		if _, ok := seen[mk]; ok {
			return
		}
		seen[mk] = struct{}{}
		result = append(result, datalog.Fact{
			Name:  pred,
			Terms: []datalog.Constant{datalog.String(value), datalog.String(pattern)},
		})
	}

	for i := range facts {
		indices, ok := byPred[facts[i].Name]
		if !ok {
			continue
		}
		for _, ci := range indices {
			cm := &compiled[ci]
			if cm.term >= len(facts[i].Terms) {
				continue
			}
			sc, ok := facts[i].Terms[cm.term].(datalog.String)
			if !ok {
				continue
			}
			s := string(sc)

			var sLower string
			if cm.caseInsensitive {
				sLower = strings.ToLower(s)
			}

			// --- Contains ---
			// A nil gate means "no pre-filter compiled" (either there was
			// nothing to gate on, or the combined gate regex failed to
			// build and compileMatchers already reported that via
			// onWarning) -- it must never be treated as "match nothing",
			// since the gate is only a speedup over the per-pattern checks
			// below, not a correctness requirement.
			if cm.containsGate == nil || cm.containsGate.MatchString(s) {
				if cm.containsWindash != nil {
					for _, we := range cm.containsWindash {
						if cm.caseInsensitive {
							if strings.Contains(sLower, we.matchLower) {
								emit(cm.containsPred, s, we.original)
							}
						} else {
							if strings.Contains(s, we.match) {
								emit(cm.containsPred, s, we.original)
							}
						}
					}
				} else if cm.caseInsensitive {
					for j, p := range cm.containsPatterns {
						if strings.Contains(sLower, cm.containsLower[j]) {
							emit(cm.containsPred, s, p)
						}
					}
				} else {
					for _, p := range cm.containsPatterns {
						if strings.Contains(s, p) {
							emit(cm.containsPred, s, p)
						}
					}
				}
			}

			// --- StartsWith ---
			if cm.startsWithGate == nil || cm.startsWithGate.MatchString(s) {
				if cm.startsWithWindash != nil {
					for _, we := range cm.startsWithWindash {
						if cm.caseInsensitive {
							if strings.HasPrefix(sLower, we.matchLower) {
								emit(cm.startsWithPred, s, we.original)
							}
						} else {
							if strings.HasPrefix(s, we.match) {
								emit(cm.startsWithPred, s, we.original)
							}
						}
					}
				} else if cm.caseInsensitive {
					for j, p := range cm.startsWithPatterns {
						if strings.HasPrefix(sLower, cm.startsWithLower[j]) {
							emit(cm.startsWithPred, s, p)
						}
					}
				} else {
					for _, p := range cm.startsWithPatterns {
						if strings.HasPrefix(s, p) {
							emit(cm.startsWithPred, s, p)
						}
					}
				}
			}

			// --- EndsWith ---
			if cm.endsWithGate == nil || cm.endsWithGate.MatchString(s) {
				if cm.caseInsensitive {
					for j, p := range cm.endsWithPatterns {
						if strings.HasSuffix(sLower, cm.endsWithLower[j]) {
							emit(cm.endsWithPred, s, p)
						}
					}
				} else {
					for _, p := range cm.endsWithPatterns {
						if strings.HasSuffix(s, p) {
							emit(cm.endsWithPred, s, p)
						}
					}
				}
			}

			// --- RegexMatch ---
			if len(cm.regexMatchCompiled) > 0 {
				if cm.regexMatchGate == nil || cm.regexMatchGate.MatchString(s) {
					for j, re := range cm.regexMatchCompiled {
						if re.MatchString(s) {
							emit(cm.regexMatchPred, s, cm.regexMatchPatterns[j])
						}
					}
				}
			}

			// --- Base64 ---
			if len(cm.base64Variants) > 0 {
				if cm.base64Gate == nil || cm.base64Gate.MatchString(s) {
					for _, bv := range cm.base64Variants {
						for _, ss := range bv.searchStrings {
							if strings.Contains(s, ss) {
								emit(bv.pred, s, bv.originalPattern)
								break
							}
						}
					}
				}
			}

			// --- CIDR ---
			if len(cm.cidrNetworks) > 0 {
				if cm.cidrIPGate == nil || cm.cidrIPGate.MatchString(s) {
					addr, err := netip.ParseAddr(s)
					if err == nil {
						for j, prefix := range cm.cidrNetworks {
							if prefix.Contains(addr) {
								emit("cidr_match", s, cm.cidrNetworkStrs[j])
							}
						}
					}
				}
			}
		}
	}

	return result, nil
}
