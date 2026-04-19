package jsonfacts

import (
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

// windashEntry maps a match variant back to its original pattern for emission.
type windashEntry struct {
	match    string
	original string
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
		entries = append(entries, windashEntry{match: p, original: p})
		if alt, ok := expandWindash(p); ok {
			entries = append(entries, windashEntry{match: alt, original: p})
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

func compileMatchers(matchers []Matcher) ([]compiledMatcher, error) {
	compiled := make([]compiledMatcher, len(matchers))
	for i, mc := range matchers {
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

		if len(mc.Contains) > 0 {
			cm.containsPatterns = mc.Contains
			if mc.Windash {
				cm.containsWindash = buildWindashEntries(mc.Contains)
				cm.containsGate = buildGateRegex(windashMatchPatterns(cm.containsWindash), "", "", ci)
			} else {
				cm.containsGate = buildGateRegex(mc.Contains, "", "", ci)
			}
			if ci {
				cm.containsLower = lowerAll(mc.Contains)
			}
		}
		if len(mc.StartsWith) > 0 {
			cm.startsWithPatterns = mc.StartsWith
			if mc.Windash {
				cm.startsWithWindash = buildWindashEntries(mc.StartsWith)
				cm.startsWithGate = buildGateRegex(windashMatchPatterns(cm.startsWithWindash), "^(?:", ")", ci)
			} else {
				cm.startsWithGate = buildGateRegex(mc.StartsWith, "^(?:", ")", ci)
			}
			if ci {
				cm.startsWithLower = lowerAll(mc.StartsWith)
			}
		}
		if len(mc.EndsWith) > 0 {
			cm.endsWithPatterns = mc.EndsWith
			cm.endsWithGate = buildGateRegex(mc.EndsWith, "(?:", ")$", ci)
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
			combined := strings.Join(mc.RegexMatch, "|")
			if ci {
				combined = "(?i)" + combined
			}
			re, err := regexp.Compile(combined)
			if err != nil {
				re = nil
			}
			cm.regexMatchGate = re
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
			cm.base64Gate = buildGateRegex(allSearch, "", "", false)
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

func buildGateRegex(patterns []string, prefix, suffix string, caseInsensitive bool) *regexp.Regexp {
	quoted := make([]string, len(patterns))
	for i, p := range patterns {
		quoted[i] = regexp.QuoteMeta(p)
	}
	e := prefix + strings.Join(quoted, "|") + suffix
	if caseInsensitive {
		e = "(?i)" + e
	}
	re, _ := regexp.Compile(e)
	return re
}

func lowerAll(ss []string) []string {
	out := make([]string, len(ss))
	for i, s := range ss {
		out[i] = strings.ToLower(s)
	}
	return out
}

// applyMatchers scans facts for matching predicates and emits derived match facts.
func applyMatchers(facts []datalog.Fact, matchers []Matcher) ([]datalog.Fact, error) {
	compiled, err := compileMatchers(matchers)
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
			if cm.containsGate != nil && cm.containsGate.MatchString(s) {
				if cm.containsWindash != nil {
					for _, we := range cm.containsWindash {
						if cm.caseInsensitive {
							if strings.Contains(sLower, strings.ToLower(we.match)) {
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
			if cm.startsWithGate != nil && cm.startsWithGate.MatchString(s) {
				if cm.startsWithWindash != nil {
					for _, we := range cm.startsWithWindash {
						if cm.caseInsensitive {
							if strings.HasPrefix(sLower, strings.ToLower(we.match)) {
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
			if cm.endsWithGate != nil && cm.endsWithGate.MatchString(s) {
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
