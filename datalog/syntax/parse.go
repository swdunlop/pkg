package syntax

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"

	"swdunlop.dev/pkg/datalog"
)

// tokenKind represents a lexer token type.
type tokenKind int

const (
	tokEOF       tokenKind = iota
	tokIdent               // identifier (predicate name or variable, depending on context)
	tokAnon                // ? (anonymous variable)
	tokString              // "quoted string"
	tokInt                 // integer literal
	tokFloat               // float literal
	tokLParen              // (
	tokRParen              // )
	tokComma               // ,
	tokDot                 // .
	tokImplies             // :-
	tokNot                 // not
	tokIs                  // is
	tokEquals              // =
	tokNotEquals           // !=
	tokLT                  // <
	tokGT                  // >
	tokLE                  // <=
	tokGE                  // >=
	tokPlus                // +
	tokMinus               // -
	tokStar                // *
	tokSlash               // /
	tokColon               // :
	tokLBrace              // {
	tokRBrace              // }
	tokLBracket            // [
	tokRBracket            // ]
	tokPipe                // |
)

type token struct {
	kind tokenKind
	val  string
	pos  int
}

// lexer tokenizes datalog input.
type lexer struct {
	input  string
	pos    int
	anonID int // counter for anonymous variables
}

func newLexer(input string) *lexer {
	return &lexer{input: input}
}

func (l *lexer) peek() byte {
	if l.pos >= len(l.input) {
		return 0
	}
	return l.input[l.pos]
}

func (l *lexer) advance() byte {
	b := l.input[l.pos]
	l.pos++
	return b
}

func (l *lexer) skipWhitespace() {
	for l.pos < len(l.input) {
		b := l.input[l.pos]
		if b == '%' {
			// line comment
			for l.pos < len(l.input) && l.input[l.pos] != '\n' {
				l.pos++
			}
			continue
		}
		if b == ' ' || b == '\t' || b == '\n' || b == '\r' {
			l.pos++
			continue
		}
		break
	}
}

func (l *lexer) next() token {
	l.skipWhitespace()
	if l.pos >= len(l.input) {
		return token{kind: tokEOF, pos: l.pos}
	}

	startPos := l.pos
	b := l.peek()

	switch {
	case b == '(':
		l.advance()
		return token{kind: tokLParen, val: "(", pos: startPos}
	case b == ')':
		l.advance()
		return token{kind: tokRParen, val: ")", pos: startPos}
	case b == '{':
		l.advance()
		return token{kind: tokLBrace, val: "{", pos: startPos}
	case b == '}':
		l.advance()
		return token{kind: tokRBrace, val: "}", pos: startPos}
	case b == '[':
		l.advance()
		return token{kind: tokLBracket, val: "[", pos: startPos}
	case b == ']':
		l.advance()
		return token{kind: tokRBracket, val: "]", pos: startPos}
	case b == '|':
		l.advance()
		return token{kind: tokPipe, val: "|", pos: startPos}
	case b == ',':
		l.advance()
		return token{kind: tokComma, val: ",", pos: startPos}
	case b == '!':
		if l.pos+1 < len(l.input) && l.input[l.pos+1] == '=' {
			l.pos += 2
			return token{kind: tokNotEquals, val: "!=", pos: startPos}
		}
		l.advance()
		return token{kind: tokEOF, val: "!", pos: startPos}
	case b == '=':
		l.advance()
		return token{kind: tokEquals, val: "=", pos: startPos}
	case b == '<':
		if l.pos+1 < len(l.input) && l.input[l.pos+1] == '=' {
			l.pos += 2
			return token{kind: tokLE, val: "<=", pos: startPos}
		}
		l.advance()
		return token{kind: tokLT, val: "<", pos: startPos}
	case b == '>':
		if l.pos+1 < len(l.input) && l.input[l.pos+1] == '=' {
			l.pos += 2
			return token{kind: tokGE, val: ">=", pos: startPos}
		}
		l.advance()
		return token{kind: tokGT, val: ">", pos: startPos}
	case b == '+':
		l.advance()
		return token{kind: tokPlus, val: "+", pos: startPos}
	case b == '*':
		l.advance()
		return token{kind: tokStar, val: "*", pos: startPos}
	case b == '/':
		l.advance()
		return token{kind: tokSlash, val: "/", pos: startPos}
	case b == ':':
		if l.pos+1 < len(l.input) && l.input[l.pos+1] == '-' {
			l.pos += 2
			return token{kind: tokImplies, val: ":-", pos: startPos}
		}
		l.advance()
		return token{kind: tokColon, val: ":", pos: startPos}
	case b == '?':
		l.advance()
		// An explicit ?N names a parser-generated variable (as printed by
		// Rule.String for desugared patterns), letting rules round-trip.
		if l.pos < len(l.input) && l.input[l.pos] >= '0' && l.input[l.pos] <= '9' {
			start := l.pos
			for l.pos < len(l.input) && l.input[l.pos] >= '0' && l.input[l.pos] <= '9' {
				l.pos++
			}
			if n, err := strconv.Atoi(l.input[start:l.pos]); err == nil && n >= l.anonID {
				l.anonID = n + 1 // fresh vars must not collide with explicit ?N
			}
			return token{kind: tokIdent, val: "?" + l.input[start:l.pos], pos: startPos}
		}
		return token{kind: tokAnon, val: "?", pos: startPos}
	case b == '.':
		// check if it's a float like .5
		if l.pos+1 < len(l.input) && l.input[l.pos+1] >= '0' && l.input[l.pos+1] <= '9' {
			return l.readNumber()
		}
		l.advance()
		return token{kind: tokDot, val: ".", pos: startPos}
	case b == '"':
		return l.readString()
	case b == '-':
		if l.pos+1 < len(l.input) && l.input[l.pos+1] >= '0' && l.input[l.pos+1] <= '9' {
			return l.readNumber()
		}
		l.advance()
		return token{kind: tokMinus, val: "-", pos: startPos}
	case b >= '0' && b <= '9':
		return l.readNumber()
	case b == '_' || unicode.IsUpper(rune(b)) || unicode.IsLower(rune(b)):
		return l.readIdent()
	case b == '@':
		// Sigil for engine builtins: @contains, @starts_with, etc.
		l.advance()
		if l.pos < len(l.input) && isIdentChar(l.input[l.pos]) {
			tok := l.readIdent()
			tok.val = "@" + tok.val
			tok.pos = startPos
			tok.kind = tokIdent
			return tok
		}
		return token{kind: tokEOF, val: "@", pos: startPos}
	default:
		l.advance()
		return token{kind: tokEOF, val: string(b), pos: startPos}
	}
}

func isIdentChar(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || b == '_'
}

func (l *lexer) readIdent() token {
	start := l.pos
	for l.pos < len(l.input) && isIdentChar(l.input[l.pos]) {
		l.pos++
	}
	val := l.input[start:l.pos]
	if val == "not" {
		return token{kind: tokNot, val: val, pos: start}
	}
	if val == "is" {
		return token{kind: tokIs, val: val, pos: start}
	}
	return token{kind: tokIdent, val: val, pos: start}
}

func (l *lexer) readString() token {
	start := l.pos
	l.advance() // skip opening "
	var buf strings.Builder
	for l.pos < len(l.input) {
		b := l.advance()
		if b == '"' {
			return token{kind: tokString, val: buf.String(), pos: start}
		}
		if b == '\\' && l.pos < len(l.input) {
			next := l.advance()
			switch next {
			case 'n':
				buf.WriteByte('\n')
			case 't':
				buf.WriteByte('\t')
			case '\\':
				buf.WriteByte('\\')
			case '"':
				buf.WriteByte('"')
			default:
				buf.WriteByte('\\')
				buf.WriteByte(next)
			}
			continue
		}
		buf.WriteByte(b)
	}
	return token{kind: tokString, val: buf.String(), pos: start}
}

func (l *lexer) readNumber() token {
	start := l.pos
	if l.peek() == '-' {
		l.advance()
	}
	for l.pos < len(l.input) && l.input[l.pos] >= '0' && l.input[l.pos] <= '9' {
		l.advance()
	}
	isFloat := false
	if l.pos < len(l.input) && l.input[l.pos] == '.' {
		// check next char is a digit (not end of statement)
		if l.pos+1 < len(l.input) && l.input[l.pos+1] >= '0' && l.input[l.pos+1] <= '9' {
			isFloat = true
			l.advance() // skip .
			for l.pos < len(l.input) && l.input[l.pos] >= '0' && l.input[l.pos] <= '9' {
				l.advance()
			}
		}
	}
	if l.pos < len(l.input) && (l.input[l.pos] == 'e' || l.input[l.pos] == 'E') {
		isFloat = true
		l.advance()
		if l.pos < len(l.input) && (l.input[l.pos] == '+' || l.input[l.pos] == '-') {
			l.advance()
		}
		for l.pos < len(l.input) && l.input[l.pos] >= '0' && l.input[l.pos] <= '9' {
			l.advance()
		}
	}
	val := l.input[start:l.pos]
	if isFloat {
		return token{kind: tokFloat, val: val, pos: start}
	}
	return token{kind: tokInt, val: val, pos: start}
}

// parser is a recursive descent parser for datalog statements.
type parser struct {
	lex     *lexer
	current token
	prev    token

	// getters accumulates atoms desugared from destructuring patterns in
	// the most recently parsed atom; patternPos records where the first
	// pattern began, for error reporting when patterns are not allowed.
	getters    []Atom
	patternPos int
}

// takeGetters returns and clears the getter atoms desugared from patterns
// in the last-parsed atom.
func (p *parser) takeGetters() []Atom {
	g := p.getters
	p.getters = nil
	return g
}

func newParser(input string) *parser {
	p := &parser{lex: newLexer(input)}
	p.advance()
	return p
}

func (p *parser) advance() token {
	p.prev = p.current
	p.current = p.lex.next()
	return p.prev
}

func (p *parser) expect(kind tokenKind) (token, error) {
	if p.current.kind != kind {
		return token{}, p.errorf(p.current.pos, "expected %v, got %q", kindName(kind), p.current.val)
	}
	return p.advance(), nil
}

// errorf formats a parse error with the 1-based line and column of pos,
// followed by the offending source line and a caret marking the column.
func (p *parser) errorf(pos int, format string, args ...any) error {
	input := p.lex.input
	if pos > len(input) {
		pos = len(input)
	}
	line := 1 + strings.Count(input[:pos], "\n")
	start := strings.LastIndexByte(input[:pos], '\n') + 1
	end := strings.IndexByte(input[pos:], '\n')
	if end < 0 {
		end = len(input)
	} else {
		end += pos
	}
	src := input[start:end]
	col := pos - start

	// Mirror tabs in the caret line so it aligns under the source line.
	caret := make([]byte, 0, col+1)
	for _, b := range []byte(src[:col]) {
		if b == '\t' {
			caret = append(caret, '\t')
		} else {
			caret = append(caret, ' ')
		}
	}
	caret = append(caret, '^')

	return fmt.Errorf("line %d, column %d: %s\n\t%s\n\t%s",
		line, col+1, fmt.Sprintf(format, args...), src, caret)
}

func kindName(k tokenKind) string {
	switch k {
	case tokEOF:
		return "end of input"
	case tokIdent:
		return "identifier"
	case tokAnon:
		return "'?'"
	case tokString:
		return "string"
	case tokInt:
		return "integer"
	case tokFloat:
		return "float"
	case tokLParen:
		return "'('"
	case tokRParen:
		return "')'"
	case tokComma:
		return "','"
	case tokDot:
		return "'.'"
	case tokImplies:
		return "':-'"
	case tokNot:
		return "'not'"
	case tokIs:
		return "'is'"
	case tokPlus:
		return "'+'"
	case tokMinus:
		return "'-'"
	case tokStar:
		return "'*'"
	case tokSlash:
		return "'/'"
	case tokEquals:
		return "'='"
	case tokNotEquals:
		return "'!='"
	case tokLT:
		return "'<'"
	case tokGT:
		return "'>'"
	case tokLE:
		return "'<='"
	case tokGE:
		return "'>='"
	case tokColon:
		return "':'"
	case tokLBrace:
		return "'{'"
	case tokRBrace:
		return "'}'"
	case tokLBracket:
		return "'['"
	case tokRBracket:
		return "']'"
	case tokPipe:
		return "'|'"
	default:
		return "unknown"
	}
}

// ParseStatement parses a single datalog statement.
// Returns one of: *Rule (fact or rule), *Query, *AggregateRule.
func ParseStatement(input string) (any, error) {
	p := newParser(input)
	return p.parseStatement()
}

// ParseAll parses a Datalog program consisting of multiple statements and returns a Ruleset.
func ParseAll(input string) (Ruleset, error) {
	p := newParser(input)
	var rs Ruleset
	for p.current.kind != tokEOF {
		stmt, err := p.parseStatement()
		if err != nil {
			return rs, err
		}
		switch v := stmt.(type) {
		case *Rule:
			rs.Rules = append(rs.Rules, *v)
		case *AggregateRule:
			rs.AggRules = append(rs.AggRules, *v)
		case *Query:
			rs.Queries = append(rs.Queries, *v)
		}
	}
	return rs, nil
}

func (p *parser) parseStatement() (any, error) {
	// Could be a rule, fact, aggregate rule, or query. Patterns are parsed
	// permissively here because the first atom may turn out to be a query
	// body atom; they are rejected below once it proves to be a head.
	head, err := p.parseAtom()
	if err != nil {
		return nil, err
	}
	headGetters := p.takeGetters()
	headPatternPos := p.patternPos

	switch p.current.kind {
	case tokDot:
		// fact: atom.
		if len(headGetters) > 0 {
			return nil, p.errorf(headPatternPos, "patterns are not allowed in rule heads")
		}
		p.advance()
		return &Rule{Head: head}, nil
	case tokAnon:
		// single-atom query: atom?
		p.advance()
		return &Query{Body: append([]Atom{head}, headGetters...)}, nil
	case tokComma:
		// multi-atom query: atom, atom, ...?
		atoms := append([]Atom{head}, headGetters...)
		for p.current.kind == tokComma {
			p.advance()
			a, err := p.parseAtom()
			if err != nil {
				return nil, err
			}
			atoms = append(atoms, a)
			atoms = append(atoms, p.takeGetters()...)
		}
		if _, err := p.expect(tokAnon); err != nil {
			return nil, err
		}
		return &Query{Body: atoms}, nil
	}

	// rule or aggregate: atom :- ...
	if len(headGetters) > 0 {
		return nil, p.errorf(headPatternPos, "patterns are not allowed in rule heads")
	}
	if _, err := p.expect(tokImplies); err != nil {
		return nil, err
	}

	// Check for aggregate pattern: Var = aggKind(...) : body
	if p.current.kind == tokIdent {
		savedPos := p.lex.pos
		savedCurrent := p.current
		savedPrev := p.prev

		varTok := p.advance()
		if p.current.kind == tokEquals {
			p.advance()
			if p.current.kind == tokIdent {
				aggName := p.current.val
				kind, ok := parseAggKind(aggName)
				if ok {
					p.advance()
					aggRule, err := p.parseAggregateBody(head, varTok.val, kind)
					if err == nil {
						return aggRule, nil
					}
				}
			}
		}

		// Not an aggregate, restore state and parse as normal rule body.
		p.lex.pos = savedPos
		p.current = savedCurrent
		p.prev = savedPrev
		p.getters = nil // drop getters from a partially parsed aggregate body
	}

	body, err := p.parseAtomList()
	if err != nil {
		return nil, err
	}
	if _, err := p.expect(tokDot); err != nil {
		return nil, err
	}
	return &Rule{Head: head, Body: body}, nil
}

func (p *parser) parseAggregateBody(head Atom, resultVar string, kind AggregateKind) (*AggregateRule, error) {
	var aggTerm datalog.Term
	if kind != AggCount {
		if _, err := p.expect(tokLParen); err != nil {
			return nil, err
		}
		t, err := p.parseTerm()
		if err != nil {
			return nil, err
		}
		aggTerm = t
		if _, err := p.expect(tokRParen); err != nil {
			return nil, err
		}
	}
	if _, err := p.expect(tokColon); err != nil {
		return nil, err
	}
	body, err := p.parseAtomList()
	if err != nil {
		return nil, err
	}
	if _, err := p.expect(tokDot); err != nil {
		return nil, err
	}
	return &AggregateRule{
		Head:      head,
		ResultVar: resultVar,
		Kind:      kind,
		AggTerm:   aggTerm,
		Body:      body,
	}, nil
}

func parseAggKind(name string) (AggregateKind, bool) {
	switch name {
	case "count":
		return AggCount, true
	case "sum":
		return AggSum, true
	case "min":
		return AggMin, true
	case "max":
		return AggMax, true
	default:
		return 0, false
	}
}

func (p *parser) parseAtomList() ([]Atom, error) {
	var atoms []Atom
	a, err := p.parseAtom()
	if err != nil {
		return nil, err
	}
	atoms = append(atoms, a)
	atoms = append(atoms, p.takeGetters()...)
	for p.current.kind == tokComma {
		p.advance()
		a, err := p.parseAtom()
		if err != nil {
			return nil, err
		}
		atoms = append(atoms, a)
		atoms = append(atoms, p.takeGetters()...)
	}
	return atoms, nil
}

func isComparisonOp(k tokenKind) bool {
	switch k {
	case tokEquals, tokNotEquals, tokLT, tokGT, tokLE, tokGE:
		return true
	}
	return false
}

func (p *parser) parseComparisonAtom() (Atom, error) {
	lhs, err := p.parseTerm()
	if err != nil {
		return Atom{}, err
	}
	if !isComparisonOp(p.current.kind) {
		return Atom{}, p.errorf(p.current.pos, "expected comparison operator, got %q", p.current.val)
	}
	op := p.advance().val
	rhs, err := p.parseTerm()
	if err != nil {
		return Atom{}, err
	}
	return Atom{Pred: op, Terms: []datalog.Term{lhs, rhs}}, nil
}

func (p *parser) parseAtom() (Atom, error) {
	negated := false
	if p.current.kind == tokNot {
		negated = true
		p.advance()
	}

	// Detect inline constraint or is-expression: T1 op T2, or Var is Expr
	if !negated {
		switch p.current.kind {
		case tokString, tokInt, tokFloat:
			return p.parseComparisonAtom()
		case tokIdent:
			// Peek ahead to distinguish predicate(args) from variable comparisons/is-expressions.
			savedPos := p.lex.pos
			savedCurrent := p.current
			savedPrev := p.prev
			p.advance() // consume ident
			nextKind := p.current.kind
			if nextKind == tokIs {
				p.lex.pos = savedPos
				p.current = savedCurrent
				p.prev = savedPrev
				return p.parseIsAtom()
			}
			if isComparisonOp(nextKind) {
				p.lex.pos = savedPos
				p.current = savedCurrent
				p.prev = savedPrev
				return p.parseComparisonAtom()
			}
			// Not is/comparison -- continue with the ident as the predicate name.
			pred := p.prev.val
			return p.parseAtomTerms(pred, negated)
		}
	}

	if p.current.kind != tokIdent {
		return Atom{}, p.errorf(p.current.pos, "expected predicate name, got %q", p.current.val)
	}
	pred := p.advance().val
	return p.parseAtomTerms(pred, negated)
}

func (p *parser) parseAtomTerms(pred string, negated bool) (Atom, error) {
	if _, err := p.expect(tokLParen); err != nil {
		return Atom{}, err
	}
	var terms []datalog.Term
	if p.current.kind != tokRParen {
		t, err := p.parseArgTerm(negated)
		if err != nil {
			return Atom{}, err
		}
		terms = append(terms, t)
		for p.current.kind == tokComma {
			p.advance()
			t, err := p.parseArgTerm(negated)
			if err != nil {
				return Atom{}, err
			}
			terms = append(terms, t)
		}
	}
	if _, err := p.expect(tokRParen); err != nil {
		return Atom{}, err
	}
	return Atom{Pred: pred, Terms: terms, Negated: negated}, nil
}

// parseArgTerm parses one atom argument: a plain term, or a destructuring
// pattern that desugars into a fresh variable plus getter atoms.
func (p *parser) parseArgTerm(negated bool) (datalog.Term, error) {
	if p.current.kind != tokLBrace && p.current.kind != tokLBracket {
		return p.parseTerm()
	}
	if negated {
		return nil, p.errorf(p.current.pos, "patterns are not allowed under negation (yet)")
	}
	if len(p.getters) == 0 {
		p.patternPos = p.current.pos
	}
	fresh := p.freshVar()
	if err := p.parsePatternInto(fresh); err != nil {
		return nil, err
	}
	return fresh, nil
}

// freshVar allocates a parser-generated anonymous variable (?0, ?1, ...).
func (p *parser) freshVar() datalog.Variable {
	name := fmt.Sprintf("?%d", p.lex.anonID)
	p.lex.anonID++
	return datalog.Variable(name)
}

// parsePatternInto parses an object or array pattern and appends getter
// atoms binding the pattern's parts of obj to p.getters.
func (p *parser) parsePatternInto(obj datalog.Term) error {
	switch p.current.kind {
	case tokLBrace:
		return p.parseObjectPattern(obj)
	case tokLBracket:
		return p.parseArrayPattern(obj)
	}
	return p.errorf(p.current.pos, "expected pattern, got %q", p.current.val)
}

// parseObjectPattern parses { field: value, ... } — open matching: the
// object may have keys beyond those named. Each field desugars to
// @json_get(obj, "key", value).
func (p *parser) parseObjectPattern(obj datalog.Term) error {
	if _, err := p.expect(tokLBrace); err != nil {
		return err
	}
	for p.current.kind != tokRBrace {
		var key string
		switch p.current.kind {
		case tokIdent:
			if c := p.current.val[0]; c >= 'A' && c <= 'Z' {
				return p.errorf(p.current.pos, "variable keys are not allowed in patterns; quote the key or enumerate with @json_items")
			}
			key = p.advance().val
		case tokString:
			key = p.advance().val
		default:
			return p.errorf(p.current.pos, "expected field key, got %q", p.current.val)
		}
		if _, err := p.expect(tokColon); err != nil {
			return err
		}
		err := p.parsePatternValue(func(val datalog.Term) {
			p.getters = append(p.getters, Atom{
				Pred:  "@json_get",
				Terms: []datalog.Term{obj, datalog.String(key), val},
			})
		})
		if err != nil {
			return err
		}
		if p.current.kind != tokComma {
			break
		}
		p.advance()
	}
	_, err := p.expect(tokRBrace)
	return err
}

// parseArrayPattern parses [ e0, e1, ... ] or [ e0, ... | Rest ]. Fixed
// arrays desugar to @json_len plus indexed @json_get atoms; a | tail
// desugars to @json_slice, which strictly shrinks and so terminates
// under recursion.
func (p *parser) parseArrayPattern(obj datalog.Term) error {
	if _, err := p.expect(tokLBracket); err != nil {
		return err
	}
	idx := int64(0)
	var rest datalog.Term
	for p.current.kind != tokRBracket && rest == nil {
		err := p.parsePatternValue(func(val datalog.Term) {
			p.getters = append(p.getters, Atom{
				Pred:  "@json_get",
				Terms: []datalog.Term{obj, datalog.Integer(idx), val},
			})
		})
		if err != nil {
			return err
		}
		idx++
		switch p.current.kind {
		case tokComma:
			p.advance()
		case tokPipe:
			p.advance()
			t, err := p.parseTerm()
			if err != nil {
				return err
			}
			rest = t
		}
	}
	if _, err := p.expect(tokRBracket); err != nil {
		return err
	}
	if rest != nil {
		p.getters = append(p.getters, Atom{
			Pred:  "@json_slice",
			Terms: []datalog.Term{obj, datalog.Integer(idx), rest},
		})
	} else {
		p.getters = append(p.getters, Atom{
			Pred:  "@json_len",
			Terms: []datalog.Term{obj, datalog.Integer(idx)},
		})
	}
	return nil
}

// parsePatternValue parses a field or element position inside a pattern:
// either a plain term or a nested pattern via a fresh intermediate variable.
// emit receives the value term before any nested pattern's getters are
// appended, so getters bind each intermediate before it is used.
func (p *parser) parsePatternValue(emit func(val datalog.Term)) error {
	if p.current.kind == tokLBrace || p.current.kind == tokLBracket {
		fresh := p.freshVar()
		emit(fresh)
		return p.parsePatternInto(fresh)
	}
	t, err := p.parseTerm()
	if err != nil {
		return err
	}
	emit(t)
	return nil
}

func (p *parser) parseIsAtom() (Atom, error) {
	if p.current.kind != tokIdent {
		return Atom{}, p.errorf(p.current.pos, "expected variable on left of 'is'")
	}
	varTok := p.advance()
	if _, err := p.expect(tokIs); err != nil {
		return Atom{}, err
	}
	expr, err := p.parseExpr()
	if err != nil {
		return Atom{}, err
	}
	return Atom{
		Pred:  "is",
		Terms: []datalog.Term{datalog.Variable(varTok.val)},
		Expr:  expr,
	}, nil
}

// parseExpr parses an arithmetic expression: additive precedence level.
func (p *parser) parseExpr() (Expr, error) {
	left, err := p.parseMulExpr()
	if err != nil {
		return nil, err
	}
	for p.current.kind == tokPlus || p.current.kind == tokMinus {
		op := p.advance().val
		right, err := p.parseMulExpr()
		if err != nil {
			return nil, err
		}
		left = BinExpr{Op: op, Left: left, Right: right}
	}
	return left, nil
}

// parseMulExpr parses a multiplicative-precedence expression.
func (p *parser) parseMulExpr() (Expr, error) {
	left, err := p.parsePrimaryExpr()
	if err != nil {
		return nil, err
	}
	for p.current.kind == tokStar || p.current.kind == tokSlash ||
		(p.current.kind == tokIdent && p.current.val == "mod") {
		op := p.advance().val
		right, err := p.parsePrimaryExpr()
		if err != nil {
			return nil, err
		}
		left = BinExpr{Op: op, Left: left, Right: right}
	}
	return left, nil
}

// parsePrimaryExpr parses a parenthesized expression, unary minus, or term leaf.
func (p *parser) parsePrimaryExpr() (Expr, error) {
	if p.current.kind == tokLParen {
		p.advance()
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(tokRParen); err != nil {
			return nil, err
		}
		return expr, nil
	}
	if p.current.kind == tokMinus {
		p.advance()
		inner, err := p.parsePrimaryExpr()
		if err != nil {
			return nil, err
		}
		return BinExpr{
			Op:    "-",
			Left:  TermExpr{Term: datalog.Integer(0)},
			Right: inner,
		}, nil
	}
	t, err := p.parseTerm()
	if err != nil {
		return nil, err
	}
	return TermExpr{Term: t}, nil
}

func (p *parser) parseTerm() (datalog.Term, error) {
	switch p.current.kind {
	case tokIdent:
		name := p.advance().val
		return datalog.Variable(name), nil
	case tokAnon:
		p.advance()
		name := fmt.Sprintf("?%d", p.lex.anonID)
		p.lex.anonID++
		return datalog.Variable(name), nil
	case tokString:
		val := p.advance().val
		return datalog.String(val), nil
	case tokInt:
		val := p.advance().val
		n, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return nil, p.errorf(p.prev.pos, "invalid integer %q: %v", val, err)
		}
		return datalog.Integer(n), nil
	case tokFloat:
		val := p.advance().val
		f, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return nil, p.errorf(p.prev.pos, "invalid float %q: %v", val, err)
		}
		return datalog.Float(f), nil
	case tokMinus:
		// Unary minus for negative literal constants.
		p.advance()
		switch p.current.kind {
		case tokInt:
			val := p.advance().val
			n, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				return nil, p.errorf(p.prev.pos, "invalid integer %q: %v", val, err)
			}
			return datalog.Integer(-n), nil
		case tokFloat:
			val := p.advance().val
			f, err := strconv.ParseFloat(val, 64)
			if err != nil {
				return nil, p.errorf(p.prev.pos, "invalid float %q: %v", val, err)
			}
			return datalog.Float(-f), nil
		default:
			return nil, p.errorf(p.current.pos, "expected number after '-'")
		}
	default:
		return nil, p.errorf(p.current.pos, "expected term, got %q", p.current.val)
	}
}
