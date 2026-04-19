package syntax

import (
	"fmt"
	"strings"

	"swdunlop.dev/pkg/datalog"
)

// Atom is a predicate applied to a list of terms, possibly negated.
// When Pred is "is", Terms holds the single LHS variable and Expr holds the RHS arithmetic expression.
type Atom struct {
	Pred    string
	Terms   []datalog.Term
	Negated bool
	Expr    Expr // non-nil only when Pred == "is"
}

// Arity returns the number of terms in the atom.
func (a Atom) Arity() int { return len(a.Terms) }

func (a Atom) String() string {
	if a.Pred == "is" && len(a.Terms) == 1 && a.Expr != nil {
		return a.Terms[0].String() + " is " + a.Expr.String()
	}
	if isComparisonPred(a.Pred) && len(a.Terms) == 2 {
		return a.Terms[0].String() + " " + a.Pred + " " + a.Terms[1].String()
	}
	var buf strings.Builder
	if a.Negated {
		buf.WriteString("not ")
	}
	buf.WriteString(a.Pred)
	buf.WriteByte('(')
	for i, t := range a.Terms {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(t.String())
	}
	buf.WriteByte(')')
	return buf.String()
}

func isComparisonPred(pred string) bool {
	switch pred {
	case "=", "!=", "<", ">", "<=", ">=":
		return true
	}
	return false
}

// Rule is a datalog rule: Head :- Body.
// A fact is a rule with an empty body whose head is fully ground.
type Rule struct {
	Head Atom
	Body []Atom
}

func (r Rule) String() string {
	var buf strings.Builder
	buf.WriteString(r.Head.String())
	if len(r.Body) > 0 {
		buf.WriteString(" :- ")
		for i, a := range r.Body {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(a.String())
		}
	}
	buf.WriteByte('.')
	return buf.String()
}

// IsFact returns true if this rule has no body and all head terms are ground.
func (r Rule) IsFact() bool {
	return len(r.Body) == 0 && IsGround(r.Head.Terms)
}

// ToFact converts a fact rule into a datalog.Fact. Panics if not a fact.
func (r Rule) ToFact() datalog.Fact {
	if !r.IsFact() {
		panic("ToFact called on non-fact rule")
	}
	terms := make([]datalog.Constant, len(r.Head.Terms))
	for i, t := range r.Head.Terms {
		terms[i] = t.(datalog.Constant)
	}
	return datalog.Fact{Name: r.Head.Pred, Terms: terms}
}

// Query is a datalog query: body?
type Query struct {
	Body []Atom
}

func (q Query) String() string {
	var buf strings.Builder
	for i, a := range q.Body {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(a.String())
	}
	buf.WriteByte('?')
	return buf.String()
}

// AggregateKind enumerates supported aggregates.
type AggregateKind int

const (
	AggCount AggregateKind = iota
	AggSum
	AggMin
	AggMax
)

func (k AggregateKind) String() string {
	switch k {
	case AggCount:
		return "count"
	case AggSum:
		return "sum"
	case AggMin:
		return "min"
	case AggMax:
		return "max"
	default:
		return fmt.Sprintf("agg(%d)", int(k))
	}
}

// AggregateRule represents: Head :- ResultVar = kind(AggTerm) : Body.
type AggregateRule struct {
	Head      Atom
	ResultVar string        // variable in Head that receives the aggregate
	Kind      AggregateKind // count, sum, min, max
	AggTerm   datalog.Term  // term to aggregate over (ignored for count)
	Body      []Atom        // body to evaluate
}

func (ar AggregateRule) String() string {
	var buf strings.Builder
	buf.WriteString(ar.Head.String())
	buf.WriteString(" :- ")
	buf.WriteString(ar.ResultVar)
	buf.WriteString(" = ")
	buf.WriteString(ar.Kind.String())
	if ar.Kind != AggCount {
		fmt.Fprintf(&buf, "(%s)", ar.AggTerm.String())
	}
	buf.WriteString(" : ")
	for i, a := range ar.Body {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(a.String())
	}
	buf.WriteByte('.')
	return buf.String()
}

// Expr is an arithmetic expression used as the right-hand side of an "is" atom.
type Expr interface {
	isExpr()
	String() string
}

// BinExpr is a binary arithmetic operation.
type BinExpr struct {
	Op    string // "+", "-", "*", "/", "mod"
	Left  Expr
	Right Expr
}

func (b BinExpr) isExpr() {}
func (b BinExpr) String() string {
	return "(" + b.Left.String() + " " + b.Op + " " + b.Right.String() + ")"
}

// TermExpr wraps a Term (variable or constant) as an Expr leaf.
type TermExpr struct {
	Term datalog.Term
}

func (t TermExpr) isExpr() {}
func (t TermExpr) String() string { return t.Term.String() }

// Ruleset collects the output of parsing a Datalog program.
type Ruleset struct {
	Rules    []Rule
	AggRules []AggregateRule
	Queries  []Query
}

// IsGround returns true if all terms are constants.
func IsGround(terms []datalog.Term) bool {
	for _, t := range terms {
		if _, ok := t.(datalog.Variable); ok {
			return false
		}
	}
	return true
}

// An Engine produces a transformer from a set of rules, this is typically semi-naive.
type Engine interface {
	Compile(ruleset Ruleset) (datalog.Transformer, error)
}

// Parse consumes a datalog string and yields either a transform that applies the supplied clauses to extend a
// database with new facts and declarations, or an error indicating some defect.
func Parse(engine Engine, str string) (datalog.Transformer, error) {
	ruleset, err := ParseAll(str)
	if err != nil {
		return nil, err
	}
	return engine.Compile(ruleset)
}
