package view

import (
	"net/url"
	"strings"
	"testing"

	html "github.com/swdunlop/html-go"
)

// TestWhyButtonLiteralIsInert pins the invariant WhyButton's doc comment
// documents: the fact literal is spliced into a single-quoted JS string
// inside data-on:click, and the only thing keeping a quote- or
// backslash-bearing string term (datalog.String renders %q) from
// terminating that JS string early is url.QueryEscape's output alphabet.
// html-go's attribute escaping is NOT sufficient on its own -- &apos;
// decodes back to a raw quote in the DOM before Datastar evaluates the
// expression -- so if a refactor ever swaps QueryEscape for PathEscape or
// raw interpolation, this must fail.
func TestWhyButtonLiteralIsInert(t *testing.T) {
	lit := `flagged("it's a \"test\"", "x")`

	esc := url.QueryEscape(lit)
	if strings.ContainsAny(esc, "'\"\\<>&") {
		t.Fatalf("QueryEscape output alphabet leaked a JS/HTML-special byte: %q", esc)
	}

	out := string(html.Append(nil, WhyButton("flagged", 2, lit)))
	if !strings.Contains(out, "fact="+esc) {
		t.Fatalf("rendered button does not carry the query-escaped literal %q:\n%s", esc, out)
	}
	if strings.Contains(out, `\`) {
		t.Fatalf("raw backslash from the literal survived into the rendered button:\n%s", out)
	}
}

// TestFactsTable_NamedHeaderAndUse pins doc/features/predicate-docs.md work
// item 4's Fact Browser surface: predicate headers must render a
// declaration's named term columns (instead of positional col0/col1/...)
// and its Use text, when known. FactsTable only renders whatever header
// strings and use text it is given (package main's fact_browser.go decides
// what those are from the declaration) -- this test pins that FactsTable
// itself actually emits them, and that an empty use renders no caption at
// all (no invented chrome for an undocumented predicate).
func TestFactsTable_NamedHeaderAndUse(t *testing.T) {
	header := []string{"host", "kind"}
	out := string(html.Append(nil, FactsTable("event", 2, header, nil, 0, 0, false, "A host observed doing something.")))
	if !strings.Contains(out, "host") || !strings.Contains(out, "kind") {
		t.Fatalf("rendered table missing named header columns:\n%s", out)
	}
	if strings.Contains(out, "col0") || strings.Contains(out, "col1") {
		t.Fatalf("rendered table fell back to positional header names despite a named header being given:\n%s", out)
	}
	if !strings.Contains(out, "A host observed doing something.") {
		t.Fatalf("rendered table missing the predicate's use text:\n%s", out)
	}
}

// TestFactsTable_EmptyUseRendersNoCaption asserts that an empty use string
// (a predicate with no declaration at all) renders no caption element --
// "do not invent new chrome" for a predicate with nothing to say.
func TestFactsTable_EmptyUseRendersNoCaption(t *testing.T) {
	header := []string{"col0", "col1"}
	out := string(html.Append(nil, FactsTable("raw", 2, header, nil, 0, 0, false, "")))
	if strings.Contains(out, "predicate-use") {
		t.Fatalf("rendered table has a predicate-use caption despite an empty use string:\n%s", out)
	}
}
