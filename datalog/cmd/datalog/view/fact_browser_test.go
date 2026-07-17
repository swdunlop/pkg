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
