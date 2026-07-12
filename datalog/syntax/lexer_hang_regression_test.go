package syntax_test

import (
	"testing"
	"time"

	"swdunlop.dev/pkg/datalog/syntax"
)

// TestParseAllHighByteDoesNotHang is a regression test for a lexer bug found
// by FuzzParseAll: nextRaw's ident-start case tested the raw byte as a rune
// (0xC5 -> U+00C5 'Å' passes unicode.IsUpper), routing it into readIdent,
// which consumes only ASCII ident chars -- so it returned a zero-length
// tokIdent without advancing, and reservedAnonID's pre-scan looped forever.
// The fix makes the ident-start test ASCII-only, so high bytes fall through
// to the unrecognized-character error path.
//
// ParseAll is run in a goroutine with a timeout because the failure mode is
// an infinite loop, not a panic or wrong result.
func TestParseAllHighByteDoesNotHang(t *testing.T) {
	inputs := []string{
		"\xc5",
		"an\xc5\xc5\xc5\xc5\xc5r(X, Y) :- parent(X, Z), ancestor(X, Y).",
		"p(1). \x80 q(2).",
		"\xff",
	}
	for _, input := range inputs {
		done := make(chan error, 1)
		go func() {
			_, err := syntax.ParseAll(input)
			done <- err
		}()
		select {
		case err := <-done:
			if err == nil {
				t.Errorf("ParseAll(%q) = nil error, want unrecognized-character error", input)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("ParseAll(%q) did not return within 5s: lexer hang on non-ASCII byte has regressed", input)
		}
	}
}
