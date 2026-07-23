package chat

import (
	"strings"
	"testing"
)

// TestMarkdownBodySafety pins the property the whole markdown feature rests
// on: agent-authored HTML never reaches the page.  If someone adds
// ghtml.WithUnsafe() to the converter, this fails.
func TestMarkdownBodySafety(t *testing.T) {
	cases := []struct {
		name     string
		in       string
		mustHave []string
		mustNot  []string
	}{
		{
			name:     "markdown renders",
			in:       "**bold** and `code`\n\n- item",
			mustHave: []string{"<strong>bold</strong>", "<code>code</code>", "<li>item</li>"},
		},
		{
			name:    "block html dropped",
			in:      "<script>alert(1)</script>",
			mustNot: []string{"<script"},
		},
		{
			name:     "inline html dropped",
			in:       "hello <img src=x onerror=alert(1)> world",
			mustHave: []string{"hello"},
			mustNot:  []string{"<img", "onerror"},
		},
		{
			name:     "html-ish text escapes",
			in:       "compare a < b && b > c",
			mustHave: []string{"&lt;", "&amp;&amp;"},
		},
		{
			name:     "link target escapes",
			in:       `[x](javascript:alert(1))`,
			mustNot:  []string{"href=\"javascript:"},
			mustHave: []string{"<a"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := string(markdownBody(c.in).AppendHTML(nil))
			for _, want := range c.mustHave {
				if !strings.Contains(got, want) {
					t.Errorf("output missing %q:\n%s", want, got)
				}
			}
			for _, bad := range c.mustNot {
				if strings.Contains(got, bad) {
					t.Errorf("output contains %q:\n%s", bad, got)
				}
			}
		})
	}
}
