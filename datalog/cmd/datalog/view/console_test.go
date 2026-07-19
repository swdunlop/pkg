package view

import (
	"strings"
	"testing"

	html "github.com/swdunlop/html-go"
)

// TestComposerSendButtonGatesOnOtherBusy is the regression (carried over
// from the v1 console drawer's send button) for the composer send button's
// disabled expression: it must disable whenever $busy holds ANY state that
// isn't this conversation's own running turn — another conversation's turn
// ($busy 'agent' but a different $busyConv) and every other busy key
// ('run', 'apply', 'query') alike — rather than a hardcoded list of other
// keys kept in sync by hand. Before the v1 fix, 'query' was missing from
// the list, so starting a turn mid-query clobbered the query's Stop
// affordance and the query's $busy ” cleanup could stomp the running
// turn's busy state (publishBusy is last-writer-wins).
func TestComposerSendButtonGatesOnOtherBusy(t *testing.T) {
	out := string(html.Append(nil, Composer("cid123")))

	i := strings.Index(out, `id='send'`)
	if i < 0 {
		t.Fatalf("send button not found in rendered composer:\n%s", out)
	}
	// The disabled expression sits on the same element; scanning a small
	// window after the id avoids depending on html-go's attribute order.
	window := out[i:min(i+500, len(out))]

	want := `data-attr:disabled='$busy &amp;&amp; !($busy === &apos;agent&apos; &amp;&amp; $busyConv === &apos;cid123&apos;)'`
	if !strings.Contains(window, want) {
		t.Fatalf("send's disabled expression must gate on any busy state other than this conversation's own turn; got window:\n%s", window)
	}
}
