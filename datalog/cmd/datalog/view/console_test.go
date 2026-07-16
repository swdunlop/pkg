package view

import (
	"strings"
	"testing"

	html "github.com/swdunlop/html-go"
)

// TestConsoleSendButtonGatesOnQueryBusy is the regression for the console
// send button (#console-send) not being gated on $busy === 'query': before
// this fix, its data-attr:disabled expression only checked 'run'/'apply',
// so starting an agent turn while a query was in flight clobbered the
// query's Stop affordance instead of being blocked, and the query's own
// $busy '' cleanup on completion (publishBusy is last-writer-wins across
// distinct job keys) could then stomp the agent turn's busy state mid-turn.
// This asserts the button's disabled expression mirrors
// BusyActionButton's own "$busy && $busy !== <own key>" gate (page.go) —
// disabled whenever $busy holds any key other than the button's own
// 'agent' — rather than a hardcoded list of the other keys that has to be
// kept in sync by hand.
func TestConsoleSendButtonGatesOnQueryBusy(t *testing.T) {
	out := string(html.Append(nil, Console(nil, nil)))

	i := strings.Index(out, `id='console-send'`)
	if i < 0 {
		t.Fatalf("console-send button not found in rendered console:\n%s", out)
	}
	// The disabled expression sits on the same element; scanning a small
	// window after the id avoids depending on html-go's attribute order.
	window := out[i:min(i+400, len(out))]

	if !strings.Contains(window, `data-attr:disabled='$busy &amp;&amp; $busy !== &apos;agent&apos;'`) {
		t.Fatalf("console-send's disabled expression must gate on any busy key other than its own ('agent'), including 'query'; got window:\n%s", window)
	}
}
