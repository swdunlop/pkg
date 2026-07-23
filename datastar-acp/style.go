package chat

import _ "embed"

// CSS is the component's optional stylesheet (design decision 12): the
// structural layout the pane needs to behave like a chat (rail beside a
// scrolling transcript, pinned composer) plus a darkberg light/dark skin.
// Every rule is scoped under the .chat root class and every color rides a
// --chat-* custom property with a light-dark() value, so hosts theme by
// overriding the variables (or setting color-scheme on an ancestor) rather
// than fighting selectors.  Hosts with their own design system simply don't
// serve it.
//
// The host decides how to deliver it — typically:
//
//	mux.HandleFunc("/chat.css", func(w http.ResponseWriter, r *http.Request) {
//		w.Header().Set("Content-Type", "text/css; charset=utf-8")
//		io.WriteString(w, chat.CSS)
//	})
//
// The .chat element sizes to its container (height: 100%), so the page must
// give it a bounded height for the transcript to scroll; see examples/demo.
//
//go:embed chat.css
var CSS string
