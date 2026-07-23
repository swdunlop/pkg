package chat

// Agent messages are markdown; this file renders them to HTML for the
// transcript.  Safety comes from goldmark's default renderer: raw HTML in the
// source — inline or block — is never emitted (goldmark drops it with an
// "<!-- raw HTML omitted -->" comment), so everything reaching the page is
// goldmark-generated, properly-escaped markup.  Do NOT add ghtml.WithUnsafe();
// that single option is the whole sandbox.

import (
	"bytes"

	"github.com/swdunlop/html-go"
	"github.com/swdunlop/html-go/tag"
	"github.com/yuin/goldmark"
	"github.com/yuin/goldmark/extension"
	ghtml "github.com/yuin/goldmark/renderer/html"
)

// markdown is the shared converter: GFM for the tables/strikethrough/
// autolinks agents commonly emit, hard wraps because a chat message's single
// newline is a visible break (the chat idiom, and what the plain-text
// renderer this replaces did with pre-wrap).
var markdown = goldmark.New(
	goldmark.WithExtensions(extension.GFM),
	goldmark.WithRendererOptions(ghtml.WithHardWraps()),
)

// markdownBody renders one agent message.  On a conversion error (rare — the
// converter is total for any byte string) it degrades to escaped plain text
// rather than dropping the message.
func markdownBody(text string) html.Content {
	var buf bytes.Buffer
	if err := markdown.Convert([]byte(text), &buf); err != nil {
		return tag.New("div.chat-agent", html.Text(text))
	}
	return tag.New("div.chat-agent", html.HTML(buf.String()))
}
