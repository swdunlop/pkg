package view

import _ "embed"

// OatCSS is oat@0.6.1, vendored verbatim from ~/src/medea/www/oat.css (itself
// a byte-identical re-vendor from unpkg). Self-hosting avoids a render-
// blocking third-party <link> stalling first paint if the CDN is slow or
// unreachable, matching medea's rationale for self-serving the same file.
//
//go:embed oat.css
var OatCSS []byte

// WorkbenchCSS is the workbench's own chrome layer — layout, pane cards,
// editor sizing — served at /workbench.css and linked after oat.css. It is
// the analogue of medea's www/style.css: oat is a themed base, not a layout;
// every page needs its own chrome on top of it.
//
//go:embed workbench.css
var WorkbenchCSS []byte
