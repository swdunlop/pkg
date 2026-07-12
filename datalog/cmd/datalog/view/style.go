package view

import _ "embed"

// OatCSS is oat@0.6.1, vendored verbatim from ~/src/medea/www/oat.css (itself
// a byte-identical re-vendor from unpkg). Self-hosting avoids a render-
// blocking third-party <link> stalling first paint if the CDN is slow or
// unreachable, matching medea's rationale for self-serving the same file.
//
//go:embed oat.css
var OatCSS []byte
