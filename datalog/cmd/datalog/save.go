package main

import (
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/swdunlop/html-go/datastar"
	"swdunlop.dev/pkg/datalog/cmd/datalog/view"
)

// handleSave is the Save action (POST /save/{doc}, doc = "schema" or
// "rules"), per doc/features/web-ui.md's "Session state and persistence":
// nothing touches disk until the user clicks Save, which writes the
// session's CANONICAL document — schemaText/rulesText, whatever was last
// Applied/Run — not the editor's unsaved draft. If the user has typed but
// not clicked Apply/Run, that draft differs from what Save writes; the two
// editor buttons' title attributes call this out (view/jsonfacts_editor.go,
// view/rules_editor.go).
//
// Path-resolution policy (chosen after reading "Project directory"): Save
// writes to the operator-given startup path — the -c flag for schema, the
// first positional rules file for rules — because those paths are the only
// operator-trusted target this handler has any business writing to without
// asking. When the corresponding flag/arg was NOT given at startup, this
// handler refuses with a toast error rather than guessing a default
// (jsonfacts.yaml / rules.dl in the cwd): the "Project directory" section
// describes the project directory as an operator-chosen convention the
// operator sets up before starting the server, not something the workbench
// should silently create on its own initiative. An operator who wants Save
// to work restarts with -c and/or a rules file positional; the toast error
// says so explicitly.
//
// Writing is plain os.WriteFile at 0644 with NO confinement check:
// schemaPath and rulesPath are operator-supplied startup flags/argv, not
// model or browser input, so there is nothing to confine them against — the
// operator already trusts themselves with arbitrary file writes by
// launching the process in the first place.
func (wb *workbench) handleSave(w http.ResponseWriter, r *http.Request) {
	stream, err := datastar.RequestStream(w, r)
	if err != nil {
		return
	}

	doc := r.PathValue("doc")

	var path, text string
	switch doc {
	case "schema":
		wb.h.mu.Lock()
		text = wb.h.sess.schemaText
		wb.h.mu.Unlock()
		path = wb.schemaPath
		if path == "" {
			_ = stream.Emit(datastar.Elements(view.Toast(
				"no schema path configured; restart with -c to enable Save", true)))
			return
		}
	case "rules":
		wb.h.mu.Lock()
		text = wb.h.sess.rulesText
		wb.h.mu.Unlock()
		path = wb.rulesPath
		if path == "" {
			_ = stream.Emit(datastar.Elements(view.Toast(
				"no rules path configured; restart with a rules file argument to enable Save", true)))
			return
		}
	default:
		_ = stream.Emit(datastar.Elements(view.Toast(fmt.Sprintf("unknown save target %q", doc), true)))
		return
	}

	msg, err := saveDocument(path, text)
	if err != nil {
		_ = stream.Emit(datastar.Elements(view.Toast(err.Error(), true)))
		return
	}
	_ = stream.Emit(datastar.Elements(view.Toast(msg, false)))
}

// saveDocument writes text to path (0644, operator-trusted path, no
// confinement — see handleSave's doc comment) and, if path's directory is
// inside a git work tree, stages and commits just that file with message
// "ui: save <filename>". Outside a repo, git is skipped silently (design:
// "skip git outside a repo"). A commit failure (e.g. nothing changed) is
// reported back as part of the success message rather than as an error —
// the write itself succeeded, and "nothing to commit" is not exceptional.
func saveDocument(path, text string) (string, error) {
	if err := os.WriteFile(path, []byte(text), 0o644); err != nil {
		return "", fmt.Errorf("writing %s: %w", path, err)
	}

	name := filepath.Base(path)
	dir := filepath.Dir(path)

	if !isGitWorkTree(dir) {
		return fmt.Sprintf("saved %s", name), nil
	}

	if err := gitAddCommit(dir, name); err != nil {
		return fmt.Sprintf("saved %s (commit skipped: %v)", name, err), nil
	}
	return fmt.Sprintf("saved %s (committed)", name), nil
}

// isGitWorkTree reports whether dir is inside a git work tree, via
// `git -C <dir> rev-parse --is-inside-work-tree`. Any error (git not
// installed, not a repo, etc.) is treated as "not a repo" — Save always
// degrades to "write the file, skip git" rather than surfacing a git
// availability problem as a Save failure.
func isGitWorkTree(dir string) bool {
	cmd := exec.Command("git", "-C", dir, "rev-parse", "--is-inside-work-tree")
	out, err := cmd.Output()
	if err != nil {
		return false
	}
	return strings.TrimSpace(string(out)) == "true"
}

// gitAddCommit runs `git -C dir add <name>` then
// `git -C dir commit -m "ui: save <name>" -- <name>`, where name is the
// file's basename WITHIN dir (not the possibly-multi-segment path the
// caller was given relative to the process's cwd) — git resolves paths
// given after -C relative to that directory, so passing the original
// (cwd-relative) path here would look for a nonexistent nested copy of it
// under dir. Every argument is passed via argv (os/exec, never a shell), so
// the filename can never be interpreted as a shell command or format
// directive — SECURITY NOTE per the task: no sh -c, no
// Sprintf-into-a-command-string.
func gitAddCommit(dir, name string) error {
	add := exec.Command("git", "-C", dir, "add", name)
	if out, err := add.CombinedOutput(); err != nil {
		return fmt.Errorf("git add: %w: %s", err, strings.TrimSpace(string(out)))
	}

	commit := exec.Command("git", "-C", dir, "commit", "-m", "ui: save "+name, "--", name)
	if out, err := commit.CombinedOutput(); err != nil {
		return fmt.Errorf("git commit: %w: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}
