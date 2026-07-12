package main

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// dataRoot confines file access to a single operator-chosen data directory.
// It wraps an os.Root, which enforces containment at the kernel/syscall
// level: absolute paths, ".." escapes, and symlinks that resolve outside the
// root are all rejected by the underlying open, even if the symlink is
// created after the root is opened (TOCTOU-safe on platforms os.Root
// supports it on; see os.OpenRoot docs). This is the security boundary for
// the MCP server (design constraint 4 in mcp-server.md): every file
// reference in a model-submitted jsonfacts config (sources[].file, *_from
// pattern files) and every sample_input argument must pass through it.
type dataRoot struct {
	root *os.Root
	dir  string // original directory path, for error messages
}

// openDataRoot opens dir as a confinement root. dir itself is trusted (it is
// an operator flag, not model input); everything read through the returned
// dataRoot is validated against it.
func openDataRoot(dir string) (*dataRoot, error) {
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, fmt.Errorf("opening data dir %s: %w", dir, err)
	}
	return &dataRoot{root: root, dir: dir}, nil
}

// Close releases the underlying directory handle.
func (d *dataRoot) Close() error {
	return d.root.Close()
}

// FS returns an fs.FS rooted at the data directory, suitable for
// jsonfacts.Config.LoadFS. Opens performed through it inherit the same
// containment guarantees as the rest of dataRoot.
func (d *dataRoot) FS() fs.FS {
	return d.root.FS()
}

// Confine validates that ref is a relative path that stays within the data
// directory, following symlinks, and returns it unchanged for use with FS,
// Open, or ReadFile. It rejects absolute paths and any ref that escapes the
// root via ".." or a symlink — the same check the ref would receive when
// actually opened, run eagerly so callers can report a clean error (with
// the offending ref) before touching the loader or the model's document.
//
// The check is two-layered because os.Root itself only detects an escape
// once it walks a component that exists: a lexically escaping ref whose
// first missing component doesn't exist yet (e.g. "a/../../etc/passwd" when
// "a" isn't present) surfaces from os.Root as a plain not-found, not an
// escape. So Confine first rejects anything filepath.IsLocal calls
// non-local (absolute, or containing a lexical ".." that would climb out of
// dataDir) — this alone catches every ".." and absolute-path case
// regardless of what exists on disk — and then, for refs that do exist,
// asks the root to Stat them, which follows symlinks and fails if a
// symlink component points outside the root. A ref that doesn't exist yet
// is accepted once it passes the lexical check: sample_input and schema
// loading only ever read existing files, so nonexistence simply surfaces
// later as their own "file not found" error.
//
// os.Root additionally refuses to follow a symlink whose target is an
// absolute path, even one that happens to resolve inside dataDir; only
// relative symlink targets can be confirmed safe without re-deriving the
// root's own absolute path, so Confine inherits that stricter rule rather
// than working around it.
func (d *dataRoot) Confine(ref string) (string, error) {
	if ref == "" {
		return "", fmt.Errorf("empty file reference")
	}
	if !filepath.IsLocal(ref) {
		return "", fmt.Errorf("%s: escapes data directory", ref)
	}
	if _, err := d.root.Stat(ref); err != nil && !os.IsNotExist(err) {
		return "", fmt.Errorf("%s: %w", ref, err)
	}
	return ref, nil
}

// Open opens ref for reading after confining it to the data directory.
func (d *dataRoot) Open(ref string) (*os.File, error) {
	if _, err := d.Confine(ref); err != nil {
		return nil, err
	}
	return d.root.Open(ref)
}

// ReadFile reads ref after confining it to the data directory. This is the
// call site sample_input uses to serve raw JSONL lines from the data dir.
func (d *dataRoot) ReadFile(ref string) ([]byte, error) {
	if _, err := d.Confine(ref); err != nil {
		return nil, err
	}
	return d.root.ReadFile(ref)
}

// confine validates that ref is a relative path that stays within dataDir,
// resolving symlinks, and returns the ref unchanged on success. It is a
// one-shot convenience wrapper around dataRoot for callers that only need a
// single validation (e.g. a CLI flag check) rather than a session's worth of
// file access; call sites that read many files from the same data directory
// (loading a jsonfacts config, serving sample_input) should open a dataRoot
// once instead and reuse it, since os.OpenRoot has to open the directory.
func confine(dataDir, ref string) (string, error) {
	d, err := openDataRoot(dataDir)
	if err != nil {
		return "", err
	}
	defer d.Close()
	return d.Confine(ref)
}
