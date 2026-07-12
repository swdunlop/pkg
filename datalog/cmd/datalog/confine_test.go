package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestConfine_PlainRelative(t *testing.T) {
	dir := t.TempDir()
	mustWriteFile(t, filepath.Join(dir, "events.jsonl"), "{}\n")

	if _, err := confine(dir, "events.jsonl"); err != nil {
		t.Fatalf("confine: %v", err)
	}
}

func TestConfine_Nested(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "sub", "dir"), 0o755); err != nil {
		t.Fatal(err)
	}
	mustWriteFile(t, filepath.Join(dir, "sub", "dir", "events.jsonl"), "{}\n")

	if _, err := confine(dir, "sub/dir/events.jsonl"); err != nil {
		t.Fatalf("confine: %v", err)
	}
}

func TestConfine_Absolute(t *testing.T) {
	dir := t.TempDir()
	outside := filepath.Join(t.TempDir(), "secret")
	mustWriteFile(t, outside, "secret\n")

	if _, err := confine(dir, outside); err == nil {
		t.Fatalf("confine: expected error for absolute path %s, got none", outside)
	}
}

func TestConfine_DotDotEscape(t *testing.T) {
	dir := t.TempDir()

	if _, err := confine(dir, "../etc/passwd"); err == nil {
		t.Fatal("confine: expected error for .. escape, got none")
	}
}

func TestConfine_DotDotEscapeDeep(t *testing.T) {
	dir := t.TempDir()

	// Lexically this collapses to something outside dir even though it
	// starts by descending; the root must still reject it.
	if _, err := confine(dir, "a/../../etc/passwd"); err == nil {
		t.Fatal("confine: expected error for a/../../etc escape, got none")
	}
}

func TestConfine_SymlinkEscape(t *testing.T) {
	dir := t.TempDir()
	outsideDir := t.TempDir()
	mustWriteFile(t, filepath.Join(outsideDir, "secret.jsonl"), "{}\n")

	link := filepath.Join(dir, "escape.jsonl")
	if err := os.Symlink(filepath.Join(outsideDir, "secret.jsonl"), link); err != nil {
		t.Fatal(err)
	}

	if _, err := confine(dir, "escape.jsonl"); err == nil {
		t.Fatal("confine: expected error for symlink escaping data dir, got none")
	}
}

func TestConfine_SymlinkWithinDataDir(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "real"), 0o755); err != nil {
		t.Fatal(err)
	}
	mustWriteFile(t, filepath.Join(dir, "real", "events.jsonl"), "{}\n")

	// A relative symlink target, as os.Root requires: an absolute
	// symlink is rejected outright regardless of where it points (see
	// the Confine doc comment), so "within data dir" must be expressed
	// relatively to be accepted.
	link := filepath.Join(dir, "alias.jsonl")
	if err := os.Symlink(filepath.Join("real", "events.jsonl"), link); err != nil {
		t.Fatal(err)
	}

	if _, err := confine(dir, "alias.jsonl"); err != nil {
		t.Fatalf("confine: expected symlink within data dir to be accepted, got %v", err)
	}
}

// A ref naming a file that does not exist yet should still be checked for
// escapes (absolute path or ".." components), since the caller may be about
// to create it (e.g. a future sample_input offset into a file that hasn't
// been loaded, or a would-be output path). It should only be rejected for
// containment, not for nonexistence.
func TestConfine_NonexistentRefStillRejectsEscape(t *testing.T) {
	dir := t.TempDir()

	if _, err := confine(dir, "does-not-exist.jsonl"); err != nil {
		t.Fatalf("confine: expected nonexistent-but-confined ref to be accepted, got %v", err)
	}

	if _, err := confine(dir, "../does-not-exist.jsonl"); err == nil {
		t.Fatal("confine: expected nonexistent ref with .. escape to be rejected")
	}
}

func mustWriteFile(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatal(err)
	}
}
