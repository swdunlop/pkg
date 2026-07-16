package main

import (
	"os"
	"path/filepath"
	"slices"
	"sort"
	"testing"
)

// TestCompleteFilePath exercises completeFilePath directly against a real
// directory tree, covering every partial-path shape the REPL's ".load"
// completion can see. Before the fix, completeFilePath reconstructed the
// directory portion via filepath.Dir/Base/Join — which normalizes away a
// leading "./" — and then sliced that reconstruction by len(partial): for
// "./x" against a matching one-character file "x", the reconstructed path
// ("x", one byte) is shorter than partial ("./x", three bytes), so
// path[len(partial):] panicked with a slice-bounds-out-of-range that
// readline has no recover for, killing the whole REPL process. Longer
// matches (e.g. "./x" against "xyz.txt") didn't panic but produced
// corrupted suggestions, since the slice offset was computed against a
// string missing the "./" the offset was supposed to skip.
func TestCompleteFilePath(t *testing.T) {
	dir := t.TempDir()
	mustWriteFile(t, filepath.Join(dir, "x"), "")            // one-byte-name match: the panic case
	mustWriteFile(t, filepath.Join(dir, "xyz.txt"), "")       // longer match: the corruption case
	mustWriteFile(t, filepath.Join(dir, "other.txt"), "")     // non-matching sibling
	mustWriteFile(t, filepath.Join(dir, ".hidden"), "")       // hidden, excluded unless base starts with "."
	if err := os.Mkdir(filepath.Join(dir, "xsub"), 0755); err != nil {
		t.Fatal(err)
	}
	mustWriteFile(t, filepath.Join(dir, "xsub", "inner.txt"), "")

	restore := chdir(t, dir)
	defer restore()

	t.Run("relative dot-slash prefix", func(t *testing.T) {
		// The panic/corruption case: partial retains "./" that the entries
		// on disk don't carry.
		got := completedNames(t, "./x")
		want := []string{"./x", "./xsub/", "./xyz.txt"}
		assertSuggestions(t, got, want)
	})

	t.Run("relative dot-slash, empty base", func(t *testing.T) {
		got := completedNames(t, "./")
		want := []string{"./other.txt", "./x", "./xsub/", "./xyz.txt"}
		assertSuggestions(t, got, want)
	})

	t.Run("plain relative, no separator", func(t *testing.T) {
		got := completedNames(t, "x")
		want := []string{"x", "xsub/", "xyz.txt"}
		assertSuggestions(t, got, want)
	})

	t.Run("directory with trailing slash", func(t *testing.T) {
		got := completedNames(t, "./xsub/")
		want := []string{"./xsub/inner.txt"}
		assertSuggestions(t, got, want)
	})

	t.Run("parent-relative prefix", func(t *testing.T) {
		sub := filepath.Join(dir, "xsub")
		restoreSub := chdir(t, sub)
		defer restoreSub()

		got := completedNames(t, "../x")
		want := []string{"../x", "../xsub/", "../xyz.txt"}
		assertSuggestions(t, got, want)
	})

	t.Run("absolute path", func(t *testing.T) {
		abs := filepath.Join(dir, "x")
		got := completedNames(t, abs)
		want := []string{abs, abs + "sub" + string(filepath.Separator), abs + "yz.txt"}
		assertSuggestions(t, got, want)
	})

	t.Run("no match returns no suggestions, no panic", func(t *testing.T) {
		ret := completeFilePath("./nope-nothing-matches")
		if len(ret.Suggestions) != 0 {
			t.Errorf("expected no suggestions, got %v", ret.Suggestions)
		}
	})
}

// completedNames calls completeFilePath(partial) and reconstructs the full
// suggested path (partial + suggestion) for each suggestion, so test cases
// can assert against the final path a user would see inserted rather than
// reason about raw suffix slicing.
func completedNames(t *testing.T, partial string) []string {
	t.Helper()
	ret := completeFilePath(partial)
	out := make([]string, len(ret.Suggestions))
	for i, s := range ret.Suggestions {
		out[i] = partial + s
	}
	if ret.Prefix != partial {
		t.Errorf("completeFilePath(%q).Prefix = %q, want %q", partial, ret.Prefix, partial)
	}
	return out
}

func assertSuggestions(t *testing.T, got, want []string) {
	t.Helper()
	sort.Strings(got)
	sort.Strings(want)
	if !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

// chdir switches the process's working directory to dir and returns a func
// that restores the original — tests using this must not run in parallel
// with each other or with anything else that reads the process cwd.
func chdir(t *testing.T, dir string) func() {
	t.Helper()
	old, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	return func() {
		if err := os.Chdir(old); err != nil {
			t.Fatal(err)
		}
	}
}
