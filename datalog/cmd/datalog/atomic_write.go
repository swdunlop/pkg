package main

import (
	"fmt"
	"os"
	"path/filepath"
)

// writeFileAtomic atomically writes text (with a trailing newline added if
// missing, matching every .dl/.yaml file this package writes elsewhere) to
// filepath.Join(dir, name): a temp file created IN THE SAME DIRECTORY (so
// os.Rename is guaranteed atomic — a cross-filesystem rename is not) is
// written and fsynced, then renamed over the target.
//
// This is the ONE atomic-write chokepoint for every CRUD write this package
// performs (doc/features/workbench-v2.md design decision 5: "Atomic write
// helper is shared... Fix at the mechanism, not the call site."). It used to
// be rules_crud.go's writeGroupFile, written solely for put_rule_group; the
// schema CRUD surface (schema_crud.go) needs byte-for-byte the same
// discipline for its one schema-file rewrite per write, so rather than
// copy/pasting a second temp-file-and-rename implementation, both callers
// share this one. The fsnotify watcher (phase 1d) depends on this
// discipline too: it must never observe a half-written file, whether the
// write came from put_rule_group, a schema CRUD tool, or (later) a vim
// save racing a debounce window.
func writeFileAtomic(dir, name, text string) error {
	if len(text) == 0 || text[len(text)-1] != '\n' {
		text += "\n"
	}
	f, err := os.CreateTemp(dir, name+".tmp-*")
	if err != nil {
		return fmt.Errorf("writing %s: creating temp file: %w", name, err)
	}
	tmpName := f.Name()
	// On any failure path below, remove the temp file so a rejected write
	// leaves no droppings in the directory — the rename below is the ONLY
	// path that consumes tmpName; every other exit removes it.
	succeeded := false
	defer func() {
		if !succeeded {
			os.Remove(tmpName)
		}
	}()

	if _, err := f.WriteString(text); err != nil {
		f.Close()
		return fmt.Errorf("writing %s: %w", name, err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("writing %s: syncing: %w", name, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("writing %s: closing: %w", name, err)
	}

	full := filepath.Join(dir, name)
	if err := os.Rename(tmpName, full); err != nil {
		return fmt.Errorf("writing %s: renaming into place: %w", full, err)
	}
	succeeded = true
	return nil
}
