package jsonfacts

import (
	"bufio"
	"fmt"
	"io/fs"
	"strings"
)

// loadPatternFileFS reads a pattern file from fsys, returning one pattern
// per non-empty, non-comment line. Lines starting with # are comments.
func loadPatternFileFS(fsys fs.FS, path string) ([]string, error) {
	f, err := fsys.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var patterns []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		patterns = append(patterns, line)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return patterns, nil
}

// ResolveFromFS reads every matcher's _from pattern files from fsys and
// merges their contents into the corresponding inline pattern lists,
// clearing the _from fields so the Config becomes self-contained. This is
// the same resolution LoadSchemaFS performs automatically for schema files
// loaded via LoadSchemaDir/LoadSchemaFS; callers that build or receive a
// Config some other way (for example, a Config parsed from a
// caller-submitted document rather than read from a schema file) call
// ResolveFromFS themselves before LoadFS, since LoadFS does not resolve
// _from fields on its own.
func (cfg *Config) ResolveFromFS(fsys fs.FS) error {
	for i := range cfg.Matchers {
		if err := cfg.Matchers[i].resolveFromFS(fsys); err != nil {
			return fmt.Errorf("matcher %d: %w", i, err)
		}
	}
	return nil
}

// resolveFromFS reads each _from file from fsys and appends the loaded
// patterns to the corresponding inline slice. The _from fields are cleared
// after resolution so the Matcher is self-contained.
func (mc *Matcher) resolveFromFS(fsys fs.FS) error {
	resolve := func(fromField *string, target *[]string) error {
		if *fromField == "" {
			return nil
		}
		patterns, err := loadPatternFileFS(fsys, *fromField)
		if err != nil {
			return fmt.Errorf("loading %s: %w", *fromField, err)
		}
		*target = append(*target, patterns...)
		*fromField = ""
		return nil
	}

	if err := resolve(&mc.ContainsFrom, &mc.Contains); err != nil {
		return err
	}
	if err := resolve(&mc.StartsWithFrom, &mc.StartsWith); err != nil {
		return err
	}
	if err := resolve(&mc.EndsWithFrom, &mc.EndsWith); err != nil {
		return err
	}
	if err := resolve(&mc.RegexMatchFrom, &mc.RegexMatch); err != nil {
		return err
	}
	if err := resolve(&mc.Base64From, &mc.Base64); err != nil {
		return err
	}
	if err := resolve(&mc.Base64UTF16From, &mc.Base64UTF16); err != nil {
		return err
	}
	if err := resolve(&mc.CIDRFrom, &mc.CIDR); err != nil {
		return err
	}
	return nil
}
