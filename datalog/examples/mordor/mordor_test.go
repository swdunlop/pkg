package mordor_test

import (
	"archive/zip"
	"context"
	"embed"
	"encoding/json"
	"io"
	"io/fs"
	"testing"
	"testing/fstest"

	"gopkg.in/yaml.v3"
	"swdunlop.dev/pkg/datalog/jsonfacts"
	"swdunlop.dev/pkg/datalog/seminaive"
	"swdunlop.dev/pkg/datalog/syntax"
)

//go:embed covenant_copy_smb.zip
//go:embed mordor.yaml
//go:embed rules.dl
var content embed.FS

// loadYAMLConfig reads a YAML jsonfacts config and returns a Config.
func loadYAMLConfig(fsys fs.FS, name string) (jsonfacts.Config, error) {
	data, err := fs.ReadFile(fsys, name)
	if err != nil {
		return jsonfacts.Config{}, err
	}
	var raw any
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return jsonfacts.Config{}, err
	}
	data, err = json.Marshal(raw)
	if err != nil {
		return jsonfacts.Config{}, err
	}
	var cfg jsonfacts.Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return jsonfacts.Config{}, err
	}
	return cfg, nil
}

// unzipToMapFS extracts a zip file from fsys into a fstest.MapFS.
func unzipToMapFS(fsys fs.FS, name string) (fstest.MapFS, error) {
	f, err := fsys.Open(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	data, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}
	r, err := zip.NewReader(bytesReaderAt(data), int64(len(data)))
	if err != nil {
		return nil, err
	}

	result := fstest.MapFS{}
	for _, zf := range r.File {
		rc, err := zf.Open()
		if err != nil {
			return nil, err
		}
		body, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			return nil, err
		}
		result[zf.Name] = &fstest.MapFile{Data: body}
	}
	return result, nil
}

type readerAtBytes []byte

func (r readerAtBytes) ReadAt(p []byte, off int64) (int, error) {
	if off >= int64(len(r)) {
		return 0, io.EOF
	}
	n := copy(p, r[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func bytesReaderAt(data []byte) readerAtBytes { return readerAtBytes(data) }

func loadDataset(t *testing.T) (*jsonfacts.Config, fstest.MapFS) {
	t.Helper()

	cfg, err := loadYAMLConfig(content, "mordor.yaml")
	if err != nil {
		t.Fatalf("loading config: %v", err)
	}

	dataFS, err := unzipToMapFS(content, "covenant_copy_smb.zip")
	if err != nil {
		t.Fatalf("unzipping dataset: %v", err)
	}

	return &cfg, dataFS
}

func TestLoadFacts(t *testing.T) {
	cfg, dataFS := loadDataset(t)

	db, err := cfg.LoadFS(dataFS)
	if err != nil {
		t.Fatal(err)
	}

	// Verify we loaded the expected predicates and counts.
	expected := map[string]struct {
		arity int
		count int
	}{
		"net_conn":       {8, 3},
		"file_create":    {3, 5},
		"image_load":     {5, 1},
		"proc_access":    {4, 10},
		"proc_terminate": {3, 3},
		"reg_key":        {3, 123},
		"reg_value":      {4, 59},
		"logon":          {7, 2},
		"special_priv":   {4, 2},
		"share_access":   {4, 2},
		"share_file":     {6, 3},
	}

	for pred, want := range expected {
		got := 0
		for range db.Facts(pred, want.arity) {
			got++
		}
		if got != want.count {
			t.Errorf("%s/%d: got %d facts, want %d", pred, want.arity, got, want.count)
		}
	}

	// Verify matchers produced results.
	matcherChecks := []struct {
		pred  string
		arity int
		desc  string
	}{
		{"contains", 2, "SMB port / admin share matches"},
		{"cidr_match", 2, "subnet matches"},
		{"ci_ends_with", 2, "executable extension matches"},
	}
	for _, mc := range matcherChecks {
		count := 0
		for range db.Facts(mc.pred, mc.arity) {
			count++
		}
		t.Logf("%s/%d (%s): %d facts", mc.pred, mc.arity, mc.desc, count)
		if count == 0 {
			t.Errorf("expected %s facts for %s, got none", mc.pred, mc.desc)
		}
	}
}

func TestLateralMovementDetection(t *testing.T) {
	cfg, dataFS := loadDataset(t)

	db, err := cfg.LoadFS(dataFS)
	if err != nil {
		t.Fatal(err)
	}

	rulesData, err := fs.ReadFile(content, "rules.dl")
	if err != nil {
		t.Fatal(err)
	}

	rs, err := syntax.ParseAll(string(rulesData))
	if err != nil {
		t.Fatal(err)
	}

	tr, err := seminaive.New().Compile(rs)
	if err != nil {
		t.Fatal(err)
	}

	output, err := tr.Transform(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}

	// Verify intermediate derived predicates.
	intermediates := []struct {
		pred  string
		arity int
		desc  string
	}{
		{"smb_conn", 3, "SMB connections"},
		{"remote_logon", 4, "remote logons"},
		{"admin_share", 4, "admin share access"},
		{"exe_drop", 5, "executable drops via share"},
		{"elevated_logon", 2, "elevated logons"},
		{"exe_on_disk", 3, "executables on disk"},
	}
	for _, im := range intermediates {
		count := 0
		for row := range output.Facts(im.pred, im.arity) {
			terms := make([]string, len(row))
			for i, c := range row {
				terms[i] = c.String()
			}
			t.Logf("  %s: %v", im.pred, terms)
			count++
		}
		t.Logf("%s: %d facts", im.desc, count)
		if count == 0 {
			t.Errorf("expected %s facts, got none", im.pred)
		}
	}

	// The kill chain: lateral movement should be fully reconstructed.
	lmCount := 0
	for row := range output.Facts("lateral_movement", 4) {
		t.Logf("LATERAL MOVEMENT: user=%s src=%s target=%s path=%s",
			row[0], row[1], row[2], row[3])
		lmCount++
	}
	if lmCount == 0 {
		t.Error("expected lateral_movement facts — kill chain not reconstructed")
	}

	// The lateral movement should also be flagged as elevated.
	elmCount := 0
	for row := range output.Facts("elevated_lateral_movement", 4) {
		t.Logf("ELEVATED LATERAL MOVEMENT: user=%s src=%s target=%s path=%s",
			row[0], row[1], row[2], row[3])
		elmCount++
	}
	if elmCount == 0 {
		t.Error("expected elevated_lateral_movement facts, got none")
	}

	// Corroboration: share drop should match Sysmon file-create.
	cdCount := 0
	for row := range output.Facts("confirmed_drop", 4) {
		t.Logf("CONFIRMED DROP: user=%s host=%s share_path=%s disk_path=%s",
			row[0], row[1], row[2], row[3])
		cdCount++
	}
	t.Logf("confirmed_drop: %d facts", cdCount)
}
