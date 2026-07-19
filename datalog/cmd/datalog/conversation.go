package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/mark3labs/kit/pkg/kit"
	"github.com/mark3labs/mcp-go/server"
)

// This file implements phase 2's conversation backend (doc/features/
// workbench-v2.md design decisions 5 and 6): conversations persist as kit
// sessions, one JSONL file per conversation under a project's
// .datalog/sessions directory, and each conversation's mode (design
// decision 5's three-mode picker) rides along in the session's extension
// data so Resume reconstructs the same mode-scoped agent kit.New built at
// Create time.
//
// kit API note (verified against the actual v0.83.1 module, not assumed
// from notes — see this task's brief): kit.Options.SessionDir is NOT a
// literal storage directory. kit.New sanitizes it into a subdirectory of
// ~/.kit/sessions/ (slashes become "--"), and kit.ListSessions(dir) applies
// the SAME sanitization to dir before scanning — it never lists a literal
// directory's contents. Neither gives this package a way to make kit write
// literally under <project>/.datalog/sessions, which decision 6 requires
// ("travels with the project," "gitignorable" — both false for a path
// that's actually under the user's home). kit.Options.SessionPath, in
// contrast, opens an EXACT literal path — but only if the file already
// exists ("open a specific session file", confirmed by experiment: New
// against a nonexistent SessionPath fails with "no such file or
// directory"). This package works with that grain instead of fighting it:
// Create pre-seeds a minimal valid session-header line at the literal
// target path itself, then every open (Create's own extension-data write,
// Resume, List's per-file inspection) uses SessionPath against that exact
// path. List and Delete are implemented directly against the directory
// (kit.ListSessions is not used — it cannot see a literal directory) but
// still read the kit JSONL format through kit.OpenTreeSession, so "persists
// as kit sessions" holds: the bytes on disk, the tree structure, and the
// extension-data mechanism are all kit's, only the discovery/listing logic
// is this package's own.

// conversationMode is the human's mode choice at conversation creation
// (design decision 5), stored in the session's extension data so Resume
// can reconstruct the same tool registration and system prompt. It maps
// 1:1 to toolMode (mcp.go) but is a distinct type: toolMode is this
// package's MCP-registration vocabulary, conversationMode is the
// conversation-manager's persisted-state vocabulary, and keeping them
// separate means a future conversationMode value (e.g. a mode with no MCP
// tools at all) would not force a matching toolMode to exist.
type conversationMode string

const (
	conversationModeQuery conversationMode = "query"
	conversationModeRules conversationMode = "rules"
	conversationModeFacts conversationMode = "facts"
)

// toolMode converts a conversationMode to the mcp.go registration
// vocabulary. An unrecognized mode maps to "" (registerToolsForMode's
// default case, which registers nothing) rather than panicking — a
// corrupted or future-versioned extension-data value should fail closed
// (no tools) not crash the conversation manager.
func (m conversationMode) toolMode() toolMode {
	switch m {
	case conversationModeQuery:
		return toolModeQuery
	case conversationModeRules:
		return toolModeRules
	case conversationModeFacts:
		return toolModeFacts
	default:
		return ""
	}
}

// validConversationMode reports whether m is one of the three modes design
// decision 5 defines — Create's one input-validation gate.
func validConversationMode(m conversationMode) bool {
	switch m {
	case conversationModeQuery, conversationModeRules, conversationModeFacts:
		return true
	default:
		return false
	}
}

// conversationExtType is the extension-data type name AppendExtensionData/
// GetExtensionData use to persist a conversation's mode (this task's
// brief). Namespaced with "datalog-" so a future kit extension (or another
// embedder sharing a session store) cannot collide with it.
const conversationExtType = "datalog-conversation"

// conversationExtData is conversationExtType's JSON payload. A struct
// (rather than a bare mode string) so a later field — model override,
// browser tab focus, whatever phase 3 needs — is additive.
type conversationExtData struct {
	Mode conversationMode `json:"mode"`
}

// conversationTitleMaxLen bounds the auto-title SetSessionName derives from
// the first user message (this task's brief: "~60 chars, whole-word").
const conversationTitleMaxLen = 60

// conversationInfo is one conversation's metadata for the rail (List) —
// trimmed from kit.SessionInfo's fields to what this package actually
// consumes, plus Mode (which kit.SessionInfo has no field for: it lives in
// this package's own extension-data convention, not kit's core session
// header).
type conversationInfo struct {
	ID           string
	Path         string
	Name         string
	Mode         conversationMode
	Created      time.Time
	Modified     time.Time
	MessageCount int
	FirstMessage string
}

// conversationManager owns one project's conversation directory
// (<project>/.datalog/sessions). It is a thin, stateless wrapper — every
// method re-reads disk — matching the spec's "disk is canonical" posture
// (design decision 3) extended to conversations: there is no in-memory
// index to fall out of sync with the JSONL files.
type conversationManager struct {
	dir string
}

// newConversationManager returns a manager rooted at dir. The directory is
// NOT created here — Create makes it on the first conversation instead — so
// a workbench that never starts a conversation (tests over the checked-in
// examples included) leaves no .datalog droppings in the project; List
// already treats a missing directory as an empty listing.
func newConversationManager(dir string) (*conversationManager, error) {
	return &conversationManager{dir: dir}, nil
}

// conversationSessionsDir derives the .datalog/sessions path from a
// project's schema file path or rules directory (design decision 6: "one
// JSONL session file per conversation under <project>/.datalog/sessions").
// configPath and rulesDir mirror newWorkbench's own parameters; the project
// root is the schema file's directory when a schema is configured (the
// common case: -c is the operator's usual entry point), falling back to the
// rules directory's parent when only --rules was given, and finally to the
// current working directory when the session has neither — an operator
// running a schema-less, rules-less session (query-only over a bare data
// directory) still gets a place for conversations to live.
func conversationSessionsDir(configPath, rulesDir string) (string, error) {
	var root string
	switch {
	case configPath != "":
		root = filepath.Dir(configPath)
	case rulesDir != "":
		root = filepath.Dir(filepath.Clean(rulesDir))
	default:
		wd, err := os.Getwd()
		if err != nil {
			return "", fmt.Errorf("resolving project directory: %w", err)
		}
		root = wd
	}
	return filepath.Join(root, ".datalog", "sessions"), nil
}

// sessionPath is the literal on-disk path for conversation id — the one
// naming rule every method in this file agrees on.
func (cm *conversationManager) sessionPath(id string) string {
	return filepath.Join(cm.dir, id+".jsonl")
}

// seedSessionFile writes the minimal valid kit session-header line kit's
// SessionPath-open path requires to already exist (see this file's top
// comment). This is the ONE place that hand-writes kit's on-disk format
// instead of going through a kit API — everything else in this file reads
// or appends through kit.OpenTreeSession/kit.New, exactly like any other
// kit session consumer. The header's shape ({"type":"session","version":3,
// "id":...,"timestamp":...,"cwd":...}) mirrors what kit itself writes as a
// brand-new session's first line (confirmed by inspecting a kit-created
// session file directly); cwd is set to cm.dir since that is the closest
// analog to "the directory this session belongs to" in a world where
// SessionDir's own meaning (see top comment) is unusable here.
func seedSessionFile(path, id, dir string) error {
	line := fmt.Sprintf(`{"type":"session","version":3,"id":%q,"timestamp":%q,"cwd":%q}`+"\n",
		id, time.Now().UTC().Format("2006-01-02T15:04:05.000Z"), dir)
	return os.WriteFile(path, []byte(line), 0o644)
}

// Create starts a new conversation in mode, persists the mode into the
// session's extension data immediately (so even a conversation with no
// messages yet reports its mode correctly), and returns its metadata.
// newSessionID is a package variable so tests can pin deterministic IDs
// without patching uuid.NewString globally.
func (cm *conversationManager) Create(mode conversationMode) (*conversationInfo, error) {
	if !validConversationMode(mode) {
		return nil, fmt.Errorf("invalid conversation mode %q", mode)
	}
	// The sessions directory appears on first use (see newConversationManager:
	// a workbench that never converses must leave no .datalog droppings).
	if err := os.MkdirAll(cm.dir, 0o755); err != nil {
		return nil, fmt.Errorf("creating conversation directory %s: %w", cm.dir, err)
	}
	id := newSessionID()
	path := cm.sessionPath(id)
	if err := seedSessionFile(path, id, cm.dir); err != nil {
		return nil, fmt.Errorf("creating conversation %s: %w", id, err)
	}

	tm, err := kit.OpenTreeSession(path)
	if err != nil {
		return nil, fmt.Errorf("opening new conversation %s: %w", id, err)
	}
	defer tm.Close()

	sm := kit.NewTreeManagerAdapter(tm)
	data, err := marshalConversationExtData(conversationExtData{Mode: mode})
	if err != nil {
		return nil, err
	}
	if _, err := sm.AppendExtensionData(conversationExtType, data); err != nil {
		return nil, fmt.Errorf("recording mode for conversation %s: %w", id, err)
	}

	return conversationInfoFromTree(tm, path)
}

// newSessionID is the ID generator Create uses — a package variable so
// tests can substitute a deterministic generator instead of monkeypatching
// the uuid package.
var newSessionID = uuid.NewString

// List returns every conversation under cm.dir, newest-first by Modified —
// this package's own directory scan (see this file's top comment for why
// kit.ListSessions cannot be used against a literal directory), reading
// each file through kit.OpenTreeSession so the parsing itself is still
// kit's own JSONL-tree logic, not a hand-rolled reader. A file that fails
// to open (mid-write, corrupted, or simply not a session file) is skipped
// rather than failing the whole listing — one bad file must not hide every
// other conversation from the rail.
func (cm *conversationManager) List() ([]conversationInfo, error) {
	entries, err := os.ReadDir(cm.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("listing conversations in %s: %w", cm.dir, err)
	}

	var out []conversationInfo
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".jsonl" {
			continue
		}
		path := filepath.Join(cm.dir, e.Name())
		tm, err := kit.OpenTreeSession(path)
		if err != nil {
			continue // skip unreadable/corrupt/mid-write files, see doc comment
		}
		info, err := conversationInfoFromTree(tm, path)
		tm.Close()
		if err != nil {
			continue
		}
		out = append(out, *info)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].Modified.After(out[j].Modified)
	})
	return out, nil
}

// Delete removes a conversation's session file entirely.
func (cm *conversationManager) Delete(id string) error {
	return kit.DeleteSession(cm.sessionPath(id))
}

// Get returns one conversation's metadata directly by id, without scanning
// the whole directory — List's per-file work, applied to a single known
// path. Used by tests (and any future single-conversation UI fetch) that
// don't need the full rail.
func (cm *conversationManager) Get(id string) (*conversationInfo, error) {
	path := cm.sessionPath(id)
	tm, err := kit.OpenTreeSession(path)
	if err != nil {
		return nil, fmt.Errorf("opening conversation %s: %w", id, err)
	}
	defer tm.Close()
	return conversationInfoFromTree(tm, path)
}

// ModeOf reads a conversation's persisted mode without constructing a full
// *kit.Kit (no LLM/provider config touched at all) — Resume's first step,
// and useful on its own for any caller that only needs the mode.
func (cm *conversationManager) ModeOf(id string) (conversationMode, error) {
	path := cm.sessionPath(id)
	tm, err := kit.OpenTreeSession(path)
	if err != nil {
		return "", fmt.Errorf("opening conversation %s: %w", id, err)
	}
	defer tm.Close()
	return modeFromTree(tm)
}

// Resume reconstructs a live *kit.Kit for an existing conversation: reads
// its persisted mode (ModeOf) and rebuilds the agent through the exact same
// newConversationKit path Create's caller uses, so the tool registration
// and system prompt match Create byte for byte (this task's brief: "on
// Resume, read the mode from extension data and reconstruct the kit agent
// with the SAME mode-appropriate system prompt and tool registration").
// kit itself rebuilds the message history from the session tree — Resume
// does not touch messages at all, only mode.
func (cm *conversationManager) Resume(ctx context.Context, cfg agentConfig, h *mcpHandlers, id string) (*kit.Kit, conversationMode, error) {
	mode, err := cm.ModeOf(id)
	if err != nil {
		return nil, "", fmt.Errorf("resuming conversation %s: %w", id, err)
	}
	k, err := newConversationKit(ctx, cfg, h, cm.sessionPath(id), mode)
	if err != nil {
		return nil, "", err
	}
	return k, mode, nil
}

// modeFromTree reads the LAST datalog-conversation extension-data entry on
// tm's current branch (GetExtensionData returns entries in the order they
// were appended; last-wins matches "the mode is whatever Create most
// recently recorded," which today is written exactly once but leaves room
// for a future mode-change feature without a format change). A session
// with no such entry — one this package did not create — has no mode.
func modeFromTree(tm *kit.TreeManager) (conversationMode, error) {
	sm := kit.NewTreeManagerAdapter(tm)
	entries := sm.GetExtensionData(conversationExtType)
	if len(entries) == 0 {
		return "", fmt.Errorf("conversation has no recorded mode")
	}
	var data conversationExtData
	if err := unmarshalConversationExtData(entries[len(entries)-1].Data, &data); err != nil {
		return "", fmt.Errorf("decoding conversation mode: %w", err)
	}
	if !validConversationMode(data.Mode) {
		return "", fmt.Errorf("conversation has unrecognized mode %q", data.Mode)
	}
	return data.Mode, nil
}

// conversationInfoFromTree builds a conversationInfo from an already-open
// tree — shared by Create (fresh, zero messages) and List (whatever is on
// disk). Created comes from the session header's own Timestamp (GetHeader,
// written once at seedSessionFile time and never touched again). Modified
// is the file's own mtime — every AppendX call is a synchronous write
// (verified by experiment: content is on disk before Close, not buffered
// until then), so mtime tracks the true last-write instant, exactly the
// signal a "vim could have touched this" world (design decision 3) needs
// for "newest first."
func conversationInfoFromTree(tm *kit.TreeManager, path string) (*conversationInfo, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("stat %s: %w", path, err)
	}
	mode, _ := modeFromTree(tm) // best-effort: a mode-less session still lists, just with Mode == ""

	msgs := tm.GetLLMMessages()
	first := ""
	if len(msgs) > 0 {
		first = firstTextPart(msgs[0])
	}

	return &conversationInfo{
		ID:           tm.GetSessionID(),
		Path:         path,
		Name:         tm.GetSessionName(),
		Mode:         mode,
		Created:      tm.GetHeader().Timestamp,
		Modified:     fi.ModTime(),
		MessageCount: tm.MessageCount(),
		FirstMessage: first,
	}, nil
}

// firstTextPart extracts the leading text content from an LLM message —
// GetLLMMessages' Content is a slice of typed parts (text, tool calls,
// reasoning, ...); the auto-title and FirstMessage fields only ever want
// the plain text a human typed, which is always a TextPart for a user
// message (the only role this package ever appends via AutoTitle/the
// prompt path).
func firstTextPart(msg kit.LLMMessage) string {
	for _, part := range msg.Content {
		if tp, ok := part.(kit.LLMTextPart); ok {
			return tp.Text
		}
	}
	return ""
}

// AutoTitle sets id's session name from the first user message's text,
// truncated to conversationTitleMaxLen on a whole word (this task's brief:
// "~60 chars, whole-word"). Called once, after the first user message of a
// new conversation — callers are responsible for not calling it again on
// later turns (SetSessionName would silently overwrite an operator-renamed
// title otherwise; this package has no rename UI yet, so that risk is
// theoretical today, but the one-call contract is the seam that keeps it
// theoretical).
func (cm *conversationManager) AutoTitle(id, firstMessage string) error {
	path := cm.sessionPath(id)
	tm, err := kit.OpenTreeSession(path)
	if err != nil {
		return fmt.Errorf("opening conversation %s: %w", id, err)
	}
	defer tm.Close()
	title := truncateTitle(firstMessage, conversationTitleMaxLen)
	if title == "" {
		return nil // an empty/whitespace-only first message leaves the default (untitled) name
	}
	return kit.NewTreeManagerAdapter(tm).SetSessionName(title)
}

// truncateTitle bounds s to at most max runes, breaking on a word boundary
// rather than mid-word, and trimming leading/trailing whitespace first so a
// multi-line first message collapses to one title-worthy line's worth of
// words. When even the first word exceeds max, that word is hard-cut (a
// title must never exceed max) with an ellipsis. A truncated result always
// ends in "…" so the rail can distinguish "the whole message" from "cut
// off" without re-deriving the length check.
func truncateTitle(s string, max int) string {
	s = strings.Join(strings.Fields(s), " ") // collapse all whitespace, including newlines
	if s == "" {
		return ""
	}
	runes := []rune(s)
	if len(runes) <= max {
		return s
	}
	// Reserve one rune for the ellipsis, then walk back to the last space
	// at or before that budget so the cut lands on a word boundary.
	budget := max - 1
	cut := budget
	for cut > 0 && runes[cut] != ' ' {
		cut--
	}
	if cut == 0 {
		// No space within budget at all (one very long "word") — hard cut.
		cut = budget
	}
	return strings.TrimRight(string(runes[:cut]), " ") + "…"
}

// -- conversation kit-agent construction -----------------------------------

// conversationSystemPrompt returns the mode-appropriate system prompt
// (this task's brief: "keep them concise; put them beside the existing kit
// system prompt in agent.go" — see agentSystemPrompt there for the sibling
// this package's three prompts live next to).
func conversationSystemPrompt(mode conversationMode) string {
	switch mode {
	case conversationModeQuery:
		return queryModeSystemPrompt
	case conversationModeRules:
		return rulesModeSystemPrompt
	case conversationModeFacts:
		return factsModeSystemPrompt
	default:
		return agentSystemPrompt // defensive fallback; validConversationMode already rejects this at Create
	}
}

// newConversationKit constructs a *kit.Kit for conversation id: opens (or,
// for a session Create already seeded, resumes) the session at its literal
// path, registers exactly mode's tool set on mcpSrv, and applies mode's
// system prompt. This is the ONE construction path both a fresh Create and
// a later Resume go through (this task's brief: "reconstruct the kit agent
// with the SAME mode-appropriate system prompt and tool registration") —
// there is no separate "create" variant, because kit.New against an
// existing SessionPath is already exactly a resume; a brand-new
// conversation is just a resume of a session that happens to have zero
// messages so far. h is the same mcpHandlers value the workbench's other
// surfaces (runMCP, the /mcp mount) share, exactly like newKitDriver's
// mcpSrv parameter (agent.go) — one session, N frontends, extended to N
// conversations.
func newConversationKit(ctx context.Context, cfg agentConfig, h *mcpHandlers, path string, mode conversationMode) (*kit.Kit, error) {
	if !validConversationMode(mode) {
		return nil, fmt.Errorf("invalid conversation mode %q", mode)
	}
	srv := server.NewMCPServer("datalog", "0.1.0",
		server.WithInstructions(mcpServerInstructions),
		server.WithRecovery(),
	)
	h.registerToolsForMode(srv, mode.toolMode())

	k, err := kit.New(ctx, &kit.Options{
		Model:            cfg.Model,
		ProviderURL:      cfg.ProviderURL,
		ProviderAPIKey:   cfg.ProviderAPIKey,
		SystemPrompt:     conversationSystemPrompt(mode),
		DisableCoreTools: true,
		SessionPath:      path,
		NoSkills:         true,
		NoExtensions:     true,
		NoContextFiles:   true,
		Quiet:            true,
		InProcessMCPServers: map[string]*kit.MCPServer{
			"datalog": srv,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("constructing agent for conversation at %s: %w", path, err)
	}
	return k, nil
}

// marshalConversationExtData/unmarshalConversationExtData are the JSON
// codec for conversationExtData, factored out only so Create/modeFromTree
// don't repeat the same two-line json.Marshal/Unmarshal dance and its error
// wrapping.
func marshalConversationExtData(data conversationExtData) (string, error) {
	b, err := json.Marshal(data)
	if err != nil {
		return "", fmt.Errorf("encoding conversation extension data: %w", err)
	}
	return string(b), nil
}

func unmarshalConversationExtData(s string, out *conversationExtData) error {
	return json.Unmarshal([]byte(s), out)
}

// -- global one-turn gate ---------------------------------------------------

// conversationTurnJobKey is the sandbox jobs-set key every conversation's
// turn registers under (this task's brief: "one turn at a time across ALL
// conversations"). Using ONE key regardless of which conversation is
// sending — rather than a per-conversation key — is what makes the gate
// global: sandbox.jobs.Begin already refuses a second Begin under the same
// key (see jobs' doc comment in sandbox.go), so reusing that exact
// mechanism (rather than inventing a parallel mutex) gets the "fails fast"
// behavior for free and keeps this package's only two turn-gating call
// sites — the v1 Agent tab's agentTurnJobKey and this one — visible to the
// same Global Cancel / $busy machinery serve.go already wires up. A
// DIFFERENT key from agentTurnJobKey ("conversation" vs "agent") is
// deliberate: v1's Agent tab and phase-2 conversations are two independent
// turn-taking surfaces until the v1 tab is retired (this task's brief:
// "the v1 agent tab may keep using a default conversation or stay as-is —
// do NOT rebuild any UI"), so gating them under the same key would block
// the untouched v1 tab against phase-2 conversations for no reason today.
// A later step that actually retires the v1 tab can fold the two keys back
// into one.
const conversationTurnJobKey = "conversation-turn"

// conversationTurnGate serializes prompt turns across every conversation,
// on top of the same *jobs cancel-set serve.go's $busy machinery already
// uses (design decision 6: "matches the single $busy mutex and the
// one-human posture"). It additionally remembers WHICH conversation
// currently owns the turn, purely so a rejected concurrent send can name it
// ("turn running in <name>") — jobs itself carries no such metadata slot.
type conversationTurnGate struct {
	jobs *jobs

	mu        sync.Mutex
	owner     string // conversation id of the in-flight turn, "" when idle
	ownerName string // that conversation's display name (or id, if untitled), for the error message
}

// newConversationTurnGate wraps an existing *jobs value — callers share the
// SAME *jobs the workbench's other Global-Cancel-able operations use
// (wb.jobs), so Stop/CancelAll reach a conversation turn exactly like they
// reach the v1 Agent tab's turn or the fsnotify watcher's re-evaluation.
func newConversationTurnGate(j *jobs) *conversationTurnGate {
	return &conversationTurnGate{jobs: j}
}

// errConversationTurnBusy is returned by Begin when another conversation's
// turn is already running. name is the running conversation's display
// name (or id, if it has no title yet) so the composer can render "turn
// running in <name>" verbatim (this task's brief).
type errConversationTurnBusy struct{ name string }

func (e *errConversationTurnBusy) Error() string {
	return fmt.Sprintf("turn running in %s", e.name)
}

// Begin acquires the gate for conversation id/name, deriving a cancellable
// context from parent exactly like jobs.Begin (Global Cancel reaches it the
// same way). The returned done func must be deferred by the caller; it
// releases the gate for the next conversation's turn.
func (g *conversationTurnGate) Begin(parent context.Context, id, name string) (context.Context, func(), error) {
	g.mu.Lock()
	if g.owner != "" {
		busyName := g.ownerName
		g.mu.Unlock()
		return nil, nil, &errConversationTurnBusy{name: busyName}
	}
	// Claim ownership before calling jobs.Begin: jobs.Begin cannot itself
	// fail here (conversationTurnJobKey is only ever held by this gate,
	// under g.mu, so no other caller can have raced us to it), but claiming
	// first keeps the "who owns it" state and the "is a turn running" state
	// from ever disagreeing even under a future refactor.
	g.owner, g.ownerName = id, name
	g.mu.Unlock()

	ctx, jobDone := g.jobs.Begin(parent, conversationTurnJobKey)
	if ctx == nil {
		// jobs disagreeing with g.owner would mean something outside this
		// gate also holds conversationTurnJobKey — a programming error, not
		// a runtime condition a caller can trigger. Release our claim and
		// report it as busy rather than panic, since a wrong tool surface
		// bug should degrade to "can't send right now," not crash a turn.
		g.mu.Lock()
		g.owner, g.ownerName = "", ""
		g.mu.Unlock()
		return nil, nil, &errConversationTurnBusy{name: name}
	}

	done := func() {
		jobDone()
		g.mu.Lock()
		g.owner, g.ownerName = "", ""
		g.mu.Unlock()
	}
	return ctx, done, nil
}

// Running reports the display name of the conversation currently holding
// the gate, and whether one is running at all — the composer's "turn
// running in <name>" disabled-state check.
func (g *conversationTurnGate) Running() (name string, running bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.ownerName, g.owner != ""
}

// RunningID reports the conversation ID currently holding the gate — the
// delete handler's "is the running turn THIS conversation's" check.
func (g *conversationTurnGate) RunningID() (id string, running bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.owner, g.owner != ""
}
