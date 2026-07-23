package chat

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// ConversationStore persists conversations; see DirStore for the default JSONL implementation.  Entries are
// append-only: replaying a conversation renders its stored entries through the same views as the live path.
type ConversationStore interface {
	// Create records a new conversation from its metadata.
	Create(meta ConversationMeta) error
	// List returns metadata for every stored conversation, newest first.
	List() ([]ConversationMeta, error)
	// Append adds one entry to a conversation's transcript.
	Append(id string, entry Entry) error
	// Read returns a conversation's metadata and full transcript.
	Read(id string) (ConversationMeta, []Entry, error)
	// Rename updates a conversation's title; the component derives it from the first prompt.
	Rename(id, title string) error
	// Delete removes a conversation and its transcript.
	Delete(id string) error
}

// ConversationMeta is the header describing one conversation.
type ConversationMeta struct {
	ID      string    `json:"id"`
	Title   string    `json:"title"`
	Profile string    `json:"profile"` // name of the AgentProfile the conversation was created with
	Created time.Time `json:"created"`

	// Ext carries host-defined metadata (datalog would keep its conversation mode here).
	Ext map[string]any `json:"ext,omitempty"`
}

// Entry is one transcript line: either a user prompt or an agent event, never both.
type Entry struct {
	Prompt string `json:"prompt,omitempty"`
	Event  *Event `json:"event,omitempty"`
}

// DirStore returns the default ConversationStore, writing one {uuid}.jsonl per conversation under dir: line 1 is
// the ConversationMeta header, each following line one Entry.
func DirStore(dir string) ConversationStore {
	return &dirStore{dir: dir}
}

type dirStore struct {
	// mu serializes Append/Create/Delete against each other for a
	// single-process posture — the file-append itself is one O_APPEND write
	// syscall (atomic on POSIX), but the mutex keeps Create's O_EXCL check and
	// a concurrent Append from interleaving on the same conversation.
	mu  sync.Mutex
	dir string
}

// path returns the JSONL file for a conversation id.  The id is a UUID minted
// by the component (never user text), so no path-escaping is attempted beyond
// rejecting a separator that would climb out of dir.
func (st *dirStore) path(id string) (string, error) {
	if id == "" || filepath.Base(id) != id {
		return "", fmt.Errorf(`chat: invalid conversation id %q`, id)
	}
	return filepath.Join(st.dir, id+".jsonl"), nil
}

func (st *dirStore) Create(meta ConversationMeta) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	if err := os.MkdirAll(st.dir, 0o755); err != nil {
		return fmt.Errorf(`chat: creating store dir: %w`, err)
	}
	path, err := st.path(meta.ID)
	if err != nil {
		return err
	}
	// O_EXCL so a duplicate id is a clean error, never a silent overwrite of an
	// existing transcript.
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		if os.IsExist(err) {
			return fmt.Errorf(`chat: conversation %q already exists`, meta.ID)
		}
		return err
	}
	defer f.Close()
	line, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	if _, err := f.Write(append(line, '\n')); err != nil {
		return err
	}
	return nil
}

func (st *dirStore) List() ([]ConversationMeta, error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	ents, err := os.ReadDir(st.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	metas := make([]ConversationMeta, 0, len(ents))
	for _, ent := range ents {
		if ent.IsDir() || filepath.Ext(ent.Name()) != ".jsonl" {
			continue
		}
		meta, err := st.readHeader(filepath.Join(st.dir, ent.Name()))
		if err != nil {
			continue // an unreadable/partial file never sinks the whole list
		}
		metas = append(metas, meta)
	}
	sort.Slice(metas, func(i, j int) bool { return metas[i].Created.After(metas[j].Created) })
	return metas, nil
}

// readHeader reads just line 1 (the ConversationMeta) without slurping the
// whole transcript, for List.
func (st *dirStore) readHeader(path string) (ConversationMeta, error) {
	f, err := os.Open(path)
	if err != nil {
		return ConversationMeta{}, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	if !sc.Scan() {
		if err := sc.Err(); err != nil {
			return ConversationMeta{}, err
		}
		return ConversationMeta{}, fmt.Errorf(`chat: empty conversation file %q`, path)
	}
	var meta ConversationMeta
	if err := json.Unmarshal(sc.Bytes(), &meta); err != nil {
		return ConversationMeta{}, err
	}
	return meta, nil
}

func (st *dirStore) Append(id string, entry Entry) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	path, err := st.path(id)
	if err != nil {
		return err
	}
	line, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	// One write syscall per entry: the marshalled line plus its newline in a
	// single buffer, so an O_APPEND write lands atomically and never interleaves
	// with a concurrent append from another process.
	if _, err := f.Write(append(line, '\n')); err != nil {
		return err
	}
	return nil
}

func (st *dirStore) Read(id string) (ConversationMeta, []Entry, error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	path, err := st.path(id)
	if err != nil {
		return ConversationMeta{}, nil, err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return ConversationMeta{}, nil, fmt.Errorf(`chat: no conversation %q`, id)
		}
		return ConversationMeta{}, nil, err
	}
	lines := bytes.Split(bytes.TrimRight(data, "\n"), []byte{'\n'})
	if len(lines) == 0 || len(lines[0]) == 0 {
		return ConversationMeta{}, nil, fmt.Errorf(`chat: empty conversation file for %q`, id)
	}
	var meta ConversationMeta
	if err := json.Unmarshal(lines[0], &meta); err != nil {
		return ConversationMeta{}, nil, err
	}
	entries := make([]Entry, 0, len(lines)-1)
	for _, ln := range lines[1:] {
		if len(ln) == 0 {
			continue
		}
		var e Entry
		if err := json.Unmarshal(ln, &e); err != nil {
			return ConversationMeta{}, nil, err
		}
		entries = append(entries, e)
	}
	return meta, entries, nil
}

func (st *dirStore) Rename(id, title string) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	path, err := st.path(id)
	if err != nil {
		return err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf(`chat: no conversation %q`, id)
		}
		return err
	}
	idx := bytes.IndexByte(data, '\n')
	if idx < 0 {
		idx = len(data)
	}
	var meta ConversationMeta
	if err := json.Unmarshal(data[:idx], &meta); err != nil {
		return err
	}
	meta.Title = title
	header, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	// The header line lives at the head of the file, so a title change rewrites
	// the whole file; write-then-rename keeps a crash from truncating the
	// transcript mid-rewrite.
	out := append(header, '\n')
	if idx < len(data) {
		out = append(out, data[idx+1:]...)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, out, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func (st *dirStore) Delete(id string) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	path, err := st.path(id)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// newMemoryStore backs New when no store is configured: conversations live only until Shutdown.
func newMemoryStore() ConversationStore {
	return &memoryStore{convs: make(map[string]*memoryConversation)}
}

type memoryStore struct {
	mu    sync.Mutex
	convs map[string]*memoryConversation
}

type memoryConversation struct {
	meta    ConversationMeta
	entries []Entry
}

func (st *memoryStore) Create(meta ConversationMeta) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	if _, dup := st.convs[meta.ID]; dup {
		return fmt.Errorf(`chat: conversation %q already exists`, meta.ID)
	}
	st.convs[meta.ID] = &memoryConversation{meta: meta}
	return nil
}

func (st *memoryStore) List() ([]ConversationMeta, error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	metas := make([]ConversationMeta, 0, len(st.convs))
	for _, conv := range st.convs {
		metas = append(metas, conv.meta)
	}
	sort.Slice(metas, func(i, j int) bool { return metas[i].Created.After(metas[j].Created) })
	return metas, nil
}

func (st *memoryStore) Append(id string, entry Entry) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	conv, ok := st.convs[id]
	if !ok {
		return fmt.Errorf(`chat: no conversation %q`, id)
	}
	conv.entries = append(conv.entries, entry)
	return nil
}

func (st *memoryStore) Read(id string) (ConversationMeta, []Entry, error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	conv, ok := st.convs[id]
	if !ok {
		return ConversationMeta{}, nil, fmt.Errorf(`chat: no conversation %q`, id)
	}
	return conv.meta, append([]Entry(nil), conv.entries...), nil
}

func (st *memoryStore) Rename(id, title string) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	conv, ok := st.convs[id]
	if !ok {
		return fmt.Errorf(`chat: no conversation %q`, id)
	}
	conv.meta.Title = title
	return nil
}

func (st *memoryStore) Delete(id string) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	delete(st.convs, id)
	return nil
}
