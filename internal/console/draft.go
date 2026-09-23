package console

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	yaml "go.yaml.in/yaml/v2"
)

// ConnectDraft is the half-filled Connect form, kept between the moment
// somebody starts filling it in and the moment capture starts.
//
// It exists because of one real interruption: the form shows a block of SQL to
// run on the database, and running it means leaving this page. Coming back to
// an empty form means typing it all again — and, worse, generating a DIFFERENT
// password from the one the block just created the account with.
//
// It is kept HERE, beside the server registry, and never in the browser: it
// holds a password, and this is where the product already keeps those, in a
// file only its owner can read. A draft is not a server: nothing lists it,
// nothing connects with it, and it is thrown away the moment the server it
// describes starts capturing, or somebody discards it.
type ConnectDraft struct {
	Name              string `yaml:"name,omitempty" json:"name"`
	Flavor            string `yaml:"flavor,omitempty" json:"flavor"`
	SourceHost        string `yaml:"source_host,omitempty" json:"source_host"`
	SourcePort        string `yaml:"source_port,omitempty" json:"source_port"`
	SourceUser        string `yaml:"source_user,omitempty" json:"source_user"`
	SourcePassword    string `yaml:"source_password,omitempty" json:"source_password"`
	Schemas           string `yaml:"schemas,omitempty" json:"schemas"`
	SourceDatabase    string `yaml:"source_database,omitempty" json:"source_database"`
	SourceSlot        string `yaml:"source_slot,omitempty" json:"source_slot"`
	SourcePublication string `yaml:"source_publication,omitempty" json:"source_publication"`
	// SavedAt is when this draft was last written (RFC3339), so a screen can
	// say how old it is before offering to restore it.
	SavedAt string `yaml:"saved_at,omitempty" json:"saved_at"`
}

// DraftStore holds the one Connect draft. One, not a set: Connect is a step
// somebody is inside, and a keyed set would need the browser to hold the key,
// which is exactly what "never in the browser" rules out. Two people
// connecting two databases at the same moment therefore share it, the same way
// they share the registry they are both writing to.
type DraftStore struct {
	path string // "" = in-memory only (an in-memory registry, and unit tests)
	mu   sync.Mutex
	mem  *ConnectDraft
}

// NewDraftStore returns the store for path. A path of "" never touches disk.
func NewDraftStore(path string) *DraftStore { return &DraftStore{path: path} }

// DefaultConnectDraftPath names the draft file as a sibling of the server
// registry, like the verify and backup run histories. An empty registry path
// (an in-memory registry) has no directory to be a sibling of, and must not
// spill a password into the working directory: it stays in memory.
func DefaultConnectDraftPath(serversPath string) string {
	if serversPath == "" {
		return ""
	}
	return filepath.Join(filepath.Dir(serversPath), "console-connect-draft.yaml")
}

// Load returns the saved draft. A missing or empty file is "no draft", not an
// error — an empty file is what a crash between create and write leaves, and
// restoring a form of blanks over what somebody is typing would be worse than
// restoring nothing.
//
// A file that does not parse IS an error, deliberately: reporting it as "no
// draft" would send somebody to type everything again while what they typed
// sits on disk.
func (d *DraftStore) Load() (ConnectDraft, bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.path == "" {
		if d.mem == nil {
			return ConnectDraft{}, false, nil
		}
		return *d.mem, true, nil
	}
	data, err := os.ReadFile(d.path)
	if errors.Is(err, os.ErrNotExist) {
		return ConnectDraft{}, false, nil
	}
	if err != nil {
		return ConnectDraft{}, false, fmt.Errorf("read saved form %s: %w", d.path, err)
	}
	if len(bytes.TrimSpace(data)) == 0 {
		return ConnectDraft{}, false, nil
	}
	var out ConnectDraft
	if err := yaml.Unmarshal(data, &out); err != nil {
		return ConnectDraft{}, false, fmt.Errorf("read saved form %s: %w", d.path, err)
	}
	return out, true, nil
}

// Save replaces the draft. The write is atomic and the file is 0600 inside a
// 0700 directory, the same discipline as the server registry: it holds a
// password.
func (d *DraftStore) Save(c ConnectDraft) error {
	c.SavedAt = time.Now().UTC().Format(time.RFC3339)
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.path == "" {
		d.mem = &c
		return nil
	}
	data, err := yaml.Marshal(&c)
	if err != nil {
		return fmt.Errorf("write saved form: %w", err)
	}
	return writeFilePrivateAtomic(d.path, data)
}

// Discard throws the draft away. Discarding one that is not there is not an
// error: it is what a second press of the button does, and what the successful
// end of Connect does after a reload already cleared it.
func (d *DraftStore) Discard() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.mem = nil
	if d.path == "" {
		return nil
	}
	if err := os.Remove(d.path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove saved form %s: %w", d.path, err)
	}
	return nil
}

// writeFilePrivateAtomic writes data to path so that a reader never sees half
// of it and nobody but the owner sees any of it: a temp file in the same
// directory, chmod 0600, fsync, rename. The directory is created 0700.
//
// This is the registry's own write discipline, which the registry has called
// through since a second file (the Connect draft) started needing exactly it.
// The files it serves hold DSN passwords, the same class as shim.yaml and
// dump.key, so a plain os.WriteFile — which can interleave concurrent writers
// and briefly exposes the default mode — is not enough.
func writeFilePrivateAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create directory %s: %w", dir, err)
	}
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+"-*")
	if err != nil {
		return fmt.Errorf("create temp file in %s: %w", dir, err)
	}
	defer os.Remove(tmp.Name()) // no-op after a successful rename
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return fmt.Errorf("chmod temp file: %w", err)
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("write %s: %w", path, err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("sync %s: %w", path, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp file: %w", err)
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return fmt.Errorf("replace %s: %w", path, err)
	}
	return nil
}
