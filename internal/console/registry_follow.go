package console

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"reflect"
	"time"

	"gopkg.in/yaml.v3"
)

// A process that runs beside the daemon and serves from the daemon's files
// (#2084: the MySQL-protocol port as a service of its own) reads the server
// registry and never writes it. Registry.save marshals the whole file from
// memory, so a second writer would put back what it read at its start over
// whatever the daemon saved since. Such a process loads the registry with
// LoadRegistryFollower and has its Server follow the file (FollowFiles).

// LoadRegistryFollower reads the registry file like LoadRegistry, for a
// process that does not own it: every mutation is refused with
// ErrRegistryReadOnly, the baseline location migration writes nothing, and
// Reload reads the file again.
func LoadRegistryFollower(path string) (*Registry, error) {
	if path == "" {
		return nil, errors.New("a registry that follows a file needs the file's path")
	}
	r, err := LoadRegistry(path)
	if err != nil {
		return nil, err
	}
	r.readOnly, r.follows = true, true
	// What the file held when it was read, to tell a change from none. Read
	// again rather than kept from LoadRegistry: a save between the two reads
	// shows as a change at the first Reload, which costs one reload.
	r.followed, _ = os.ReadFile(path)
	return r, nil
}

// RegistryChange is one server whose entry differs after a Reload. Old is
// the zero entry for a server that was added, New for one that was removed.
type RegistryChange struct {
	Old, New ServerEntry
}

// Added and Removed say which side of the change is missing.
func (c RegistryChange) Added() bool   { return c.Old.ID == "" }
func (c RegistryChange) Removed() bool { return c.New.ID == "" }

// Reload reads the registry file again and returns the servers whose entries
// changed, in no particular order; nil when the file holds what it held.
// Only a registry loaded with LoadRegistryFollower reloads: one that saves
// would lose what it has in memory.
//
// A file that cannot be read or parsed, or that is missing or empty where
// there were servers, changes nothing and is an error: the daemon replaces
// the file in one rename, so none of those is a state it wrote, and dropping
// every server over a disk hiccup would close every client's connection.
func (r *Registry) Reload() ([]RegistryChange, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.follows {
		return nil, errors.New("this server registry is not a follower of its file; it does not reload")
	}
	data, err := os.ReadFile(r.path)
	if err != nil {
		return nil, fmt.Errorf("read server registry %s: %w", r.path, err)
	}
	if bytes.Equal(data, r.followed) {
		return nil, nil
	}
	if len(bytes.TrimSpace(data)) == 0 && len(r.file.Servers) > 0 {
		return nil, fmt.Errorf("server registry %s is empty; the %d servers read before are kept", r.path, len(r.file.Servers))
	}
	next := registryFile{Version: registryVersion}
	if err := yaml.Unmarshal(data, &next); err != nil {
		return nil, fmt.Errorf("parse server registry %s: %w", r.path, err)
	}
	if next.Version == 0 {
		next.Version = registryVersion
	}
	old := make(map[string]ServerEntry, len(r.file.Servers))
	for _, e := range r.file.Servers {
		old[e.ID] = e
	}
	var changes []RegistryChange
	for _, e := range next.Servers {
		was, ok := old[e.ID]
		delete(old, e.ID)
		if !ok || !reflect.DeepEqual(was, e) {
			changes = append(changes, RegistryChange{Old: was, New: e})
		}
	}
	for _, was := range old {
		changes = append(changes, RegistryChange{Old: was})
	}
	r.file, r.followed = next, data
	warnUnusableSSLModes(r.path, r.file.Servers)
	r.syncBucketStores()
	return changes, nil
}

// DefaultFollowInterval is how often FollowFiles looks at the files.
const DefaultFollowInterval = 2 * time.Second

// FollowFiles keeps this server in step with the files another process owns,
// until ctx ends: the server registry (which must come from
// LoadRegistryFollower) and the MySQL port's saved setting, for the password
// the web interface generates. It looks at them at once and then every
// interval (0 means DefaultFollowInterval). A caller that must not serve
// before the first look calls RefreshFollowedFiles itself first.
func (s *Server) FollowFiles(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = DefaultFollowInterval
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		s.RefreshFollowedFiles()
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// RefreshFollowedFiles reads the followed files once and applies what
// changed. A file that cannot be read is said once per reason, and what was
// read before stays in force, except the port's password: see
// refreshFlashbackPassword.
func (s *Server) RefreshFollowedFiles() {
	s.refreshFlashbackPassword()
	changes, err := s.cm.reg.Reload()
	s.noteFollow("registry", "console: the server registry could not be read again; the servers read before stay in force", err)
	for _, c := range changes {
		s.applyRegistryChange(c)
	}
}

// applyRegistryChange does for an entry another process changed what the
// handlers do for one changed here (handleServersUpdate,
// handleServersDelete): connections on the port that forward with an account
// no longer in force are closed, and what was cached for the server is
// dropped or recomputed. A server that was added needs nothing: it is opened
// when a connection first names it.
func (s *Server) applyRegistryChange(c RegistryChange) {
	id := c.Old.ID
	switch {
	case c.Added():
		slog.Info("console: a server was added to the registry", "server", c.New.Name)
		return
	case c.Removed():
		s.cm.evict(id)
		s.sessionProfiles.invalidate(id)
		s.dropRoutedConns(id, "the server was deleted", forwardDSNOf(c.Old), c.Old.SourceSSL())
		slog.Info("console: a server was removed from the registry", "server", c.Old.Name)
		return
	}
	if forwardDSNOf(c.New) != forwardDSNOf(c.Old) {
		s.dropRoutedConns(id, "the account the port forwards with changed", forwardDSNOf(c.Old), c.Old.SourceSSL())
	}
	if c.New.DSN != c.Old.DSN {
		s.cm.evict(id)
		s.sessionProfiles.invalidate(id)
	} else {
		s.cm.rebuildDerived(c.New)
	}
}

// refreshFlashbackPassword reads the port's saved setting again for its
// password. A file that is gone is a password that was removed. One that is
// there and cannot be read or parsed also leaves no password from it: a
// credential this process cannot read is not one it goes on accepting, and
// the token, when there is one, still opens the port.
func (s *Server) refreshFlashbackPassword() {
	fb := &s.flashback
	fb.mu.Lock()
	path, startup := fb.path, fb.startup
	fb.mu.Unlock()
	if path == "" || startup {
		return
	}
	f, err := LoadFlashbackFile(path)
	s.noteFollow("port", "console: the MySQL port's saved setting could not be read; the password created in the web interface is not accepted until it can", err)
	password, createdAt := "", ""
	if err == nil {
		password, createdAt = f.Password, f.PasswordCreatedAt
	}
	fb.mu.Lock()
	fb.saved.Password, fb.saved.PasswordCreatedAt = password, createdAt
	fb.mu.Unlock()
}

// noteFollow logs a followed file's trouble once per change of reason, and
// its recovery once.
func (s *Server) noteFollow(what, msg string, err error) {
	reason := ""
	if err != nil {
		reason = err.Error()
	}
	s.followMu.Lock()
	if s.followNotes == nil {
		s.followNotes = map[string]string{}
	}
	was := s.followNotes[what]
	s.followNotes[what] = reason
	s.followMu.Unlock()
	switch {
	case reason == was:
	case reason != "":
		slog.Warn(msg, "error", err)
	default:
		slog.Info("console: a file that could not be read is read again", "file", what)
	}
}
