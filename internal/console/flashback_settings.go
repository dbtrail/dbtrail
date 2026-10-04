package console

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"go.yaml.in/yaml/v2"
)

// The MySQL-protocol port can be turned on from the web interface (#2101),
// not only with --flashback-listen. This file is what that needs: the saved
// setting, the password a MySQL client connects with, and the server's live
// view of both.

// flashbackFileVersion is the saved-setting schema this binary writes and
// fully understands. A file with a HIGHER version is honoured as far as its
// known fields go and refuses changes: same contract as the registry and the
// managed MCP token.
const flashbackFileVersion = 1

// FlashbackFileName is the saved setting's file name. It lives beside the
// servers file, which every working installation already keeps somewhere
// that survives a restart.
const FlashbackFileName = "console-mysql-port.yaml"

// flashbackPasswordPrefix makes a port password recognizable in a client's
// configuration without revealing anything: the shape is not the secret.
const flashbackPasswordPrefix = "bfp_"

// defaultFlashbackPort is the port offered when none was saved. 3308 is the
// standalone shim's.
const defaultFlashbackPort = "3309"

// ErrFlashbackFileReadOnly is returned by write paths when the file on disk
// was written by a newer bintrail.
var ErrFlashbackFileReadOnly = errors.New("the MySQL port settings file was written by a newer bintrail; changes are refused")

// FlashbackFile is the saved setting. Password is stored AS TYPED BY THE
// CLIENT, not hashed: MySQL-protocol authentication derives its challenge
// response from the raw password (go-mysql hashes Credential.Passwords on
// demand), so a digest could not authenticate anyone. That makes this file a
// credential, like shim.yaml's mysql_password: 0600, in a 0700 directory, and
// never serialized to the API after the response that creates it.
type FlashbackFile struct {
	Version           int    `yaml:"version"`
	Enabled           bool   `yaml:"enabled"`
	Listen            string `yaml:"listen,omitempty"`
	Password          string `yaml:"password,omitempty"`
	PasswordCreatedAt string `yaml:"password_created_at,omitempty"`
	// Extra preserves unknown top-level fields a newer binary wrote, across
	// this binary's load and save.
	Extra map[string]any `yaml:",inline"`

	readOnly bool
}

// ReadOnly reports a file written by a newer bintrail.
func (f *FlashbackFile) ReadOnly() bool { return f != nil && f.readOnly }

// LoadFlashbackFile reads the saved setting. A missing file is the port never
// having been set up here: an empty setting and no error.
func LoadFlashbackFile(path string) (*FlashbackFile, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return &FlashbackFile{Version: flashbackFileVersion}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read MySQL port settings %s: %w", path, err)
	}
	var f FlashbackFile
	if err := yaml.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("parse MySQL port settings %s: %w", path, err)
	}
	if f.Version > flashbackFileVersion {
		f.readOnly = true
	}
	return &f, nil
}

// saveFlashbackFile writes atomically: marshal, temp file in the same
// directory, fsync, rename. File 0600, directory 0700: it holds a credential.
func saveFlashbackFile(path string, f *FlashbackFile) error {
	if f.readOnly {
		return fmt.Errorf("%s: %w", path, ErrFlashbackFileReadOnly)
	}
	f.Version = flashbackFileVersion
	data, err := yaml.Marshal(f)
	if err != nil {
		return fmt.Errorf("marshal MySQL port settings %s: %w", path, err)
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create MySQL port settings directory %s: %w", dir, err)
	}
	tmp, err := os.CreateTemp(dir, ".console-mysql-port-*.yaml")
	if err != nil {
		return fmt.Errorf("create temp MySQL port settings file in %s: %w", dir, err)
	}
	defer os.Remove(tmp.Name()) // no-op after a successful rename
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return fmt.Errorf("chmod temp MySQL port settings file for %s: %w", path, err)
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("write MySQL port settings %s: %w", path, err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("sync MySQL port settings %s: %w", path, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp MySQL port settings file for %s: %w", path, err)
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return fmt.Errorf("replace MySQL port settings %s: %w", path, err)
	}
	return nil
}

// newFlashbackPassword mints a port password: 24 random bytes, hex, behind
// the recognizable prefix. Hex keeps it free of every character a shell, a
// DSN or a JDBC URL would need escaped.
func newFlashbackPassword() (string, error) {
	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate MySQL port password: %w", err)
	}
	return flashbackPasswordPrefix + hex.EncodeToString(b), nil
}

// NormalizeFlashbackListen validates an address typed in the web interface
// and returns it trimmed. It must be host:port with a real port; the host may
// be empty (every interface). consoleListen is the web interface's own
// address: the two cannot share a port.
func NormalizeFlashbackListen(addr, consoleListen string) (string, error) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return "", errors.New("the address is empty; use host:port, for example 127.0.0.1:" + defaultFlashbackPort)
	}
	if strings.ContainsAny(addr, " \t\r\n/") {
		return "", fmt.Errorf("%q is not an address; use host:port, for example 127.0.0.1:%s", addr, defaultFlashbackPort)
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", fmt.Errorf("%q is not an address; use host:port, for example 127.0.0.1:%s", addr, defaultFlashbackPort)
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 1 || n > 65535 {
		return "", fmt.Errorf("%q is not a port; use a number from 1 to 65535", port)
	}
	if _, cport, err := net.SplitHostPort(consoleListen); err == nil && cport == strconv.Itoa(n) {
		return "", fmt.Errorf("port %d is the web interface's own port; choose another", n)
	}
	return net.JoinHostPort(host, strconv.Itoa(n)), nil
}

// defaultFlashbackListen is the address offered when none was saved: the same
// reach as the web interface, on the port's own number. A web interface that
// only this machine can open gets a port only this machine can open; one that
// listens on every interface (the container case, where the published port
// decides who gets in) gets the same.
func defaultFlashbackListen(consoleListen string) string {
	host, _, err := net.SplitHostPort(consoleListen)
	if err != nil {
		host = "127.0.0.1"
	}
	switch strings.ToLower(host) {
	case "localhost":
		host = "127.0.0.1"
	case "", "::":
		host = "0.0.0.0"
	}
	return net.JoinHostPort(host, defaultFlashbackPort)
}

// FlashbackController opens and closes the MySQL-protocol port while the
// daemon runs. The watch command supplies it; a console without one (serve)
// has no port to manage.
type FlashbackController interface {
	// Apply makes the port listen on listen, or closes it when listen is
	// empty. On an error the port is left as it was.
	Apply(listen string) error
}

// flashbackState is the server's live view of the port.
type flashbackState struct {
	// mutate serializes the handlers that change the port. It is NOT held
	// by readers, and mu is never held across control.Apply: closing the
	// port waits for its connections, whose handshakes read the passwords
	// under mu.
	mutate sync.Mutex

	mu sync.Mutex
	// startup: the address came from where DBTrail is started
	// (Config.FlashbackListen). It decides, and the saved setting is not
	// consulted.
	startup bool
	// listen is the address the port is bound on now; empty = off.
	listen string
	// path is the saved setting's file; empty = this console keeps none.
	path    string
	control FlashbackController
	saved   FlashbackFile
	// lastErr is why a port saved as on is not up.
	lastErr string
}

// FlashbackPasswords returns every password that authenticates the MySQL
// port: the static token, and the password generated in the web interface
// unless the port's address was set at startup (whoever starts it that way
// chose the token). Never an empty string: an empty entry would authorise a
// passwordless handshake.
func (s *Server) FlashbackPasswords() []string {
	var out []string
	if s.token != "" {
		out = append(out, s.token)
	}
	fb := &s.flashback
	fb.mu.Lock()
	defer fb.mu.Unlock()
	if !fb.startup && fb.saved.Password != "" {
		out = append(out, fb.saved.Password)
	}
	return out
}

// ManageFlashback hands the server the port's controller and brings the port
// up if it was saved as on. It never fails: this process also captures, and a
// saved address that cannot be bound (taken by something else since) must not
// keep it from starting. The reason is kept for the web interface instead.
func (s *Server) ManageFlashback(control FlashbackController) {
	fb := &s.flashback
	fb.mutate.Lock()
	defer fb.mutate.Unlock()

	fb.mu.Lock()
	fb.control = control
	path, startup := fb.path, fb.startup
	fb.mu.Unlock()
	if startup || path == "" || control == nil {
		return
	}
	f, err := LoadFlashbackFile(path)
	if err != nil {
		s.setFlashbackError(err.Error())
		return
	}
	fb.mu.Lock()
	fb.saved = *f
	fb.mu.Unlock()
	if !f.Enabled || f.Password == "" || f.Listen == "" {
		return
	}
	if err := control.Apply(f.Listen); err != nil {
		s.setFlashbackError(err.Error())
		return
	}
	fb.mu.Lock()
	fb.listen = f.Listen
	fb.mu.Unlock()
}

func (s *Server) setFlashbackError(msg string) {
	s.flashback.mu.Lock()
	s.flashback.lastErr = msg
	s.flashback.mu.Unlock()
}
