package console

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
)

// LocalAccess is what a command run on the daemon's own machine needs to
// reach it: where it answers, and a token it accepts. The daemon writes it to
// a file only its own account can read (0600) each time it starts, with a new
// token, so `bintrail-console server add` works with nothing to configure.
//
// It grants nothing the file's reader did not already have: the same folder
// holds the login file, which that reader can rewrite with `user
// set-password`, and the server registry with every database password. Being
// able to read the daemon's folder IS the credential, as with a MySQL socket.
//
// It is deliberately NOT Config.Token. That one is the operator's own, and
// having it changes what the daemon does (first-run setup closes, /mcp and
// the MySQL port accept it). This one is accepted on /api only, as a Bearer.
// There it is as strong as the operator's token: full access, outside any
// session policy, able to set the first password. The audit trail tells the
// two apart (localCLIActor).
type LocalAccess struct {
	URL   string `json:"url"`
	Token string `json:"token"`
}

// LocalAccessFileName is the file, beside the login file.
const LocalAccessFileName = "console-cli.json"

// LocalAccessPath is where the daemon that keeps its login at authPath writes
// its LocalAccess. An empty authPath is the default login path.
func LocalAccessPath(authPath string) string {
	if authPath == "" {
		authPath = DefaultAuthPath()
	}
	return filepath.Join(filepath.Dir(authPath), LocalAccessFileName)
}

// localAccessURL is the address a command on this machine uses for a daemon
// bound to listen: a wildcard bind is reached on loopback.
func localAccessURL(listen string, tls bool) string {
	scheme := "http"
	if tls {
		scheme = "https"
	}
	return scheme + "://" + displayHost(listen)
}

// newLocalToken makes the token of one run.
func newLocalToken() (string, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("generate local access token: %w", err)
	}
	return hex.EncodeToString(raw), nil
}

// writeLocalAccess writes url and token to path, replacing the file in one
// step.
func writeLocalAccess(path, url, token string) error {
	data, err := json.Marshal(LocalAccess{URL: url, Token: token})
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create %s: %w", dir, err)
	}
	// CreateTemp makes the file 0600, so the token is never readable by
	// another account, not even between the write and the rename.
	tmp, err := os.CreateTemp(dir, ".console-cli-*.tmp")
	if err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(append(data, '\n')); err != nil {
		tmp.Close()
		return fmt.Errorf("write %s: %w", path, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	return nil
}

// publishLocalAccess writes this run's LocalAccess file. Serve calls it once
// the port is open, never before: a second DBTrail started by mistake on the
// same folder dies on the busy port, and had it written the file first, the
// one still running would have lost its commands until its next restart.
// A file that cannot be written is a warning and no local token is accepted.
func (s *Server) publishLocalAccess() {
	if s.localAccessPath == "" {
		return
	}
	if err := writeLocalAccess(s.localAccessPath, localAccessURL(s.listen, s.tlsConf != nil), s.localToken); err != nil {
		slog.Warn("the file that lets commands on this machine reach the web interface could not be written; `bintrail-console server` and the like will need BINTRAIL_CONSOLE_TOKEN",
			"path", s.localAccessPath, "error", err.Error())
		s.localAccessPath = ""
	}
}

// withdrawLocalAccess removes the file when this run ends, so a command says
// "nothing is running" instead of offering a dead token. Only its own file:
// one that holds another token belongs to a DBTrail started since.
func (s *Server) withdrawLocalAccess() {
	if s.localAccessPath == "" {
		return
	}
	if a, err := ReadLocalAccess(s.localAccessPath); err == nil && a.Token == s.localToken {
		_ = os.Remove(s.localAccessPath)
	}
}

// ReadLocalAccess reads the file a running daemon wrote at path. A missing
// file is os.ErrNotExist, wrapped; a file with no address or no token is an
// error, never a half-filled value.
func ReadLocalAccess(path string) (LocalAccess, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return LocalAccess{}, err
	}
	var a LocalAccess
	if err := json.Unmarshal(data, &a); err != nil {
		return LocalAccess{}, fmt.Errorf("%s is not the file DBTrail writes: %w", path, err)
	}
	if a.URL == "" || a.Token == "" {
		return LocalAccess{}, errors.New(path + " has no address or no token; restarting DBTrail writes it again")
	}
	return a, nil
}

// ServerName is the name of the registry server with this id, for a log line
// a person reads. An id the registry does not hold (the server DBTrail was
// started with, or one removed since) is returned as it is.
func (s *Server) ServerName(id string) string {
	if e, ok := s.cm.reg.Get(id); ok && e.Name != "" {
		return e.Name
	}
	return id
}
