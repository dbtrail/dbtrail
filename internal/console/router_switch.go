package console

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"path/filepath"
	"sync"
	"time"
)

// The router's own switch and password. A router is a MySQL port that
// applications connect to, run as a process of its own; this process never
// binds it. Until this file existed the router took the password of the
// MySQL port inside this process (console-mysql-port.yaml), so the panel
// that turns THAT port on and off was also what opened and closed the
// router: turning the one off, or replacing its password, dropped every
// application connected to the other.
//
// Now the router has a setting of its own, saved here and followed by the
// router from the file: on or off, and a password. This process only writes
// the file. The format is the MySQL port's (FlashbackFile, with no address:
// the router binds its own), so the router reads it with the code it had.

// RouterFileName is the saved setting's file name, beside the servers file.
const RouterFileName = "console-router.yaml"

// RouterSwitchPath is where the setting is saved for an installation whose
// servers file is serversFile ("" = the default one). The console that
// writes it and the router that follows it both ask here, so the two cannot
// come to name different files.
func RouterSwitchPath(serversFile string) string {
	if serversFile == "" {
		serversFile = DefaultRegistryPath()
	}
	return filepath.Join(filepath.Dir(serversFile), RouterFileName)
}

// routerPasswordPrefix marks a password made here. Not the MySQL port's
// prefix: a password found in a log or a ticket says which door it opens.
const routerPasswordPrefix = "brp_"

// newRouterPassword mints a router password: 24 random bytes, hex, behind the
// prefix. Hex keeps it free of every character a shell, a DSN or a JDBC URL
// would need escaped.
func newRouterPassword() (string, error) {
	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate router password: %w", err)
	}
	return routerPasswordPrefix + hex.EncodeToString(b), nil
}

// routerSwitch is the saved setting as this process holds it.
type routerSwitch struct {
	// mutate serializes the handlers that change the setting.
	mutate sync.Mutex
	mu     sync.Mutex
	// path is where the setting is saved; empty on a console that manages
	// no router (serve, and a process that is itself the router).
	path    string
	saved   FlashbackFile
	loadErr string
}

// loadRouterSwitch names the file and reads it for the first time.
func (s *Server) loadRouterSwitch(path string) {
	if path == "" {
		return
	}
	rs := &s.routerSwitch
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.path = path
	s.rereadRouterSwitchLocked()
}

// rereadRouterSwitchLocked reads the file again, before every answer and
// every change: the router follows the file, not this process, so what this
// process last wrote is not what the router is doing once anything else
// touched it (a file copied in or restored, another DBTrail on the same
// directory). A change made from a stale picture would write a new password
// over one that applications are connected with. A file that cannot be read
// leaves the switch unmanageable, with the reason, rather than writing over
// what is there; it is said in the log once per reason. routerSwitch.mu is
// held.
func (s *Server) rereadRouterSwitchLocked() {
	rs := &s.routerSwitch
	f, err := LoadFlashbackFile(rs.path)
	if err != nil {
		if rs.loadErr != err.Error() {
			slog.Warn("console: the router's saved setting could not be read; it cannot be changed from the web interface until it can", "path", rs.path, "error", err)
		}
		rs.loadErr, rs.saved = err.Error(), FlashbackFile{}
		return
	}
	rs.loadErr, rs.saved = "", *f
}

// routerAccessDTO is the router's switch as GET /api/router reports it.
type routerAccessDTO struct {
	// CanManage: this console can turn the router's port on and off and
	// replace its password. Refusal says why not, when not.
	CanManage bool   `json:"can_manage"`
	Refusal   string `json:"refusal,omitempty"`
	// Token: this process was started with an access token. A router that
	// reads the same settings takes that token as its only password and
	// ignores the setting saved here.
	Token bool `json:"token,omitempty"`
	// Enabled: the saved setting says the router's port is on.
	Enabled bool `json:"enabled"`
	// HasPassword: a password made here exists. The password itself is only
	// ever in the response that creates it.
	HasPassword       bool   `json:"has_password,omitempty"`
	PasswordCreatedAt string `json:"password_created_at,omitempty"`
}

// routerAccess reports the switch, or nil on a console that manages none.
func (s *Server) routerAccess() *routerAccessDTO {
	rs := &s.routerSwitch
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if rs.path == "" {
		return nil
	}
	s.rereadRouterSwitchLocked()
	_, refusal := s.routerManageableLocked()
	return &routerAccessDTO{
		CanManage: refusal == "", Refusal: refusal, Token: s.token != "",
		Enabled: rs.saved.Enabled && rs.saved.Password != "", HasPassword: rs.saved.Password != "", PasswordCreatedAt: rs.saved.PasswordCreatedAt,
	}
}

// routerManageableLocked says why this console cannot change the router's
// setting, or returns the saved one. routerSwitch.mu is held.
func (s *Server) routerManageableLocked() (saved FlashbackFile, refusal string) {
	rs := &s.routerSwitch
	switch {
	case rs.path == "":
		return saved, "this console does not manage a router"
	case s.token != "":
		return saved, "DBTrail was started with an access token, and a router that reads the same settings takes that token as its only password: a password made here would open nothing. Remove the token where DBTrail starts to manage the router's password here"
	case rs.saved.readOnly:
		return saved, "the router's settings file was written by a newer bintrail; changes are refused"
	case rs.loadErr != "":
		return saved, "the router's settings file could not be read, so it is not changed: " + rs.loadErr + ". Fix or remove the file."
	}
	return rs.saved, ""
}

// routerRefuseRestricted refuses a session with a data access policy: the
// router forwards to every server's source with that server's forwarding
// account and filters nothing by session, so the password it would hand
// over reads past that session's own redaction. Same posture as the MySQL
// port and the MCP token.
func routerRefuseRestricted(w http.ResponseWriter, r *http.Request) bool {
	if pol := policyFrom(r.Context()); pol.DataRestricted() {
		recordProfileGateDeny(r, "router")
		writeJSONError(w, http.StatusForbidden,
			"the router cannot be changed from a session with a data access policy: it reads every server without your session's redaction. Ask an operator without data restrictions.")
		return true
	}
	return false
}

// routerChangeDTO answers a change to the router's setting: the state as GET
// reports it and, in the one response that created it, the password.
type routerChangeDTO struct {
	routerViewDTO
	Password string `json:"password,omitempty"`
}

// saveRouterSwitch saves next and, when that worked, makes it the setting
// this process holds. routerSwitch.mutate is held.
func (s *Server) saveRouterSwitch(w http.ResponseWriter, next FlashbackFile) bool {
	rs := &s.routerSwitch
	if err := saveFlashbackFile(rs.path, &next); err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, ErrFlashbackFileReadOnly) {
			status = http.StatusConflict
		}
		// The saver's own words name the MySQL port, whose file format this is.
		reason := err.Error()
		if errors.Is(err, ErrFlashbackFileReadOnly) {
			reason = "the router's settings file was written by a newer bintrail; changes are refused"
		}
		writeJSONError(w, status, "the router's setting could not be saved, so nothing changed: "+reason)
		return false
	}
	rs.mu.Lock()
	rs.saved = next
	rs.mu.Unlock()
	return true
}

// handleRouterPut turns the router's port on or off. Turning it on for the
// first time also creates its password, returned once. Nothing is bound
// here: the router follows the file and opens or closes its own port, with
// every connection on it, within a few seconds.
func (s *Server) handleRouterPut(w http.ResponseWriter, r *http.Request) {
	if routerRefuseRestricted(w, r) {
		return
	}
	var req struct {
		Enabled *bool `json:"enabled"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}
	if req.Enabled == nil {
		writeJSONError(w, http.StatusBadRequest, `"enabled" is required: true to turn the router's port on, false to turn it off`)
		return
	}
	rs := &s.routerSwitch
	rs.mutate.Lock()
	defer rs.mutate.Unlock()
	rs.mu.Lock()
	if rs.path != "" {
		s.rereadRouterSwitchLocked()
	}
	saved, refusal := s.routerManageableLocked()
	rs.mu.Unlock()
	if refusal != "" {
		writeJSONError(w, http.StatusConflict, refusal)
		return
	}
	next := saved
	next.Enabled = *req.Enabled
	created := ""
	if next.Enabled && next.Password == "" {
		pw, err := newRouterPassword()
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		created = pw
		next.Password, next.PasswordCreatedAt = pw, time.Now().UTC().Format(time.RFC3339)
	}
	if !s.saveRouterSwitch(w, next) {
		return
	}
	slog.Info("console: the router's port was turned from the web interface", "on", next.Enabled, "password_created", created != "")
	writeJSON(w, http.StatusOK, routerChangeDTO{routerViewDTO: s.routerView(r.Context()), Password: created})
}

// handleRouterPassword replaces the router's password and returns the new
// one, once. The router closes the connections it holds when it sees the
// change: it has no other way to put out someone who holds the old one.
func (s *Server) handleRouterPassword(w http.ResponseWriter, r *http.Request) {
	if routerRefuseRestricted(w, r) {
		return
	}
	rs := &s.routerSwitch
	rs.mutate.Lock()
	defer rs.mutate.Unlock()
	rs.mu.Lock()
	if rs.path != "" {
		s.rereadRouterSwitchLocked()
	}
	saved, refusal := s.routerManageableLocked()
	rs.mu.Unlock()
	if refusal != "" {
		writeJSONError(w, http.StatusConflict, refusal)
		return
	}
	pw, err := newRouterPassword()
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	next := saved
	next.Password, next.PasswordCreatedAt = pw, time.Now().UTC().Format(time.RFC3339)
	if !s.saveRouterSwitch(w, next) {
		return
	}
	slog.Info("console: the router's password was replaced from the web interface")
	writeJSON(w, http.StatusOK, routerChangeDTO{routerViewDTO: s.routerView(r.Context()), Password: pw})
}
