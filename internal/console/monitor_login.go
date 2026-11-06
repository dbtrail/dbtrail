package console

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
)

// The monitor login of the MySQL-protocol port: the username and password a
// proxy's health checks log in with.
//
// ProxySQL checks every backend with ONE monitor username and password
// (mysql-monitor_username/_password), MySQL included, so the port has to
// accept the pair the operator already gave MySQL's monitor account; a
// password generated here could not be it. Without a monitor login every
// check of the port is refused with 1045 and both sides log it, every few
// seconds.
//
// The login opens no server. A connection opened with it answers what
// ProxySQL 3.1.12's monitor sends (lib/MySQL_Monitor.cpp): COM_PING, the
// `SET wait_timeout=N` sent on each new monitor connection, and the
// read_only check of a server in mysql_replication_hostgroups, which is
// answered 1 (the port is a reader; 0 would move it to the writer hostgroup).
// Everything else is refused.

// MonitorLoginKey is the loginServer of a connection opened with the monitor
// login: what flashbackCreds.credentialWithLogin returns for it, in the slot a
// server's id takes for an application login. No
// server id can be it: ids are hex. Monitor connections are tracked under it
// (TrackLoginConn), so a changed or removed monitor login closes them.
const MonitorLoginKey = "\x00monitor"

// ErrAmbiguousMonitorLogin: the monitor login's username also names a server
// or is a server's application login (a file edited by hand). The port
// refuses it rather than choose.
var ErrAmbiguousMonitorLogin = errors.New("the MySQL port's monitor login has the username of a server or of an application login; change one of them")

// monitorUser is the saved monitor login's username ("" when none). It is
// the Registry's reservedUser: no server may be named it.
func (s *Server) monitorUser() string {
	fb := &s.flashback
	fb.mu.Lock()
	defer fb.mu.Unlock()
	if fb.startup || fb.saved.MonitorPassword == "" {
		return ""
	}
	return fb.saved.MonitorUser
}

// MonitorPasswordFor is the monitor login's password when user is its
// username, exactly as typed (MySQL compares usernames by case). ok is false
// when user is not the monitor login, and while the port is closed: the
// monitor login never opens a closed port. err is ErrAmbiguousMonitorLogin
// when the saved login cannot be used (monitorProblem: a username that also
// selects a server, or a shape the API refuses); the caller refuses it.
// Read per handshake, like the port's own password.
func (s *Server) MonitorPasswordFor(user string) (password string, ok bool, err error) {
	if user == "" || len(s.FlashbackPasswords()) == 0 {
		return "", false, nil
	}
	fb := &s.flashback
	fb.mu.Lock()
	mu, mp, startup := fb.saved.MonitorUser, fb.saved.MonitorPassword, fb.startup
	fb.mu.Unlock()
	if startup || mp == "" || mu != user {
		return "", false, nil
	}
	if why := s.monitorProblem(mu, mp); why != "" {
		s.warnMonitorProblem(mu, why)
		return "", false, ErrAmbiguousMonitorLogin
	}
	return mp, true, nil
}

// monitorProblem is why a saved monitor login cannot be used ("" when it
// can): a username that also selects a server, or one the API would have
// refused for its shape ("default", "@copy", spaces). Only a hand edit (of
// the port's settings file or of the server registry) gets here.
func (s *Server) monitorProblem(user, password string) string {
	if err := portLoginShape(user, password); err != nil {
		return err.Error()
	}
	if s.cm != nil && s.cm.reg != nil {
		for _, e := range s.cm.reg.List() {
			switch {
			case e.ID == user:
				return fmt.Sprintf("it is the id of server %q", e.Name)
			case e.Name == user:
				return fmt.Sprintf("it is the name of server %q", e.Name)
			case e.PortUser == user:
				return fmt.Sprintf("it is the application login of server %q", e.Name)
			}
		}
	}
	return ""
}

// warnMonitorProblem says, once per username and reason, why the monitor
// login is refused: the client reads the same 1045 as for a wrong password
// (telling it more would say which usernames exist), so the operator learns
// it here and on GET /api/flashback (monitor_error).
func (s *Server) warnMonitorProblem(user, why string) {
	key := user + "\x00" + why
	w := &s.monitorWarned
	w.mu.Lock()
	said := w.last == key
	w.last = key
	w.mu.Unlock()
	if said {
		return
	}
	slog.Warn("console: the MySQL port's monitor login is refused, and so is every login under its username, until this is fixed: "+why,
		"monitor_user", user)
}

// publishSaved makes next the port's saved setting in this process, and
// closes the monitor login's connections when it changed the monitor login:
// whichever change wrote it (this one, or a hand edit the change read back).
func (s *Server) publishSaved(next FlashbackFile) {
	fb := &s.flashback
	fb.mu.Lock()
	old := fb.saved
	fb.saved = next
	fb.mu.Unlock()
	s.noteMonitorLoginChange(old.MonitorUser, old.MonitorPassword, next.MonitorUser, next.MonitorPassword)
}

// monitorLoginConflict reports what makes user the same username as
// something else on the port: a server's id or name, or its application
// login. Case is ignored, as for application logins.
func monitorLoginConflict(entries []ServerEntry, user string) error {
	for _, e := range entries {
		switch {
		case strings.EqualFold(e.ID, user):
			return fmt.Errorf("%w: %q is the id of server %q", ErrPortLoginConflict, user, e.Name)
		case strings.EqualFold(e.Name, user):
			return fmt.Errorf("%w: %q is the name of a server", ErrPortLoginConflict, user)
		case e.PortUser != "" && strings.EqualFold(e.PortUser, user):
			return fmt.Errorf("%w: %q is the application login of server %q", ErrPortLoginConflict, user, e.Name)
		}
	}
	return nil
}

// noteMonitorLoginChange closes the connections opened with the monitor
// login when the saved one was replaced or removed: a credential that no
// longer opens the port does not go on answering through a connection it
// opened. Called outside fb.mu.
func (s *Server) noteMonitorLoginChange(oldUser, oldPassword, newUser, newPassword string) {
	if oldPassword == "" || (oldUser == newUser && oldPassword == newPassword) {
		return
	}
	if n := s.loginConns.drop(MonitorLoginKey); n > 0 {
		slog.Info("console: the MySQL port's monitor login was changed or removed; the connections opened with it are closed", "connections", n)
	}
}

// withRegistryLocked runs f under the registry's lock, or with no entries
// when this console has no registry.
func (s *Server) withRegistryLocked(f func(entries []ServerEntry) error) error {
	if s.cm == nil || s.cm.reg == nil {
		return f(nil)
	}
	return s.cm.reg.underLock(f)
}

type monitorLoginRequest struct {
	User     *string `json:"user"`
	Password *string `json:"password"`
}

// handleFlashbackMonitorLogin saves (PUT) or removes (DELETE) the monitor
// login in the port's settings file.
func (s *Server) handleFlashbackMonitorLogin(w http.ResponseWriter, r *http.Request) {
	if flashbackRefuseRestricted(w, r) {
		return
	}
	user, password := "", ""
	if r.Method == http.MethodPut {
		var req monitorLoginRequest
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&req); err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
			return
		}
		if req.User != nil {
			user = *req.User
		}
		if req.Password != nil {
			password = *req.Password
		}
		if user == "" && password != "" {
			writeJSONError(w, http.StatusBadRequest, "a password was sent with no username; name the monitor login, or use DELETE to remove it")
			return
		}
		if err := validatePortLoginShape(user, password); err != nil {
			status := http.StatusBadRequest
			if errors.Is(err, ErrPortLoginConflict) {
				status = http.StatusConflict
			}
			writeJSONError(w, status, strings.Replace(err.Error(), "the login's", "the monitor login's", 1))
			return
		}
	}
	fb := &s.flashback
	fb.mutate.Lock()
	defer fb.mutate.Unlock()
	path, _, saved, _, refusal := s.flashbackManageable()
	if refusal != "" {
		writeJSONError(w, http.StatusConflict, refusal)
		return
	}
	next := saved
	next.MonitorUser, next.MonitorPassword = user, password
	// The conflict check, the save and the reservation the registry reads
	// (monitorUser) under the registry's lock: a server added or a login
	// saved meanwhile is checked against the new reservation, or this
	// against it. Connections are closed after it is released: closing
	// waits on handshakes, which read the registry.
	var old FlashbackFile
	err := s.withRegistryLocked(func(entries []ServerEntry) error {
		if user != "" {
			if err := monitorLoginConflict(entries, user); err != nil {
				return err
			}
		}
		if err := saveFlashbackFile(path, &next); err != nil {
			return err
		}
		fb.mu.Lock()
		old = fb.saved
		fb.saved = next
		fb.mu.Unlock()
		return nil
	})
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, ErrFlashbackFileReadOnly) || errors.Is(err, ErrPortLoginConflict) {
			status = http.StatusConflict
		}
		writeJSONError(w, status, err.Error())
		return
	}
	s.noteMonitorLoginChange(old.MonitorUser, old.MonitorPassword, user, password)
	if user == "" {
		slog.Info("console: MySQL port monitor login removed from the web interface")
	} else {
		slog.Info("console: MySQL port monitor login saved from the web interface", "user", user)
	}
	writeJSON(w, http.StatusOK, s.flashbackStatus())
}
