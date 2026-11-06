package console

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"
)

// A server's application login on the MySQL-protocol port: a username and a
// password that open the port for that server alone.
//
// It exists for a proxy in front of the application, ProxySQL first among
// them. A proxy logs into every backend with the username and password the
// application logged into it with (ProxySQL separates the two only for LDAP
// and AWS IAM logins), so with the port's shared password the application's
// password on its MySQL would have to change. With a login saved here, the
// proxy reaches the port with the credentials the application already uses,
// and the port answers for this server only.

// CopyOnlySuffix, after a server's id or name in the username a client
// connects as, asks for the copy alone (#2341): "shop@copy". A login may not
// end in it, or "<login>@copy" would mean two things.
const CopyOnlySuffix = "@copy"

// maxPortUserLen is MySQL's limit on a username (8.0 and later), which
// ProxySQL shares. A longer one could never be sent by a client.
const maxPortUserLen = 32

// maxPortPasswordLen bounds what is kept; it is far above any real password.
const maxPortPasswordLen = 512

// errPortLoginInvalid marks a login refused for its own shape (the value
// sent), which the API answers with 400.
var errPortLoginInvalid = errors.New("invalid application login")

// validatePortLoginShape checks the parts of a login that do not depend on
// the other servers.
func validatePortLoginShape(user, password string) error {
	if err := portLoginShape(user, password); err != nil {
		if errors.Is(err, ErrPortLoginConflict) {
			return err
		}
		return fmt.Errorf("%w: %w", errPortLoginInvalid, err)
	}
	return nil
}

func portLoginShape(user, password string) error {
	switch {
	case user == "":
		return errors.New("the login's username is required")
	case password == "":
		return errors.New("the login's password is required")
	case !utf8.ValidString(user):
		return errors.New("the login's username is not valid UTF-8")
	case strings.TrimSpace(user) != user:
		return errors.New("the login's username starts or ends with a space: a client sends it as typed, so it would never match")
	case utf8.RuneCountInString(user) > maxPortUserLen:
		return fmt.Errorf("the login's username is longer than %d characters, the most MySQL and ProxySQL accept", maxPortUserLen)
	case len(password) > maxPortPasswordLen:
		return fmt.Errorf("the login's password is longer than %d bytes", maxPortPasswordLen)
	case strings.ContainsRune(password, 0):
		return errors.New("the login's password holds a NUL byte, which no MySQL client can send")
	}
	for _, r := range user {
		if unicode.IsControl(r) {
			return errors.New("the login's username holds a control character")
		}
	}
	if strings.EqualFold(user, bootServerID) {
		return fmt.Errorf("%w: %q names the command-line server", ErrPortLoginConflict, bootServerID)
	}
	if len(user) >= len(CopyOnlySuffix) && strings.EqualFold(user[len(user)-len(CopyOnlySuffix):], CopyOnlySuffix) {
		return fmt.Errorf("the login's username ends in %q, which asks for the copy alone", CopyOnlySuffix)
	}
	return nil
}

// portLoginConflict reports what makes user, as entry id's login, the same
// username as something else on the port: another server's id or name, or
// another server's login. Case is ignored on purpose, though the port
// matches exactly: two usernames that differ only in case are a trap.
// Entry id's own name and id are allowed: either one selects the same server.
func portLoginConflict(entries []ServerEntry, id, user string) error {
	for _, e := range entries {
		if e.ID == id {
			continue
		}
		switch {
		case strings.EqualFold(e.ID, user):
			return fmt.Errorf("%w: %q is the id of server %q", ErrPortLoginConflict, user, e.Name)
		case strings.EqualFold(e.Name, user):
			return fmt.Errorf("%w: %q is the name of another server", ErrPortLoginConflict, user)
		case e.PortUser != "" && strings.EqualFold(e.PortUser, user):
			return fmt.Errorf("%w: %q is the application login of server %q", ErrPortLoginConflict, user, e.Name)
		}
	}
	return nil
}

// SetPortLogin saves entry id's application login. It is the only writer of
// ServerEntry.PortUser and PortPassword: an empty user removes the login (and
// the password must be empty too). It changes nothing else in the entry.
func (r *Registry) SetPortLogin(id, user, password string) (ServerEntry, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.readOnly {
		return ServerEntry{}, ErrRegistryReadOnly
	}
	if user != "" {
		if err := validatePortLoginShape(user, password); err != nil {
			return ServerEntry{}, err
		}
		if err := portLoginConflict(r.file.Servers, id, user); err != nil {
			return ServerEntry{}, err
		}
	} else if password != "" {
		return ServerEntry{}, fmt.Errorf("%w: a password was sent with no username; name the login, or send neither to remove it", errPortLoginInvalid)
	}
	for i, old := range r.file.Servers {
		if old.ID != id {
			continue
		}
		r.file.Servers[i].PortUser, r.file.Servers[i].PortPassword = user, password
		if err := r.save(); err != nil {
			r.file.Servers[i] = old // roll back
			return ServerEntry{}, err
		}
		return r.file.Servers[i], nil
	}
	return ServerEntry{}, ErrUnknownServer
}

// portLoginMatch is what a username means as an application login on the
// port: the entry whose login it is, exactly as typed (MySQL compares
// usernames by case), and whether it is ambiguous, which the port refuses
// rather than pick one. A saved registry cannot hold an ambiguous login
// (SetPortLogin and checkName refuse it), but one edited by hand can.
func portLoginMatch(entries []ServerEntry, user string) (id string, ok, ambiguous bool) {
	if user == "" {
		return "", false, false
	}
	for _, e := range entries {
		if e.PortUser == "" || e.PortUser != user {
			continue
		}
		if ok {
			return "", false, true // two logins with one username
		}
		id, ok = e.ID, true
	}
	if !ok {
		return "", false, false
	}
	for _, e := range entries {
		if e.ID != id && (e.ID == user || e.Name == user) {
			return "", false, true // the username also selects another server
		}
	}
	return id, true, false
}

// ErrAmbiguousPortLogin: the username is the application login of a server
// and also selects another one (a registry edited by hand). The port refuses
// it rather than choose.
var ErrAmbiguousPortLogin = errors.New("this username is the application login of one server and also names another; fix the server registry so it names one")

// portLoginTarget resolves selector as an application login: the server id
// it opens, ok, and ErrAmbiguousPortLogin when it cannot be told apart.
func (s *Server) portLoginTarget(selector string) (string, bool, error) {
	id, ok, ambiguous := portLoginMatch(s.cm.reg.List(), selector)
	if ambiguous {
		return "", false, ErrAmbiguousPortLogin
	}
	return id, ok, nil
}

// FlashbackPasswordsFor is the passwords that authenticate user on the
// MySQL-protocol port: the port's own (FlashbackPasswords) and, when user is
// a server's application login, that login's password. A login password is
// accepted under its own username only, and only while the port is open: a
// port with no password of its own is closed, and a login does not open it.
// Read per handshake, so a login saved, changed or removed in the web
// interface is the one the next client needs.
func (s *Server) FlashbackPasswordsFor(user string) []string {
	out := s.FlashbackPasswords()
	if len(out) == 0 || s.loginsUnreadable.Load() {
		return out
	}
	// One reading of the registry for both the match and the password, so
	// a login renamed between two readings cannot offer its new password
	// under its old username.
	entries := s.cm.reg.List()
	id, ok, ambiguous := portLoginMatch(entries, user)
	if !ok || ambiguous {
		return out
	}
	for _, e := range entries {
		if e.ID == id && e.PortPassword != "" {
			out = append(out, e.PortPassword)
		}
	}
	return out
}

// setLoginsUnreadable records whether the registry file this process follows
// could be read the last time it looked (RefreshFollowedFiles). While it
// cannot, no application login opens the port: what this process still holds
// may be a login removed since, and a credential this process cannot read is
// not one it goes on accepting (the port's own password is handled the same
// way, refreshFlashbackPassword). The connections opened with a login are
// closed when that starts. The port's own password and the token still work.
func (s *Server) setLoginsUnreadable(unreadable bool) {
	if s.loginsUnreadable.Swap(unreadable) == unreadable || !unreadable {
		return
	}
	if n := s.loginConns.dropAll(); n > 0 {
		slog.Warn("console: closed the MySQL-protocol port's connections opened with application logins: the server registry cannot be read, so no login is accepted until it can",
			"connections", n)
	}
}

// loginConnSet holds the port's open connections opened with an
// application login, per server, and an epoch that every login change moves
// on. A connection reads the epoch before its handshake checks its password
// (LoginEpoch) and is tracked only while the epoch is still that one: a
// change made in between, to any server's login, refuses it, and a change
// made after it was tracked closes it. One epoch for every server keeps that
// exact without knowing beforehand which server a username will open. The
// zero value is ready to use (a Server built without New in a test has one).
type loginConnSet struct {
	mu    sync.Mutex
	epoch uint64
	next  uint64
	conns map[string]map[uint64]func()
}

func (l *loginConnSet) current() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.epoch
}

func (l *loginConnSet) track(serverID string, epoch uint64, drop func()) (func(), bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.epoch != epoch {
		return func() {}, false
	}
	l.next++
	n := l.next
	if l.conns == nil {
		l.conns = map[string]map[uint64]func(){}
	}
	if l.conns[serverID] == nil {
		l.conns[serverID] = map[uint64]func(){}
	}
	l.conns[serverID][n] = drop
	return func() {
		l.mu.Lock()
		delete(l.conns[serverID], n)
		l.mu.Unlock()
	}, true
}

// dropAll moves the epoch on and closes every tracked connection.
func (l *loginConnSet) dropAll() int {
	l.mu.Lock()
	l.epoch++
	all := l.conns
	l.conns = nil
	l.mu.Unlock()
	n := 0
	for _, conns := range all {
		for _, d := range conns {
			d()
			n++
		}
	}
	return n
}

// drop moves the epoch on and closes serverID's tracked connections.
func (l *loginConnSet) drop(serverID string) int {
	l.mu.Lock()
	l.epoch++
	dropped := l.conns[serverID]
	delete(l.conns, serverID)
	l.mu.Unlock()
	for _, d := range dropped {
		d()
	}
	return len(dropped)
}

// LoginEpoch is the epoch of the port's application logins. Read it BEFORE
// a connection's handshake and hand it to TrackLoginConn: the handshake
// checks the password against the logins as they are then, so any login
// change after that moment, including a login moved to another server
// between the password check and the binding, refuses the connection
// instead of serving it a server its password was never checked for.
func (s *Server) LoginEpoch() uint64 { return s.loginConns.current() }

// TrackLoginConn registers a client connection of the port opened with
// server serverID's application login, so it is closed when that login is
// changed or removed (dropLoginConns): a credential that no longer opens the
// port must not go on reading through a connection it opened. epoch is
// LoginEpoch read before the connection's handshake; ok is false when a
// login changed since, and the connection must not be served.
func (s *Server) TrackLoginConn(serverID string, epoch uint64, drop func()) (untrack func(), ok bool) {
	return s.loginConns.track(serverID, epoch, drop)
}

// dropLoginConns closes the port's open connections opened with server
// serverID's application login.
func (s *Server) dropLoginConns(serverID, why string) {
	if n := s.loginConns.drop(serverID); n > 0 {
		slog.Info("console: closed the MySQL-protocol port's connections opened with this server's application login",
			"server", serverID, "connections", n, "reason", why)
	}
}

// portLoginDTO is the answer of PUT and DELETE /api/servers/{id}/port-login.
// The password is never sent back.
type portLoginDTO struct {
	PortUser        string `json:"port_user"`
	HasPortPassword bool   `json:"has_port_password"`
}

const portLoginBootRefusal = "the command-line server cannot have an application login; " +
	"add the server from the web interface (+ Add server) to give it one"

// handlePortLoginPut serves PUT /api/servers/{id}/port-login: {"user": ...,
// "password": ...} saves the server's application login on the MySQL port,
// replacing the one saved before.
func (s *Server) handlePortLoginPut(w http.ResponseWriter, r *http.Request) {
	s.savePortLogin(w, r, true)
}

// handlePortLoginDelete serves DELETE /api/servers/{id}/port-login.
func (s *Server) handlePortLoginDelete(w http.ResponseWriter, r *http.Request) {
	s.savePortLogin(w, r, false)
}

func (s *Server) savePortLogin(w http.ResponseWriter, r *http.Request, put bool) {
	if flashbackRefuseRestricted(w, r) {
		return
	}
	id := r.PathValue("id")
	if id == bootServerID {
		writeJSONError(w, http.StatusConflict, portLoginBootRefusal)
		return
	}
	old, ok := s.cm.reg.Get(id)
	if !ok {
		writeJSONError(w, http.StatusNotFound, ErrUnknownServer.Error())
		return
	}
	var user, password string
	if put {
		var body struct {
			User     *string `json:"user"`
			Password *string `json:"password"`
		}
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&body); err != nil {
			writeBodyDecodeError(w, err)
			return
		}
		if dec.More() {
			writeJSONError(w, http.StatusBadRequest, "the body holds more than one JSON value")
			return
		}
		// Both are required on a PUT: an empty username here would remove
		// the login, which is what DELETE is for.
		if body.User == nil || body.Password == nil || *body.User == "" || *body.Password == "" {
			writeJSONError(w, http.StatusBadRequest, `send "user" and "password", both not empty; DELETE removes the login`)
			return
		}
		user, password = *body.User, *body.Password
	}
	e, err := s.cm.reg.SetPortLogin(id, user, password)
	if err != nil {
		status := registryErrStatus(err)
		switch {
		case errors.Is(err, ErrPortLoginConflict):
			status = http.StatusConflict
		case errors.Is(err, errPortLoginInvalid):
			status = http.StatusBadRequest
		}
		writeJSONError(w, status, err.Error())
		return
	}
	if old.PortUser != e.PortUser || old.PortPassword != e.PortPassword {
		// A connection opened with the login before keeps reading through
		// what that login opened: close them, so a removed or changed login
		// revokes at once. This process serves the port in some setups; the
		// router, which serves it in the others, does the same when it reads
		// the registry again (applyRegistryChange).
		s.dropLoginConns(id, "the server's application login changed")
	}
	if put {
		slog.Info("console: application login saved for the MySQL port", "actor", consoleActor(r), "server", e.Name, "id", id, "user", e.PortUser)
	} else {
		slog.Info("console: application login removed from the MySQL port", "actor", consoleActor(r), "server", e.Name, "id", id)
	}
	writeJSON(w, http.StatusOK, portLoginDTO{PortUser: e.PortUser, HasPortPassword: e.PortPassword != ""})
}
