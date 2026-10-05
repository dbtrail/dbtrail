package console

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"time"
)

// flashbackStatusDTO is the wire view of the embedded MySQL-protocol
// time-travel port (#996) for the Connect page (#1446): whether it is on and
// where it listens. The password that authenticates it is the console token,
// which is never serialized; the username rule (a server's registry id or
// name, "default" for the boot entry) is the same selector the /mcp path
// uses, so the frontend derives it from the server list it already holds.
type flashbackStatusDTO struct {
	// Enabled: this process bound the port (Config.FlashbackListen). False on
	// the standalone serve, which owns no such port, and on a watch daemon
	// that did not opt in.
	Enabled bool `json:"enabled"`
	// Listen is the bind address exactly as configured (host:port).
	Listen string `json:"listen,omitempty"`
	// Host and Port are Listen split for the ready-to-copy mysql line. Host
	// is EMPTY when the bind is on every interface (":3308", "0.0.0.0:3308",
	// "[::]:3308"): the daemon cannot know which name the browser reaches it
	// by, so the page fills in the address it was opened on.
	Host string `json:"host,omitempty"`
	Port string `json:"port,omitempty"`
	// Routing is the read router's state on this port (#2038): present
	// whenever the port is on (so the page can say "off" and name the flag),
	// absent when the port itself is off.
	Routing *routingStatusDTO `json:"routing,omitempty"`

	// The rest is for turning the port on from the web interface (#2101).

	// Source is who decided the port's address: "startup" (the flag or the
	// environment variable, which the web interface cannot change) or
	// "saved" (the setting saved here). Empty when the port is off.
	Source string `json:"source,omitempty"`
	// CanManage: this console can turn the port on and off itself. False on
	// serve (it runs no port), when the address was set at startup, and when
	// the saved setting was written by a newer version.
	CanManage bool `json:"can_manage"`
	// SuggestedListen is the address to offer in the form: the one saved
	// last, else one with the web interface's own reach. Only with CanManage.
	SuggestedListen string `json:"suggested_listen,omitempty"`
	// HasPassword: a password generated here exists. The password itself is
	// only ever in the response that creates it.
	HasPassword       bool   `json:"has_password,omitempty"`
	PasswordCreatedAt string `json:"password_created_at,omitempty"`
	// Error is why a port saved as on is not listening (its address was
	// taken, the settings file could not be read).
	Error string `json:"error,omitempty"`
}

// routingStatusDTO: whether MySQL answers on the port, under which rule, and
// who has answered so far.
type routingStatusDTO struct {
	// Enabled: --route-max-copy-age is set. False = the port serves the copy
	// only, and the fields below are omitted.
	Enabled bool `json:"enabled"`
	// MaxCopyAge is the freshness limit as a Go duration ("15m0s").
	MaxCopyAge string `json:"max_copy_age,omitempty"`
	// CostThreshold / ScanRows are the EXPLAIN thresholds; 0 = that rule off.
	CostThreshold float64 `json:"cost_threshold,omitempty"`
	ScanRows      int64   `json:"scan_rows,omitempty"`
	// ReadOnly: the port refuses every statement that is not a read
	// (--route-read-only). False = read-write: writes are sent to MySQL.
	ReadOnly bool `json:"read_only,omitempty"`
	// Since is when the tally started (daemon start), RFC 3339 UTC.
	Since string `json:"since,omitempty"`
	// Servers is the tally per registry id; a server with no decision yet
	// has no entry. Reasons uses the shim's RouteReason* vocabulary.
	Servers map[string]routingServerDTO `json:"servers,omitempty"`
}

// routingServerDTO is one server's decisions since Since.
type routingServerDTO struct {
	Copy  uint64 `json:"copy"`
	MySQL uint64 `json:"mysql"`
	// Refused counts statements nobody ran: the port is read-only and they
	// were not reads. Omitted while zero.
	Refused uint64            `json:"refused,omitempty"`
	Reasons map[string]uint64 `json:"reasons"`
	// Unavailable, when set, is why connections to this server cannot route
	// (the copy answers everything there), as the last connection found it.
	Unavailable string `json:"unavailable,omitempty"`
	// AccountRefused, when set, is what the source answered the last time a
	// connection of the port tried to log in and was turned away: which
	// account, its user name and MySQL's error. Clients of such a
	// connection get error 2006. Cleared when a connection logs in.
	AccountRefused string `json:"account_refused,omitempty"`
}

func (s *Server) routingStatus() *routingStatusDTO {
	rr := s.readRouting
	if rr.MaxCopyAge <= 0 {
		return &routingStatusDTO{}
	}
	return &routingStatusDTO{
		Enabled:       true,
		MaxCopyAge:    rr.MaxCopyAge.String(),
		CostThreshold: rr.CostThreshold,
		ScanRows:      rr.ScanRows,
		ReadOnly:      rr.ReadOnly,
		Since:         s.routing.since.UTC().Format(time.RFC3339),
		Servers:       s.routing.snapshot(),
	}
}

func (s *Server) flashbackStatus() flashbackStatusDTO {
	fb := &s.flashback
	fb.mu.Lock()
	listen, startup := fb.listen, fb.startup
	manageable := !startup && fb.control != nil && fb.path != "" && !fb.saved.readOnly && fb.loadErr == ""
	saved, lastErr := fb.saved, fb.lastErr
	fb.mu.Unlock()

	dto := flashbackStatusDTO{CanManage: manageable, Error: lastErr}
	if !startup {
		dto.HasPassword, dto.PasswordCreatedAt = saved.Password != "", saved.PasswordCreatedAt
	}
	if manageable {
		dto.SuggestedListen = saved.Listen
		if dto.SuggestedListen == "" {
			dto.SuggestedListen = defaultFlashbackListen(s.listen)
		}
	}
	if listen == "" {
		return dto
	}
	dto.Enabled, dto.Listen, dto.Routing = true, listen, s.routingStatus()
	dto.Source = "saved"
	if startup {
		dto.Source = "startup"
	}
	// A value net.Listen would have refused (no port) cannot reach here:
	// a bind failure aborts startup or refuses the save. Report the raw
	// address alone rather than guess a split.
	host, port, err := net.SplitHostPort(listen)
	if err != nil {
		return dto
	}
	dto.Port = port
	switch host {
	case "", "0.0.0.0", "::":
		// Wildcard bind: no single host to name.
	default:
		dto.Host = host
	}
	return dto
}

// handleFlashbackGet reports the time-travel port's state and address, never
// a password that authenticates it. Behind tokenMiddleware like every /api
// route; classified settings:read (it is daemon configuration, not row data).
func (s *Server) handleFlashbackGet(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.flashbackStatus())
}

// flashbackChangeDTO answers a change to the port: its new state and, in the
// one response that created it, the password.
type flashbackChangeDTO struct {
	flashbackStatusDTO
	Password string `json:"password,omitempty"`
}

// flashbackManageable says why this console cannot change the port, or
// returns the pieces a change needs. Callers hold flashback.mutate.
func (s *Server) flashbackManageable() (path string, control FlashbackController, saved FlashbackFile, listen string, refusal string) {
	fb := &s.flashback
	fb.mu.Lock()
	defer fb.mu.Unlock()
	switch {
	case fb.startup:
		return "", nil, saved, "", "the MySQL port's address was set where DBTrail is started, so it is changed there, not here"
	case fb.control == nil || fb.path == "":
		return "", nil, saved, "", "this console does not run the MySQL port"
	case fb.saved.readOnly:
		return "", nil, saved, "", ErrFlashbackFileReadOnly.Error()
	case fb.loadErr != "":
		return "", nil, saved, "", "the MySQL port's settings file could not be read, so it is not changed: " + fb.loadErr + ". Fix or remove the file and restart DBTrail."
	}
	return fb.path, fb.control, fb.saved, fb.listen, ""
}

// flashbackRefuseRestricted refuses a session with a data access policy: the
// port answers with every schema of every server and filters nothing, so the
// password it would hand over reads past that session's own redaction. Same
// posture as the MCP token.
func flashbackRefuseRestricted(w http.ResponseWriter, r *http.Request) bool {
	if pol := policyFrom(r.Context()); pol.DataRestricted() {
		recordProfileGateDeny(r, "flashback")
		writeJSONError(w, http.StatusForbidden,
			"the MySQL port cannot be changed from a session with a data access policy: the port reads every server without your session's redaction. Ask an operator without data restrictions.")
		return true
	}
	return false
}

// handleFlashbackPut turns the port on (at an address) or off. Turning it on
// for the first time also creates its password, returned once.
//
// Order matters on the way up: the port is bound BEFORE the setting is saved,
// so an address that cannot be opened is refused with nothing changed, and a
// setting that cannot be saved closes the port again rather than leaving one
// open that a restart would silently drop.
func (s *Server) handleFlashbackPut(w http.ResponseWriter, r *http.Request) {
	if flashbackRefuseRestricted(w, r) {
		return
	}
	var req struct {
		Enabled *bool  `json:"enabled"`
		Listen  string `json:"listen"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}
	if req.Enabled == nil {
		writeJSONError(w, http.StatusBadRequest, `"enabled" is required: true to turn the port on, false to turn it off`)
		return
	}
	fb := &s.flashback
	fb.mutate.Lock()
	defer fb.mutate.Unlock()
	path, control, saved, was, refusal := s.flashbackManageable()
	if refusal != "" {
		writeJSONError(w, http.StatusConflict, refusal)
		return
	}
	next := saved
	next.Enabled = *req.Enabled

	if !next.Enabled {
		if err := saveFlashbackFile(path, &next); err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		// Closing cannot fail; the address is read back all the same, so the
		// status never names a port on trust.
		_ = control.Apply("")
		fb.mu.Lock()
		fb.saved, fb.listen, fb.lastErr = next, control.Listening(), ""
		fb.mu.Unlock()
		slog.Info("console: MySQL port turned off from the web interface")
		writeJSON(w, http.StatusOK, flashbackChangeDTO{flashbackStatusDTO: s.flashbackStatus()})
		return
	}

	addr := req.Listen
	if addr == "" {
		addr = saved.Listen
	}
	if addr == "" {
		addr = defaultFlashbackListen(s.listen)
	}
	listen, err := NormalizeFlashbackListen(addr, s.listen)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	next.Listen = listen
	created := ""
	if next.Password == "" {
		if created, err = newFlashbackPassword(); err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		next.Password, next.PasswordCreatedAt = created, time.Now().UTC().Format(time.RFC3339)
	}
	// The password is in place before the port accepts its first handshake.
	fb.mu.Lock()
	fb.saved = next
	fb.mu.Unlock()
	// restore puts the saved setting back and takes the port's address from
	// the controller, not from what was asked: a move that was refused can
	// have cost the old address too (same port number, taken in between).
	restore := func() {
		now := control.Listening()
		fb.mu.Lock()
		fb.saved, fb.listen = saved, now
		if now != was {
			fb.lastErr = "the port was on " + was + " and could not be put back there"
		}
		fb.mu.Unlock()
	}
	if err := control.Apply(listen); err != nil {
		restore()
		writeJSONError(w, http.StatusConflict, "the port could not be opened on "+listen+": "+err.Error())
		return
	}
	if err := saveFlashbackFile(path, &next); err != nil {
		// Back to where it was; if that address is gone, closed. Never left
		// open on an address neither the file nor the status names.
		if rerr := control.Apply(was); rerr != nil {
			slog.Warn("console: MySQL port could not be put back after a failed save; it is closed", "listen", was, "error", rerr)
			_ = control.Apply("")
		}
		restore()
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	fb.mu.Lock()
	fb.listen, fb.lastErr = listen, ""
	fb.mu.Unlock()
	slog.Info("console: MySQL port turned on from the web interface", "listen", listen, "password_created", created != "")
	writeJSON(w, http.StatusOK, flashbackChangeDTO{flashbackStatusDTO: s.flashbackStatus(), Password: created})
}

// handleFlashbackPassword replaces the port's password and returns the new
// one, once. Connections already open stay open; the next handshake needs the
// new password.
func (s *Server) handleFlashbackPassword(w http.ResponseWriter, r *http.Request) {
	if flashbackRefuseRestricted(w, r) {
		return
	}
	fb := &s.flashback
	fb.mutate.Lock()
	defer fb.mutate.Unlock()
	path, _, saved, _, refusal := s.flashbackManageable()
	if refusal != "" {
		writeJSONError(w, http.StatusConflict, refusal)
		return
	}
	pw, err := newFlashbackPassword()
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	next := saved
	next.Password, next.PasswordCreatedAt = pw, time.Now().UTC().Format(time.RFC3339)
	if err := saveFlashbackFile(path, &next); err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, ErrFlashbackFileReadOnly) {
			status = http.StatusConflict
		}
		writeJSONError(w, status, err.Error())
		return
	}
	fb.mu.Lock()
	fb.saved = next
	fb.mu.Unlock()
	slog.Info("console: MySQL port password replaced from the web interface")
	writeJSON(w, http.StatusOK, flashbackChangeDTO{flashbackStatusDTO: s.flashbackStatus(), Password: pw})
}
