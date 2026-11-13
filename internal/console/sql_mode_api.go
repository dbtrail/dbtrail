package console

import (
	"encoding/json"
	"log/slog"
	"net/http"
)

// sqlModeDTO is PUT /api/servers/{id}/sql-mode (#2546): whether the server's
// statements on the SQL port read its newest captured changes over its
// snapshot (fresh) or the snapshot alone (fast). It is what is SAVED: the
// router applies it when it reads the registry again, and says what it
// applies in its status document (sql_mode per server). A server with
// nothing saved is fresh. BINTRAIL_SQL_TAILS=on or =off where the router
// starts overrides every server's.
//
// like_ignores_case is the server's LIKE switch: on, statements with LIKE go
// to the copy, written ILIKE there (MySQL compares LIKE without case under
// its default collations, the copy with it); off, they stay on MySQL. The
// port reads it when a connection opens. Each field is saved only when it
// is sent, so one choice never resets the other.
//
// max_behind_source_seconds is the server's lag limit (#2777): how far
// behind the source, in seconds, the data of a statement on the copy may be
// for the copy to answer it. 0 (the default) is no limit. Past it a routed
// statement goes to MySQL and one on a connection that does not route is
// refused. Read per statement, so it applies to open connections too.
type sqlModeDTO struct {
	Fresh bool `json:"fresh"`
	// FreshSaved: a choice is saved for the server, as opposed to fresh by
	// the default. Only in the answer.
	FreshSaved      bool  `json:"fresh_saved"`
	LikeIgnoresCase bool  `json:"like_ignores_case"`
	MaxBehindSource int64 `json:"max_behind_source_seconds"`
}

// likeSwitchChanged is why the port's open connections of a server are
// closed when its LIKE switch changes.
const likeSwitchChanged = "the LIKE switch changed; connections read it when they open"

const sqlModeBootRefusal = "the command-line server has no switch of its own; " +
	"add the server from the web interface (+ Add server) to choose its mode"

// handleSQLModePut serves PUT /api/servers/{id}/sql-mode.
func (s *Server) handleSQLModePut(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == bootServerID {
		writeJSONError(w, http.StatusConflict, sqlModeBootRefusal)
		return
	}
	if _, ok := s.cm.reg.Get(id); !ok {
		writeJSONError(w, http.StatusNotFound, ErrUnknownServer.Error())
		return
	}
	// Strict on purpose: {} or a typo'd key would decode to false and turn
	// fresh mode off. The choice must be said.
	var body struct {
		Fresh           *bool  `json:"fresh"`
		LikeIgnoresCase *bool  `json:"like_ignores_case"`
		MaxBehindSource *int64 `json:"max_behind_source_seconds"`
	}
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&body); err != nil {
		writeBodyDecodeError(w, err)
		return
	}
	if dec.More() {
		writeJSONError(w, http.StatusBadRequest, "the body holds more than one JSON value")
		return
	}
	if body.Fresh == nil && body.LikeIgnoresCase == nil && body.MaxBehindSource == nil {
		writeJSONError(w, http.StatusBadRequest, `send "fresh": true for fresh mode or "fresh": false for fast mode, `+
			`or "like_ignores_case": true or false, or "max_behind_source_seconds": a number of seconds (0 for no limit)`)
		return
	}
	if body.MaxBehindSource != nil {
		if err := checkMaxBehindSource(*body.MaxBehindSource); err != nil {
			writeJSONError(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	old, _ := s.cm.reg.Get(id)
	e, err := s.cm.reg.SetSQLMode(id, body.Fresh, body.LikeIgnoresCase, body.MaxBehindSource)
	if err != nil {
		writeJSONError(w, registryErrStatus(err), err.Error())
		return
	}
	if body.Fresh != nil {
		mode := "fast"
		if e.SQLFreshOn() {
			mode = "fresh"
		}
		slog.Info("console: SQL mode saved", "actor", consoleActor(r), "server", e.Name, "id", id, "mode", mode)
	}
	if body.LikeIgnoresCase != nil {
		slog.Info("console: SQL LIKE switch saved", "actor", consoleActor(r), "server", e.Name, "id", id, "like_ignores_case", e.SQLLikeIgnoresCase)
	}
	if body.MaxBehindSource != nil {
		slog.Info("console: SQL lag limit saved", "actor", consoleActor(r), "server", e.Name, "id", id, "max_behind_source_seconds", e.SQLMaxBehindSource)
	}
	if e.SQLLikeIgnoresCase != old.SQLLikeIgnoresCase {
		// A connection keeps the switch it read when it opened: close the
		// open ones so the change is in force at once, turning it off above
		// all, which is how a wrong answer is stopped.
		s.closeRoutedConns(id, likeSwitchChanged)
	}
	// This process serves no port of its own in the usual setup (the router
	// does, and reads the registry again within seconds), but one that does
	// applies it now.
	s.applySQLFresh()
	writeJSON(w, http.StatusOK, sqlModeDTO{Fresh: e.SQLFreshOn(), FreshSaved: e.SQLFreshSaved(), LikeIgnoresCase: e.SQLLikeIgnoresCase,
		MaxBehindSource: e.SQLMaxBehindSource})
}
