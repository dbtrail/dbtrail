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
// applies in its status document (sql_mode per server). BINTRAIL_SQL_TAILS=on
// where the router starts overrides every server's.
//
// like_ignores_case is the server's LIKE switch: on, statements with LIKE go
// to the copy, written ILIKE there (MySQL compares LIKE without case under
// its default collations, the copy with it); off, they stay on MySQL. The
// port reads it when a connection opens. Each field is saved only when it
// is sent, so one choice never resets the other.
type sqlModeDTO struct {
	Fresh           bool `json:"fresh"`
	LikeIgnoresCase bool `json:"like_ignores_case"`
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
		Fresh           *bool `json:"fresh"`
		LikeIgnoresCase *bool `json:"like_ignores_case"`
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
	if body.Fresh == nil && body.LikeIgnoresCase == nil {
		writeJSONError(w, http.StatusBadRequest, `send "fresh": true for fresh mode or "fresh": false for fast mode, `+
			`or "like_ignores_case": true or false`)
		return
	}
	old, _ := s.cm.reg.Get(id)
	e, err := s.cm.reg.SetSQLMode(id, body.Fresh, body.LikeIgnoresCase)
	if err != nil {
		writeJSONError(w, registryErrStatus(err), err.Error())
		return
	}
	if body.Fresh != nil {
		mode := "fast"
		if e.SQLFresh {
			mode = "fresh"
		}
		slog.Info("console: SQL mode saved", "actor", consoleActor(r), "server", e.Name, "id", id, "mode", mode)
	}
	if body.LikeIgnoresCase != nil {
		slog.Info("console: SQL LIKE switch saved", "actor", consoleActor(r), "server", e.Name, "id", id, "like_ignores_case", e.SQLLikeIgnoresCase)
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
	writeJSON(w, http.StatusOK, sqlModeDTO{Fresh: e.SQLFresh, LikeIgnoresCase: e.SQLLikeIgnoresCase})
}
