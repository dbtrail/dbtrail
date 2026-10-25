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
type sqlModeDTO struct {
	Fresh bool `json:"fresh"`
}

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
		Fresh *bool `json:"fresh"`
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
	if body.Fresh == nil {
		writeJSONError(w, http.StatusBadRequest, `send "fresh": true for fresh mode or "fresh": false for fast mode`)
		return
	}
	e, err := s.cm.reg.SetSQLFresh(id, *body.Fresh)
	if err != nil {
		writeJSONError(w, registryErrStatus(err), err.Error())
		return
	}
	mode := "fast"
	if e.SQLFresh {
		mode = "fresh"
	}
	slog.Info("console: SQL mode saved", "actor", consoleActor(r), "server", e.Name, "id", id, "mode", mode)
	// This process serves no port of its own in the usual setup (the router
	// does, and reads the registry again within seconds), but one that does
	// applies it now.
	s.applySQLFresh()
	writeJSON(w, http.StatusOK, sqlModeDTO{Fresh: e.SQLFresh})
}
