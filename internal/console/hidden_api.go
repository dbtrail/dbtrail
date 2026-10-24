package console

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"

	"github.com/dbtrail/dbtrail/internal/hiderules"
)

// hiddenBootRefusal is why the command-line server has no hidden rules: it is
// never written to the registry, so there is nowhere to keep them (#2529).
const hiddenBootRefusal = "the command-line server cannot hide tables or columns; " +
	"add the server from the web interface (+ Add server) to set rules for it"

// hiddenDTO is GET and PUT /api/servers/{id}/hidden. The lists are never null
// on the wire, so a reader can iterate them without a check.
type hiddenDTO struct {
	Tables  []string `json:"tables"`
	Columns []string `json:"columns"`
	// Editable is false on the command-line server; Refusal says why.
	Editable bool   `json:"editable"`
	Refusal  string `json:"refusal,omitempty"`
}

func newHiddenDTO(r *hiderules.Rules) hiddenDTO {
	d := hiddenDTO{Tables: []string{}, Columns: []string{}, Editable: true}
	if r != nil {
		d.Tables = append(d.Tables, r.Tables...)
		d.Columns = append(d.Columns, r.Columns...)
	}
	return d
}

// handleHiddenGet serves GET /api/servers/{id}/hidden.
func (s *Server) handleHiddenGet(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == bootServerID {
		d := newHiddenDTO(nil)
		d.Editable, d.Refusal = false, hiddenBootRefusal
		writeJSON(w, http.StatusOK, d)
		return
	}
	e, ok := s.cm.reg.Get(id)
	if !ok {
		writeJSONError(w, http.StatusNotFound, ErrUnknownServer.Error())
		return
	}
	writeJSON(w, http.StatusOK, newHiddenDTO(e.Hidden))
}

// handleHiddenPut serves PUT /api/servers/{id}/hidden: the body replaces both
// lists. Empty lists remove the rules. A refused entry saves nothing, and the
// error names every refused entry.
func (s *Server) handleHiddenPut(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == bootServerID {
		writeJSONError(w, http.StatusConflict, hiddenBootRefusal)
		return
	}
	if _, ok := s.cm.reg.Get(id); !ok {
		writeJSONError(w, http.StatusNotFound, ErrUnknownServer.Error())
		return
	}
	// Strict on purpose: a typo'd key, a wrong nesting or a null would decode
	// to empty lists, and empty lists REMOVE every rule. Clearing must be
	// said with both lists present and empty.
	var body struct {
		Tables  *[]string `json:"tables"`
		Columns *[]string `json:"columns"`
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
	if body.Tables == nil || body.Columns == nil {
		writeJSONError(w, http.StatusBadRequest, `send both "tables" and "columns"; empty lists remove the rules`)
		return
	}
	req := hiderules.Rules{Tables: *body.Tables, Columns: *body.Columns}
	set, err := hiderules.Parse(req)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	e, err := s.cm.reg.SetHidden(id, set)
	if err != nil {
		writeJSONError(w, registryErrStatus(err), err.Error())
		return
	}
	saved := set.Rules()
	slog.Info("console: hidden rules saved", "actor", consoleActor(r), "server", e.Name, "id", id,
		"tables", strings.Join(saved.Tables, ","), "columns", strings.Join(saved.Columns, ","))
	writeJSON(w, http.StatusOK, newHiddenDTO(e.Hidden))
}
