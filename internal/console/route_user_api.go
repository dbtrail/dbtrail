package console

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
)

// PUT and DELETE /api/servers/{id}/route: the forwarding account of one
// server, and nothing else of it.
//
// The account is also a field of PUT /api/servers/{id}. That route is the
// edit form's: it replaces the entry, so a field the body leaves out is
// cleared (the schema filter, the S3 store...). Fine for a form that always
// sends every field, and a trap for a script or a command that means to
// change one thing. This route changes that one thing.

// handleRouteUserPut serves PUT /api/servers/{id}/route: {"user": ...,
// "password": ...}. A user alone keeps the password when it is the stored
// account's; a password alone replaces the stored account's.
func (s *Server) handleRouteUserPut(w http.ResponseWriter, r *http.Request) {
	var body struct {
		User     *string `json:"user"`
		Password *string `json:"password"`
		// CaptureAccount is the answer about forwarding with the capture
		// account (#3054, capture_forward.go), sent alone.
		CaptureAccount *bool `json:"capture_account"`
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
	if body.CaptureAccount != nil {
		if body.User != nil || body.Password != nil {
			writeJSONError(w, http.StatusBadRequest, `send "capture_account" alone, or "user" and "password" without it: they are two settings`)
			return
		}
		s.saveCaptureAnswer(w, r, *body.CaptureAccount)
		return
	}
	// An empty user here would remove the account, which is what DELETE is
	// for; an empty password is not a password.
	if (body.User == nil && body.Password == nil) ||
		(body.User != nil && strings.TrimSpace(*body.User) == "") ||
		(body.Password != nil && *body.Password == "") {
		writeJSONError(w, http.StatusBadRequest, `send "user", "password" or both, none of them empty, or "capture_account" alone; DELETE removes the forwarding account`)
		return
	}
	s.saveRouteUser(w, r, serverRequest{RouteUser: body.User, RoutePassword: body.Password})
}

// handleRouteUserDelete serves DELETE /api/servers/{id}/route.
func (s *Server) handleRouteUserDelete(w http.ResponseWriter, r *http.Request) {
	empty := ""
	s.saveRouteUser(w, r, serverRequest{RouteUser: &empty})
}

func (s *Server) saveRouteUser(w http.ResponseWriter, r *http.Request, req serverRequest) {
	id := r.PathValue("id")
	if id == bootServerID {
		writeJSONError(w, http.StatusConflict, "the command-line server cannot be edited; it mirrors --index-dsn")
		return
	}
	// Worked out and saved under one hold of the registry's lock, from the
	// entry as it is at that moment: every other field is the one stored.
	var old, saved ServerEntry
	var buildErr error
	err := s.cm.reg.UpdateWith(id, func(cur ServerEntry) (ServerEntry, bool) {
		old = cur
		dsn, err := buildRouteDSN(req, cur.RouteDSN, cur.SourceDSN, cur.SourceDSN, cur.SourceFlavor())
		if err != nil {
			buildErr = err
			return cur, false
		}
		cur.RouteDSN = dsn
		saved = cur
		return cur, true
	})
	switch {
	case buildErr != nil:
		writeJSONError(w, http.StatusBadRequest, buildErr.Error())
		return
	case errors.Is(err, ErrUnknownServer):
		writeJSONError(w, http.StatusNotFound, ErrUnknownServer.Error())
		return
	case err != nil:
		writeJSONError(w, registryErrStatus(err), err.Error())
		return
	}
	// Connections the port opened as the old account must not outlive it.
	if forwardChanged(old, saved) {
		s.dropRoutedConns(id, "the account the port forwards with changed", forwardDSNOf(old), old.SourceSSL())
	}
	writeJSON(w, http.StatusOK, s.entryDTO(saved))
}

// saveCaptureAnswer saves the answer about forwarding with the capture
// account (#3054) through the registry's only writer of it.
func (s *Server) saveCaptureAnswer(w http.ResponseWriter, r *http.Request, on bool) {
	id := r.PathValue("id")
	if id == bootServerID {
		writeJSONError(w, http.StatusConflict, "the command-line server cannot be edited; it mirrors --index-dsn")
		return
	}
	old, ok := s.cm.reg.Get(id)
	if !ok {
		writeJSONError(w, http.StatusNotFound, ErrUnknownServer.Error())
		return
	}
	saved, err := s.cm.reg.SetForwardWithCaptureAccount(id, on)
	switch {
	case errors.Is(err, errNoSourceToForward), errors.Is(err, errSourceUnreadable):
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	case errors.Is(err, ErrUnknownServer):
		writeJSONError(w, http.StatusNotFound, ErrUnknownServer.Error())
		return
	case err != nil:
		writeJSONError(w, registryErrStatus(err), err.Error())
		return
	}
	if forwardChanged(old, saved) {
		s.dropRoutedConns(id, "the account the port forwards with changed", forwardDSNOf(old), old.SourceSSL())
	}
	dto := s.entryDTO(saved)
	if on {
		dto.CaptureAccountWarning = s.captureRoleWarning(r.Context(), saved)
	}
	writeJSON(w, http.StatusOK, dto)
}
