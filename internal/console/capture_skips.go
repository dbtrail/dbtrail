package console

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/dbtrail/dbtrail/internal/status"
)

// captureSkipAckRequest is the acknowledge endpoint's body (#1314).
type captureSkipAckRequest struct {
	// SeenTotal is the total the CLIENT rendered. It is the stale-render
	// guard: a tab left open for an hour must not be able to acknowledge
	// skips that happened while nobody was looking. The server compares it
	// against its own read and refuses with 409 when the live tally is
	// higher — it never STAMPS this number, so a client cannot acknowledge
	// more than actually happened either.
	//
	// Absent (or negative) means "acknowledge whatever is there". Kept as a
	// pointer so a client that omits the field is distinguishable from one
	// that honestly saw zero.
	SeenTotal *int64 `json:"seen_total"`
}

// handleCaptureSkipsAck serves POST /api/capture-skips/ack: record that an
// operator has seen this server's capture-skip tally.
//
// Server selection follows the X-Bintrail-Server header, exactly like
// /api/status — NOT a path {id}. That is deliberate: the acknowledgement must
// land on the same index whose numbers the operator is looking at, and reusing
// the selection that produced those numbers is the only way to guarantee it.
//
// The write is a plain UPDATE. The console runs no DDL on a registry index (see
// connManager), so an index that predates the column is refused with the CLI
// command that migrates it rather than a driver error.
func (s *Server) handleCaptureSkipsAck(w http.ResponseWriter, r *http.Request) {
	b := s.resolveOr(w, r)
	if b == nil {
		return
	}
	seen := int64(-1)
	if r.Body != nil {
		var req captureSkipAckRequest
		// A malformed body is ignored rather than rejected: the guard it
		// carries is an optimization on top of a correct write, and failing
		// the acknowledgement over it would leave the operator stuck with the
		// alarm this endpoint exists to retire.
		if err := json.NewDecoder(r.Body).Decode(&req); err == nil && req.SeenTotal != nil {
			seen = *req.SeenTotal
		}
	}
	ackd, err := status.AcknowledgeCaptureSkips(r.Context(), b.db, seen, time.Now())
	switch {
	case errors.Is(err, status.ErrAcknowledgeStale):
		writeJSONError(w, http.StatusConflict,
			"more events were skipped since the Status page loaded ("+err.Error()+"); reload and read the new count before acknowledging it")
		return
	case errors.Is(err, status.ErrNothingToAcknowledge):
		writeJSONError(w, http.StatusBadRequest,
			"there is no capture-skip tally to acknowledge on this server")
		return
	case errors.Is(err, status.ErrAckColumnMissing):
		writeJSONError(w, http.StatusUnprocessableEntity,
			"this index predates the acknowledgement column; run `bintrail status --index-dsn <index> --ack-capture-skips` once against it (the web interface does not alter index schemas)")
		return
	case err != nil:
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"acknowledged":    true,
		"acknowledged_at": ackd.At.Format(status.TSFmt),
		"total":           ackd.Total,
		"reasons":         ackd.Reasons,
	})
}
