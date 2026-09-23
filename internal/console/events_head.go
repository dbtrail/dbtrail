package console

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/query"
)

// eventsHeadTimeout bounds the one query this route runs.
//
// The console sets no write timeout on purpose (a slow archive read has to be
// able to finish), so nothing else ends a request that neither answers nor
// fails. Two ordinary ways this one can hang: rotation's ALTER holds the
// table's metadata lock while MySQL's lock_wait_timeout defaults to a year,
// and an index host that goes away leaves the connection blocked on TCP
// retransmission for the ten-odd minutes the write-timeout help text names.
// The page behind this asks every five seconds and shows the rows it already
// has while it waits, so a request that hangs would leave it looking healthy
// and frozen; a refusal it can report is strictly better. One index descent
// per partition is milliseconds, so anything near this bound is a fault.
const eventsHeadTimeout = 5 * time.Second

// eventsHeadResponse is GET /api/events/head (#1801).
type eventsHeadResponse struct {
	// NewestEventID is the highest event_id in the live index, 0 when it holds
	// none. The Overview compares it with the last one it saw; any difference,
	// up or down (a stream reset deletes rows), means the list is read again.
	NewestEventID uint64 `json:"newest_event_id"`
}

// handleEventsHead serves GET /api/events/head: the newest event id in the
// selected server's index, which the Overview asks for every few seconds to
// learn whether anything changed (#1801).
//
// Why a separate route rather than asking for the events list every few
// seconds: the list carries row images, so every read of it records a
// query.run on the audit seam, and an Overview left open would have written
// one every five seconds per tab while nothing changed. This carries no row
// data and records nothing; the list is read, and audited, only when this
// number moves.
//
// The number is not narrowed by a session's data profile. What it discloses
// is that the index gained a row, not in which table: the coverage card
// already gives every session the newest event's time. A session whose
// profile does not exist on this server is refused here exactly as the list
// refuses it, so the Overview stops asking where the list says no.
//
// Cheap on any index size: event_id leads the primary key, so the maximum is
// one index descent per partition, not a scan.
func (s *Server) handleEventsHead(w http.ResponseWriter, r *http.Request) {
	b := s.resolveOr(w, r)
	if b == nil {
		return
	}
	if b.db == nil {
		writeJSONError(w, http.StatusBadGateway, "the server's index connection is not open")
		return
	}
	if _, err := s.applySessionProfile(r.Context(), r, b, query.Options{}); err != nil {
		writeSessionProfileError(w, r, err)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), eventsHeadTimeout)
	defer cancel()
	var resp eventsHeadResponse
	err := b.db.QueryRowContext(ctx, "SELECT COALESCE(MAX(event_id), 0) FROM binlog_events").Scan(&resp.NewestEventID)
	var me *mysql.MySQLError
	switch {
	case err == nil:
	case errors.As(err, &me) && me.Number == 1146:
		// The index database exists and its tables do not yet: nothing captured.
	case ctx.Err() != nil && r.Context().Err() == nil:
		// OUR deadline, not the client going away. The driver's wording for
		// it ("context deadline exceeded", or "canceling query due to user
		// request" once it reaches the server) says nothing about what did
		// not answer, and this sentence is shown to a person on the page.
		writeJSONError(w, http.StatusGatewayTimeout,
			"the index did not answer within "+eventsHeadTimeout.String()+"; it may be locked by another statement, or its server may be unreachable")
		return
	default:
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, resp)
}
