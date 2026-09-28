package console

import (
	"context"
	"net/http"
)

// GET /api/capture-status: whether capture has read everything the selected
// server's source wrote (#1794).
//
// From the index alone a server nobody writes to and a capture that fell
// behind look the same, and the Overview said so on the coverage card while
// the flow drawing beside it said "quiet". Under watch the process is
// connected to the source and can ask it. This is the read that does, and
// it answers with one of three states. "Could not ask" is its own state: it
// is never sent as up to date, and never as behind.
//
// It is a route of its own, asked for by the Overview AFTER the card is
// drawn and only while capture is idle, and never a field of
// GET /api/coverage: that read must not wait on the operator's production
// server. The reporter bounds each read, keeps the answer for a short while
// and runs one read at a time per server.

const (
	// CaptureStateUpToDate: the source was asked, and capture has recorded
	// every transaction it has.
	CaptureStateUpToDate = "up_to_date"
	// CaptureStateBehind: the source was asked twice, and it has
	// transactions capture has not recorded.
	CaptureStateBehind = "behind"
	// CaptureStateUnknown: the source could not be asked, or what it said
	// does not settle it. Detail says which.
	CaptureStateUnknown = "unknown"
)

// CaptureStatus is the answer of GET /api/capture-status.
type CaptureStatus struct {
	// ServerID is the server the answer is about, so the page can drop an
	// answer that is not for the server on screen.
	ServerID string `json:"server_id"`
	State    string `json:"state"`
	// Detail says why the state is unknown or behind. No DSN, no error text.
	Detail string `json:"detail,omitempty"`
	// RetryInSeconds is set when the source looked ahead on a first read:
	// asking again after this long settles it.
	RetryInSeconds int `json:"retry_in_seconds,omitempty"`
}

// CaptureStatusReporter asks a server's source whether capture is caught
// up. nil when this process is not connected to any source (the read-only
// web interface), where the answer is always unknown.
type CaptureStatusReporter interface {
	// CaptureStatus answers for e. e.ID is the boot id for the daemon's own
	// index, whose source the reporter knows.
	CaptureStatus(ctx context.Context, e ServerEntry) CaptureStatus
}

func (s *Server) handleCaptureStatus(w http.ResponseWriter, r *http.Request) {
	id := r.Header.Get(serverHeader)
	if id == "" {
		id = s.cm.defaultID()
	}
	var entry ServerEntry
	switch {
	case id == "":
		writeJSONError(w, http.StatusNotFound, errNoServers.Error())
		return
	case id == bootServerID:
		b, dsn := s.cm.bootInfo()
		if b == nil {
			writeJSONError(w, http.StatusNotFound, ErrUnknownServer.Error())
			return
		}
		entry = ServerEntry{ID: bootServerID, DSN: dsn}
	default:
		var ok bool
		if s.cm.reg != nil {
			entry, ok = s.cm.reg.Get(id)
		}
		if !ok {
			writeJSONError(w, http.StatusNotFound, ErrUnknownServer.Error())
			return
		}
	}
	if s.captureStatus == nil {
		writeJSON(w, http.StatusOK, CaptureStatus{ServerID: entry.ID, State: CaptureStateUnknown,
			Detail: "this web interface is read-only and is not connected to the source"})
		return
	}
	got := s.captureStatus.CaptureStatus(r.Context(), entry)
	// The id is the one that was asked about, whatever the reporter filled
	// in, and a state this build does not know is unknown.
	got.ServerID = entry.ID
	if got.State != CaptureStateUpToDate && got.State != CaptureStateBehind {
		got.State = CaptureStateUnknown
	}
	if got.State != CaptureStateUnknown {
		got.RetryInSeconds = 0
	}
	writeJSON(w, http.StatusOK, got)
}
