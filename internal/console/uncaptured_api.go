package console

import (
	"net/http"

	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/status"
)

// handleUncapturedTables serves GET /api/uncaptured-tables (#1802): how many
// tables the selected server's current schema snapshot captures, and every
// table it left out, with the reason and the statement that fixes it. The
// Overview names them, because a table left out after capture started used to
// be named only in the capture log.
//
// Server selection is the X-Bintrail-Server header, like /api/status: the list
// must belong to the index whose other numbers are on the same page.
//
// The read is fresh on every request, never the bundle's preloaded resolver,
// which is opened once and can be days old on a long-lived daemon. The newest
// snapshot is what capture decodes against after its next reload, and it is
// what a table that got its primary key and was re-read drops out of.
//
// Three things narrow the list before it is served:
//   - A PostgreSQL server gets "not applicable": its capture writes one
//     snapshot per table and never records exclusions, so a count taken from
//     its newest snapshot would read "1 of 1" on a healthy install.
//   - The scope capture runs with (captureFilterFor): tables outside it are
//     not "uncaptured tables", and where the scope cannot be known no count
//     is claimed at all.
//   - The session's data scope (#1449/#1452): a table the session may not read
//     is counted, never named, and its fix goes with it. The counts stay whole,
//     as the capture-health counts on /api/status do.
func (s *Server) handleUncapturedTables(w http.ResponseWriter, r *http.Request) {
	b := s.resolveOr(w, r)
	if b == nil {
		return
	}
	opts, err := s.applySessionProfile(r.Context(), r, b, query.Options{
		DenyTables:    s.denyTables,
		RedactColumns: s.redactCols,
		ProfileActive: s.profileActive,
	})
	if err != nil {
		writeSessionProfileError(w, r, err)
		return
	}
	entry, isRegistry := s.selectedEntry(r)
	var capture *status.TableCapture
	switch {
	case isRegistry && entry.IsPostgres():
		capture = &status.TableCapture{State: status.TableCaptureNotApplicable}
	default:
		capture = status.LoadTableCapture(r.Context(), b.db).WithFilter(s.captureFilterFor(entry, isRegistry))
	}
	writeJSON(w, http.StatusOK, capture.View(tableVisible(opts.DenyTables, opts.AllowTables)))
}

// captureFilterFor is what THIS process knows about the scope the selected
// server's capture runs with. Known only where this process is the one that
// runs or supervises that capture:
//
//   - a registry server, when the control plane is wired in: the supervisor
//     starts its stream from the entry, and a ServerEntry has no table filter
//     at all, so the scope is the entry's schemas and every table in them;
//   - the boot entry, when this process runs the boot capture and was told
//     its flags (BootCaptureFilter, set by `bintrail-console watch`).
//
// Everywhere else — a read-only console over someone else's index, a server
// whose capture another process runs — the answer is "not known", and the
// report then counts nothing rather than counting the snapshot and calling it
// coverage. A stream started outside this process with its own --tables
// filter is the residual case: nothing in the index records that filter, which
// is why this is decided per process rather than read back.
//
// One window it deliberately does not model: editing a server persists the
// new schemas and evicts only the index connection, so between that edit and
// the stream's restart this reports the DESIRED scope while capture still
// runs the old one.
func (s *Server) captureFilterFor(entry ServerEntry, isRegistry bool) status.CaptureFilter {
	if isRegistry {
		// A source connection is what makes an entry one this console
		// captures FOR — the same signal /api/servers and the first-run list
		// read. An index-only entry is captured by nobody here.
		if s.monitorCtrl == nil || entry.SourceDSN == "" {
			return status.CaptureFilter{}
		}
		return status.CaptureFilter{Known: true, Schemas: splitSchemas(entry.Schemas)}
	}
	if s.bootCaptureFilter == nil {
		return status.CaptureFilter{}
	}
	return *s.bootCaptureFilter
}
