package console

import (
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/dbtrail/dbtrail/internal/query"
)

// newBootServer builds a Server whose ephemeral boot entry wraps db — the
// multi-server equivalent of the pre-registry single-DB construction these
// tests used (&Server{db: ..., engine: ...}). The boot bundle is reachable as
// s.cm.boot for per-test tweaks (baseline gates, resolver, ...). noArchive is
// true, matching the old tests' default (no planner / archive discovery).
func newBootServer(db *sql.DB) *Server {
	// sessionProfiles is set like production (server.go New) — handlers that
	// resolve a session's profile per request (events, schemas)
	// reach the cache even in handler-level tests.
	s := &Server{token: "t", cm: newConnManager(nil, false), sessionProfiles: newProfileRuleCache()}
	s.cm.boot = &bundle{db: db, engine: query.New(db), noArchive: true}
	s.mux = s.buildHandler()
	return s
}

// newSQLMock wraps sqlmock.New with the usual fatal-on-error + closer.
func newSQLMock(t *testing.T) (*sql.DB, sqlmock.Sqlmock, func()) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	return db, mock, func() { db.Close() }
}
