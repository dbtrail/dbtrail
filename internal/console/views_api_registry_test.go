package console

import (
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
)

// TestViewsAvailable_readsTheRegistry: no other TestViewsAvailable case
// executes the registry read (a nil db short-circuits it), so this is the one
// that does: a
// live registry, no baseline, one local-only archive; the gate must say yes
// and the handler must serve it. It does NOT pin which routing the gate uses
// (a local-only row resolves the same under both); that the two routings
// agree on the count, which is what lets the gate and the handler differ, is
// pinned by TestArchiveRoutingsAgreeOnCount in internal/query.
func TestViewsAvailable_readsTheRegistry(t *testing.T) {
	dir := t.TempDir()
	localBase := filepath.Join(dir, "bintrail_id=only-local")
	if err := os.MkdirAll(filepath.Join(localBase, "event_date=2026-06-05", "event_hour=10"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(localBase, "event_date=2026-06-05", "event_hour=10", "events.parquet"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	cols := []string{"bintrail_id", "sample_local", "sample_bucket", "sample_key"}
	// The gate reads once and the handler reads once.
	for range 2 {
		mock.ExpectQuery("FROM archive_state").WillReturnRows(sqlmock.NewRows(cols).
			AddRow("only-local", filepath.Join(localBase, "events.parquet"), nil, nil))
	}

	srv := newViewsServer(t, "", false)
	srv.cm.boot.db = db
	r := httptest.NewRequest("GET", "http://127.0.0.1:8090/api/views.sql", nil)
	if !srv.viewsAvailable(r, srv.cm.boot) {
		t.Fatal("gate hides the button for a local-only archive the handler would serve")
	}
	rec, body := doServersReq(t, srv, "GET", "/api/views.sql?include_events=1", "")
	if rec.Code != 200 {
		t.Fatalf("handler code = %d, body = %s; the gate said yes", rec.Code, body)
	}
	if !strings.Contains(string(body), localBase) {
		t.Errorf("local-only source missing from the file:\n%s", body)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestViewsAPI_registryReadFailure: a registry that cannot be read is not
// "nothing archived yet". With no baseline half there is nothing honest to
// serve (502, like an unlistable baseline root); with one, the file is served
// and its header names the failure where the operator reads it.
func TestViewsAPI_registryReadFailure(t *testing.T) {
	denied := &mysql.MySQLError{Number: 1142, Message: "SELECT command denied"}

	t.Run("no baseline: 502, not a 404 claiming nothing is archived", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		mock.ExpectQuery("FROM archive_state").WillReturnError(denied)

		srv := newViewsServer(t, "", false)
		srv.cm.boot.db = db
		rec, body := doServersReq(t, srv, "GET", "/api/views.sql", "")
		if rec.Code != 502 {
			t.Fatalf("code = %d, body = %s; want 502", rec.Code, body)
		}
		if strings.Contains(string(body), "no archived partitions") {
			t.Errorf("a read failure was relabeled as an empty registry: %s", body)
		}
	})

	t.Run("with a baseline: served, header names the failure", func(t *testing.T) {
		dir := t.TempDir()
		writeBaselineFixture(t, dir, "2026-06-10T12-00-00Z", "shop", "orders.parquet")
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		mock.ExpectQuery("FROM archive_state").WillReturnError(denied)

		srv := newViewsServer(t, dir, false)
		srv.cm.boot.db = db
		rec, body := doServersReq(t, srv, "GET", "/api/views.sql", "")
		if rec.Code != 200 {
			t.Fatalf("code = %d, body = %s; want the baseline half served", rec.Code, body)
		}
		sql := string(body)
		if !strings.Contains(sql, "could not be read from archive_state") {
			t.Errorf("header does not name the registry failure:\n%s", sql)
		}
		for _, claim := range []string{"none registered in archive_state", "no archive sources are registered"} {
			if strings.Contains(sql, claim) {
				t.Errorf("file claims an empty registry after a failed read (%q):\n%s", claim, sql)
			}
		}
		// The error text names the index host and the DB user; it belongs in
		// the console log and the 502 body, never in a shareable file.
		if strings.Contains(sql, "SELECT command denied") {
			t.Errorf("raw registry error leaked into the downloadable file:\n%s", sql)
		}
		if !strings.Contains(sql, `CREATE OR REPLACE VIEW "shop"."orders"`) {
			t.Errorf("baseline half missing:\n%s", sql)
		}
	})
}
