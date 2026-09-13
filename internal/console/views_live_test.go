package console

import (
	"bytes"
	"fmt"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	drivermysql "github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/ext"
)

// The index the download describes. The password is here for one reason: to
// be looked for in the artifact.
const (
	liveTestDSN      = "reader:hunter2@tcp(db.internal:3307)/idx"
	liveTestPassword = "hunter2"
)

// newLiveViewsServer builds a console whose boot bundle points at a mocked
// index, so the download's live half runs the REAL resolution path (the DSN
// parse, the column probe, the attribution query) rather than a hand-built
// views.LiveIndex.
func newLiveViewsServer(t *testing.T, dsn string) (*Server, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	srv := newViewsServer(t, "", false)
	srv.cm.boot.db = db
	srv.cm.boot.dsn = dsn
	return srv, mock
}

// expectArchiveSource queues the TWO archive_state reads buildViewsInput does,
// so there is a layout to describe: the per-source roots, then the per-column-
// set groups (#1535).
//
// The group read answers with a NULL column_set on purpose. That is what a
// registry looks like before `archive reconcile --repair` has recorded one, and
// it keeps these tests on the globbed leg they were written against — the
// grouped shape has its own tests, where the difference is the point.
func expectArchiveSource(mock sqlmock.Sqlmock, id string) {
	mock.ExpectQuery("FROM archive_state").WillReturnRows(
		sqlmock.NewRows([]string{"bintrail_id", "sample_local", "sample_bucket", "sample_key"}).
			AddRow(id, nil, "bkt", "events/bintrail_id="+id+"/f.parquet"))
	mock.ExpectQuery("FROM archive_state").WillReturnRows(
		sqlmock.NewRows([]string{"bintrail_id", "local_path", "s3_key", "column_set"}).
			AddRow(id, nil, "events/bintrail_id="+id+"/f.parquet", nil))
}

// TestViewsAPI_includeLiveAddsTheHotLeg: the download can now carry the leg
// over the live index (#1480), which the console used to be structurally
// unable to offer (LiveLegUnavailable was hardcoded). Asking for it must
// produce the ATTACH, the two-leg events view, and the location facts a reader
// on another machine needs.
func TestViewsAPI_includeLiveAddsTheHotLeg(t *testing.T) {
	const id = "aaaa"
	srv, mock := newLiveViewsServer(t, liveTestDSN)
	expectArchiveSource(mock, id)
	mock.ExpectQuery("information_schema.COLUMNS").WillReturnRows(
		sqlmock.NewRows([]string{"COLUMN_NAME"}).
			AddRow("event_id").AddRow("event_timestamp").AddRow("schema_name").AddRow("table_name"))
	mock.ExpectQuery("FROM bintrail_servers").WillReturnRows(
		sqlmock.NewRows([]string{"n", "id"}).AddRow(1, id))

	rec, body := doServersReq(t, srv, "GET", "/api/views.sql?include_events=1&include_live=1", "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	sql := string(body)
	for _, want := range []string{
		"ATTACH ''",          // the live catalog is opened
		"bintrail_live",      // and the two-leg view reads it
		"HOST 'db.internal'", // resolved from the console's own DSN
		"PORT 3307",
		"DATABASE 'idx'",
		"USER 'reader'",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("the requested live leg is missing %q from the file:\n%s", want, sql)
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// The password is the one connection fact the file must never carry: it is
// meant to be shared, and its header says it holds no credentials. The slot
// is emitted empty for the operator to fill in their own session.
func TestViewsAPI_includeLiveNeverCarriesThePassword(t *testing.T) {
	srv, mock := newLiveViewsServer(t, liveTestDSN)
	expectArchiveSource(mock, "aaaa")
	mock.ExpectQuery("information_schema.COLUMNS").WillReturnRows(
		sqlmock.NewRows([]string{"COLUMN_NAME"}).AddRow("event_id"))
	mock.ExpectQuery("FROM bintrail_servers").WillReturnRows(
		sqlmock.NewRows([]string{"n", "id"}).AddRow(1, "aaaa"))

	rec, body := doServersReq(t, srv, "GET", "/api/views.sql?include_events=1&include_live=1", "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	sql := string(body)
	if strings.Contains(sql, liveTestPassword) {
		t.Errorf("the downloaded file carries the index password:\n%s", sql)
	}
	if !strings.Contains(sql, "PASSWORD ''") {
		t.Errorf("the password slot is not empty for the reader to fill:\n%s", sql)
	}
}

// TestViewsAPI_liveLegIsOptIn: the leg names the index by host and port in a
// shareable file, and a query against the two-leg view reads the live capture
// index. Neither may happen because someone clicked download.
func TestViewsAPI_liveLegIsOptIn(t *testing.T) {
	srv, mock := newLiveViewsServer(t, liveTestDSN)
	expectArchiveSource(mock, "aaaa")

	rec, body := doServersReq(t, srv, "GET", "/api/views.sql?include_events=1", "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	sql := string(body)
	if strings.Contains(sql, "bintrail_live") || strings.Contains(sql, "ATTACH ''") {
		t.Errorf("the live leg is in a file nobody asked it for:\n%s", sql)
	}
	// And the archives-only note names the route this reader HAS, which is now
	// the CLI. It used to be a checkbox, and the note named it verbatim; the
	// card no longer offers the live leg, so the console stops overriding
	// LiveLegHowTo and the generator's own wording becomes the true one.
	// Inverted deliberately: the rule did not change, the reader's controls did.
	if !strings.Contains(sql, "--include-live") {
		t.Errorf("the archives-only note names no route at all for the live leg:\n%s", sql)
	}
	if strings.Contains(sql, "and the live index") {
		t.Errorf("the note still points at a checkbox this card no longer has:\n%s", sql)
	}
	// The probe queries must not have run either: this download asked the
	// index nothing.
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// TestViewsAPI_includeLiveStillRefusesAProfiledSession: the parameter is new
// surface on an existing gate. A data profile refuses this file because it maps
// straight onto the unredacted Parquet, and asking for one more leg cannot be
// the way around that.
func TestViewsAPI_includeLiveStillRefusesAProfiledSession(t *testing.T) {
	srv, _ := newLiveViewsServer(t, liveTestDSN)

	for _, path := range []string{"/api/views.sql", "/api/views.sql?include_events=1&include_live=1"} {
		req := httptest.NewRequest("GET", "http://127.0.0.1:8090"+path, nil)
		req.Host = "127.0.0.1:8090"
		req.Header.Set("Authorization", "Bearer t")
		req = req.WithContext(withPolicy(&ext.AccessPolicy{Profile: "analyst", Permissions: ext.AllPermissions()}))
		rec := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rec, req)
		if rec.Code != 403 {
			t.Errorf("%s: code = %d, body = %s; want 403 for a profiled session", path, rec.Code, rec.Body.String())
		}
		if strings.Contains(rec.Body.String(), "db.internal") {
			t.Errorf("%s: the refusal describes the index anyway: %s", path, rec.Body.String())
		}
	}
}

// A server this console reaches over a unix socket cannot carry the leg
// however it is asked for: the file locates the index by host and port so it
// can run elsewhere. Refused with that reason, never a file quietly missing
// the half the reader ticked a box for.
func TestViewsAPI_includeLiveRefusesASocketIndex(t *testing.T) {
	srv, mock := newLiveViewsServer(t, "reader:hunter2@unix(/tmp/mysql.sock)/idx")
	// One per request below: the layout is resolved before the live half.
	expectArchiveSource(mock, "aaaa")
	expectArchiveSource(mock, "aaaa")

	rec, body := doServersReq(t, srv, "GET", "/api/views.sql?include_events=1&include_live=1", "")
	if rec.Code != 422 {
		t.Fatalf("code = %d, body = %s; want 422", rec.Code, body)
	}
	if !strings.Contains(string(body), "unix socket") {
		t.Errorf("the refusal does not say why: %s", body)
	}
	if strings.Contains(string(body), liveTestPassword) {
		t.Errorf("the refusal echoes the index password: %s", body)
	}
	// The archives-only download for that same server names no route at all,
	// rather than pointing at a box that would refuse.
	rec, body = doServersReq(t, srv, "GET", "/api/views.sql?include_events=1", "")
	if rec.Code != 200 {
		t.Fatalf("plain download: code = %d, body = %s", rec.Code, body)
	}
	if strings.Contains(string(body), `ticking "Include the live index"`) {
		t.Errorf("the file offers a route this server cannot take:\n%s", body)
	}
	if !strings.Contains(string(body), "no way to reach the index") {
		t.Errorf("the file does not say the live leg is unavailable here:\n%s", body)
	}
}

// An index with no binlog_events table cannot back the view the leg IS. The
// generated file would otherwise be a binder error that defines nothing.
func TestViewsAPI_includeLiveRefusesAnIndexWithoutBinlogEvents(t *testing.T) {
	srv, mock := newLiveViewsServer(t, liveTestDSN)
	expectArchiveSource(mock, "aaaa")
	mock.ExpectQuery("information_schema.COLUMNS").WillReturnRows(sqlmock.NewRows([]string{"COLUMN_NAME"}))

	rec, body := doServersReq(t, srv, "GET", "/api/views.sql?include_events=1&include_live=1", "")
	if rec.Code != 422 {
		t.Fatalf("code = %d, body = %s; want 422", rec.Code, body)
	}
	if !strings.Contains(string(body), "binlog_events") {
		t.Errorf("the refusal does not name what is missing: %s", body)
	}
}

// The hot leg names only columns the index HAS: it has no union_by_name, so a
// column named that is not there makes DuckDB refuse the whole file. An index
// the console never migrated is exactly that case.
func TestViewsAPI_includeLiveNamesOnlyObservedColumns(t *testing.T) {
	srv, mock := newLiveViewsServer(t, liveTestDSN)
	expectArchiveSource(mock, "aaaa")
	// A legacy index: no connection_id, no query_text, no commit_ts_us.
	mock.ExpectQuery("information_schema.COLUMNS").WillReturnRows(
		sqlmock.NewRows([]string{"COLUMN_NAME"}).
			AddRow("event_id").AddRow("event_timestamp").AddRow("schema_name"))
	mock.ExpectQuery("FROM bintrail_servers").WillReturnRows(
		sqlmock.NewRows([]string{"n", "id"}).AddRow(1, "aaaa"))

	rec, body := doServersReq(t, srv, "GET", "/api/views.sql?include_events=1&include_live=1", "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	if !strings.Contains(string(body), "not a column of this index's binlog_events") {
		t.Errorf("the hot leg does not mark the columns this index lacks:\n%s", body)
	}
}

// TestViewsAPI_liveLegAttributionOutcomes covers the three things the file can
// say about WHOSE rows the hot leg carries. They are one sentence each in the
// artifact and they are not interchangeable: "several sources are registered"
// was once printed for a file-mode index that serves exactly one, for an index
// too old to have the table, and for a dropped connection.
//
// The archives here are registered under a DIFFERENT id from the one the
// registry reports, on purpose for the attributed case: the generator refuses
// to assert either identity when the two disagree, and a test whose ids match
// would never exercise that.
func TestViewsAPI_liveLegAttributionOutcomes(t *testing.T) {
	for _, tc := range []struct {
		name    string
		servers func(sqlmock.Sqlmock)
		want    string
	}{
		{
			name: "no such table",
			servers: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("FROM bintrail_servers").WillReturnError(&drivermysql.MySQLError{
					Number: 1146, Message: "Table 'idx.bintrail_servers' doesn't exist"})
			},
			// An index that never ran the migration registers no source. It is
			// NOT evidence of several, which is the inversion this pins.
			want: "this index registers no source id",
		},
		{
			name: "several sources",
			servers: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("FROM bintrail_servers").WillReturnRows(
					sqlmock.NewRows([]string{"n", "id"}).AddRow(2, "aaaa"))
			},
			want: "more than one source is registered",
		},
		{
			name: "the list could not be read",
			servers: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("FROM bintrail_servers").WillReturnError(&drivermysql.MySQLError{
					Number: 1142, Message: "SELECT command denied to user 'reader'@'%' for table 'bintrail_servers'"})
			},
			want: "could not be read",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, mock := newLiveViewsServer(t, liveTestDSN)
			expectArchiveSource(mock, "aaaa")
			mock.ExpectQuery("information_schema.COLUMNS").WillReturnRows(
				sqlmock.NewRows([]string{"COLUMN_NAME"}).AddRow("event_id"))
			tc.servers(mock)

			rec, body := doServersReq(t, srv, "GET", "/api/views.sql?include_events=1&include_live=1", "")
			if rec.Code != 200 {
				t.Fatalf("code = %d, body = %s", rec.Code, body)
			}
			if !strings.Contains(string(body), tc.want) {
				t.Errorf("the file does not state what was observed (%q):\n%s", tc.want, body)
			}
			// The leg is still there: an unattributed leg is a leg, and
			// dropping it would turn a missing sentence into missing data.
			if !strings.Contains(string(body), "bintrail_live") {
				t.Errorf("the live leg is gone because attribution was inconclusive:\n%s", body)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Error(err)
			}
		})
	}
}

// An unreadable source list leaves ONE sentence in the file, and that sentence
// deliberately names no cause: a revoked SELECT, a dropped connection and a
// timeout read the same there. They are not one thing to fix, so the cause has
// to reach the log, which is what stays on the host.
func TestViewsAPI_liveLegAttributionFailureIsLogged(t *testing.T) {
	var logs bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	srv, mock := newLiveViewsServer(t, liveTestDSN)
	expectArchiveSource(mock, "aaaa")
	mock.ExpectQuery("information_schema.COLUMNS").WillReturnRows(
		sqlmock.NewRows([]string{"COLUMN_NAME"}).AddRow("event_id"))
	mock.ExpectQuery("FROM bintrail_servers").WillReturnError(&drivermysql.MySQLError{
		Number: 1142, Message: "SELECT command denied to user 'reader'@'%' for table 'bintrail_servers'"})

	rec, _ := doServersReq(t, srv, "GET", "/api/views.sql?include_events=1&include_live=1", "")
	if rec.Code != 200 {
		t.Fatalf("code = %d", rec.Code)
	}
	if !strings.Contains(logs.String(), "SELECT command denied") {
		t.Errorf("the cause of the unreadable source list never reached the log; the operator has nothing to act on:\n%s", logs.String())
	}
}

// The column probe is the half that decides whether the file binds, so a
// driver failure there is an upstream fault (502), not a file quietly missing
// the leg. And the answer goes through scrubDSNError: a driver error can echo
// the DSN it dialed, and this body reaches a browser.
func TestViewsAPI_includeLiveColumnProbeFailureIs502(t *testing.T) {
	srv, mock := newLiveViewsServer(t, liveTestDSN)
	expectArchiveSource(mock, "aaaa")
	mock.ExpectQuery("information_schema.COLUMNS").WillReturnError(
		fmt.Errorf("dial tcp: lookup db.internal: no such host (dsn %s)", liveTestDSN))

	rec, body := doServersReq(t, srv, "GET", "/api/views.sql?include_events=1&include_live=1", "")
	if rec.Code != 502 {
		t.Fatalf("code = %d, body = %s; want 502", rec.Code, body)
	}
	if strings.Contains(string(body), liveTestPassword) {
		t.Errorf("the 502 body carries the index password: %s", body)
	}
	if !strings.Contains(string(body), "binlog_events columns") {
		t.Errorf("the 502 does not say which read failed: %s", body)
	}
}

// An unrecognized include_live value is refused, not silently read as "off".
// "on" is what a bare HTML checkbox posts, and answering it with 200 and an
// archives-only file whose own note says to tick the box is the worst of the
// three possible answers.
func TestViewsAPI_includeLiveRejectsAnUnknownValue(t *testing.T) {
	for _, v := range []string{"on", "yes", "TRUE!", "2"} {
		srv, mock := newLiveViewsServer(t, liveTestDSN)
		rec, body := doServersReq(t, srv, "GET", "/api/views.sql?include_events=1&include_live="+v, "")
		if rec.Code != 400 {
			t.Errorf("include_live=%s: code = %d, body = %s; want 400", v, rec.Code, body)
			continue
		}
		if !strings.Contains(string(body), "include_live=1") {
			t.Errorf("include_live=%s: the refusal does not name a value that works: %s", v, body)
		}
		// Refused before the layout is resolved: nothing was asked of the index.
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("include_live=%s: %v", v, err)
		}
	}
	// The values that DO work still work, in both spellings and both cases.
	for _, v := range []string{"true", "TRUE", "1"} {
		srv, mock := newLiveViewsServer(t, liveTestDSN)
		expectArchiveSource(mock, "aaaa")
		mock.ExpectQuery("information_schema.COLUMNS").WillReturnRows(
			sqlmock.NewRows([]string{"COLUMN_NAME"}).AddRow("event_id"))
		mock.ExpectQuery("FROM bintrail_servers").WillReturnRows(
			sqlmock.NewRows([]string{"n", "id"}).AddRow(1, "aaaa"))
		rec, body := doServersReq(t, srv, "GET", "/api/views.sql?include_events=1&include_live="+v, "")
		if rec.Code != 200 || !strings.Contains(string(body), "bintrail_live") {
			t.Errorf("include_live=%s: code = %d, and the leg is %v", v, rec.Code, strings.Contains(string(body), "bintrail_live"))
		}
	}
	// And "off" spellings stay off, without a refusal.
	for _, v := range []string{"0", "false", ""} {
		srv, mock := newLiveViewsServer(t, liveTestDSN)
		expectArchiveSource(mock, "aaaa")
		rec, body := doServersReq(t, srv, "GET", "/api/views.sql?include_events=1&include_live="+v, "")
		if rec.Code != 200 || strings.Contains(string(body), "bintrail_live") {
			t.Errorf("include_live=%q: code = %d, and the leg is present = %v", v, rec.Code, strings.Contains(string(body), "bintrail_live"))
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("include_live=%q: %v", v, err)
		}
	}
}

// TestViewsAPI_includeLiveRefusesAnAddressWithNoHost: ":3306" parses, and the
// driver reads the empty host as localhost on its own machine. The FILE cannot
// do that: it would carry HOST ” to a reader who is somewhere else, and the
// generator's loopback warning does not recognize "" as a local address, so
// nothing anywhere would say the address is unusable.
func TestViewsAPI_includeLiveRefusesAnAddressWithNoHost(t *testing.T) {
	srv, mock := newLiveViewsServer(t, "reader:hunter2@tcp(:3306)/idx")
	expectArchiveSource(mock, "aaaa")
	expectArchiveSource(mock, "aaaa")

	rec, body := doServersReq(t, srv, "GET", "/api/views.sql?include_events=1&include_live=1", "")
	if rec.Code != 422 {
		t.Fatalf("code = %d, body = %s; want 422", rec.Code, body)
	}
	if !strings.Contains(string(body), "no host") {
		t.Errorf("the refusal does not say what is missing: %s", body)
	}

	// And the archives-only file for that server offers no route, exactly as
	// it does for a socket: the checkbox would refuse.
	rec, body = doServersReq(t, srv, "GET", "/api/views.sql?include_events=1", "")
	if rec.Code != 200 {
		t.Fatalf("plain download: code = %d, body = %s", rec.Code, body)
	}
	if strings.Contains(string(body), `"…and the live index"`) {
		t.Errorf("the file offers a route this server cannot take:\n%s", body)
	}
	// Positive evidence that the file did not simply render the leg with an
	// empty host somewhere else in it.
	if strings.Contains(string(body), "HOST ''") {
		t.Errorf("the file names an empty index host:\n%s", body)
	}
}
