package console

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/internal/audittest"
)

// TestEventsHead is GET /api/events/head (#1801): the newest event id the
// selected server's index holds, which the Overview asks for every few
// seconds to learn whether anything changed. It carries no row data, so it
// records nothing on the audit seam: an Overview left open used to be a
// dashboard that never refreshed, and refreshing it by re-reading the events
// list would have written one query.run to the audit trail every five seconds
// per open tab. The list is read again, and audited, only when this number
// moves.
func TestEventsHead(t *testing.T) {
	rec := audittest.Install(t)
	for _, c := range []struct {
		name string
		rows *sqlmock.Rows
		err  error
		code int
		want uint64
	}{
		{"the newest id", sqlmock.NewRows([]string{"n"}).AddRow(uint64(42)), nil, 200, 42},
		{"an empty index is zero", sqlmock.NewRows([]string{"n"}).AddRow(uint64(0)), nil, 200, 0},
		{"an index whose tables are not created yet is zero", nil, &mysql.MySQLError{Number: 1146, Message: "Table 'x.binlog_events' doesn't exist"}, 200, 0},
		{"any other failure is an error, never a zero", nil, errors.New("connection reset"), 500, 0},
	} {
		t.Run(c.name, func(t *testing.T) {
			rec.Reset()
			db, mock, closeDB := newSQLMock(t)
			defer closeDB()
			q := mock.ExpectQuery(`SELECT COALESCE\(MAX\(event_id\), 0\) FROM binlog_events`)
			if c.err != nil {
				q.WillReturnError(c.err)
			} else {
				q.WillReturnRows(c.rows)
			}
			s := newBootServer(db)
			w := httptest.NewRecorder()
			s.handleEventsHead(w, httptest.NewRequest("GET", "/api/events/head", nil))
			if w.Code != c.code {
				t.Fatalf("code = %d, body = %s", w.Code, w.Body.String())
			}
			if c.code == 200 {
				var got struct {
					Newest *uint64 `json:"newest_event_id"`
				}
				if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil || got.Newest == nil || *got.Newest != c.want {
					t.Fatalf("body = %s, want newest_event_id %d", w.Body.String(), c.want)
				}
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Error(err)
			}
			if ev := rec.Events(); len(ev) != 0 {
				t.Errorf("the head recorded %d audit event(s): %+v", len(ev), ev)
			}
		})
	}
}

// TestEventsHeadIsBounded: the query carries a deadline of its own (#1801).
// The console sets no write timeout on purpose, so a request that neither
// answers nor fails has nothing to end it: MySQL's lock_wait_timeout defaults
// to a year, so an ALTER holding the metadata lock (rotation drops a
// partition) or an index host that went away can hold this open for minutes.
// The page asks every five seconds and shows stale rows while it waits, so a
// rejection is what it needs. Precedent: activityComputeTimeout.
func TestEventsHeadIsBounded(t *testing.T) {
	if eventsHeadTimeout <= 0 || eventsHeadTimeout > 30*time.Second {
		t.Fatalf("eventsHeadTimeout = %v, want a short positive bound", eventsHeadTimeout)
	}
	db, mock, closeDB := newSQLMock(t)
	defer closeDB()
	// The driver reports the deadline; the handler must pass it on rather
	// than hold the request open.
	mock.ExpectQuery(`SELECT COALESCE\(MAX\(event_id\), 0\) FROM binlog_events`).
		WillDelayFor(2 * eventsHeadTimeout).
		WillReturnRows(sqlmock.NewRows([]string{"n"}).AddRow(uint64(7)))
	s := newBootServer(db)
	w := httptest.NewRecorder()
	start := time.Now()
	s.handleEventsHead(w, httptest.NewRequest("GET", "/api/events/head", nil))
	took := time.Since(start)
	if w.Code == http.StatusOK {
		t.Fatalf("a query past the deadline answered 200: %s", w.Body.String())
	}
	// What the page shows a person. "context deadline exceeded" is the Go
	// runtime's phrase for it and says nothing about a database.
	if body := w.Body.String(); !strings.Contains(body, "did not answer within 5s") || strings.Contains(body, "context deadline") {
		t.Errorf("the refusal reads %s; want a sentence naming what did not answer", body)
	}
	if took > 3*eventsHeadTimeout {
		t.Errorf("the handler took %v, want it to give up near %v", took, eventsHeadTimeout)
	}
}

// TestEventsHeadClientGoneIsNotOurDeadline: a client that navigated away
// cancels the request, and that is not the index failing to answer. Reporting
// it as "the index did not answer" would put a database fault in the log for
// every operator who closed a tab mid-refresh, which the page does often.
func TestEventsHeadClientGoneIsNotOurDeadline(t *testing.T) {
	db, mock, closeDB := newSQLMock(t)
	defer closeDB()
	mock.ExpectQuery(`SELECT COALESCE\(MAX\(event_id\), 0\) FROM binlog_events`).
		WillDelayFor(time.Second).
		WillReturnRows(sqlmock.NewRows([]string{"n"}).AddRow(uint64(3)))
	s := newBootServer(db)
	ctx, cancel := context.WithCancel(context.Background())
	r := httptest.NewRequest("GET", "/api/events/head", nil).WithContext(ctx)
	go func() { time.Sleep(50 * time.Millisecond); cancel() }()
	w := httptest.NewRecorder()
	s.handleEventsHead(w, r)
	if strings.Contains(w.Body.String(), "did not answer within") {
		t.Errorf("a cancelled client was reported as the index timing out: %s", w.Body.String())
	}
}

// TestEventsHeadRefusesWhatEventsRefuses: a session whose data profile does
// not exist on this server is refused the events list, and the head refuses
// it the same way, so the Overview's refresh cannot keep asking where the
// list itself says no.
func TestEventsHeadRefusesWhatEventsRefuses(t *testing.T) {
	db, mock, closeDB := newSQLMock(t)
	defer closeDB()
	mock.ExpectQuery(`SELECT EXISTS\(SELECT 1 FROM profiles WHERE name = \?\)`).WithArgs("ghost").
		WillReturnRows(sqlmock.NewRows([]string{"e"}).AddRow(false))
	s := newBootServer(db)
	r := httptest.NewRequest("GET", "/api/events/head", nil)
	r = r.WithContext(context.WithValue(r.Context(), policyCtxKey{}, &ext.AccessPolicy{Profile: "ghost"}))
	w := httptest.NewRecorder()
	s.handleEventsHead(w, r)
	if w.Code != http.StatusForbidden {
		t.Fatalf("code = %d, body = %s, want 403", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// TestEventsHeadIsInTheAuditContract: ext/audit.go carries the list of what
// is deliberately NOT audited, and that file is where an auditor looks to
// find out. The behaviour is pinned above (the handler records nothing); this
// pins that the contract says so, so the two cannot drift apart silently.
func TestEventsHeadIsInTheAuditContract(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "ext", "audit.go"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), "/api/events/head") {
		t.Error("ext/audit.go does not name GET /api/events/head among the reads it deliberately does not audit")
	}
}

// TestEventsHeadTieredWithEvents: the head answers whoever may read the
// events list, and nobody else.
func TestEventsHeadTieredWithEvents(t *testing.T) {
	head, ok := permForRoute("GET", "/api/events/head")
	events, _ := permForRoute("GET", "/api/events")
	if !ok || head != events {
		t.Fatalf("GET /api/events/head: classified %v as %q, want %q like the events list", ok, head, events)
	}
}
