package console

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/status"
	"github.com/dbtrail/dbtrail/internal/views"
)

// Which tables of a PostgreSQL copy the port sends to the source because
// capture did not keep them whole since their snapshot was read.
func TestPGNotCapturedOf(t *testing.T) {
	read := time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)
	ledger := func(at time.Time, truncated bool, tables ...string) sql.NullString {
		doc, err := json.Marshal(map[string]any{"replica_identity_not_full": map[string]any{
			"count": 3, "last_at": at, "tables": tables, "tables_truncated": truncated}})
		if err != nil {
			t.Fatal(err)
		}
		return sql.NullString{String: string(doc), Valid: true}
	}
	state := func(skips sql.NullString) *status.StreamStateInfo {
		return &status.StreamStateInfo{Mode: "gtid", GapColumnsPresent: true, CaptureSkips: skips}
	}
	tables := []views.BaselineTable{{Schema: "public", Table: "orders"}, {Schema: "public", Table: "items"}}
	cutAt := func(at time.Time) func(views.BaselineTable) pgCut {
		return func(views.BaselineTable) pgCut { return pgCut{md: baseline.DumpMetadata{LastDumpAt: at}} }
	}
	lost := func(st *status.StreamStateInfo, unreadable string, cutOf func(views.BaselineTable) pgCut) string {
		var names []string
		for _, l := range pgNotCapturedOf(tables, cutOf, st, unreadable) {
			if l.Why == "" {
				t.Errorf("%s.%s is listed without a reason", l.Schema, l.Table)
			}
			names = append(names, l.Schema+"."+l.Table)
		}
		return strings.Join(names, " ")
	}
	lostSlot := state(sql.NullString{})
	lostSlot.GapLostAt = sql.NullTime{Time: read.Add(time.Minute), Valid: true}
	for _, tc := range []struct {
		name       string
		st         *status.StreamStateInfo
		unreadable string
		cutOf      func(views.BaselineTable) pgCut
		want       string
	}{
		{"no ledger", state(sql.NullString{}), "", cutAt(read), ""},
		{"a drop of orders after the snapshot read", state(ledger(read.Add(time.Minute), false, "public.orders")), "", cutAt(read), "public.orders"},
		{"a drop older than the snapshot read: the copy answers as today", state(ledger(read.Add(-time.Minute), false, "public.orders")), "", cutAt(read), ""},
		// The table was ALTERed to FULL after its drops: the ledger still
		// dates its last drop after this snapshot, and it stays listed until
		// a snapshot read after that drop.
		{"altered to FULL after drops, the same snapshot", state(ledger(read.Add(time.Minute), false, "public.orders")), "", cutAt(read), "public.orders"},
		{"altered to FULL after drops, a snapshot read after the last drop", state(ledger(read.Add(time.Minute), false, "public.orders")), "", cutAt(read.Add(2 * time.Minute)), ""},
		{"a ledger whose list of tables is cut short", state(ledger(read.Add(time.Minute), true, "public.other")), "", cutAt(read), "public.orders public.items"},
		{"a ledger that names no table", state(ledger(read.Add(time.Minute), false)), "", cutAt(read), "public.orders public.items"},
		{"a ledger that does not parse", state(sql.NullString{String: "{", Valid: true}), "", cutAt(read), "public.orders public.items"},
		{"a ledger that cannot be read", nil, "the capture's record of dropped changes could not be read", cutAt(read), "public.orders public.items"},
		{"a slot lost after the snapshot read", lostSlot, "", cutAt(read), "public.orders public.items"},
		{"no state of the capture in the index", nil, "", cutAt(read), "public.orders public.items"},
		{"a tail refused for another reason is not a loss", state(ledger(read.Add(time.Minute), true)), "",
			func(views.BaselineTable) pgCut { return pgCut{refusal: "the snapshot of x does not record an LSN to continue from"} }, ""},
		{"a snapshot published over a known loss", state(sql.NullString{}), "",
			func(views.BaselineTable) pgCut {
				return pgCut{refusal: "the snapshot of x was published over a known loss of captured changes", gap: true}
			}, "public.orders public.items"},
		{"a snapshot that does not say when it was read is not a loss", state(ledger(read.Add(time.Minute), true)), "", cutAt(time.Time{}), ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := lost(tc.st, tc.unreadable, tc.cutOf); got != tc.want {
				t.Errorf("listed %q, want %q", got, tc.want)
			}
		})
	}
}

// expectStreamState is what status.LoadStreamState asks, in order; skips is
// what its own, best-effort read of the ledger gets.
func expectStreamState(mock sqlmock.Sqlmock, skips error) {
	cols := []string{"mode", "binlog_file", "binlog_position", "gtid_set", "events_indexed", "last_event_time", "last_checkpoint", "server_id", "bintrail_id", "gap_lost_at", "gap_lost_detail"}
	mock.ExpectQuery("SELECT mode, binlog_file").WillReturnRows(sqlmock.NewRows(cols).
		AddRow("lsn", "", 0, nil, 0, nil, time.Now(), 0, "x", nil, nil))
	mock.ExpectQuery("SELECT gap_acked_at FROM").WillReturnRows(sqlmock.NewRows([]string{"gap_acked_at"}).AddRow(nil))
	mock.ExpectQuery("SELECT source_health FROM").WillReturnRows(sqlmock.NewRows([]string{"source_health"}).AddRow(nil))
	if skips != nil {
		mock.ExpectQuery("SELECT capture_skips FROM").WillReturnError(skips)
	} else {
		mock.ExpectQuery("SELECT capture_skips FROM").WillReturnRows(sqlmock.NewRows([]string{"capture_skips"}).AddRow(nil))
	}
	mock.ExpectQuery("SELECT capture_skips_ack FROM").WillReturnRows(sqlmock.NewRows([]string{"capture_skips_ack"}).AddRow(nil))
	mock.ExpectQuery("SELECT MAX\\(snapshot_time\\)").WillReturnRows(sqlmock.NewRows([]string{"t"}).AddRow(nil))
}

// The ledger is read strictly: LoadStreamState turns an error reading it
// into "no drop", which would let the copy answer a table capture drops.
func TestPGCaptureLossState(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("lost connection to the index")
	ledger := `{"primary_key_deferrable":{"count":1,"last_at":"2026-11-21T12:00:00Z","tables":["public.orders"]}}`
	for _, tc := range []struct {
		name       string
		best       error // LoadStreamState's own read
		strict     func(*sqlmock.ExpectedQuery)
		unreadable bool
		skips      string
	}{
		{name: "both reads fail", best: boom, strict: func(q *sqlmock.ExpectedQuery) { q.WillReturnError(boom) }, unreadable: true},
		{name: "the best-effort read failed, the strict one reads it", best: boom,
			strict: func(q *sqlmock.ExpectedQuery) {
				q.WillReturnRows(sqlmock.NewRows([]string{"capture_skips"}).AddRow(ledger))
			}, skips: ledger},
		{name: "an index without the ledger column", strict: func(q *sqlmock.ExpectedQuery) {
			q.WillReturnError(&mysql.MySQLError{Number: 1054, Message: "Unknown column 'capture_skips'"})
		}},
		{name: "the row went between the reads", strict: func(q *sqlmock.ExpectedQuery) { q.WillReturnError(sql.ErrNoRows) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			expectStreamState(mock, tc.best)
			tc.strict(mock.ExpectQuery("SELECT capture_skips FROM stream_state WHERE id = 1"))
			st, why := pgCaptureLossState(ctx, db)
			if (why != "") != tc.unreadable {
				t.Fatalf("unreadable = %q, want %v", why, tc.unreadable)
			}
			if strings.Contains(why, boom.Error()) {
				t.Errorf("the reason reaches clients and must not carry the index's error: %q", why)
			}
			if !tc.unreadable && (st == nil || st.CaptureSkips.String != tc.skips) {
				t.Errorf("state %+v, want the ledger %q", st, tc.skips)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Error(err)
			}
		})
	}

	// The state itself cannot be read: every table is listed.
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery("SELECT mode, binlog_file").WillReturnError(boom)
	if _, why := pgCaptureLossState(ctx, db); why == "" {
		t.Error("an unreadable state must say so")
	}
}
