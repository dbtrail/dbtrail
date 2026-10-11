package rotation

import (
	"context"
	"database/sql"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"

	"github.com/dbtrail/dbtrail/internal/indexer"
)

// windowDB is an index as ResolveWindow reads it: the retention record (""
// = none, "!" = a read that fails) and the partition listing (ages nil = a
// listing that fails).
func windowDB(t *testing.T, record string, recordedAgo time.Duration, ages []time.Duration) (*sql.DB, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	switch record {
	case "":
		mock.ExpectQuery("rotation_policy").WillReturnError(sql.ErrNoRows)
	case "!":
		mock.ExpectQuery("rotation_policy").WillReturnError(sql.ErrConnDone)
	default:
		mock.ExpectQuery("rotation_policy").
			WillReturnRows(sqlmock.NewRows([]string{"initial_retain", "recorded_at"}).AddRow(record, time.Now().Add(-recordedAgo)))
	}
	if ages == nil {
		mock.ExpectQuery("information_schema.PARTITIONS").WillReturnError(sql.ErrConnDone)
		return db, mock
	}
	rows := sqlmock.NewRows([]string{"PARTITION_NAME", "PARTITION_DESCRIPTION", "PARTITION_ORDINAL_POSITION"})
	for i, age := range ages {
		rows.AddRow(indexer.PartitionName(time.Now().UTC().Add(-age)), "0", i+1)
	}
	mock.ExpectQuery("information_schema.PARTITIONS").WillReturnRows(rows)
	return db, mock
}

// TestResolveWindow pins the answer a daemon's doctor run grades an index
// over (#2328) against the loop's own branches: off, explicit, or the index's
// record behind the upgrade guard.
func TestResolveWindow(t *testing.T) {
	const d = 24 * time.Hour
	deep := []time.Duration{100 * d, time.Hour}
	recent := []time.Duration{10 * d, time.Hour}
	implicit := Settings{Enabled: true, Retain: 48 * time.Hour, RetainRaw: "48h", Interval: time.Hour}
	explicit := implicit
	explicit.Explicit, explicit.Retain, explicit.RetainRaw = true, 6*time.Hour, "6h"

	cases := []struct {
		name       string
		s          Settings
		record     string
		recordedAt time.Duration
		ages       []time.Duration
		asked      bool // the index must have been read (both queries)
		retain     time.Duration
		noted      bool
		blocked    bool
	}{
		// The two that must NOT ask: these databases would answer "blocked".
		{"rotation off: nothing, the index is not asked", Settings{}, "", 0, deep, false, 0, false, false},
		{"explicit window: it, the index is not asked", explicit, "", 0, deep, false, 6 * time.Hour, false, false},
		{"implicit, a record, recent history", implicit, "48h", 200 * d, recent, true, 48 * time.Hour, true, false},
		{"implicit, no record, recent history: the legacy window", implicit, "", 0, recent, true, 30 * d, true, false},
		{"implicit, no record, deep history: guarded", implicit, "", 0, deep, true, 30 * d, true, true},
		{"implicit, a record newer than the history (a restore): guarded", implicit, "48h", time.Hour, deep, true, 48 * time.Hour, true, true},
		{"implicit, a record older than the history (an outage): not guarded", implicit, "48h", 200 * d, deep, true, 48 * time.Hour, true, false},
		{"implicit, unreadable record, deep history: legacy window, guarded like the loop", implicit, "!", 0, deep, true, 30 * d, true, true},
		{"implicit, a record of zero, deep history: legacy window, guarded", implicit, "0", 200 * d, deep, true, 30 * d, true, true},
		{"implicit, a record this build cannot parse, deep history: legacy window, guarded", implicit, "two weeks", 200 * d, deep, true, 30 * d, true, true},
		{"implicit, the listing fails: the window, not known to be guarded", implicit, "", 0, nil, true, 30 * d, true, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, mock := windowDB(t, tc.record, tc.recordedAt, tc.ages)
			w := ResolveWindow(context.Background(), db, "binlog_index", tc.s)
			if w.Retain != tc.retain || (w.Note != "") != tc.noted || w.Guard.Blocked != tc.blocked {
				t.Fatalf("window = {%v note=%q blocked=%v}, want {%v noted=%v blocked=%v}", w.Retain, w.Note, w.Guard.Blocked, tc.retain, tc.noted, tc.blocked)
			}
			if tc.blocked && w.Guard.Oldest.IsZero() {
				t.Fatal("a blocked window must carry the oldest partition")
			}
			if met := mock.ExpectationsWereMet() == nil; met != tc.asked {
				t.Fatalf("index asked = %v, want %v", met, tc.asked)
			}
		})
	}
}

// The DSN form sits on the daemons' boot path: an index it cannot open must
// answer what those runs assumed before they asked, never an error or a
// different window.
func TestResolveWindowDSN_bestEffort(t *testing.T) {
	implicit := Settings{Enabled: true, Retain: 48 * time.Hour, RetainRaw: "48h", Interval: time.Hour}
	for name, dsn := range map[string]string{
		"unparsable DSN":   "not a dsn",
		"no database name": "root:x@tcp(127.0.0.1:1)/",
		"nothing answers":  "root:x@tcp(127.0.0.1:1)/binlog_index",
	} {
		if w := ResolveWindowDSN(context.Background(), dsn, implicit); w.Retain != 48*time.Hour || w.Note != "" || w.Guard.Blocked {
			t.Errorf("%s: window = %+v, want the settings' own 48h, no note, no guard", name, w)
		}
	}
	if w := ResolveWindowDSN(context.Background(), "root:x@tcp(127.0.0.1:1)/binlog_index", Settings{}); w.Retain != 0 {
		t.Errorf("rotation off: window = %+v, want zero", w)
	}
}
