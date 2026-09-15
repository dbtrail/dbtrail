//go:build integration

package reconstruct

import (
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestEventDecoder_snapshotReadAfterALaterDDL is #1667: a snapshot reads the
// live schema, so one taken after a second DDL already holds it. Dating it at
// its own DDL would decode the events between the two with the later
// definition: a VARCHAR value written before a change to TEXT would be
// base64-decoded into garbage.
func TestEventDecoder_snapshotReadAfterALaterDDL(t *testing.T) {
	base := time.Date(2026, 9, 15, 10, 0, 0, 0, time.UTC)
	st := func(d time.Duration) string { return base.Add(d).Format("2006-01-02 15:04:05") }
	run := func(t *testing.T, snaps []struct {
		id       int
		taken    time.Duration
		ctype    string
		detected *time.Duration
	}, eventAt time.Duration) any {
		db, _ := testutil.CreateTestDB(t)
		testutil.InitIndexTables(t, db)
		for _, s := range snaps {
			testutil.InsertSnapshot(t, db, s.id, st(s.taken), "shop", "t", "id", 1, "PRI", "int", "NO")
			testutil.InsertSnapshot(t, db, s.id, st(s.taken), "shop", "t", "c", 2, "", s.ctype, "YES")
			if s.detected != nil {
				testutil.MustExec(t, db, `INSERT INTO schema_changes
					(detected_at, binlog_file, binlog_pos, schema_name, table_name, ddl_type, ddl_query, snapshot_id)
					VALUES (?, 'binlog.000001', 4, 'shop', 'x', 'ALTER TABLE', 'x', ?)`, st(*s.detected), s.id)
			}
		}
		dec := newEventDecoder(db, "shop", "t", nil)
		ev := []query.ResultRow{{EventTimestamp: base.Add(eventAt), RowAfter: map[string]any{"id": 1, "c": "test"}}}
		dec.decodeBinaries(ev)
		return ev[0].RowAfter["c"]
	}
	d := func(x time.Duration) *time.Duration { return &x }
	type sn = struct {
		id       int
		taken    time.Duration
		ctype    string
		detected *time.Duration
	}

	t.Run("capture lag spans two DDLs", func(t *testing.T) {
		// S1 varchar era. D1 (other table) at +10s, D2 (c VARCHAR->TEXT) at +5m.
		// Capture reaches D1 at +10m: S2 reads live schema, c already TEXT.
		// S3 for D2 at +10m01s. Row with c='test' (VARCHAR, stored plain) at +1m.
		got := run(t, []sn{
			{1, -2 * time.Hour, "varchar", nil},
			{2, 10 * time.Minute, "text", d(10 * time.Second)},
			{3, 10*time.Minute + time.Second, "text", d(5 * time.Minute)},
		}, time.Minute)
		t.Logf("c = %q", got)
		if got != "test" {
			t.Errorf("plain VARCHAR value %q was base64-decoded into %q", "test", got)
		}
	})
	t.Run("file-mode backfill of an old binlog", func(t *testing.T) {
		// S1 at -30d varchar. S2 at -1d for D (c VARCHAR->TEXT) detected -1d.
		// Operator re-indexes an old binlog (DDL on another table at -20d) with
		// --source-dsn today: S3 = today's schema (TEXT), detected -20d.
		// Row c='test' at -15d.
		got := run(t, []sn{
			{1, -30 * 24 * time.Hour, "varchar", nil},
			{2, -24 * time.Hour, "text", d(-24 * time.Hour)},
			{3, 0, "text", d(-20 * 24 * time.Hour)},
		}, -15*24*time.Hour)
		t.Logf("c = %q", got)
		if got != "test" {
			t.Errorf("plain VARCHAR value %q was base64-decoded into %q", "test", got)
		}
	})
}
