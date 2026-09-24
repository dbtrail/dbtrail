//go:build integration

package consoleapp

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #1791 against a real server: the index's real stream_state read through
// status.LoadStreamState, and the source's real variables, so the probe's
// queries are read from the other side of the seam rather than from a mock
// written to match them. One server plays both roles here, which is itself
// one of the cases: an index on its source server is never compared, since
// its own checkpoint writes keep the source ahead of any capture. The
// caught-up path needs two servers; see the PR for that run.
func TestIntegrationProbeCapture(t *testing.T) {
	db, name := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	indexDSN := testutil.DefaultDSN + "/" + name
	sourceDSN := testutil.DefaultDSN + "/"
	ctx := context.Background()
	anchor := time.Now().Add(-3 * time.Hour)

	probe := func() captureProbeResult {
		t.Helper()
		return captureFromDBs(ctx, indexDSN, sourceDSN, anchor, time.Time{})
	}
	// A file-mode index: no stream_state row.
	if r := probe(); r.verdict != "" || r.detail != "the index has no live capture on record" {
		t.Fatalf("no capture on record: %+v", r)
	}
	checkpoint := func(mode string, gtid any) {
		t.Helper()
		testutil.MustExec(t, db, `REPLACE INTO stream_state (id, mode, binlog_file, binlog_position, gtid_set, flavor, last_checkpoint, server_id, capture_skips)
			VALUES (1, ?, 'binlog.000001', 4, ?, 'mysql', UTC_TIMESTAMP(), 1, '{}')`, mode, gtid)
	}
	// A position-mode capture: settled from the index alone.
	checkpoint("position", nil)
	if r := probe(); r.verdict != "" || r.detail != "the capture runs in binlog-position mode, which is not compared" {
		t.Fatalf("position mode: %+v", r)
	}
	// Rows dropped an hour before the anchor, and no full backup on record:
	// dated against the last read of the source (none), not the anchor.
	testutil.MustExec(t, db, `REPLACE INTO stream_state (id, mode, binlog_file, binlog_position, gtid_set, flavor, last_checkpoint, server_id, capture_skips)
		VALUES (1, 'gtid', 'binlog.000001', 4, ?, 'mysql', UTC_TIMESTAMP(), 1, ?)`,
		"3e11fa47-71ca-11e1-9e33-c80aa9429562:1-10",
		`{"column_count_mismatch":{"count":3,"last_at":"`+anchor.Add(-time.Hour).UTC().Format(time.RFC3339)+`"}}`)
	if r := probe(); r.verdict != "" || r.detail != "the capture dropped events that no full read has read from the source since" {
		t.Fatalf("rows dropped before an update anchor: %+v", r)
	}
	// A GTID capture whose index is on the source server.
	checkpoint("gtid", "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-10")
	if r := probe(); r.verdict != "" || !strings.HasPrefix(r.detail, "the index lives on the source server") {
		t.Fatalf("index on the source server: %+v", r)
	}
	// A capture gap is dated against the anchor: one before it passes to the
	// source question (and stops at the same-server answer), one after it
	// refuses from the index alone.
	for _, c := range []struct {
		lost time.Time
		want string
	}{
		{anchor.Add(-time.Hour), "the index lives on the source server"},
		{anchor.Add(time.Hour), "the capture lost events to a binlog gap after the previous snapshot"},
	} {
		testutil.MustExec(t, db, "UPDATE stream_state SET gap_lost_at = ?, gap_lost_detail = 'purged' WHERE id = 1", c.lost.UTC())
		if r := probe(); r.verdict != "" || !strings.HasPrefix(r.detail, c.want) {
			t.Fatalf("a gap at %s against the anchor %s: %+v, want %q", c.lost.UTC(), anchor.UTC(), r, c.want)
		}
	}
	testutil.MustExec(t, db, "UPDATE stream_state SET gap_lost_at = NULL, gap_lost_detail = NULL WHERE id = 1")

	// The source's GTID read, for real: empty with GTIDs off, a
	// whitespace-free set that compares with itself once they are on.
	executed, err := readExecutedGTIDs(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	var mode string
	if err := db.QueryRow("SELECT @@GLOBAL.gtid_mode").Scan(&mode); err != nil {
		t.Fatal(err)
	}
	if !strings.EqualFold(mode, "ON") && executed != "" {
		t.Fatalf("gtid_mode=%s but the read returned %q, want empty", mode, executed)
	}
	if !stepGTIDModeOnForProbe(t, db) {
		return
	}
	testutil.MustExec(t, db, "CREATE TABLE owns_a_gtid (id INT PRIMARY KEY)")
	before, err := readExecutedGTIDs(ctx, db)
	if err != nil || before == "" || strings.ContainsAny(before, " \n\t") {
		t.Fatalf("executed set with GTIDs on = %q, %v; want a non-empty set with no whitespace", before, err)
	}
	if v, d := compareGTIDSets(before, before); v != console.CaptureCaughtUp {
		t.Fatalf("the server's own set against itself: %q (%s)", v, d)
	}
	testutil.MustExec(t, db, "CREATE TABLE owns_another (id INT PRIMARY KEY)")
	after, err := readExecutedGTIDs(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if v, d := compareGTIDSets(before, after); v != console.CaptureBehind {
		t.Fatalf("a capture holding the set from before a write: %q (%s), want behind", v, d)
	}
	if v, d := compareGTIDSets(after, before); v != "" {
		t.Fatalf("a capture holding more than the source (a reset): %q (%s), want unknown", v, d)
	}
}

// stepGTIDModeOnForProbe is internal/streamrun's stepGTIDModeOn (the
// online OFF → ON climb, walked back on cleanup), which is a test helper of
// that package and cannot be imported. It reports false, with a log line,
// when the server refuses the climb; with BINTRAIL_REQUIRE_MYSQL=1 that is a
// failure instead, so CI cannot pass this part by skipping it.
func stepGTIDModeOnForProbe(t *testing.T, db *sql.DB) bool {
	t.Helper()
	refuse := func(format string, args ...any) bool {
		t.Helper()
		if testutil.MySQLRequired() {
			t.Fatalf(format, args...)
		}
		t.Logf(format, args...)
		return false
	}
	var mode, enforce string
	if err := db.QueryRow("SELECT @@GLOBAL.gtid_mode, @@GLOBAL.enforce_gtid_consistency").Scan(&mode, &enforce); err != nil {
		return refuse("cannot read gtid_mode: %v", err)
	}
	if strings.EqualFold(mode, "ON") {
		return true
	}
	if !strings.EqualFold(mode, "OFF") {
		return refuse("gtid_mode=%s: not a state this test steps from", mode)
	}
	if _, err := db.Exec("SET GLOBAL enforce_gtid_consistency = ON"); err != nil {
		return refuse("SET GLOBAL enforce_gtid_consistency refused: %v", err)
	}
	t.Cleanup(func() {
		for _, m := range []string{"ON_PERMISSIVE", "OFF_PERMISSIVE", "OFF"} {
			db.Exec("SET GLOBAL gtid_mode = " + m)
		}
		db.Exec("SET GLOBAL enforce_gtid_consistency = " + enforce)
		var restored string
		if err := db.QueryRow("SELECT @@GLOBAL.gtid_mode").Scan(&restored); err == nil && !strings.EqualFold(restored, "OFF") {
			t.Logf("WARNING: could not step gtid_mode back down (still %s); later position-mode tests on this server may misbehave", restored)
		}
	})
	for _, m := range []string{"OFF_PERMISSIVE", "ON_PERMISSIVE", "ON"} {
		if _, err := db.Exec("SET GLOBAL gtid_mode = " + m); err != nil {
			return refuse("SET GLOBAL gtid_mode = %s refused: %v", m, err)
		}
	}
	return true
}
