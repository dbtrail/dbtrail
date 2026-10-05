//go:build integration

package consoleapp

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2085 with real GTID sets: the watermark over the reporter's own reads of a
// MySQL source in GTID mode (its executed set, its binary log's filters) and
// of a real stream_state, with nothing replaced but the clock.
//
// The index must not be on the source server (an index there is never
// compared), so its stream_state lives on the MariaDB test server, which has
// no server_uuid to be confused with the source's. The capture's saved
// position is written by hand: what is under test is what the reporter makes
// of the two sets, not the stream.
func TestIntegrationCaptureWatermark_realGTIDSets_2085(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	testutil.SkipIfNoMariaDB(t)
	ctx := context.Background()
	src, _ := testutil.CreateTestDB(t)
	if !stepGTIDModeOnForProbe(t, src) {
		return
	}
	idx, idxName := testutil.CreateTestMariaDB(t)
	testutil.MustExec(t, idx, `CREATE TABLE stream_state (
		id INT UNSIGNED PRIMARY KEY DEFAULT 1, mode ENUM('position','gtid') NOT NULL,
		binlog_file VARCHAR(255) NOT NULL DEFAULT '', binlog_position BIGINT UNSIGNED NOT NULL DEFAULT 0,
		gtid_set TEXT DEFAULT NULL, flavor VARCHAR(16) NOT NULL DEFAULT 'mysql', events_indexed BIGINT UNSIGNED NOT NULL DEFAULT 0,
		last_event_time DATETIME DEFAULT NULL, last_checkpoint DATETIME NOT NULL, server_id INT UNSIGNED NOT NULL,
		bintrail_id CHAR(36) NULL DEFAULT NULL, gap_lost_at DATETIME DEFAULT NULL, gap_lost_detail TEXT DEFAULT NULL,
		capture_skips TEXT DEFAULT NULL)`)
	saved := func(gtidSet string) {
		t.Helper()
		testutil.MustExec(t, idx, `REPLACE INTO stream_state (id, mode, binlog_file, binlog_position, gtid_set, last_checkpoint, server_id, capture_skips)
			VALUES (1, 'gtid', 'binlog.000001', 4, ?, UTC_TIMESTAMP(), 1, '{}')`, gtidSet)
	}
	n := 0
	write := func() string {
		t.Helper()
		n++
		testutil.MustExec(t, src, "CREATE TABLE wm_"+string(rune('a'+n))+" (id INT PRIMARY KEY)")
		executed, err := readExecutedGTIDs(ctx, src)
		if err != nil || executed == "" {
			t.Fatalf("the source's executed set = %q, %v", executed, err)
		}
		return executed
	}

	now := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	c := newCaptureStatusReporter("")
	c.now = func() time.Time { return now }
	e := console.ServerEntry{ID: "a1", Name: "prod", DSN: testutil.MariaDBBaseDSN() + "/" + idxName, SourceDSN: testutil.BaseDSN() + "/"}
	ask := func() console.CaptureWatermark {
		t.Helper()
		now = now.Add(captureStatusTTL + time.Second) // past the last answer's lifetime: a real read
		return c.CaptureWatermark(ctx, e)
	}

	// The server is shared with other tests, which write to it too: what the
	// source has executed is read from it each time, never assumed.
	executedNow := func() string {
		t.Helper()
		executed, err := readExecutedGTIDs(ctx, src)
		if err != nil || executed == "" {
			t.Fatalf("the source's executed set = %q, %v", executed, err)
		}
		return executed
	}

	// The source is ahead of the saved position: nothing proven yet.
	saved(write())
	write()
	write()
	first := ask()
	firstAsked := now
	if !first.Through.IsZero() || !strings.HasPrefix(first.Detail, "the source is ahead") {
		t.Fatalf("source ahead on a first read: %+v, want no watermark and why", first)
	}
	// Capture reaches what the source had at that read (and whatever it has
	// executed since), and the source moves on again: complete as of the
	// FIRST read, not of this one.
	saved(executedNow())
	write()
	write()
	second := ask()
	if !second.Through.Equal(firstAsked) {
		t.Fatalf("after capture reached the first sample: through = %v (%s), want the instant of the first read %v", second.Through, second.Detail, firstAsked)
	}
	// Capture holds everything the source has: complete as of this read.
	// Another test may write between the save and the read, so a few tries.
	var third console.CaptureWatermark
	for try := 0; try < 10 && !third.Through.Equal(now); try++ {
		saved(executedNow())
		third = ask()
	}
	if !third.Through.Equal(now) {
		t.Fatalf("with equal sets: through = %v (%s), want the instant of this read %v", third.Through, third.Detail, now)
	}
	latest := executedNow()
	// A saved position with a transaction the source never executed: the
	// source was reset or replaced, and nothing proven about the old one
	// holds.
	saved(latest + ",3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5")
	if fourth := ask(); !fourth.Through.IsZero() || !strings.Contains(fourth.Detail, "the source does not have") {
		t.Fatalf("capture holding more than the source: %+v, want no watermark", fourth)
	}
}
