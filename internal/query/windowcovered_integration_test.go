//go:build integration

package query

import (
	"context"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestWindowCovered pins the three answers the cascade gate keys on
// (#1615): a window fully inside the live partitions is contiguous; a window
// with one rotated-out hour is not; hours past the newest explicit partition
// are live (p_future); and an index that cannot be classified never answers
// "contiguous".
func TestWindowCovered(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	ctx := context.Background()

	h := time.Now().UTC().Add(-6 * time.Hour).Truncate(time.Hour)
	// Live hours: h, h+1, h+2 — then a hole at h+3 — then h+4.
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h, h.Add(time.Hour), h.Add(2 * time.Hour), h.Add(4 * time.Hour)})

	cases := []struct {
		name         string
		since, until time.Time
		want         bool
	}{
		{"inside live hours", h.Add(10 * time.Minute), h.Add(2*time.Hour + 50*time.Minute), true},
		{"single live hour", h.Add(10 * time.Minute), h.Add(14 * time.Minute), true},
		{"window crosses the hole", h.Add(2*time.Hour + 10*time.Minute), h.Add(4*time.Hour + 10*time.Minute), false},
		{"window starts before the oldest live hour", h.Add(-time.Hour), h.Add(10 * time.Minute), false},
		// Past the newest explicit partition the rows live in p_future, which is
		// never rotated: contiguous, not a gap (a lapsed add-future horizon must
		// not cost every cascade its Phase-2).
		{"window ends after the newest live hour", h.Add(4 * time.Hour), h.Add(5*time.Hour + 10*time.Minute), true},
		{"window entirely past the horizon", h.Add(6 * time.Hour), h.Add(7 * time.Hour), true},
		{"hole then horizon", h.Add(2*time.Hour + 10*time.Minute), h.Add(6 * time.Hour), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := WindowCovered(ctx, db, dbName, tc.since, tc.until, true)
			if err != nil {
				t.Fatalf("WindowCovered: %v", err)
			}
			if got != tc.want {
				t.Errorf("WindowCovered(%s, %s) = %v, want %v", tc.since.Format(time.RFC3339), tc.until.Format(time.RFC3339), got, tc.want)
			}
		})
	}

	// No database name → nothing to classify against → never "contiguous".
	if got, err := WindowCovered(ctx, db, "", h, h.Add(time.Minute), true); err != nil || got {
		t.Errorf("unclassifiable window must report false: got %v, %v", got, err)
	}
	// An index with only p_future (no explicit hourly partition) cannot place
	// its horizon, so it is never "contiguous" either.
	db3, dbName3 := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db3)
	if got, err := WindowCovered(ctx, db3, dbName3, h, h.Add(time.Minute), true); err != nil || got {
		t.Errorf("no explicit partition must report false: got %v, %v", got, err)
	}
	// A closed handle → an error, never a silent false.
	db2, _ := testutil.CreateTestDB(t)
	db2.Close()
	if _, err := WindowCovered(ctx, db2, dbName, h, h.Add(time.Minute), true); err == nil {
		t.Errorf("unreadable partition list must surface as an error")
	}
}

// TestWindowCovered_archiveCredit pins the noArchive mirror: an hour held only
// by a registered archive is covered for a scan that reads archives and a gap
// for one that does not.
func TestWindowCovered_archiveCredit(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	ctx := context.Background()
	h := time.Now().UTC().Add(-6 * time.Hour).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h.Add(2 * time.Hour)}) // live: h+2 only
	testutil.MustExec(t, db, `INSERT INTO archive_state (bintrail_id, partition_name, local_path)
		VALUES ('bt', ?, '/nonexistent/bintrail_id=bt/x.parquet')`, "p_"+h.Add(time.Hour).Format("2006010215")) // archived: h+1
	since, until := h.Add(time.Hour+10*time.Minute), h.Add(2*time.Hour+10*time.Minute)
	if got, err := WindowCovered(ctx, db, dbName, since, until, false); err != nil || !got {
		t.Errorf("archived h+1 and live h+2 cover the window for an archive-reading scan: %v, %v", got, err)
	}
	if got, err := WindowCovered(ctx, db, dbName, since, until, true); err != nil || got {
		t.Errorf("archived h+1 is a gap for a live-only scan: %v, %v", got, err)
	}
	if got, err := WindowCovered(ctx, db, dbName, h.Add(10*time.Minute), until, false); err != nil || got {
		t.Errorf("hour h is held by nothing: %v, %v", got, err)
	}
}
