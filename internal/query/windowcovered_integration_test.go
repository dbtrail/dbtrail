//go:build integration

package query

import (
	"context"
	"path/filepath"
	"strings"
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
			got, err := WindowCovered(ctx, db, dbName, tc.since, tc.until, ScopeFromPaths(nil))
			if err != nil {
				t.Fatalf("WindowCovered: %v", err)
			}
			if got != tc.want {
				t.Errorf("WindowCovered(%s, %s) = %v, want %v", tc.since.Format(time.RFC3339), tc.until.Format(time.RFC3339), got, tc.want)
			}
		})
	}

	// No database name → nothing to classify against → never "contiguous".
	if got, err := WindowCovered(ctx, db, "", h, h.Add(time.Minute), ScopeFromPaths(nil)); err != nil || got {
		t.Errorf("unclassifiable window must report false: got %v, %v", got, err)
	}
	// An index with only p_future (no explicit hourly partition) cannot place
	// its horizon, so it is never "contiguous" either.
	db3, dbName3 := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db3)
	if got, err := WindowCovered(ctx, db3, dbName3, h, h.Add(time.Minute), ScopeFromPaths(nil)); err != nil || got {
		t.Errorf("no explicit partition must report false: got %v, %v", got, err)
	}
	// A closed handle → an error, never a silent false.
	db2, _ := testutil.CreateTestDB(t)
	db2.Close()
	if _, err := WindowCovered(ctx, db2, dbName, h, h.Add(time.Minute), ScopeFromPaths(nil)); err == nil {
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
	if got, err := WindowCovered(ctx, db, dbName, since, until, OnlyArchives("bt")); err != nil || !got {
		t.Errorf("archived h+1 and live h+2 cover the window for a scan that opens bt: %v, %v", got, err)
	}
	if got, err := WindowCovered(ctx, db, dbName, since, until, ScopeFromPaths(nil)); err != nil || got {
		t.Errorf("archived h+1 is a gap for a scan that opens no archive: %v, %v", got, err)
	}
	if got, err := WindowCovered(ctx, db, dbName, since, until, OnlyArchives("other")); err != nil || got {
		t.Errorf("an archive the scan does not open is not coverage (#1232): %v, %v", got, err)
	}
	if got, err := WindowCovered(ctx, db, dbName, h.Add(10*time.Minute), until, AllArchives()); err != nil || got {
		t.Errorf("hour h is held by nothing: %v, %v", got, err)
	}
	// archive_state present but unreadable → an error, never a silent verdict.
	testutil.MustExec(t, db, `ALTER TABLE archive_state RENAME COLUMN partition_name TO partition_nam3`)
	if _, err := WindowCovered(ctx, db, dbName, since, until, AllArchives()); err == nil {
		t.Errorf("an unreadable archive_state must surface as an error")
	}
	if got, err := WindowCovered(ctx, db, dbName, h.Add(2*time.Hour+10*time.Minute), until, ScopeFromPaths(nil)); err != nil || !got {
		t.Errorf("a scan opening no archive never reads archive_state: %v, %v", got, err)
	}
}

// TestMergedFetcher_gapNote pins the advisory gap report: hours inside a
// scanned window that neither the live index nor an opened archive holds
// are collected across scans and rendered once, in the posture of the run.
func TestMergedFetcher_gapNote(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	ctx := context.Background()
	h := time.Now().UTC().Add(-6 * time.Hour).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h.Add(2 * time.Hour)}) // live: h+2
	testutil.MustExec(t, db, `INSERT INTO archive_state (bintrail_id, partition_name, local_path)
		VALUES ('bt', ?, ?)`, "p_"+h.Add(time.Hour).Format("2006010215"), filepath.Join(t.TempDir(), "bintrail_id=bt", "x.parquet")) // archived: h+1 (file absent: the fetch of it fails, so use a no-op fetcher)
	noop := func(context.Context, Options, string) ([]ResultRow, error) { return nil, nil }

	m := &MergedFetcher{DB: db, Engine: New(db), DBName: dbName, ArchiveFetcher: noop}
	since, until := h.Add(10*time.Minute), h.Add(2*time.Hour+10*time.Minute)
	if _, err := m.Fetch(ctx, Options{Schema: "s", Table: "t", Since: &since, Until: &until}); err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if gaps := m.GapHours(); len(gaps) != 1 || !gaps[0].Equal(h) {
		t.Fatalf("only hour h is held by nothing (h+1 archived, h+2 live); got %v", gaps)
	}
	note := m.GapNote()
	if !strings.Contains(note, "1 hour(s)") || !strings.Contains(note, "rotated out with no archive") || strings.Contains(note, "excluded") {
		t.Errorf("note must name the hour and the archive-reading posture: %q", note)
	}
	if !strings.Contains(note, "before the index existed") {
		t.Errorf("hour h predates the oldest hour the index ever held; the note must say so: %q", note)
	}
	if strings.Contains(strings.Join(m.Notes(), " "), "registered archives were not read") {
		t.Errorf("this scan READ the archive (the hour is not live-covered); no elision note: %v", m.Notes())
	}
	// A second scan over an already-known gap does not double count.
	if _, err := m.Fetch(ctx, Options{Schema: "s", Table: "t", Since: &since, Until: &until}); err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if gaps := m.GapHours(); len(gaps) != 1 {
		t.Errorf("gap hours are a set, got %v", gaps)
	}

	confined := &MergedFetcher{DB: db, Engine: New(db), DBName: dbName, NoArchive: true}
	if _, err := confined.Fetch(ctx, Options{Schema: "s", Table: "t", Since: &since, Until: &until}); err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if gaps := confined.GapHours(); len(gaps) != 2 {
		t.Errorf("under NoArchive the archived hour is a gap too; got %v", gaps)
	}
	if note := confined.GapNote(); !strings.Contains(note, "excluded on this run") || strings.Contains(note, "before the index existed") ||
		!strings.Contains(note, h.Format("2006-01-02 15:04")) || !strings.Contains(note, h.Add(time.Hour).Format("2006-01-02 15:04")) {
		t.Errorf("NoArchive posture and the first–last range must be named: %q", note)
	}

	// Elision (#1353): a window the live index provably satisfies leaves the
	// registered archive unread, and Notes says so.
	elider := &MergedFetcher{DB: db, Engine: New(db), DBName: dbName, ArchiveFetcher: noop}
	liveSince, liveUntil := h.Add(2*time.Hour+5*time.Minute), h.Add(2*time.Hour+20*time.Minute)
	if _, err := elider.Fetch(ctx, Options{Schema: "s", Table: "t", Since: &liveSince, Until: &liveUntil}); err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if notes := strings.Join(elider.Notes(), " "); !strings.Contains(notes, "registered archives were not read") || strings.Contains(notes, "not held") {
		t.Errorf("a live-satisfied window must report elision and no gap: %v", elider.Notes())
	}
	if (&MergedFetcher{DB: db, Engine: New(db), DBName: dbName, NoArchive: true}).GapNote() != "" {
		t.Errorf("no scan, no note")
	}
}
