//go:build integration

package reconstruct_test

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/rotation"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// archivedWindow is the #2186 rig: a snapshot at H+5m whose event mark is
// event 10 ending at binlog.000007:200, the read's window [H+5m, H+30m] of
// shop.orders, and the hours H-1 to H+1 rotated into Parquet archives (real
// rotation, local files) and dropped from binlog_events, so the live check
// sees none of the window.
type archivedWindow struct {
	db     *sql.DB
	dbName string
	h      time.Time
	window reconstruct.ReadWindow
	anchor *query.BinlogPos
	mark   string
}

const archivedBintrailID = "21860000-dead-beef-dead-beefdeadbeef"

// newArchivedWindow seeds the mark, one change after the snapshot in the same
// numbering, the events extra adds, and (unless nothingLive) one recent event
// that stays live; then rotates.
func newArchivedWindow(t *testing.T, nothingLive bool, extra func(db *sql.DB, schema string, h time.Time)) *archivedWindow {
	t.Helper()
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	now := time.Now().UTC()
	h := now.Add(-72 * time.Hour).Truncate(time.Hour)
	recent := now.Add(-time.Hour).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h.Add(-time.Hour), h, h.Add(time.Hour), recent, recent.Add(time.Hour)})
	markStreamCaptured(t, db)
	insertEventAt(t, db, "shop", "orders", "binlog.000006", 5, 100, h.Add(-30*time.Minute), "9", `{"id":9,"status":"x"}`)
	insertEventAt(t, db, "shop", "orders", "binlog.000007", 10, 100, h.Add(4*time.Minute), "1", `{"id":1,"status":"A"}`)
	insertEventAt(t, db, "shop", "orders", "binlog.000007", 11, 300, h.Add(6*time.Minute), "2", `{"id":2,"status":"B"}`)
	if extra != nil {
		extra(db, "shop", h)
	}
	if !nothingLive {
		insertEventAt(t, db, "shop", "orders", "binlog.000007", 50, 9000, now.Add(-30*time.Minute), "3", `{"id":3,"status":"C"}`)
	}
	if _, err := rotation.Perform(context.Background(), db, dbName, rotation.Options{
		RetainDur: 48 * time.Hour, RetainRaw: "48h", ArchiveDir: t.TempDir(), ArchiveCompression: "zstd",
		BintrailID: archivedBintrailID, Format: "json",
	}); err != nil {
		t.Fatalf("rotation.Perform: %v", err)
	}
	var live int
	if err := db.QueryRow(`SELECT COUNT(*) FROM binlog_events WHERE event_timestamp < ?`, h.Add(2*time.Hour)).Scan(&live); err != nil {
		t.Fatal(err)
	}
	if live != 0 {
		t.Fatalf("fixture: %d events of the window are still live; rotation must have archived and dropped them", live)
	}
	return &archivedWindow{
		db: db, dbName: dbName, h: h,
		window: reconstruct.ReadWindow{Schema: "shop", Table: "orders", Since: h.Add(5 * time.Minute), Until: h.Add(30 * time.Minute)},
		anchor: &query.BinlogPos{File: "binlog.000007", Pos: 200},
		mark:   reconstruct.EventMark{ID: 10, File: "binlog.000007", End: 200}.Encode(),
	}
}

func (a *archivedWindow) check(t *testing.T) (string, error) {
	t.Helper()
	return reconstruct.CheckNumberingFromRead(context.Background(), a.db, a.anchor, a.mark, a.window)
}

func (a *archivedWindow) archiveRow(t *testing.T, hour time.Time) (local string) {
	t.Helper()
	if err := a.db.QueryRow(`SELECT local_path FROM archive_state WHERE partition_name = ?`, indexer.PartitionName(hour)).Scan(&local); err != nil {
		t.Fatalf("archive_state row of %s: %v", indexer.PartitionName(hour), err)
	}
	return local
}

func TestCheckNumbering_archivedWindow_2186(t *testing.T) {
	renumbered := func(db *sql.DB, schema string, h time.Time) {
		insertEventAt(t, db, schema, "orders", "binlog.000001", 12, 100, h.Add(7*time.Minute), "1", `{"id":1,"status":"Z"}`)
	}
	for _, tc := range []struct {
		name        string
		nothingLive bool
		extra       func(db *sql.DB, schema string, h time.Time)
		// after edits the index after rotation.
		after  func(t *testing.T, a *archivedWindow)
		refuse bool
		note   string
	}{
		{name: "steady state: archives written before and after the snapshot"},
		{name: "a late change after the snapshot in an hour before it, in the same numbering",
			extra: func(db *sql.DB, schema string, h time.Time) {
				insertEventAt(t, db, schema, "orders", "binlog.000007", 12, 500, h.Add(-20*time.Minute), "4", `{"id":4,"status":"L"}`)
			}},
		{name: "the numbering started over inside the archived window", extra: renumbered, refuse: true},
		{name: "the same, with nothing live at all", nothingLive: true, extra: renumbered, refuse: true},
		{name: "the numbering started over in another table only",
			extra: func(db *sql.DB, schema string, h time.Time) {
				insertEventAt(t, db, schema, "customers", "binlog.000001", 12, 100, h.Add(7*time.Minute), "1", `{"id":1}`)
			}},
		{name: "the numbering started over after the window's end",
			extra: func(db *sql.DB, schema string, h time.Time) {
				insertEventAt(t, db, schema, "orders", "binlog.000001", 12, 100, h.Add(50*time.Minute), "1", `{"id":1,"status":"Z"}`)
			}},
		{name: "an archive with no record of its newest event is read",
			extra: renumbered, refuse: true,
			after: func(t *testing.T, a *archivedWindow) {
				testutil.MustExec(t, a.db, `UPDATE archive_state SET max_event_id = NULL, max_binlog_file = NULL, max_start_pos = NULL`)
			}},
		{name: "an archive holding nothing after the mark is not read",
			after: func(t *testing.T, a *archivedWindow) {
				// H-1 holds only events 5 (and nothing after the mark): its file
				// may be gone without the check needing it.
				if err := os.Remove(a.archiveRow(t, a.h.Add(-time.Hour))); err != nil {
					t.Fatal(err)
				}
			}},
		// #2186 review: a restarted capture's resume cleanup deleted the mark
		// and captured its events again under new ids, at their old positions
		// (before the mark's end); then rotation archived the hour. That is
		// not a numbering that started over: the archives no longer hold the
		// mark, so the check cannot vouch for it and says so.
		{name: "the mark deleted by a resume cleanup, replayed, then archived: cannot check, no refusal",
			extra: func(db *sql.DB, schema string, h time.Time) {
				testutil.MustExec(t, db, `DELETE FROM binlog_events WHERE event_id = 10`)
				insertEventAt(t, db, schema, "orders", "binlog.000007", 13, 50, h.Add(4*time.Minute), "1", `{"id":1,"status":"A"}`)
				insertEventAt(t, db, schema, "orders", "binlog.000007", 14, 100, h.Add(4*time.Minute), "1", `{"id":1,"status":"A"}`)
			},
			note: "was deleted from the index"},
		{name: "an archive the window needs whose file is gone: cannot check",
			after: func(t *testing.T, a *archivedWindow) {
				if err := os.Remove(a.archiveRow(t, a.h)); err != nil {
					t.Fatal(err)
				}
			},
			note: "the archived hour " + "p_"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := newArchivedWindow(t, tc.nothingLive, tc.extra)
			if tc.after != nil {
				tc.after(t, a)
			}
			note, err := a.check(t)
			switch {
			case tc.refuse:
				if !errors.Is(err, reconstruct.ErrBinlogRenumbered) || !strings.Contains(err.Error(), "binlog.000001:200") {
					t.Fatalf("= %q, %v; want ErrBinlogRenumbered naming binlog.000001:200", note, err)
				}
			case err != nil:
				t.Fatalf("err = %v", err)
			case tc.note == "" && note != "":
				t.Fatalf("note = %q; want none", note)
			case tc.note != "" && !strings.Contains(note, tc.note):
				t.Fatalf("note = %q; want it to say %q", note, tc.note)
			case strings.HasPrefix(tc.note, "the archived hour") && !strings.Contains(note, indexer.PartitionName(a.h)):
				t.Fatalf("note = %q; want it to name %s", note, indexer.PartitionName(a.h))
			}
		})
	}
}

// #2186 under an explicit --at: a check that could not tell is said on the
// run's output; a refresh keeps its own log line only.
func TestReconstructAt_uncheckedNumberingWarns_2186(t *testing.T) {
	r, T := renumberRig(t, false, false)
	insertEventAt(t, r.db, r.schema, "orders", "binlog.000007", 15, 300, T.Add(10*time.Minute), "2", `{"id":2,"status":"X"}`)
	testutil.MustExec(t, r.db, `INSERT INTO index_state (binlog_file, file_size, last_position, events_indexed, status, started_at, completed_at)
		VALUES ('binlog.000005', 1, 150, 1, 'completed', UTC_TIMESTAMP(), UTC_TIMESTAMP())`)
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	for _, explicit := range []bool{true, false} {
		buf.Reset()
		dump, err := r.explicitAt(t, T.Add(20*time.Minute), explicit)
		if err != nil {
			t.Fatalf("explicit=%v: %v", explicit, err)
		}
		holds(t, dump, []string{"X"}, nil)
		said := strings.Contains(buf.String(), "binlog numbering not checked")
		if said != explicit {
			t.Errorf("explicit=%v: warned %v; log: %s", explicit, said, buf.String())
		}
		if explicit && !strings.Contains(buf.String(), "bintrail index") {
			t.Errorf("the warning does not name the cause: %s", buf.String())
		}
	}
}
