package reconstruct

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

// #3084: a run records the id of the newest event only when it took every
// change through it, so that a reader whose cursor is past that id knows it
// has every change the run's snapshot holds. Each other shape of the run's
// end records none.
func TestResolvePGRunEnd_reachID_3084(t *testing.T) {
	at := time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name       string
		newestLSN  uint64 // 0: an empty index
		checkpoint int64  // -1: no row
		found      pgFound
		wantReach  uint64
		wantUntil  uint64
		wantNone   bool
	}{
		{name: "every change taken, the newest transaction complete", newestLSN: 0x8000, checkpoint: 0x8000, wantReach: 100, wantUntil: 0x8000},
		{name: "the checkpoint past the newest change", newestLSN: 0x8000, checkpoint: 0x9000, wantReach: 100, wantUntil: 0x8000},
		// A transaction visible in parts: left whole to the next fold.
		{name: "the newest transaction split", newestLSN: 0x8000, checkpoint: 0x7000, wantReach: 0, wantUntil: 0x7FFF},
		{name: "no saved position", newestLSN: 0x8000, checkpoint: -1, wantReach: 0, wantUntil: 0x7FFF},
		// A change past the point: the cursor passes the run's end by
		// position once it reaches that change.
		{name: "a change past the point", newestLSN: 0x8000, found: pgFound{0x6000, 90}, wantReach: 0, wantUntil: 0x5FFF},
		{name: "an empty index", wantNone: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			mock.ExpectQuery("SELECT pg_caught_up_source_at FROM stream_state").WillReturnRows(sqlmock.NewRows([]string{"pg_caught_up_source_at"}))
			rows := sqlmock.NewRows([]string{"end_pos", "event_timestamp", "event_id"})
			if c.newestLSN != 0 {
				rows.AddRow(c.newestLSN, at.Add(-time.Minute), uint64(100))
			}
			mock.ExpectQuery("ORDER BY event_id DESC LIMIT 1").WillReturnRows(rows)
			if c.wantNone {
				mock.ExpectQuery("FROM archive_state").WillReturnRows(sqlmock.NewRows([]string{"n"}).AddRow(0))
			} else {
				withFirstEventsPast(t, c.found)
				if c.found.lsn == 0 {
					cp := sqlmock.NewRows([]string{"binlog_position"})
					if c.checkpoint >= 0 {
						cp.AddRow(c.checkpoint)
					}
					mock.ExpectQuery("SELECT binlog_position FROM stream_state").WillReturnRows(cp)
				}
			}
			run, err := resolvePGRunEnd(context.Background(), db, at)
			if err != nil {
				t.Fatal(err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
			if c.wantNone {
				if !run.none || run.held != nil || run.reachID != 0 {
					t.Fatalf("an empty index: none %v, held %v, reachID %d; want none, no record, no id", run.none, run.held, run.reachID)
				}
				return
			}
			if run.until != c.wantUntil || run.reachID != c.wantReach || run.held == nil {
				t.Fatalf("until %X reachID %d held %v; want until %X reachID %d and a record", run.until, run.reachID, run.held != nil, c.wantUntil, c.wantReach)
			}
		})
	}
}

// What the run writes, and that it writes nothing when it has nothing to say.
func TestPGRunCutFile_3084(t *testing.T) {
	at := time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)
	newRun := func() *pgRunEnd {
		return &pgRunEnd{at: at, until: 0x7FFF, reachID: 42, held: &pgRunHeld{}}
	}
	snapDir := func(t *testing.T) string {
		d := filepath.Join(t.TempDir(), SnapshotDirName(at))
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
		return d
	}

	t.Run("written and read back", func(t *testing.T) {
		dir := snapDir(t)
		run := newRun()
		run.holdThrough("public", "orders")
		run.holdThrough("public", "items")
		run.holdThrough("public", "orders")
		if err := run.writeCut(dir); err != nil {
			t.Fatal(err)
		}
		cut, found, err := ReadPGRunCut(dir)
		if err != nil || !found {
			t.Fatalf("ReadPGRunCut: found %v, %v", found, err)
		}
		if cut.LSN != 0x8000 || cut.ReachID != 42 || !cut.At.Equal(at) {
			t.Fatalf("read back %+v; want LSN 8000 (the run's end plus one), reach 42, at %s", cut, at)
		}
		if !cut.Holds("public", "orders") || !cut.Holds("public", "items") {
			t.Fatal("a table the run held is not in the record")
		}
		for _, n := range [][2]string{{"public", "other"}, {"Public", "orders"}, {"public", "Orders"}, {"public", "orders "}, {"", ""}, {"public.orders", ""}} {
			if cut.Holds(n[0], n[1]) {
				t.Errorf("%q.%q is in the record, and the run never held it", n[0], n[1])
			}
		}
		if _, err := os.Stat(filepath.Join(dir, PGRunCutFile+".tmp")); !os.IsNotExist(err) {
			t.Errorf("the temporary file is left: %v", err)
		}
	})

	t.Run("nothing to say: no file", func(t *testing.T) {
		held := newRun()
		held.holdThrough("public", "orders")
		none := newRun()
		none.none = true
		none.holdThrough("public", "orders")
		noRecord := &pgRunEnd{at: at, until: 0x7FFF}
		noRecord.holdThrough("public", "orders")
		for name, c := range map[string]struct {
			run *pgRunEnd
			dir func(t *testing.T) string
		}{
			// A MySQL run has no pgRun at all.
			"no run (a MySQL source)":          {nil, snapDir},
			"an index with no change":          {none, snapDir},
			"a run with no record":             {noRecord, snapDir},
			"no table held through the end":    {newRun(), snapDir},
			"a directory whose name is no time": {held, func(t *testing.T) string { return t.TempDir() }},
			"no directory":                     {held, func(*testing.T) string { return "" }},
		} {
			t.Run(name, func(t *testing.T) {
				dir := c.dir(t)
				if err := c.run.writeCut(dir); err != nil {
					t.Fatal(err)
				}
				if dir == "" {
					return
				}
				if _, found, err := ReadPGRunCut(dir); found || err != nil {
					t.Fatalf("a record was written: found %v, %v", found, err)
				}
			})
		}
	})

	t.Run("a record that does not read is an error, never a cut", func(t *testing.T) {
		for name, body := range map[string]string{
			"not JSON":      "{",
			"a new version": `{"version":2,"lsn":5,"at":"2026-11-25T12:00:00Z","tables":[["public","orders"]]}`,
			"no position":   `{"version":1,"lsn":0,"at":"2026-11-25T12:00:00Z","tables":[["public","orders"]]}`,
			"no time":       `{"version":1,"lsn":5,"tables":[["public","orders"]]}`,
		} {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, PGRunCutFile), []byte(body), 0o644); err != nil {
				t.Fatal(err)
			}
			if _, found, err := ReadPGRunCut(dir); found || err == nil {
				t.Errorf("%s: found %v, err %v; want an error", name, found, err)
			}
		}
	})

	// The read layer keeps this name off schemas without importing this
	// package (pgbaseline.snapshotControlFiles): the two must agree.
	if PGRunCutFile != "_PG_RUN_CUT" {
		t.Fatalf("PGRunCutFile is %q; change pgbaseline's list of control files with it", PGRunCutFile)
	}
}

// A table is held through the run only when nothing was lost in its window.
func TestPGHoldThroughRun_3084(t *testing.T) {
	run := &pgRunEnd{until: 1, held: &pgRunHeld{}}
	cfg := FullTableConfig{pgRun: run}
	pgHoldThroughRun(cfg, "public", "lost", &CaptureGap{}, false)
	pgHoldThroughRun(cfg, "public", "dropped", nil, true)
	pgHoldThroughRun(cfg, "public", "whole", nil, false)
	pgHoldThroughRun(FullTableConfig{}, "public", "mysql", nil, false) // no run: nothing, no panic
	if run.held.tables[[2]string{"public", "lost"}] || run.held.tables[[2]string{"public", "dropped"}] {
		t.Fatal("a table with a loss in its window is held through the run")
	}
	if !run.held.tables[[2]string{"public", "whole"}] {
		t.Fatal("a whole table is not held through the run")
	}
}
