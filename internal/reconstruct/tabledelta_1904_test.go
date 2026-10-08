package reconstruct

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/query"
)

// #1904: a chain of deltas keeps its start for up to a day, and a reader of
// the table fetches events from that start. On a quiet server nothing else
// moves it, so the refresh has to end the chain (write the table in full)
// before the index drops the events at its start. ChainStartFloor is how the
// daemon says where that line is.

func TestTableDeltaCompactReason_chainStartFloor(t *testing.T) {
	noSizeFloor(t)
	at := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	start := at.Add(-6 * time.Hour)
	chain := &tableDelta{Meta: baseline.DumpMetadata{DeltaChainStart: start}, PairSize: 1}
	cases := []struct {
		name  string
		prev  *tableDelta
		floor time.Time
		want  string
		why   string
	}{
		{"no floor", chain, time.Time{}, "",
			"zero is what a restore and an unreadable index pass: nothing is ended on this ground"},
		{"start well after the floor", chain, start.Add(-time.Hour), "",
			"a reader can still fetch from the start, and extending is what deltas are for"},
		{"start one second after the floor", chain, start.Add(-time.Second), "",
			"the line is the floor itself, not a band around it"},
		{"start exactly at the floor", chain, start, "oldest events",
			"at the line counts as past it, like the aging verdict's >= that the gate uses"},
		{"start before the floor", chain, start.Add(time.Hour), "oldest events",
			"already too close: the table is written in full and a new chain starts at this run"},
		// With no chain yet, a chain begun here starts at the BASE's time
		// (newStart below), not at this run, so it is held to the same line.
		{"no chain yet, base above the floor", nil, start.Add(-time.Hour), "",
			"a chain begun over this base starts inside coverage"},
		{"no chain yet, base at the floor", nil, start, "oldest events",
			"a chain begun over this base would start on the line: written in full, the new chain starts at this run"},
	}
	for _, c := range cases {
		got := tableDeltaCompactReason(c.prev, "/b/t.parquet", 1000, nil, at, true, "", c.floor, start, 0)
		if (c.want == "") != (got == "") || !strings.Contains(got, c.want) {
			t.Errorf("%s: reason = %q, want it to contain %q (%s)", c.name, got, c.want, c.why)
		}
	}
}

// TestPublishWithTableDelta_aQuietChainIsEndedAtTheFloor drives the real
// publish over empty windows, which is what a quiet server's refresh is. With
// no floor an empty window leaves the chain and its start alone (that is the
// bug). With the floor at or past the start, the table is written in full and
// the new snapshot's chain starts at the run.
func TestPublishWithTableDelta_aQuietChainIsEndedAtTheFloor(t *testing.T) {
	noCompaction(t)
	t0 := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	base, dirTime := chainFrom(t, t0, 2) // a chain that started at t0
	root := filepath.Dir(filepath.Dir(filepath.Dir(base)))
	cut := &query.BinlogPos{File: "binlog.000009", Pos: 5000}

	at1 := dirTime.Add(time.Hour)
	nb, rep, err := deltaWindow(t, root, base, dirTime, changeMap(), at1, cut, nil)
	if err != nil {
		t.Fatal(err)
	}
	if rep.DeltaCompacted != "" {
		t.Fatalf("empty window, no floor: the table was rewritten (%q); an unchanged chain should travel as it is", rep.DeltaCompacted)
	}
	if got, err := DeltaChainStart(context.Background(), nb); err != nil || !got.Equal(t0) {
		t.Fatalf("empty window, no floor: chain start = %v (err %v), want it unchanged at %s", got, err, t0)
	}

	at2 := at1.Add(time.Hour)
	nb, rep, err = deltaWindow(t, root, nb, at1, changeMap(), at2, cut, func(p *tableDeltaPublish) {
		p.cfg.ChainStartFloor = t0
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(rep.DeltaCompacted, "oldest events") {
		t.Fatalf("empty window, floor at the chain's start: DeltaCompacted = %q, want the table written in full", rep.DeltaCompacted)
	}
	got, err := DeltaChainStart(context.Background(), nb)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Equal(at2) {
		t.Fatalf("after the rewrite a reader starts at %s, want this run (%s): the rewrite has to move the start", got, at2)
	}
}

// TestSnapshotReadsFrom is the instant the gate watches: the oldest chain
// start of the snapshot, read the way the backup-age verdict reads it.
func TestSnapshotReadsFrom(t *testing.T) {
	noCompaction(t)
	t0 := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	ctx := context.Background()

	t.Run("a chain: its start, not the folder's time", func(t *testing.T) {
		base, dirTime := chainFrom(t, t0, 3)
		root := filepath.Dir(filepath.Dir(filepath.Dir(base)))
		// A table with no chain beside it in the same snapshot reads from the
		// folder, which is later: the oldest wins.
		plain := writeStampedBaseline(t, t.TempDir(), dirTime)
		copyTable(t, plain, filepath.Dir(base), "plain")
		// A nanosecond instant names the same folder (whole seconds).
		got, err := SnapshotReadsFrom(ctx, root, dirTime.Add(500*time.Millisecond))
		if err != nil {
			t.Fatal(err)
		}
		if !got.Equal(t0) {
			t.Fatalf("SnapshotReadsFrom = %s, want the chain's start %s (the folder is %s)", got, t0, dirTime)
		}
	})

	t.Run("no chain: the folder's time", func(t *testing.T) {
		root := t.TempDir()
		writeStampedBaseline(t, root, t0)
		got, err := SnapshotReadsFrom(ctx, root, t0)
		if err != nil || !got.Equal(t0) {
			t.Fatalf("SnapshotReadsFrom = %s, %v, want %s", got, err, t0)
		}
	})

	t.Run("no table at that instant: an error, never the instant asked", func(t *testing.T) {
		root := t.TempDir()
		writeStampedBaseline(t, root, t0)
		if got, err := SnapshotReadsFrom(ctx, root, t0.Add(time.Hour)); err == nil {
			t.Fatalf("SnapshotReadsFrom = %s with no error for a snapshot that is not there", got)
		}
	})

	t.Run("a damaged chain: an error", func(t *testing.T) {
		base, dirTime := chainFrom(t, t0, 2)
		root := filepath.Dir(filepath.Dir(filepath.Dir(base)))
		_, ups := baseline.TableDeltaPaths(base, 1)
		if _, err := os.Stat(ups); err != nil {
			t.Fatalf("fixture: %v", err)
		}
		rm(t, ups)
		if got, err := SnapshotReadsFrom(ctx, root, dirTime); err == nil {
			t.Fatalf("SnapshotReadsFrom = %s with no error over half a pair", got)
		}
	})

	t.Run("an unreadable chain start: an error", func(t *testing.T) {
		base, dirTime := chainFrom(t, t0, 2)
		root := filepath.Dir(filepath.Dir(filepath.Dir(base)))
		_, ups := baseline.TableDeltaPaths(base, 1)
		if err := os.WriteFile(ups, []byte("not parquet"), 0o644); err != nil {
			t.Fatal(err)
		}
		if got, err := SnapshotReadsFrom(ctx, root, dirTime); err == nil {
			t.Fatalf("SnapshotReadsFrom = %s with no error over an unreadable footer", got)
		}
	})
}

// A table with no chain yet: the chain a refresh begins over it starts at the
// base's time, not at the run. Over a base already on the line, the table is
// written in full instead, so the new chain starts at the run.
func TestPublishWithTableDelta_aNewChainOverAnOldBaseIsNotBegun(t *testing.T) {
	noCompaction(t)
	t0 := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	root := t.TempDir()
	base := writeStampedBaseline(t, root, t0)
	cut := &query.BinlogPos{File: "binlog.000009", Pos: 5000}
	at := t0.Add(20 * time.Hour)
	nb, rep, err := deltaWindow(t, root, base, t0, changeMap(), at, cut, func(p *tableDeltaPublish) {
		p.cfg.ChainStartFloor = t0
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(rep.DeltaCompacted, "oldest events") {
		t.Fatalf("DeltaCompacted = %q, want the table written in full: a chain begun here would start at %s, on the line", rep.DeltaCompacted, t0)
	}
	if got, err := DeltaChainStart(context.Background(), nb); err != nil || !got.Equal(at) {
		t.Fatalf("chain start = %v (err %v), want this run %s", got, err, at)
	}
}

// A folder of the snapshot the listing could not read hides its tables, and
// their chains may be the oldest: that is an error, never the minimum of what
// was left.
func TestSnapshotReadsFrom_anUnreadableSchemaIsAnError(t *testing.T) {
	noCompaction(t)
	t0 := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	base, dirTime := chainFrom(t, t0, 2)
	root := filepath.Dir(filepath.Dir(filepath.Dir(base)))
	plain := writeStampedBaseline(t, t.TempDir(), dirTime)
	copyTable(t, plain, filepath.Join(root, SnapshotDirName(dirTime), "other"), "plain")
	schemaDir := filepath.Dir(base)
	if err := os.Chmod(schemaDir, 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(schemaDir, 0o755) })
	if _, err := os.ReadDir(schemaDir); err == nil {
		t.Skip("this user can read a mode-000 directory (root?), so the case cannot be staged")
	}
	if got, err := SnapshotReadsFrom(context.Background(), root, dirTime); err == nil {
		t.Fatalf("SnapshotReadsFrom = %s with no error while the folder holding the oldest chain was unreadable", got)
	}
}
