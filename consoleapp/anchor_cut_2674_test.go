package consoleapp

import (
	"context"
	"database/sql"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/status"
)

// #2674, point 2: a snapshot with no run record here (made by `bintrail
// baseline` from a shell) used to be updated at its first slot with the count
// of changes unknown, and with capture stopped that update published the
// snapshot's rows again under a new date. The snapshot's own binary log
// coordinates, against capture's checkpoint, now say whether anything was
// captured since it.

const cutUUID = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
const otherUUID = "4f22ab58-82db-22f2-af44-d91bb0530673"

func gtidCheckpoint(set string) *status.StreamStateInfo {
	return &status.StreamStateInfo{Mode: "gtid", GTIDSet: sql.NullString{String: set, Valid: true}}
}

func posCheckpoint(file string, pos uint64) *status.StreamStateInfo {
	return &status.StreamStateInfo{Mode: "position", BinlogFile: file, BinlogPosition: pos}
}

func gtidCut(set string) baseline.DumpMetadata { return baseline.DumpMetadata{GTIDSet: set} }

func posCut(file string, pos int64) baseline.DumpMetadata {
	return baseline.DumpMetadata{BinlogFile: file, BinlogPos: pos}
}

func TestCapturedNothingSince_2674(t *testing.T) {
	cases := []struct {
		name string
		st   *status.StreamStateInfo
		cuts []baseline.DumpMetadata
		want bool
	}{
		// GTID.
		{"checkpoint equals the snapshot's set", gtidCheckpoint(cutUUID + ":1-100"), []baseline.DumpMetadata{gtidCut(cutUUID + ":1-100")}, true},
		{"checkpoint was behind the snapshot already", gtidCheckpoint(cutUUID + ":1-90"), []baseline.DumpMetadata{gtidCut(cutUUID + ":1-100")}, true},
		{"one transaction captured past the snapshot", gtidCheckpoint(cutUUID + ":1-101"), []baseline.DumpMetadata{gtidCut(cutUUID + ":1-100")}, false},
		{"a transaction of another server captured", gtidCheckpoint(cutUUID + ":1-100," + otherUUID + ":1"), []baseline.DumpMetadata{gtidCut(cutUUID + ":1-100")}, false},
		{"the snapshot's set in capitals, over two lines", gtidCheckpoint(cutUUID + ":1-100," + otherUUID + ":1-5"),
			[]baseline.DumpMetadata{gtidCut(strings.ToUpper(cutUUID) + ":1-100,\n" + strings.ToUpper(otherUUID) + ":1-5")}, true},
		{"the union of the tables: a carried table's older set does not hide the newest", gtidCheckpoint(cutUUID + ":1-100"),
			[]baseline.DumpMetadata{gtidCut(cutUUID + ":1-40"), gtidCut(cutUUID + ":1-100")}, true},
		{"only an older carried table's set: the count stays unknown", gtidCheckpoint(cutUUID + ":1-100"),
			[]baseline.DumpMetadata{gtidCut(cutUUID + ":1-40")}, false},
		{"one table's set does not parse, another holds the checkpoint", gtidCheckpoint(cutUUID + ":1-100"),
			[]baseline.DumpMetadata{gtidCut("not a gtid set"), gtidCut(cutUUID + ":1-100")}, true},
		{"no table's set parses", gtidCheckpoint(cutUUID + ":1-100"), []baseline.DumpMetadata{gtidCut("not a gtid set")}, false},
		{"a MariaDB GTID position does not parse as a set", gtidCheckpoint("0-1-100"), []baseline.DumpMetadata{gtidCut("0-1-100")}, false},
		{"the snapshot recorded no set, only a position", gtidCheckpoint(cutUUID + ":1-100"), []baseline.DumpMetadata{posCut("binlog.000003", 900)}, false},
		{"an empty checkpoint set", gtidCheckpoint(""), []baseline.DumpMetadata{gtidCut(cutUUID + ":1-100")}, false},
		{"a checkpoint set not recorded (NULL)", &status.StreamStateInfo{Mode: "gtid"}, []baseline.DumpMetadata{gtidCut(cutUUID + ":1-100")}, false},
		{"the checkpoint does not parse", gtidCheckpoint("garbage"), []baseline.DumpMetadata{gtidCut(cutUUID + ":1-100")}, false},
		// Position.
		{"position: checkpoint before the snapshot in the same file", posCheckpoint("binlog.000003", 800), []baseline.DumpMetadata{posCut("binlog.000003", 900)}, true},
		{"position: checkpoint at the snapshot", posCheckpoint("binlog.000003", 900), []baseline.DumpMetadata{posCut("binlog.000003", 900)}, true},
		{"position: checkpoint past the snapshot", posCheckpoint("binlog.000003", 901), []baseline.DumpMetadata{posCut("binlog.000003", 900)}, false},
		{"position: checkpoint in a later file", posCheckpoint("binlog.000004", 4), []baseline.DumpMetadata{posCut("binlog.000003", 900)}, false},
		{"position: checkpoint in an earlier file", posCheckpoint("binlog.000002", 99999), []baseline.DumpMetadata{posCut("binlog.000003", 900)}, true},
		{"position: past the rollover from 999999 to 1000000", posCheckpoint("binlog.1000000", 4), []baseline.DumpMetadata{posCut("binlog.999999", 900)}, false},
		{"position: another binary log name", posCheckpoint("mysql-bin.000001", 4), []baseline.DumpMetadata{posCut("binlog.000003", 900)}, false},
		{"position: the newest table's coordinate counts", posCheckpoint("binlog.000003", 850),
			[]baseline.DumpMetadata{posCut("binlog.000003", 700), posCut("binlog.000003", 900)}, true},
		{"position: no file recorded on the checkpoint", posCheckpoint("", 0), []baseline.DumpMetadata{posCut("binlog.000003", 900)}, false},
		{"position: no file recorded on the snapshot", posCheckpoint("binlog.000003", 4), []baseline.DumpMetadata{posCut("", 0)}, false},
		// Neither.
		{"no capture on record", nil, []baseline.DumpMetadata{gtidCut(cutUUID + ":1-100")}, false},
		{"no table files", gtidCheckpoint(cutUUID + ":1-100"), nil, false},
		{"a mode this does not know", &status.StreamStateInfo{Mode: "lsn", BinlogFile: "0/16B3748"}, []baseline.DumpMetadata{posCut("0/16B3748", 0)}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, held := capturedNothingSince(c.st, c.cuts)
			if got != c.want {
				t.Fatalf("capturedNothingSince = %v, want %v", got, c.want)
			}
			// The snapshot's set comes back exactly where the source is to
			// be compared with it: nothing captured since, by GTID.
			if (got && c.st.Mode == "gtid") != (held != "") {
				t.Fatalf("held = %q for got=%v mode=%q", held, got, c.st.Mode)
			}
		})
	}
}

// stubAnchorCut makes the snapshot's coordinates and capture's checkpoint
// what the test says, and counts how often they are read.
func stubAnchorCut(t *testing.T, cuts []baseline.DumpMetadata, st *status.StreamStateInfo) *atomic.Int32 {
	t.Helper()
	var reads atomic.Int32
	prevCuts, prevCkpt := anchorCutsFor, captureCheckpointFor
	t.Cleanup(func() { anchorCutsFor, captureCheckpointFor = prevCuts, prevCkpt })
	anchorCutsFor = func(context.Context, string, time.Time) ([]baseline.DumpMetadata, error) {
		reads.Add(1)
		return cuts, nil
	}
	captureCheckpointFor = func(context.Context, string) (*status.StreamStateInfo, error) { return st, nil }
	return &reads
}

// stubProbeAgainst answers the probe that compares the source with the
// snapshot's set: the real verdict (compareGTIDSets) of the set it is
// handed against a source whose executed set is source. It records the set,
// so a test fails if capture's set is handed instead of the snapshot's.
func stubProbeAgainst(t *testing.T, source string) *[]string {
	t.Helper()
	var asked []string
	prev := probeCaptureAgainst
	t.Cleanup(func() { probeCaptureAgainst = prev })
	probeCaptureAgainst = func(_ context.Context, _, _ string, _ config.SSL, _, _ time.Time, against string) captureProbeResult {
		asked = append(asked, against)
		v, d := compareGTIDSets(against, source)
		return captureProbeResult{verdict: v, detail: d}
	}
	return &asked
}

// The slot, end to end through the schedule's own decision: a shell snapshot
// with no record, eight days old on an hourly schedule.
func TestShellSnapshot_capturedNothingSinceIsDecidedAsAQuietServer_2674(t *testing.T) {
	now := snapshotAnchor.Add(8 * 24 * time.Hour)
	snap := []baseline.DumpMetadata{gtidCut(cutUUID + ":1-100")}
	setup := func(t *testing.T, probe captureProbeResult) (*backupScheduler, console.ServerEntry) {
		b, reg, _ := newScheduleFixture(t, true)
		b.window = b.measureWindow
		e := addScheduled(t, reg, true) // no run record for its snapshot
		stubCaptureProbe(t, probe)
		stubCoverage(t, true, true)
		return b, e
	}
	cases := []struct {
		name, capture, source string
		method                string
	}{
		// Capture stopped at the snapshot, the source wrote more: a full
		// read, on evidence, not a new date.
		{"capture at the snapshot, the source wrote past it", cutUUID + ":1-100", cutUUID + ":1-120", console.BackupMethodFull},
		// The review's case: capture stopped BEFORE the snapshot was taken by
		// hand, and the source wrote nothing since. The snapshot is current,
		// so the update runs (its new date is true), never a full read.
		{"capture behind the snapshot, the source idle since it", cutUUID + ":1-90", cutUUID + ":1-100", console.BackupMethodRefresh},
		// The same, but the source wrote past the snapshot: now a full read
		// has its evidence.
		{"capture behind the snapshot, the source wrote past it", cutUUID + ":1-90", cutUUID + ":1-105", console.BackupMethodFull},
		{"capture at the snapshot, the source idle (caught up)", cutUUID + ":1-100", cutUUID + ":1-100", console.BackupMethodRefresh},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			b, e := setup(t, captureProbeResult{detail: "probeCapture must not be asked here"})
			stubAnchorCut(t, snap, gtidCheckpoint(c.capture))
			asked := stubProbeAgainst(t, c.source)
			logs := captureSlogFor(t)
			method, why := freshDecision(t, b, e, now)
			t.Logf("%s: %s", method, why)
			if method != c.method {
				t.Fatalf("method=%q why=%q, want %q", method, why, c.method)
			}
			if len(*asked) == 0 || (*asked)[0] != cutUUID+":1-100" {
				t.Fatalf("the source was compared with %v, want the snapshot's set", *asked)
			}
			if method == console.BackupMethodFull && console.BackupWhyCode(why) != "window_age" {
				t.Fatalf("the full read's why %q", why)
			}
			if !strings.Contains(logs.String(), anchorCutZeroMsg) || strings.Contains(logs.String(), countUnknownLine) {
				t.Fatalf("the log does not say where the zero came from, or says the count is unknown: %q", logs.String())
			}
		})
	}
	t.Run("capture stopped, the source cannot be asked: the slot is skipped", func(t *testing.T) {
		b, e := setup(t, captureProbeResult{detail: positionMode})
		stubAnchorCut(t, []baseline.DumpMetadata{posCut("binlog.000003", 900)}, posCheckpoint("binlog.000003", 870))
		asked := stubProbeAgainst(t, cutUUID+":1-100")
		method, why := freshDecision(t, b, e, now)
		if method != console.BackupMethodSkip || why != console.SkipUnconfirmedReason(positionMode) || len(*asked) != 0 {
			t.Fatalf("method=%q why=%q asked against %v, want the skip and no GTID comparison", method, why, *asked)
		}
	})
	t.Run("capture healthy, a burst right after the snapshot: the update runs", func(t *testing.T) {
		b, e := setup(t, captureProbeResult{verdict: console.CaptureBehind, detail: captureDetailBehind})
		stubAnchorCut(t, snap, gtidCheckpoint(cutUUID+":1-5000"))
		method, why := freshDecision(t, b, e, now)
		if method != console.BackupMethodRefresh || console.IsUnconfirmedUpdate(why) {
			t.Fatalf("method=%q why=%q, want the plain update", method, why)
		}
	})
	t.Run("coordinates that cannot be compared: the update runs, as before", func(t *testing.T) {
		b, e := setup(t, captureProbeResult{verdict: console.CaptureBehind, detail: captureDetailBehind})
		stubAnchorCut(t, []baseline.DumpMetadata{{}}, gtidCheckpoint(cutUUID+":1-100"))
		logs := captureSlogFor(t)
		if method, why := freshDecision(t, b, e, now); method != console.BackupMethodRefresh {
			t.Fatalf("method=%q why=%q, want the update", method, why)
		}
		if !strings.Contains(logs.String(), countUnknownLine) {
			t.Fatalf("the unknown count is no longer said: %q", logs.String())
		}
	})
	t.Run("a PostgreSQL source is not read", func(t *testing.T) {
		b, e := setup(t, captureProbeResult{verdict: console.CaptureBehind, detail: captureDetailBehind})
		reads := stubAnchorCut(t, snap, gtidCheckpoint(cutUUID+":1-100"))
		pg := e
		pg.Flavor = "postgres"
		freshDecision(t, b, pg, now)
		if reads.Load() != 0 {
			t.Fatalf("the snapshot's coordinates were read %d times for a PostgreSQL source", reads.Load())
		}
	})
}

// asOfSnapshot puts the snapshot's set in place of capture's, and nothing
// else: the loss record and the mode stay capture's.
func TestAsOfSnapshot_2720(t *testing.T) {
	st := gtidCheckpoint(cutUUID + ":1-90")
	st.GapColumnsPresent = true
	got := asOfSnapshot(st, cutUUID+":1-100")
	if got.GTIDSet.String != cutUUID+":1-100" || !got.GapColumnsPresent || got.Mode != "gtid" || st.GTIDSet.String != cutUUID+":1-90" {
		t.Fatalf("asOfSnapshot = %+v (and the original %+v)", got, st)
	}
	if asOfSnapshot(st, "") != st || asOfSnapshot(posCheckpoint("binlog.000001", 4), cutUUID+":1-100").Mode != "position" || asOfSnapshot(nil, "x") != nil {
		t.Fatal("asOfSnapshot changed a checkpoint it has no set for")
	}
}

// The coordinates are read only where the zero decides something: past the
// cut-over age, and only when no record names a mark.
func TestShellSnapshot_coordinatesAreReadOnlyWhereTheyDecide_2674(t *testing.T) {
	b, reg, sup := newScheduleFixture(t, true)
	b.window = b.measureWindow
	e := addScheduled(t, reg, true)
	reads := stubAnchorCut(t, []baseline.DumpMetadata{gtidCut(cutUUID + ":1-100")}, gtidCheckpoint(cutUUID+":1-100"))
	stubCaptureProbe(t, captureProbeResult{verdict: console.CaptureCaughtUp})
	// A snapshot younger than the cut-over age (the hourly one is six hours).
	fresh := time.Now().Add(-time.Hour)
	forgetWindow(b)
	if w := b.measureWindow(context.Background(), e, fresh); w.Events != -1 || reads.Load() != 0 {
		t.Fatalf("a fresh snapshot: window=%+v, %d reads; want the count unknown and no read", w, reads.Load())
	}
	// A record naming a mark for the old snapshot: the mark decides.
	mark := indexMark{events: 1500}
	stubIndexMark(t, &mark, true)
	if err := sup.history.Append(console.BaselineRunRecord{ServerID: e.ID, Kind: console.BaselineRunRefresh,
		SnapshotTime: snapshotAnchor.Format(time.RFC3339), StartedAt: snapshotAnchor.Format(time.RFC3339),
		FinishedAt: snapshotAnchor.Format(time.RFC3339), IndexMark: 1000, IndexMarkKnown: true}); err != nil {
		t.Fatal(err)
	}
	forgetWindow(b)
	if w := b.measureWindow(context.Background(), e, snapshotAnchor); w.Events != 500 || reads.Load() != 0 {
		t.Fatalf("with a record: window=%+v, %d reads; want 500 events counted from the mark and no read", w, reads.Load())
	}
}
