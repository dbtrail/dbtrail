package consoleapp

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/status"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
)

// #2674: a snapshot made by `bintrail baseline` from a shell (or by another
// process) has no run record here, so no index mark says how far the index
// had been written when it was made, and the count of changes since it is
// unknown. An unknown count used to run the update whatever the anchor's age,
// and with capture stopped that update published the snapshot's rows again
// under a new date: the page then called the copy current when nothing had
// checked the source.
//
// The snapshot itself says where in the source's binary log it was read: its
// table files carry the GTID set or the file and position of the read (the
// same coordinates an update starts its fold from). Capture's checkpoint says
// how far capture has recorded. When the checkpoint is not past those
// coordinates, capture has recorded nothing the snapshot does not already
// hold, which is exactly what a known count of zero says, and the slot is
// decided like one: the source is asked whether capture is caught up, and a
// slot whose source cannot be asked is skipped instead of being published
// under a new date.
//
// Only that one direction is ever concluded. A checkpoint past the snapshot,
// coordinates that cannot be compared (a PostgreSQL or MariaDB source, a
// snapshot with no coordinates, a GTID set that does not parse, two binary
// logs of different names) or anything that could not be read leave the count
// unknown, and the update runs as it did before.

// anchorCutsFor reads the coordinates every table file of the snapshot at
// anchor recorded; a package variable so the schedule's use of it can be
// driven without snapshot files.
var anchorCutsFor = anchorCutsFromDisk

func anchorCutsFromDisk(ctx context.Context, dir string, anchor time.Time) ([]baseline.DumpMetadata, error) {
	files, err := listBaselines(ctx, dir)
	if err != nil {
		return nil, err
	}
	var cuts []baseline.DumpMetadata
	for _, f := range files {
		if !f.SnapshotTime.Equal(anchor) {
			continue
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		md, err := baseline.ReadParquetMetadata(f.Path)
		if err != nil {
			return nil, fmt.Errorf("read the coordinates of %s: %w", f.Path, err)
		}
		cuts = append(cuts, md)
	}
	return cuts, nil
}

// captureCheckpointFor reads capture's checkpoint from the index (nil: no row);
// a package variable for the reason anchorCutsFor is.
var captureCheckpointFor = captureCheckpointFromDB

func captureCheckpointFromDB(ctx context.Context, dsn string) (*status.StreamStateInfo, error) {
	db, err := config.Connect(dsn)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	return status.LoadStreamState(ctx, db)
}

// capturedNothingSince reports whether capture's checkpoint st is at or before
// the coordinates the snapshot's table files recorded (cuts): capture has
// recorded nothing the snapshot does not already hold. False whenever that
// cannot be established, which leaves the count unknown.
//
// By GTID: the snapshot holds the union of its tables' sets (a table carried
// forward from an older snapshot records an older, smaller set, so the union
// is never more than the snapshot holds), and nothing was captured since when
// that union contains every transaction the checkpoint names. By position:
// the newest coordinate of its tables, against the checkpoint's, in the same
// order the fold's lower bound uses (query.BinlogPos.AtOrBefore), and only
// within one binary log name: positions in two differently named sequences
// say nothing about each other.
func capturedNothingSince(st *status.StreamStateInfo, cuts []baseline.DumpMetadata) bool {
	if st == nil || len(cuts) == 0 {
		return false
	}
	switch st.Mode {
	case "gtid":
		if !st.GTIDSet.Valid {
			return false
		}
		have, err := gomysql.ParseMysqlGTIDSet(compactGTIDSet(st.GTIDSet.String))
		if err != nil || have.IsEmpty() {
			return false
		}
		held := gomysql.NewMysqlGTIDSet()
		found := false
		for _, c := range cuts {
			raw := compactGTIDSet(c.GTIDSet)
			if raw == "" {
				continue
			}
			// A table whose set does not parse is left out: the union is
			// then smaller, which can only keep the count unknown.
			if err := held.Update(raw); err != nil {
				continue
			}
			found = true
		}
		return found && held.Contain(have)
	case "position":
		if st.BinlogFile == "" {
			return false
		}
		ckpt := query.BinlogPos{File: st.BinlogFile, Pos: st.BinlogPosition}
		var newest *query.BinlogPos
		for _, c := range cuts {
			if c.BinlogFile == "" || c.BinlogPos < 0 || !sameBinlogName(c.BinlogFile, st.BinlogFile) {
				continue
			}
			p := query.BinlogPos{File: c.BinlogFile, Pos: uint64(c.BinlogPos)}
			if newest == nil || newest.AtOrBefore(p) {
				newest = &p
			}
		}
		return newest != nil && ckpt.AtOrBefore(*newest)
	}
	return false
}

// compactGTIDSet drops the whitespace a GTID set may carry: MySQL prints a
// long executed set over several lines, and the snapshot records it as read.
func compactGTIDSet(s string) string {
	return strings.Join(strings.Fields(s), "")
}

// sameBinlogName reports whether two binary log file names belong to one
// sequence: the same name before the numeric suffix ("binlog.000009" and
// "binlog.000010" do, "mysql-bin.000009" and "binlog.000009" do not).
func sameBinlogName(a, b string) bool {
	base := func(f string) string {
		if i := strings.LastIndexByte(f, '.'); i >= 0 {
			return f[:i]
		}
		return f
	}
	return base(a) == base(b)
}

// nothingCapturedSinceAnchor is the schedule's use of capturedNothingSince
// for e's snapshot at anchor, within one bounded read (windowProbeTimeout:
// the window probe runs on page loads too). The snapshot's files are read
// first, so a snapshot with no coordinates never opens the index. Said once
// per snapshot, at Info, when it does establish the zero: the decision that
// follows (asking the source, a skip, a full read) otherwise reads as if the
// daemon had a record it does not have.
func (b *backupScheduler) nothingCapturedSinceAnchor(ctx context.Context, e console.ServerEntry, anchor time.Time) bool {
	if e.IsPostgres() || strings.TrimSpace(e.BaselineDir) == "" || anchor.IsZero() {
		return false
	}
	ctx, cancel := context.WithTimeout(ctx, windowProbeTimeout)
	defer cancel()
	cuts, err := anchorCutsFor(ctx, e.BaselineDir, anchor)
	if err != nil || len(cuts) == 0 {
		slog.Debug("snapshot schedule: the previous snapshot's binary log coordinates could not be read; the number of changes since it stays unknown",
			"server", e.Name, "id", e.ID, "previous_backup", anchor.UTC().Format(time.RFC3339), "error", err)
		return false
	}
	st, err := captureCheckpointFor(ctx, probeDSN(e.DSN))
	if err != nil || !capturedNothingSince(st, cuts) {
		return false
	}
	if b.sup.gateEdge.Fire("anchor-cut:"+e.ID, anchor.UTC().Format(time.RFC3339)) {
		slog.Info(anchorCutZeroMsg, "server", e.Name, "id", e.ID, "previous_backup", anchor.UTC().Format(time.RFC3339))
	}
	return true
}

// anchorCutZeroMsg is that line.
const anchorCutZeroMsg = "snapshot schedule: the previous snapshot has no run record here, but the binary log position it was read at shows capture recorded nothing since it, " +
	"so nothing was indexed since it; the slot is decided as for a quiet server"
