package consoleapp

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/console"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-sql-driver/mysql"
)

// Whether the capture has everything the source wrote (#1791).
//
// The backup schedule asks this in one case only: nothing has been indexed
// since the previous snapshot and that snapshot is past the cut-over age.
// There, a full backup used to follow at every slot, because from the index
// alone "the source wrote nothing" and "the capture stopped" read the same
// (#1223): the binlog syncer retries without limit (MaxReconnectAttempts 0)
// and the stream's checkpoint ticker keeps stamping stream_state while it
// does, so a fresh checkpoint proves the loop runs, not that it captures.
// The source is what can tell: has it written past the capture's checkpoint?
// Only a definite "no" lets the update run; anything unanswered keeps the
// full backup, which is what happened before.

// captureCheckpoint is the index's stream_state row as the probe needs it.
// present is false when the index has no row at all: a file-mode index
// (`bintrail index`) that never had a live capture, which is neither caught
// up nor dead, and is answered as unknown.
type captureCheckpoint struct {
	present bool
	mode    string // "gtid" or "position"
	file    string
	pos     uint64
	gtidSet string
	flavor  string
}

// sourceHead is where the source's binary log is now: its executed GTID set
// (empty when GTIDs are not fully on) or its current file and position.
type sourceHead struct {
	gtid string
	file string
	pos  uint64
}

// compareCapture is the verdict, pure: console.CaptureCaughtUp,
// console.CaptureBehind, or "" (unknown), and a clause saying why for
// anything but caught up.
func compareCapture(cp captureCheckpoint, src sourceHead) (verdict, detail string) {
	if !cp.present {
		return "", "the index has no live capture on record"
	}
	switch cp.mode {
	case "gtid":
		if cp.flavor == console.FlavorMariaDB {
			return "", "MariaDB GTIDs are not compared yet"
		}
		if strings.TrimSpace(cp.gtidSet) == "" {
			return "", "the capture has no GTID checkpoint"
		}
		if strings.TrimSpace(src.gtid) == "" {
			return "", "the source reported no GTID set"
		}
		have, err := gomysql.ParseMysqlGTIDSet(cp.gtidSet)
		if err != nil {
			return "", "the capture's GTID set does not parse"
		}
		wrote, err := gomysql.ParseMysqlGTIDSet(src.gtid)
		if err != nil {
			return "", "the source's GTID set does not parse"
		}
		if have.Contain(wrote) {
			return console.CaptureCaughtUp, ""
		}
		return console.CaptureBehind, "the capture's GTID set does not contain the source's"
	case "position":
		if cp.file == "" || src.file == "" {
			return "", "no binlog position to compare"
		}
		capBase, capSeq, ok1 := binlogSequence(cp.file)
		srcBase, srcSeq, ok2 := binlogSequence(src.file)
		if !ok1 || !ok2 || capBase != srcBase {
			return "", "the capture and the source name their binlogs differently"
		}
		if capSeq > srcSeq || (capSeq == srcSeq && cp.pos >= src.pos) {
			return console.CaptureCaughtUp, ""
		}
		return console.CaptureBehind, fmt.Sprintf("the capture is at %s:%d, the source at %s:%d", cp.file, cp.pos, src.file, src.pos)
	}
	return "", "the capture's checkpoint mode is not one this build compares"
}

// binlogSequence splits "binlog.000012" into its base name and sequence
// number, compared as a number so a suffix that grows a digit still orders.
func binlogSequence(name string) (base string, seq uint64, ok bool) {
	i := strings.LastIndexByte(name, '.')
	if i <= 0 || i == len(name)-1 {
		return "", 0, false
	}
	n, err := strconv.ParseUint(name[i+1:], 10, 64)
	if err != nil {
		return "", 0, false
	}
	return name[:i], n, true
}

// captureCheckpointQuery reads the one stream_state row.
const captureCheckpointQuery = "SELECT mode, binlog_file, binlog_position, gtid_set, flavor FROM stream_state WHERE id = 1"

// readCaptureCheckpoint reads the index's checkpoint. known is false only
// when the read failed; no row is a known answer (present false).
func readCaptureCheckpoint(ctx context.Context, db *sql.DB) (cp captureCheckpoint, known bool) {
	var gtid sql.NullString
	err := db.QueryRowContext(ctx, captureCheckpointQuery).Scan(&cp.mode, &cp.file, &cp.pos, &gtid, &cp.flavor)
	if errors.Is(err, sql.ErrNoRows) {
		return captureCheckpoint{}, true
	}
	if err != nil {
		return captureCheckpoint{}, false
	}
	cp.present, cp.gtidSet = true, gtid.String
	return cp, true
}

// probeCapture is a package variable for the reason readIndexMark is: it
// opens two databases, and the window probe that calls it is exercised at
// the unit tier.
var probeCapture = probeCaptureFromDBs

// probeCaptureFromDBs is captureFromDBs with one bound on the whole of it:
// windowProbeTimeout, after which the answer is unknown. It runs inside the
// window probe, on page loads too, and each step's own bound (the dial, the
// reads) only limits that step: a source that accepts the connection and
// then answers slowly would otherwise hold a page for several of them. The
// probe keeps running in the background after that, until those same
// bounds end it; its answer is then dropped.
func probeCaptureFromDBs(ctx context.Context, indexDSN, sourceDSN string) (verdict, detail string) {
	return boundedCaptureProbe(ctx, windowProbeTimeout, func(ctx context.Context) (string, string) {
		return captureFromDBs(ctx, indexDSN, sourceDSN)
	})
}

// boundedCaptureProbe runs probe and waits at most within for it. The
// context is cancelled here, after the wait, never by the probe's goroutine:
// cancelled there, an answer sent in time and the expired context would be
// ready together, and the select below may take either.
func boundedCaptureProbe(ctx context.Context, within time.Duration, probe func(context.Context) (string, string)) (verdict, detail string) {
	ctx, cancel := context.WithTimeout(ctx, within)
	defer cancel()
	type answer struct{ verdict, detail string }
	done := make(chan answer, 1) // buffered: a late answer must not block the goroutine that sends it
	go func() {
		v, d := probe(ctx)
		done <- answer{v, d}
	}()
	select {
	case a := <-done:
		return a.verdict, a.detail
	case <-ctx.Done():
		return "", "the probe did not finish in time"
	}
}

// captureFromDBs reads the capture's checkpoint from the index, then, only
// when there is one, where the source's binary log is now, and compares
// them. Unknown on any failure: the caller keeps the full backup. Each
// connection is bounded like the window probe (windowProbeTimeout, dial and
// reads); the source's reads go through config's helpers, which take no
// context, so its DSN carries the read timeout too.
func captureFromDBs(ctx context.Context, indexDSN, sourceDSN string) (verdict, detail string) {
	idx, err := config.Connect(probeDSN(indexDSN))
	if err != nil {
		return "", "the index did not answer"
	}
	defer idx.Close()
	cp, known := readCaptureCheckpoint(ctx, idx)
	if !known {
		return "", "the capture's checkpoint could not be read"
	}
	if !cp.present {
		return compareCapture(cp, sourceHead{})
	}
	src, err := config.Connect(sourceProbeDSN(sourceDSN))
	if err != nil {
		// Never the error text on the page: it can carry the host, and the
		// Debug line is enough to diagnose.
		slog.Debug("backup schedule: the source did not answer the capture probe", "error", err)
		return "", "the source did not answer"
	}
	defer src.Close()
	var head sourceHead
	if cp.mode == "gtid" {
		head.gtid, err = config.CurrentGTIDExecuted(src)
	} else {
		var pos uint32
		head.file, pos, err = config.CurrentBinlogPosition(src)
		head.pos = uint64(pos)
	}
	if err != nil {
		slog.Debug("backup schedule: the source did not report its binlog position", "error", err)
		return "", "the source did not report where its binlog is"
	}
	return compareCapture(cp, head)
}

// sourceProbeDSN is probeDSN plus read and write timeouts: config's source
// helpers run their queries without a context, so the connection itself has
// to bound them. A DSN that does not parse is handed on as is.
func sourceProbeDSN(dsn string) string {
	cfg, err := mysql.ParseDSN(probeDSN(dsn))
	if err != nil {
		return dsn
	}
	if cfg.ReadTimeout == 0 || cfg.ReadTimeout > windowProbeTimeout {
		cfg.ReadTimeout = windowProbeTimeout
	}
	if cfg.WriteTimeout == 0 || cfg.WriteTimeout > windowProbeTimeout {
		cfg.WriteTimeout = windowProbeTimeout
	}
	return cfg.FormatDSN()
}
