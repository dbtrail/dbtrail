package consoleapp

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/status"
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

// captureComparable reports whether st can be compared with the source at
// all, and why not: every condition that rules "caught up" out without
// asking the source. st is the index's stream_state (nil: no row), anchor
// the previous snapshot's instant. Each refusal keeps the full backup, the
// direction that cannot lose data:
//
//   - no live capture on record (a file-mode index), or a loss record this
//     index does not have (GapColumnsPresent false: unevaluable, not "no
//     gap");
//   - a loss stamped at or after the anchor: a capture that auto-advanced
//     past an unfillable gap reaches the head with the gap's events gone,
//     and only a full backup reads them again;
//   - a skip ledger that is unreadable (not clean: unevaluable), or holds a
//     skip at or after the anchor: a capture that drops every row it reads
//     (#1034) advances its checkpoint all the same and indexes nothing. An
//     acknowledgement changes nothing here: it records that an operator saw
//     the tally, not that the rows came back;
//   - position mode: on a source without GTIDs the parser emits no commit
//     boundary for an anonymous transaction, so the checkpoint stops at the
//     end of its last rows event, short of the Xid the source's head is
//     past, and a healthy capture would read as behind forever;
//   - no GTID set checkpointed.
func captureComparable(st *status.StreamStateInfo, anchor time.Time) (ok bool, detail string) {
	switch {
	case st == nil:
		return false, "the index has no live capture on record"
	case !st.GapColumnsPresent:
		return false, "the index predates the capture's loss record"
	case st.GapLostAt.Valid && !st.GapLostAt.Time.Before(anchor):
		return false, "the capture lost events to a binlog gap after the previous backup"
	}
	skips, readable := st.ParseCaptureSkips()
	if !readable {
		return false, "the capture's record of dropped events is not readable"
	}
	for _, sk := range skips {
		if sk.Count > 0 && (sk.LastAt.IsZero() || !sk.LastAt.Before(anchor)) {
			return false, "the capture dropped events it read after the previous backup"
		}
	}
	if st.Mode != "gtid" {
		return false, "the capture runs in binlog-position mode, which is not compared"
	}
	if !st.GTIDSet.Valid || strings.TrimSpace(st.GTIDSet.String) == "" {
		return false, "the capture has no GTID checkpoint"
	}
	return true, ""
}

// compareGTIDSets is the verdict on two GTID sets, pure:
// console.CaptureCaughtUp, console.CaptureBehind, or "" (unknown), and a
// clause saying why for anything but caught up. Caught up is EQUAL, not
// "contains": a healthy capture of a source can only hold less (behind) or
// the same, and holding more means the source's history changed under it
// (reset, restored, or replaced by a server that has not written those
// transactions), which is the stuck capture this check exists to catch.
func compareGTIDSets(captured, executed string) (verdict, detail string) {
	if strings.TrimSpace(executed) == "" {
		return "", "the source reported no GTID set"
	}
	have, err := gomysql.ParseMysqlGTIDSet(captured)
	if err != nil {
		return "", "the capture's GTID set does not parse"
	}
	wrote, err := gomysql.ParseMysqlGTIDSet(executed)
	if err != nil {
		return "", "the source's GTID set does not parse"
	}
	switch {
	case have.Equal(wrote):
		return console.CaptureCaughtUp, ""
	case wrote.Contain(have):
		// Not necessarily stopped: a transaction in the last checkpoint
		// interval, a trailing statement with no commit of its own (a GRANT
		// the parser commits with the next GTID) or a tagged GTID (8.3+,
		// which the parser does not record) read this way too. The
		// direction is safe: the full backup stays.
		return console.CaptureBehind, "the source reports transactions the capture's checkpoint does not include"
	}
	return "", "the capture holds transactions the source does not have: the source may have been reset, restored or replaced"
}

// captureProbeResult is one answer of the capture probe: the verdict and
// its reason for the page, and, when a read failed, its cause for the log
// only (scrubbed of both DSNs).
type captureProbeResult struct {
	verdict, detail, cause string
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
func probeCaptureFromDBs(ctx context.Context, indexDSN, sourceDSN string, anchor time.Time) captureProbeResult {
	return boundedCaptureProbe(ctx, windowProbeTimeout, func(ctx context.Context) captureProbeResult {
		return captureFromDBs(ctx, indexDSN, sourceDSN, anchor)
	})
}

// boundedCaptureProbe runs probe and waits at most within for it. The
// context is cancelled here, after the wait, never by the probe's goroutine:
// cancelled there, an answer sent in time and the expired context would be
// ready together, and the select below may take either.
func boundedCaptureProbe(ctx context.Context, within time.Duration, probe func(context.Context) captureProbeResult) captureProbeResult {
	ctx, cancel := context.WithTimeout(ctx, within)
	defer cancel()
	done := make(chan captureProbeResult, 1) // buffered: a late answer must not block the goroutine that sends it
	go func() { done <- probe(ctx) }()
	select {
	case r := <-done:
		return r
	case <-ctx.Done():
		return captureProbeResult{detail: "the probe did not finish in time", cause: "timed out after " + within.String()}
	}
}

// captureFromDBs reads the capture's stream_state from the index
// (status.LoadStreamState, which tolerates an index older than its newer
// columns) and, only when it is comparable, the source's executed GTID set,
// and compares them. Unknown on any failure: the caller keeps the full
// backup. Each connection is bounded like the window probe
// (windowProbeTimeout, dial and reads), and its queries by ctx.
func captureFromDBs(ctx context.Context, indexDSN, sourceDSN string, anchor time.Time) captureProbeResult {
	fail := func(detail string, err error) captureProbeResult {
		return captureProbeResult{detail: detail, cause: config.ScrubDSNError(err, indexDSN, sourceDSN)}
	}
	idx, err := config.Connect(probeDSN(indexDSN))
	if err != nil {
		return fail("the index did not answer", err)
	}
	defer idx.Close()
	st, err := status.LoadStreamState(ctx, idx)
	if err != nil {
		return fail("the capture's checkpoint could not be read", err)
	}
	// No connection to production for a verdict the index already settles.
	if ok, detail := captureComparable(st, anchor); !ok {
		return captureProbeResult{detail: detail}
	}
	src, err := config.Connect(sourceProbeDSN(sourceDSN))
	if err != nil {
		return fail("the source did not answer", err)
	}
	defer src.Close()
	// An index on the source server writes its own checkpoints there: each
	// one is a transaction the capture can only record at the next
	// checkpoint, so the two sets are never equal, and "behind" would be
	// said about a healthy capture forever.
	same, err := sameServer(ctx, idx, src)
	if err != nil {
		return fail("the index and the source could not be told apart", err)
	}
	if same {
		return captureProbeResult{detail: "the index lives on the source server, whose own checkpoint writes keep the source ahead of the capture"}
	}
	executed, err := readExecutedGTIDs(ctx, src)
	if err != nil {
		return fail("the source did not report its GTID set", err)
	}
	verdict, detail := compareGTIDSets(st.GTIDSet.String, executed)
	return captureProbeResult{verdict: verdict, detail: detail}
}

// readExecutedGTIDs is the source's @@GLOBAL.gtid_executed with its
// whitespace removed, or "" when GTIDs are not fully on (or the server has no
// gtid_mode at all, MariaDB's 1193): config.CurrentGTIDExecuted's answer, read
// here instead because that helper is the stream's start-position discovery.
// It warns about starting in position mode on an empty set, which is not
// this probe's news, and it takes no context.
func readExecutedGTIDs(ctx context.Context, db *sql.DB) (string, error) {
	var gtidMode, executed string
	err := db.QueryRowContext(ctx, "SELECT @@GLOBAL.gtid_mode, @@GLOBAL.gtid_executed").Scan(&gtidMode, &executed)
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) && mysqlErr.Number == 1193 { // ER_UNKNOWN_SYSTEM_VARIABLE
		return "", nil
	}
	if err != nil {
		return "", err
	}
	if !strings.EqualFold(gtidMode, "ON") {
		return "", nil
	}
	return strings.Join(strings.Fields(executed), ""), nil
}

// sameServer reports whether a and b are the same MySQL server, by
// @@server_uuid. A server without one (MariaDB, 1193) is never "the same":
// this check only guards the GTID comparison, which MariaDB never reaches.
func sameServer(ctx context.Context, a, b *sql.DB) (bool, error) {
	ua, err := serverUUID(ctx, a)
	if err != nil {
		return false, err
	}
	ub, err := serverUUID(ctx, b)
	if err != nil {
		return false, err
	}
	return ua != "" && strings.EqualFold(ua, ub), nil
}

func serverUUID(ctx context.Context, db *sql.DB) (string, error) {
	var uuid string
	err := db.QueryRowContext(ctx, "SELECT @@GLOBAL.server_uuid").Scan(&uuid)
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) && mysqlErr.Number == 1193 { // ER_UNKNOWN_SYSTEM_VARIABLE
		return "", nil
	}
	return uuid, err
}

// sourceProbeDSN is probeDSN plus read and write timeouts: config.Connect's
// ping runs without the probe's context, so the connection itself bounds it
// alongside the dial. A DSN that does not parse is handed on as is.
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
