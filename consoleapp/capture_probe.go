package consoleapp

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync/atomic"
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
// since the previous snapshot and that snapshot is past the cut-over age
// (console.BackupCutoverAge).
// There, a full backup used to follow at every slot, because from the index
// alone "the source wrote nothing" and "the capture stopped" read the same
// (#1223): the binlog syncer retries without limit (MaxReconnectAttempts 0)
// and the stream's checkpoint ticker keeps stamping stream_state while it
// does, so a fresh checkpoint proves the loop runs, not that it captures.
// The source is what can tell: has it written past the capture's checkpoint?
// Only a definite "no" lets the update run. A source that is ahead, a
// capture whose own record says it lost changes, and any answer under which
// a loss cannot be ruled out keep the full backup. A source that cannot be
// asked, for one of the reasons listed for it (captureClasses), is no
// evidence either way, and that slot is skipped (#2659,
// console.WindowChoice).

// captureComparable reports whether st can be compared with the source at
// all, and why not: every condition that rules "caught up" out without
// asking the source. st is the index's stream_state (nil: no row), anchor
// the previous snapshot's instant, sourceRead when the newest full backup
// that read the source started (BaselineRunHistory.LastSourceRead; zero:
// none on record). No refusal lets the update run as if capture were
// confirmed; what the schedule does with each is captureClasses (#2659):
//
//   - no live capture on record (a file-mode index), or a loss record this
//     index does not have (GapColumnsPresent false: unevaluable, not "no
//     gap");
//   - a loss stamped at or after the anchor: a capture that auto-advanced
//     past an unfillable gap reaches the head with the gap's events gone,
//     and only a full backup reads them again. Acknowledged or not: this
//     reads GapLostAt, which an acknowledgement (GapAckedAt) leaves set;
//   - a skip ledger that is unreadable (not clean: unevaluable), or holds a
//     skip no full backup has read the source after: a capture that drops
//     every row it reads (#1034) advances its checkpoint all the same and
//     indexes nothing. Dated against sourceRead, not the anchor: an update
//     is folded from the index, which never received a dropped row, so only
//     a read of the source brings one back; with none on record, any skip
//     refuses. An acknowledgement changes nothing here: it records that an
//     operator saw the tally, not that the rows came back;
//   - position mode: on a source without GTIDs the parser emits no commit
//     boundary for an anonymous transaction, so the checkpoint stops at the
//     end of its last rows event, short of the Xid the source's head is
//     past, and a healthy capture would read as behind forever;
//   - no GTID set checkpointed.
func captureComparable(st *status.StreamStateInfo, anchor, sourceRead time.Time) (ok bool, detail string) {
	switch {
	case st == nil:
		return false, captureDetailNoCapture
	case !st.GapColumnsPresent:
		return false, captureDetailNoLossRecord
	case st.GapLostAt.Valid && !st.GapLostAt.Time.Before(anchor):
		return false, captureDetailLost
	}
	skips, readable := st.ParseCaptureSkips()
	if !readable {
		return false, captureDetailSkipsUnreadable
	}
	for _, sk := range skips {
		if sk.Count > 0 && (sourceRead.IsZero() || sk.LastAt.IsZero() || !sk.LastAt.Before(sourceRead)) {
			return false, captureDetailDropped
		}
	}
	return checkpointComparable(st)
}

// checkpointComparable is the part of captureComparable that is about the
// checkpoint itself and not about what a snapshot needs: a capture on
// record, in GTID mode, with a GTID set saved. The capture status read
// (#1794) refuses on these and on nothing else: a loss or a dropped row is
// said by the page's own continuity and capture health lines.
func checkpointComparable(st *status.StreamStateInfo) (ok bool, detail string) {
	if st == nil {
		return false, captureDetailNoCapture
	}
	if st.Mode != "gtid" {
		return false, captureDetailPositionMode
	}
	if !st.GTIDSet.Valid || strings.TrimSpace(st.GTIDSet.String) == "" {
		return false, captureDetailNoCheckpoint
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
//
// An EMPTY set on the source is a set like any other, and the caller hands
// one over only for a source with GTIDs fully on (gtidModeRefusal): a
// capture that holds transactions beside it holds what the source does not
// have, which is what a source after RESET MASTER looks like.
func compareGTIDSets(captured, executed string) (verdict, detail string) {
	have, err := gomysql.ParseMysqlGTIDSet(captured)
	if err != nil {
		return "", captureDetailCaptureUnparsed
	}
	wrote, err := gomysql.ParseMysqlGTIDSet(executed)
	if err != nil {
		return "", captureDetailSourceUnparsed
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
		return console.CaptureBehind, captureDetailBehind
	}
	return "", captureDetailDiverged
}

// Every reason the capture probe gives for not answering "caught up"
// (#1791), as constants: the snapshot schedule decides on them
// (captureClassOf), and a reason written as a bare string would be decided
// by nobody. A test reads this file and the schedule loop and fails on a
// detail that is not one of these, or on one of these with no class.
const (
	// What the index's own record of the capture says.
	captureDetailNoCapture       = "the index has no live capture on record"
	captureDetailNoLossRecord    = "the index predates the capture's loss record"
	captureDetailLost            = "the capture lost events to a binlog gap after the previous snapshot"
	captureDetailSkipsUnreadable = "the capture's record of dropped events is not readable"
	captureDetailDropped         = "the capture dropped events that no full read has read from the source since"
	captureDetailPositionMode    = "the capture runs in binlog-position mode, which is not compared"
	captureDetailNoCheckpoint    = "the capture has no GTID checkpoint"
	// What comparing the two GTID sets found.
	// The source's gtid_mode, read with its executed set (gtidModeRefusal).
	// The probe gets this far only for a capture that follows the source by
	// GTID and has a GTID position saved. MariaDB has no such variable. A
	// MySQL source serves a reader by GTID only with gtid_mode=ON (the
	// manual, CHANGE REPLICATION SOURCE TO, SOURCE_AUTO_POSITION:
	// "GTID_MODE=ON on the source"), so under any other value the capture
	// cannot read from it while the source keeps writing. Verified on MySQL
	// 8.0.46 in each of the three (#2674,
	// TestIntegrationGTIDModeRefusesCaptureByGTID_2674): the source answers
	// capture's GTID request, and `bintrail stream` resuming from a GTID
	// checkpoint, with ERROR 1236 "The replication sender thread cannot
	// start in AUTO_POSITION mode: this server has GTID_MODE = <mode>
	// instead of ON.", and serves it under ON.
	captureDetailNoGTIDMode        = "the source has no gtid_mode (a MariaDB source), which is not compared"
	captureDetailGTIDModeOff       = "GTIDs are off on the source (gtid_mode=OFF) while capture follows it by GTID, so capture cannot read from it"
	captureDetailGTIDOffPermissive = "the source's gtid_mode is OFF_PERMISSIVE, not ON, while capture follows it by GTID, so capture cannot read from it"
	captureDetailGTIDOnPermissive  = "the source's gtid_mode is ON_PERMISSIVE, not ON, while capture follows it by GTID, so capture cannot read from it"
	captureDetailGTIDModeOther     = "the source's gtid_mode is not ON while capture follows it by GTID, so capture cannot read from it"
	captureDetailCaptureUnparsed   = "the capture's GTID set does not parse"
	captureDetailSourceUnparsed    = "the source's GTID set does not parse"
	captureDetailBehind            = "the source reports transactions the capture's checkpoint does not include"
	captureDetailDiverged          = "the capture holds transactions the source does not have: the source may have been reset, restored or replaced"
	// What reaching the two servers ran into.
	captureDetailCancelled = "the request that asked was cancelled"
	// captureDetailTimedOut is the bound on the whole probe running out
	// with nothing said about how far it got. The schedule's own probe
	// never returns it: it says which side was being read
	// (probeCaptureFromDBs). The capture status read does.
	captureDetailTimedOut         = "the probe did not finish in time"
	captureDetailIndexSlow        = "the index did not answer in time"
	captureDetailSourceSlow       = "the source did not answer in time"
	captureDetailIndexSilent      = "the index did not answer"
	captureDetailCheckpointUnread = "the capture's checkpoint could not be read"
	captureDetailSourceSilent     = "the source did not answer"
	captureDetailNotToldApart     = "the index and the source could not be told apart"
	captureDetailSameServer       = "the index lives on the source server, whose own checkpoint writes keep the source ahead of the capture"
	captureDetailSourceGTIDUnread = "the source did not report its GTID set"
	// The schedule's own, before the probe is tried (captureVerdict).
	captureDetailNoSource = "this server has no source to ask"
	captureDetailPostgres = "PostgreSQL sources are not compared yet"
	// The capture status read's own (#1794: its MariaDB comparison and its
	// panic guard). The snapshot schedule's probe never returns them; they
	// are constants here so that every reason is, and are decided like any
	// reason nobody listed.
	captureDetailMariaDBNoPositionVar  = "the source has no MariaDB GTID position: is it a MariaDB server?"
	captureDetailMariaDBPositionUnread = "the source did not report its GTID position"
	captureDetailMariaDBNoPosition     = "the source reported no GTID position"
	captureDetailMariaDBSourceUnparsed = "the source's GTID position does not parse"
	captureDetailMariaDBAhead          = "the source is ahead of the capture's checkpoint; a MariaDB source is only compared for an exact match"
	captureDetailMariaDBMismatch       = "the capture's checkpoint and the source's GTID position do not match: the source may have been reset, restored or replaced"
	captureDetailPanicked              = "the source could not be read"
)

// captureClass is what the snapshot schedule does with a quiet index whose
// capture was not confirmed caught up, by the reason (#2659).
type captureClass int

const (
	// captureUnsettled is the zero value on purpose: a reason nobody
	// classified, one added tomorrow included, keeps what the schedule did
	// before the skip existed, the full read. Also the explicit class of
	// the reasons under which a loss cannot be ruled out.
	captureUnsettled captureClass = iota
	// captureEvidence: something on record says the index is missing
	// changes, or holds some the source does not have. The full read, with
	// the reason worded as what was found.
	captureEvidence
	// captureCannotAsk: the source cannot be asked, and nothing says a
	// change was missed. The slot is skipped (console.WindowChoice).
	captureCannotAsk
	// captureIndexUnread: the index itself could not be read for the
	// capture's record, so nothing was learned about the source or about
	// what the snapshot would lose by waiting. The update runs.
	captureIndexUnread
)

// captureClasses is the decision for each reason. Only the reasons the
// decision on #2659 names skip the slot; the rest keep the full read.
var captureClasses = map[string]captureClass{
	captureDetailNoCapture:        captureCannotAsk,
	captureDetailPositionMode:     captureCannotAsk,
	captureDetailNoCheckpoint:     captureCannotAsk,
	captureDetailNoGTIDMode:       captureCannotAsk,
	captureDetailSourceSlow:       captureCannotAsk,
	captureDetailSourceSilent:     captureCannotAsk,
	captureDetailSourceGTIDUnread: captureCannotAsk,
	captureDetailNotToldApart:     captureCannotAsk,
	captureDetailSameServer:       captureCannotAsk,
	captureDetailNoSource:         captureCannotAsk,
	captureDetailPostgres:         captureCannotAsk,

	captureDetailLost:     captureEvidence,
	captureDetailDropped:  captureEvidence,
	captureDetailDiverged: captureEvidence,
	// The verdict console.CaptureBehind decides this one before any class
	// is read; listed so that it is decided here too.
	captureDetailBehind: captureEvidence,
	// A capture by GTID that the source refuses to serve, while the source
	// keeps writing: what was found, and a loss in the making.
	captureDetailGTIDModeOff:       captureEvidence,
	captureDetailGTIDOffPermissive: captureEvidence,
	captureDetailGTIDOnPermissive:  captureEvidence,
	captureDetailGTIDModeOther:     captureEvidence,

	captureDetailNoLossRecord:    captureUnsettled,
	captureDetailSkipsUnreadable: captureUnsettled,
	captureDetailCaptureUnparsed: captureUnsettled,
	captureDetailSourceUnparsed:  captureUnsettled,

	captureDetailIndexSilent:      captureIndexUnread,
	captureDetailIndexSlow:        captureIndexUnread,
	captureDetailCheckpointUnread: captureIndexUnread,
	captureDetailCancelled:        captureIndexUnread,
	// The time ran out and nothing says on which side: the index's record
	// of the capture, where evidence of a loss would be, may not have been
	// read. Never the skip.
	captureDetailTimedOut: captureIndexUnread,

	// The capture status read's own. Not reachable from the schedule.
	captureDetailMariaDBNoPositionVar:  captureUnsettled,
	captureDetailMariaDBPositionUnread: captureUnsettled,
	captureDetailMariaDBNoPosition:     captureUnsettled,
	captureDetailMariaDBSourceUnparsed: captureUnsettled,
	captureDetailMariaDBAhead:          captureUnsettled,
	captureDetailMariaDBMismatch:       captureUnsettled,
	captureDetailPanicked:              captureUnsettled,
}

// gtidModeRefusal is why a source with this gtid_mode is not compared, or
// "" for ON, the one mode whose executed set is compared. mode is the
// source's @@GLOBAL.gtid_mode, empty for a server with no such variable.
func gtidModeRefusal(mode string) string {
	switch strings.ToUpper(strings.TrimSpace(mode)) {
	case "ON":
		return ""
	case "":
		return captureDetailNoGTIDMode
	case "OFF":
		return captureDetailGTIDModeOff
	case "OFF_PERMISSIVE":
		return captureDetailGTIDOffPermissive
	case "ON_PERMISSIVE":
		return captureDetailGTIDOnPermissive
	}
	return captureDetailGTIDModeOther
}

// captureClassOf is the class of a probe reason; one that is not in the
// table is captureUnsettled, the full read.
func captureClassOf(detail string) captureClass {
	return captureClasses[detail]
}

// captureProbeResult is one answer of the capture probe: the verdict and
// its reason for the page, and, when a read failed, its cause for the log
// only (scrubbed of both DSNs).
type captureProbeResult struct {
	verdict, detail, cause string
	// For the capture status read (#1794): the two sets that were compared,
	// and when the capture last saved its position. Empty when the source
	// was not read.
	captured, executed string
	checkpoint         time.Time
	// purged is the source's @@GLOBAL.gtid_purged, read with executed:
	// transactions it counts as executed that no binlog carries, so capture
	// can never read them (#1794).
	purged string
	// logsAll is true when the source was asked for its binary log filters
	// and has none; logFilter says otherwise why not (a filter, or the read
	// that failed). Only the capture status read asks, for the watermark
	// (#2085, capture_watermark.go).
	logsAll   bool
	logFilter string
	// uncomparable is true when the capture's checkpoint itself cannot be
	// compared with a source (checkpointComparable refused): no capture on
	// record, position mode, no GTID set saved. Not the same as a source
	// that was not reached: here what was proven before is void (#2085).
	uncomparable bool
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
//
// When the bound runs out, which side was being read decides what the
// schedule does (#2659): the index's record of the capture is where a loss
// shows (dropped rows, a gap), so a probe that ran out before that record
// was read learned nothing and must not read as "the source cannot be
// asked". Only once the index's part is done is it the source that did not
// answer in time.
func probeCaptureFromDBs(ctx context.Context, indexDSN, sourceDSN string, ssl config.SSL, anchor, sourceRead time.Time) captureProbeResult {
	return probeCaptureAgainstFromDBs(ctx, indexDSN, sourceDSN, ssl, anchor, sourceRead, "")
}

// probeCaptureAgainst is probeCapture with the source compared against
// another GTID set than capture's: the snapshot's, when the snapshot's own
// coordinates showed that capture recorded nothing since it (#2720 review,
// asOfSnapshot). A package variable for the reason probeCapture is.
var probeCaptureAgainst = probeCaptureAgainstFromDBs

func probeCaptureAgainstFromDBs(ctx context.Context, indexDSN, sourceDSN string, ssl config.SSL, anchor, sourceRead time.Time, against string) captureProbeResult {
	var indexRead atomic.Bool
	r := boundedCaptureProbe(ctx, windowProbeTimeout, func(ctx context.Context) captureProbeResult {
		return captureFromDBsAgainst(ctx, indexDSN, sourceDSN, ssl, anchor, sourceRead, &indexRead, against)
	})
	return timedOutSide(r, indexRead.Load())
}

// timedOutSide names the side a timed-out probe was reading: indexRead says
// whether the index's record of the capture had been read by then.
func timedOutSide(r captureProbeResult, indexRead bool) captureProbeResult {
	if r.detail != captureDetailTimedOut {
		return r
	}
	if indexRead {
		r.detail = captureDetailSourceSlow
	} else {
		r.detail = captureDetailIndexSlow
	}
	return r
}

// boundedCaptureProbe runs probe and waits at most within for it. The
// context is cancelled here, after the wait, never by the probe's goroutine:
// cancelled there, an answer sent in time and the expired context would be
// ready together, and the select below may take either. A caller that went
// away first is not a timeout, and is not reported as one.
func boundedCaptureProbe(parent context.Context, within time.Duration, probe func(context.Context) captureProbeResult) captureProbeResult {
	ctx, cancel := context.WithTimeout(parent, within)
	defer cancel()
	done := make(chan captureProbeResult, 1) // buffered: a late answer must not block the goroutine that sends it
	go func() { done <- probe(ctx) }()
	select {
	case r := <-done:
		return r
	case <-ctx.Done():
		if parent.Err() != nil {
			return captureProbeResult{detail: captureDetailCancelled}
		}
		return captureProbeResult{detail: captureDetailTimedOut, cause: "timed out after " + within.String()}
	}
}

// captureFromDBs reads the capture's stream_state from the index
// (status.LoadStreamState, which tolerates an index older than its newer
// columns) and hands it to compareCapture with a way to reach the source.
// Unknown on any failure: the caller keeps the full backup. Each connection
// is bounded like the window probe (windowProbeTimeout, dial and reads),
// and its queries by ctx; a failure's cause is scrubbed of both DSNs.
func captureFromDBs(ctx context.Context, indexDSN, sourceDSN string, ssl config.SSL, anchor, sourceRead time.Time, indexRead *atomic.Bool) captureProbeResult {
	return captureFromDBsAgainst(ctx, indexDSN, sourceDSN, ssl, anchor, sourceRead, indexRead, "")
}

// captureFromDBsAgainst is captureFromDBs with the checkpoint's GTID set
// replaced by against when that is set (asOfSnapshot).
func captureFromDBsAgainst(ctx context.Context, indexDSN, sourceDSN string, ssl config.SSL, anchor, sourceRead time.Time, indexRead *atomic.Bool, against string) captureProbeResult {
	idx, err := config.Connect(probeDSN(indexDSN))
	if err != nil {
		return captureProbeResult{detail: captureDetailIndexSilent, cause: config.ScrubDSNError(err, indexDSN, sourceDSN)}
	}
	defer idx.Close()
	st, err := status.LoadStreamState(ctx, idx)
	if err != nil {
		return captureProbeResult{detail: captureDetailCheckpointUnread, cause: config.ScrubDSNError(err, indexDSN, sourceDSN)}
	}
	// The capture's record is read: from here on only the source is slow.
	if indexRead != nil {
		indexRead.Store(true)
	}
	st = asOfSnapshot(st, against)
	r, err := compareCapture(ctx, idx, st, anchor, sourceRead, func() (*sql.DB, error) {
		return connectSource(sourceProbeDSN(sourceDSN), ssl)
	})
	if err != nil {
		r.cause = config.ScrubDSNError(err, indexDSN, sourceDSN)
	}
	return r
}

// compareCapture is the verdict on st, the capture's stream_state read from
// idx: refused without touching the source when captureComparable refuses,
// else compared with the executed GTID set of the source openSource
// connects to. A failed step returns its detail with the error, which the
// caller scrubs; the source connection is closed here.
func compareCapture(ctx context.Context, idx *sql.DB, st *status.StreamStateInfo, anchor, sourceRead time.Time, openSource func() (*sql.DB, error)) (captureProbeResult, error) {
	// No connection to production for a verdict the index already settles.
	if ok, detail := captureComparable(st, anchor, sourceRead); !ok {
		return captureProbeResult{detail: detail}, nil
	}
	return compareWithSource(ctx, idx, st, openSource, readExecutedOnly)
}

// sourceGTIDs is what a source says about its GTIDs: its gtid_mode as
// reported (empty for a server with no such variable, MariaDB's 1193), its
// executed set and, where the caller reads it, its purged set, both with
// their whitespace removed. The three states that used to read alike as "no
// GTID set" are told apart by mode: no variable, a mode that is not ON, and
// ON with an empty set.
type sourceGTIDs struct {
	mode, executed, purged string
}

// sourceGTIDReader reads the source's gtid_mode and executed GTID set, and,
// where the caller uses it, its purged set.
type sourceGTIDReader func(ctx context.Context, db *sql.DB) (sourceGTIDs, error)

// readExecutedOnly is the snapshot schedule's reader: it reads no purged
// set, since there "behind" keeps the full backup, the safe direction.
func readExecutedOnly(ctx context.Context, db *sql.DB) (sourceGTIDs, error) {
	return readExecutedGTIDs(ctx, db)
}

// compareWithSource is the half of compareCapture that reaches the source,
// for a stream_state that was already found comparable: the same-server
// check, the source's GTID sets (read), and the verdict on the two sets.
// The capture status read (#1794) shares it.
func compareWithSource(ctx context.Context, idx *sql.DB, st *status.StreamStateInfo, openSource func() (*sql.DB, error), read sourceGTIDReader) (captureProbeResult, error) {
	src, err := openSource()
	if err != nil {
		return captureProbeResult{detail: captureDetailSourceSilent}, err
	}
	defer src.Close()
	// An index on the source server writes its own checkpoints there: each
	// one is a transaction the capture can only record at the next
	// checkpoint, so the two sets are never equal, and "behind" would be
	// said about a healthy capture forever.
	same, err := sameServer(ctx, idx, src)
	if err != nil {
		return captureProbeResult{detail: captureDetailNotToldApart}, err
	}
	if same {
		return captureProbeResult{detail: captureDetailSameServer}, nil
	}
	got, err := read(ctx, src)
	if err != nil {
		return captureProbeResult{detail: captureDetailSourceGTIDUnread}, err
	}
	// Only a source with GTIDs fully on has an executed set to compare. The
	// capture's checkpoint is a GTID set here (checkpointComparable), so a
	// source in any other mode is one the capture cannot be reading.
	if refusal := gtidModeRefusal(got.mode); refusal != "" {
		return captureProbeResult{detail: refusal}, nil
	}
	verdict, why := compareGTIDSets(st.GTIDSet.String, got.executed)
	r := captureProbeResult{verdict: verdict, captured: strings.Join(strings.Fields(st.GTIDSet.String), ""),
		executed: got.executed, purged: got.purged, checkpoint: st.LastCheckpoint}
	r.detail = why
	return r, nil
}

// readExecutedGTIDs is the source's @@GLOBAL.gtid_mode and
// @@GLOBAL.gtid_executed, read in one statement. A server with no gtid_mode
// at all (MariaDB's 1193) answers with an empty mode and no error. Not
// config.CurrentGTIDExecuted: that helper is the stream's start-position
// discovery, it warns about starting in position mode on an empty set, which
// is not this probe's news, and it takes no context.
func readExecutedGTIDs(ctx context.Context, db *sql.DB) (sourceGTIDs, error) {
	var got sourceGTIDs
	err := db.QueryRowContext(ctx, "SELECT @@GLOBAL.gtid_mode, @@GLOBAL.gtid_executed").Scan(&got.mode, &got.executed)
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) && mysqlErr.Number == 1193 { // ER_UNKNOWN_SYSTEM_VARIABLE
		return sourceGTIDs{}, nil
	}
	if err != nil {
		return sourceGTIDs{}, err
	}
	got.executed = strings.Join(strings.Fields(got.executed), "")
	return got, nil
}

// readExecutedAndPurgedGTIDs is readExecutedGTIDs plus @@GLOBAL.gtid_purged,
// in the same statement so both sets are of one instant. Only the capture
// status read (#1794) uses it.
func readExecutedAndPurgedGTIDs(ctx context.Context, db *sql.DB) (sourceGTIDs, error) {
	var got sourceGTIDs
	err := db.QueryRowContext(ctx, "SELECT @@GLOBAL.gtid_mode, @@GLOBAL.gtid_executed, @@GLOBAL.gtid_purged").Scan(&got.mode, &got.executed, &got.purged)
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) && mysqlErr.Number == 1193 { // ER_UNKNOWN_SYSTEM_VARIABLE
		return sourceGTIDs{}, nil
	}
	if err != nil {
		return sourceGTIDs{}, err
	}
	got.executed = strings.Join(strings.Fields(got.executed), "")
	got.purged = strings.Join(strings.Fields(got.purged), "")
	return got, nil
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
