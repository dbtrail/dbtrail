package verify

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/dbtrail/internal/consistency"
	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/query"
)

// #2150: live-source verify cuts the reconstruction at the source snapshot's
// own GTID position instead of at the wall-clock time the scan ended. A
// change committed while the table was being read is then left out of the
// reconstruction exactly as it is left out of the snapshot, so a table with
// steady writes compares equal.
//
// The position is a GTID set; the read's exact upper bound
// (query.Options.UntilPos) is a binlog coordinate. snapshotCut translates the
// one into the other from the index: the end of the newest indexed change
// whose GTID the set holds. That is exact because, on one server, the binary
// log is written in commit order (binlog_order_commits, MySQL's default) and
// a consistent snapshot sees a prefix of it: every change after the last one
// the snapshot holds is one it does not hold, so no later change can belong
// below the cut.

// snapshotMember reports whether one indexed change, by its GTID, is one the
// snapshot holds. An error means its GTID cannot be judged.
type snapshotMember func(gtid string) (bool, error)

// errTaggedGTIDSet: the snapshot's set carries tagged GTIDs (MySQL 8.3+). The
// index records a change's GTID without its tag (and uuid:5 and uuid:tag:5
// are two different transactions), so membership cannot be told.
var errTaggedGTIDSet = errors.New("the source's GTID set carries tagged GTIDs, which the index does not record")

// newSnapshotMembership builds the membership test for the snapshot position
// set, in flavor's format (consistency.GTIDFlavor*).
//
// MySQL: plain set containment of the change's uuid:gno.
//
// MariaDB: @@gtid_binlog_pos holds the last GTID per domain, and a sequence
// number is domain-wide (a failover gives the next transaction another server
// id and a higher sequence), so a change is in the snapshot when the snapshot
// has its domain at an equal or higher sequence: the same comparison
// indexCoversMariaDB makes. A domain the snapshot has not seen is a domain
// that started after it.
func newSnapshotMembership(flavor, set string) (snapshotMember, error) {
	switch flavor {
	case consistency.GTIDFlavorMySQL:
		parsed, err := gomysql.ParseMysqlGTIDSet(strings.Join(strings.Fields(set), ""))
		if err != nil {
			return nil, fmt.Errorf("the snapshot's GTID set does not parse: %w", err)
		}
		snap := parsed.(*gomysql.MysqlGTIDSet)
		for _, tags := range *snap {
			for tag := range tags {
				if tag != (gomysql.Tag{}) {
					return nil, errTaggedGTIDSet
				}
			}
		}
		return func(gtid string) (bool, error) {
			one, err := gomysql.ParseMysqlGTIDSet(strings.TrimSpace(gtid))
			if err != nil {
				return false, fmt.Errorf("indexed GTID %q does not parse: %v", gtid, err)
			}
			// One change carries one uuid:gno. A range or a list parses as
			// a set too; it is not what the index stores for a change.
			if !singleGTID(one.(*gomysql.MysqlGTIDSet)) {
				return false, fmt.Errorf("indexed GTID %q is not one transaction's", gtid)
			}
			return snap.Contain(one), nil
		}, nil
	case consistency.GTIDFlavorMariaDB:
		pos, err := parser.ParseMariaDBPosition(set)
		if err != nil {
			return nil, fmt.Errorf("the snapshot's GTID position does not parse: %w", err)
		}
		return func(gtid string) (bool, error) {
			g, err := gomysql.ParseMariadbGTID(strings.TrimSpace(gtid))
			if err != nil || strings.TrimSpace(gtid) == "" {
				return false, fmt.Errorf("indexed GTID %q does not parse: %v", gtid, err)
			}
			have, ok := pos[g.DomainID]
			return ok && g.SequenceNumber <= have.SequenceNumber, nil
		}, nil
	default:
		return nil, fmt.Errorf("no GTID format %q", flavor)
	}
}

// singleGTID reports whether s is exactly one untagged uuid:gno.
func singleGTID(s *gomysql.MysqlGTIDSet) bool {
	if len(*s) != 1 {
		return false
	}
	for _, tags := range *s {
		ivs, ok := tags[gomysql.Tag{}]
		return ok && len(tags) == 1 && len(ivs) == 1 && ivs[0].Stop-ivs[0].Start == 1
	}
	return false
}

// snapshotCutPage is how many changes one step of the walk reads.
const snapshotCutPage = 1000

// snapshotCut returns the binlog coordinate the read must stop at so that it
// holds exactly the changes the snapshot holds, or a reason it cannot be
// placed (the caller reports that inconclusive: a comparison against a
// guessed cut is not one).
//
// It walks the live index from its newest change down, in event_id order,
// which the caller has proven to be binary log order (query.IDsFollowStream):
// changes the snapshot does not hold are skipped, and the first one it holds
// ends the walk; its end position is the cut. Capture keeps inserting while
// this runs, and everything it inserts later sorts after the cut, so it stays
// out. The cost is the changes indexed since the snapshot opened.
//
// A change on the way down whose GTID cannot be judged (none recorded, or one
// that does not parse) cannot be placed on either side of the cut, so the
// cut is not placed.
//
// The walk reads only changes stamped at or after floor (the time the read
// opened, less snapshotCutFloorMargin), so MySQL prunes it to the recent
// hourly partitions instead of merging every one of them on each page. Past
// the floor:
//   - if it skipped changes, every change of the last hour and more is newer
//     than the snapshot (a read that took that long): the cut is not placed;
//   - if it skipped none, nothing was indexed near the read at all (a quiet
//     source). The newest change below the floor is then read alone
//     (one row, ORDER BY event_id DESC LIMIT 1): held by the snapshot, its end
//     is the cut; not held, or not judgeable, the cut is not placed.
//
// A change stamped below the floor but committed after a change above it (a
// transaction that ran for longer than the margin) is not seen by the walk.
// If the snapshot does not hold it, it sorts after the cut anyway; if it
// does, the cut lands before it and the comparison reports a mismatch, never
// a false match.
//
// An empty index has nothing to walk. Its cut is the capture's checkpoint
// position, read BEFORE the walk: the coverage check proved the checkpoint
// holds the snapshot, so every change the snapshot holds ends at or before
// it; and a change the capture wrote at or before it was indexed before the
// checkpoint was saved, so the walk that found nothing would have found it.
//
// firstOut is where the oldest change the walk skipped (the first one the
// snapshot does not hold) starts; nil when it skipped none.
func snapshotCut(ctx context.Context, db *sql.DB, member snapshotMember, floor time.Time) (cut, firstOut *query.BinlogPos, why string, err error) {
	return snapshotCutPaged(ctx, db, member, floor, snapshotCutPage)
}

// snapshotCutFloorMargin is how far before the read's opening the walk looks
// (see snapshotCut): a change's stamp is its statement's start, so one held
// by the snapshot can be stamped before the read by as long as its
// transaction ran.
const snapshotCutFloorMargin = time.Hour

func snapshotCutPaged(ctx context.Context, db *sql.DB, member snapshotMember, floor time.Time, page int) (cut, firstOut *query.BinlogPos, why string, err error) {
	var ckFile sql.NullString
	var ckPos sql.NullInt64
	err = db.QueryRowContext(ctx, "SELECT binlog_file, binlog_position FROM stream_state WHERE id = 1").Scan(&ckFile, &ckPos)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, nil, "", fmt.Errorf("read the capture's checkpoint: %w", err)
	}

	floor = floor.UTC()
	const cols = snapshotCutCols
	above := snapshotCutAbove(floor)
	var (
		last    uint64
		skipped int
		first   = true
	)
	// judge decides one change: stop with a cut, stop unplaced, or go on.
	judge := func(rows *sql.Rows) (done bool, cut *query.BinlogPos, why string, err error) {
		var (
			id         uint64
			gtid       sql.NullString
			file       string
			start, end uint64
		)
		if err := rows.Scan(&id, &gtid, &file, &start, &end); err != nil {
			return true, nil, "", fmt.Errorf("walk the index from its newest change: %w", err)
		}
		last = id
		if !gtid.Valid || strings.TrimSpace(gtid.String) == "" {
			return true, nil, fmt.Sprintf("the read cannot be cut at the snapshot: indexed change %d (%s:%d) has no GTID, so whether the snapshot holds it cannot be told", id, file, end), nil
		}
		in, err := member(gtid.String)
		if err != nil {
			return true, nil, "the read cannot be cut at the snapshot: " + err.Error(), nil
		}
		if in {
			return true, &query.BinlogPos{File: file, Pos: end}, "", nil
		}
		skipped++
		firstOut = &query.BinlogPos{File: file, Pos: start}
		return false, nil, "", nil
	}
	for {
		var rows *sql.Rows
		if first {
			rows, err = db.QueryContext(ctx, snapshotCutFirstPageSQL(floor, page), floor)
		} else {
			rows, err = db.QueryContext(ctx, cols+above+fmt.Sprintf(" AND event_id < ? ORDER BY event_id DESC LIMIT %d", page), floor, last)
		}
		if err != nil {
			return nil, nil, "", fmt.Errorf("walk the index from its newest change: %w", err)
		}
		n := 0
		for rows.Next() {
			n++
			done, c, why, err := judge(rows)
			if done {
				rows.Close()
				if c != nil {
					return c, firstOut, "", nil
				}
				return nil, nil, why, err
			}
		}
		err := rows.Err()
		rows.Close()
		if err != nil {
			return nil, nil, "", fmt.Errorf("walk the index from its newest change: %w", err)
		}
		first = false
		if n < page {
			break
		}
	}
	if skipped > 0 {
		return nil, nil, fmt.Sprintf("the read cannot be cut at the snapshot: all %d changes indexed since %s are newer than the snapshot", skipped, floor.Format(time.RFC3339)), nil
	}
	// Nothing indexed near the read: the newest change below the floor.
	rows, err := db.QueryContext(ctx, cols+" WHERE event_timestamp < ? ORDER BY event_id DESC LIMIT 1", floor)
	if err != nil {
		return nil, nil, "", fmt.Errorf("read the newest indexed change: %w", err)
	}
	defer rows.Close()
	if rows.Next() {
		_, c, why, err := judge(rows)
		if c != nil {
			return c, nil, "", nil
		}
		if why == "" && err == nil {
			why = "the read cannot be cut at the snapshot: the newest indexed change, from before " + floor.Format(time.RFC3339) + ", is not one the snapshot holds"
		}
		return nil, nil, why, err
	}
	if err := rows.Err(); err != nil {
		return nil, nil, "", fmt.Errorf("read the newest indexed change: %w", err)
	}
	if !ckFile.Valid || ckFile.String == "" || !ckPos.Valid || ckPos.Int64 <= 0 {
		return nil, nil, "the read cannot be cut at the snapshot: the index holds no change and its checkpoint has no position", nil
	}
	return &query.BinlogPos{File: ckFile.String, Pos: uint64(ckPos.Int64)}, nil, "", nil
}

const snapshotCutCols = "SELECT event_id, gtid, binlog_file, start_pos, end_pos FROM binlog_events"

// snapshotCutAbove is the walk's floor condition, with floor as its one
// parameter. The TO_SECONDS literal prunes partitions the way
// query.buildQuery's hints do; the plain comparison is the exact filter.
func snapshotCutAbove(floor time.Time) string {
	return fmt.Sprintf(" WHERE TO_SECONDS(event_timestamp) >= %d AND event_timestamp >= ?", toSecondsUTC(floor.UTC().Truncate(time.Hour)))
}

// snapshotCutFirstPageSQL is the walk's first statement (floor bound as its
// parameter), shared with the test that EXPLAINs it.
func snapshotCutFirstPageSQL(floor time.Time, page int) string {
	return snapshotCutCols + snapshotCutAbove(floor) + fmt.Sprintf(" ORDER BY event_id DESC LIMIT %d", page)
}

// toSecondsUTC is MySQL's TO_SECONDS of t (seconds since year 0).
func toSecondsUTC(t time.Time) int64 { return t.UTC().Unix() + 62167219200 }
