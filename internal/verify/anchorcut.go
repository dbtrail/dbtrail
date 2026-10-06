package verify

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/dbtrail/internal/consistency"
	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
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
// snapshot does not hold) starts; nil when it skipped none. cutAt is the cut
// change's stamp (zero for the checkpoint cut).
func snapshotCut(ctx context.Context, db *sql.DB, member snapshotMember, floor time.Time) (cut, firstOut *query.BinlogPos, cutAt time.Time, why string, err error) {
	return snapshotCutPaged(ctx, db, member, floor, snapshotCutPage)
}

// snapshotCutFloorMargin is how far before the read's opening the walk looks
// (see snapshotCut): a change's stamp is its statement's start, so one held
// by the snapshot can be stamped before the read by as long as its
// transaction ran.
const snapshotCutFloorMargin = time.Hour

// cutResult is one walk's outcome.
type cutResult struct {
	cut, firstOut *query.BinlogPos
	cutAt         time.Time // the cut change's stamp; zero for the checkpoint cut
	why           string
}

func snapshotCutPaged(ctx context.Context, db *sql.DB, member snapshotMember, floor time.Time, page int) (cut, firstOut *query.BinlogPos, cutAt time.Time, why string, err error) {
	var ckFile sql.NullString
	var ckPos sql.NullInt64
	err = db.QueryRowContext(ctx, "SELECT binlog_file, binlog_position FROM stream_state WHERE id = 1").Scan(&ckFile, &ckPos)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, nil, time.Time{}, "", fmt.Errorf("read the capture's checkpoint: %w", err)
	}
	ck := &query.BinlogPos{File: ckFile.String, Pos: uint64(max(ckPos.Int64, 0))}
	if !ckFile.Valid || ckFile.String == "" || !ckPos.Valid || ckPos.Int64 <= 0 {
		ck = nil
	}
	floor = floor.UTC()

	// The partitions that can hold a change at or after the floor, named
	// explicitly: MySQL's own pruning of the timestamp condition keeps the
	// table's oldest partition, which an ordered walk would then read whole
	// on every page (#1692). Rotation can move the layout meanwhile, so it
	// is listed again after the walk; a moved layout, or one that cannot be
	// listed, walks again with no clause (slower, complete).
	clause, names, perr := reconstruct.PartitionsAtOrAfter(ctx, db, floor)
	if perr != nil {
		if ctx.Err() != nil {
			return nil, nil, time.Time{}, "", ctx.Err()
		}
		slog.Warn("verify: cannot bound the snapshot-cut walk to partitions; walking all of them", "error", perr)
		clause = ""
	}
	r, err := walkSnapshotCut(ctx, db, member, floor, page, clause, ck)
	if err == nil && clause != "" {
		_, again, lerr := reconstruct.PartitionsAtOrAfter(ctx, db, floor)
		if lerr != nil || !slices.Equal(again, names) {
			r, err = walkSnapshotCut(ctx, db, member, floor, page, "", ck)
		}
	}
	if err != nil {
		return nil, nil, time.Time{}, "", err
	}
	return r.cut, r.firstOut, r.cutAt, r.why, nil
}

// walkSnapshotCut is one walk (see snapshotCut), bounded to the partitions
// partClause names ("" for all of them).
func walkSnapshotCut(ctx context.Context, db *sql.DB, member snapshotMember, floor time.Time, page int, partClause string, ck *query.BinlogPos) (cutResult, error) {
	var (
		last     uint64
		skipped  int
		firstOut *query.BinlogPos
	)
	// judge decides one change: stop with a cut, stop unplaced, or go on.
	judge := func(rows *sql.Rows) (done bool, r cutResult, err error) {
		var (
			id         uint64
			gtid       sql.NullString
			file       string
			start, end uint64
			at         time.Time
		)
		if err := rows.Scan(&id, &gtid, &file, &start, &end, &at); err != nil {
			return true, cutResult{}, fmt.Errorf("walk the index from its newest change: %w", err)
		}
		last = id
		if !gtid.Valid || strings.TrimSpace(gtid.String) == "" {
			return true, cutResult{why: fmt.Sprintf("the read cannot be cut at the snapshot: indexed change %d (%s:%d) has no GTID, so whether the snapshot holds it cannot be told", id, file, end)}, nil
		}
		in, err := member(gtid.String)
		if err != nil {
			return true, cutResult{why: "the read cannot be cut at the snapshot: " + err.Error()}, nil
		}
		if in {
			return true, cutResult{cut: &query.BinlogPos{File: file, Pos: end}, firstOut: firstOut, cutAt: at.UTC()}, nil
		}
		skipped++
		firstOut = &query.BinlogPos{File: file, Pos: start}
		return false, cutResult{}, nil
	}
	for first := true; ; first = false {
		var (
			rows *sql.Rows
			err  error
		)
		if first {
			rows, err = db.QueryContext(ctx, snapshotCutFirstPageSQL(floor, partClause, page), floor)
		} else {
			rows, err = db.QueryContext(ctx, snapshotCutCols+partClause+snapshotCutAbove(floor)+fmt.Sprintf(" AND event_id < ? ORDER BY event_id DESC LIMIT %d", page), floor, last)
		}
		if err != nil {
			return cutResult{}, fmt.Errorf("walk the index from its newest change: %w", err)
		}
		n := 0
		for rows.Next() {
			n++
			if done, r, err := judge(rows); done {
				rows.Close()
				return r, err
			}
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return cutResult{}, fmt.Errorf("walk the index from its newest change: %w", err)
		}
		if n < page {
			break
		}
	}
	if skipped > 0 {
		return cutResult{why: fmt.Sprintf("the read cannot be cut at the snapshot: all %d changes indexed since %s are newer than the snapshot", skipped, floor.Format(time.RFC3339))}, nil
	}
	// Nothing indexed near the read: the newest change below the floor.
	rows, err := db.QueryContext(ctx, snapshotCutCols+" WHERE event_timestamp < ? ORDER BY event_id DESC LIMIT 1", floor)
	if err != nil {
		return cutResult{}, fmt.Errorf("read the newest indexed change: %w", err)
	}
	defer rows.Close()
	if rows.Next() {
		_, r, err := judge(rows)
		if err == nil && r.cut == nil && r.why == "" {
			r.why = "the read cannot be cut at the snapshot: the newest indexed change, from before " + floor.Format(time.RFC3339) + ", is not one the snapshot holds"
		}
		return r, err
	}
	if err := rows.Err(); err != nil {
		return cutResult{}, fmt.Errorf("read the newest indexed change: %w", err)
	}
	if ck == nil {
		return cutResult{why: "the read cannot be cut at the snapshot: the index holds no change and its checkpoint has no position"}, nil
	}
	return cutResult{cut: ck}, nil
}

const snapshotCutCols = "SELECT event_id, gtid, binlog_file, start_pos, end_pos, event_timestamp FROM binlog_events"

// snapshotCutAbove is the walk's floor condition, with floor as its one
// parameter: a TO_SECONDS literal like query.buildQuery's hints, and the
// exact comparison.
func snapshotCutAbove(floor time.Time) string {
	return fmt.Sprintf(" WHERE TO_SECONDS(event_timestamp) >= %d AND event_timestamp >= ?", toSecondsUTC(floor.UTC().Truncate(time.Hour)))
}

// snapshotCutFirstPageSQL is the walk's first statement (floor bound as its
// parameter), shared with the test that EXPLAINs it.
func snapshotCutFirstPageSQL(floor time.Time, partClause string, page int) string {
	return snapshotCutCols + partClause + snapshotCutAbove(floor) + fmt.Sprintf(" ORDER BY event_id DESC LIMIT %d", page)
}

// toSecondsUTC is MySQL's TO_SECONDS of t (seconds since year 0).
func toSecondsUTC(t time.Time) int64 { return t.UTC().Unix() + 62167219200 }
