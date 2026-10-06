package verify

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

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
// cut is not placed. Neither is it when every change in the live index is
// newer than the snapshot: the ones it holds were moved to the archive tier,
// whose order against the live index is not known here.
//
// An empty live index has nothing to walk. Its cut is the capture's
// checkpoint position, read BEFORE the walk: the coverage check proved the
// checkpoint holds the snapshot, so every change the snapshot holds ends at or
// before it; and a change the capture wrote at or before it was indexed
// before the checkpoint was saved, so the walk that found nothing would have
// found it.
//
// firstOut is where the oldest change the walk skipped (the first one the
// snapshot does not hold) starts; nil when it skipped none.
func snapshotCut(ctx context.Context, db *sql.DB, member snapshotMember) (cut, firstOut *query.BinlogPos, why string, err error) {
	return snapshotCutPaged(ctx, db, member, snapshotCutPage)
}

func snapshotCutPaged(ctx context.Context, db *sql.DB, member snapshotMember, page int) (cut, firstOut *query.BinlogPos, why string, err error) {
	var ckFile sql.NullString
	var ckPos sql.NullInt64
	err = db.QueryRowContext(ctx, "SELECT binlog_file, binlog_position FROM stream_state WHERE id = 1").Scan(&ckFile, &ckPos)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, nil, "", fmt.Errorf("read the capture's checkpoint: %w", err)
	}

	const cols = "SELECT event_id, gtid, binlog_file, start_pos, end_pos FROM binlog_events"
	var (
		last    uint64
		skipped int
		first   = true
	)
	for {
		var rows *sql.Rows
		if first {
			rows, err = db.QueryContext(ctx, cols+fmt.Sprintf(" ORDER BY event_id DESC LIMIT %d", page))
		} else {
			rows, err = db.QueryContext(ctx, cols+fmt.Sprintf(" WHERE event_id < ? ORDER BY event_id DESC LIMIT %d", page), last)
		}
		if err != nil {
			return nil, nil, "", fmt.Errorf("walk the index from its newest change: %w", err)
		}
		n := 0
		for rows.Next() {
			n++
			var (
				id         uint64
				gtid       sql.NullString
				file       string
				start, end uint64
			)
			if err := rows.Scan(&id, &gtid, &file, &start, &end); err != nil {
				rows.Close()
				return nil, nil, "", fmt.Errorf("walk the index from its newest change: %w", err)
			}
			last = id
			if !gtid.Valid || strings.TrimSpace(gtid.String) == "" {
				rows.Close()
				return nil, nil, fmt.Sprintf("the read cannot be cut at the snapshot: indexed change %d (%s:%d) has no GTID, so whether the snapshot holds it cannot be told", id, file, end), nil
			}
			in, err := member(gtid.String)
			if err != nil {
				rows.Close()
				return nil, nil, "the read cannot be cut at the snapshot: " + err.Error(), nil
			}
			if in {
				rows.Close()
				return &query.BinlogPos{File: file, Pos: end}, firstOut, "", nil
			}
			skipped++
			firstOut = &query.BinlogPos{File: file, Pos: start}
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
		return nil, nil, fmt.Sprintf("the read cannot be cut at the snapshot: all %d changes in the live index are newer than the snapshot, and the ones it holds are archived", skipped), nil
	}
	if !ckFile.Valid || ckFile.String == "" || !ckPos.Valid || ckPos.Int64 <= 0 {
		return nil, nil, "the read cannot be cut at the snapshot: the index holds no change and its checkpoint has no position", nil
	}
	return &query.BinlogPos{File: ckFile.String, Pos: uint64(ckPos.Int64)}, nil, "", nil
}
