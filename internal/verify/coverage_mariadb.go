package verify

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/dbtrail/dbtrail/internal/parser"
)

// indexCoversMariaDB is indexCovers for a MariaDB source, whose snapshot is
// anchored at @@gtid_binlog_pos (srcPos): one domain-server-seq per domain.
//
// The index covers the snapshot when, in EVERY domain the source has, the
// index's checkpoint (stream_state.gtid_set) has reached at least the
// source's sequence number. A MariaDB sequence is domain-wide, so the server
// id is not compared: after a failover the next transaction of the domain
// carries another server id and a higher sequence. A domain only the index
// has is fine, and so is an index past the snapshot, which is the normal
// state of a running capture. The same containment the MySQL path checks
// with idxSet.Contain(srcSet), and the same verdict when it fails.
//
// Known limit, shared with the MySQL path: a source whose history restarted
// (RESET MASTER, a restore) below the index's checkpoint reads as covered,
// because an index past the snapshot is also what a healthy capture of a
// busy source looks like, and the two cannot be told apart from here.
//
// Unlike the MySQL "GTIDs disabled" branch, nothing here assumes: an empty
// source position, a capture in binlog-position mode, a missing checkpoint,
// a set that does not parse (parser.ParseMariaDBPosition refuses an empty
// position, an empty entry and a domain named twice), or a read that failed
// all return (false, why), which the caller reports as inconclusive.
func indexCoversMariaDB(ctx context.Context, indexDB *sql.DB, srcPos string) (bool, string) {
	if strings.TrimSpace(srcPos) == "" {
		return false, "coverage cannot be checked: the MariaDB source reported no GTID position (@@gtid_binlog_pos is empty)"
	}
	src, err := parser.ParseMariaDBPosition(srcPos)
	if err != nil {
		return false, "source GTID position is unparseable: " + err.Error()
	}
	var mode, idxGTID sql.NullString
	err = indexDB.QueryRowContext(ctx,
		"SELECT mode, gtid_set FROM stream_state WHERE id = 1").Scan(&mode, &idxGTID)
	if errors.Is(err, sql.ErrNoRows) {
		return false, "index has no stream state yet (DBTrail not running or never checkpointed)"
	}
	if err != nil {
		return false, "could not read index coverage: " + err.Error()
	}
	// Checked before the set: a stream restarted in position mode can leave
	// the GTID set of an earlier run behind, and that set says nothing about
	// what the index holds now.
	if mode.String != "gtid" {
		return false, "coverage cannot be checked: the index's capture runs in binlog-position mode, which records no GTID position to compare with the MariaDB source. " +
			gtidModeAdvice("mariadb")
	}
	// GTID mode with an empty set is a healthy capture: one started on a
	// MariaDB that had written nothing yet holds the empty set until it records
	// its first transaction. The source position is not empty (checked above),
	// so the index is behind. The --reset advice would record a false loss.
	if strings.TrimSpace(idxGTID.String) == "" {
		return false, fmt.Sprintf(indexBehind+": the capture has recorded no transaction yet (snapshot %s); re-run once DBTrail catches up",
			srcPos)
	}
	idx, err := parser.ParseMariaDBPosition(idxGTID.String)
	if err != nil {
		return false, "index GTID set is unparseable as a MariaDB position: " + err.Error()
	}
	for domain, want := range src {
		if have, ok := idx[domain]; !ok || have.SequenceNumber < want.SequenceNumber {
			return false, fmt.Sprintf(indexBehind+" (indexed %s does not contain snapshot %s); re-run once DBTrail catches up",
				idxGTID.String, srcPos)
		}
	}
	return true, ""
}

// gtidModeAdvice is what to do when the index has no GTID checkpoint to
// compare with. It never advises a plain restart at the source's current
// position: with the index behind, that skips events and nothing records
// the skip. DBTrail cannot derive the GTID position a binlog-position
// checkpoint stands for, so there is no switch without a hole: the advice
// names --reset, which records the skipped span as permanently lost (the
// same record persistResetDiscard writes), and a new full snapshot, which
// is the only way to read back what those events changed.
func gtidModeAdvice(flavor string) string {
	return fmt.Sprintf("Comparing needs the capture in GTID mode, and DBTrail cannot tell which GTID position the index's current checkpoint stands for, "+
		"so it cannot switch without a hole. Restart the capture with --reset --start-gtid \"$(mysql -N -e '%s')\": "+
		"the events between the last one the index holds and that point are not captured, and --reset records that span as permanently lost. "+
		"Then take a new full snapshot, the only way the index gets back what those events changed.",
		parser.GTIDExecutedHint(flavor))
}
