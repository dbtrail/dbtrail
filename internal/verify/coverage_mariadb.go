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
		return false, "coverage cannot be checked: the index's capture runs in binlog-position mode, which records no GTID position to compare with the MariaDB source; run the capture in GTID mode (--start-gtid) to make it comparable"
	}
	if strings.TrimSpace(idxGTID.String) == "" {
		return false, fmt.Sprintf("index has no GTID checkpoint (no stream has checkpointed in GTID mode against this index); if a stream is running, restart it with --start-gtid \"$(mysql -N -e '%s')\"",
			parser.GTIDExecutedHint("mariadb"))
	}
	idx, err := parser.ParseMariaDBPosition(idxGTID.String)
	if err != nil {
		return false, "index GTID set is unparseable as a MariaDB position: " + err.Error()
	}
	for domain, want := range src {
		if have, ok := idx[domain]; !ok || have.SequenceNumber < want.SequenceNumber {
			return false, fmt.Sprintf("index is behind the source snapshot (indexed %s does not contain snapshot %s); re-run once DBTrail catches up",
				idxGTID.String, srcPos)
		}
	}
	return true, ""
}
