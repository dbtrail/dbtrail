package consoleapp

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/status"
)

// The capture status read (#1794) for a MariaDB source.
//
// A MariaDB source has no @@gtid_executed. Its analog is @@gtid_binlog_pos,
// one domain-server-seq per replication domain, which is what the capture
// reads and what its own gap detection compares with. The rules are the
// MySQL read's, cut down to the part that holds without a second read:
//
//   - up to date is the two positions EQUAL, domain by domain, server id
//     included. The same sequence written by another server is another
//     history, not the same position;
//   - anything else is unknown with a reason. A source ahead is not called
//     behind: the MySQL read needs a purged set, a count of what is missing
//     and a second read before it says that, and none of it is built for
//     MariaDB here;
//   - a capture in binlog-position mode, or with no GTID checkpoint, is
//     unknown without asking the source (checkpointComparable).
//
// No same-server check: the index is always a MySQL server, so a MariaDB
// source is never the index's own server.

// captureHeadFromDBsMariaDB is captureHeadFromDBs for a MariaDB source.
func captureHeadFromDBsMariaDB(ctx context.Context, indexDSN, sourceDSN string) captureProbeResult {
	idx, err := config.Connect(probeDSN(indexDSN))
	if err != nil {
		return captureProbeResult{detail: "the index did not answer", cause: config.ScrubDSNError(err, indexDSN, sourceDSN)}
	}
	defer idx.Close()
	st, err := status.LoadStreamState(ctx, idx)
	if err != nil {
		return captureProbeResult{detail: "the capture's checkpoint could not be read", cause: config.ScrubDSNError(err, indexDSN, sourceDSN)}
	}
	r, err := headFromStateMariaDB(ctx, st, func() (*sql.DB, error) {
		return config.Connect(sourceProbeDSN(sourceDSN))
	})
	if err != nil {
		r.cause = config.ScrubDSNError(err, indexDSN, sourceDSN)
	}
	return r
}

// headFromStateMariaDB is headFromState for a MariaDB source: refused
// without touching the source when checkpointComparable refuses, else the
// source's @@gtid_binlog_pos compared with st's GTID set. A failed step
// returns its detail with the error, which the caller scrubs; the source
// connection is closed here.
func headFromStateMariaDB(ctx context.Context, st *status.StreamStateInfo, openSource func() (*sql.DB, error)) (captureProbeResult, error) {
	if ok, detail := checkpointComparable(st); !ok {
		return captureProbeResult{detail: detail}, nil
	}
	src, err := openSource()
	if err != nil {
		return captureProbeResult{detail: "the source did not answer"}, err
	}
	defer src.Close()
	var pos sql.NullString
	err = src.QueryRowContext(ctx, "SELECT @@GLOBAL.gtid_binlog_pos").Scan(&pos)
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) && mysqlErr.Number == 1193 { // ER_UNKNOWN_SYSTEM_VARIABLE
		// Registered as MariaDB, answers as something else. Not a failure
		// to read, and not an answer either.
		return captureProbeResult{detail: "the source has no MariaDB GTID position: is it a MariaDB server?"}, nil
	}
	if err != nil {
		return captureProbeResult{detail: "the source did not report its GTID position"}, err
	}
	executed := strings.Join(strings.Fields(pos.String), "")
	verdict, detail := compareMariaDBPositions(st.GTIDSet.String, executed)
	return captureProbeResult{verdict: verdict, detail: detail,
		captured: strings.Join(strings.Fields(st.GTIDSet.String), ""), executed: executed, checkpoint: st.LastCheckpoint}, nil
}

// compareMariaDBPositions is the verdict on the capture's checkpoint and the
// source's @@gtid_binlog_pos, pure: console.CaptureCaughtUp when the two are
// equal domain by domain (server id included), else "" (unknown) and why.
// It never returns console.CaptureBehind.
func compareMariaDBPositions(captured, executed string) (verdict, detail string) {
	if strings.TrimSpace(executed) == "" {
		return "", "the source reported no GTID position"
	}
	have, err := parser.ParseMariaDBPosition(captured)
	if err != nil {
		return "", "the capture's GTID set does not parse"
	}
	wrote, err := parser.ParseMariaDBPosition(executed)
	if err != nil {
		return "", "the source's GTID position does not parse"
	}
	equal := len(have) == len(wrote)
	sourceAhead := true
	for domain, w := range wrote {
		h, ok := have[domain]
		if !ok || h != w {
			equal = false
		}
		// Ahead means a higher sequence. The same sequence from another
		// server is another history.
		if ok && h != w && h.SequenceNumber >= w.SequenceNumber {
			sourceAhead = false
		}
	}
	for domain := range have {
		if _, ok := wrote[domain]; !ok {
			sourceAhead = false
		}
	}
	switch {
	case equal:
		return console.CaptureCaughtUp, ""
	case sourceAhead:
		// Not said as behind: see the rules at the top of this file.
		return "", "the source is ahead of the capture's checkpoint; a MariaDB source is only compared for an exact match"
	}
	return "", "the capture's checkpoint and the source's GTID position do not match: the source may have been reset, restored or replaced"
}
