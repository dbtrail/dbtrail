package reconstruct

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"strings"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/status"
)

// A PostgreSQL capture drops the changes of a table it does not take (one
// that is not at REPLICA IDENTITY FULL, a deferrable primary key, ...) and
// records each in its ledger (stream_state.capture_skips) while the other
// tables keep flowing. A full-table reconstruct of such a table from the
// index would lay none of those changes over its file, and its result, a
// snapshot newer than the file, would hold rows the source no longer has.
// So the table is refused, as for a capture gap (ErrCaptureGap), and a
// snapshot schedule reads the source in its place.

// PGSkipClockMargin is how much earlier than its rows' read a drop dated by
// the clock still counts as after it, where positions cannot be compared
// (PGSkippedSince): the read is dated by the PostgreSQL server's clock and
// the drop by the capture daemon's, so a daemon clock behind the server's
// would make a new drop look old. Refusing a little more is the safe side.
const PGSkipClockMargin = 5 * time.Minute

// PGSkippedSince says why the capture's ledger of dropped changes rules out
// serving schema.table from rows read from the source, plus the changes
// recorded after them; "" when it does not (the table is covered).
//
//   - st is the capture's state; its CaptureSkips is the ledger, which the
//     caller must have read strictly (status.LoadStreamState reads it best
//     effort and leaves it NULL on an error, which would read as no drop).
//     nil, NULL or empty: no drop.
//   - readLSN is the WAL position the rows reach, the snapshot's inclusive
//     floor (baseline.DumpMetadata.LSN, MetaKeyLSN): every change at or after
//     it is laid over them. 0 when not known.
//   - sourceRead is when the rows were read from the source
//     (baseline.SourceReadOf(md).At); zero when not known. Used only where a
//     position cannot be compared.
//
// An entry of the ledger covers the table when it names it, or does not say
// which tables it covers (no list, or a list cut short). A covering entry
// refuses when its newest drop is at or after the rows: by position when
// readLSN and the entry's LastPos are both known (LastPos >= readLSN);
// otherwise by the clock, with PGSkipClockMargin (LastAt >= sourceRead -
// margin); and when neither can be compared, always. An entry's newest drop
// is of any table it names, so a table named beside a newer drop of another
// is refused too: it over-refuses, never covers old rows. A ledger that does
// not parse refuses. Pure.
func PGSkippedSince(st *status.StreamStateInfo, readLSN uint64, sourceRead time.Time, schema, table string) string {
	if st == nil || !st.CaptureSkips.Valid || strings.TrimSpace(st.CaptureSkips.String) == "" {
		return ""
	}
	skips, readable := st.ParseCaptureSkips()
	if !readable {
		return "the capture's record of dropped changes is not readable"
	}
	name := schema + "." + table
	reasons := make([]string, 0, len(skips))
	for reason := range skips {
		reasons = append(reasons, reason)
	}
	sort.Strings(reasons)
	for _, reason := range reasons {
		sk := skips[reason]
		if sk.Count == 0 || !(len(sk.Tables) == 0 || sk.TablesTruncated || slices.Contains(sk.Tables, name)) {
			continue
		}
		switch {
		case readLSN != 0 && sk.LastPos != 0:
			if sk.LastPos < readLSN {
				continue
			}
		case !sourceRead.IsZero() && !sk.LastAt.IsZero():
			if sk.LastAt.Before(sourceRead.Add(-PGSkipClockMargin)) {
				continue
			}
		}
		why := fmt.Sprintf("capture dropped changes of %s (%s) since its rows were read from the source", name, reason)
		if sk.LastDetail != "" && len(sk.Tables) == 1 && sk.Tables[0] == name {
			why += ": " + sk.LastDetail
		}
		return why
	}
	return ""
}

// checkPGSkipped is PGSkippedSince read from db, as an error wrapping
// ErrCaptureGap, or a warning when allowGaps lets the run go on.
//
// The ledger is read here on its own, strictly: status.LoadStreamState reads
// it best-effort and leaves it NULL on any error, which this check would take
// for "no drop" and publish the old rows. No row (no capture ever ran) and an
// index without the column (1054) are no drop; any other error refuses.
func checkPGSkipped(ctx context.Context, db *sql.DB, schema, table string, readLSN uint64, sourceRead time.Time, allowGaps bool) error {
	var raw sql.NullString
	err := db.QueryRowContext(ctx, `SELECT capture_skips FROM stream_state WHERE id = 1`).Scan(&raw)
	var me *mysqldriver.MySQLError
	switch {
	case errors.Is(err, sql.ErrNoRows), errors.As(err, &me) && me.Number == 1054:
		return nil
	case err != nil:
		return fmt.Errorf("%s.%s: read the capture's record of dropped changes: %v; %s: %w", schema, table, err,
			"without it a snapshot built from the index could hold rows the source no longer has", ErrCaptureGap)
	}
	why := PGSkippedSince(&status.StreamStateInfo{CaptureSkips: raw}, readLSN, sourceRead, schema, table)
	if why == "" {
		return nil
	}
	if allowGaps {
		slog.Warn("the table is rebuilt from the index although capture dropped some of its changes; they are missing from the result",
			"schema", schema, "table", table, "reason", why)
		return nil
	}
	return pgSkippedError(why)
}

// pgSkippedError is the refusal for why (PGSkippedSince).
func pgSkippedError(why string) error {
	return fmt.Errorf("%s; a snapshot built from the index would hold rows the source no longer has, so the table needs a full read: %w", why, ErrCaptureGap)
}
