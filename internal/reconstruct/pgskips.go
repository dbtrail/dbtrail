package reconstruct

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/status"
)

// A PostgreSQL capture drops the changes of a table it does not take (one
// that is not at REPLICA IDENTITY FULL, a deferrable primary key, ...) and
// records each in its ledger (stream_state.capture_skips) while the other
// tables keep flowing. A full-table reconstruct of such a table from the
// index would lay none of those changes over its file, and its result, a
// snapshot newer than the file, would hold rows the source no longer has.
// So the table is published as the file it is (pgtable.go), footer and all,
// and readers that ask PGSkippedSince of that footer find it not covered;
// where it cannot be, the update is refused as for a capture gap
// (ErrCaptureGap).

// PGReadPoint is what PGSkippedSince compares a table's file with, from its
// footer: the file's WAL floor when the file is a full read of the source
// (fold generation 0), else 0; and when its rows were last read from the
// source (a fold keeps that instant). Pure.
func PGReadPoint(md baseline.DumpMetadata) (readLSN uint64, sourceRead time.Time) {
	read := baseline.SourceReadOf(md)
	if read.Known() && read.Folds == 0 {
		return md.LSN, read.At
	}
	return 0, read.At
}

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
//     floor (baseline.DumpMetadata.LSN, MetaKeyLSN), and ONLY for a file that
//     is a full read of the source; 0 for any other. PGReadPoint gives both
//     arguments from a footer: use it.
//   - sourceRead is when the rows were last read from the source
//     (baseline.SourceReadOf(md).At), which a fold keeps; zero when not
//     known. Used where readLSN is 0, or a drop records no position.
//
// Why the position only for a full read: capture writes a change's rows to
// the index before it saves the ledger (with its checkpoint), and a fold's
// floor is the newest change the index holds. A fold that read a ledger
// without a drop still to be saved has a floor past it, and a position
// compare would call the table covered for good; the same for a copy folded
// by 0.137.0 or 0.138.0 over dropped changes. Against the last real read,
// which the fold keeps, any drop since refuses.
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

// pgNotCovered is PGSkippedSince over the ledger read from db, "" when the
// table is covered.
//
// The ledger is read here on its own, strictly: status.LoadStreamState reads
// it best-effort and leaves it NULL on any error, which would read as "no
// drop" and fold the table as current. No row (no capture ever ran) and an
// index without the column (1054) are no drop; any other error refuses the
// run, as a capture gap.
func pgNotCovered(ctx context.Context, db *sql.DB, schema, table string, readLSN uint64, sourceRead time.Time) (string, error) {
	var raw sql.NullString
	err := db.QueryRowContext(ctx, `SELECT capture_skips FROM stream_state WHERE id = 1`).Scan(&raw)
	var me *mysqldriver.MySQLError
	switch {
	case errors.Is(err, sql.ErrNoRows), errors.As(err, &me) && me.Number == 1054:
		return "", nil
	case err != nil:
		return "", fmt.Errorf("%s.%s: read the capture's record of dropped changes: %v; %s: %w", schema, table, err,
			"without it a snapshot built from the index could hold rows the source no longer has", ErrCaptureGap)
	}
	return PGSkippedSince(&status.StreamStateInfo{CaptureSkips: raw}, readLSN, sourceRead, schema, table), nil
}

// pgSkippedError is the refusal for why (PGSkippedSince), where the table
// cannot be published as it is.
func pgSkippedError(why string) error {
	return fmt.Errorf("%s; a snapshot built from the index would hold rows the source no longer has, so the table needs a full read: %w", why, ErrCaptureGap)
}

// PGNotCovered is PGSkippedSince for schema.table over md, the footer of
// the snapshot a read starts from, with the ledger read strictly from db: ""
// when the table is covered. An error is a ledger that could not be read.
func PGNotCovered(ctx context.Context, db *sql.DB, schema, table string, md baseline.DumpMetadata) (string, error) {
	lsn, read := PGReadPoint(md)
	return pgNotCovered(ctx, db, schema, table, lsn, read)
}

// ErrPGPastReadNotCovered marks a read of one PostgreSQL table at a past
// moment (single-row reconstruct, AS OF, _snapshot, _flashback) refused by
// CheckPGPastRead.
var ErrPGPastReadNotCovered = errors.New("capture dropped changes of the table that this read would need")

// CheckPGPastRead refuses a read of schema.table at a past moment on a
// PostgreSQL index when the capture dropped changes of the table since the
// rows the read starts from (PGSkippedSince over md, the footer of the
// snapshot the read folds from; the zero value for a read with no snapshot,
// which folds the recorded changes alone and is then refused over any drop
// that may cover the table). The ledger names only its newest drop, so a
// moment before the drop is refused as well: the answer could be right,
// and nothing can show it. nil on any other index: a MySQL or MariaDB read
// is not asked, and pays one read of stream_state's flavor. The message
// names no command-line flag: the CLI, MCP, the web interface and the
// PostgreSQL port all print it as it is.
func CheckPGPastRead(ctx context.Context, db *sql.DB, schema, table string, md baseline.DumpMetadata) error {
	if db == nil || query.SourceFlavor(db) != "postgres" {
		return nil
	}
	lsn, read := PGReadPoint(md)
	why, err := pgNotCovered(ctx, db, schema, table, lsn, read)
	if err != nil {
		return err
	}
	if why == "" {
		why = pgSlotLostSince(ctx, db, read)
	}
	if why == "" {
		return nil
	}
	quoted := `"` + strings.ReplaceAll(schema, `"`, `""`) + `"."` + strings.ReplaceAll(table, `"`, `""`) + `"`
	return &pgPastReadError{msg: fmt.Sprintf("a read of %s.%s at a past moment is refused: %s. What DBTrail holds of the table misses those changes, so the row it would show could be wrong. "+
		"Fix the cause on the source (for a table that is not at REPLICA IDENTITY FULL: ALTER TABLE %s REPLICA IDENTITY FULL), then take a full read of it: "+
		"a moment after that full read can be read", schema, table, why, quoted)}
}

// pgSlotLostSince says why a capture loss stamped in stream_state
// (gap_lost_at: a lost replication slot) rules out the rows read from the
// source at read, "" when it does not. The loss records no WAL position and
// is stamped by the capture daemon's clock, read by the PostgreSQL server's:
// a loss up to PGSkipClockMargin before read counts as after it. An unknown
// read is ruled out by any loss on record. A state that cannot be read, or
// an index without the loss columns, is left to the callers' own capture-gap
// check (CaptureGapStatus), which says so.
func pgSlotLostSince(ctx context.Context, db *sql.DB, read time.Time) string {
	st, err := status.LoadStreamState(ctx, db)
	if err != nil || st == nil || !st.GapColumnsPresent || !st.GapLostAt.Valid {
		return ""
	}
	if !read.IsZero() && st.GapLostAt.Time.Before(read.Add(-PGSkipClockMargin)) {
		return ""
	}
	detail := st.GapLostDetail.String
	if detail == "" {
		detail = "no detail recorded"
	}
	return fmt.Sprintf("capture lost events at %s (%s), after the rows were read from the source or too close to that read to tell apart", st.GapLostAt.Time.UTC().Format(time.RFC3339), detail)
}

// pgPastReadError is CheckPGPastRead's refusal: its message alone, and
// errors.Is ErrPGPastReadNotCovered.
type pgPastReadError struct{ msg string }

func (e *pgPastReadError) Error() string        { return e.msg }
func (e *pgPastReadError) Is(target error) bool { return target == ErrPGPastReadNotCovered }
