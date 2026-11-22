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

// pgSkippedSince says why the capture's ledger rules out building schema.table
// from the index over rows read from the source at sourceRead, "" when it
// does not. The rule is the copy tail's (console.pgTailCaptureLoss): a drop
// dated at or after the read, in an entry that names the table or does not
// say which tables it covers (no list, or a list cut short). An entry's time
// is its newest drop of any table it names, so a table named beside a newer
// drop of another is refused too: over-refusing reads the source, never
// publishes old rows. An unknown sourceRead refuses any entry that may cover
// the table. A NULL ledger is no drop; one that does not parse refuses. Pure.
func pgSkippedSince(st *status.StreamStateInfo, sourceRead time.Time, schema, table string) string {
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
		if sk.Count == 0 || (!sourceRead.IsZero() && !sk.LastAt.IsZero() && sk.LastAt.Before(sourceRead)) {
			continue
		}
		if len(sk.Tables) == 0 || sk.TablesTruncated || slices.Contains(sk.Tables, name) {
			why := fmt.Sprintf("capture dropped changes of %s (%s) since its rows were read from the source", name, reason)
			if sk.LastDetail != "" && slices.Contains(sk.Tables, name) && len(sk.Tables) == 1 {
				why += ": " + sk.LastDetail
			}
			return why
		}
	}
	return ""
}

// checkPGSkipped is pgSkippedSince read from db, as an error wrapping
// ErrCaptureGap, or a warning when allowGaps lets the run go on.
//
// The ledger is read here on its own, strictly: status.LoadStreamState reads
// it best-effort and leaves it NULL on any error, which this check would take
// for "no drop" and publish the old rows. No row (no capture ever ran) and an
// index without the column (1054) are no drop; any other error refuses.
func checkPGSkipped(ctx context.Context, db *sql.DB, schema, table string, sourceRead time.Time, allowGaps bool) error {
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
	why := pgSkippedSince(&status.StreamStateInfo{CaptureSkips: raw}, sourceRead, schema, table)
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

// pgSkippedError is the refusal for why (pgSkippedSince).
func pgSkippedError(why string) error {
	return fmt.Errorf("%s; a snapshot built from the index would hold rows the source no longer has, so the table needs a full read: %w", why, ErrCaptureGap)
}
