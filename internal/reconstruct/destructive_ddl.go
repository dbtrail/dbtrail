package reconstruct

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
)

// ErrDestructiveDDL is wrapped into the error CheckDestructiveDDL returns
// when it finds a TRUNCATE/DROP/RENAME/CREATE OR REPLACE on the target table
// inside the reconstruction window (#764).
var ErrDestructiveDDL = errors.New("destructive DDL in reconstruction window")

// CheckDestructiveDDL queries schema_changes for a TRUNCATE TABLE, DROP
// TABLE, RENAME TABLE or CREATE OR REPLACE TABLE detected on schema.table in (since, until] — the
// exact window a baseline+delta merge replays (since is the baseline's
// snapshot time, until is the requested --at / AsOf instant).
//
// TRUNCATE and DROP+re-CREATE emit no row-level binlog events — the parser
// only records them as an audit entry in schema_changes (#700-adjacent DDL
// tracking) — so ReconstructTable's and the shim's _snapshot full-table merge
// have no delta to apply and would silently pass every baseline row straight
// through, resurrecting rows the DDL actually deleted as if they still
// existed at --at (#764). Refusing with a clear, actionable error is the
// chosen fix over auto-truncating-and-replaying-from-the-DDL-point, which
// would be materially more complex and itself error prone (Occam's razor).
//
// RENAME TABLE is included because it moves the table's row-event stream to
// a new name; the baseline for the old name can no longer be trusted to
// represent schema.table's state either.
//
// MariaDB's CREATE OR REPLACE TABLE is included because on an existing table
// it drops the rows with no row events, exactly like DROP then CREATE (#1664).
//
// A missing schema_changes table (a pre-DDL-tracking index, or a caller that
// hasn't run indexer.EnsureSchema) is treated as "nothing to check" rather
// than a hard failure: this is an additive safety net on top of the existing
// reconstruct contract, not a new hard dependency.
func CheckDestructiveDDL(ctx context.Context, db *sql.DB, schema, table string, since, until time.Time) error {
	ddlType, detectedAt, found, err := findDestructiveDDL(ctx, db, schema, table, since, until)
	if errors.Is(err, errSchemaChangesMissing) {
		return nil // nothing to check, as this has always answered
	}
	if err != nil || !found {
		return err
	}
	return fmt.Errorf(
		"%w: %s on %s.%s detected at %s (between the baseline snapshot and the requested point-in-time) "+
			"emits no row-level binlog events to replay — reconstructing to this point-in-time would silently "+
			"resurrect pre-%s rows as if they still existed; re-baseline the table after this DDL and "+
			"reconstruct from the new baseline instead",
		ErrDestructiveDDL, ddlType, schema, table, detectedAt.UTC().Format(time.RFC3339), strings.ToLower(ddlType))
}

// errSchemaChangesMissing marks an index that has no schema_changes table at
// all — one that predates DDL tracking, or a caller that has not run
// indexer.EnsureSchema. It is an ANSWER, not a failure, but callers must say
// what they do with it: the baseline paths have always treated it as "nothing
// to check", while the binlog-only fallback says out loud that it could not
// check.
var errSchemaChangesMissing = errors.New("this index has no schema_changes table")

// FindDestructiveDDL is CheckDestructiveDDL's finding without its message, for
// a caller that is not reconstructing and says it in its own words (verify).
// An index with no schema_changes table answers not found, as
// CheckDestructiveDDL does.
func FindDestructiveDDL(ctx context.Context, db *sql.DB, schema, table string, since, until time.Time) (ddlType string, detectedAt time.Time, found bool, err error) {
	ddlType, detectedAt, found, err = findDestructiveDDL(ctx, db, schema, table, since, until)
	if errors.Is(err, errSchemaChangesMissing) {
		return "", time.Time{}, false, nil
	}
	return ddlType, detectedAt, found, err
}

// findDestructiveDDL is CheckDestructiveDDL's query without its message, for a
// caller whose window is not "since the baseline snapshot" (the binlog-only
// fallback, #1674). found is false with a nil error when there is none, and
// errSchemaChangesMissing when the table does not exist.
func findDestructiveDDL(ctx context.Context, db *sql.DB, schema, table string, since, until time.Time) (ddlType string, detectedAt time.Time, found bool, err error) {
	// schema_name = '' is matched too, and the arm is NOT removable. Since
	// #1435 parseDDL resolves an unqualified statement ("TRUNCATE TABLE
	// orders" after "USE mydb") against the QUERY_EVENT's session default
	// database, so NEW rows carry a real schema — but every row indexed
	// before that fix has schema_name = '' (the original session's default
	// is unrecoverable, so no backfill exists), and deleting this arm as
	// "redundant now" would silently stop destructive-DDL detection for
	// exactly the historical windows people reconstruct after an incident
	// (#764's return path). The '' match can only widen a match (favoring an
	// over-cautious refusal on historical rows), never narrow one.
	const q = `SELECT ddl_type, detected_at FROM schema_changes
		WHERE (schema_name = ? OR schema_name = '') AND table_name = ?
		AND ddl_type IN ('TRUNCATE TABLE', 'DROP TABLE', 'RENAME TABLE', 'CREATE OR REPLACE TABLE')
		AND detected_at > ? AND detected_at <= ?
		ORDER BY detected_at ASC LIMIT 1`

	err = db.QueryRowContext(ctx, q, schema, table, since, until).Scan(&ddlType, &detectedAt)
	switch {
	case err == nil:
		return ddlType, detectedAt, true, nil
	case errors.Is(err, sql.ErrNoRows):
		return "", time.Time{}, false, nil
	default:
		// Graded on the ERROR NUMBER, never on its text. 1146 is "no such
		// table", the index too old to have schema_changes; 1932 is "table
		// doesn't exist IN ENGINE", a missing or corrupt tablespace — the
		// check is BROKEN, not absent. Their messages share the words
		// "doesn't exist", so a string match reads a damaged index as a clean
		// one, and on the binlog-only path this lookup is the whole defense
		// against resurrecting removed rows.
		var me *mysqldriver.MySQLError
		if errors.As(err, &me) && me.Number == 1146 {
			return "", time.Time{}, false, errSchemaChangesMissing
		}
		return "", time.Time{}, false, fmt.Errorf("check schema_changes for destructive DDL on %s.%s: %w", schema, table, err)
	}
}
