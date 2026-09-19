package query

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
)

// SourceFlavor returns the source flavor recorded in stream_state ("mysql",
// "mariadb", "postgres"), the authoritative single-source signal for an index
// database. Best-effort: a nil db, or any read failure (no stream_state row on
// a file-indexed DB, very old schema), returns "" — callers treat empty as
// MySQL-family semantics, the established default (see
// recovery.DialectForFlavor, which owns the canonical "postgres" literal).
// The nil guard lets a caller pass an as-yet-unopened handle directly.
//
// Failures are logged, not swallowed: a missing row (sql.ErrNoRows) is the
// expected shape of a file-indexed / pre-stream index and logs at Debug, but
// any other error (connection blip, old schema without the flavor column) is
// indistinguishable from a legitimate MySQL index once flattened to "" — and
// on the recover path "" selects the MySQL dialect, so a silent flatten is a
// latent wrong-SQL vector. Those log at Warn.
func SourceFlavor(db *sql.DB) string {
	flavor, _ := SourceFlavorDetail(db)
	return flavor
}

// SourceFlavorDetail is SourceFlavor with the "why is it empty" discriminated:
// noStream is true when the emptiness is a MISSING stream_state row — the
// expected shape of a file-indexed / pre-stream index, where the source is
// provably MySQL-family (`bintrail index` parses MySQL/MariaDB binlog files by
// construction, and every PostgreSQL stream stamps flavor='postgres'). An
// empty flavor with noStream=false means the read genuinely failed and the
// dialect is unknown. Callers that warn about dialect assumptions should
// hedge only in the second case (#1121).
func SourceFlavorDetail(db *sql.DB) (flavor string, noStream bool) {
	if db == nil {
		return "", false
	}
	if err := db.QueryRow("SELECT flavor FROM stream_state WHERE id = 1").Scan(&flavor); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			slog.Debug("source flavor unavailable — no stream_state row (file-indexed or pre-stream index); defaulting to MySQL-family semantics")
			return "", true
		}
		slog.Warn("source flavor read failed — defaulting to MySQL-family semantics; recovery SQL dialect and gap-check semantics may be wrong for a non-MySQL source",
			"error", err)
		return "", false
	}
	return flavor, false
}

// StreamCaptured reports whether a `stream` (or `watch`) wrote this index:
// stream_state has its row. That is the condition under which event_id order
// is binlog order, which Options.SinceEventID relies on (#1720). A file-mode
// index (`bintrail index`, no stream_state row) answers false; a read failure
// answers false too, with the error, so a caller never takes the floor on a
// guess.
func StreamCaptured(ctx context.Context, db *sql.DB) (bool, error) {
	var one int
	err := db.QueryRowContext(ctx, "SELECT 1 FROM stream_state WHERE id = 1").Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read stream_state: %w", err)
	}
	return true, nil
}
