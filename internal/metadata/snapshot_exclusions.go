package metadata

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
)

// DDLSnapshotExclusions records which base tables a DEGRADED snapshot
// (TakeSnapshotExcludingInvalid, #1051) left out and why. It is the EXPLICIT
// source of truth the cascade FK loaders read to flag
// CascadeFK.ChildExcludedFromSnapshot: inferring exclusion from "the child has
// fk_constraints rows but no schema_snapshots rows" is unsound — a table
// created between TakeSnapshot's columns query and its FK query produces that
// exact shape with nothing excluded — and a false "provably partial" caveat is
// the cry-wolf failure the cascade Result contract forbids. An absent table
// (legacy index, or no degraded snapshot ever taken) simply means "no
// exclusions"; readers must tolerate it.
const DDLSnapshotExclusions = `CREATE TABLE IF NOT EXISTS snapshot_exclusions (
    snapshot_id INT UNSIGNED NOT NULL,
    schema_name VARCHAR(64)  NOT NULL,
    table_name  VARCHAR(64)  NOT NULL,
    reason      VARCHAR(64)  NOT NULL,
    pk_column   VARCHAR(64)  DEFAULT NULL COMMENT 'a column name this table does not already use, for the PRIMARY KEY it would take to capture it (#1802); NULL when none was needed or the row predates this column',
    PRIMARY KEY (snapshot_id, schema_name, table_name)
) ENGINE=InnoDB COMMENT='tables a degraded snapshot excluded (no PK / non-InnoDB, #1051); see metadata.DDLSnapshotExclusions'`

// The reasons a degraded snapshot records in snapshot_exclusions.reason. They
// are PERSISTED vocabulary: every index that ever excluded a table holds these
// exact strings, and the status report classifies them to name the fix (#1802).
// Renaming one would turn every recorded row into an unknown reason, which the
// report still shows but can no longer offer a fix for. A table that fails both
// checks gets one row whose reason joins the two with ExclusionReasonSeparator,
// not-InnoDB first.
const (
	ExclusionReasonNotInnoDB    = "not InnoDB"
	ExclusionReasonNoPrimaryKey = "no primary key"
	ExclusionReasonSeparator    = "; "
)

// snapshotExclusion is one table takeSnapshot's degrade branch left out of the
// snapshot, carried separately from the "schema.table" display strings so the
// insert never has to re-split a concatenated key.
type snapshotExclusion struct {
	schema, table, reason string
	// pkColumn is the free column name a PRIMARY KEY would take on this
	// table (#1802); empty when the table already has one and only its
	// engine is wrong.
	pkColumn string
}

// ensureSnapshotExclusionsTable lazily creates snapshot_exclusions on an index
// that predates it — the ensureSnapshotIDSeqTable pattern: indexer.EnsureSchema
// provisions it eagerly at daemon startup, but the writer must self-heal for
// paths that never run EnsureSchema.
//
// It reports whether pk_column (#1802) is there to write, and adding it is
// BEST EFFORT: the column feeds a report, and a snapshot must never fail over
// one. A false return means the insert leaves it out, and the reader then
// prints no statement instead of a guessed one.
func ensureSnapshotExclusionsTable(ctx context.Context, db *sql.DB) (hasPKColumn bool, err error) {
	var exists bool
	if err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) > 0 FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'snapshot_exclusions'",
	).Scan(&exists); err != nil {
		return false, fmt.Errorf("metadata: check snapshot_exclusions table: %w", err)
	}
	if !exists {
		if _, err := db.ExecContext(ctx, DDLSnapshotExclusions); err != nil {
			return false, fmt.Errorf("metadata: create snapshot_exclusions: %w", err)
		}
		return true, nil
	}
	return ensureSnapshotExclusionsPKColumn(ctx, db), nil
}

// ensureSnapshotExclusionsPKColumn adds pk_column to a snapshot_exclusions
// table written before it existed. Best effort, and loud in the log: without
// it the card names the table and cannot hand over a statement for it.
func ensureSnapshotExclusionsPKColumn(ctx context.Context, db *sql.DB) bool {
	var present bool
	if err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) > 0 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'snapshot_exclusions' AND COLUMN_NAME = 'pk_column'",
	).Scan(&present); err != nil {
		slog.Warn("could not check snapshot_exclusions.pk_column; the report will name each uncaptured table without the statement that fixes it", "error", err)
		return false
	}
	if present {
		return true
	}
	if _, err := db.ExecContext(ctx, "ALTER TABLE snapshot_exclusions ADD COLUMN pk_column VARCHAR(64) DEFAULT NULL"); err != nil {
		slog.Warn("could not add snapshot_exclusions.pk_column; the report will name each uncaptured table without the statement that fixes it", "error", err)
		return false
	}
	return true
}

// SuggestPKColumn picks a column name a table does not already use, for the
// PRIMARY KEY it would take to capture it. Only the snapshot can do this: an
// excluded table has no columns in the snapshot, so no reader of the index
// can see what is already there.
//
// `id` unless the table has one — MySQL refuses a duplicate column name with
// ERROR 1060, and a plain non-key `id` column is the commonest shape of a
// key-less table, so the obvious guess is exactly the one that fails. Names
// compare case-insensitively, as MySQL compares them.
//
// strings.ToLower here is deliberate. The invariant this function needs is
// one-directional: the "taken" key must fold AT LEAST as much as the server
// does. Folding more than the server costs a less pretty name and nothing
// else; folding LESS offers a name the server already has, and the operator
// gets a statement that dies with ERROR 1060 — the exact failure this card
// exists to remove. strings.ToLower satisfies it, including for the Turkish
// dotted İ, which it maps to i exactly as the server does.
//
// So do not reach for IdentEqualFold above, or for strings.EqualFold, without
// re-checking that invariant: EqualFold folds NEITHER Turkish letter, so on a
// table whose only key-ish column is `İd` it would offer `id` and reproduce
// the 1060. Both answers, and the server's own verdict on each, are pinned by
// TestIntegrationSuggestPKColumnNameIsAccepted: `İd` gets `dbtrail_id`
// because the server really does refuse `id` beside it, and `ıd` gets `id`
// because the server really does accept it.
func SuggestPKColumn(existing []string) string {
	taken := make(map[string]bool, len(existing))
	for _, c := range existing {
		taken[strings.ToLower(strings.TrimSpace(c))] = true
	}
	if !taken["id"] {
		return "id"
	}
	if !taken["dbtrail_id"] {
		return "dbtrail_id"
	}
	for n := 2; ; n++ {
		if candidate := fmt.Sprintf("dbtrail_id_%d", n); !taken[candidate] {
			return candidate
		}
	}
}

// loadSnapshotExclusions returns the "schema.table" → reason map a degraded
// snapshot (#1051) recorded for snapshotID. A missing snapshot_exclusions
// table is NOT an error (see the DDLSnapshotExclusions doc: an index that only
// ever took strict snapshots simply has no exclusions) — nil map, nil error.
func loadSnapshotExclusions(db *sql.DB, snapshotID int) (map[string]string, error) {
	var exists bool
	if err := db.QueryRow(
		"SELECT COUNT(*) > 0 FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'snapshot_exclusions'",
	).Scan(&exists); err != nil {
		return nil, fmt.Errorf("metadata: check snapshot_exclusions table: %w", err)
	}
	if !exists {
		return nil, nil
	}
	rows, err := db.Query(
		"SELECT schema_name, table_name, reason FROM snapshot_exclusions WHERE snapshot_id = ?",
		snapshotID)
	if err != nil {
		return nil, fmt.Errorf("metadata: query snapshot_exclusions for snapshot %d: %w", snapshotID, err)
	}
	defer rows.Close()

	var excluded map[string]string
	for rows.Next() {
		var schemaName, tableName, reason string
		if err := rows.Scan(&schemaName, &tableName, &reason); err != nil {
			return nil, fmt.Errorf("metadata: scan snapshot_exclusions row: %w", err)
		}
		if excluded == nil {
			excluded = make(map[string]string)
		}
		excluded[schemaName+"."+tableName] = reason
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("metadata: iterate snapshot_exclusions: %w", err)
	}
	return excluded, nil
}

// IdentEqualFold reports whether two column identifiers name the same column
// for the purposes of this repo's comparisons. It is strings.EqualFold plus a
// fold of the Turkish dotted İ (U+0130) and dotless ı (U+0131) onto 'i'.
//
// Measured on MySQL 8.4.9, default utf8mb4_0900_ai_ci,
// lower_case_table_names=0, each case a CREATE TABLE carrying both names as
// columns, through the same driver the product uses — pinned by
// TestIntegrationMySQLFoldsOnlyASCIICaseInColumnIdentifiers:
//
//	i vs I    ERROR 1060: one identifier
//	i vs İ    ERROR 1060: one identifier  (so the dotted I DOES fold)
//	I vs İ    ERROR 1060: one identifier
//	i vs ı    accepted: two distinct columns
//	I vs ı    accepted: two distinct columns
//	e vs é    accepted: two distinct columns
//
// So the dotted İ folds and Unicode simple folding does not know it, which is
// why strings.EqualFold alone is not enough: it would read the legal
// case-style rename İstanbul -> istanbul as schema drift (#700).
//
// The dotless ı is the other half, and it is NOT verified as folding — the
// server above keeps it apart. This helper folds it anyway, wider than the
// server, on purpose: both callers are safe in that direction and only that
// direction. The drift check (#700) folding more means warning less, never
// reporting drift that is not there; the uncaptured-tables list (#1802)
// folding more shows an extra row, while folding less HIDES a table from a
// list whose whole job is to hide nothing. Narrowing it to match the server
// exactly is therefore a behaviour change to the drift check, not a cleanup.
//
// Accents are not folded by identifiers either. Note this is NOT the data
// collation, which is a different rule with different answers: under
// utf8mb4_0900_ai_ci the string comparison 'e' = 'é' is TRUE (it is
// accent-INsensitive) while the two column names above stay distinct.
//
// SuggestPKColumn below deliberately does NOT use this; see its own note.
func IdentEqualFold(a, b string) bool {
	if strings.EqualFold(a, b) {
		return true
	}
	dotless := func(r rune) rune {
		if r == 'İ' || r == 'ı' {
			return 'i'
		}
		return r
	}
	return strings.EqualFold(strings.Map(dotless, a), strings.Map(dotless, b))
}
