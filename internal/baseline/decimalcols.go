package baseline

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"

	_ "github.com/duckdb/duckdb-go/v2" // DuckDB driver for the footer read below

	"github.com/dbtrail/dbtrail/internal/duckdbutil"
)

// MaxDuckDBDecimalPrecision is the widest DECIMAL DuckDB can represent. MySQL
// allows up to 65 digits, so a column past this ceiling has no DuckDB DECIMAL
// to be cast to and stays text. Measured against the linked engine, not assumed:
// DECIMAL(39,2) is refused with "Width must be between 1 and 38".
const MaxDuckDBDecimalPrecision = 38

// TableFooter is what the state views need from one baseline file's embedded
// CREATE TABLE.
type TableFooter struct {
	// Decimals are the table's decimal and numeric columns.
	Decimals []DecimalColumn
	// DeltaReserved says the table has a column under a name a table delta
	// reserves (TableDeltaReservedColumns). Such a table is never published
	// with a delta, and the chain-aware state SQL does not even bind over it:
	// DuckDB refuses the filename and file_row_number options on a file that
	// already has a column of that name.
	DeltaReserved bool
	// Datetimes are the table's DATETIME columns, by name. The writer stores
	// DATETIME and TIMESTAMP the same way (a UTC-adjusted Parquet timestamp),
	// so the file alone cannot tell a wall-clock DATETIME from a TIMESTAMP
	// instant; the embedded CREATE TABLE can, and a reader under a session
	// time zone other than UTC needs to (views.BaselineTable.Datetimes).
	Datetimes []string
}

// TableFootersFor reports, for each baseline Parquet file, the decimal and
// numeric columns and whether a table delta reserves one of its column names,
// read from the CREATE TABLE the writer embedded in the file's footer. The
// result is keyed by the path as it was passed in.
//
// PRESENCE in the map means the file's schema was read, and is deliberately
// distinct from the value being empty. A table with no decimal column maps to
// an empty Decimals slice; a file whose footer carries no CREATE TABLE at all
// (a baseline written before that key existed) is ABSENT. Collapsing the two
// would let a caller report "this table has no decimal columns" about a table
// it never managed to look at.
//
// One DuckDB session reads every footer in a single parquet_kv_metadata() call,
// falling back to one call per file if that batch fails (see below). Neither
// loops over ReadParquetMetadataAny: on S3 that helper validates the whole
// object against its snapshot manifest and opens its own session per file, so a
// per-table loop through it would download an entire baseline to learn its
// column types. parquet_kv_metadata reads footers only.
//
// Callers are expected to treat a returned error as "no type information
// available" and carry on — knowing a column's precision is an improvement to
// the output, never a precondition for producing it. The error is returned
// rather than swallowed so the caller can say so in its own voice. Note that a
// per-FILE failure is not one of those errors: it leaves that path absent and
// is logged here, because the readable files' answers are still worth having.
func TableFootersFor(ctx context.Context, paths []string) (map[string]TableFooter, error) {
	if len(paths) == 0 {
		return nil, nil
	}

	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}
	defer db.Close()

	if anyS3(paths) {
		if err := duckdbutil.LoadHTTPFS(ctx, db); err != nil {
			return nil, fmt.Errorf("load httpfs extension: %w", err)
		}
		if err := duckdbutil.EnableS3CredentialChain(ctx, db); err != nil {
			return nil, err
		}
	}

	// file_name comes back as the string DuckDB was handed, which is what lets
	// the result be keyed by the caller's own path. A path DuckDB reports
	// differently would simply not be found by the caller and lose its casts,
	// which is the same output this function existed to improve on.
	rows, err := db.QueryContext(ctx, decimalFooterQuery(paths))
	if err != nil {
		// DuckDB resolves the file list up front, so ONE unreadable file (a
		// zero-byte upload, a truncated write, an object that vanished between
		// the listing and now) fails the whole batch. Read them one at a time
		// instead, in this same session, so a local fault costs only its own
		// table its casts rather than every table in the snapshot.
		//
		// The batch error travels into the fallback's own report. When the
		// fault is session-wide instead of per-file (an S3 403, no httpfs) every
		// per-file read fails too, and this is the one error that says why.
		return decimalColumnsPerFile(ctx, db, paths, err), nil
	}
	defer rows.Close()

	out := make(map[string]TableFooter)
	collectDecimalRows(rows, out)
	return out, nil
}

// decimalFooterQuery reads the embedded CREATE TABLE out of each listed file's
// footer. parquet_kv_metadata reads footers only, never row data.
func decimalFooterQuery(paths []string) string {
	return "SELECT file_name, value FROM parquet_kv_metadata(" + fileListLiteral(paths) + ") WHERE key = " +
		sqlQuoteLiteral(MetaKeyCreateTableSQL)
}

// decimalColumnsPerFile is the batched read's fallback: one query per file, so
// the files that ARE readable keep their entries. Files that fail stay absent
// from the map, which is how the caller reports "could not look" as distinct
// from "nothing to cast".
//
// The failures are logged rather than returned. The caller's answer is the map
// either way, an absent table already says so in the generated file, and a
// snapshot with several unreadable footers should not turn into an error that
// costs the readable tables their casts, which is the exact failure this
// fallback exists to undo.
func decimalColumnsPerFile(ctx context.Context, db *sql.DB, paths []string, batchErr error) map[string]TableFooter {
	out := make(map[string]TableFooter)
	var failed []string
	for _, p := range paths {
		rows, err := db.QueryContext(ctx, decimalFooterQuery([]string{p}))
		if err != nil {
			failed = append(failed, p)
			slog.Debug("baseline: could not read a Parquet footer for its column types",
				"path", p, "error", err)
			continue
		}
		collectDecimalRows(rows, out)
		rows.Close()
	}
	if len(failed) > 0 {
		// batchErr is reported alongside the count because when EVERY file
		// failed the cause is usually not any one file (no httpfs, an S3 403,
		// a cancelled context) and the per-file errors are all the same
		// downstream symptom.
		slog.Warn("baseline: some Parquet footers could not be read for their column types; "+
			"those tables' state views will not cast decimal columns",
			"unreadable_files", len(failed), "total_files", len(paths),
			"first", failed[0], "error", batchErr)
	}
	return out
}

// collectDecimalRows folds one footer query's rows into the result map. Shared
// by the batched read and the per-file fallback so the two cannot disagree
// about what an entry means.
func collectDecimalRows(rows *sql.Rows, out map[string]TableFooter) {
	for rows.Next() {
		var file string
		// parquet_kv_metadata types both key and value as BLOB.
		var createSQL []byte
		if err := rows.Scan(&file, &createSQL); err != nil {
			slog.Debug("baseline: could not scan Parquet footer metadata", "error", err)
			continue
		}
		cols, err := ParseSchemaText(string(createSQL))
		if err != nil {
			// One unparseable CREATE TABLE costs that table its casts, not the
			// whole run. Left ABSENT, so the generated file reports it as a
			// table whose types are unknown rather than one with no decimals.
			//
			// Warn, not Debug: a baseline that predates the embedded schema
			// produces no row here at all (the query filters on the key), so
			// reaching this branch means the key IS present and its value does
			// not parse. That is always an anomaly worth naming, never the
			// ordinary old-baseline case.
			slog.Warn("baseline: the CREATE TABLE embedded in a Parquet footer would not parse; "+
				"this table's state view will not cast decimal columns",
				"path", file, "error", err)
			continue
		}
		decs := DecimalColumns(cols)
		if decs == nil {
			// Non-nil so the entry is unambiguously "schema read, no decimal
			// columns" rather than a nil that reads like an absent key.
			decs = []DecimalColumn{}
		}
		out[file] = TableFooter{Decimals: decs, DeltaReserved: hasDeltaReservedColumn(cols), Datetimes: DatetimeColumns(cols)}
	}
	if err := rows.Err(); err != nil {
		// Warn: an iteration that dies partway leaves every file after the
		// break absent, and those tables lose their casts for a reason that is
		// nowhere in the output otherwise.
		//
		// Deliberately NOT a len(out) vs len(paths) shortfall check. A short
		// result is the NORMAL shape for a baseline older than the embedded
		// schema and for every PostgreSQL-source baseline, neither of which
		// carries the key, so counting would fire a fault on the two cases
		// where nothing is wrong. rows.Err is the signal that something
		// actually broke.
		slog.Warn("baseline: reading Parquet footer metadata ended early; "+
			"some tables' state views will not cast decimal columns", "error", err)
	}
}

// DecimalColumns picks the decimal and numeric columns out of a parsed schema.
func DecimalColumns(cols []Column) []DecimalColumn {
	var out []DecimalColumn
	for _, c := range cols {
		if c.MySQLType != "decimal" && c.MySQLType != "numeric" {
			continue
		}
		out = append(out, DecimalColumn{
			Name:      c.Name,
			Precision: c.DecimalPrecision,
			Scale:     c.DecimalScale,
		})
	}
	return out
}

// DatetimeColumns picks the DATETIME columns out of a parsed schema: the ones
// whose value is a wall clock with no zone. TIMESTAMP columns are instants and
// are not listed.
func DatetimeColumns(cols []Column) []string {
	var out []string
	for _, c := range cols {
		if c.MySQLType == "datetime" {
			out = append(out, c.Name)
		}
	}
	return out
}

func anyS3(paths []string) bool {
	for _, p := range paths {
		if strings.HasPrefix(p, "s3://") {
			return true
		}
	}
	return false
}

// fileListLiteral renders paths as a DuckDB list literal ['a', 'b'].
func fileListLiteral(paths []string) string {
	quoted := make([]string, len(paths))
	for i, p := range paths {
		quoted[i] = sqlQuoteLiteral(p)
	}
	return "[" + strings.Join(quoted, ", ") + "]"
}

func sqlQuoteLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// hasDeltaReservedColumn reports whether any column is named like one a table
// delta reserves, by the same case-insensitive rule TableDeltaColumns refuses
// such a table with.
func hasDeltaReservedColumn(cols []Column) bool {
	for _, c := range cols {
		for _, reserved := range TableDeltaReservedColumns {
			if strings.EqualFold(c.Name, reserved) {
				return true
			}
		}
	}
	return false
}
