package baseline

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"path/filepath"
	"sort"
	"strings"
	"sync"

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
	// BinaryText are the table's text columns under a _bin collation, by
	// name (BinaryCollationColumns): the ones MySQL compares byte by byte.
	BinaryText []string
	// Columns are the table's columns in the order its CREATE TABLE declares
	// them, which is the order MySQL's `SELECT *` returns. The file itself
	// holds them sorted by name. Nil when the order is not known: the file
	// does not hold exactly the columns the CREATE TABLE lists, or its own
	// column list could not be read (confirmColumnOrder). Never a guess.
	Columns []string
	// StarDiffers says, in words, why MySQL's `SELECT *` on this table returns
	// a different set of columns from the one the file holds (a generated
	// column, an invisible one, a definition that could not be read), or ""
	// when it returns the same set (starDifference). No ordering fixes it.
	StarDiffers string
	// NotHeld names the columns a statement on MySQL can name and the file
	// does not hold: the generated ones, invisible or not, and a MariaDB
	// versioned table's implicit row_start and row_end (columnsNotHeld).
	// NotHeldUnread says the list may be short of one: a column definition
	// could not be read, so its name is not known.
	NotHeld       []string
	NotHeldUnread bool
	// Temporal are the table's DATE, DATETIME, TIMESTAMP, TIME and YEAR
	// columns, each with its type, and every column whose type is not one
	// this package knows, with an empty type (TemporalColumns). Read routing
	// keeps a statement that does arithmetic on one, or names a TIME or a
	// YEAR, on the source (#2133).
	Temporal []TemporalColumn
}

// TemporalColumn is a column MySQL holds as a date, a time or a year, and the
// copy under a type that computes or compares another way. Type is the MySQL
// type in lower case (date, datetime, timestamp, time, year), or "" for a
// column whose type is not known here: such a column is treated as a date
// could be, because not known is never read as "not a date".
type TemporalColumn struct {
	Name, Type string
}

// knownOtherTypes are the column types this package knows and that are not
// temporal: every type MySQL 8.4 and MariaDB 11.4 print in SHOW CREATE TABLE
// that is not DATE, DATETIME, TIMESTAMP, TIME or YEAR.
var knownOtherTypes = map[string]bool{
	"tinyint": true, "smallint": true, "mediumint": true, "int": true, "integer": true, "bigint": true,
	"decimal": true, "numeric": true, "float": true, "double": true, "real": true, "bit": true, "bool": true, "boolean": true,
	"char": true, "varchar": true, "tinytext": true, "text": true, "mediumtext": true, "longtext": true,
	"binary": true, "varbinary": true, "tinyblob": true, "blob": true, "mediumblob": true, "longblob": true,
	"enum": true, "set": true, "json": true,
	"geometry": true, "point": true, "linestring": true, "polygon": true, "multipoint": true, "multilinestring": true,
	"multipolygon": true, "geometrycollection": true, "geomcollection": true,
	"uuid": true, "inet4": true, "inet6": true, "vector": true,
}

// TemporalColumns picks out of a parsed schema the columns read routing must
// know the type of (#2133): the DATE, DATETIME, TIMESTAMP, TIME and YEAR
// ones, and any column of a type that is neither one of those nor in
// knownOtherTypes, listed with an empty type.
func TemporalColumns(cols []Column) []TemporalColumn {
	var out []TemporalColumn
	for _, c := range cols {
		switch c.MySQLType {
		case "date", "datetime", "timestamp", "time", "year":
			out = append(out, TemporalColumn{Name: c.Name, Type: c.MySQLType})
		default:
			if !knownOtherTypes[c.MySQLType] {
				out = append(out, TemporalColumn{Name: c.Name})
			}
		}
	}
	return out
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
	r, err := ReadTableFooters(ctx, paths)
	return r.Footers, err
}

// SchemaLossConsequence is what a table's state view does without its
// CREATE TABLE, in the words every warning about it uses: the decimal columns
// stay text, a text column MySQL declares _bin is compared like every other
// one, without regard to case (#2083), and `SELECT *` returns the columns in
// the file's order, alphabetical, instead of the table's (#2111).
const SchemaLossConsequence = "will not cast decimal columns, will compare _bin text columns without regard to case " +
	"and will return the columns of SELECT * in alphabetical order instead of the table's"

// FooterRead is ReadTableFooters' whole answer: what was read, and for every
// other file which of two different things happened.
type FooterRead struct {
	// Footers holds the files whose embedded CREATE TABLE was read, keyed by
	// the path as passed.
	Footers map[string]TableFooter
	// NoSchema are the files that were looked at and carry no usable CREATE
	// TABLE: none in the footer (a snapshot of a PostgreSQL source, or one
	// written before the key existed) or one that does not parse. A fact about
	// an immutable file: looking again gives the same answer.
	NoSchema []string
	// Unread are the files that could not be looked at: the footer read
	// failed, a row would not scan, or the read ended early. A fault, which
	// may be gone on the next try; a caller that caches must not keep it.
	//
	// A file can be in Footers AND here: its CREATE TABLE was read and its
	// own column list was not, so it has its casts and not its column order
	// (TableFooter.Columns). Asking again is how it gets the order.
	Unread []string
}

// ReadTableFooters is TableFootersFor with the two kinds of absence told
// apart. Both are also logged here, once: Unread as one Warn per call with the
// count, NoSchema as one Warn per table for the life of the process, because
// either one changes how that table's view answers (SchemaLossConsequence) and
// the generated SQL is not where an operator looks.
func ReadTableFooters(ctx context.Context, paths []string) (FooterRead, error) {
	if len(paths) == 0 {
		return FooterRead{}, nil
	}

	db, err := sql.Open("duckdb", "")
	if err != nil {
		return FooterRead{}, fmt.Errorf("open duckdb: %w", err)
	}
	defer db.Close()

	if anyS3(paths) {
		if err := duckdbutil.LoadHTTPFS(ctx, db); err != nil {
			return FooterRead{}, fmt.Errorf("load httpfs extension: %w", err)
		}
		if err := duckdbutil.EnableS3CredentialChain(ctx, db); err != nil {
			return FooterRead{}, err
		}
	}

	// file_name comes back as the string DuckDB was handed, which is what lets
	// the result be keyed by the caller's own path. A path DuckDB reports
	// differently would simply not be found by the caller and lose its casts,
	// which is the same output this function existed to improve on.
	st := footerScan{footers: make(map[string]TableFooter), unparsable: map[string]bool{}, failed: map[string]bool{}}
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
		decimalColumnsPerFile(ctx, db, paths, &st)
	} else {
		collectDecimalRows(rows, &st)
		rows.Close()
	}
	confirmColumnOrder(ctx, db, &st)
	return st.result(paths, err), nil
}

// footerScan accumulates one ReadTableFooters call.
type footerScan struct {
	footers map[string]TableFooter
	// unparsable are the files whose embedded CREATE TABLE would not parse.
	unparsable map[string]bool
	// failed are the files whose own footer read failed.
	failed map[string]bool
	// lostRows counts rows that would not scan, and incomplete says the read
	// ended early: in both cases SOME file went unseen and nothing says which,
	// so every file without an answer counts as unread.
	lostRows   int
	incomplete bool
	// orderUnread are the files whose CREATE TABLE was read and whose own
	// column list was not (confirmColumnOrder).
	orderUnread []string
	// orderErr is the last error of that read, for the one warning about it.
	orderErr error
}

// result sorts every path into read, no schema, or unread, and logs the last
// two. batchErr is the batched read's error when the per-file fallback ran.
func (st *footerScan) result(paths []string, batchErr error) FooterRead {
	r := FooterRead{Footers: st.footers}
	var legacy []string
	for _, p := range paths {
		switch _, read := st.footers[p]; {
		case read:
		case st.unparsable[p]:
			r.NoSchema = append(r.NoSchema, p)
		case st.failed[p], st.incomplete, st.lostRows > 0:
			r.Unread = append(r.Unread, p)
		default:
			r.NoSchema = append(r.NoSchema, p)
			legacy = append(legacy, p)
		}
	}
	warnNoSchema(legacy)
	if len(r.Unread) > 0 {
		// batchErr is reported alongside the count because when EVERY file
		// failed the cause is usually not any one file (no httpfs, an S3 403,
		// a cancelled context) and the per-file errors are all the same
		// downstream symptom.
		slog.Warn("baseline: some Parquet footers could not be read for their column types; "+
			"those tables' state views "+SchemaLossConsequence+" until a later read succeeds",
			"unreadable_files", len(r.Unread), "total_files", len(paths),
			"first", r.Unread[0], "rows_that_would_not_scan", st.lostRows, "ended_early", st.incomplete,
			"error", batchErr)
	}
	if len(st.orderUnread) > 0 {
		sort.Strings(st.orderUnread)
		r.Unread = append(r.Unread, st.orderUnread...)
		slog.Warn("baseline: some snapshot files' column lists could not be read, so those tables' column order is not known; "+
			"their state views return SELECT * in alphabetical order until a later read succeeds",
			"files", len(st.orderUnread), "first", st.orderUnread[0], "error", st.orderErr)
	}
	return r
}

// warnedNoSchema holds the tables already warned about for carrying no CREATE
// TABLE. The key is the table within its snapshot root (noSchemaKey), so a
// later snapshot of the same table does not warn again and the same table
// name under another source's root does.
var warnedNoSchema sync.Map

// noSchemaWarnCap is how many tables one read names. A source where no table
// carries a CREATE TABLE (every PostgreSQL source) has as many of these as it
// has tables, and a one-shot command would print them all on every run.
const noSchemaWarnCap = 10

// noSchemaKey splits a snapshot file's path, <root>/<snapshot>/<schema>/
// <table>.parquet, into what identifies the table across snapshots (the root
// and "schema.table") and the name to print.
func noSchemaKey(path string) (key, table string) {
	parts := strings.Split(filepath.ToSlash(path), "/")
	table = strings.TrimSuffix(parts[len(parts)-1], ".parquet")
	if len(parts) < 2 {
		return table, table
	}
	table = parts[len(parts)-2] + "." + table
	if len(parts) < 4 {
		return table, table
	}
	return strings.Join(parts[:len(parts)-3], "/") + "|" + table, table
}

// warnNoSchema logs, once per table per process, that a snapshot file carries
// no CREATE TABLE. It is not a fault (a PostgreSQL-source snapshot never
// carries one, and neither does one older than the key), which is why nothing
// was logged before; it is logged now because of what it costs. The first
// noSchemaWarnCap tables of a read are named; the rest are counted in one
// line and named at Debug.
func warnNoSchema(paths []string) {
	more := 0
	named := 0
	for _, p := range paths {
		key, table := noSchemaKey(p)
		if _, seen := warnedNoSchema.LoadOrStore(key, struct{}{}); seen {
			continue
		}
		if named == noSchemaWarnCap {
			more++
			slog.Debug("baseline: this table's snapshot file carries no CREATE TABLE", "table", table, "path", p)
			continue
		}
		named++
		slog.Warn("baseline: this table's snapshot file carries no CREATE TABLE, so its column types and collations are unknown; "+
			"its state view "+SchemaLossConsequence+", and under read routing MySQL answers every statement over this table. "+
			"Take a new full snapshot: one of a MySQL or MariaDB source records them "+
			"(a PostgreSQL source has none to record)",
			"table", table, "path", p)
	}
	if more > 0 {
		slog.Warn("baseline: more tables' snapshot files carry no CREATE TABLE, with the same consequence; "+
			"the debug log names each", "more_tables", more)
	}
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
// The failures are recorded in the scan rather than returned as an error
// (footerScan.result counts and logs them): a snapshot with several unreadable
// footers should not turn into an error that costs the readable tables their
// casts, which is the exact failure this fallback exists to undo.
func decimalColumnsPerFile(ctx context.Context, db *sql.DB, paths []string, st *footerScan) {
	for _, p := range paths {
		rows, err := db.QueryContext(ctx, decimalFooterQuery([]string{p}))
		if err != nil {
			st.failed[p] = true
			slog.Debug("baseline: could not read a Parquet footer for its column types",
				"path", p, "error", err)
			continue
		}
		collectDecimalRows(rows, st)
		rows.Close()
	}
}

// collectDecimalRows folds one footer query's rows into the result map. Shared
// by the batched read and the per-file fallback so the two cannot disagree
// about what an entry means.
func collectDecimalRows(rows *sql.Rows, st *footerScan) {
	for rows.Next() {
		var file string
		// parquet_kv_metadata types both key and value as BLOB.
		var createSQL []byte
		if err := rows.Scan(&file, &createSQL); err != nil {
			st.lostRows++
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
			st.unparsable[file] = true
			slog.Warn("baseline: the CREATE TABLE embedded in a Parquet footer would not parse; "+
				"this table's state view "+SchemaLossConsequence,
				"path", file, "error", err)
			continue
		}
		decs := DecimalColumns(cols)
		if decs == nil {
			// Non-nil so the entry is unambiguously "schema read, no decimal
			// columns" rather than a nil that reads like an absent key.
			decs = []DecimalColumn{}
		}
		// Columns is the DECLARED order here; confirmColumnOrder keeps it only
		// for a file that holds exactly these columns.
		notHeld, notHeldUnread := columnsNotHeld(string(createSQL))
		st.footers[file] = TableFooter{Decimals: decs, DeltaReserved: hasDeltaReservedColumn(cols), Datetimes: DatetimeColumns(cols),
			BinaryText: BinaryCollationColumns(string(createSQL), cols),
			Columns:    columnNames(cols), StarDiffers: starDifference(string(createSQL)),
			NotHeld: notHeld, NotHeldUnread: notHeldUnread, Temporal: TemporalColumns(cols)}
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
		st.incomplete = true
		slog.Warn("baseline: reading Parquet footer metadata ended early; "+
			"some tables' state views "+SchemaLossConsequence, "error", err)
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
