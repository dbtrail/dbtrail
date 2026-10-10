package doctor

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"
	"unicode"

	mysqldriver "github.com/go-sql-driver/mysql"
)

// What a full read (mydumper's dump of the source, then its conversion to
// Parquet) needs on disk, as the source reports it. The console checks it
// before every full read (consoleapp/backup_disk.go, which holds the account
// of the measurements behind the two ratios) and `bintrail doctor
// --staging-dir` reports it (#2259). Both read the numbers here, so the two
// cannot give different answers about the same tables.

// DumpableTablesWhere is the information_schema.TABLES filter for the tables
// a console dump selects, shared by the console's no-lock count
// (dumpableTableCountQuery) and the size estimate below (dumpSizeQuery,
// #1938) so the two cannot drift apart.
func DumpableTablesWhere(schemas []string) (string, []any) {
	// SYSTEM VERSIONED is how MariaDB lists a system-versioned table; it is
	// a table mydumper dumps like any other (#1993 reads this list to find
	// tables a snapshot lacks, and would never see one created that way).
	const base = "TABLE_TYPE IN ('BASE TABLE', 'SYSTEM VERSIONED') AND "
	if len(schemas) == 0 {
		return base + "TABLE_SCHEMA NOT IN ('mysql','sys','performance_schema','information_schema')", nil
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(schemas)), ",")
	args := make([]any, len(schemas))
	for i, s := range schemas {
		args[i] = s
	}
	return base + "TABLE_SCHEMA IN (" + placeholders + ")", args
}

// DumpPeakTenths is the dump-plus-Parquet peak as tenths of the estimate:
// 18 = 1.8x, the highest ratio the #1938 measurement saw against
// DATA_LENGTH + INDEX_LENGTH (random binary: 94 MiB dump + 84 MiB Parquet
// over 99 MiB).
//
// It was measured with the whole dump on disk until the last table
// was converted. Since #1938 each table's dump data is removed when its Parquet
// file is written (baseline.Config.RemoveConvertedData), so the real peak is
// at most what it was and usually lower: how much lower depends on how many
// tables there are next to the CPUs converting them, and on whether one table
// dominates. The constant is deliberately left as it was until that is
// measured (#2256): it warns too early, never too late.
const DumpPeakTenths = 18

// DumpRefuseTenths is the refusal line as tenths of the tables' DATA size:
// below half of it no measured dump fit, so refusing there is not a guess.
const DumpRefuseTenths = 5

// DumpEstimateServerLimit is how long the SOURCE may spend on the estimate's
// query before it stops it itself. Under DumpEstimateTimeout, so what comes
// back is the server's own error and not a connection closed under a query
// that goes on running. (When connecting and the session's settings take more
// than the difference, the wait ends first and the query runs on for at most
// this long: still bounded.)
const DumpEstimateServerLimit = 12 * time.Second

// dumpCompressedNamed is how many compressed tables a note names: the largest
// ones, which are the ones the sizes are most wrong about.
const dumpCompressedNamed = 2

// DumpEstimateTimeout bounds the information_schema read. It runs before every
// full read, and a source that does not answer in time must cost a note, not
// the backup.
const DumpEstimateTimeout = 15 * time.Second

// DumpEstimate is what the source reports about the tables a dump selects.
type DumpEstimate struct {
	Bytes int64 // DATA_LENGTH + INDEX_LENGTH, summed: what the warnings read
	// DataBytes is DATA_LENGTH alone, summed: what the refusal reads, since
	// a dump holds no secondary indexes.
	DataBytes int64
	Tables    int
	// Compressed counts tables whose reported size is their compressed size;
	// CompressedTop names the largest dumpCompressedNamed of them.
	Compressed    int
	CompressedTop []string
	// Unsized counts tables the server reported no size for (NULL). They add
	// nothing to Bytes, so the note says the estimate is short by them.
	Unsized int
	// Stale: the source refused to serve current sizes (not the 1193 of a
	// server that has no cache), so they may be up to a day old.
	Stale bool
}

// dumpSizeQuery is the estimate's query over the same table selection as
// dumpableTableCountQuery, which is the selection buildConsoleMydumperArgs
// asks mydumper for. One row per table, summed here: the source walks every
// table for a SUM anyway, a second query for the compressed ones would walk
// them again, and GROUP_CONCAT cuts a list of names at 1024 bytes without a
// word.
func dumpSizeQuery(schemas []string) (string, []any) {
	where, args := DumpableTablesWhere(schemas)
	return "SELECT TABLE_SCHEMA, TABLE_NAME, ENGINE, ROW_FORMAT, DATA_LENGTH, INDEX_LENGTH " +
		"FROM information_schema.TABLES WHERE " + where, args
}

// dumpTableRow is one row of dumpSizeQuery. Everything but the names can be
// NULL: a table the server cannot open reports no engine, format or size.
type dumpTableRow struct {
	schema, table     string
	engine, rowFormat sql.NullString
	data, index       sql.NullInt64
}

// storedCompressed reports whether the server's size for this table is its
// compressed size. Exact words, any case: MySQL and MariaDB both spell the
// row format "Compressed", and MyRocks reports its engine as "ROCKSDB".
func (r dumpTableRow) storedCompressed() bool {
	return (r.rowFormat.Valid && strings.EqualFold(r.rowFormat.String, "Compressed")) ||
		(r.engine.Valid && strings.EqualFold(r.engine.String, "ROCKSDB"))
}

// dumpTableSum adds rows up as they are read, so a source with a million
// tables costs this process a few numbers and two names, not a million rows
// held in the capture process. A NULL size adds nothing and counts the table
// as unsized, so the note says the estimate is short by it.
type dumpTableSum struct {
	est DumpEstimate
	// top holds the largest compressed tables seen so far, largest first,
	// at most dumpCompressedNamed of them.
	top []namedSize
}

type namedSize struct {
	name string
	size int64
}

// before reports whether a goes ahead of b: larger first, then by name, so
// the names a note prints do not change between two runs over equal sizes.
func (a namedSize) before(b namedSize) bool {
	if a.size != b.size {
		return a.size > b.size
	}
	return a.name < b.name
}

func (s *dumpTableSum) add(r dumpTableRow) {
	s.est.Tables++
	if !r.data.Valid || !r.index.Valid {
		s.est.Unsized++
	}
	data, index := max(r.data.Int64, 0), max(r.index.Int64, 0)
	s.est.DataBytes += data
	s.est.Bytes += data + index
	if !r.storedCompressed() {
		return
	}
	s.est.Compressed++
	n := namedSize{printableName(r.schema) + "." + printableName(r.table), data + index}
	at := len(s.top)
	for at > 0 && n.before(s.top[at-1]) {
		at--
	}
	s.top = slices.Insert(s.top, at, n)
	s.top = s.top[:min(len(s.top), dumpCompressedNamed)]
}

func (s *dumpTableSum) estimate() DumpEstimate {
	est := s.est
	est.CompressedTop = nil
	for _, n := range s.top {
		est.CompressedTop = append(est.CompressedTop, n.name)
	}
	return est
}

// summarizeDumpTables is dumpTableSum over a list.
func summarizeDumpTables(rows []dumpTableRow) DumpEstimate {
	var sum dumpTableSum
	for _, r := range rows {
		sum.add(r)
	}
	return sum.estimate()
}

// printableName keeps a schema or table name on the line it is printed in: a
// name may hold a newline or any other control character, and the note it
// lands in is cut at its first line in more than one place.
func printableName(s string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return '?'
		}
		return r
	}, s)
}

// DumpCompressedSentence is what a verdict says about tables stored
// compressed, "" when there are none. It starts with a space, like the other
// parts of a note's tail.
func DumpCompressedSentence(est DumpEstimate) string {
	if est.Compressed == 0 {
		return ""
	}
	names := strings.Join(est.CompressedTop, ", ")
	if more := est.Compressed - len(est.CompressedTop); more > 0 {
		names += fmt.Sprintf(" and %d more", more)
	}
	if est.Compressed == 1 {
		return fmt.Sprintf(" 1 table uses compressed storage (%s): the server reports its compressed size, and a full read writes it "+
			"uncompressed, so a full read can need more than the sizes say.", names)
	}
	return fmt.Sprintf(" %d tables use compressed storage (%s): the server reports their compressed size, and a full read writes them "+
		"uncompressed, so a full read can need more than the sizes say.", est.Compressed, names)
}

// DumpSessionExecer is the part of a connection PrepareDumpEstimateSession uses.
type DumpSessionExecer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// PrepareDumpEstimateSession sets up the estimate's own connection, and reports
// whether the sizes it will read may be stale.
//
// Fresh sizes: MySQL 8.0 and newer serve information_schema sizes from a
// cache that can be a day old (information_schema_stats_expiry), which
// undercounts a table that grew. A server without the variable (MariaDB, 5.7)
// refuses the SET with 1193, and its sizes are not cached. Any other refusal
// (a proxy that rejects the SET, say) leaves the day-old cache in play.
//
// A limit the source enforces: the read's own timeout only ends the waiting.
// The driver then closes the connection without KILL QUERY, and with fresh
// sizes asked for the server opens every table, which on a source with tens
// of thousands of them goes on for minutes after nobody is listening. MySQL
// takes max_execution_time (milliseconds). MariaDB does not know it (1193)
// and takes max_statement_time (seconds), which is sent ONLY then: some
// Percona builds read that same name in milliseconds, where 12 would end
// every estimate at once. A limit that could not be set is not a reason to
// skip the check: the wait is still bounded.
func PrepareDumpEstimateSession(ctx context.Context, conn DumpSessionExecer) (stale bool) {
	unknownVariable := func(err error) bool {
		var me *mysqldriver.MySQLError
		return errors.As(err, &me) && me.Number == 1193
	}
	if _, err := conn.ExecContext(ctx, "SET SESSION information_schema_stats_expiry = 0"); err != nil {
		if unknownVariable(err) {
			slog.Debug("full read disk check: the source has no size cache to turn off", "error", err)
		} else {
			slog.Warn("full read disk check: could not ask the source for current table sizes; it may answer with sizes up to a day old", "error", err)
			stale = true
		}
	}
	refused := func(set string, err error) {
		slog.Warn("full read disk check: the source refused a time limit for the size query, so the query can go on there after "+
			"this read stops waiting for it", "statement", set, "error", err)
	}
	mysqlLimit := fmt.Sprintf("SET SESSION max_execution_time = %d", DumpEstimateServerLimit.Milliseconds())
	_, err := conn.ExecContext(ctx, mysqlLimit)
	switch {
	case err == nil:
	case unknownVariable(err):
		mariaLimit := fmt.Sprintf("SET SESSION max_statement_time = %d", int(DumpEstimateServerLimit.Seconds()))
		if _, err := conn.ExecContext(ctx, mariaLimit); err != nil {
			refused(mariaLimit, err)
		}
	default:
		refused(mysqlLimit, err)
	}
	return stale
}

// ReadDumpEstimate runs dumpSizeQuery on db, on one connection prepared by
// PrepareDumpEstimateSession: the SET SESSION statements hold for that
// connection only. The caller opens db, with its own TLS settings.
func ReadDumpEstimate(ctx context.Context, db *sql.DB, schemas []string) (DumpEstimate, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return DumpEstimate{}, err
	}
	defer conn.Close()
	stale := PrepareDumpEstimateSession(ctx, conn)
	q, args := dumpSizeQuery(schemas)
	res, err := conn.QueryContext(ctx, q, args...)
	if err != nil {
		return DumpEstimate{}, err
	}
	defer res.Close()
	var sum dumpTableSum
	for res.Next() {
		var r dumpTableRow
		if err := res.Scan(&r.schema, &r.table, &r.engine, &r.rowFormat, &r.data, &r.index); err != nil {
			return DumpEstimate{}, err
		}
		sum.add(r)
	}
	// A limit that fires mid-scan ends the rows early with an error here; a
	// partial sum must never be returned as the whole.
	if err := res.Err(); err != nil {
		return DumpEstimate{}, err
	}
	est := sum.estimate()
	est.Stale = stale
	return est, nil
}
