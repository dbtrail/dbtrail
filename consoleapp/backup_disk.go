package consoleapp

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

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/console"
	mysqldriver "github.com/go-sql-driver/mysql"
)

// A full read (#1938) writes mydumper's whole dump, uncompressed, into the
// staging folder, and only then converts it to Parquet. While the conversion
// runs both are on disk. The check below looks at that peak BEFORE mydumper
// starts, so a disk that cannot hold it costs a refusal and not a dump of the
// source that dies halfway and fills the disk other things live on.
//
// The size is SUM(DATA_LENGTH + INDEX_LENGTH) of the tables the dump selects.
// Measured on synthetic data (issue #1938): the dump stayed below that sum for
// every table shape tried, and the dump plus its Parquet reached 1.8x of it for
// random binary data, which Parquet cannot compress. (The issue quotes 1.6x;
// that ratio is against the table's file on disk, which is larger than
// DATA_LENGTH + INDEX_LENGTH. Against the sum this check reads, the same
// table peaked at 1.8x.) The sum is also an upper bound for the dump alone:
// secondary indexes are not dumped and deleted rows still count, and the
// smallest dump measured was 0.6x of it for a table that was not mostly
// deleted. So a refusal waits for less than half the estimate, a line no
// measured dump reached, and everything between that and 1.8x runs with a
// warning that says the same things a refusal would.
//
// The REFUSAL does not read that sum. A dump holds no secondary indexes, so a
// table that is mostly indexes (10 GB of rows under 30 GB of indexes) was
// refused with room to spare, and a refusal has no way past it. It reads
// SUM(DATA_LENGTH) alone: the smallest dump measured against it was 0.89x
// (apart from a table with most of its rows deleted), so half of it is still
// a line no measured dump reached, and since data is never more than data
// plus indexes the change can only turn refusals into warnings. The warnings
// keep the sum.
//
// Neither number bounds anything for a table stored COMPRESSED
// (ROW_FORMAT=COMPRESSED, MyRocks): the server reports the compressed size and
// the dump is written uncompressed. Those tables are counted and named, no
// verdict calls the sizes a bound when there are any, and "there is room"
// becomes "the check cannot vouch for it". InnoDB page compression
// (COMPRESSION=, PAGE_COMPRESSED) is not in that set: whether its reported
// size is the compressed one has not been measured.

// Values of BaselineStatus.DiskCheck.
const (
	dumpDiskOK        = "ok"        // room for the peak
	dumpDiskLow       = "low"       // room for the dump, maybe not for dump plus Parquet
	dumpDiskUnchecked = "unchecked" // the check could not run, or could not vouch for its sizes; the read went ahead
)

// dumpPeakTenths is the dump-plus-Parquet peak as tenths of the estimate:
// 18 = 1.8x, the highest ratio the #1938 measurement saw against
// DATA_LENGTH + INDEX_LENGTH (random binary: 94 MiB dump + 84 MiB Parquet
// over 99 MiB).
const dumpPeakTenths = 18

// dumpRefuseTenths is the refusal line as tenths of the tables' DATA size:
// below half of it no measured dump fit, so refusing there is not a guess.
const dumpRefuseTenths = 5

// dumpEstimateServerLimit is how long the SOURCE may spend on the estimate's
// query before it stops it itself. Under dumpEstimateTimeout, so what comes
// back is the server's own error and not a connection closed under a query
// that goes on running. (When connecting and the session's settings take more
// than the difference, the wait ends first and the query runs on for at most
// this long: still bounded.)
const dumpEstimateServerLimit = 12 * time.Second

// dumpCompressedNamed is how many compressed tables a note names: the largest
// ones, which are the ones the sizes are most wrong about.
const dumpCompressedNamed = 2

// dumpEstimateTimeout bounds the information_schema read. It runs before every
// full read, and a source that does not answer in time must cost a note, not
// the backup.
const dumpEstimateTimeout = 15 * time.Second

// dumpEstimate is what the source reports about the tables a dump selects.
type dumpEstimate struct {
	bytes int64 // DATA_LENGTH + INDEX_LENGTH, summed: what the warnings read
	// dataBytes is DATA_LENGTH alone, summed: what the refusal reads, since
	// a dump holds no secondary indexes.
	dataBytes int64
	tables    int
	// compressed counts tables whose reported size is their compressed size;
	// compressedTop names the largest dumpCompressedNamed of them.
	compressed    int
	compressedTop []string
	// unsized counts tables the server reported no size for (NULL). They add
	// nothing to bytes, so the note says the estimate is short by them.
	unsized int
	// stale: the source refused to serve current sizes (not the 1193 of a
	// server that has no cache), so they may be up to a day old.
	stale bool
}

// dumpSizeEstimateFn reads the estimate from the source; a test replaces it.
var dumpSizeEstimateFn = estimateDumpSize

// sameFilesystemFn reports whether two existing paths are on one filesystem;
// a test replaces it.
var sameFilesystemFn = sameFilesystem

// dumpSizeQuery is the estimate's query over the same table selection as
// dumpableTableCountQuery, which is the selection buildConsoleMydumperArgs
// asks mydumper for. One row per table, summed here: the source walks every
// table for a SUM anyway, a second query for the compressed ones would walk
// them again, and GROUP_CONCAT cuts a list of names at 1024 bytes without a
// word.
func dumpSizeQuery(schemas []string) (string, []any) {
	where, args := dumpableTablesWhere(schemas)
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
	est dumpEstimate
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
	s.est.tables++
	if !r.data.Valid || !r.index.Valid {
		s.est.unsized++
	}
	data, index := max(r.data.Int64, 0), max(r.index.Int64, 0)
	s.est.dataBytes += data
	s.est.bytes += data + index
	if !r.storedCompressed() {
		return
	}
	s.est.compressed++
	n := namedSize{printableName(r.schema) + "." + printableName(r.table), data + index}
	at := len(s.top)
	for at > 0 && n.before(s.top[at-1]) {
		at--
	}
	s.top = slices.Insert(s.top, at, n)
	s.top = s.top[:min(len(s.top), dumpCompressedNamed)]
}

func (s *dumpTableSum) estimate() dumpEstimate {
	est := s.est
	est.compressedTop = nil
	for _, n := range s.top {
		est.compressedTop = append(est.compressedTop, n.name)
	}
	return est
}

// summarizeDumpTables is dumpTableSum over a list.
func summarizeDumpTables(rows []dumpTableRow) dumpEstimate {
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

// compressedTablesSentence is what a verdict says about tables stored
// compressed, "" when there are none. It starts with a space, like the other
// parts of a note's tail.
func compressedTablesSentence(est dumpEstimate) string {
	if est.compressed == 0 {
		return ""
	}
	names := strings.Join(est.compressedTop, ", ")
	if more := est.compressed - len(est.compressedTop); more > 0 {
		names += fmt.Sprintf(" and %d more", more)
	}
	if est.compressed == 1 {
		return fmt.Sprintf(" 1 table uses compressed storage (%s): the server reports its compressed size, and a full read writes it "+
			"uncompressed, so this read can need more than the sizes say.", names)
	}
	return fmt.Sprintf(" %d tables use compressed storage (%s): the server reports their compressed size, and a full read writes them "+
		"uncompressed, so this read can need more than the sizes say.", est.compressed, names)
}

// sessionExecer is the part of a connection prepareEstimateSession uses.
type sessionExecer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// prepareEstimateSession sets up the estimate's own connection, and reports
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
func prepareEstimateSession(ctx context.Context, conn sessionExecer) (stale bool) {
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
	mysqlLimit := fmt.Sprintf("SET SESSION max_execution_time = %d", dumpEstimateServerLimit.Milliseconds())
	_, err := conn.ExecContext(ctx, mysqlLimit)
	switch {
	case err == nil:
	case unknownVariable(err):
		mariaLimit := fmt.Sprintf("SET SESSION max_statement_time = %d", int(dumpEstimateServerLimit.Seconds()))
		if _, err := conn.ExecContext(ctx, mariaLimit); err != nil {
			refused(mariaLimit, err)
		}
	default:
		refused(mysqlLimit, err)
	}
	return stale
}

// estimateDumpSize runs dumpSizeQuery on the source, on a session prepared by
// prepareEstimateSession.
func estimateDumpSize(ctx context.Context, sourceDSN string, ssl config.SSL, schemas []string) (dumpEstimate, error) {
	db, err := connectSource(sourceDSN, ssl)
	if err != nil {
		return dumpEstimate{}, err
	}
	defer db.Close()
	conn, err := db.Conn(ctx)
	if err != nil {
		return dumpEstimate{}, err
	}
	defer conn.Close()
	stale := prepareEstimateSession(ctx, conn)
	q, args := dumpSizeQuery(schemas)
	res, err := conn.QueryContext(ctx, q, args...)
	if err != nil {
		return dumpEstimate{}, err
	}
	defer res.Close()
	var sum dumpTableSum
	for res.Next() {
		var r dumpTableRow
		if err := res.Scan(&r.schema, &r.table, &r.engine, &r.rowFormat, &r.data, &r.index); err != nil {
			return dumpEstimate{}, err
		}
		sum.add(r)
	}
	// A limit that fires mid-scan ends the rows early with an error here; a
	// partial sum must never be returned as the whole.
	if err := res.Err(); err != nil {
		return dumpEstimate{}, err
	}
	est := sum.estimate()
	est.stale = stale
	return est, nil
}

// checkDumpDisk is the preflight execute runs before mydumper. It returns the
// verdict and the line the job's status and history carry, or an error that
// wraps errFoldDiskFull when the staging folder cannot hold the dump. It never
// refuses on a guess: an estimate that could not be read, or a disk that
// cannot be measured, lets the read go ahead and says so.
func (s *baselineSupervisor) checkDumpDisk(req console.BaselineRequest) (check, note string, err error) {
	ctx, cancel := context.WithTimeout(s.ctx, dumpEstimateTimeout)
	defer cancel()
	est, estErr := dumpSizeEstimateFn(ctx, req.SourceDSN, req.SourceSSL, req.Schemas)
	return dumpDiskVerdict(s.stagingDir, req.LocalDir, est, estErr)
}

// dumpDiskVerdict decides from the estimate and the free space where each
// part is written. The dump always goes to stagingDir. The Parquet goes to
// localDir, or to stagingDir too when the destination is S3 only (localDir
// empty). When both land on one filesystem the peak is the dump plus the
// Parquet; when they do not, the staging folder only has to hold the dump,
// and the other disk gets a warning at most, since its share was measured
// below the estimate in every case.
func dumpDiskVerdict(stagingDir, localDir string, est dumpEstimate, estErr error) (check, note string, err error) {
	if estErr != nil {
		return dumpDiskUnchecked, "Disk check did not run: the table sizes could not be read from the source (" +
			firstLineOf(estErr.Error()) + "). The full read went ahead.", nil
	}
	if est.tables == 0 {
		// Not "needs 0 B": nothing matched, which is its own fact. mydumper
		// runs with the same user and selection, so it has nothing to write.
		return dumpDiskOK, "Disk check: no tables match what this read dumps, so there was nothing to size.", nil
	}
	stageFree, ok, why := measureFree(stagingDir)
	if !ok {
		return dumpDiskUnchecked, "Disk check did not run: the free space at " + stagingDir + " could not be measured (" +
			why + "). The full read went ahead.", nil
	}
	need := uint64(max(est.bytes, 0))
	peak := need * dumpPeakTenths / 10
	data := uint64(max(est.dataBytes, 0))
	floor := data * dumpRefuseTenths / 10
	shared, unknown := localDir == "", false
	outDir := stagingDir
	if !shared {
		outDir = existingParent(localDir)
		same, serr := sameFilesystemFn(existingParent(stagingDir), outDir)
		if serr != nil {
			// Unknown counts as shared: it asks for the larger margin, and a
			// warning too many is cheaper than a full disk nobody saw coming.
			// The snapshot folder is still measured below on its own.
			slog.Warn("full read disk check: could not tell whether the staging folder and the snapshot folder share a disk; checking for the larger margin",
				"staging", stagingDir, "snapshots", localDir, "error", serr)
			unknown = true
		}
		shared = same || unknown
	}
	short := ""
	if est.unsized > 0 {
		short += fmt.Sprintf(" The source gave no size for %d table(s), so the real need is higher.", est.unsized)
	}
	if est.stale {
		short += " The source may have reported sizes up to a day old."
	}
	short += compressedTablesSentence(est)
	// The sizes bound the dump from above only when every table reports its
	// real size.
	bound := ", an upper bound (secondary indexes are not dumped, deleted rows still count),"
	if est.compressed > 0 {
		bound = ""
	}

	if stageFree < floor {
		// Two numbers, and what did not happen. Why the line is where it is
		// belongs to the comment above, not to the reader of a refusal.
		dataS, freeS := sizePair(data, stageFree)
		return "", "", fmt.Errorf("%w: the full read was not started. The tables to read take up about %s on the database server, "+
			"not counting indexes, and the working folder %s has %s free.%s Nothing was read from your database, and earlier snapshots are unchanged. %s",
			errFoldDiskFull, dataS, stagingDir, freeS, short, moveStagingHint)
	}
	if stageFree < need {
		// Below the bound, above the refusal line: the dump may fit, may not.
		// Loud, and with everything a refusal would say.
		return dumpDiskLow, fmt.Sprintf("Low disk: a full read writes the whole dump to %s before converting it. The tables add up to about %s%s "+
			"and the folder has %s free, so the dump may not fit and this read may fail with a full disk.%s %s",
			stagingDir, humanSize(int64(need)), bound, humanSize(int64(stageFree)), short, moveStagingHint), nil
	}
	if !shared || unknown {
		outFree, ok, why := measureFree(outDir)
		if !ok {
			return dumpDiskUnchecked, fmt.Sprintf("Disk check did not finish: the dump needs about %s free at %s, which has %s. "+
				"The free space at %s, where the Parquet copy goes, could not be measured (%s). The full read went ahead.%s",
				humanSize(int64(need)), stagingDir, humanSize(int64(stageFree)), localDir, why, short), nil
		}
		if outFree < need {
			// "Up to" and "fits" are claims about a bound, which the sizes
			// are not with tables stored compressed.
			copyTakes, dumpFits := "the copy can take up to about", "The dump itself fits at"
			if est.compressed > 0 {
				copyTakes, dumpFits = "by the sizes the server reports the copy takes about", "By those sizes the dump fits at"
			}
			return dumpDiskLow, fmt.Sprintf("Low disk: %s, where the Parquet copy goes, has %s free, and %s %s. "+
				"This read may fail with a full disk. %s %s (%s free).%s",
				localDir, humanSize(int64(outFree)), copyTakes, humanSize(int64(need)), dumpFits, stagingDir, humanSize(int64(stageFree)), short), nil
		}
		if !shared {
			return roomVerdict(est, fmt.Sprintf("Disk check: the dump needs about %s free at %s; %s free.%s",
				humanSize(int64(need)), stagingDir, humanSize(int64(stageFree)), short))
		}
	}
	where := "on the same disk"
	if unknown {
		where = "possibly on the same disk"
		short = " DBTrail could not tell whether the Parquet copy goes to this disk, so it counted it." + short
	}
	if stageFree < peak {
		return dumpDiskLow, fmt.Sprintf("Low disk: %s has %s free. The dump needs about %s, and with its Parquet copy "+
			"%s it can reach about %s for data that does not compress. This read may fail with a full disk.%s %s",
			stagingDir, humanSize(int64(stageFree)), humanSize(int64(need)), where, humanSize(int64(peak)), short, moveStagingHint), nil
	}
	return roomVerdict(est, fmt.Sprintf("Disk check: a full read needs about %s free at %s (up to %s with the Parquet copy); %s free.%s",
		humanSize(int64(need)), stagingDir, humanSize(int64(peak)), humanSize(int64(stageFree)), short))
}

// roomVerdict is the verdict and the note when the free space covers what the
// sizes ask for; note opens with "Disk check: ". With tables stored
// compressed the sizes ask for too little, so the check cannot say there is
// room: it says it cannot tell, in the note's first words, and the page shows
// that as a note and not as an alarm.
func roomVerdict(est dumpEstimate, note string) (string, string, error) {
	if est.compressed > 0 {
		return dumpDiskUnchecked, "Disk check cannot tell whether this read fits. By the sizes the server reports: " +
			strings.TrimPrefix(note, "Disk check: "), nil
	}
	return dumpDiskOK, note, nil
}

// moveStagingHint is how an operator gives the staging folder more room.
const moveStagingHint = "Free space there, or move this folder to a bigger disk " +
	"(the \"Working folder\" setting, or BINTRAIL_CONSOLE_BASELINE_STAGING) and restart DBTrail."

// measureFree is diskSpaceFn with the fold check's reading of it: an error,
// or a filesystem that reports no size, is "cannot tell", never "full".
func measureFree(dir string) (free uint64, ok bool, why string) {
	free, total, err := diskSpaceFn(existingParent(dir))
	switch {
	case err != nil:
		return 0, false, firstLineOf(err.Error())
	case total == 0:
		return 0, false, "the filesystem reports no size"
	}
	return free, true, ""
}

// sizePair renders two sizes, in bytes when they would otherwise print alike,
// so a refusal never reads "needs 1.0 GiB, has 1.0 GiB".
func sizePair(a, b uint64) (string, string) {
	as, bs := humanSize(int64(a)), humanSize(int64(b))
	if as == bs {
		return fmt.Sprintf("%d bytes", a), fmt.Sprintf("%d bytes", b)
	}
	return as, bs
}

func firstLineOf(s string) string {
	s = strings.TrimSpace(s)
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return strings.TrimSpace(s[:i])
	}
	return s
}

// noteDumpDisk puts the check's verdict on the running job's status, which
// this run owns until it publishes (Trigger wrote it; nothing else takes the
// slot while it reads "running").
func (s *baselineSupervisor) noteDumpDisk(serverID, check, note string) {
	if check == dumpDiskUnchecked || check == dumpDiskLow {
		slog.Warn("full read: "+note, "id", serverID)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if st := s.jobs[serverID]; st != nil && st.State == "running" {
		st.DiskCheck, st.DiskNote = check, note
	}
}

// dumpDiskOf reads back what noteDumpDisk wrote, for the run's history
// record: from own once the run has published (a later job may hold the map
// entry by then), from the map entry before.
func (s *baselineSupervisor) dumpDiskOf(serverID string, own *console.BaselineStatus) (check, note string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := own
	if st == nil {
		st = s.jobs[serverID]
	}
	if st == nil {
		return "", ""
	}
	return st.DiskCheck, st.DiskNote
}

// transportNoteKey carries, in a dump's context, where runMydumper reports a
// read made without encryption (#1996): a side channel, so the many callers
// and fakes of runMydumper keep their signature.
type transportNoteKey struct{}

func withTransportNote(ctx context.Context, note func(string)) context.Context {
	return context.WithValue(ctx, transportNoteKey{}, note)
}

// reportTransportNote hands note to the dump's run, when one listens.
func reportTransportNote(ctx context.Context, note string) {
	if f, ok := ctx.Value(transportNoteKey{}).(func(string)); ok {
		f(note)
	}
}

// noteDumpTransport records on the running job that its read reached the
// source without encryption, as noteDumpDisk records the disk check.
func (s *baselineSupervisor) noteDumpTransport(serverID, note string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if st := s.jobs[serverID]; st != nil && st.State == "running" {
		st.TransportNote = note
	}
}

// dumpTransportOf reads back what noteDumpTransport wrote, as dumpDiskOf.
func (s *baselineSupervisor) dumpTransportOf(serverID string, own *console.BaselineStatus) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := own
	if st == nil {
		st = s.jobs[serverID]
	}
	if st == nil {
		return ""
	}
	return st.TransportNote
}
