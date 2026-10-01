package consoleapp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

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

// Values of BaselineStatus.DiskCheck.
const (
	dumpDiskOK        = "ok"        // room for the peak
	dumpDiskLow       = "low"       // room for the dump, maybe not for dump plus Parquet
	dumpDiskUnchecked = "unchecked" // the check could not run; the read went ahead
)

// dumpPeakTenths is the dump-plus-Parquet peak as tenths of the estimate:
// 18 = 1.8x, the highest ratio the #1938 measurement saw against
// DATA_LENGTH + INDEX_LENGTH (random binary: 94 MiB dump + 84 MiB Parquet
// over 99 MiB).
const dumpPeakTenths = 18

// dumpRefuseTenths is the refusal line as tenths of the estimate: below half
// of it no measured dump fit, so refusing there is not a guess.
const dumpRefuseTenths = 5

// dumpEstimateTimeout bounds the information_schema read. It runs before every
// full read, and a source that does not answer in time must cost a note, not
// the backup.
const dumpEstimateTimeout = 15 * time.Second

// dumpEstimate is what the source reports about the tables a dump selects.
type dumpEstimate struct {
	bytes  int64 // DATA_LENGTH + INDEX_LENGTH, summed
	tables int
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
// asks mydumper for. COALESCE twice: SUM over no rows is NULL, which would
// read as "the check failed" on an empty schema, and a NULL size would drop
// its table from the sum without a word.
func dumpSizeQuery(schemas []string) (string, []any) {
	where, args := dumpableTablesWhere(schemas)
	return "SELECT COUNT(*), " +
		"COALESCE(SUM(COALESCE(DATA_LENGTH, 0) + COALESCE(INDEX_LENGTH, 0)), 0), " +
		"COALESCE(SUM(DATA_LENGTH IS NULL OR INDEX_LENGTH IS NULL), 0) " +
		"FROM information_schema.TABLES WHERE " + where, args
}

// estimateDumpSize runs dumpSizeQuery on the source. MySQL 8.0 and newer
// serve information_schema sizes from a cache that can be a day old
// (information_schema_stats_expiry), which undercounts a table that grew; the
// session asks for fresh values first. A server without the variable
// (MariaDB, 5.7) refuses the SET with 1193, and its sizes are current anyway.
// Any other refusal (a proxy that rejects the SET, say) leaves the day-old
// cache in play, and the estimate says so.
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
	var est dumpEstimate
	// ctx bounds how long this waits, not how long the source works: on a
	// timeout the driver closes the connection without KILL QUERY, so an
	// information_schema scan over many tables can keep running on the source
	// for a while after the read has moved on.
	if _, err := conn.ExecContext(ctx, "SET SESSION information_schema_stats_expiry = 0"); err != nil {
		var me *mysqldriver.MySQLError
		if errors.As(err, &me) && me.Number == 1193 {
			slog.Debug("full read disk check: the source has no size cache to turn off", "error", err)
		} else {
			slog.Warn("full read disk check: could not ask the source for current table sizes; it may answer with sizes up to a day old", "error", err)
			est.stale = true
		}
	}
	q, args := dumpSizeQuery(schemas)
	if err := conn.QueryRowContext(ctx, q, args...).Scan(&est.tables, &est.bytes, &est.unsized); err != nil {
		return dumpEstimate{}, err
	}
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
	floor := need * dumpRefuseTenths / 10
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

	if stageFree < floor {
		freeS, floorS := sizePair(stageFree, floor)
		return "", "", fmt.Errorf("%w: a full read writes the whole dump to %s before converting it. The tables add up to about %s, "+
			"an upper bound (secondary indexes are not dumped, deleted rows still count), and no measured dump came to less than "+
			"half of it, %s; the folder has %s free.%s Nothing was dumped from the source. %s",
			errFoldDiskFull, stagingDir, humanSize(int64(need)), floorS, freeS, short, moveStagingHint)
	}
	if stageFree < need {
		// Below the bound, above the refusal line: the dump may fit, may not.
		// Loud, and with everything a refusal would say.
		return dumpDiskLow, fmt.Sprintf("Low disk: a full read writes the whole dump to %s before converting it. The tables add up to about %s, "+
			"an upper bound (secondary indexes are not dumped, deleted rows still count), and the folder has %s free, so the dump may not fit "+
			"and this read may fail with a full disk.%s %s",
			stagingDir, humanSize(int64(need)), humanSize(int64(stageFree)), short, moveStagingHint), nil
	}
	if !shared || unknown {
		outFree, ok, why := measureFree(outDir)
		if !ok {
			return dumpDiskUnchecked, fmt.Sprintf("Disk check did not finish: the dump needs about %s free at %s, which has %s. "+
				"The free space at %s, where the Parquet copy goes, could not be measured (%s). The full read went ahead.%s",
				humanSize(int64(need)), stagingDir, humanSize(int64(stageFree)), localDir, why, short), nil
		}
		if outFree < need {
			return dumpDiskLow, fmt.Sprintf("Low disk: %s, where the Parquet copy goes, has %s free, and the copy can take up to about %s. "+
				"This read may fail with a full disk. The dump itself fits at %s (%s free).%s",
				localDir, humanSize(int64(outFree)), humanSize(int64(need)), stagingDir, humanSize(int64(stageFree)), short), nil
		}
		if !shared {
			return dumpDiskOK, fmt.Sprintf("Disk check: the dump needs about %s free at %s; %s free.%s",
				humanSize(int64(need)), stagingDir, humanSize(int64(stageFree)), short), nil
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
	return dumpDiskOK, fmt.Sprintf("Disk check: a full read needs about %s free at %s (up to %s with the Parquet copy); %s free.%s",
		humanSize(int64(need)), stagingDir, humanSize(int64(peak)), humanSize(int64(stageFree)), short), nil
}

// moveStagingHint is how an operator gives the staging folder more room.
const moveStagingHint = "Free space there, or move this folder to a bigger disk " +
	"(the \".sql build folder\" setting, or BINTRAIL_CONSOLE_BASELINE_STAGING) and restart DBTrail."

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
