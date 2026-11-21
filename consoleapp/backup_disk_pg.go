package consoleapp

import (
	"context"
	"fmt"
	"strings"

	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/pgbaseline"
)

// The disk check before a PostgreSQL snapshot (#3029): the counterpart of
// checkDumpDisk, which a MySQL full read runs before mydumper.
//
// A PostgreSQL snapshot has no dump. It copies each table straight into a
// Parquet file in the server's local snapshot folder, so that folder is the
// one place that fills, and what has to fit there is the Parquet alone.
//
// What MySQL's check learned, and how it holds here:
//
//   - It never refuses on a guess. For MySQL the refusal reads the tables'
//     data and waits for less than half of it free, a line no measured dump
//     reached. A Parquet copy has no such line. Measured on PostgreSQL 16
//     with zstd (issue #3029), the copy over pg_table_size was 0.007x for a
//     table of equal rows, 0.2x for ordinary order rows and for long text
//     PostgreSQL had already compressed, 0.3x for a table with half its rows
//     deleted, 0.65x to 0.7x for random UUIDs and hex text, and 1.22x for
//     random bytea, which is written as hex. So no fraction of the tables'
//     size is a line every real copy stays above, and a refusal by size
//     would turn away snapshots that fit, with no way past it. MySQL's check
//     reached the same rule for the folder its Parquet goes to: a small one
//     is a warning, never a refusal.
//   - What does refuse is the floor the snapshot updates already keep
//     (foldDiskMargin): with less than that free in the snapshot folder no
//     update of the snapshot would be written afterwards, whatever the
//     tables' size, so the first copy is not started into it either.
//   - The warning reads the highest ratio measured, as MySQL's does:
//     pgCopyPeakTenths. It warns too early for most data, never too late for
//     any that was measured.
//   - A size that could not be read, or a disk that cannot be measured, lets
//     the snapshot go ahead and says so.
//   - Tables whose reported size bounds nothing (another storage than the
//     heap, or no size at all) are counted, and "there is room" becomes "the
//     check cannot vouch for it".
//   - Once the snapshot is written, a warning turns into a note about the
//     next one (dumpDiskOnceItFit), so its risk sentence is diskRisk, once.

// pgCopyPeakTenths is the snapshot's size as tenths of pg_table_size at the
// highest ratio the #3029 measurement saw: 1.22x, random bytea (149 MB of
// Parquet over 122 MB of table), rounded up to the next tenth.
const pgCopyPeakTenths = 13

// pgCopyEstimateFn reads the estimate from the source; a test replaces it.
var pgCopyEstimateFn = pgbaseline.ReadCopyEstimate

// moveLocalFolderHint is how an operator gives the snapshot folder more room.
const moveLocalFolderHint = "Free space there, or give this server a Local folder on a bigger disk (" + console.PageSnapshots + " page, under Where and how often)."

// checkPGCopyDisk is the preflight executePG runs before the snapshot. It
// returns the verdict and the line the job's status and history carry, or an
// error that wraps errFoldDiskFull when the snapshot folder is below the
// floor.
func (s *baselineSupervisor) checkPGCopyDisk(req console.BaselineRequest) (check, note string, err error) {
	ctx, cancel := context.WithTimeout(s.ctx, pgbaseline.CopyEstimateTimeout)
	defer cancel()
	// The same narrowing pgBaselineConfig gives the snapshot.
	filters := cliutil.BuildIndexFilters(strings.Join(req.Schemas, ","), "")
	est, estErr := pgCopyEstimateFn(ctx, req.SourceDSN, req.Publication, filters)
	return pgCopyDiskVerdict(req.LocalDir, est, estErr)
}

// pgCopyDiskVerdict decides from the estimate and the free space of the
// snapshot folder. Pure but for the measurement (diskSpaceFn).
func pgCopyDiskVerdict(localDir string, est pgbaseline.CopyEstimate, estErr error) (check, note string, err error) {
	if estErr == nil && est.Tables == 0 {
		// Not "needs 0 B": nothing matched, which is its own fact, and the
		// snapshot refuses an empty publication in its own words.
		return dumpDiskOK, "Disk check: the publication holds no table this snapshot copies, so there was nothing to size.", nil
	}
	free, ok, why := measureFree(localDir)
	if !ok {
		return dumpDiskUnchecked, "Disk check did not run: the free space at " + localDir + " could not be measured (" +
			why + "). The snapshot went ahead.", nil
	}
	need := uint64(max(est.Bytes, 0))
	if free < foldDiskMargin {
		sized := ""
		if estErr == nil && need > 0 {
			sized = fmt.Sprintf(" The tables to copy take up about %s on the database server.", humanSize(int64(need)))
		}
		freeS, floorS := sizePair(free, foldDiskMargin)
		// What happened and both numbers come first: a screen that shows a
		// failed snapshot cuts a long first line.
		return "", "", fmt.Errorf("%w: it was not started, and nothing was read from your database. The snapshot folder %s has %s free, "+
			"and DBTrail keeps %s free there.%s %s Earlier snapshots are unchanged. With less than that free no update of a snapshot is written either.",
			errFoldDiskFull, localDir, freeS, floorS, sized, moveLocalFolderHint)
	}
	if estErr != nil {
		return dumpDiskUnchecked, fmt.Sprintf("Disk check did not run: the table sizes could not be read from the source (%s). "+
			"%s free at %s. The snapshot went ahead.", firstLineOf(estErr.Error()), humanSize(int64(free)), localDir), nil
	}
	if need == 0 && est.Unsized > 0 {
		// Nothing was sized, so there is no sum to compare: "needs about 0 B"
		// would read as a pass.
		return dumpDiskUnchecked, fmt.Sprintf("Disk check cannot tell whether this snapshot fits: the source gave no size for any of the %d table(s) to copy. "+
			"%s free at %s. The snapshot went ahead.", est.Unsized, humanSize(int64(free)), localDir), nil
	}
	short := ""
	if est.Unsized > 0 {
		short += fmt.Sprintf(" The source gave no size for %d table(s), so the real need is higher.", est.Unsized)
	}
	short += pgbaseline.CopyOtherStorageSentence(est)
	peak := need * pgCopyPeakTenths / 10
	if free < peak {
		return dumpDiskLow, fmt.Sprintf("Low disk: the snapshot folder %s has %s free. The tables to copy take up about %s on the database server, "+
			"and their snapshot can reach about %s for binary data, which does not compress; for most data it is much smaller. %s%s %s",
			localDir, humanSize(int64(free)), humanSize(int64(need)), humanSize(int64(peak)), diskRisk, short, moveLocalFolderHint), nil
	}
	room := fmt.Sprintf("the tables to copy take up about %s on the database server, and their snapshot at most about %s; %s free at %s.%s",
		humanSize(int64(need)), humanSize(int64(peak)), humanSize(int64(free)), localDir, short)
	if est.Unsized > 0 || est.OtherStorage > 0 {
		// A size that is missing, or compressed, asks for too little: room
		// for the sum is not room for the snapshot.
		return dumpDiskUnchecked, "Disk check cannot tell whether this snapshot fits. By the sizes the server reports: " + room, nil
	}
	return dumpDiskOK, "Disk check: " + room, nil
}
