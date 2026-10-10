package consoleapp

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/doctor"
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
	// dumpDiskTight is what a "low" verdict becomes once the read it warned
	// about has written its snapshot: the warning was about this read, the
	// read fit, and what is left to say is about the next one. A separate
	// value, not a word in the note, so the page never calls a read that
	// worked a failure.
	dumpDiskTight = "tight"
)

// The risk a "low" note states while the read has not written its snapshot,
// and what each becomes once it has. dumpDiskVerdict builds its notes from
// the first of each pair and dumpDiskOnceItFit swaps in the second, so the
// two cannot drift apart: a note in the future tense over a snapshot that
// exists ("this read may fail") is a sentence that is false as shown.
const (
	diskRiskDump    = "so the dump may not fit and this read may fail with a full disk."
	diskRiskDumpFit = "so the dump was not sure to fit. This read fit. The next one may not."
	diskRisk        = "This read may fail with a full disk."
	diskRiskFit     = "This read fit. The next one may not."
)

// dumpDiskOnceItFit is the check and note of a full read whose snapshot is
// written. Only a "low" verdict changes, and only when its note carries a
// risk sentence this build wrote: anything else (another verdict, a note in
// an older wording) is returned as it came, since "it fit" must not be said
// over a sentence that was not read.
func dumpDiskOnceItFit(check, note string) (string, string) {
	if check != dumpDiskLow {
		return check, note
	}
	for _, p := range [][2]string{{diskRiskDump, diskRiskDumpFit}, {diskRisk, diskRiskFit}} {
		if strings.Count(note, p[0]) == 1 {
			return dumpDiskTight, strings.Replace(note, p[0], p[1], 1)
		}
	}
	return check, note
}

// The estimate itself (the query, the sum, the session it runs on and the
// two ratios) lives in internal/doctor, where `bintrail doctor` reads the
// same numbers (#2259). What follows is this process's use of it: where each
// part of a full read is written, and what the job says about it.
type dumpEstimate = doctor.DumpEstimate

const (
	dumpPeakTenths      = doctor.DumpPeakTenths
	dumpRefuseTenths    = doctor.DumpRefuseTenths
	dumpEstimateTimeout = doctor.DumpEstimateTimeout
)

// dumpSizeEstimateFn reads the estimate from the source; a test replaces it.
var dumpSizeEstimateFn = estimateDumpSize

// sameFilesystemFn reports whether two existing paths are on one filesystem;
// a test replaces it.
var sameFilesystemFn = sameFilesystem

// estimateDumpSize reads the estimate over a connection opened with this
// process's own TLS settings for the source.
func estimateDumpSize(ctx context.Context, sourceDSN string, ssl config.SSL, schemas []string) (dumpEstimate, error) {
	db, err := connectSource(sourceDSN, ssl)
	if err != nil {
		return dumpEstimate{}, err
	}
	defer db.Close()
	return doctor.ReadDumpEstimate(ctx, db, schemas)
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
	if est.Tables == 0 {
		// Not "needs 0 B": nothing matched, which is its own fact. mydumper
		// runs with the same user and selection, so it has nothing to write.
		return dumpDiskOK, "Disk check: no tables match what this read dumps, so there was nothing to size.", nil
	}
	stageFree, ok, why := measureFree(stagingDir)
	if !ok {
		return dumpDiskUnchecked, "Disk check did not run: the free space at " + stagingDir + " could not be measured (" +
			why + "). The full read went ahead.", nil
	}
	need := uint64(max(est.Bytes, 0))
	peak := need * dumpPeakTenths / 10
	data := uint64(max(est.DataBytes, 0))
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
	if est.Unsized > 0 {
		short += fmt.Sprintf(" The source gave no size for %d table(s), so the real need is higher.", est.Unsized)
	}
	if est.Stale {
		short += " The source may have reported sizes up to a day old."
	}
	short += doctor.DumpCompressedSentence(est)
	// The sizes bound the dump from above only when every table reports its
	// real size.
	bound := ", an upper bound (secondary indexes are not dumped, deleted rows still count),"
	if est.Compressed > 0 {
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
			"and the folder has %s free, %s%s %s",
			stagingDir, humanSize(int64(need)), bound, humanSize(int64(stageFree)), diskRiskDump, short, moveStagingHint), nil
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
			if est.Compressed > 0 {
				copyTakes, dumpFits = "by the sizes the server reports the copy takes about", "By those sizes the dump fits at"
			}
			return dumpDiskLow, fmt.Sprintf("Low disk: %s, where the Parquet copy goes, has %s free, and %s %s. "+
				"%s %s %s (%s free).%s",
				localDir, humanSize(int64(outFree)), copyTakes, humanSize(int64(need)), diskRisk, dumpFits, stagingDir, humanSize(int64(stageFree)), short), nil
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
			"%s it can reach about %s for data that does not compress. %s%s %s",
			stagingDir, humanSize(int64(stageFree)), humanSize(int64(need)), where, humanSize(int64(peak)), diskRisk, short, moveStagingHint), nil
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
	if est.Compressed > 0 {
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
