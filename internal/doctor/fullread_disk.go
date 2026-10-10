package doctor

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/dbtrail/dbtrail/internal/config"
)

// FullReadDiskCheckName is the check `bintrail doctor --staging-dir` adds
// (#2259): the free space of a working folder beside what a full read of the
// source writes there.
const FullReadDiskCheckName = "Disk for a full read"

// fullReadEstimateWaitDefault bounds the size query, like the console's own
// wait for it: the source stops the query itself before this
// (DumpEstimateServerLimit).
const fullReadEstimateWaitDefault = DumpEstimateTimeout

// Seams a test replaces: the free-space probe, the read of the estimate, and
// how long that read may take.
var (
	fullReadDiskSpaceFn  = diskSpace
	fullReadEstimateFn   = readFullReadEstimate
	fullReadEstimateWait = fullReadEstimateWaitDefault
)

func readFullReadEstimate(ctx context.Context, sourceDSN string, schemas []string) (DumpEstimate, error) {
	db, err := config.Connect(sourceDSN)
	if err != nil {
		return DumpEstimate{}, err
	}
	defer db.Close()
	return ReadDumpEstimate(ctx, db, schemas)
}

// CheckFullReadDisk compares the free space at stagingDir with what a full
// read of the source would write there. It is advisory: it warns at most and
// never changes the exit code, because doctor does not know where the
// snapshot is written (a local folder, or S3 only) and may run on another
// machine than the one that reads. With no folder it says where the check
// that does know runs.
//
// The folder is measured on the machine this runs on, and the tables are the
// ones `--schemas` selects, which need not be what a server saved in the web
// interface reads. Every outcome says both.
func CheckFullReadDisk(ctx context.Context, stagingDir, sourceDSN string, schemas []string) CheckResult {
	dir := strings.TrimSpace(stagingDir)
	if dir == "" {
		return CheckResult{Name: FullReadDiskCheckName, Status: StatusSkip,
			Detail: "not checked: pass --staging-dir <folder> to compare its free space with the tables a full read dumps. " +
				"The web interface runs this check on its own Working folder before every full read and shows the result on the Snapshots page"}
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		return CheckResult{Name: FullReadDiskCheckName, Status: StatusSkip,
			Detail: "not checked: " + dir + " could not be made a full path (" + firstLine(err.Error()) + ")"}
	}
	measured, isFile, err := existingFolder(abs)
	switch {
	case isFile != "":
		return CheckResult{Name: FullReadDiskCheckName, Status: StatusWarn,
			Detail:      isFile + " is a file, so " + abs + " cannot be a working folder",
			Remediation: "Pass a folder, or a path that does not exist yet under one."}
	case err != nil:
		return CheckResult{Name: FullReadDiskCheckName, Status: StatusSkip,
			Detail: "not checked: " + abs + " could not be looked at (" + firstLine(err.Error()) + ")"}
	}
	notThere := ""
	if measured != abs {
		notThere = measured
	}
	free, total, err := fullReadDiskSpaceFn(measured)
	switch {
	case err != nil:
		return CheckResult{Name: FullReadDiskCheckName, Status: StatusSkip,
			Detail: "not checked: the free space at " + abs + " could not be measured (" + firstLine(err.Error()) + ")"}
	case total == 0:
		return CheckResult{Name: FullReadDiskCheckName, Status: StatusSkip,
			Detail: "not checked: the free space at " + abs + " could not be measured (the filesystem reports no size)"}
	}
	ectx, cancel := context.WithTimeout(ctx, fullReadEstimateWait)
	defer cancel()
	est, estErr := fullReadEstimateFn(ectx, sourceDSN, schemas)
	return fullReadDiskResult(abs, notThere, free, schemas, est, estErr)
}

// existingFolder returns the nearest folder that exists at or above path,
// which is the one whose filesystem a folder created at path would be on.
// isFile names the first thing on the way up that exists and is not a
// folder: nothing can be created under it.
func existingFolder(path string) (folder, isFile string, err error) {
	for p := path; ; {
		info, serr := os.Stat(p)
		switch {
		case serr == nil && info.IsDir():
			return p, "", nil
		case serr == nil:
			return "", p, nil
		// ENOTDIR is how a path under a file answers; the file is found one
		// step up.
		case !errors.Is(serr, fs.ErrNotExist) && !errors.Is(serr, syscall.ENOTDIR):
			return "", "", serr
		}
		// Something is there that Stat could not follow: a link to a folder
		// that is missing or not mounted. A folder made "at" it would land
		// wherever the link points, not on the parent's disk.
		if li, lerr := os.Lstat(p); lerr == nil && li.Mode()&os.ModeSymlink != 0 {
			return "", "", fmt.Errorf("%s is a link to something that is not there", p)
		}
		parent := filepath.Dir(p)
		if parent == p {
			return "", "", serr
		}
		p = parent
	}
}

// fullReadDiskResult grades free bytes at dir against the estimate, on the
// two lines the console's check uses: under half the tables' data the
// console refuses to start, and under their whole size the dump may not fit.
// Above that the folder holds the dump. Whether it also has to hold the
// Parquet copy (DumpPeakTenths) depends on where the snapshot is written,
// which is not known here, so that is said as a condition and not graded.
//
// With tables stored compressed the sizes are not a bound (#1938), and a
// table the server gave no size for adds nothing to them, so with either
// there is no pass: it cannot tell.
func fullReadDiskResult(dir, measuredAt string, free uint64, schemas []string, est DumpEstimate, estErr error) CheckResult {
	at := dir
	if measuredAt != "" {
		at = dir + " (does not exist yet; measured at " + measuredAt + ")"
	}
	scope := "every schema but the system ones"
	if len(schemas) > 0 {
		scope = "--schemas " + strings.Join(schemas, ",")
	}
	// Whose disk and whose tables: neither is necessarily what a full read
	// started from the web interface uses.
	const here = ", measured on the machine this command runs on"
	freeS := BinarySize(clampSize(free))
	res := CheckResult{Name: FullReadDiskCheckName}

	if estErr != nil {
		why := firstLine(estErr.Error())
		if errors.Is(estErr, context.DeadlineExceeded) {
			why = fmt.Sprintf("the source did not answer within %s", fullReadEstimateWait)
		}
		res.Status = StatusSkip
		res.Detail = fmt.Sprintf("%s free at %s%s; the table sizes could not be read from the source (%s), so it is not compared with what a full read needs",
			freeS, at, here, why)
		return res
	}
	if est.Tables == 0 {
		// Not a pass: this account seeing no table (a missing privilege, a
		// misspelled schema) reads the same as there being none, and doctor
		// need not run as the user that reads.
		res.Status = StatusSkip
		res.Detail = fmt.Sprintf("nothing was sized: this user sees no tables in %s; %s free at %s%s", scope, freeS, at, here)
		return res
	}
	need := uint64(max(est.Bytes, 0))
	data := uint64(max(est.DataBytes, 0))
	floor := data * DumpRefuseTenths / 10
	peak := need * DumpPeakTenths / 10

	tail := ""
	if est.Unsized > 0 {
		tail += fmt.Sprintf(" The source gave no size for %d table(s), so the real need is higher.", est.Unsized)
	}
	if est.Stale {
		tail += " The source may have reported sizes up to a day old."
	}
	tail += DumpCompressedSentence(est)
	fix := "Free space there, or use a folder on a bigger disk: for the web interface that is its Working folder setting " +
		"(or BINTRAIL_CONSOLE_BASELINE_STAGING). Sizing: docs/capacity.md, \"Disk for a full read\"."

	switch {
	case free < floor:
		dataS, fS := binarySizePair(data, free)
		res.Status, res.Remediation = StatusWarn, fix
		res.Detail = fmt.Sprintf("the tables of %s take up about %s on the database server, not counting indexes, and %s has %s free%s: "+
			"the web interface refuses to start a full read with this little room.%s", scope, dataS, at, fS, here, tail)
	case free < need:
		// The sizes bound the dump from above only when every table reports
		// its real size.
		bound := ", an upper bound for the dump (secondary indexes are not dumped, deleted rows still count),"
		if est.Compressed > 0 {
			bound = ""
		}
		needS, fS := binarySizePair(need, free)
		res.Status, res.Remediation = StatusWarn, fix
		res.Detail = fmt.Sprintf("the tables of %s add up to about %s%s and %s has %s free%s, so the dump of a full read may not fit.%s",
			scope, needS, bound, at, fS, here, tail)
	case est.Compressed > 0 || est.Unsized > 0:
		// A size that is compressed, or missing, asks for too little: room
		// for the sum is not room for the read.
		res.Status = StatusSkip
		res.Detail = fmt.Sprintf("cannot tell whether a full read has room: by the sizes the server reports, the tables of %s add up to about %s and %s has %s free%s.%s",
			scope, BinarySize(clampSize(need)), at, freeS, here, tail)
	default:
		res.Status = StatusPass
		res.Detail = fmt.Sprintf("a full read of %s needs about %s at %s; %s free%s.", scope, BinarySize(clampSize(need)), at, freeS, here)
		if free < peak {
			res.Detail += fmt.Sprintf(" If the snapshot is written to this disk too (a server that keeps its snapshots only in S3, or a local folder on it), "+
				"the dump and its Parquet copy together can reach about %s for data that does not compress.", BinarySize(clampSize(peak)))
		}
		res.Detail += tail
	}
	// The report prints a detail inside parentheses.
	res.Detail = strings.TrimSuffix(res.Detail, ".")
	return res
}

// BinarySize renders bytes in binary units with one decimal. A value that
// would round up to 1024.0 moves to the next unit.
func BinarySize(b int64) string {
	b = max(b, 0)
	if b < 1024 {
		return fmt.Sprintf("%d B", b)
	}
	v, exp := float64(b)/1024, 0
	for v >= 1023.95 && exp < 5 {
		v /= 1024
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", v, "KMGTPE"[exp])
}

// clampSize keeps a size some filesystems report as "unlimited" (2^63 bytes
// and more) from printing as a negative number.
func clampSize(b uint64) int64 { return int64(min(b, math.MaxInt64)) }

// binarySizePair renders two sizes, in bytes when they would otherwise print
// alike, so a line never reads "about 10.0 GiB and has 10.0 GiB free".
func binarySizePair(a, b uint64) (string, string) {
	as, bs := BinarySize(clampSize(a)), BinarySize(clampSize(b))
	if as == bs {
		return fmt.Sprintf("%d bytes", a), fmt.Sprintf("%d bytes", b)
	}
	return as, bs
}

func firstLine(s string) string {
	s = strings.TrimSpace(s)
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return strings.TrimSpace(s[:i])
	}
	return s
}
