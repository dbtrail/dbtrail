package consoleapp

import (
	"context"
	"errors"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
)

// unusedDefaultBuildAge is how long nothing in a .sql build must have been
// written for the build to count as dead where this daemon is not the one
// that owns the folder. A build is written to while it is made, and its own
// daemon removes it sqlExportTTL after it finishes unless a download is still
// reading it. Six times that with no write leaves room for a slow download;
// one that outlasts it is cut off, which its reader sees as a failed
// download and not as a short file.
const unusedDefaultBuildAge = 6 * sqlExportTTL

// unusedDefaultSweepEvery is how often a running daemon looks again. At
// startup alone the sweep would find almost nothing: what an upgrade leaves
// in the old default is a build the restart interrupted or one still in its
// download window, both younger than unusedDefaultBuildAge, and a `watch`
// daemon can go months without another start.
const unusedDefaultSweepEvery = time.Hour

// sweepUnusedDefaultFolders removes the dead .sql builds under the default
// working folders this daemon is NOT using (#2255).
//
// The working folder has two defaults: beside DBTrail's data, and under the
// system temp folder. A daemon that was given no folder uses one of them, and
// which one can change between two starts: an upgrade across #2255 moves it
// out of the temp folder, a data folder that stops being writable moves it
// back. The startup sweep (sweepSQLExportStaging) reads only the folder in
// use, so builds a previous process left in the other one would stay on disk
// with nothing ever looking at them. Each is a full plaintext copy of a
// database.
//
// It is far more careful than that sweep, because the folder is not this
// daemon's alone: the temp one is shared by every DBTrail on the host that
// was given no folder, older versions included, and the system temp folder
// is one any user can create entries in.
//
//   - Only .sql builds, and only by age. They carry no journal and no lock,
//     so "nothing in it was written for a day" is the only proof there is.
//   - Only inside folders that belong to this process's user and that no
//     other user can write into, from the working folder down to the build
//     (ownFolder), and only when no other user can rename the working folder
//     itself (heldInPlace). Without that, an entry could be swapped for a
//     link between the check and the removal.
//   - Never the dump, staged-snapshot or staged-run folders. Those of a dead
//     job this install journaled are reclaimed through the journal, which
//     stores absolute paths, when a snapshot feature is on; any other is
//     another process's work or cannot be told from it.
//   - Never the folder itself.
//   - Nothing at all when a folder was set: the defaults are then not this
//     daemon's to clear, and were not before either.
func sweepUnusedDefaultFolders(reg *console.Registry, current string, now time.Time) {
	if effectiveStagingDir(reg, upBaselineStageDir) != "" {
		return
	}
	for _, dir := range defaultWorkingFolders(reg.Path()) {
		if sameFolder(dir, current) {
			continue
		}
		sweepDeadSQLBuilds(dir, now)
	}
}

// runUnusedDefaultSweep repeats sweepUnusedDefaultFolders until the daemon
// stops. Each pass is guarded: this process is also the capture plane, and a
// panic in housekeeping must not stop it.
func runUnusedDefaultSweep(ctx context.Context, reg *console.Registry, every time.Duration) {
	if every <= 0 {
		every = unusedDefaultSweepEvery
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			sweepUnusedDefaultFoldersGuarded(reg)
		}
	}
}

func sweepUnusedDefaultFoldersGuarded(reg *console.Registry) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("working folder: clearing old .sql builds hit an internal error and skipped a pass. Capture and the "+
				"web interface keep running. Please report this with the stack recorded here.",
				"panic", r, "stack", string(debug.Stack()))
		}
	}()
	sweepUnusedDefaultFolders(reg, baselineStagingDirFor(reg), time.Now())
}

// sweepWorkingFoldersAtBoot is the startup sweep of .sql builds: everything a
// previous process left in the folder in use, and the dead ones in the
// default folders this daemon is not using.
func sweepWorkingFoldersAtBoot(reg *console.Registry, now time.Time) {
	current := baselineStagingDirFor(reg)
	sweepSQLExportStaging(current)
	sweepUnusedDefaultFolders(reg, current, now)
}

// defaultWorkingFolders are the folders defaultWorkingFolder can answer.
func defaultWorkingFolders(serversPath string) []string {
	out := []string{tempWorkingFolder()}
	if strings.TrimSpace(serversPath) == "" {
		return out
	}
	if abs, err := filepath.Abs(serversPath); err == nil {
		out = append(out, filepath.Join(filepath.Dir(abs), workingFolderName))
	}
	return out
}

// sameFolder reports whether two paths are one folder, by any spelling. When
// it cannot tell, the answer is yes: the caller skips a folder that may be
// the one in use.
func sameFolder(a, b string) bool {
	if filepath.Clean(a) == filepath.Clean(b) {
		return true
	}
	fa, errA := os.Stat(a)
	fb, errB := os.Stat(b)
	switch {
	case errA == nil && errB == nil:
		return os.SameFile(fa, fb)
	case errors.Is(errA, fs.ErrNotExist) || errors.Is(errB, fs.ErrNotExist):
		// One of them is not there, so they are not one folder.
		return false
	}
	return true
}

// unusedSweepSaid remembers what was already said about a folder that is
// not swept, so an hourly pass says it once and not every hour.
var unusedSweepSaid sync.Map

func sayOnce(key, msg string, args ...any) {
	if _, said := unusedSweepSaid.LoadOrStore(key, true); !said {
		slog.Warn(msg, args...)
	}
}

func sweepDeadSQLBuilds(stagingDir string, now time.Time) {
	base := filepath.Join(stagingDir, "sql-export")
	if _, err := os.Lstat(base); errors.Is(err, fs.ErrNotExist) {
		return
	}
	const kept = "working folder: old .sql builds may be left in a default folder DBTrail is not using here, and it will not remove them. Each is a full copy of a database: check the folder and remove it by hand"
	if why := heldInPlace(stagingDir); why != "" {
		sayOnce(base, kept, "dir", base, "why", why)
		return
	}
	for _, d := range []string{stagingDir, base} {
		if why := ownFolder(d); why != "" {
			sayOnce(base, kept, "dir", base, "why", d+": "+why)
			return
		}
	}
	servers, err := os.ReadDir(base)
	if err != nil {
		sayOnce(base, kept, "dir", base, "why", firstLineOf(err.Error()))
		return
	}
	cutoff := now.Add(-unusedDefaultBuildAge)
	var builds, unsized int
	var freed int64
	for _, srv := range servers {
		srvDir := filepath.Join(base, srv.Name())
		if why := ownFolder(srvDir); why != "" {
			sayOnce(srvDir, kept, "dir", srvDir, "why", why)
			continue
		}
		runs, err := os.ReadDir(srvDir)
		if err != nil {
			sayOnce(srvDir, kept, "dir", srvDir, "why", firstLineOf(err.Error()))
			continue
		}
		for _, run := range runs {
			dir := filepath.Join(srvDir, run.Name())
			if why := ownFolder(dir); why != "" {
				sayOnce(dir, kept, "dir", dir, "why", why)
				continue
			}
			untouched, err := untouchedSince(dir, cutoff)
			if err != nil {
				sayOnce(dir, kept, "dir", dir, "why", "it cannot be read through: "+firstLineOf(err.Error()))
				continue
			}
			if !untouched {
				continue
			}
			n, sized, err := removeStagedBuild(base, dir)
			if err != nil {
				slog.Warn("working folder: could not remove an old .sql build from a default folder DBTrail is not using here", "error", err)
				continue
			}
			builds++
			if sized {
				freed += n
			} else {
				unsized++
			}
		}
		// Only an empty folder goes.
		_ = os.Remove(srvDir)
	}
	_ = os.Remove(base)
	if builds > 0 {
		slog.Info("working folder: removed old .sql builds from a default folder DBTrail is not using here",
			"dir", stagingDir, "builds", builds, "bytes", freed, "builds_of_unknown_size", unsized)
	}
}

// untouchedSince reports whether nothing under dir, dir included, was
// modified after cutoff. An error means it could not read all of it, which
// is not proof that a build is dead.
func untouchedSince(dir string, cutoff time.Time) (bool, error) {
	untouched := true
	err := filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		if info.ModTime().After(cutoff) {
			untouched = false
			return fs.SkipAll
		}
		return nil
	})
	return untouched, err
}
