package consoleapp

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// The update that folds from a read of some tables alone (#2640).
//
// refreshRequest.Reread names the tables. The run reads them (stageReread),
// waits until capture has passed the coordinate of that read, and folds every
// table of the server to one cut, the read tables from the staged files. The
// staged folder is removed when the run ends, however it ends, and a kill is
// covered by the job's journal. Nothing sets the field yet: the schedule
// choosing this path is the next part of the issue.

// rereadOrder is the read an update is asked to fold from.
type rereadOrder struct {
	// Tables are the tables to read again, "schema.table" as the snapshot
	// lists them. They must be some of the snapshot's tables, not all.
	Tables []string
	// Source is how the read reaches the server: console.BaselineRequestFor
	// of the same entry the update is for.
	Source console.BaselineRequest
}

// errRereadCaptureBehind is a read whose coordinate the index did not show
// reached within rereadCaptureWait. Nothing was folded.
var errRereadCaptureBehind = errors.New("capture did not reach the point of the read of these tables in time")

var (
	// rereadCaptureWait bounds the wait for capture. It holds this server's
	// job slot, and no snapshot lane.
	rereadCaptureWait = 5 * time.Minute
	// rereadCapturePoll is how often the index is asked.
	rereadCapturePoll = 5 * time.Second
	// rereadCaptureReached is the engine's check; a test replaces it.
	rereadCaptureReached = reconstruct.BaselineOverridesReached
)

// prepareReread makes the read and waits for capture. It returns the files
// for FullTableConfig.BaselineOverride and the function that removes them,
// which is never nil and which the caller runs when the fold has returned.
// On an error there are no files, and the run must fold nothing.
func (s *baselineSupervisor) prepareReread(req refreshRequest, job *jobRun) (map[string]string, func(), error) {
	none := func() {}
	source := req.Reread.Source
	if req.SourcePostgres || source.Flavor == console.FlavorPostgres {
		return nil, none, fmt.Errorf("%w: a PostgreSQL table is not read again this way", errRereadUnavailable)
	}
	if source.ServerID != req.ServerID {
		return nil, none, fmt.Errorf("the read of some tables names server %q and this update is for %q", source.ServerID, req.ServerID)
	}
	want, err := rereadTableList(req.Reread.Tables)
	if err != nil {
		return nil, none, err
	}
	// Everything that can refuse without reading production comes first,
	// against the snapshot the fold will read (the caller resolved
	// req.FoldSource).
	if strings.TrimSpace(req.BaselineDir) == "" {
		return nil, none, console.NoLocalFolderError()
	}
	foldSource := baselineFoldSource(req)
	prev, inSnapshot, err := newestSnapshotTables(s.ctx, foldSource)
	if err != nil {
		return nil, none, fmt.Errorf("list the snapshot to refresh: %w", err)
	}
	if err := rereadFitsSnapshot(want, inSnapshot); err != nil {
		return nil, none, err
	}
	if err := foldSourceRefusal(req.IndexDSN, foldSource, prev); err != nil {
		return nil, none, err
	}
	for _, root := range []string{foldSource, req.BaselineDir} {
		if pathInside(s.stagingDir, root) {
			return nil, none, fmt.Errorf("%w: the working folder %s is inside the snapshots folder %s, where a read would be found as a snapshot of its own",
				errRereadUnavailable, s.stagingDir, root)
		}
	}
	if job != nil {
		source.Journal = job
	}
	read, err := s.stageReread(source, want)
	if err != nil {
		return nil, none, fmt.Errorf("read %s again: %w", strings.Join(want, ", "), err)
	}
	discard := func() {
		if err := read.discard(); err != nil {
			slog.Error("snapshot refresh: could not remove the read of some tables; it still takes room in the working folder",
				"server", req.ServerName, "path", read.root, "error", err)
		}
	}
	// Until the caller holds discard, it is this function's to run, on an
	// error and on a panic: the job's release would drop the folder from the
	// journal, and nothing else ever removes it.
	handedOver := false
	defer func() {
		if !handedOver {
			discard()
		}
	}()
	if err := s.waitForRereadCapture(req, read); err != nil {
		return nil, none, err
	}
	handedOver = true
	return read.files, discard, nil
}

// rereadFitsSnapshot refuses a list the fold could not use. A table the
// snapshot does not hold is not part of the run. A list of every table leaves
// the run with no snapshot to carry the record of skipped views from, so that
// case takes an ordinary full read.
func rereadFitsSnapshot(want, inSnapshot []string) error {
	for _, st := range want {
		if !slices.Contains(inSnapshot, st) {
			return fmt.Errorf("%w: %s is not in the snapshot this update starts from", errRereadUnavailable, st)
		}
	}
	for _, st := range inSnapshot {
		if !slices.Contains(want, st) {
			return nil
		}
	}
	return fmt.Errorf("%w: the list holds every table of the snapshot, which is a full read", errRereadUnavailable)
}

// pathInside says whether p is root or below it, links followed as far as
// they exist. A bucket holds no local folder.
func pathInside(p, root string) bool {
	if root == "" || strings.HasPrefix(root, "s3://") {
		return false
	}
	resolve := func(x string) string {
		if abs, err := filepath.Abs(x); err == nil {
			x = abs
		}
		if real, err := filepath.EvalSymlinks(x); err == nil {
			return real
		}
		return filepath.Clean(x)
	}
	rel, err := filepath.Rel(resolve(root), resolve(p))
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator))
}

// waitForRereadCapture returns once a run starting now would find the read
// reached. Each try asks about the present instant: the cut of a fixed instant
// stops moving at the first event stamped after it, and a source whose clock
// runs ahead of this host can stamp one before the read's coordinate.
//
// Only "not reached yet" is tried again. A source with no write after the read
// is reached once capture saves a position past it (the engine's
// resolveCutPastCapture), but only when that position shows it: capture by
// GTID, a binary log rotated after the last transaction and an index holding no
// change all land on the bound, and so does a binary log renumbered under the
// read. The error does
// not claim to know which.
func (s *baselineSupervisor) waitForRereadCapture(req refreshRequest, read stagedReread) error {
	deadline := time.Now().Add(rereadCaptureWait)
	for {
		err := rereadCaptureReached(s.ctx, reconstruct.OverrideProbe{
			IndexDSN: req.IndexDSN, BaselineSrc: baselineFoldSource(req), OutputDir: req.BaselineDir,
			At: time.Now().UTC(), Override: read.files,
		})
		if err == nil {
			return nil
		}
		if !errors.Is(err, reconstruct.ErrBaselineOverrideAhead) {
			return fmt.Errorf("check the read of some tables against the index: %w", err)
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf("%w (waited %s; capture is stopped or behind, the source has written nothing since the read and capture's saved position does not show it reached (capture by GTID, a binary log rotated after the last transaction, an index holding no change), or its binary log was renumbered): %w",
				errRereadCaptureBehind, rereadCaptureWait, err)
		}
		timer := time.NewTimer(rereadCapturePoll)
		select {
		case <-s.ctx.Done():
			timer.Stop()
			return fmt.Errorf("stopped while waiting for capture to reach the read of some tables: %w", s.ctx.Err())
		case <-timer.C:
		}
	}
}
