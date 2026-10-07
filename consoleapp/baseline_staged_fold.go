package consoleapp

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"
)

// Updates for a server whose snapshots go only to S3 (#2212).
//
// Such a server has a bucket and no Local folder. Its update reads the
// previous snapshot from the bucket (#1539), and needs a filesystem only to
// WRITE the new one, so it writes it into a folder of its own under the
// staging folder (BINTRAIL_CONSOLE_BASELINE_STAGING), the same place a full
// read of that server stages its Parquet:
//
//	<staging>/refresh-<random>/<snapshot>
//
// The snapshot is nested one level down on purpose. The fold publishes the
// newest-snapshot pointer (`current`) in the PARENT of the snapshot
// directory, and a parent shared by every run, or the staging folder itself,
// would keep that pointer, and with it a stale answer, after the run. With
// the run folder as the parent, deleting the run folder removes everything
// the run wrote: the snapshot, the pointer, and the download of the previous
// snapshot (FullTableConfig.DownloadDir), which sits beside the snapshot and
// never inside it, since everything inside the snapshot is uploaded.
//
// The run folder is journaled (stagingNamePrefixes), so a run the process
// dies in is removed at the next start, and it is deleted at the end of
// every run, published or not: there is no local copy to keep, because the
// server has no Local folder to keep one in. An upload that fails therefore
// loses the update, and says so (errStagedSnapshotNotUploaded); the next run
// folds again from the newest snapshot in the bucket.

// stagedRunPrefix names a run folder. A fixed prefix, never the server id:
// an id is not a file name, and the reclaim only removes names it knows.
const stagedRunPrefix = "refresh-"

// errStagedSnapshotNotUploaded marks the upload failure of an update built in
// the staging folder. Unlike errSnapshotNotUploaded it is NOT a published
// snapshot: the staged copy is deleted with its run folder, so nothing is
// left anywhere, and nothing may say otherwise (foldPublished stays false).
// It still keeps the schedule from answering the failure with a full read
// (BaselineStatus.UploadFailed): that one uploads to the same bucket.
var errStagedSnapshotNotUploaded = errors.New("the update was not sent to the snapshot destination")

// stagingProbeEvery is how long stagedUpdatesRefusal's answer is reused.
const stagingProbeEvery = time.Minute

// stagedOutputRoot is the folder a refresh writes its snapshot directory
// into: the run folder for an S3-only server, the server's own folder
// otherwise.
func stagedOutputRoot(req refreshRequest) string {
	if req.StagedRun != "" {
		return req.StagedRun
	}
	return req.BaselineDir
}

// stagedUpdatesRefusal reports why the update of an S3-only server cannot be
// built in this daemon's staging folder, nil when it can: the folder exists
// (or can be made) and a file can be created in it. Cached for a minute, so a
// permission fixed on the host is seen without a restart, and a page load is
// not a disk probe.
func (s *baselineSupervisor) stagedUpdatesRefusal() error {
	s.stagingMu.Lock()
	defer s.stagingMu.Unlock()
	if !s.stagingChecked.IsZero() && time.Since(s.stagingChecked) < stagingProbeEvery {
		return s.stagingErr
	}
	s.stagingErr = probeStagingDir(s.stagingDir)
	s.stagingChecked = time.Now()
	return s.stagingErr
}

func probeStagingDir(dir string) error {
	if dir == "" {
		return errors.New("no staging folder is set (BINTRAIL_CONSOLE_BASELINE_STAGING)")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("the staging folder %s cannot be created: %w", dir, err)
	}
	f, err := os.CreateTemp(dir, ".write-probe-*")
	if err != nil {
		return fmt.Errorf("the staging folder %s cannot be written: %w", dir, err)
	}
	name := f.Name()
	f.Close()
	if err := os.Remove(name); err != nil {
		slog.Warn("snapshot refresh: could not remove the staging folder's write probe", "file", name, "error", err)
	}
	return nil
}

// beginStagedRun makes this run's folder under the staging folder and
// journals it before anything is written into it. The error names the
// staging folder: it is the setting to fix.
func (s *baselineSupervisor) beginStagedRun(job *jobRun) (string, error) {
	if s.stagingDir == "" {
		return "", errors.New("this server keeps its snapshots only in S3, so its update is built in the staging folder, and no staging folder is set (BINTRAIL_CONSOLE_BASELINE_STAGING)")
	}
	if err := os.MkdirAll(s.stagingDir, 0o755); err != nil {
		return "", fmt.Errorf("this server keeps its snapshots only in S3, so its update is built in the staging folder %s, which cannot be created: %w", s.stagingDir, err)
	}
	dir, err := os.MkdirTemp(s.stagingDir, stagedRunPrefix)
	if err != nil {
		return "", fmt.Errorf("this server keeps its snapshots only in S3, so its update is built in the staging folder %s, which cannot be written: %w", s.stagingDir, err)
	}
	job.Created(s.stagingDir, filepath.Base(dir))
	return dir, nil
}

// removeStagedRun deletes a run folder. A failure is said at Error: the
// folder holds a whole snapshot's worth of disk, and the journal entry that
// would reclaim it at the next start is dropped when this run ends.
func removeStagedRun(req refreshRequest) {
	if req.StagedRun == "" {
		return
	}
	if err := removeAllDir(req.StagedRun); err != nil {
		slog.Error("snapshot refresh: could not delete this update's folder in the staging folder; delete it by hand",
			"server", req.ServerName, "id", req.ServerID, "dir", req.StagedRun, "error", err)
	}
}
