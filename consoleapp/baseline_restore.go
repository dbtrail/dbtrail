package consoleapp

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// Point-in-time restore (the backups page's PITR action): fold the snapshot
// at-or-before the chosen instant forward through the index's deltas and
// publish the result as a NEW discoverable snapshot named by that instant.
// It shares foldSnapshot with the periodic refresh — same all-or-nothing
// publication, same sentinel-classified refusals (capture gap, destructive
// DDL), and the same in-daemon resource posture: the conservative DuckDB
// budget, a fixed table parallelism, and the volume warning turned on with
// advice written for this binary rather than for the CLI (this is a daemon
// that is also capturing; see executeRefresh's resource-posture comment and
// the daemonFold* constants beside it).

// TriggerRestore starts a restore, sharing the supervisor's per-server
// single-flight with dumps and refreshes: all three write the same store.
func (s *baselineSupervisor) TriggerRestore(req console.BaselineRestoreRequest) error {
	s.mu.Lock()
	if s.busyLocked(req.ServerID) {
		s.mu.Unlock()
		return console.ErrBaselineRunning
	}
	s.restores[req.ServerID] = &console.BaselineStatus{State: "running", Since: nowStamp(),
		At: req.At.UTC().Format(time.RFC3339)}
	s.mu.Unlock()

	slog.Info("baseline restore: starting", "server", req.ServerName, "id", req.ServerID,
		"at", req.At.UTC().Format(time.RFC3339))
	go s.runRestore(req)
	return nil
}

// RestoreStatus reports the last restore for a server (idle if none ran here).
func (s *baselineSupervisor) RestoreStatus(serverID string) console.BaselineStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	if st, ok := s.restores[serverID]; ok {
		return *st
	}
	return console.BaselineStatus{State: "idle"}
}

func (s *baselineSupervisor) runRestore(req console.BaselineRestoreRequest) {
	defer s.recoverBaselineJob(baselineJobRestore, req.ServerID, req.ServerName)
	started := time.Now().UTC()
	tables, refused, reuse, err := s.executeRestore(req)
	// Same rule as runRefresh, for the same reason: on a server whose backups
	// go to S3, a snapshot that exists on one box is not where the backups
	// live, and a restore that stopped there would be the one Parquet
	// publisher retention could never reclaim (PruneLocal confirms the S3 copy
	// before it deletes a local one). Gated on the fold's success so an
	// incomplete snapshot never reaches the destination.
	var uploaded int
	if err == nil && req.BaselineS3 != "" {
		uploaded, err = uploadRefreshedSnapshot(s.ctx, restoreFoldRequest(req), req.At.UTC())
	}
	s.recordRun(req.ServerID, req.ServerName, foldRunCounts(console.BaselineRunRecord{
		Kind: console.BaselineRunRestore, StartedAt: started.Format(time.RFC3339),
		SnapshotTime: publishedSnapshotTime(req.At, err),
		Uploaded:     uploaded,
	}, tables, refused, reuse), err)

	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.restores[req.ServerID]
	if st == nil { // defensive; never cleared under lock
		st = &console.BaselineStatus{}
		s.restores[req.ServerID] = st
	}
	applyFoldStatus(st, tables, refused, reuse, err)
	if err != nil {
		if foldPublished(err) {
			// The fold did its work; only the sending failed. Said as such,
			// because "published nothing" below would send an operator looking
			// for a backup that is sitting complete in the local directory.
			slog.Warn("baseline restore: published locally, not uploaded", "server", req.ServerName, "id", req.ServerID,
				"tables", tables, "error", err)
			return
		}
		// Warn, never Error: a refusal (gap, schema change) is the fail-closed
		// contract working, and the operator picks another moment.
		slog.Warn("baseline restore: published nothing", "server", req.ServerName, "id", req.ServerID,
			"refused", refused, "error", err)
		return
	}
	slog.Info("baseline restore: published", "server", req.ServerName, "id", req.ServerID,
		"tables", tables, "reused", reuse.reused, "reused_copied", reuse.copied, "uploaded", uploaded,
		"at", req.At.UTC().Format(time.RFC3339))
}

// snapshotAt is reconstruct.SnapshotAt behind a seam, for the reason
// newestSnapshotTables is: on an S3-backed server the listing reads the
// bucket, and a unit test that reached one would be neither hermetic nor
// offline-safe. Written by tests only, and a test that replaces it must not
// restore it until the job it started has reached a terminal state (the same
// rule as foldTables).
var snapshotAt = reconstruct.SnapshotAt

// executeRestore folds toward the chosen instant. Unlike runRefresh it does
// not reclaim the partial snapshot a refusal leaves behind: the operator who
// asked is there to look at it (docs/dump-and-baseline.md says so), and the
// daemon log names the directory. The table list comes from
// the snapshot FindBaseline will anchor on (newest at-or-before At), NOT the
// newest snapshot overall: restoring to a moment before the newest snapshot
// must fold the older snapshot's tables.
//
// The list and the fold read the SAME location, derived once: the one
// baselineFoldSource picks for the translated request, which is where the
// scheduled update reads too (#1541) — the bucket on an S3-backed server, the
// local directory otherwise. Listing req.BaselineDir here was the bug: on a
// server whose backups are uploaded and pruned, or made by another host, the
// local directory holds only what this daemon folded since it started, and
// the refusal below fired while the bucket held dozens of backups. Deriving
// the listing's source separately from the fold's would let the two name
// different locations, and the fold would then refuse tables the list
// promised.
func (s *baselineSupervisor) executeRestore(req console.BaselineRestoreRequest) (tables, refused int, reuse reuseTally, err error) {
	fold := restoreFoldRequest(req)
	source := baselineFoldSource(fold)
	tableList, anchor, err := snapshotAt(s.ctx, source, req.At)
	if err != nil {
		return 0, 0, reuseTally{}, fmt.Errorf("list the snapshot to restore from: %w", err)
	}
	if len(tableList) == 0 {
		return 0, 0, reuseTally{}, fmt.Errorf("no backup exists at or before %s; a restore folds an existing backup forward, so pick a moment after your oldest backup", req.At.UTC().Format("2006-01-02 15:04:05"))
	}
	if reconstruct.SnapshotDirName(anchor) == reconstruct.SnapshotDirName(req.At) {
		// Compared by the DIRECTORY NAME, which is what collides, not by the
		// instant: the name is whole seconds, and the console truncates At on
		// the way in, but a caller that did not would fold a 10:00:00.5
		// restore onto the 10:00:00 snapshot's directory.
		// The handler refuses a COMPLETE local snapshot at exactly this
		// instant before the job starts; this is the same refusal for the
		// bucket, which the handler does not open. Without it the fold would
		// rebuild that very snapshot and the upload would overwrite it in
		// place, writing the _INCOMPLETE marker into a complete remote backup
		// first, so a failure midway leaves the bucket copy hidden from every
		// listing and nothing saying so.
		return 0, 0, reuseTally{}, fmt.Errorf("a backup already exists at exactly %s in %s; pick another second, or use that backup", req.At.UTC().Format("2006-01-02 15:04:05"), source)
	}
	return s.foldSnapshot(fold, req.At.UTC(), tableList)
}

// restoreFoldRequest translates a restore request into the fold request the
// refresh path uses, which is the whole claim wireBaselineExtras makes below:
// same fold, same store, same read source and same destination; the delta is
// who picks the instant.
//
// Split out so that claim is checkable without an index and a baseline. Left
// inline, dropping CarryForwardUnchanged compiled, passed every test, and
// silently made the restore the one Parquet publisher that ignores the
// operator's setting. Dropping BaselineS3 would compile too, and would make it
// the one fold that reads and writes the local directory alone on a server
// whose backups live in a bucket (#1541).
func restoreFoldRequest(req console.BaselineRestoreRequest) refreshRequest {
	return refreshRequest{
		ServerID: req.ServerID, ServerName: req.ServerName,
		IndexDSN: req.IndexDSN, BaselineDir: req.BaselineDir,
		// Read source AND upload destination, exactly as the scheduled update
		// carries it: refreshFoldConfig reads the previous snapshot from it and
		// runRestore uploads to it.
		BaselineS3: req.BaselineS3,
		// The console resolves the effective value at request time; this only
		// executes it. Resolving again here would let a toggle saved while a
		// restore sits in the queue change what the operator asked for.
		CarryForwardUnchanged: req.CarryForwardUnchanged,
	}
}

// wireBaselineExtras attaches the run history, the restore capability and
// the sql-export capability to a freshly built supervisor, and exposes them
// on the console Config. Called by
// both watch entry paths; a no-op without a supervisor (serve-only consoles
// keep their 403s).
//
// Restore rides EITHER opt-in deliberately, which looks like the derivation
// the BaselineRestorer comment warns about — it is not. #1171's hazard was a
// derived flag turning on a feature of a DIFFERENT class (a dump locks and
// reads the source; mydumper may not exist). A restore is the same fold the
// refresh already performs, into the same store, with no source contact,
// reading its previous snapshot from the same place and sending the result
// to the same destination; the only delta is who picks the instant. A daemon
// that opted into either baseline producer has already accepted exactly this
// work. The sql export rides the same reasoning: the same fold, into staging
// instead of the store, handed out only through the query:execute download.
// An unreadable history file runs without history rather than refusing the
// daemon — durations degrade to file-timestamp spans, and not opening a store
// means nothing overwrites the file it might describe.
func wireBaselineExtras(cfg *console.Config, sup *baselineSupervisor, serversPath string) {
	if sup == nil {
		return
	}
	history, err := console.OpenBaselineHistory(console.DefaultBaselineHistoryPath(serversPath))
	if err != nil {
		slog.Error("baseline history unavailable; run durations will NOT be recorded (file timestamps still approximate them); fix or move the file and restart", "error", err)
		history = nil
	}
	sup.history = history
	cfg.BaselineHistory = history
	cfg.BaselineRestore = sup
	cfg.SQLExport = sup
}
