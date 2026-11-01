package consoleapp

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/storage"
)

// pruneS3Fn is reconstruct.PruneS3, replaced in tests.
var pruneS3Fn = reconstruct.PruneS3

// s3PruneBusyFn reports whether a job that reads old snapshots from S3 runs
// now for any of the servers (a restore, a SQL file build). Set where the
// baseline supervisor is built; nil is never busy. The S3 prune skips a
// destination while one does, so a snapshot is not removed under a reader.
var s3PruneBusyFn func(serverIDs []string) bool

// s3PruneCutFn is the instant before which a destination's snapshots are
// removed by the S3 prune (#2596), and whether it prunes at all. Set by
// startBaselinePruneLoop from the registry it re-reads; nil prunes nothing.
// Both paths that send a snapshot the bucket lacks ask it, so a snapshot the
// prune removed on purpose is not sent back.
var s3PruneCutFn func(s3URL string, now time.Time) (time.Time, bool)

// s3PruneCut answers s3PruneCutFn from the registry and the local age, the
// way the prune cycle decides.
func s3PruneCut(reg *console.Registry, globalS3, retainRaw, s3URL string, now time.Time) (time.Time, bool) {
	if reg == nil {
		return time.Time{}, false
	}
	local, on := effectiveRetain(reg, retainRaw)
	if !on {
		local = 0
	}
	age, _, ok := console.S3RetentionFor(reg.List(), globalS3, s3URL, local)
	if !ok {
		return time.Time{}, false
	}
	return now.Add(-age), true
}

// pastS3Cut reports whether the snapshot taken at `at` is one the S3 prune
// removes from s3URL: not to be sent there again.
func pastS3Cut(s3URL string, at, now time.Time) bool {
	if s3PruneCutFn == nil {
		return false
	}
	cut, on := s3PruneCutFn(s3URL, now)
	return on && at.Before(cut)
}

// runS3PruneCycle removes, at each S3 destination with an age, the snapshots
// older than it (#2596). It runs after the local prune of the same cycle,
// which removes a local snapshot only while its copy is still in S3.
func runS3PruneCycle(ctx context.Context, entries []console.ServerEntry, globalS3 string, localRetain time.Duration, now time.Time) {
	seen := map[string]bool{}
	for _, e := range entries {
		dest := strings.TrimSuffix(strings.TrimSpace(e.BaselineS3), "/")
		if dest == "" || seen[dest] || ctx.Err() != nil {
			continue
		}
		seen[dest] = true
		age, ids, on := console.S3RetentionFor(entries, globalS3, dest, localRetain)
		if !on {
			continue
		}
		if s3PruneBusyFn != nil && s3PruneBusyFn(ids) {
			slog.Info("snapshot S3 cleanup: a restore or a SQL file build is reading this destination; it is cleaned next time",
				"destination", dest)
			console.RecordS3Cleanup(dest, console.S3CleanupRecord{At: now.UTC().Format(time.RFC3339), Skipped: true})
			continue
		}
		// Asked again before each snapshot: a run can take minutes, and a
		// restore that starts in it reads an old snapshot it may be removing.
		proceed := func() bool { return s3PruneBusyFn == nil || !s3PruneBusyFn(ids) }
		res, err := pruneS3Fn(ctx, reconstruct.PruneS3Options{S3URL: dest, Retain: age, Now: now, Proceed: proceed})
		// For the settings page, which shows the last cleanup of the
		// destination beside the age.
		rec := console.S3CleanupRecord{At: now.UTC().Format(time.RFC3339), Removed: len(res.Pruned),
			KeptNewest: res.KeptNewest, Deferred: res.Deferred, Stopped: res.Stopped}
		if err != nil {
			rec.Error, rec.Snapshot, rec.AccessDenied = err.Error(), res.Failed, storage.IsAccessDenied(err)
		}
		console.RecordS3Cleanup(dest, rec)
		if res.Deferred > 0 && err == nil {
			switch res.Stopped {
			case "busy":
				slog.Info("snapshot S3 cleanup: stopped because a restore or a SQL file build started reading this destination; the rest go in the next cleanup",
					"destination", dest, "left_for_next_time", res.Deferred)
			case "cancelled":
				slog.Info("snapshot S3 cleanup: stopped by shutdown; the rest go in the next cleanup", "destination", dest, "left_for_next_time", res.Deferred)
			default:
				slog.Info("snapshot S3 cleanup: more snapshots are past the age than one cleanup removes; the rest go in the next ones",
					"destination", dest, "left_for_next_time", res.Deferred)
			}
		}
		if len(res.Pruned) > 0 {
			slog.Info("snapshot S3 cleanup: removed snapshots older than the age set for this destination",
				"destination", dest, "removed", len(res.Pruned), "older_than", age, "kept_newest_of_a_table", res.KeptNewest,
				"left_for_next_time", res.Deferred)
		}
		if err != nil {
			args := []any{"destination", dest, "error", err}
			if res.Failed != "" {
				args = append(args, "snapshot", res.Failed)
			}
			if storage.IsAccessDenied(err) {
				args = append(args, "hint", "removing needs s3:DeleteObject and s3:PutObject on the prefix")
			}
			slog.Warn("snapshot S3 cleanup: stopped; nothing more is removed from this destination until the next cleanup, and a snapshot it stopped in the middle of is finished then", args...)
		}
	}
}

// beingPruned reports whether the S3 cleanup may be removing the snapshot
// folder dest of the destination s3URL, so a send must not write into it:
// it could leave _SUCCESS over files the cleanup then removes. Asked of S3
// only where the destination has an age (no cleanup runs anywhere else). A
// probe that fails is returned as such: the caller does not send, and says
// why.
func beingPruned(ctx context.Context, s3URL, dest string) (bool, error) {
	if s3PruneCutFn == nil {
		return false, nil
	}
	if _, on := s3PruneCutFn(s3URL, time.Now()); !on {
		return false, nil
	}
	return s3ObjectPresent(ctx, strings.TrimSuffix(dest, "/")+"/"+reconstruct.PruneMarker)
}
