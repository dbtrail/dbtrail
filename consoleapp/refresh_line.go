package consoleapp

import (
	"context"
	"database/sql"
	"log/slog"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/rotation"
	"github.com/dbtrail/dbtrail/internal/status"
)

// refreshRetentionReadTimeout bounds the one query the Snapshots page makes
// for the index's own retention, on the page's render path.
const refreshRetentionReadTimeout = 2 * time.Second

// refreshLineSource is console.RefreshLineSource (#3083): the rule the
// snapshot updates this daemon runs for a server draw their line with, so the
// Snapshots page grades "aging" on that same line. A server no update of this
// daemon refreshes gets no rule, and the page grades it on the span alone: a
// copy nothing is going to rewrite must not read "ok" on the strength of an
// update that never comes.
type refreshLineSource struct {
	sup   *baselineSupervisor
	sched *backupScheduler
	reg   *console.Registry
	// The daemon-wide refresh loop's own inputs (startBaselineRefreshLoop):
	// the boot index and folder, and its interval, zero when it does not run.
	globalDSN, globalDir string
	globalEvery          time.Duration
}

// newRefreshLineSource returns nil, a true nil interface, when no supervisor
// updates any snapshot.
func newRefreshLineSource(sup *baselineSupervisor, sched *backupScheduler, reg *console.Registry, globalDSN, globalDir string, globalEvery time.Duration) console.RefreshLineSource {
	if sup == nil {
		return nil
	}
	return &refreshLineSource{sup: sup, sched: sched, reg: reg, globalDSN: globalDSN, globalDir: globalDir, globalEvery: globalEvery}
}

// refreshEvery is the interval of the updates this daemon runs for serverID,
// zero when none does:
//
//   - the daemon-wide loop, when it runs and the server is one of its targets
//     (baselineRefreshTargets, the loop's own selection: an S3-only server or
//     one whose folder another server writes is not);
//   - the server's own schedule, when the schedule loop would run it
//     (console.CheckBackupSchedule with the loop's own gates).
//
// With both, the longer interval. The line only moves later as the interval
// grows (a wider margin, and the young-index rule ending sooner), so the
// longer one marks a file aging as soon as either update would rewrite it.
func (l *refreshLineSource) refreshEvery(serverID string) time.Duration {
	var every time.Duration
	entries := registryEntries(l.reg)
	if l.globalEvery > 0 {
		targets, _, _ := baselineRefreshTargets(entries, l.globalDSN, l.globalDir)
		for _, t := range targets {
			if t.ServerID == serverID {
				every = l.globalEvery
				break
			}
		}
	}
	if l.sched != nil {
		for _, e := range entries {
			if e.ID != serverID || e.BackupSchedule == nil {
				continue
			}
			p, err := e.BackupSchedule.Parse()
			if err == nil && p.Every > 0 && console.CheckBackupSchedule(e, *e.BackupSchedule, l.sched.gates()) == nil {
				every = max(every, p.Every)
			}
		}
	}
	return every
}

// RefreshRule builds the rule with the same code the updates use
// (buildCoverageRule), reading the index's own retention on the page's
// connection with a short deadline rather than opening one per page load.
func (l *refreshLineSource) RefreshRule(ctx context.Context, serverID string, db *sql.DB, dbName string) (status.RefreshRule, bool) {
	every := l.refreshEvery(serverID)
	if every <= 0 {
		return status.RefreshRule{}, false
	}
	rule, _, unreadable, readErr := l.sup.buildCoverageRule(every, func() rotation.Effective {
		if db == nil {
			return rotation.Effective{Source: rotation.RetainUnreadable}
		}
		rctx, cancel := context.WithTimeout(ctx, refreshRetentionReadTimeout)
		defer cancel()
		return rotation.ResolveEffective(rctx, db, dbName)
	})
	if unreadable {
		// The update says this once a day on its own run (reportGateBlind);
		// here, only at Debug: the page then grades as the update does with
		// no window known, which is the cautious line.
		slog.Debug("snapshots page: could not read the retention this index was created under; aging is graded as if its window were unknown",
			"server", serverID, "error", readErr)
	}
	return rule.shared(), true
}
