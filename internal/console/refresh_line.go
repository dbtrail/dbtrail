package console

import (
	"context"
	"time"

	"github.com/dbtrail/dbtrail/internal/status"
)

// RefreshLineSource hands the console the rule the daemon's snapshot update
// draws its line with (#3083), built where the update builds its own, so the
// Snapshots page grades "aging" on that same line: a file the update keeps is
// never shown as aging, and a file it is going to rewrite is.
type RefreshLineSource interface {
	// RefreshRule is the rule for one index updated every `every`. False when
	// it cannot be built; the page then grades on the span alone.
	RefreshRule(ctx context.Context, indexDSN string, every time.Duration) (status.RefreshRule, bool)
}

// refreshRuleFor is the rule the selected server's snapshot update draws its
// line with, or nil when this process updates no snapshot of it.
func (s *Server) refreshRuleFor(ctx context.Context, serverID, indexDSN string) *status.RefreshRule {
	if s.refreshLine == nil || indexDSN == "" {
		return nil
	}
	every := s.refreshEveryOf(serverID)
	if every <= 0 {
		return nil
	}
	rule, ok := s.refreshLine.RefreshRule(ctx, indexDSN, every)
	if !ok {
		return nil
	}
	return &rule
}

// refreshEveryOf is how often the server's snapshot is updated: its own
// schedule when this daemon runs it, else the daemon-wide refresh interval,
// zero when neither updates it. A server updated by both is graded on its
// schedule, the cadence its page shows; the two lines differ only while the
// index is within an hour and the longer interval of its retention.
func (s *Server) refreshEveryOf(serverID string) time.Duration {
	if s.cm != nil && s.cm.reg != nil {
		if e, ok := s.cm.reg.Get(serverID); ok && e.BackupSchedule != nil {
			if p, err := e.BackupSchedule.Parse(); err == nil && p.Every > 0 &&
				CheckBackupSchedule(e, *e.BackupSchedule, s.scheduleGates()) == nil {
				return p.Every
			}
		}
	}
	if d := s.baselineRefreshDefaults; d.Scheduled && d.RefreshEvery > 0 {
		return d.RefreshEvery
	}
	return 0
}
