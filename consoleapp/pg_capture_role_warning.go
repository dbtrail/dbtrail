package consoleapp

import (
	"context"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/pgstreamrun"
)

// CaptureRoleWarning is console.CaptureRoleWarner for the daemon: what the
// checks say of the power of the role e's source logs in as (0.140.0). A
// role that could not be checked is said, never taken for one with little
// power.
func (m *monitorSupervisor) CaptureRoleWarning(ctx context.Context, e console.ServerEntry) string {
	if !e.IsPostgres() || e.SourceDSN == "" {
		return ""
	}
	w, err := pgstreamrun.CaptureRoleWarning(ctx, e.SourceDSN)
	if err != nil {
		return "the power of the user DBTrail connects with could not be checked (" + err.Error() + "): if it is a superuser or can bypass row-level security, give DBTrail a role that can only replicate and read the published tables"
	}
	return w
}

var _ console.CaptureRoleWarner = (*monitorSupervisor)(nil)
