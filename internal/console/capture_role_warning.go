package console

import (
	"context"
	"time"
)

// CaptureRoleWarner is a MonitorController that can read how much power the
// role a PostgreSQL server's source logs in as has (0.140.0). Saving the yes
// about forwarding as that role asks it, so the answer to the save says
// what the checks say: a superuser, or a role that bypasses row-level
// security, and the way to a role with less.
type CaptureRoleWarner interface {
	// CaptureRoleWarning is the warning for e's source role, "" when there
	// is none; a role that could not be checked is said as such.
	CaptureRoleWarning(ctx context.Context, e ServerEntry) string
}

// captureRoleCheckFor bounds the check a save of the yes waits for.
const captureRoleCheckFor = 10 * time.Second

// captureRoleWarning is CaptureRoleWarner's answer for e, "" when no warner
// is wired or e does not forward as its capture account.
func (s *Server) captureRoleWarning(ctx context.Context, e ServerEntry) string {
	w, ok := s.monitorCtrl.(CaptureRoleWarner)
	if !ok || !pgForwardTargetOf(e).CaptureAccount {
		return ""
	}
	ctx, cancel := context.WithTimeout(ctx, captureRoleCheckFor)
	defer cancel()
	return w.CaptureRoleWarning(ctx, e)
}
