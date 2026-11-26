package console

import (
	"context"
	"database/sql"

	"github.com/dbtrail/dbtrail/internal/status"
)

// RefreshLineSource hands the console the rule the daemon's snapshot updates
// of a server draw their line with (#3083), built where the updates build
// their own, so the Snapshots page grades "aging" on that same line: a file
// an update keeps is never shown as aging, and a file it is going to rewrite
// is.
type RefreshLineSource interface {
	// RefreshRule is the rule for the server's index, read through the
	// page's own connection (db, dbName). False when no update of this daemon
	// refreshes that server; the page then grades on the span alone.
	RefreshRule(ctx context.Context, serverID string, db *sql.DB, dbName string) (status.RefreshRule, bool)
}

// refreshRuleFor is the rule the selected server's snapshot updates draw
// their line with, or nil when this process runs no update of it.
func (s *Server) refreshRuleFor(ctx context.Context, serverID string, db *sql.DB, dbName string) *status.RefreshRule {
	if s.refreshLine == nil {
		return nil
	}
	rule, ok := s.refreshLine.RefreshRule(ctx, serverID, db, dbName)
	if !ok {
		return nil
	}
	return &rule
}
