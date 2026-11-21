package pgbaseline

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/event"
)

// SetTestHookAfterSnapshot installs the after-anchor seam for the external
// integration tests (boundary test); returns a restore func. Test binary only.
func SetTestHookAfterSnapshot(f func()) (restore func()) {
	prev := testHookAfterSnapshot
	testHookAfterSnapshot = f
	return func() { testHookAfterSnapshot = prev }
}

// SetCheckReadAccess replaces the read-access check for a test that needs the
// snapshot to run without it; returns a restore func. Test binary only.
func SetCheckReadAccess(f func(context.Context, *pgx.Conn, string, event.Filters) error) (restore func()) {
	prev := checkReadAccess
	checkReadAccess = f
	return func() { checkReadAccess = prev }
}
