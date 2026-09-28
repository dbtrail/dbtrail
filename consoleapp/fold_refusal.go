package consoleapp

import (
	"context"
	"errors"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// foldRefusal is a fold's run error together with the tables that caused it
// (#1653). The list travels ON the error because the refusal is the error:
// every place that reports a failed fold already holds it, so the status the
// page polls and the run history read the same list from the same value, and
// a run that failed for another reason (no snapshot to start from, an upload)
// carries none.
//
// It reads and unwraps as the error it holds, so the text operators alert on
// and every errors.Is on the fold's sentinels are unchanged.
type foldRefusal struct {
	err     error
	tables  []console.RefusedTable
	omitted int
}

func (e *foldRefusal) Error() string { return e.err.Error() }
func (e *foldRefusal) Unwrap() error { return e.err }

// withRefusedTables attaches the refused tables among outcomes to runErr.
// outcomes come from reconstruct.RefreshOutcomes, the rule the command line
// prints from, so the verdict is decided once. A nil runErr stays nil: a run
// that published has no refusal, whatever the outcomes say. secrets are the
// connection strings the fold used; see console.RefusedTablesOf.
func withRefusedTables(runErr error, outcomes []reconstruct.RefreshOutcome, secrets ...string) error {
	if runErr == nil {
		return nil
	}
	if errors.Is(runErr, context.Canceled) {
		// The run was stopped (the daemon is shutting down), and every table
		// in flight reports that as its own failure. No table refused
		// anything, so none is recorded as having stopped the update.
		return runErr
	}
	kept, omitted := console.RefusedTablesOf(outcomes, secrets...)
	if len(kept) == 0 {
		return runErr
	}
	return &foldRefusal{err: runErr, tables: kept, omitted: omitted}
}

// refusedTablesIn reads the list a fold error carries: nil and zero for a nil
// error and for a failure that was not a table's.
func refusedTablesIn(err error) ([]console.RefusedTable, int) {
	var r *foldRefusal
	if !errors.As(err, &r) {
		return nil, 0
	}
	return r.tables, r.omitted
}
