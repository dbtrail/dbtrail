package consoleapp

import (
	"errors"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/mydumperlock"
)

// mydumperTooOldError is a refusal whose fix is installing a newer mydumper:
// the build on PATH reads as older than the lock mode, or the source, needs.
// Made where planMydumper and runMydumper refuse for that, so the page's
// "install mydumper X" card (#1986) never reads the words. A version that
// could not be read, a binary that cannot run, or no binary at all are NOT
// this: no version was compared, so none can be named.
type mydumperTooOldError struct {
	err error
	min string
}

func (e *mydumperTooOldError) Error() string { return e.err.Error() }
func (e *mydumperTooOldError) Unwrap() error { return e.err }

// chosenModeError marks a dump failure whose lock mode an operator chose
// (a startup variable or a saved setting), so the page may say the fix can
// be undoing that choice. Made in execute, which knows where the mode came
// from; the message is unchanged.
type chosenModeError struct{ err error }

func (e *chosenModeError) Error() string { return e.err.Error() }
func (e *chosenModeError) Unwrap() error { return e.err }

// snapshotFailureOf is what the page's failure card needs about a failed run
// that published nothing (#1986): nil only for a success. Its Kind is set
// only for the two causes the card explains in plain words; any other
// failure carries Summary, the error's first line, as its hint.
//
// Kinds come from typed errors, never from the message. No kind when the
// statement it would show could be wrong:
//   - ftwrl's missing privileges on an Amazon RDS or Aurora host name: RDS
//     refuses BACKUP_ADMIN outright, so the GRANT could not work;
//   - mydumper's own refusal of the global read lock (ftwrlDeniedError): the
//     privilege check passed, so the user already holds RELOAD;
//   - no account read from SHOW GRANTS (the DSN's user at '%' would be a
//     guess at the account's host);
//   - a Postgres source, which never runs mydumper.
//
// After execute's automatic retry the error wraps only the lock-all attempt's
// failure, so the statement is for what lock-all needs, the mode tried last.
func snapshotFailureOf(err error, req console.BaselineRequest) *console.SnapshotFailure {
	if err == nil {
		return nil
	}
	f := &console.SnapshotFailure{Server: req.ServerName, Scheduled: req.Trigger == console.BaselineRunTriggerScheduled}
	var chosen *chosenModeError
	f.ModeChosen = errors.As(err, &chosen)
	if req.Flavor == console.FlavorPostgres {
		f.Postgres = true
	} else if old := (*mydumperTooOldError)(nil); errors.As(err, &old) {
		f.Kind, f.MinVersion = console.SnapshotFailureMydumperTooOld, old.min
	} else if mp := (*mydumperlock.MissingPrivilegesError)(nil); errors.As(err, &mp) &&
		!(errors.Is(mp, mydumperlock.ErrFTWRLPrivilegesMissing) && sourceIsManaged(req.SourceDSN)) {
		if grant := mp.Grant(); grant != "" {
			f.Kind, f.Grant, f.Privileges = console.SnapshotFailureMissingPermission, grant, len(mp.Missing)
		}
	}
	if f.Kind == "" {
		f.Summary = refusalSummary(err)
	}
	return f
}
