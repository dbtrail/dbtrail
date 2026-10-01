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

// snapshotFailureOf is why a full read failed, for the page, when the cause
// is one it can explain in plain words (#1986); nil for every other failure,
// which the page draws as the generic card with the error text folded.
//
// Read only from typed errors, never from the message. It gives no kind when
// the statement it would show could be wrong:
//   - ftwrl's missing privileges on an Amazon RDS or Aurora host name: RDS
//     refuses BACKUP_ADMIN outright, so the GRANT could not work;
//   - mydumper's own refusal of the global read lock (ftwrlDeniedError): the
//     privilege check passed, so the user already holds RELOAD;
//   - no account read from SHOW GRANTS (the DSN's user at '%' would be a
//     guess at the account's host).
//
// After execute's automatic retry the error wraps only the lock-all attempt's
// failure, so the statement is for what lock-all needs, the mode tried last.
func snapshotFailureOf(err error, req console.BaselineRequest) *console.SnapshotFailure {
	if err == nil || req.Flavor == console.FlavorPostgres {
		return nil
	}
	if old := (*mydumperTooOldError)(nil); errors.As(err, &old) {
		return &console.SnapshotFailure{Kind: console.SnapshotFailureMydumperTooOld, MinVersion: old.min}
	}
	var mp *mydumperlock.MissingPrivilegesError
	if !errors.As(err, &mp) {
		return nil
	}
	if errors.Is(mp, mydumperlock.ErrFTWRLPrivilegesMissing) && sourceIsManaged(req.SourceDSN) {
		return nil
	}
	grant := mp.Grant()
	if grant == "" {
		return nil
	}
	return &console.SnapshotFailure{Kind: console.SnapshotFailureMissingPermission, Grant: grant, Privileges: len(mp.Missing)}
}
