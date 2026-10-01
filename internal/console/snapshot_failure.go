package console

// SnapshotFailure says, as data, why a full read of a MySQL or MariaDB server
// failed when that is one of the few causes the page can explain in plain
// words (#1986). The error string beside it (LastError, Error) stays as it
// was, for the page's "Technical details" fold and for API clients.
//
// A failure with no Kind is still a failure: the page draws the generic card
// for it. Kind is set only where the cause is certain, because a wrong one
// (a GRANT for the wrong account or privilege) sends the operator to fix the
// wrong thing, which is worse than none.
type SnapshotFailure struct {
	Kind string `json:"kind,omitempty"`
	// Grant is the statement to run on the source, for missing_permission.
	Grant string `json:"grant,omitempty"`
	// Privileges is how many privileges Grant gives, so the page says "one
	// more permission" only when it is one.
	Privileges int `json:"privileges,omitempty"`
	// MinVersion is the mydumper version to install, for mydumper_too_old.
	MinVersion string `json:"min_version,omitempty"`
}

const (
	// SnapshotFailureMissingPermission: the source's database user lacks a
	// privilege the lock mode it last tried needs. Grant names it.
	SnapshotFailureMissingPermission = "missing_permission"
	// SnapshotFailureMydumperTooOld: the mydumper DBTrail runs is older than
	// this snapshot needs. MinVersion names the version to install.
	SnapshotFailureMydumperTooOld = "mydumper_too_old"
)
