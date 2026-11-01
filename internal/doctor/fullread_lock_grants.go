package doctor

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/dbtrail/dbtrail/internal/metadata"
)

// FullReadLockGrantsCheckName is the check that an account set up for the
// default full-read lock can see what that lock would wait behind (#2530).
const FullReadLockGrantsCheckName = "Full read lock permissions"

// checkFullReadLockGrants: a full read in lock mode ftwrl first waits until no
// statement has run for a second or more on the source, so its lock does not
// stop writes behind a long query, and it can see other accounts' statements
// only with PROCESS. Advisory (Optional): capture works without it, and the
// read still runs, only without that wait seeing the application.
//
// The mode is the web interface's or the command line's to choose, so this
// reads what the account can do instead: RELOAD or FLUSH_TABLES is what ftwrl
// needs, and an account without either reads with lock-all, which does not
// wait. Grants are read with metadata's parser; the full read's own check
// (mydumperlock) reads them with its own, and a test there holds the two to
// the same answer.
func checkFullReadLockGrants(ctx context.Context, db *sql.DB) CheckResult {
	grants, err := showGrants(ctx, db)
	if err != nil {
		// The replication-grants check fails on this already.
		return CheckResult{Name: FullReadLockGrantsCheckName, Status: StatusSkip,
			Detail: "not checked: SHOW GRANTS could not be read: " + err.Error()}
	}
	if !metadata.HasGlobalPrivilege(grants, "RELOAD", "FLUSH_TABLES") {
		return CheckResult{Name: FullReadLockGrantsCheckName, Status: StatusSkip,
			Detail: "not checked: this account has no RELOAD, so full reads use lock mode lock-all, which a running query does not hold up and which does not need PROCESS"}
	}
	if metadata.HasGlobalPrivilege(grants, "PROCESS") {
		return CheckResult{Name: FullReadLockGrantsCheckName, Status: StatusPass,
			Detail: "PROCESS: before taking its lock, a full read waits for every account's long queries to end"}
	}
	user := "<bintrail-user>"
	if len(grants) > 0 {
		if u := extractGrantUser(grants[0]); u != "" {
			user = u
		}
	}
	return CheckResult{
		Name:     FullReadLockGrantsCheckName,
		Status:   StatusWarn,
		Optional: true,
		Detail: "missing: PROCESS. Before taking its lock, a full read waits until no query has run for a second or more, " +
			"but without PROCESS it sees only its own: a long query by the application is not waited for, and writes to " +
			"the tables it reads stop until it ends",
		Remediation: fmt.Sprintf("Run on the source as a user that can grant privileges:\n\n  GRANT PROCESS ON *.* TO %s;\n\n"+
			"PROCESS only lets the account see what each connection is running; it changes nothing.", user),
	}
}

// showGrants reads SHOW GRANTS for this connection's account.
func showGrants(ctx context.Context, db *sql.DB) ([]string, error) {
	rows, err := db.QueryContext(ctx, "SHOW GRANTS")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var grants []string
	for rows.Next() {
		var g string
		if err := rows.Scan(&g); err != nil {
			return nil, err
		}
		grants = append(grants, strings.TrimSpace(g))
	}
	return grants, rows.Err()
}
