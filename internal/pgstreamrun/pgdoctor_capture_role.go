package pgstreamrun

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/doctor"
)

// captureRoleCheck is the name of the check below.
const captureRoleCheck = "Power of the user DBTrail connects with"

// The PostgreSQL port forwards as the user DBTrail connects with unless the
// server has a forwarding user of its own (0.140.0: it serves a server only
// when it can forward). Behind its gate that is a read-only SELECT whose
// every table is a table of the copy, and still a superuser, or a role that
// bypasses row-level security, reads there what nobody may have meant it to:
// rows a policy hides from everyone else. Said here, at `server add` and in
// every check, with the way to a role with less power. A warning, never a
// failure: capture needs what the role has.

// captureRoleQuery reads the two attributes of the session's role that the
// gate does not narrow.
const captureRoleQuery = "SELECT current_user, rolsuper, rolbypassrls FROM pg_roles WHERE rolname = current_user"

// addCaptureRoleCheck reports a capture role with more power than a
// read-only forwarding user needs.
func addCaptureRoleCheck(ctx context.Context, r *doctor.Report, conn *pgx.Conn) {
	var role string
	var super, bypass bool
	if err := conn.QueryRow(ctx, captureRoleQuery).Scan(&role, &super, &bypass); err != nil {
		r.Add(doctor.CheckResult{Name: captureRoleCheck, Status: doctor.StatusWarn, Detail: "could not check: " + err.Error()})
		return
	}
	r.Add(captureRoleResult(role, super, bypass))
}

// captureRoleResult maps the role's attributes to the check. Pure.
func captureRoleResult(role string, super, bypass bool) doctor.CheckResult {
	var what []string
	if super {
		what = append(what, "a superuser")
	}
	if bypass {
		what = append(what, "can bypass row-level security (BYPASSRLS)")
	}
	if len(what) == 0 {
		return doctor.CheckResult{Name: captureRoleCheck, Status: doctor.StatusPass}
	}
	detail := fmt.Sprintf("The role %q is %s", role, strings.Join(what, ", and "))
	if !super {
		detail = fmt.Sprintf("The role %q %s", role, what[0])
	}
	return doctor.CheckResult{
		Name:   captureRoleCheck,
		Status: doctor.StatusWarn,
		Detail: detail,
		Remediation: "Unless the server has a forwarding user of its own, the PostgreSQL port sends what the copy does not answer to this database as this role: " +
			"read-only, and only a SELECT whose every table is a table of the copy, but with every row this role can read, those a row-level security policy hides included. " +
			"For less power, create a role that can only read, and set it as the server's forwarding user:\n\n" +
			"  CREATE ROLE dbtrail_ro LOGIN PASSWORD '<password>';\n" +
			"  GRANT USAGE ON SCHEMA <schema> TO dbtrail_ro;\n" +
			"  GRANT SELECT ON ALL TABLES IN SCHEMA <schema> TO dbtrail_ro;\n\n" +
			"  bintrail-console server forward-user <server> -u dbtrail_ro -p\n\n" +
			"Recording changes needs what this role has, so this does not stop the server from being added or started.",
	}
}
