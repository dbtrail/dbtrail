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

// The PostgreSQL port forwards as the user DBTrail connects with when the
// server says yes and has no forwarding user of its own (0.140.0: it serves
// a server only when it can forward). Behind its gate that is a read-only
// SELECT whose every table is a plain table of the copy, never one under
// row-level security; still, a superuser, or a role that bypasses
// row-level security, is more power than that gate should have to stand
// in front of. Said here, at `server add` and in every check, and when the
// yes is saved, with the way to a role with less. A warning, never a
// failure.

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
		Remediation: "When the server forwards as this role (the answer yes about the capture account, and no forwarding user of its own), the PostgreSQL port sends it, read-only, " +
			"only a SELECT whose every table is a plain table of the copy; a table under row-level security is never sent. " +
			"A role with less power leaves less behind that check: give DBTrail a role that can only replicate and read the published tables, " +
			"as in \"Create a replication user\" of the PostgreSQL guide, and point the server's source at it:\n\n" +
			"  CREATE ROLE dbtrail_repl LOGIN REPLICATION PASSWORD '<password>';\n" +
			"  GRANT USAGE ON SCHEMA <schema> TO dbtrail_repl;\n" +
			"  GRANT SELECT ON <each published table> TO dbtrail_repl;\n\n" +
			"A forwarding user of its own is not held to that check: it reads whatever its own grants allow. " +
			"This does not stop the server from being added or started.",
	}
}

// CaptureRoleWarning is the warning above for the role dsn logs in as, ""
// when there is none: what saving the yes about the capture account says.
func CaptureRoleWarning(ctx context.Context, dsn string) (string, error) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return "", err
	}
	defer conn.Close(context.Background())
	var role string
	var super, bypass bool
	if err := conn.QueryRow(ctx, captureRoleQuery).Scan(&role, &super, &bypass); err != nil {
		return "", err
	}
	res := captureRoleResult(role, super, bypass)
	if res.Status != doctor.StatusWarn {
		return "", nil
	}
	return res.Detail + ". " + res.Remediation, nil
}
