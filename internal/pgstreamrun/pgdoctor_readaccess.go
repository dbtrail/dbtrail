package pgstreamrun

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/doctor"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/pgcapture"
)

// readAccessCheck is the name of the check below.
const readAccessCheck = "Read access to the published tables"

// addReadAccessCheck reports the published tables the connection's role
// cannot read (#3022, pgcapture.QueryReadAccess). Capture needs no SELECT and
// the first copy does, so without this every check passed for a role that
// the first snapshot then stopped on with 42501.
//
// A warning, never a failure. This report is also what Start reads, and a
// failure there keeps capture from starting: a role that replicates and
// cannot read records changes today, and what is missing for one function
// (the copy) must not stop another (capture). The copy is where it is
// refused, before it creates or reads anything (pgbaseline.Run). Here it is
// said early, with the statements to run.
//
// It does not depend on wal_level: the publication and the privileges are
// there to read whatever the WAL carries.
func addReadAccessCheck(ctx context.Context, r *doctor.Report, conn *pgx.Conn, publication string, filters event.Filters) {
	acc, err := pgcapture.QueryReadAccess(ctx, conn, publication, filters)
	if err != nil {
		r.Add(doctor.CheckResult{Name: readAccessCheck, Status: doctor.StatusWarn, Detail: "could not check: " + err.Error()})
		return
	}
	r.Add(readAccessResult(acc))
}

// readAccessResult maps what the role can read to the check. Pure.
func readAccessResult(acc pgcapture.ReadAccess) doctor.CheckResult {
	switch {
	case acc.Tables == 0:
		// Not a pass: nothing was asked. A publication that is missing, or
		// holds none of the tables in scope, is "Publication coverage"'s to
		// report, and the copy refuses an empty one.
		return doctor.CheckResult{Name: readAccessCheck, Status: doctor.StatusSkip,
			Detail: "the publication holds no table the first copy would read, so there was nothing to check"}
	case len(acc.Unreadable) == 0:
		return doctor.CheckResult{Name: readAccessCheck, Status: doctor.StatusPass}
	}
	problem := acc.Problem()
	var rem strings.Builder
	rem.WriteString("Recording changes does not need these permissions, so this does not stop the server from being added or started. " +
		"The first copy of the tables needs them: it reads each one as this role, and DBTrail refuses to start it until they are there. " +
		"DBTrail grants nothing by itself. Run this on the database, as the tables' owner or a superuser:\n\n")
	for _, g := range acc.Grants() {
		rem.WriteString("  " + g + "\n")
	}
	if acc.WholeSchemas() {
		rem.WriteString("\nThat is one statement per schema and not one per table, since many tables lack SELECT: " +
			"it gives SELECT on every table of the schema, published or not.")
	}
	// The report prints a detail inside parentheses, so it ends without a
	// full stop. No Kind: a screen draws a finding that has one from its own
	// short text and the statements alone, and this one needs its prose too
	// (why the permission is needed, and that a statement on a whole schema
	// gives more than the publication).
	detail := strings.TrimSuffix(strings.ToUpper(problem[:1])+problem[1:], ".")
	return doctor.CheckResult{
		Name:        readAccessCheck,
		Status:      doctor.StatusWarn,
		Detail:      detail,
		Remediation: rem.String(),
	}
}
