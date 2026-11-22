package pgstreamrun

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/doctor"
	"github.com/dbtrail/dbtrail/internal/pgcapture"
)

// truncateCheck is the name of the check below.
const truncateCheck = "Publication publishes TRUNCATE"

// addTruncateCheck reports a publication that does not publish TRUNCATE
// (#3108, pgcapture.QueryPublishesTruncate). pgoutput then sends nothing for
// a TRUNCATE, and the rows it removed stay in the copy and in every rebuild.
//
// A warning, never a failure, as readAccessCheck is and for the same reason:
// this report is what Start reads, and every row change is captured without
// TRUNCATE, so what is missing for one record must not stop capture. A
// missing insert, update or delete is "Publication coverage"'s failure.
//
// It does not depend on wal_level: it reads the publication's catalog row.
func addTruncateCheck(ctx context.Context, r *doctor.Report, conn *pgx.Conn, publication string) {
	exists, publishes, err := pgcapture.QueryPublishesTruncate(ctx, conn, publication)
	if err != nil {
		r.Add(doctor.CheckResult{Name: truncateCheck, Status: doctor.StatusWarn, Detail: "could not check: " + err.Error()})
		return
	}
	r.Add(truncateResult(publication, exists, publishes))
}

// truncateResult maps what the publication publishes to the check. Pure.
func truncateResult(publication string, exists, publishes bool) doctor.CheckResult {
	switch {
	case !exists:
		// "Publication coverage" fails for a missing publication.
		return doctor.CheckResult{Name: truncateCheck, Status: doctor.StatusSkip, Detail: "the publication does not exist"}
	case publishes:
		return doctor.CheckResult{Name: truncateCheck, Status: doctor.StatusPass}
	}
	problem := pgcapture.TruncateNotPublishedProblem(publication)
	return doctor.CheckResult{
		Name:   truncateCheck,
		Status: doctor.StatusWarn,
		Detail: strings.ToUpper(problem[:1]) + problem[1:],
		Remediation: "Inserts, updates and deletes are captured without it, so this does not stop the server from being added or started. " +
			"A snapshot taken after a TRUNCATE starts from what the table holds then; a rebuild of a moment before that snapshot still shows the removed rows. " +
			"DBTrail changes nothing in the publication by itself. Run this on the database, as the publication's owner or a superuser " +
			"(it changes only which operations are published, and keeps the publication's tables and its other options):\n\n" +
			"  " + pgcapture.TruncateFixStatement(publication) + "\n",
	}
}
