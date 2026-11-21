package pgstreamrun

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/doctor"
	"github.com/dbtrail/dbtrail/internal/pgcapture"
)

// replicaIdentityCheck is the name of the check on tables below REPLICA
// IDENTITY FULL. It was a failure while capture needed FULL; it is a warning
// now, under the same name, because other code keys on it
// (dependentCheckNames, and whatever read the report before).
const replicaIdentityCheck = "REPLICA IDENTITY FULL"

// identityKeyCheck is the name of the check on tables whose replica identity
// leaves a primary-key column out: the one kind of table below FULL that is
// not captured.
const identityKeyCheck = "Replica identity holds the primary key"

// identityStatementsShown bounds how many statements a check prints; a
// publication of hundreds of tables says the rest in a line.
const identityStatementsShown = 20

// addReplicaIdentityChecks reports the published tables that are not at
// REPLICA IDENTITY FULL (#3027), as two checks.
//
// FULL is needed for one thing: undo. A table below it is recorded, and
// everything else works on it; so replicaIdentityCheck is a warning, never a
// failure. This report is also what `server add` and Start read, and a
// failure there keeps capture from starting: what is missing for one function
// (undo) must not stop another (capture).
//
// identityKeyCheck is a failure: capture does not record such a table at all
// (pgcapture.SkipReplicaIdentityIndexWithoutPrimaryKey). As a failure it
// stops `server add` and Start for the whole server until the table is fixed
// or out of the publication, which is said in its text; a capture that is
// running when a table becomes one skips that table alone.
func addReplicaIdentityChecks(ctx context.Context, r *doctor.Report, conn *pgx.Conn, publication string) {
	ids, err := pgcapture.QueryTableIdentities(ctx, conn, publication)
	if err != nil {
		r.Add(doctor.CheckResult{Name: replicaIdentityCheck, Status: doctor.StatusWarn, Detail: "could not check: " + err.Error()})
		r.Add(doctor.CheckResult{Name: identityKeyCheck, Status: doctor.StatusWarn, Detail: "could not check: " + err.Error()})
		return
	}
	withoutUndo, key := replicaIdentityResults(ids)
	r.Add(withoutUndo)
	r.Add(key)
}

// replicaIdentityResults maps the tables below FULL to the two checks. Pure.
func replicaIdentityResults(ids []pgcapture.TableIdentity) (withoutUndo, key doctor.CheckResult) {
	undo, missesKey, noIdentity := pgcapture.SplitTableIdentities(ids)
	return withoutUndoResult(undo, noIdentity), identityKeyResult(missesKey)
}

func quotedTable(t pgcapture.TableIdentity) string {
	return pgx.Identifier{t.Schema, t.Table}.Sanitize()
}

// tableNames lists the tables, quoted, at most identityStatementsShown of
// them: a publication of hundreds of tables is not printed whole.
func tableNames(ts []pgcapture.TableIdentity) string {
	var names []string
	for i, t := range ts {
		if i == identityStatementsShown {
			names = append(names, fmt.Sprintf("and %d more", len(ts)-i))
			break
		}
		name := quotedTable(t)
		if n := len(t.PartitionsBelowFull); n > 0 {
			name += fmt.Sprintf(" (%d of its partitions)", n)
		}
		names = append(names, name)
	}
	return strings.Join(names, ", ")
}

func tableCount(n int) string {
	if n == 1 {
		return "1 table"
	}
	return fmt.Sprintf("%d tables", n)
}

// identityStatements writes one statement per table, at most
// identityStatementsShown of them, and says how many it left out. The
// statements are the only indented lines: those are what a screen offers to
// copy.
func identityStatements(b *strings.Builder, stmts []string) {
	for i, stmt := range stmts {
		if i == identityStatementsShown {
			fmt.Fprintf(b, "\nThe same statement applies to the other %s.\n", tableCount(len(stmts)-i))
			break
		}
		b.WriteString("  " + stmt + "\n")
	}
}

func withoutUndoResult(undo, noIdentity []pgcapture.TableIdentity) doctor.CheckResult {
	if len(undo)+len(noIdentity) == 0 {
		return doctor.CheckResult{Name: replicaIdentityCheck, Status: doctor.StatusPass}
	}
	all := append(append([]pgcapture.TableIdentity{}, undo...), noIdentity...)
	isAre := "are"
	if len(all) == 1 {
		isAre = "is"
	}
	// The report prints a detail inside parentheses, so it ends without a
	// full stop (as readAccessResult's does).
	detail := fmt.Sprintf("%s of the publication %s not at REPLICA IDENTITY FULL: %s", tableCount(len(all)), isAre, tableNames(all))
	if len(all) == 1 && all[0].Identity == "f" {
		detail = fmt.Sprintf("1 partitioned table of the publication has partitions that are not at REPLICA IDENTITY FULL: %s", tableNames(all))
	}

	var rem strings.Builder
	rem.WriteString("FULL is optional: it is needed only for undo. The changes of these tables are recorded, and everything else works on them: " +
		"history, the copy and the queries it answers, snapshots and their updates. This does not stop the server from being added or started. " +
		"The exception is undo: PostgreSQL sends only the key of the row an UPDATE or a DELETE changed, so DBTrail does not have the earlier row to put back, " +
		"and refuses to undo those changes. Undo of an INSERT works. To make a table's changes undoable, run this on the database:\n\n")
	var stmts []string
	partitioned := 0
	for _, t := range all {
		// Identity "f" is a partitioned table at FULL listed for its
		// partitions alone.
		if t.Identity != "f" {
			stmts = append(stmts, "ALTER TABLE "+quotedTable(t)+" REPLICA IDENTITY FULL;")
		}
		for _, part := range t.PartitionsBelowFull {
			stmts = append(stmts, "ALTER TABLE "+part+" REPLICA IDENTITY FULL;")
		}
		if len(t.PartitionsBelowFull) > 0 {
			partitioned++
		}
	}
	identityStatements(&rem, stmts)
	rem.WriteString("\nIt takes effect for changes made after it, not for earlier ones. It costs WAL: the table then logs the whole earlier row for every UPDATE and DELETE.")
	if partitioned > 0 {
		rem.WriteString(" REPLICA IDENTITY is set per partition, and the statement on a partitioned table does not reach its partitions: " +
			"the table and each of its partitions need it, which is why the partitions are listed above.")
	}
	if len(noIdentity) > 0 {
		have := "have"
		if len(noIdentity) == 1 {
			have = "has"
		}
		fmt.Fprintf(&rem, "\n\n%s of them %s no replica identity at all (REPLICA IDENTITY NOTHING, or no usable primary key): %s. "+
			"PostgreSQL itself refuses UPDATE and DELETE on such a table while it is in a publication, with or without DBTrail, so only its INSERT and TRUNCATE are recorded. "+
			"Give the table a primary key, or run the statement above for it.",
			tableCount(len(noIdentity)), have, tableNames(noIdentity))
	}
	return doctor.CheckResult{
		Name:        replicaIdentityCheck,
		Status:      doctor.StatusWarn,
		Detail:      detail,
		Remediation: rem.String(),
	}
}

func identityKeyResult(missesKey []pgcapture.TableIdentity) doctor.CheckResult {
	if len(missesKey) == 0 {
		return doctor.CheckResult{Name: identityKeyCheck, Status: doctor.StatusPass}
	}
	parts := make([]string, len(missesKey))
	for i, t := range missesKey {
		parts[i] = fmt.Sprintf("%s (primary-key column %s left out)", quotedTable(t), strings.Join(t.MissesKey, ", "))
	}
	has := "have"
	if len(missesKey) == 1 {
		has = "has"
	}
	detail := fmt.Sprintf("%s of the publication %s a replica identity that does not hold the primary key: %s", tableCount(len(missesKey)), has, strings.Join(parts, ", "))

	var rem strings.Builder
	rem.WriteString("A server is not added or started while this check fails. A capture that is already running does not record the changes of these tables " +
		"(each one is counted as skipped in capture health) and records the other tables normally. " +
		"For an UPDATE of the primary key PostgreSQL would send no earlier row, and for a DELETE an earlier row without the primary key, " +
		"so a change could not be matched to the row it changed. Run this on the database:\n\n")
	stmts := make([]string, len(missesKey))
	for i, t := range missesKey {
		stmts[i] = "ALTER TABLE " + quotedTable(t) + " REPLICA IDENTITY FULL;"
		if t.Identity == "i" {
			stmts[i] = "ALTER TABLE " + quotedTable(t) + " REPLICA IDENTITY DEFAULT;"
		}
	}
	identityStatements(&rem, stmts)
	rem.WriteString("\nREPLICA IDENTITY DEFAULT sends the primary key. REPLICA IDENTITY FULL works for every table and also makes its changes undoable, at the cost of more WAL. " +
		"Recording of the table starts by itself after the statement; the changes skipped before it stay missing until the next full copy of the table.")
	return doctor.CheckResult{
		Name:        identityKeyCheck,
		Status:      doctor.StatusFail,
		Detail:      detail,
		Remediation: rem.String(),
	}
}
