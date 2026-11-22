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
// IDENTITY FULL. A failure: DBTrail captures a table only at FULL (the
// decoder's gate, pgcapture.SkipReplicaIdentityNotFull). Its name is the one
// it had while it was a warning (version 0.138.0) and before, because other
// code keys on it (dependentCheckNames, and whatever read the report before).
const replicaIdentityCheck = "REPLICA IDENTITY FULL"

// identityKeyCheck is the name of the check on tables whose replica identity
// leaves a primary-key column out. Such a table is below FULL, so it is in
// replicaIdentityCheck too, with the same statement; this check says the
// second thing wrong with it.
const identityKeyCheck = "Replica identity holds the primary key"

// identityStatementsShown bounds how many statements a check prints; a
// publication of hundreds of tables says the rest in a line.
const identityStatementsShown = 20

// addReplicaIdentityChecks reports the published tables that are not at
// REPLICA IDENTITY FULL, as two checks, both failures when they find a table.
//
// As failures they stop `server add` and Start for the whole server until
// the tables are fixed or out of the publication, which is said in their
// text: that is the moment the operator can still fix them before anything is
// missed. A capture that is already running, or that the daemon resumes at
// boot (which does not run these checks), skips such a table alone and
// records the others.
//
// On PostgreSQL 18 the generated columns of those tables are read too: at
// FULL, one the publication does not publish makes PostgreSQL refuse the
// application's UPDATE and DELETE, so the statement given for such a table
// is not ALTER TABLE ... FULL alone (notFullResult).
func addReplicaIdentityChecks(ctx context.Context, r *doctor.Report, conn *pgx.Conn, publication string) {
	ids, err := pgcapture.QueryTableIdentities(ctx, conn, publication)
	if err != nil {
		r.Add(doctor.CheckResult{Name: replicaIdentityCheck, Status: doctor.StatusWarn, Detail: "could not check: " + err.Error()})
		r.Add(doctor.CheckResult{Name: identityKeyCheck, Status: doctor.StatusWarn, Detail: "could not check: " + err.Error()})
		return
	}
	var gen []pgcapture.UnpublishedGenerated
	var genErr error
	if len(ids) > 0 {
		gen, genErr = pgcapture.QueryUnpublishedGeneratedBelowFull(ctx, conn, publication)
		// A partitioned table at FULL is listed for its partitions: its own
		// generated columns are read with the tables at FULL, and kept for
		// the tables listed here (the partitions inherit them).
		listed := map[[2]string]bool{}
		for _, t := range ids {
			if t.Identity == "f" {
				listed[[2]string{t.Schema, t.Table}] = true
			}
		}
		if genErr == nil && len(listed) > 0 {
			var atFull []pgcapture.UnpublishedGenerated
			atFull, genErr = pgcapture.QueryUnpublishedGenerated(ctx, conn, publication)
			for _, g := range atFull {
				if listed[[2]string{g.Schema, g.Table}] {
					gen = append(gen, g)
				}
			}
		}
	}
	full, key := replicaIdentityResults(ids, gen, genErr)
	r.Add(full)
	r.Add(key)
}

// replicaIdentityResults maps the tables below FULL to the two checks; gen
// are the generated columns of those tables that a publication does not
// publish (PostgreSQL 18), genErr why they could not be read. Pure.
func replicaIdentityResults(ids []pgcapture.TableIdentity, gen []pgcapture.UnpublishedGenerated, genErr error) (full, key doctor.CheckResult) {
	_, missesKey, _ := pgcapture.SplitTableIdentities(ids)
	return notFullResult(ids, gen, genErr), identityKeyResult(missesKey)
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

func partitionCount(n int) string {
	if n == 1 {
		return "1 partition"
	}
	return fmt.Sprintf("%d partitions", n)
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

func notFullResult(all []pgcapture.TableIdentity, gen []pgcapture.UnpublishedGenerated, genErr error) doctor.CheckResult {
	if len(all) == 0 {
		return doctor.CheckResult{Name: replicaIdentityCheck, Status: doctor.StatusPass}
	}
	// The report prints a detail inside parentheses, so it ends without a
	// full stop (as readAccessResult's does).
	isAre := "are"
	if len(all) == 1 {
		isAre = "is"
	}
	detail := fmt.Sprintf("%s of the publication %s not at REPLICA IDENTITY FULL, so %s not captured: %s",
		tableCount(len(all)), isAre, itsTheir(len(all)), tableNames(all))
	if len(all) == 1 && all[0].Identity == "f" {
		detail = fmt.Sprintf("1 partitioned table of the publication has partitions that are not at REPLICA IDENTITY FULL, so its changes are not captured: %s", tableNames(all))
	}

	// PostgreSQL 18: a generated column a publication does not publish.
	type tbl struct{ schema, table string }
	storedPubs := map[tbl][]string{}
	virtualCols := map[tbl][]string{}
	virtualPubs := map[tbl][]string{}
	for _, g := range gen {
		k := tbl{g.Schema, g.Table}
		if g.Virtual {
			virtualCols[k] = appendNew(virtualCols[k], g.Column)
			virtualPubs[k] = appendNew(virtualPubs[k], g.Publication)
		} else {
			storedPubs[k] = appendNew(storedPubs[k], g.Publication)
		}
	}

	var rem strings.Builder
	rem.WriteString("DBTrail captures a table only at REPLICA IDENTITY FULL, where PostgreSQL sends the whole earlier row of an UPDATE and a DELETE. " +
		"A server is not added or started while this check fails. A capture that is already running does not record the changes of these tables " +
		"(each one is counted as skipped in capture health), and records the other tables normally. Run this on the database:\n\n")
	var stmts []string
	partitioned := 0
	var virtual []string
	for _, t := range all {
		k := tbl{t.Schema, t.Table}
		if len(virtualCols[k]) > 0 {
			// No statement makes such a table capturable: see below.
			virtual = append(virtual, quotedTable(t))
			continue
		}
		for _, p := range storedPubs[k] {
			stmts = appendNew(stmts, "ALTER PUBLICATION "+pgx.Identifier{p}.Sanitize()+" SET (publish_generated_columns = stored);")
		}
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
	if genErr != nil {
		// Not one statement: on PostgreSQL 18 one of them could stop the
		// source's own application from writing (see below).
		fmt.Fprintf(&rem, "No statement is given here: the generated columns of these tables could not be read (%v); "+
			"on PostgreSQL 18, ALTER TABLE ... REPLICA IDENTITY FULL on a table with a generated column that a publication does not publish "+
			"makes PostgreSQL refuse the application's UPDATE and DELETE. Run the check again.\n", genErr)
	} else {
		identityStatements(&rem, stmts)
	}
	rem.WriteString("\nCapture of a table starts by itself after the statement, with no restart. The changes made before it are not recorded: " +
		"the table's copy is brought up to date by its next full read, and its history of that stretch stays missing. " +
		"FULL costs WAL: the table then logs the whole earlier row for every UPDATE and DELETE.")
	if partitioned > 0 {
		rem.WriteString(" REPLICA IDENTITY is set per partition, and the statement on a partitioned table does not reach its partitions: " +
			"the table and each of its partitions need it, which is why the partitions are listed above. " +
			"A partitioned table is captured again once PostgreSQL describes it with every partition at FULL: before the first change made after the statement in the partition the change is in, and at the latest when capture restarts.")
	}
	var noIdentity []pgcapture.TableIdentity
	for _, t := range all {
		if t.NoIdentity {
			noIdentity = append(noIdentity, t)
		}
	}
	if len(noIdentity) > 0 {
		have := "have"
		if len(noIdentity) == 1 {
			have = "has"
		}
		fmt.Fprintf(&rem, "\n\n%s of them %s no replica identity at all (REPLICA IDENTITY NOTHING, or no usable primary key): %s. "+
			"PostgreSQL itself refuses UPDATE and DELETE on such a table while it is in a publication, with or without DBTrail, and DBTrail does not record its INSERTs either until it is at FULL. "+
			"FULL needs no primary key.",
			tableCount(len(noIdentity)), have, tableNames(noIdentity))
	}
	if len(storedPubs) > 0 {
		rem.WriteString("\n\nOn PostgreSQL 18, a table with a STORED generated column that a publication does not publish refuses the application's UPDATE and DELETE once it is at FULL. " +
			"That is why the statements above make each such publication publish its stored generated columns first; run them in that order.")
	}
	if len(virtual) > 0 {
		var cols, drops []string
		for _, t := range all {
			k := tbl{t.Schema, t.Table}
			for _, c := range virtualCols[k] {
				cols = append(cols, pgx.Identifier{t.Schema, t.Table, c}.Sanitize())
			}
			for _, p := range virtualPubs[k] {
				drops = append(drops, "ALTER PUBLICATION "+pgx.Identifier{p}.Sanitize()+" DROP TABLE "+quotedTable(t)+";")
			}
		}
		fmt.Fprintf(&rem, "\n\nOn PostgreSQL 18, %s %s a VIRTUAL generated column (%s), which no publication can publish. "+
			"PostgreSQL refuses UPDATE and DELETE on a table at FULL that has one, so DBTrail cannot capture %s while %s that column, and no statement above is given for %s: "+
			"do not run ALTER TABLE ... REPLICA IDENTITY FULL on %s, or the application's UPDATE and DELETE fail. "+
			"To capture it, replace the column with a STORED generated one and publish it (publish_generated_columns = stored). "+
			"Or take it out of the publication, and this check stops listing it: %s",
			strings.Join(virtual, ", "), hasOrHaveN(len(virtual)), strings.Join(cols, ", "),
			itOrThem(len(virtual)), itHasOrTheyHave(len(virtual)), itOrThem(len(virtual)), itOrThem(len(virtual)), strings.Join(drops, " "))
		rem.WriteString(" (a FOR ALL TABLES publication has no way to leave one table out: use a publication that lists its tables).")
	}
	return doctor.CheckResult{
		Name:        replicaIdentityCheck,
		Status:      doctor.StatusFail,
		Detail:      detail,
		Remediation: rem.String(),
	}
}

func appendNew(list []string, s string) []string {
	for _, x := range list {
		if x == s {
			return list
		}
	}
	return append(list, s)
}

func itsTheir(n int) string {
	if n == 1 {
		return "its changes are"
	}
	return "their changes are"
}

func itOrThem(n int) string {
	if n == 1 {
		return "it"
	}
	return "them"
}

func hasOrHaveN(n int) string {
	if n == 1 {
		return "has"
	}
	return "have"
}

func itHasOrTheyHave(n int) string {
	if n == 1 {
		return "it has"
	}
	return "they have"
}

func identityKeyResult(missesKey []pgcapture.TableIdentity) doctor.CheckResult {
	if len(missesKey) == 0 {
		return doctor.CheckResult{Name: identityKeyCheck, Status: doctor.StatusPass}
	}
	parts := make([]string, len(missesKey))
	for i, t := range missesKey {
		parts[i] = fmt.Sprintf("%s (primary-key column %s left out)", quotedTable(t), strings.Join(t.MissesKey, ", "))
		if len(t.MissesKey) == 0 {
			parts[i] = fmt.Sprintf("%s (in %s: %s)", quotedTable(t), partitionCount(len(t.PartitionsMissKey)), strings.Join(t.PartitionsMissKey, ", "))
		}
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
	var stmts []string
	for _, t := range missesKey {
		if len(t.MissesKey) > 0 {
			stmts = append(stmts, "ALTER TABLE "+quotedTable(t)+" REPLICA IDENTITY FULL;")
		}
		// A partition with an identity of its own that leaves the key out:
		// the statement is the partition's.
		for _, part := range t.PartitionsMissKey {
			stmts = append(stmts, "ALTER TABLE "+part+" REPLICA IDENTITY FULL;")
		}
	}
	identityStatements(&rem, stmts)
	rem.WriteString("\nThe statements are the ones of the check \"" + replicaIdentityCheck + "\": FULL sends the whole row, the primary key included, and DBTrail captures a table only at FULL " +
		"(REPLICA IDENTITY DEFAULT would send the primary key, but the table would still not be captured). " +
		"Recording of the table starts by itself after the statement. The changes skipped before it are never recorded: the next full copy of the table brings its rows up to date, and the history of that stretch stays missing.")
	return doctor.CheckResult{
		Name:        identityKeyCheck,
		Status:      doctor.StatusFail,
		Detail:      detail,
		Remediation: rem.String(),
	}
}
