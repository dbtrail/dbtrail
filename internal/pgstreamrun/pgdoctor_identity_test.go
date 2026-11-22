package pgstreamrun

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/doctor"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/pgcapture"
)

// What `server add`, Start and `bintrail-pg doctor` say about tables that are
// not at REPLICA IDENTITY FULL: a failure, since DBTrail captures a table only
// at FULL, with the statement that fixes each table, and never one that would
// stop the source's own application from writing (PostgreSQL 18).
// The doctor asks only of the tables capture takes: a table outside the
// server's schemas or tables is not skipped by capture and must not keep the
// server from being added.
func TestInScopeIdentities(t *testing.T) {
	ids := []pgcapture.TableIdentity{{Schema: "public", Table: "a", Identity: "d"}, {Schema: "other", Table: "b", Identity: "d"}}
	got := inScopeIdentities(ids, event.Filters{Schemas: map[string]bool{"public": true}})
	if len(got) != 1 || got[0].Table != "a" {
		t.Errorf("got %+v", got)
	}
	if got := inScopeIdentities(ids, event.Filters{}); len(got) != 2 {
		t.Errorf("no filter keeps every table: %+v", got)
	}
}

// The generated-columns check, too, asks only of the tables capture takes.
func TestGeneratedInScope(t *testing.T) {
	all := []pgcapture.UnpublishedGenerated{{Schema: "public", Table: "a", Column: "g"}, {Schema: "other", Table: "b", Column: "g"}}
	if got := generatedInScope(all, event.Filters{Schemas: map[string]bool{"public": true}}); len(got) != 1 || got[0].Table != "a" {
		t.Errorf("got %+v", got)
	}
	if got := generatedInScope(all, event.Filters{}); len(got) != 2 {
		t.Errorf("no filter keeps every column: %+v", got)
	}
}

func TestReplicaIdentityResults(t *testing.T) {
	def := pgcapture.TableIdentity{Schema: "public", Table: "orders", Identity: "d"}
	idx := pgcapture.TableIdentity{Schema: "Sales Data", Table: `Order "Items"`, Identity: "i"}
	nothing := pgcapture.TableIdentity{Schema: "public", Table: "log", Identity: "n", NoIdentity: true}
	idxNoKey := pgcapture.TableIdentity{Schema: "public", Table: "accounts", Identity: "i", MissesKey: []string{"id"}}
	include := pgcapture.TableIdentity{Schema: "public", Table: "wide", Identity: "d", MissesKey: []string{"extra"}}

	const tail = "\nCapture of a table starts by itself after the statement, with no restart. The changes made before it are not recorded: " +
		"the table's copy is brought up to date by its next full read, and its history of that stretch stays missing. " +
		"FULL costs WAL: the table then logs the whole earlier row for every UPDATE and DELETE."

	t.Run("every table at FULL", func(t *testing.T) {
		full, key := replicaIdentityResults(nil, nil, nil)
		for _, got := range []doctor.CheckResult{full, key} {
			if got.Status != doctor.StatusPass || got.Detail != "" || got.Remediation != "" {
				t.Errorf("got %+v, want a bare pass", got)
			}
		}
		if full.Name != "REPLICA IDENTITY FULL" || key.Name != "Replica identity holds the primary key" {
			t.Errorf("names %q, %q: other code keys on the first one", full.Name, key.Name)
		}
	})

	t.Run("a table at DEFAULT is a failure", func(t *testing.T) {
		full, key := replicaIdentityResults([]pgcapture.TableIdentity{def}, nil, nil)
		t.Logf("check: %s\n%s\n%s", full.Name, full.Detail, full.Remediation)
		want := doctor.CheckResult{
			Name:   "REPLICA IDENTITY FULL",
			Status: doctor.StatusFail,
			Detail: `1 table of the publication is not at REPLICA IDENTITY FULL, so its changes are not captured: "public"."orders"`,
			Remediation: "DBTrail captures a table only at REPLICA IDENTITY FULL, where PostgreSQL sends the whole earlier row of an UPDATE and a DELETE. " +
				"A server is not added or started while this check fails. A capture that is already running does not record the changes of these tables " +
				"(each one is counted as skipped in capture health), and records the other tables normally. Run this on the database:\n\n" +
				"  ALTER TABLE \"public\".\"orders\" REPLICA IDENTITY FULL;\n" + tail,
		}
		if full.Name != want.Name || full.Status != want.Status || full.Detail != want.Detail || full.Remediation != want.Remediation ||
			full.Optional || full.Kind != "" {
			t.Errorf("got  %+v\nwant %+v", full, want)
		}
		if key.Status != doctor.StatusPass {
			t.Errorf("the key check must pass: %+v", key)
		}
		for _, gone := range []string{"optional", "undo", "REPLICA IDENTITY DEFAULT"} {
			if strings.Contains(full.Detail+full.Remediation, gone) {
				t.Errorf("the check still says %q", gone)
			}
		}
	})

	t.Run("a table with no identity is said apart, in the same failure", func(t *testing.T) {
		full, key := replicaIdentityResults([]pgcapture.TableIdentity{def, nothing, idx}, nil, nil)
		t.Logf("%s\n%s", full.Detail, full.Remediation)
		if full.Status != doctor.StatusFail || key.Status != doctor.StatusPass {
			t.Fatalf("statuses %s, %s", full.Status, key.Status)
		}
		if want := `3 tables of the publication are not at REPLICA IDENTITY FULL, so their changes are not captured: "public"."orders", "public"."log", "Sales Data"."Order ""Items"""`; full.Detail != want {
			t.Errorf("detail %q\nwant   %q", full.Detail, want)
		}
		for _, want := range []string{
			"  ALTER TABLE \"Sales Data\".\"Order \"\"Items\"\"\" REPLICA IDENTITY FULL;\n",
			"  ALTER TABLE \"public\".\"log\" REPLICA IDENTITY FULL;\n",
			"\n\n1 table of them has no replica identity at all (REPLICA IDENTITY NOTHING, or no usable primary key): \"public\".\"log\". " +
				"PostgreSQL itself refuses UPDATE and DELETE on such a table while it is in a publication, with or without DBTrail, and DBTrail does not record its INSERTs either until it is at FULL. " +
				"FULL needs no primary key.",
		} {
			if !strings.Contains(full.Remediation, want) {
				t.Errorf("remediation lacks %q:\n%s", want, full.Remediation)
			}
		}
	})

	t.Run("an identity without the primary key fails both checks, with one statement", func(t *testing.T) {
		full, key := replicaIdentityResults([]pgcapture.TableIdentity{idxNoKey, include}, nil, nil)
		t.Logf("check: %s\n%s\n%s", key.Name, key.Detail, key.Remediation)
		if full.Status != doctor.StatusFail || !strings.Contains(full.Remediation, "  ALTER TABLE \"public\".\"accounts\" REPLICA IDENTITY FULL;\n") {
			t.Errorf("a table below FULL is in the FULL check whatever else is wrong with it: %+v", full)
		}
		want := doctor.CheckResult{
			Name:   "Replica identity holds the primary key",
			Status: doctor.StatusFail,
			Detail: `2 tables of the publication have a replica identity that does not hold the primary key: "public"."accounts" (primary-key column id left out), "public"."wide" (primary-key column extra left out)`,
			Remediation: "A server is not added or started while this check fails. A capture that is already running does not record the changes of these tables " +
				"(each one is counted as skipped in capture health) and records the other tables normally. " +
				"For an UPDATE of the primary key PostgreSQL would send no earlier row, and for a DELETE an earlier row without the primary key, " +
				"so a change could not be matched to the row it changed. Run this on the database:\n\n" +
				"  ALTER TABLE \"public\".\"accounts\" REPLICA IDENTITY FULL;\n" +
				"  ALTER TABLE \"public\".\"wide\" REPLICA IDENTITY FULL;\n" +
				"\nThe statements are the ones of the check \"REPLICA IDENTITY FULL\": FULL sends the whole row, the primary key included, and DBTrail captures a table only at FULL " +
				"(REPLICA IDENTITY DEFAULT would send the primary key, but the table would still not be captured). " +
				"Recording of the table starts by itself after the statement. The changes skipped before it are never recorded: the next full copy of the table brings its rows up to date, and the history of that stretch stays missing.",
		}
		if key.Name != want.Name || key.Status != want.Status || key.Detail != want.Detail || key.Remediation != want.Remediation {
			t.Errorf("got  %+v\nwant %+v", key, want)
		}
	})

	// REPLICA IDENTITY is set per partition: a partitioned table at FULL whose
	// partitions are not is not captured, and the statement that fixes it is
	// one per partition.
	t.Run("a partitioned table: the partitions are what needs the statement", func(t *testing.T) {
		root := pgcapture.TableIdentity{Schema: "public", Table: "events", Identity: "f",
			PartitionsBelowFull: []string{`"public"."events_2026_01"`, `"public"."events_2026_02"`}}
		full, key := replicaIdentityResults([]pgcapture.TableIdentity{root}, nil, nil)
		t.Logf("%s\n%s", full.Detail, full.Remediation)
		if full.Status != doctor.StatusFail || key.Status != doctor.StatusPass {
			t.Fatalf("statuses %s, %s", full.Status, key.Status)
		}
		if want := `1 partitioned table of the publication has partitions that are not at REPLICA IDENTITY FULL, so its changes are not captured: "public"."events" (2 of its partitions)`; full.Detail != want {
			t.Errorf("detail %q\nwant   %q", full.Detail, want)
		}
		if !strings.Contains(full.Remediation, "\n\n  ALTER TABLE \"public\".\"events_2026_01\" REPLICA IDENTITY FULL;\n  ALTER TABLE \"public\".\"events_2026_02\" REPLICA IDENTITY FULL;\n\n") ||
			strings.Contains(full.Remediation, `ALTER TABLE "public"."events" `) {
			t.Errorf("the statements must be the partitions', and not the parent's (it is at FULL already):\n%s", full.Remediation)
		}
		if !strings.HasSuffix(full.Remediation, "REPLICA IDENTITY is set per partition, and the statement on a partitioned table does not reach its partitions: "+
			"the table and each of its partitions need it, which is why the partitions are listed above. "+
			"A partitioned table is captured again once PostgreSQL describes it with every partition at FULL: before the first change made after the statement in the partition the change is in, and at the latest when capture restarts.") {
			t.Errorf("remediation:\n%s", full.Remediation)
		}

		// Parent and partitions all below FULL: every one of them is listed.
		root.Identity = "d"
		full, _ = replicaIdentityResults([]pgcapture.TableIdentity{def, root}, nil, nil)
		if n := strings.Count(full.Remediation, "ALTER TABLE "); n != 4 {
			t.Errorf("%d statements, want 4 (orders, events, and its two partitions):\n%s", n, full.Remediation)
		}
		if want := `2 tables of the publication are not at REPLICA IDENTITY FULL, so their changes are not captured: "public"."orders", "public"."events" (2 of its partitions)`; full.Detail != want {
			t.Errorf("detail %q\nwant   %q", full.Detail, want)
		}
	})

	t.Run("a partition whose own identity leaves the key out fails for its table", func(t *testing.T) {
		root := pgcapture.TableIdentity{Schema: "public", Table: "events", Identity: "f",
			PartitionsBelowFull: []string{`"public"."events_1"`}, PartitionsMissKey: []string{`"public"."events_1"`}}
		full, key := replicaIdentityResults([]pgcapture.TableIdentity{root}, nil, nil)
		t.Logf("%s\n%s", key.Detail, key.Remediation)
		if full.Status != doctor.StatusFail || key.Status != doctor.StatusFail {
			t.Fatalf("statuses %s, %s", full.Status, key.Status)
		}
		if want := `1 table of the publication has a replica identity that does not hold the primary key: "public"."events" (in 1 partition: "public"."events_1")`; key.Detail != want {
			t.Errorf("detail %q\nwant   %q", key.Detail, want)
		}
		for _, rem := range []string{full.Remediation, key.Remediation} {
			if !strings.Contains(rem, "\n\n  ALTER TABLE \"public\".\"events_1\" REPLICA IDENTITY FULL;\n\n") || strings.Contains(rem, `ALTER TABLE "public"."events" `) {
				t.Errorf("the statement must be the partition's, at FULL:\n%s", rem)
			}
		}
	})

	// PostgreSQL 18: at FULL, a generated column that a publication does not
	// publish makes PostgreSQL refuse the application's UPDATE and DELETE.
	t.Run("PostgreSQL 18, a STORED generated column: the publication first", func(t *testing.T) {
		gen := []pgcapture.UnpublishedGenerated{{Schema: "public", Table: "orders", Column: "total", Publication: "pub"}}
		full, _ := replicaIdentityResults([]pgcapture.TableIdentity{def, idx}, gen, nil)
		t.Logf("%s\n%s", full.Detail, full.Remediation)
		pubAt := strings.Index(full.Remediation, "  ALTER PUBLICATION \"pub\" SET (publish_generated_columns = stored);\n")
		tblAt := strings.Index(full.Remediation, "  ALTER TABLE \"public\".\"orders\" REPLICA IDENTITY FULL;\n")
		if full.Status != doctor.StatusFail || pubAt < 0 || tblAt < 0 || pubAt > tblAt {
			t.Errorf("the publication must be told to publish the column before the table is put at FULL:\n%s", full.Remediation)
		}
		if !strings.Contains(full.Remediation, "On PostgreSQL 18, a table with a STORED generated column that a publication does not publish refuses the application's UPDATE and DELETE once it is at FULL.") {
			t.Errorf("remediation:\n%s", full.Remediation)
		}
	})

	t.Run("PostgreSQL 18, a VIRTUAL generated column: no statement that would stop the application", func(t *testing.T) {
		gen := []pgcapture.UnpublishedGenerated{
			{Schema: "public", Table: "orders", Column: "label", Virtual: true, Publication: "pub"},
			{Schema: "public", Table: "orders", Column: "label", Virtual: true, Publication: "other"},
		}
		full, _ := replicaIdentityResults([]pgcapture.TableIdentity{def, idx}, gen, nil)
		t.Logf("%s\n%s", full.Detail, full.Remediation)
		if strings.Contains(full.Remediation, `ALTER TABLE "public"."orders" REPLICA IDENTITY FULL;`) {
			t.Errorf("FULL must not be offered for a table with a VIRTUAL generated column:\n%s", full.Remediation)
		}
		for _, want := range []string{
			"  ALTER TABLE \"Sales Data\".\"Order \"\"Items\"\"\" REPLICA IDENTITY FULL;\n",
			"On PostgreSQL 18, \"public\".\"orders\" has a VIRTUAL generated column (\"public\".\"orders\".\"label\"), which no publication can publish. " +
				"PostgreSQL refuses UPDATE and DELETE on a table at FULL that has one, so DBTrail cannot capture it while it has that column, and no statement above is given for it: " +
				"do not run ALTER TABLE ... REPLICA IDENTITY FULL on it, or the application's UPDATE and DELETE fail.",
			`ALTER PUBLICATION "pub" DROP TABLE "public"."orders"; ALTER PUBLICATION "other" DROP TABLE "public"."orders";`,
		} {
			if !strings.Contains(full.Remediation, want) {
				t.Errorf("remediation lacks %q:\n%s", want, full.Remediation)
			}
		}
		// Still listed, still a failure: it is not captured.
		if full.Status != doctor.StatusFail || !strings.Contains(full.Detail, `"public"."orders"`) {
			t.Errorf("got %+v", full)
		}
	})

	t.Run("generated columns that could not be read: no statement at all", func(t *testing.T) {
		full, _ := replicaIdentityResults([]pgcapture.TableIdentity{def}, nil, errors.New("permission denied"))
		t.Logf("%s", full.Remediation)
		if full.Status != doctor.StatusFail || strings.Contains(full.Remediation, "ALTER TABLE \"") ||
			!strings.Contains(full.Remediation, "No statement is given here: the generated columns of these tables could not be read (permission denied)") {
			t.Errorf("remediation:\n%s", full.Remediation)
		}
		for line := range strings.SplitSeq(full.Remediation, "\n") {
			if strings.HasPrefix(line, " ") {
				t.Errorf("an indented line is offered as a statement to copy: %q", line)
			}
		}
	})

	t.Run("PostgreSQL 18, a partitioned table at FULL with a VIRTUAL column: no statement for its partitions", func(t *testing.T) {
		root := pgcapture.TableIdentity{Schema: "public", Table: "events", Identity: "f", PartitionsBelowFull: []string{`"public"."events_1"`}}
		gen := []pgcapture.UnpublishedGenerated{{Schema: "public", Table: "events", Column: "label", Virtual: true, Publication: "pub"}}
		full, _ := replicaIdentityResults([]pgcapture.TableIdentity{root}, gen, nil)
		if strings.Contains(full.Remediation, `ALTER TABLE "public"."events_1"`) || !strings.Contains(full.Remediation, `ALTER PUBLICATION "pub" DROP TABLE "public"."events";`) {
			t.Errorf("remediation:\n%s", full.Remediation)
		}
	})

	t.Run("hundreds of tables: bounded", func(t *testing.T) {
		var ids []pgcapture.TableIdentity
		for i := range 500 {
			ids = append(ids, pgcapture.TableIdentity{Schema: "public", Table: fmt.Sprintf("t%03d", i), Identity: "d"})
		}
		full, _ := replicaIdentityResults(ids, nil, nil)
		if !strings.HasSuffix(full.Detail, `"public"."t019", and 480 more`) || len(full.Detail) > 600 {
			t.Errorf("detail (%d bytes): %s", len(full.Detail), full.Detail)
		}
		if n := strings.Count(full.Remediation, "ALTER TABLE "); n != 20 {
			t.Errorf("%d statements, want 20", n)
		}
		if !strings.Contains(full.Remediation, ";\n\nThe same statement applies to the other 480 tables.\n\nCapture of a table starts") {
			t.Errorf("remediation:\n%s", full.Remediation)
		}
	})

	// The statements are the lines a screen offers to copy: every one of them
	// indented, and nothing else indented.
	t.Run("the statements are the only indented lines", func(t *testing.T) {
		gen := []pgcapture.UnpublishedGenerated{{Schema: "public", Table: "orders", Column: "total", Publication: "pub"}}
		full, key := replicaIdentityResults([]pgcapture.TableIdentity{def, nothing, idxNoKey}, gen, nil)
		indented := func(rem string) []string {
			var out []string
			for line := range strings.SplitSeq(rem, "\n") {
				if strings.HasPrefix(line, " ") {
					out = append(out, strings.TrimSpace(line))
				}
			}
			return out
		}
		if got, want := indented(full.Remediation), []string{`ALTER PUBLICATION "pub" SET (publish_generated_columns = stored);`, `ALTER TABLE "public"."orders" REPLICA IDENTITY FULL;`,
			`ALTER TABLE "public"."log" REPLICA IDENTITY FULL;`, `ALTER TABLE "public"."accounts" REPLICA IDENTITY FULL;`}; !slices.Equal(got, want) {
			t.Errorf("indented %q, want %q", got, want)
		}
		if got, want := indented(key.Remediation), []string{`ALTER TABLE "public"."accounts" REPLICA IDENTITY FULL;`}; !slices.Equal(got, want) {
			t.Errorf("indented %q, want %q", got, want)
		}
	})
}
