package pgstreamrun

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/doctor"
	"github.com/dbtrail/dbtrail/internal/pgcapture"
)

// What `server add`, Start and `bintrail-pg doctor` say about tables that are
// not at REPLICA IDENTITY FULL (#3027): a warning, since FULL is needed only
// for undo, and a failure for the one kind of table that is not captured.
func TestReplicaIdentityResults(t *testing.T) {
	def := pgcapture.TableIdentity{Schema: "public", Table: "orders", Identity: "d"}
	idx := pgcapture.TableIdentity{Schema: "Sales Data", Table: `Order "Items"`, Identity: "i"}
	nothing := pgcapture.TableIdentity{Schema: "public", Table: "log", Identity: "n", NoIdentity: true}
	idxNoKey := pgcapture.TableIdentity{Schema: "public", Table: "accounts", Identity: "i", MissesKey: []string{"id"}}
	include := pgcapture.TableIdentity{Schema: "public", Table: "wide", Identity: "d", MissesKey: []string{"extra"}}

	t.Run("every table at FULL", func(t *testing.T) {
		undo, key := replicaIdentityResults(nil)
		for _, got := range []doctor.CheckResult{undo, key} {
			if got.Status != doctor.StatusPass || got.Detail != "" || got.Remediation != "" {
				t.Errorf("got %+v, want a bare pass", got)
			}
		}
		if undo.Name != "REPLICA IDENTITY FULL" || key.Name != "Replica identity holds the primary key" {
			t.Errorf("names %q, %q: other code keys on the first one", undo.Name, key.Name)
		}
	})

	t.Run("a table at DEFAULT is a warning, never a failure", func(t *testing.T) {
		undo, key := replicaIdentityResults([]pgcapture.TableIdentity{def})
		t.Logf("check: %s\n%s\n%s", undo.Name, undo.Detail, undo.Remediation)
		want := doctor.CheckResult{
			Name:   "REPLICA IDENTITY FULL",
			Status: doctor.StatusWarn,
			Detail: `1 table of the publication is not at REPLICA IDENTITY FULL: "public"."orders"`,
			Remediation: "FULL is optional: it is needed only for undo. The changes of these tables are recorded, and everything else works on them: " +
				"history, the copy and the queries it answers, snapshots and their updates. This does not stop the server from being added or started. " +
				"The exception is undo: PostgreSQL sends only the key of the row an UPDATE or a DELETE changed, so DBTrail does not have the earlier row to put back, " +
				"and refuses to undo those changes. Undo of an INSERT works. To make a table's changes undoable, run this on the database:\n\n" +
				"  ALTER TABLE \"public\".\"orders\" REPLICA IDENTITY FULL;\n" +
				"\nIt takes effect for changes made after it, not for earlier ones. It costs WAL: the table then logs the whole earlier row for every UPDATE and DELETE.",
		}
		if undo.Name != want.Name || undo.Status != want.Status || undo.Detail != want.Detail || undo.Remediation != want.Remediation ||
			undo.Optional || undo.Kind != "" {
			t.Errorf("got  %+v\nwant %+v", undo, want)
		}
		if key.Status != doctor.StatusPass {
			t.Errorf("the key check must pass: %+v", key)
		}
	})

	t.Run("a table with no identity is said apart, in the same warning", func(t *testing.T) {
		undo, key := replicaIdentityResults([]pgcapture.TableIdentity{def, nothing, idx})
		t.Logf("%s\n%s", undo.Detail, undo.Remediation)
		if undo.Status != doctor.StatusWarn || key.Status != doctor.StatusPass {
			t.Fatalf("statuses %s, %s", undo.Status, key.Status)
		}
		if want := `3 tables of the publication are not at REPLICA IDENTITY FULL: "public"."orders", "Sales Data"."Order ""Items""", "public"."log"`; undo.Detail != want {
			t.Errorf("detail %q\nwant   %q", undo.Detail, want)
		}
		for _, want := range []string{
			"  ALTER TABLE \"Sales Data\".\"Order \"\"Items\"\"\" REPLICA IDENTITY FULL;\n",
			"\n\n1 table of them has no replica identity at all (REPLICA IDENTITY NOTHING, or no usable primary key): \"public\".\"log\". " +
				"PostgreSQL itself refuses UPDATE and DELETE on such a table while it is in a publication, with or without DBTrail, so only its INSERT and TRUNCATE are recorded. " +
				"Give the table a primary key, or run the statement above for it.",
		} {
			if !strings.Contains(undo.Remediation, want) {
				t.Errorf("remediation lacks %q:\n%s", want, undo.Remediation)
			}
		}
	})

	t.Run("an identity without the primary key is a failure of its own", func(t *testing.T) {
		undo, key := replicaIdentityResults([]pgcapture.TableIdentity{idxNoKey, include})
		t.Logf("check: %s\n%s\n%s", key.Name, key.Detail, key.Remediation)
		// Not captured, so not in the warning that says "everything works".
		if undo.Status != doctor.StatusPass {
			t.Errorf("a table that is not captured must not be listed as captured without undo: %+v", undo)
		}
		want := doctor.CheckResult{
			Name:   "Replica identity holds the primary key",
			Status: doctor.StatusFail,
			Detail: `2 tables of the publication have a replica identity that does not hold the primary key: "public"."accounts" (primary-key column id left out), "public"."wide" (primary-key column extra left out)`,
			Remediation: "A server is not added or started while this check fails. A capture that is already running does not record the changes of these tables " +
				"(each one is counted as skipped in capture health) and records the other tables normally. " +
				"For an UPDATE of the primary key PostgreSQL would send no earlier row, and for a DELETE an earlier row without the primary key, " +
				"so a change could not be matched to the row it changed. Run this on the database:\n\n" +
				"  ALTER TABLE \"public\".\"accounts\" REPLICA IDENTITY DEFAULT;\n" +
				"  ALTER TABLE \"public\".\"wide\" REPLICA IDENTITY FULL;\n" +
				"\nREPLICA IDENTITY DEFAULT sends the primary key. REPLICA IDENTITY FULL works for every table and also makes its changes undoable, at the cost of more WAL. " +
				"Recording of the table starts by itself after the statement. The changes skipped before it are never recorded: the next full copy of the table brings its rows up to date, and the history of that stretch stays missing.",
		}
		if key.Name != want.Name || key.Status != want.Status || key.Detail != want.Detail || key.Remediation != want.Remediation {
			t.Errorf("got  %+v\nwant %+v", key, want)
		}
	})

	// REPLICA IDENTITY is set per partition: a partitioned table at FULL whose
	// partitions are not is without undo, and the statement that fixes it is
	// one per partition.
	t.Run("a partitioned table: the partitions are what needs the statement", func(t *testing.T) {
		root := pgcapture.TableIdentity{Schema: "public", Table: "events", Identity: "f",
			PartitionsBelowFull: []string{`"public"."events_2026_01"`, `"public"."events_2026_02"`}}
		undo, key := replicaIdentityResults([]pgcapture.TableIdentity{root})
		t.Logf("%s\n%s", undo.Detail, undo.Remediation)
		if undo.Status != doctor.StatusWarn || key.Status != doctor.StatusPass {
			t.Fatalf("statuses %s, %s", undo.Status, key.Status)
		}
		if want := `1 partitioned table of the publication has partitions that are not at REPLICA IDENTITY FULL: "public"."events" (2 of its partitions)`; undo.Detail != want {
			t.Errorf("detail %q\nwant   %q", undo.Detail, want)
		}
		if !strings.Contains(undo.Remediation, "\n\n  ALTER TABLE \"public\".\"events_2026_01\" REPLICA IDENTITY FULL;\n  ALTER TABLE \"public\".\"events_2026_02\" REPLICA IDENTITY FULL;\n\n") ||
			strings.Contains(undo.Remediation, `ALTER TABLE "public"."events" `) {
			t.Errorf("the statements must be the partitions', and not the parent's (it is at FULL already):\n%s", undo.Remediation)
		}
		if !strings.HasSuffix(undo.Remediation, "REPLICA IDENTITY is set per partition, and the statement on a partitioned table does not reach its partitions: "+
			"the table and each of its partitions need it, which is why the partitions are listed above.") {
			t.Errorf("remediation:\n%s", undo.Remediation)
		}

		// Parent and partitions all below FULL: every one of them is listed.
		root.Identity = "d"
		undo, _ = replicaIdentityResults([]pgcapture.TableIdentity{def, root})
		if n := strings.Count(undo.Remediation, "ALTER TABLE "); n != 4 {
			t.Errorf("%d statements, want 4 (orders, events, and its two partitions):\n%s", n, undo.Remediation)
		}
		if want := `2 tables of the publication are not at REPLICA IDENTITY FULL: "public"."orders", "public"."events" (2 of its partitions)`; undo.Detail != want {
			t.Errorf("detail %q\nwant   %q", undo.Detail, want)
		}
	})

	t.Run("a partition whose own identity leaves the key out fails for its table", func(t *testing.T) {
		root := pgcapture.TableIdentity{Schema: "public", Table: "events", Identity: "f",
			PartitionsBelowFull: []string{`"public"."events_1"`}, PartitionsMissKey: []string{`"public"."events_1"`}}
		undo, key := replicaIdentityResults([]pgcapture.TableIdentity{root})
		t.Logf("%s\n%s", key.Detail, key.Remediation)
		if undo.Status != doctor.StatusPass || key.Status != doctor.StatusFail {
			t.Fatalf("statuses %s, %s: a table that is not captured is a failure and is not listed as recorded", undo.Status, key.Status)
		}
		if want := `1 table of the publication has a replica identity that does not hold the primary key: "public"."events" (in 1 partition: "public"."events_1")`; key.Detail != want {
			t.Errorf("detail %q\nwant   %q", key.Detail, want)
		}
		if !strings.Contains(key.Remediation, "\n\n  ALTER TABLE \"public\".\"events_1\" REPLICA IDENTITY DEFAULT;\n\n") || strings.Contains(key.Remediation, `ALTER TABLE "public"."events" `) {
			t.Errorf("the statement must be the partition's:\n%s", key.Remediation)
		}
	})

	t.Run("hundreds of tables: bounded", func(t *testing.T) {
		var ids []pgcapture.TableIdentity
		for i := range 500 {
			ids = append(ids, pgcapture.TableIdentity{Schema: "public", Table: fmt.Sprintf("t%03d", i), Identity: "d"})
		}
		undo, _ := replicaIdentityResults(ids)
		if !strings.HasSuffix(undo.Detail, `"public"."t019", and 480 more`) || len(undo.Detail) > 600 {
			t.Errorf("detail (%d bytes): %s", len(undo.Detail), undo.Detail)
		}
		if n := strings.Count(undo.Remediation, "ALTER TABLE "); n != 20 {
			t.Errorf("%d statements, want 20", n)
		}
		if !strings.Contains(undo.Remediation, ";\n\nThe same statement applies to the other 480 tables.\n\nIt takes effect") {
			t.Errorf("remediation:\n%s", undo.Remediation)
		}
	})

	// The statements are the lines a screen offers to copy: every one of them
	// indented, and nothing else indented.
	t.Run("the statements are the only indented lines", func(t *testing.T) {
		undo, key := replicaIdentityResults([]pgcapture.TableIdentity{def, nothing, idxNoKey})
		indented := func(rem string) []string {
			var out []string
			for line := range strings.SplitSeq(rem, "\n") {
				if strings.HasPrefix(line, " ") {
					out = append(out, strings.TrimSpace(line))
				}
			}
			return out
		}
		if got, want := indented(undo.Remediation), []string{`ALTER TABLE "public"."orders" REPLICA IDENTITY FULL;`, `ALTER TABLE "public"."log" REPLICA IDENTITY FULL;`}; !slices.Equal(got, want) {
			t.Errorf("indented %q, want %q", got, want)
		}
		if got, want := indented(key.Remediation), []string{`ALTER TABLE "public"."accounts" REPLICA IDENTITY DEFAULT;`}; !slices.Equal(got, want) {
			t.Errorf("indented %q, want %q", got, want)
		}
	})
}
