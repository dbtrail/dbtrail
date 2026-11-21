//go:build integration

package pgstreamrun_test

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/doctor"
	"github.com/dbtrail/dbtrail/internal/pgstreamrun"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #3108 through the doctor, against a real PostgreSQL: a publication without
// TRUNCATE is a warning (never a failure: Start reads this report), with a
// statement that, run as it is printed, makes it publish TRUNCATE and keeps
// what else the publication had. FOR ALL TABLES is checked the same way: the
// check is per publication.
func TestDoctorTruncateNotPublished_3108(t *testing.T) {
	dsn := testutil.SkipIfNoPostgres(t)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close(context.Background()) })
	const parent, part, plain = "doc3108_parted", "doc3108_parted_p1", "doc3108_plain"
	pubs := []string{"doc3108_def_pub", "doc3108_root_pub", "doc3108_all_pub"}
	cleanup := func() {
		bg := context.Background()
		for _, p := range pubs {
			_, _ = conn.Exec(bg, "DROP PUBLICATION IF EXISTS "+p)
		}
		_, _ = conn.Exec(bg, "DROP TABLE IF EXISTS "+parent+", "+plain)
	}
	cleanup()
	t.Cleanup(cleanup)
	mustExec := func(q string) {
		t.Helper()
		if _, err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("exec %q: %v", q, err)
		}
	}
	mustExec("CREATE TABLE " + plain + " (id int PRIMARY KEY)")
	mustExec("CREATE TABLE " + parent + " (id int PRIMARY KEY) PARTITION BY RANGE (id)")
	mustExec("CREATE TABLE " + part + " PARTITION OF " + parent + " FOR VALUES FROM (0) TO (1000)")

	report := func(pub string) doctor.CheckResult {
		t.Helper()
		r := pgstreamrun.BuildPGReport(ctx, pgstreamrun.PGDoctorConfig{QueryDSN: dsn, SlotName: "doc3108_no_slot", Publication: pub})
		if c := findCheck(t, r, "Publication coverage"); c.Status != doctor.StatusPass {
			t.Fatalf("Publication coverage = %s (%s), want pass", c.Status, c.Detail)
		}
		return findCheck(t, r, "Publication publishes TRUNCATE")
	}
	options := func(pub string) (truncate, viaRoot bool) {
		t.Helper()
		if err := conn.QueryRow(ctx, "SELECT pubtruncate, pubviaroot FROM pg_publication WHERE pubname = $1", pub).Scan(&truncate, &viaRoot); err != nil {
			t.Fatal(err)
		}
		return truncate, viaRoot
	}
	// fixOf is the statement the check printed, taken from its text: what an
	// operator pastes, not a copy written here.
	fixOf := func(c doctor.CheckResult) string {
		t.Helper()
		for _, line := range strings.Split(c.Remediation, "\n") {
			if s := strings.TrimSpace(line); strings.HasPrefix(s, "ALTER PUBLICATION ") {
				return s
			}
		}
		t.Fatalf("no statement in the remediation:\n%s", c.Remediation)
		return ""
	}

	t.Run("default publication", func(t *testing.T) {
		mustExec("CREATE PUBLICATION doc3108_def_pub FOR TABLE " + plain)
		if c := report("doc3108_def_pub"); c.Status != doctor.StatusPass || c.Detail != "" {
			t.Errorf("default publication: %s (%s), want pass", c.Status, c.Detail)
		}
	})

	t.Run("publish_via_partition_root is kept", func(t *testing.T) {
		mustExec("CREATE PUBLICATION doc3108_root_pub FOR TABLE " + parent + " WITH (publish = 'insert, update, delete', publish_via_partition_root = true)")
		c := report("doc3108_root_pub")
		t.Logf("check: %s\n%s\n%s", c.Status, c.Detail, c.Remediation)
		if c.Status != doctor.StatusWarn {
			t.Fatalf("status = %s, want warn", c.Status)
		}
		if !strings.Contains(c.Detail, `"doc3108_root_pub" does not publish TRUNCATE`) {
			t.Errorf("detail: %s", c.Detail)
		}
		mustExec(fixOf(c))
		if truncate, viaRoot := options("doc3108_root_pub"); !truncate || !viaRoot {
			t.Errorf("after the statement: pubtruncate=%v pubviaroot=%v, want both true", truncate, viaRoot)
		}
		var tables int
		if err := conn.QueryRow(ctx, "SELECT count(*) FROM pg_publication_rel pr JOIN pg_publication p ON p.oid = pr.prpubid WHERE p.pubname = 'doc3108_root_pub'").Scan(&tables); err != nil || tables != 1 {
			t.Errorf("tables of the publication after the statement: %d (%v), want 1", tables, err)
		}
		if c := report("doc3108_root_pub"); c.Status != doctor.StatusPass {
			t.Errorf("after the statement: %s (%s), want pass", c.Status, c.Detail)
		}
	})

	t.Run("FOR ALL TABLES", func(t *testing.T) {
		// Dropped as soon as it is checked: while it exists PostgreSQL
		// refuses UPDATE and DELETE on every table of the shared test
		// database that has no replica identity.
		mustExec("CREATE PUBLICATION doc3108_all_pub FOR ALL TABLES WITH (publish = 'insert, update, delete')")
		defer func() { _, _ = conn.Exec(context.Background(), "DROP PUBLICATION IF EXISTS doc3108_all_pub") }()
		c := report("doc3108_all_pub")
		if c.Status != doctor.StatusWarn {
			t.Fatalf("FOR ALL TABLES without truncate: %s (%s), want warn", c.Status, c.Detail)
		}
		mustExec(fixOf(c))
		if truncate, _ := options("doc3108_all_pub"); !truncate {
			t.Error("after the statement the publication still does not publish TRUNCATE")
		}
		var all bool
		if err := conn.QueryRow(ctx, "SELECT puballtables FROM pg_publication WHERE pubname = 'doc3108_all_pub'").Scan(&all); err != nil || !all {
			t.Errorf("after the statement puballtables = %v (%v), want true", all, err)
		}
		if c := report("doc3108_all_pub"); c.Status != doctor.StatusPass {
			t.Errorf("after the statement: %s (%s), want pass", c.Status, c.Detail)
		}
	})

	t.Run("missing publication", func(t *testing.T) {
		r := pgstreamrun.BuildPGReport(ctx, pgstreamrun.PGDoctorConfig{QueryDSN: dsn, SlotName: "doc3108_no_slot", Publication: "doc3108_missing_pub"})
		if c := findCheck(t, r, "Publication publishes TRUNCATE"); c.Status != doctor.StatusSkip {
			t.Errorf("missing publication: %s (%s), want skip", c.Status, c.Detail)
		}
	})
}
