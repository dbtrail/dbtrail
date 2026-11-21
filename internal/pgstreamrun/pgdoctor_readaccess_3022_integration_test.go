//go:build integration

package pgstreamrun_test

import (
	"context"
	"net/url"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/doctor"
	"github.com/dbtrail/dbtrail/internal/pgstreamrun"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #3022 in the report `server add`, Start and `bintrail-pg doctor` read,
// against a real PostgreSQL: a role that replicates and has no SELECT on a
// published table gets a warning with the statement to run, and no failure,
// so the server is added, capture starts and doctor exits 0. After that
// statement the warning is gone.
func TestBuildPGReport_ReadAccess_3022(t *testing.T) {
	dsn := testutil.SkipIfNoPostgres(t)
	ctx := context.Background()
	const (
		slot     = "bintrail_doctor_ra3022"
		pub      = "bintrail_doctor_ra3022_pub"
		tbl      = "doctor_ra3022_customers"
		role     = "doctor_ra3022_role"
		password = "ra3022 pw"
		check    = "Read access to the published tables"
	)
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { admin.Close(context.Background()) })
	drop := func() {
		bg := context.Background()
		_, _ = admin.Exec(bg, "DROP PUBLICATION IF EXISTS "+pub)
		_, _ = admin.Exec(bg, "DROP TABLE IF EXISTS "+tbl)
		_, _ = admin.Exec(bg, "SELECT pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", slot)
		_, _ = admin.Exec(bg, "DROP ROLE IF EXISTS "+role)
	}
	drop()
	t.Cleanup(drop)
	for _, stmt := range []string{
		"CREATE TABLE " + tbl + " (id int PRIMARY KEY, name text)",
		"ALTER TABLE " + tbl + " REPLICA IDENTITY FULL",
		"CREATE PUBLICATION " + pub + " FOR TABLE " + tbl,
		"SELECT pg_create_logical_replication_slot('" + slot + "', 'pgoutput')",
		"CREATE ROLE " + role + " LOGIN REPLICATION PASSWORD '" + password + "'",
	} {
		if _, err := admin.Exec(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	u.User = url.UserPassword(role, password)
	cfg := pgstreamrun.PGDoctorConfig{QueryDSN: u.String(), SlotName: slot, Publication: pub}

	r := pgstreamrun.BuildPGReport(ctx, cfg)
	c := findCheck(t, r, check)
	const grant = `GRANT SELECT ON TABLE "public"."doctor_ra3022_customers" TO "doctor_ra3022_role";`
	// No failure at all: Start and the add read Failed, and `bintrail-pg
	// doctor` exits with Err().
	if c.Status != doctor.StatusWarn || r.Failed != 0 || r.Err() != nil {
		for _, ck := range r.Checks {
			t.Logf("%s = %s (%s)", ck.Name, ck.Status, ck.Detail)
		}
		t.Fatalf("%s = %s, report failed = %d, Err() = %v; want a warning and no failure", check, c.Status, r.Failed, r.Err())
	}
	if c.Optional {
		t.Error("the warning is marked optional: a screen would fold it under tips")
	}
	if want := `The role "doctor_ra3022_role" cannot read 1 of the 1 table the copy reads: "public"."doctor_ra3022_customers" (no SELECT)`; c.Detail != want {
		t.Errorf("detail:\n got %s\nwant %s", c.Detail, want)
	}
	if !strings.Contains(c.Remediation, "\n\n  "+grant+"\n") || c.Kind != "" {
		t.Errorf("kind %q, remediation:\n%s", c.Kind, c.Remediation)
	}

	if _, err := admin.Exec(ctx, grant); err != nil {
		t.Fatalf("the statement shown: %v", err)
	}
	r = pgstreamrun.BuildPGReport(ctx, cfg)
	if c := findCheck(t, r, check); c.Status != doctor.StatusPass || r.Failed != 0 {
		t.Errorf("after the statement shown: %s = %s (%s), report failed = %d", check, c.Status, c.Detail, r.Failed)
	}

	// A filter that leaves the table out leaves nothing to ask about.
	if _, err := admin.Exec(ctx, "REVOKE SELECT ON "+tbl+" FROM "+role); err != nil {
		t.Fatal(err)
	}
	cfg.Schemas = "some_other_schema"
	if c := findCheck(t, pgstreamrun.BuildPGReport(ctx, cfg), check); c.Status != doctor.StatusSkip {
		t.Errorf("with the table filtered out: %s = %s (%s), want skip", check, c.Status, c.Detail)
	}
}
