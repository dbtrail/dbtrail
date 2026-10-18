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

func findCheck(t *testing.T, r *doctor.Report, name string) doctor.CheckResult {
	t.Helper()
	for _, c := range r.Checks {
		if c.Name == name {
			return c
		}
	}
	t.Fatalf("report has no check named %q; checks=%v", name, r.Checks)
	return doctor.CheckResult{}
}

// TestBuildPGReport_Integration drives the doctor report against a live PostgreSQL:
// a healthy source (slot + publication + REPLICA IDENTITY FULL) passes the required
// checks, and a wrong publication name surfaces as a publication-coverage FAIL.
//
// Requires BINTRAIL_TEST_PG_DSN; the MySQL CI jobs leave it unset and skip cleanly.
func TestBuildPGReport_Integration(t *testing.T) {
	dsn := testutil.SkipIfNoPostgres(t)
	ctx := context.Background()

	const slot = "bintrail_doctor_it"
	const pub = "bintrail_doctor_it_pub"
	const tbl = "doctor_it_t"

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { conn.Close(context.Background()) })

	mustExec := func(sql string) {
		t.Helper()
		if _, err := conn.Exec(ctx, sql); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}
	dropAll := func() {
		bg := context.Background()
		_, _ = conn.Exec(bg, "DROP PUBLICATION IF EXISTS "+pub)
		_, _ = conn.Exec(bg, "DROP TABLE IF EXISTS "+tbl)
		_, _ = conn.Exec(bg, "SELECT pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", slot)
	}
	dropAll()
	t.Cleanup(dropAll)

	mustExec("CREATE TABLE " + tbl + " (id int PRIMARY KEY, v text)")
	mustExec("ALTER TABLE " + tbl + " REPLICA IDENTITY FULL")
	mustExec("CREATE PUBLICATION " + pub + " FOR TABLE " + tbl)
	mustExec("SELECT pg_create_logical_replication_slot('" + slot + "', 'pgoutput')")

	t.Run("healthy", func(t *testing.T) {
		r := pgstreamrun.BuildPGReport(ctx, pgstreamrun.PGDoctorConfig{
			QueryDSN: dsn, SlotName: slot, Publication: pub,
		})
		for _, name := range []string{"Source PostgreSQL connection", "wal_level = logical", "Publication coverage", "REPLICA IDENTITY FULL"} {
			if c := findCheck(t, r, name); c.Status != doctor.StatusPass {
				t.Errorf("%s = %s (%s), want pass", name, c.Status, c.Detail)
			}
		}
		// The slot is reserved (healthy) under the default unlimited retention.
		if c := findCheck(t, r, "Replication slot health"); c.Status != doctor.StatusPass {
			t.Errorf("slot health = %s (%s), want pass", c.Status, c.Detail)
		}
		// max_slot_wal_keep_size runs and reads the real setting: the dev/CI container
		// leaves it at the -1 default, so it WARNs (the production red line). A bounded
		// server would PASS; either way it must not FAIL or SKIP in a healthy run.
		if c := findCheck(t, r, "max_slot_wal_keep_size"); c.Status != doctor.StatusWarn && c.Status != doctor.StatusPass {
			t.Errorf("max_slot_wal_keep_size = %s (%s), want warn (unlimited) or pass (bounded)", c.Status, c.Detail)
		}
		// Informational, whatever the setting.
		if c := findCheck(t, r, "track_commit_timestamp"); c.Status != doctor.StatusPass {
			t.Errorf("track_commit_timestamp = %s (%s), want pass", c.Status, c.Detail)
		}
		if r.Failed != 0 {
			t.Errorf("healthy source has %d failed checks, want 0", r.Failed)
		}
		if err := r.Err(); err != nil {
			t.Errorf("Err() = %v, want nil for a healthy source", err)
		}
	})

	t.Run("missing-publication", func(t *testing.T) {
		r := pgstreamrun.BuildPGReport(ctx, pgstreamrun.PGDoctorConfig{
			QueryDSN: dsn, SlotName: slot, Publication: "no_such_pub",
		})
		if c := findCheck(t, r, "Publication coverage"); c.Status != doctor.StatusFail {
			t.Errorf("Publication coverage = %s, want fail for a non-existent publication", c.Status)
		}
		if r.Err() == nil {
			t.Error("Err() = nil, want non-nil when a required check failed")
		}
	})
}

// TestBuildPGReport_LostSlot_Integration is the #532 linchpin: it drives a real slot
// to wal_status='lost' and asserts the doctor reports it as a loud FAIL with the
// recovery path. max_slot_wal_keep_size is a server-wide SIGHUP setting; the CI runs
// PG packages with -p 1 (serial) and these tests are non-parallel, so the keep-size
// window is isolated. t.Cleanup RESETs it even on failure.
func TestBuildPGReport_LostSlot_Integration(t *testing.T) {
	dsn := testutil.SkipIfNoPostgres(t)
	ctx := context.Background()

	const slot = "bintrail_lostslot_it"
	const tbl = "lostslot_it_churn"

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { conn.Close(context.Background()) })

	mustExec := func(sql string) {
		t.Helper()
		if _, err := conn.Exec(ctx, sql); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}

	// Restore the server-wide setting and clean up regardless of outcome.
	t.Cleanup(func() {
		bg := context.Background()
		_, _ = conn.Exec(bg, "ALTER SYSTEM RESET max_slot_wal_keep_size")
		_, _ = conn.Exec(bg, "SELECT pg_reload_conf()")
		_, _ = conn.Exec(bg, "DROP TABLE IF EXISTS "+tbl)
		_, _ = conn.Exec(bg, "SELECT pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", slot)
	})
	// Start clean.
	_, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS "+tbl)
	_, _ = conn.Exec(ctx, "SELECT pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", slot)

	// Bound WAL retention tightly, create an (idle) slot, then churn WAL past the
	// bound so the next checkpoint invalidates the slot.
	mustExec("ALTER SYSTEM SET max_slot_wal_keep_size = '1MB'")
	mustExec("SELECT pg_reload_conf()")
	mustExec("SELECT pg_create_logical_replication_slot('" + slot + "', 'pgoutput')")
	mustExec("CREATE TABLE " + tbl + " (id serial, pad text)")

	lost := false
	for i := 0; i < 10 && !lost; i++ {
		mustExec("INSERT INTO " + tbl + " (pad) SELECT repeat('x', 900) FROM generate_series(1, 4000)")
		mustExec("SELECT pg_switch_wal()")
		mustExec("CHECKPOINT")
		var status string
		if err := conn.QueryRow(ctx, "SELECT wal_status FROM pg_replication_slots WHERE slot_name=$1", slot).Scan(&status); err != nil {
			t.Fatalf("read wal_status: %v", err)
		}
		lost = status == "lost"
	}
	if !lost {
		t.Skip("could not drive the slot to wal_status=lost on this server; skipping the lost-slot assertion")
	}

	r := pgstreamrun.BuildPGReport(ctx, pgstreamrun.PGDoctorConfig{
		QueryDSN: dsn, SlotName: slot, Publication: "irrelevant_for_slot_check",
	})
	c := findCheck(t, r, "Replication slot health")
	if c.Status != doctor.StatusFail {
		t.Fatalf("lost slot reported %s, want fail; detail=%q", c.Status, c.Detail)
	}
	if r.Err() == nil {
		t.Error("Err() = nil, want non-nil when the slot is lost")
	}
	// The recovery path must be present and reassure that recovery is unaffected.
	for _, want := range []string{"re-baseline", "recovery never needs the slot"} {
		if !strings.Contains(c.Remediation, want) {
			t.Errorf("lost-slot remediation missing %q:\n%s", want, c.Remediation)
		}
	}
}

// TestBuildPGReport_CoverageWarnings_Integration drives the two new advisory coverage
// guards against live PostgreSQL — the catalog QUERIES themselves (listUnloggedCapture
// Tables #555, listUncoveredCascadeChildren #556 incl. cascadeAction), which the pure
// result-mapper unit tests cannot reach. The #555 query in particular MUST go through
// pg_class (an UNLOGGED table is never in pg_publication_tables), so a live assertion is
// the only thing that proves it actually fires.
//
// Requires BINTRAIL_TEST_PG_DSN; the MySQL CI jobs leave it unset and skip cleanly.
func TestBuildPGReport_CoverageWarnings_Integration(t *testing.T) {
	dsn := testutil.SkipIfNoPostgres(t)
	ctx := context.Background()

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { conn.Close(context.Background()) })

	mustExec := func(sql string) {
		t.Helper()
		if _, err := conn.Exec(ctx, sql); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}

	t.Run("unlogged-under-for-all-tables", func(t *testing.T) {
		// #555: an UNLOGGED table is invisible to pg_publication_tables but real on
		// pg_class; under FOR ALL TABLES the operator believes it is captured. The guard
		// must scan pg_class and WARN. A Tables filter scopes the assertion to our table
		// (so any unrelated UNLOGGED table on the dev server can't perturb it).
		const pub = "bintrail_cov_all_pub"
		const ult = "doctor_cov_unlogged"
		dropAll := func() {
			bg := context.Background()
			_, _ = conn.Exec(bg, "DROP PUBLICATION IF EXISTS "+pub)
			_, _ = conn.Exec(bg, "DROP TABLE IF EXISTS "+ult)
		}
		dropAll()
		t.Cleanup(dropAll)

		mustExec("CREATE UNLOGGED TABLE " + ult + " (id int PRIMARY KEY, v text)")
		mustExec("CREATE PUBLICATION " + pub + " FOR ALL TABLES")

		r := pgstreamrun.BuildPGReport(ctx, pgstreamrun.PGDoctorConfig{
			QueryDSN: dsn, SlotName: "bintrail_cov_all_slot", Publication: pub,
			Tables: "public." + ult,
		})
		c := findCheck(t, r, "No UNLOGGED tables")
		if c.Status != doctor.StatusWarn {
			t.Errorf("No UNLOGGED tables = %s (%s), want warn", c.Status, c.Detail)
		}
		if !strings.Contains(c.Detail, ult) {
			t.Errorf("detail should name the UNLOGGED table %q, got %q", ult, c.Detail)
		}
	})

	t.Run("uncovered-cascade-child", func(t *testing.T) {
		// #556: a published parent whose ON DELETE CASCADE child is NOT published — the
		// cascade rewrites on the child would be silently lost. The guard must WARN and
		// name the child, the action, and the parent (exercises cascadeAction too).
		const pub = "bintrail_cov_tbl_pub"
		const parent = "doctor_cov_parent"
		const child = "doctor_cov_child"
		dropAll := func() {
			bg := context.Background()
			_, _ = conn.Exec(bg, "DROP PUBLICATION IF EXISTS "+pub)
			_, _ = conn.Exec(bg, "DROP TABLE IF EXISTS "+child) // child first (FK)
			_, _ = conn.Exec(bg, "DROP TABLE IF EXISTS "+parent)
		}
		dropAll()
		t.Cleanup(dropAll)

		mustExec("CREATE TABLE " + parent + " (id int PRIMARY KEY)")
		mustExec("ALTER TABLE " + parent + " REPLICA IDENTITY FULL")
		mustExec("CREATE TABLE " + child + " (id int PRIMARY KEY, pid int REFERENCES " + parent + "(id) ON DELETE CASCADE)")
		mustExec("ALTER TABLE " + child + " REPLICA IDENTITY FULL")
		mustExec("CREATE PUBLICATION " + pub + " FOR TABLE " + parent) // child intentionally omitted

		r := pgstreamrun.BuildPGReport(ctx, pgstreamrun.PGDoctorConfig{
			QueryDSN: dsn, SlotName: "bintrail_cov_tbl_slot", Publication: pub,
		})
		c := findCheck(t, r, "FK cascade-child coverage")
		if c.Status != doctor.StatusWarn {
			t.Errorf("FK cascade-child coverage = %s (%s), want warn", c.Status, c.Detail)
		}
		for _, want := range []string{child, "ON DELETE CASCADE", parent} {
			if !strings.Contains(c.Detail, want) {
				t.Errorf("detail should mention %q, got %q", want, c.Detail)
			}
		}
	})

	t.Run("unlogged-under-schema-publication", func(t *testing.T) {
		// #1211: a FOR TABLES IN SCHEMA publication (PG 15+) accepts a schema that
		// contains UNLOGGED tables — pg_publication_tables silently omits them, the
		// exact silent-loss shape #555 closed for FOR ALL TABLES. The guard must
		// enumerate pg_publication_namespace and WARN about UNLOGGED tables in the
		// published schemas ONLY: the same table shape in an UNPUBLISHED schema is
		// out of capture scope and must NOT be reported — that absence is what
		// proves the scan is schema-scoped, not the FOR ALL TABLES scan rerun.
		var version int
		if err := conn.QueryRow(ctx, "SELECT current_setting('server_version_num')::int").Scan(&version); err != nil {
			t.Fatalf("read server_version_num: %v", err)
		}
		if version < 150000 {
			t.Skipf("FOR TABLES IN SCHEMA requires PostgreSQL 15+; server is %d", version)
		}

		const pub = "bintrail_cov_sch_pub"
		const schIn = "bintrail_cov_sch_in"   // published via FOR TABLES IN SCHEMA
		const schOut = "bintrail_cov_sch_out" // NOT published
		dropAll := func() {
			bg := context.Background()
			_, _ = conn.Exec(bg, "DROP PUBLICATION IF EXISTS "+pub)
			_, _ = conn.Exec(bg, "DROP SCHEMA IF EXISTS "+schIn+" CASCADE")
			_, _ = conn.Exec(bg, "DROP SCHEMA IF EXISTS "+schOut+" CASCADE")
		}
		dropAll()
		t.Cleanup(dropAll)

		mustExec("CREATE SCHEMA " + schIn)
		mustExec("CREATE SCHEMA " + schOut)
		mustExec("CREATE UNLOGGED TABLE " + schIn + ".cov_unlogged (id int PRIMARY KEY, v text)")
		mustExec("CREATE UNLOGGED TABLE " + schOut + ".cov_unlogged (id int PRIMARY KEY, v text)")
		mustExec("CREATE PUBLICATION " + pub + " FOR TABLES IN SCHEMA " + schIn)

		// The Tables filter admits BOTH candidates, so the unpublished one being
		// absent from the detail can only come from the schema scoping itself.
		r := pgstreamrun.BuildPGReport(ctx, pgstreamrun.PGDoctorConfig{
			QueryDSN: dsn, SlotName: "bintrail_cov_sch_slot", Publication: pub,
			Tables: schIn + ".cov_unlogged," + schOut + ".cov_unlogged",
		})
		c := findCheck(t, r, "No UNLOGGED tables")
		if c.Status != doctor.StatusWarn {
			t.Errorf("No UNLOGGED tables = %s (%s), want warn", c.Status, c.Detail)
		}
		if !strings.Contains(c.Detail, schIn+".cov_unlogged") {
			t.Errorf("detail should name the published schema's UNLOGGED table, got %q", c.Detail)
		}
		if strings.Contains(c.Detail, schOut) {
			t.Errorf("detail must NOT name the unpublished schema %s, got %q", schOut, c.Detail)
		}
	})

	t.Run("unlogged-check-clean-under-for-table-publication", func(t *testing.T) {
		// A plain FOR TABLE publication has no schema members, and PostgreSQL refuses
		// UNLOGGED members outright — the check must PASS, not WARN "could not check".
		// On a pre-15 server this same path exercises the pg_publication_namespace
		// degrade: the catalog does not exist there (SQLSTATE 42P01) and the guard
		// must treat that as "no schema members", never as a probe failure — the
		// PG 14 CI cell runs this against a server that genuinely lacks the catalog.
		const pub = "bintrail_cov_plain_pub"
		const tbl = "doctor_cov_plain"
		dropAll := func() {
			bg := context.Background()
			_, _ = conn.Exec(bg, "DROP PUBLICATION IF EXISTS "+pub)
			_, _ = conn.Exec(bg, "DROP TABLE IF EXISTS "+tbl)
		}
		dropAll()
		t.Cleanup(dropAll)

		mustExec("CREATE TABLE " + tbl + " (id int PRIMARY KEY)")
		mustExec("ALTER TABLE " + tbl + " REPLICA IDENTITY FULL")
		mustExec("CREATE PUBLICATION " + pub + " FOR TABLE " + tbl)

		r := pgstreamrun.BuildPGReport(ctx, pgstreamrun.PGDoctorConfig{
			QueryDSN: dsn, SlotName: "bintrail_cov_plain_slot", Publication: pub,
		})
		c := findCheck(t, r, "No UNLOGGED tables")
		if c.Status != doctor.StatusPass {
			t.Errorf("No UNLOGGED tables = %s (%s), want pass", c.Status, c.Detail)
		}
	})
}
