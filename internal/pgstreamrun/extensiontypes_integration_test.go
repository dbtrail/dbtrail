//go:build integration

package pgstreamrun_test

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/pgstreamrun"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/recovery"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// pgExtTypeCase is one row of the EXTENSION-type round-trip matrix (#1210):
// like pgTypeCase, but each case names the extension whose CREATE EXTENSION
// must succeed before the case's column type exists. The plain postgres CI
// cells ship none of these extensions, so those cases skip there (loudly);
// the dedicated extension cells (postgis/postgis and pgvector/pgvector
// images, job integration-postgres-extensions) set
// BINTRAIL_REQUIRE_PGEXT=<ext>, which turns that skip into a FAILURE —
// mirroring BINTRAIL_REQUIRE_POSTGRES's green-via-skip guard.
type pgExtTypeCase struct{ name, ext, typeDDL, valSQL string }

var pgExtTypeMatrixCases = []pgExtTypeCase{
	// PostGIS: geometry/geography render through their output function (and
	// ::text) as hex EWKB with the SRID embedded — a self-contained text form
	// the input function accepts back, so the pgoutput text decode should be
	// lossless. Binary-heavy custom send/recv is exactly why this needs
	// empirical proof rather than "should".
	{"geometry", "postgis", "geometry", "'SRID=4326;POINT(1 2)'"},
	{"geometry_typed", "postgis", "geometry(Point,4326)", "'SRID=4326;POINT(-70.5 42.1)'"},
	{"geography", "postgis", "geography(Point,4326)", "'SRID=4326;POINT(-70.5 42.1)'"},
	// pgvector: components chosen float4-exact (0.5, -1.25, 3.75) so the text
	// rendering is bit-stable and a mismatch means capture loss, not float
	// formatting noise.
	{"vector", "vector", "vector(3)", "'[0.5,-1.25,3.75]'"},
}

// TestOne_PGExtensionTypeRoundTripMatrix extends the #533 type-fidelity audit
// to extension-provided types (#1210): a value flows PG → pgoutput → index →
// recover → EXECUTE the reverse-INSERT against live PostgreSQL, and the
// column's canonical ::text rendering must round-trip byte-for-byte. Same
// shape as TestOne_PGTypeRoundTripMatrix, restricted to the cases whose
// extension is actually installable in the test database.
//
// CREATE EXTENSION here is the TEST HARNESS preparing its own throwaway
// server — capture itself stays a pure logical-replication client over the
// built-in pgoutput plugin (the red line); bintrail never installs anything
// in a source database.
func TestOne_PGExtensionTypeRoundTripMatrix(t *testing.T) {
	pgDSN := testutil.SkipIfNoPostgres(t)
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()

	pg, err := pgx.Connect(ctx, pgDSN)
	if err != nil {
		t.Fatalf("connect PG: %v", err)
	}
	t.Cleanup(func() { pg.Close(context.Background()) })

	// ── Extension availability probe ────────────────────────────────────────
	// CREATE EXTENSION IF NOT EXISTS per distinct extension; the ones that
	// fail (packages not in the image) drop their cases — loudly, and fatally
	// when BINTRAIL_REQUIRE_PGEXT names them. Extensions are left installed:
	// they are harmless server-wide state in a throwaway test container.
	required := testutil.RequiredPGExtensions()
	available := map[string]bool{}
	for _, c := range pgExtTypeMatrixCases {
		if _, probed := available[c.ext]; probed {
			continue
		}
		if _, err := pg.Exec(ctx, "CREATE EXTENSION IF NOT EXISTS "+c.ext); err != nil {
			if required[c.ext] {
				t.Fatalf("BINTRAIL_REQUIRE_PGEXT requires extension %q but CREATE EXTENSION failed — the extension CI cell must run an image that ships it: %v", c.ext, err)
			}
			t.Logf("SKIPPING %q cases: extension not installable in the test database (expected on plain postgres images): %v", c.ext, err)
			available[c.ext] = false
			continue
		}
		available[c.ext] = true
	}
	cases := make([]pgExtTypeCase, 0, len(pgExtTypeMatrixCases))
	for _, c := range pgExtTypeMatrixCases {
		if available[c.ext] {
			cases = append(cases, c)
		}
	}
	if len(cases) == 0 {
		t.Skip("skipping: no PG extension (postgis/pgvector) installable here — the integration-postgres-extensions CI cells run this for real")
	}

	indexDB, dbName := testutil.CreateTestDB(t)
	indexDSN := testutil.BaseDSN() + "/" + dbName

	const slot = "bintrail_pgexttypes"
	const pub = "bintrail_pgexttypes_pub"
	tblOf := func(name string) string { return "pgexttype_" + name }

	dropAll := func() {
		bg := context.Background()
		_, _ = pg.Exec(bg, "DROP PUBLICATION IF EXISTS "+pub)
		for _, c := range pgExtTypeMatrixCases {
			_, _ = pg.Exec(bg, "DROP TABLE IF EXISTS "+tblOf(c.name))
		}
		_, _ = pg.Exec(bg, "SELECT pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", slot)
	}
	dropAll()
	t.Cleanup(dropAll)

	mustExec := func(sqlStr string) {
		t.Helper()
		if _, err := pg.Exec(ctx, sqlStr); err != nil {
			t.Fatalf("exec %q: %v", sqlStr, err)
		}
	}
	tbls := make([]string, len(cases))
	for i, c := range cases {
		tbl := tblOf(c.name)
		tbls[i] = tbl
		mustExec(fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY, val %s)", tbl, c.typeDDL))
		mustExec(fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL", tbl))
	}
	mustExec("CREATE PUBLICATION " + pub + " FOR TABLE " + strings.Join(tbls, ", "))

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	cfg := pgstreamrun.Config{
		IndexDSN: indexDSN, ReplDSN: replDSN(pgDSN), QueryDSN: pgDSN,
		SlotName: slot, Publication: pub, ServerID: 47,
		BatchSize: 200, Checkpoint: 200 * time.Millisecond,
	}
	runErr := make(chan error, 1)
	go func() { runErr <- pgstreamrun.One(runCtx, cfg) }()
	waitFor(t, 15*time.Second, func() bool {
		var active bool
		if err := pg.QueryRow(ctx, "SELECT active AND confirmed_flush_lsn IS NOT NULL FROM pg_replication_slots WHERE slot_name=$1", slot).Scan(&active); err != nil {
			return false
		}
		return active
	}, "replication slot active")

	// INSERT then DELETE one row per type; capture the original canonical
	// ::text first (hex EWKB for PostGIS, bracketed float list for vector).
	orig := make(map[string]string, len(cases))
	for _, c := range cases {
		tbl := tblOf(c.name)
		mustExec(fmt.Sprintf("INSERT INTO %s (id, val) VALUES (1, %s)", tbl, c.valSQL))
		var got sql.NullString
		if err := pg.QueryRow(ctx, fmt.Sprintf("SELECT val::text FROM %s WHERE id=1", tbl)).Scan(&got); err != nil {
			t.Fatalf("%s: capture original: %v", c.name, err)
		}
		orig[c.name] = got.String
		mustExec(fmt.Sprintf("DELETE FROM %s WHERE id=1", tbl))
	}

	wantEvents := 2 * len(cases) // INSERT + DELETE per table
	waitFor(t, 30*time.Second, func() bool {
		var n int
		if err := indexDB.QueryRow("SELECT COUNT(*) FROM binlog_events WHERE schema_name='public'").Scan(&n); err != nil {
			return false
		}
		return n >= wantEvents
	}, "all extension-type events indexed")

	cancel()
	if err := <-runErr; err != nil {
		t.Fatalf("One returned error: %v", err)
	}

	resolver, err := metadata.NewResolver(indexDB, 0)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tbl := tblOf(c.name)
			rows, err := query.New(indexDB).Fetch(ctx, query.Options{Schema: "public", Table: tbl, Order: "ASC"})
			if err != nil {
				t.Fatalf("fetch: %v", err)
			}
			var del *query.ResultRow
			for i := range rows {
				if rows[i].EventType == event.EventDelete {
					del = &rows[i]
				}
			}
			if del == nil {
				t.Fatalf("no DELETE event indexed for %s", tbl)
			}
			var buf bytes.Buffer
			if _, err := recovery.NewForDialect(indexDB, resolver, recovery.PostgresDialect).
				GenerateSQLFromRows([]query.ResultRow{*del}, &buf); err != nil {
				t.Fatalf("generate: %v", err)
			}
			if _, err := pg.Exec(ctx, buf.String()); err != nil {
				t.Fatalf("reverse INSERT did not execute against PostgreSQL: %v\nSQL:\n%s", err, buf.String())
			}
			var got sql.NullString
			if err := pg.QueryRow(ctx, fmt.Sprintf("SELECT val::text FROM %s WHERE id=1", tbl)).Scan(&got); err != nil {
				t.Fatalf("re-select: %v", err)
			}
			if got.String != orig[c.name] {
				t.Errorf("round-trip mismatch (%s):\n got  %q\n want %q", c.typeDDL, got.String, orig[c.name])
			}
		})
	}
}
