//go:build integration

package pgbaseline_test

import (
	"context"
	"net/url"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/pgbaseline"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// The size the disk check reads (#3029), against a real PostgreSQL: it is
// the server's own pg_table_size of the tables a snapshot copies, a
// partitioned table counts by its leaves, and tables out of scope are not
// counted.
func TestReadCopyEstimate_3029(t *testing.T) {
	dsn := testutil.SkipIfNoPostgres(t)
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { admin.Close(context.Background()) })
	const pub, s, other, role, password = "cs3029_pub", "cs3029_s", "Cs3029 Other", "cs3029_role", "cs3029 pw"
	drop := func() {
		bg := context.Background()
		_, _ = admin.Exec(bg, "DROP PUBLICATION IF EXISTS "+pub)
		_, _ = admin.Exec(bg, "DROP SCHEMA IF EXISTS "+s+" CASCADE")
		_, _ = admin.Exec(bg, `DROP SCHEMA IF EXISTS "`+other+`" CASCADE`)
		_, _ = admin.Exec(bg, "DROP ROLE IF EXISTS "+role)
	}
	drop()
	t.Cleanup(drop)
	exec := func(stmt string) {
		t.Helper()
		if _, err := admin.Exec(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	size := func(expr string) int64 {
		t.Helper()
		var n int64
		if err := admin.QueryRow(ctx, "SELECT "+expr).Scan(&n); err != nil {
			t.Fatalf("%s: %v", expr, err)
		}
		return n
	}
	republish := func(what string) {
		t.Helper()
		exec("DROP PUBLICATION IF EXISTS " + pub)
		exec("CREATE PUBLICATION " + pub + " " + what)
	}
	read := func(filters event.Filters) pgbaseline.CopyEstimate {
		t.Helper()
		est, err := pgbaseline.ReadCopyEstimate(ctx, dsn, pub, filters)
		if err != nil {
			t.Fatalf("ReadCopyEstimate: %v", err)
		}
		return est
	}

	exec("CREATE SCHEMA " + s)
	exec(`CREATE SCHEMA "` + other + `"`)
	exec("CREATE TABLE " + s + ".orders (id int PRIMARY KEY, v text)")
	exec("INSERT INTO " + s + ".orders SELECT g, md5(g::text) FROM generate_series(1, 20000) g")
	// Long values go to TOAST, which a snapshot copies and the heap's own
	// file does not hold.
	exec("CREATE TABLE " + s + ".docs (id int PRIMARY KEY, doc text)")
	exec("ALTER TABLE " + s + ".docs ALTER COLUMN doc SET STORAGE EXTERNAL")
	exec("INSERT INTO " + s + ".docs SELECT g, repeat(md5(g::text), 200) FROM generate_series(1, 300) g")
	exec("CREATE TABLE " + s + ".empty (id int PRIMARY KEY)")
	exec("CREATE TABLE " + s + ".events (id int, at date, PRIMARY KEY (id, at)) PARTITION BY RANGE (at)")
	exec("CREATE TABLE " + s + ".events_2025 PARTITION OF " + s + ".events FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')")
	exec("CREATE TABLE " + s + ".events_2026 PARTITION OF " + s + ".events FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')")
	exec("INSERT INTO " + s + ".events SELECT g, DATE '2025-06-01' + (g % 2) * 365 FROM generate_series(1, 20000) g")
	exec(`CREATE TABLE "` + other + `"."Big Table" (id int PRIMARY KEY, v text)`)
	exec(`INSERT INTO "` + other + `"."Big Table" SELECT g, md5(g::text) FROM generate_series(1, 20000) g`)

	orders := size("pg_table_size('" + s + ".orders')")
	docs := size("pg_table_size('" + s + ".docs')")
	leaves := size("pg_table_size('" + s + ".events_2025') + pg_table_size('" + s + ".events_2026')")
	big := size(`pg_table_size('"` + other + `"."Big Table"')`)
	if orders < 1<<20 || leaves < 500<<10 || big < 1<<20 {
		t.Fatalf("the tables are too small to tell sizes apart: orders %d, leaves %d, big %d", orders, leaves, big)
	}
	if heap := size("pg_relation_size('" + s + ".docs')"); docs < heap+500<<10 {
		t.Fatalf("docs: pg_table_size %d is not well above its heap %d, so nothing went to TOAST", docs, heap)
	}
	if withIndexes := size("pg_total_relation_size('" + s + ".orders')"); withIndexes <= orders {
		t.Fatalf("orders: indexes add nothing (%d against %d), so the test cannot tell them apart", withIndexes, orders)
	}

	t.Run("the server's pg_table_size, TOAST in and indexes out", func(t *testing.T) {
		republish("FOR TABLE " + s + ".orders, " + s + ".docs, " + s + ".empty")
		got := read(event.Filters{})
		if want := orders + docs + size("pg_table_size('"+s+".empty')"); got.Bytes != want || got.Tables != 3 || got.Unsized != 0 || got.OtherStorage != 0 {
			t.Errorf("estimate = %+v, want %d bytes over 3 tables", got, want)
		}
	})
	t.Run("a partitioned table published through its root counts by its leaves", func(t *testing.T) {
		republish("FOR TABLE " + s + ".events WITH (publish_via_partition_root = true)")
		if got := read(event.Filters{}); got.Bytes != leaves || got.Tables != 1 || got.Unsized != 0 {
			t.Errorf("estimate = %+v, want %d bytes over 1 table", got, leaves)
		}
	})
	t.Run("a partitioned table published by its leaves", func(t *testing.T) {
		republish("FOR TABLE " + s + ".events")
		if got := read(event.Filters{}); got.Bytes != leaves || got.Tables != 2 {
			t.Errorf("estimate = %+v, want %d bytes over 2 tables", got, leaves)
		}
	})
	t.Run("a partitioned table with no partitions is an empty table, not an unsized one", func(t *testing.T) {
		exec("CREATE TABLE " + s + ".bare (id int PRIMARY KEY) PARTITION BY RANGE (id)")
		republish("FOR TABLE " + s + ".bare WITH (publish_via_partition_root = true)")
		if got := read(event.Filters{}); got.Bytes != 0 || got.Tables != 1 || got.Unsized != 0 {
			t.Errorf("estimate = %+v, want 0 bytes over 1 table, none unsized", got)
		}
	})
	t.Run("FOR ALL TABLES, narrowed to the schemas the snapshot copies", func(t *testing.T) {
		republish("FOR ALL TABLES")
		got := read(event.Filters{Schemas: map[string]bool{other: true}})
		if got.Bytes != big || got.Tables != 1 {
			t.Errorf("estimate = %+v, want %d bytes over 1 table", got, big)
		}
		// Unnarrowed it holds at least everything this test made.
		if all := read(event.Filters{}); all.Bytes < orders+docs+leaves+big || all.Tables < 6 {
			t.Errorf("unnarrowed estimate = %+v, want at least %d bytes over 6 tables", all, orders+docs+leaves+big)
		}
	})
	t.Run("a publication with no tables, and one that does not exist", func(t *testing.T) {
		republish("")
		if got := read(event.Filters{}); got.Tables != 0 || got.Bytes != 0 {
			t.Errorf("empty publication: %+v", got)
		}
		exec("DROP PUBLICATION " + pub)
		if got := read(event.Filters{}); got.Tables != 0 || got.Bytes != 0 {
			t.Errorf("missing publication: %+v", got)
		}
	})
	// The role DBTrail connects with need not own or even read the tables to
	// have them sized.
	t.Run("a role with no permission on the tables still gets their size", func(t *testing.T) {
		republish("FOR TABLE " + s + ".orders, \"" + other + "\".\"Big Table\"")
		exec("CREATE ROLE " + role + " LOGIN PASSWORD '" + password + "'")
		u, err := url.Parse(dsn)
		if err != nil {
			t.Fatal(err)
		}
		u.User = url.UserPassword(role, password)
		got, err := pgbaseline.ReadCopyEstimate(ctx, u.String(), pub, event.Filters{})
		if err != nil {
			t.Fatalf("ReadCopyEstimate as a role without permissions: %v", err)
		}
		if got.Bytes != orders+big || got.Tables != 2 || got.Unsized != 0 {
			t.Errorf("estimate = %+v, want %d bytes over 2 tables", got, orders+big)
		}
	})
	t.Run("a source that cannot be reached is an error, not an empty estimate", func(t *testing.T) {
		u, err := url.Parse(dsn)
		if err != nil {
			t.Fatal(err)
		}
		u.User = url.UserPassword("cs3029_no_such_role", "x")
		if got, err := pgbaseline.ReadCopyEstimate(ctx, u.String(), pub, event.Filters{}); err == nil {
			t.Errorf("no error for a login that does not exist: %+v", got)
		}
	})
}
