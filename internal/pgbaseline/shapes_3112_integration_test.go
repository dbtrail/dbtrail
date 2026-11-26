//go:build integration

package pgbaseline

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// The tables a snapshot taken now would hold, with their columns as the
// catalog has them now (#3112), against a real PostgreSQL.
func TestShapesOfNextRun_3112(t *testing.T) {
	dsn := testutil.SkipIfNoPostgres(t)
	ctx := context.Background()
	pg, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { pg.Close(context.Background()) })
	const pub, schema = "bintrail_shp_it_pub", `"Shp It Other"`
	drop := func() {
		bg := context.Background()
		_, _ = pg.Exec(bg, "DROP PUBLICATION IF EXISTS "+pub)
		_, _ = pg.Exec(bg, `DROP TABLE IF EXISTS shp_it_a, "Shp It B", shp_it_unpublished, shp_it_gone`)
		_, _ = pg.Exec(bg, "DROP SCHEMA IF EXISTS "+schema+" CASCADE")
	}
	drop()
	t.Cleanup(drop)
	exec := func(q string) {
		t.Helper()
		if _, err := pg.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	exec("CREATE TABLE shp_it_a (id int PRIMARY KEY, status text, amount numeric(10,2), total int GENERATED ALWAYS AS (id * 2) STORED)")
	exec(`CREATE TABLE "Shp It B" (id int PRIMARY KEY, "Mixed Col" varchar(20))`)
	exec("CREATE TABLE shp_it_unpublished (id int PRIMARY KEY)")
	exec("CREATE TABLE shp_it_gone (id int PRIMARY KEY)")
	exec("CREATE SCHEMA " + schema)
	exec("CREATE TABLE " + schema + `."Shp It B" (id bigint PRIMARY KEY)`)
	exec("CREATE PUBLICATION " + pub + ` FOR TABLE shp_it_a, "Shp It B", shp_it_gone, ` + schema + `."Shp It B"`)

	// name:type-oid:typmod:attnum of every column, per table.
	read := func(schemas string) map[string]string {
		t.Helper()
		got, err := ShapesOfNextRun(ctx, dsn, pub, cliutil.BuildIndexFilters(schemas, ""))
		if err != nil {
			t.Fatalf("ShapesOfNextRun: %v", err)
		}
		out := map[string]string{}
		for _, tb := range got {
			if tb.OID == 0 {
				t.Errorf("%s.%s has no OID", tb.Schema, tb.Table)
			}
			var cols []string
			for _, c := range tb.Columns {
				cols = append(cols, c.Name+":"+pgTypeName(t, pg, c.TypeOID, c.TypeMod)+":"+string(rune('0'+c.Ordinal)))
			}
			if _, dup := out[tb.Schema+"|"+tb.Table]; dup {
				t.Errorf("%s.%s listed twice", tb.Schema, tb.Table)
			}
			out[tb.Schema+"|"+tb.Table] = strings.Join(cols, ", ")
		}
		return out
	}
	want := func(got map[string]string, exp map[string]string) {
		t.Helper()
		if len(got) != len(exp) {
			t.Fatalf("tables = %v, want %v", got, exp)
		}
		for k, v := range exp {
			if got[k] != v {
				t.Fatalf("%s: columns %q, want %q (all: %v)", k, got[k], v, got)
			}
		}
	}

	// As created: the generated column is not one a file stores; a quoted
	// name is spelled as it is; the same table name in two schemas is two
	// tables; a table outside the publication is not listed.
	want(read(""), map[string]string{
		"public|shp_it_a":       "id:integer:1, status:text:2, amount:numeric(10,2):3",
		"public|Shp It B":       "id:integer:1, Mixed Col:character varying(20):2",
		"public|shp_it_gone":    "id:integer:1",
		"Shp It Other|Shp It B": "id:bigint:1",
	})
	// Narrowed to one schema, as a snapshot narrows them.
	want(read("Shp It Other"), map[string]string{"Shp It Other|Shp It B": "id:bigint:1"})

	// Every change of columns shows without a write: dropped, added with a
	// default, renamed, retyped, and dropped and added again (the same name
	// and type under another attnum). A table that is dropped is not listed.
	exec("ALTER TABLE shp_it_a DROP COLUMN status")
	exec("ALTER TABLE shp_it_a ADD COLUMN region text NOT NULL DEFAULT 'eu'")
	exec("ALTER TABLE shp_it_a ALTER COLUMN amount TYPE numeric(12,2)")
	exec(`ALTER TABLE "Shp It B" RENAME COLUMN "Mixed Col" TO "Other Col"`)
	exec("ALTER TABLE " + schema + `."Shp It B" DROP COLUMN id`)
	exec("ALTER TABLE " + schema + `."Shp It B" ADD COLUMN id bigint`)
	exec("DROP TABLE shp_it_gone")
	want(read(""), map[string]string{
		"public|shp_it_a":       "id:integer:1, amount:numeric(12,2):3, region:text:5",
		"public|Shp It B":       "id:integer:1, Other Col:character varying(20):2",
		"Shp It Other|Shp It B": "id:bigint:2",
	})

	// What changes no column changes nothing here.
	before := read("")
	exec("CREATE INDEX shp_it_a_region ON shp_it_a (region)")
	exec("ALTER TABLE shp_it_a ALTER COLUMN region SET DEFAULT 'us'")
	exec("ALTER TABLE shp_it_a ALTER COLUMN amount SET NOT NULL")
	want(read(""), before)

	// A source that cannot be asked is an error, never an empty answer.
	if _, err := ShapesOfNextRun(ctx, dsn, "bintrail_shp_it_no_such_pub", cliutil.BuildIndexFilters("", "")); err == nil || !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("a missing publication: err = %v, want it said", err)
	}
	short, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if got, err := ShapesOfNextRun(short, "postgres://nobody:x@127.0.0.1:1/none?connect_timeout=2", pub, cliutil.BuildIndexFilters("", "")); err == nil {
		t.Fatalf("an unreachable source answered %v", got)
	}
}

// pgTypeName spells a type as PostgreSQL does.
func pgTypeName(t *testing.T, pg *pgx.Conn, oid uint32, mod int32) string {
	t.Helper()
	var name string
	if err := pg.QueryRow(context.Background(), "SELECT format_type($1::oid, $2)", oid, mod).Scan(&name); err != nil {
		t.Fatalf("format_type(%d, %d): %v", oid, mod, err)
	}
	return name
}
