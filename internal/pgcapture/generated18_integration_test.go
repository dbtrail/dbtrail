//go:build integration

package pgcapture

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// Generated columns against a real PostgreSQL. On 18 the check must find
// exactly the tables whose UPDATE the server refuses, which the test proves
// by running that UPDATE; on 17 and older it finds nothing, and every UPDATE
// works.
func TestCheckGeneratedColumns_realServer(t *testing.T) {
	dsn := testutil.SkipIfNoPostgres(t)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { conn.Close(context.Background()) })
	var version int
	if err := conn.QueryRow(ctx, "SELECT current_setting('server_version_num')::int").Scan(&version); err != nil {
		t.Fatal(err)
	}
	pg18 := version >= 180000

	const pub, other = "bintrail_gen18_it_pub", "bintrail_gen18_it_other"
	drop := func() {
		bg := context.Background()
		_, _ = conn.Exec(bg, "DROP PUBLICATION IF EXISTS "+pub+", "+other)
		_, _ = conn.Exec(bg, "DROP TABLE IF EXISTS gen18_stored, gen18_virtual, gen18_plain, gen18_keyonly")
	}
	drop()
	t.Cleanup(drop)
	exec := func(q string) {
		t.Helper()
		if _, err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	exec("CREATE TABLE gen18_stored (id int PRIMARY KEY, v text, up text GENERATED ALWAYS AS (upper(v)) STORED)")
	exec("CREATE TABLE gen18_plain (id int PRIMARY KEY, v text)")
	exec("CREATE TABLE gen18_keyonly (id int PRIMARY KEY, v text, up text GENERATED ALWAYS AS (upper(v)) STORED)")
	tables := []string{"gen18_stored", "gen18_plain", "gen18_keyonly"}
	if pg18 {
		exec("CREATE TABLE gen18_virtual (id int PRIMARY KEY, v text, up text GENERATED ALWAYS AS (upper(v)) VIRTUAL)")
		tables = append(tables, "gen18_virtual")
	}
	for _, tb := range tables {
		if tb != "gen18_keyonly" { // stays at REPLICA IDENTITY DEFAULT
			exec("ALTER TABLE " + tb + " REPLICA IDENTITY FULL")
		}
		exec("INSERT INTO " + tb + " VALUES (1, 'a')")
	}

	// refused says which of the tables the server refuses an UPDATE on.
	refused := func() map[string]bool {
		t.Helper()
		out := map[string]bool{}
		for _, tb := range tables {
			_, err := conn.Exec(ctx, "UPDATE "+tb+" SET v = v || 'x' WHERE id = 1")
			if err != nil {
				if !strings.Contains(err.Error(), "generated column") {
					t.Fatalf("UPDATE %s failed for another reason: %v", tb, err)
				}
				out[tb] = true
			}
		}
		return out
	}
	// found says which tables the check names, and holds it to the server.
	check := func(when string) []UnpublishedGenerated {
		t.Helper()
		found, err := QueryUnpublishedGenerated(ctx, conn, pub)
		if err != nil {
			t.Fatalf("%s: %v", when, err)
		}
		named := map[string]bool{}
		for _, g := range found {
			named[g.Table] = true
		}
		server := refused()
		for _, tb := range tables {
			if named[tb] != server[tb] {
				t.Errorf("%s: %s: the check says blocked=%v, the server refuses the UPDATE=%v", when, tb, named[tb], server[tb])
			}
		}
		gate := CheckGeneratedColumns(ctx, conn, pub)
		if (gate != nil) != (len(found) > 0) || (gate != nil && !errors.Is(gate, ErrGeneratedBlocksWrites)) {
			t.Errorf("%s: the gate returned %v for %d findings", when, gate, len(found))
		}
		return found
	}

	exec("CREATE PUBLICATION " + pub + " FOR TABLE " + strings.Join(tables, ", "))
	found := check("a publication that publishes no generated column")
	if !pg18 {
		if len(found) != 0 {
			t.Fatalf("PostgreSQL %d has no such rule, and the check found %+v", version, found)
		}
		return
	}
	// 18: the stored and the virtual table, by column and kind; not the plain
	// one, and not the one that records its key only.
	want := map[string]bool{"gen18_stored": false, "gen18_virtual": true}
	if len(found) != len(want) {
		t.Fatalf("found %+v, want gen18_stored and gen18_virtual", found)
	}
	for _, g := range found {
		virtual, ok := want[g.Table]
		if !ok || g.Virtual != virtual || g.Column != "up" || g.Publication != pub || g.Schema != "public" {
			t.Errorf("finding %+v", g)
		}
	}

	// The stored column published: only the virtual one is left.
	exec("ALTER PUBLICATION " + pub + " SET (publish_generated_columns = stored)")
	if found = check("the publication publishes stored generated columns"); len(found) != 1 || found[0].Table != "gen18_virtual" {
		t.Fatalf("found %+v, want gen18_virtual alone", found)
	}

	// Another publication of the same table that does not publish it blocks
	// the table all the same, and is the one named.
	exec("CREATE PUBLICATION " + other + " FOR TABLE gen18_stored")
	found = check("a second publication that leaves the stored column out")
	var names []string
	for _, g := range found {
		names = append(names, g.Table+"@"+g.Publication)
	}
	if got := strings.Join(names, " "); got != "gen18_stored@"+other+" gen18_virtual@"+pub {
		t.Fatalf("found %s", got)
	}
	exec("DROP PUBLICATION " + other)

	// The way out the message gives: with the virtual table out of the
	// publication nothing is blocked, and the gate passes.
	exec("ALTER PUBLICATION " + pub + " DROP TABLE gen18_virtual")
	tables = tables[:3]
	if found = check("the virtual table out of the publication"); len(found) != 0 {
		t.Fatalf("found %+v, want nothing", found)
	}
}
