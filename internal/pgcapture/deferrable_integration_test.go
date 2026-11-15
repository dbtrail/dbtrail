//go:build integration

package pgcapture

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// Deferrable primary keys against a real PostgreSQL: the check names the
// tables on which the server really lets two rows share a key inside a
// statement, which the test proves by running that statement.
func TestCheckDeferrableKeys_realServer(t *testing.T) {
	dsn := testutil.SkipIfNoPostgres(t)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { conn.Close(context.Background()) })
	const pub = "bintrail_defkey_it_pub"
	tables := []string{"defkey_plain", "defkey_immediate", "defkey_deferred", "defkey_unique_only", "defkey_unpublished"}
	drop := func() {
		bg := context.Background()
		_, _ = conn.Exec(bg, "DROP PUBLICATION IF EXISTS "+pub)
		for _, tb := range tables {
			_, _ = conn.Exec(bg, "DROP TABLE IF EXISTS "+tb)
		}
	}
	drop()
	t.Cleanup(drop)
	exec := func(q string) {
		t.Helper()
		if _, err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	exec("CREATE TABLE defkey_plain (id int PRIMARY KEY, v text)")
	exec("CREATE TABLE defkey_immediate (id int, v text, PRIMARY KEY (id) DEFERRABLE INITIALLY IMMEDIATE)")
	exec("CREATE TABLE defkey_deferred (id int, v text, CONSTRAINT my_key PRIMARY KEY (id) DEFERRABLE INITIALLY DEFERRED)")
	// A deferrable UNIQUE constraint beside an ordinary primary key is not
	// the key the copy is kept by.
	exec("CREATE TABLE defkey_unique_only (id int PRIMARY KEY, v text, UNIQUE (v) DEFERRABLE)")
	exec("CREATE TABLE defkey_unpublished (id int, v text, PRIMARY KEY (id) DEFERRABLE)")
	for _, tb := range tables {
		// A deferrable key cannot be a table's replica identity, so without
		// this a published table with one refuses every UPDATE for that
		// reason; whole rows are what capture asks for anyway.
		exec("ALTER TABLE " + tb + " REPLICA IDENTITY FULL")
		exec("INSERT INTO " + tb + " VALUES (1, 'a'), (2, 'b'), (3, 'c')")
	}
	exec("CREATE PUBLICATION " + pub + " FOR TABLE defkey_plain, defkey_immediate, defkey_deferred, defkey_unique_only")

	// shifts says whether the server lets every row take its neighbour's key
	// in one statement: only a deferrable key does.
	shifts := func(tb string) bool {
		t.Helper()
		_, err := conn.Exec(ctx, "UPDATE "+tb+" SET id = id + 1")
		if err == nil {
			exec("UPDATE " + tb + " SET id = id - 1") // put it back, highest first is not needed: deferrable
			return true
		}
		return false
	}
	check := func(when string, want map[string]string) {
		t.Helper()
		found, err := QueryDeferrableKeys(ctx, conn, pub)
		if err != nil {
			t.Fatalf("%s: %v", when, err)
		}
		got := map[string]string{}
		for _, k := range found {
			if k.Schema != "public" {
				t.Errorf("%s: schema %q", when, k.Schema)
			}
			got[k.Table] = k.Constraint
		}
		for _, tb := range tables[:4] {
			if named, shares := got[tb] != "", shifts(tb); named != shares {
				t.Errorf("%s: %s: the check names it: %v, the server lets rows share a key: %v", when, tb, named, shares)
			}
		}
		if len(got) != len(want) {
			t.Fatalf("%s: found %v, want %v", when, got, want)
		}
		for tb, con := range want {
			if got[tb] != con {
				t.Errorf("%s: %s: constraint %q, want %q", when, tb, got[tb], con)
			}
		}
		gate := CheckDeferrableKeys(ctx, conn, pub)
		if (gate != nil) != (len(want) > 0) || (gate != nil && !errors.Is(gate, ErrDeferrableKey)) {
			t.Errorf("%s: the gate returned %v", when, gate)
		}
	}
	check("two deferrable keys", map[string]string{"defkey_immediate": "defkey_immediate_pkey", "defkey_deferred": "my_key"})

	// What the message says to do, on one of them; the other out of the publication.
	exec("ALTER TABLE defkey_immediate DROP CONSTRAINT defkey_immediate_pkey, ADD PRIMARY KEY (id)")
	check("one key made not deferrable", map[string]string{"defkey_deferred": "my_key"})
	exec("ALTER PUBLICATION " + pub + " DROP TABLE defkey_deferred")
	tables = []string{"defkey_plain", "defkey_immediate", "defkey_unique_only", "defkey_unique_only"}
	check("the other out of the publication", map[string]string{})
}
