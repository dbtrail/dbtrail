//go:build integration

package pgcapture_test

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/pgcapture"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #3108: a publication created WITH (publish = 'insert, update, delete')
// sends nothing for a TRUNCATE. Capture still starts and records every row
// change, and says once, at its start, that a TRUNCATE is not recorded and
// which statement fixes it. The health probe says it too. Control: the
// default publication records the TRUNCATE and says nothing.
func TestTruncateNotPublishedIsSaidAtStart_3108(t *testing.T) {
	baseDSN := testutil.SkipIfNoPostgres(t)
	for _, tc := range []struct {
		name, slot, pub, with string
		want                  string // the table's events: I insert, T truncate
	}{
		{"default publication", "t3108_cap_def", "t3108_cap_def_pub", "", "IITI"},
		{"publish without truncate", "t3108_cap_nt", "t3108_cap_nt_pub", " WITH (publish = 'insert, update, delete')", "III"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			table := tc.slot + "_tbl"
			setup, err := pgx.Connect(ctx, baseDSN)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { setup.Close(context.Background()) })
			cleanup := func() {
				bg := context.Background()
				_, _ = setup.Exec(bg, "DROP PUBLICATION IF EXISTS "+tc.pub)
				_, _ = setup.Exec(bg, "DROP TABLE IF EXISTS "+table)
				_, _ = setup.Exec(bg, "SELECT pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", tc.slot)
			}
			cleanup()
			t.Cleanup(cleanup)
			mustExec := func(q string) {
				t.Helper()
				if _, err := setup.Exec(ctx, q); err != nil {
					t.Fatalf("exec %q: %v", q, err)
				}
			}
			mustExec("CREATE TABLE " + table + " (id int PRIMARY KEY, v text)")
			mustExec("ALTER TABLE " + table + " REPLICA IDENTITY FULL")
			mustExec("CREATE PUBLICATION " + tc.pub + " FOR TABLE " + table + tc.with)

			r := startNofull(t, ctx, setup, baseDSN, tc.slot, tc.pub)
			mustExec("INSERT INTO " + table + " VALUES (1, 'a'), (2, 'b')")
			mustExec("TRUNCATE " + table)
			mustExec("INSERT INTO " + table + " VALUES (99, 'sentinel')")
			evs := r.until(func(ev event.Event) bool {
				return ev.Table == table && ev.EventType == event.EventInsert && ev.PKValues == "99"
			})
			r.stop()
			if kinds := rowKinds(of(evs, table)); kinds != tc.want {
				t.Errorf("events = %q, want %q", kinds, tc.want)
			}
			logs := r.logText()
			t.Logf("capture log:\n%s", logs)
			fix := pgcapture.TruncateFixStatement(tc.pub)
			said := strings.Count(logs, "does not publish TRUNCATE")
			if tc.with == "" {
				if said != 0 || strings.Contains(logs, "TRUNCATE") {
					t.Errorf("a default publication warns about TRUNCATE:\n%s", logs)
				}
			} else {
				if said != 1 {
					t.Errorf("the warning is said %d times at start, want once:\n%s", said, logs)
				}
				for _, want := range []string{"does not publish TRUNCATE, so " + pgcapture.TruncateNotRecorded, "Inserts, updates and deletes are captured normally", "SET (publish = 'insert, update, delete, truncate')"} {
					if !strings.Contains(logs, want) {
						t.Errorf("the start log lacks %q:\n%s", want, logs)
					}
				}
			}

			snap, err := pgcapture.ProbeHealth(ctx, baseDSN, tc.slot, tc.pub, event.Filters{})
			if err != nil {
				t.Fatalf("health probe: %v", err)
			}
			wantFix := ""
			if tc.with != "" {
				wantFix = fix
			}
			if snap.TruncateFix != wantFix || snap.TruncateErr != nil {
				t.Errorf("health: TruncateFix = %q (err %v), want %q", snap.TruncateFix, snap.TruncateErr, wantFix)
			}
		})
	}
}

// QueryPublishesTruncate over every publication shape: FOR TABLE, FOR ALL
// TABLES, one that does not exist, and a name that needs quoting. The check
// is per publication, never per table.
func TestQueryPublishesTruncate_3108(t *testing.T) {
	baseDSN := testutil.SkipIfNoPostgres(t)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, baseDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close(context.Background()) })
	const table = "t3108_q_tbl"
	pubs := []string{"t3108_q_def", "t3108_q_nt", "t3108_q_all_nt", `T3108 "Q" Mixed`}
	cleanup := func() {
		bg := context.Background()
		for _, p := range pubs {
			_, _ = conn.Exec(bg, "DROP PUBLICATION IF EXISTS "+pgx.Identifier{p}.Sanitize())
		}
		_, _ = conn.Exec(bg, "DROP TABLE IF EXISTS "+table)
	}
	cleanup()
	t.Cleanup(cleanup)
	for _, q := range []string{
		"CREATE TABLE " + table + " (id int PRIMARY KEY)",
		"CREATE PUBLICATION t3108_q_def FOR TABLE " + table,
		"CREATE PUBLICATION t3108_q_nt FOR TABLE " + table + " WITH (publish = 'insert, update, delete')",
		// Insert only: a FOR ALL TABLES publication of updates and deletes
		// makes PostgreSQL refuse them on every table of the shared test
		// database that has no replica identity, for as long as it exists.
		"CREATE PUBLICATION t3108_q_all_nt FOR ALL TABLES WITH (publish = 'insert')",
		"CREATE PUBLICATION " + pgx.Identifier{pubs[3]}.Sanitize() + " FOR TABLE " + table + " WITH (publish = 'insert, update, delete')",
	} {
		if _, err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("exec %q: %v", q, err)
		}
	}
	for _, c := range []struct {
		pub               string
		exists, publishes bool
	}{
		{"t3108_q_def", true, true},
		{"t3108_q_nt", true, false},
		{"t3108_q_all_nt", true, false},
		{pubs[3], true, false},
		{"t3108_q_missing", false, false},
	} {
		exists, publishes, err := pgcapture.QueryPublishesTruncate(ctx, conn, c.pub)
		if err != nil || exists != c.exists || publishes != c.publishes {
			t.Errorf("%q: exists=%v publishes=%v err=%v, want %v %v", c.pub, exists, publishes, err, c.exists, c.publishes)
		}
	}
	// The statement shown for the quoted name runs as it is.
	if _, err := conn.Exec(ctx, pgcapture.TruncateFixStatement(pubs[3])); err != nil {
		t.Fatalf("the statement for %q does not run: %v", pubs[3], err)
	}
	if _, publishes, err := pgcapture.QueryPublishesTruncate(ctx, conn, pubs[3]); err != nil || !publishes {
		t.Errorf("after the statement: publishes=%v err=%v", publishes, err)
	}
}
