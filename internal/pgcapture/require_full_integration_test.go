//go:build integration

package pgcapture_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/pgcapture"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestCapturer_RequireFull_Integration runs the real Capturer, with its gate
// on, against a live PostgreSQL: DBTrail captures a table only at REPLICA
// IDENTITY FULL. A table below it, or a partitioned table with a partition
// below it, is skipped and recorded while the other tables flow; a table
// at FULL with no primary key is captured; a table that goes from FULL to
// DEFAULT while capture runs has its changes made at DEFAULT dropped, never
// stored in part, and is captured again, whole, once it is back at FULL, with
// no restart.
func TestCapturer_RequireFull_Integration(t *testing.T) {
	baseDSN := testutil.SkipIfNoPostgres(t)
	ctx := context.Background()
	const slot = "bintrail_requirefull_it"
	const pub = "bintrail_requirefull_it_pub"
	tables := []string{"rf_full", "rf_def", "rf_nothing", "rf_nopk", "rf_parted", "rf_parted_ok", "rf_end"}

	setup, err := pgx.Connect(ctx, baseDSN)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { setup.Close(context.Background()) })
	cleanup := func() {
		bg := context.Background()
		_, _ = setup.Exec(bg, "DROP PUBLICATION IF EXISTS "+pub)
		_, _ = setup.Exec(bg, "DROP TABLE IF EXISTS "+strings.Join(tables, ", "))
		_, _ = setup.Exec(bg, "SELECT pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", slot)
	}
	cleanup()
	t.Cleanup(cleanup)
	mustExec := func(q string) {
		t.Helper()
		if _, err := setup.Exec(ctx, q); err != nil {
			t.Fatalf("exec %q: %v", q, err)
		}
	}

	mustExec("CREATE TABLE rf_full (id int PRIMARY KEY, v text)")
	mustExec("CREATE TABLE rf_def (id int PRIMARY KEY, v text)") // DEFAULT
	mustExec("CREATE TABLE rf_nothing (id int PRIMARY KEY, v text)")
	mustExec("ALTER TABLE rf_nothing REPLICA IDENTITY NOTHING")
	mustExec("CREATE TABLE rf_nopk (a int, b text)")
	// A partitioned table at FULL with one partition at DEFAULT, and one
	// whose partitions are all at FULL, both published through their root.
	mustExec("CREATE TABLE rf_parted (id int PRIMARY KEY, v text) PARTITION BY RANGE (id)")
	mustExec("CREATE TABLE rf_parted_1 PARTITION OF rf_parted FOR VALUES FROM (0) TO (1000)")
	mustExec("CREATE TABLE rf_parted_2 PARTITION OF rf_parted FOR VALUES FROM (1000) TO (2000)") // DEFAULT
	mustExec("CREATE TABLE rf_parted_ok (id int PRIMARY KEY, v text) PARTITION BY RANGE (id)")
	mustExec("CREATE TABLE rf_parted_ok_1 PARTITION OF rf_parted_ok FOR VALUES FROM (0) TO (1000)")
	mustExec("CREATE TABLE rf_end (id int PRIMARY KEY)")
	for _, tb := range []string{"rf_full", "rf_nopk", "rf_parted", "rf_parted_1", "rf_parted_ok", "rf_parted_ok_1", "rf_end"} {
		mustExec("ALTER TABLE " + tb + " REPLICA IDENTITY FULL")
	}
	mustExec("CREATE PUBLICATION " + pub + " FOR TABLE " + strings.Join(tables, ", ") + " WITH (publish_via_partition_root = true)")

	r := startCapture(t, ctx, setup, baseDSN, slot, pub, true)
	end := 0
	untilEnd := func() []event.Event {
		t.Helper()
		end++
		mustExec(fmt.Sprintf("INSERT INTO rf_end VALUES (%d)", end))
		return r.until(func(ev event.Event) bool { return ev.Table == "rf_end" && ev.PKValues == fmt.Sprint(end) })
	}

	for _, tb := range []string{"rf_full", "rf_def", "rf_parted", "rf_parted_ok"} {
		mustExec("INSERT INTO " + tb + " VALUES (1, 'a')")
		mustExec("UPDATE " + tb + " SET v = 'b' WHERE id = 1")
		mustExec("DELETE FROM " + tb + " WHERE id = 1")
	}
	// In the partition at DEFAULT.
	mustExec("INSERT INTO rf_parted VALUES (1001, 'a')")
	mustExec("UPDATE rf_parted SET v = 'b' WHERE id = 1001")
	mustExec("INSERT INTO rf_nothing VALUES (1, 'a')") // PostgreSQL refuses its UPDATE and DELETE
	mustExec("INSERT INTO rf_nopk VALUES (1, 'a')")
	mustExec("UPDATE rf_nopk SET b = 'b' WHERE a = 1")
	mustExec("DELETE FROM rf_nopk WHERE a = 1")
	evs := untilEnd()

	noPartialImage(t, evs)
	for tb, want := range map[string]string{"rf_full": "IUD", "rf_nopk": "IUD", "rf_parted_ok": "IUD", "rf_def": "", "rf_nothing": "", "rf_parted": ""} {
		if got := rowKinds(of(evs, tb)); got != want {
			t.Errorf("%s: events %q, want %q", tb, got, want)
		}
	}
	if del := of(evs, "rf_nopk"); len(del) == 3 && del[2].RowBefore["b"] != "b" {
		t.Errorf("rf_nopk DELETE: the earlier row is whole, got %v", del[2].RowBefore)
	}
	reason := pgcapture.SkipReplicaIdentityNotFull
	if n := r.skips.Count(reason); n != 9 || r.skips.Total() != 9 {
		t.Errorf("want 9 skips under %q (3 of rf_def, 1 of rf_nothing, 5 of rf_parted), got %d of %d", reason, n, r.skips.Total())
	}
	assertLedgerNames(t, r.skips, reason, "public.rf_def", "public.rf_parted", "public.rf_nothing") // in the order first skipped
	logs := r.logText()
	t.Logf("capture log:\n%s", logs)
	for _, want := range []string{
		"3 tables not at REPLICA IDENTITY FULL",
		"table is not at REPLICA IDENTITY FULL; its changes are NOT captured",
		"partitioned table with partitions that are not at REPLICA IDENTITY FULL; the table's changes are NOT captured",
	} {
		if !strings.Contains(logs, want) {
			t.Errorf("the log lacks %q", want)
		}
	}

	// FULL to DEFAULT while capture runs, and back.
	mustExec("INSERT INTO rf_full VALUES (2, 'a')")
	mustExec("ALTER TABLE rf_full REPLICA IDENTITY DEFAULT")
	mustExec("UPDATE rf_full SET v = 'b' WHERE id = 2")
	mustExec("UPDATE rf_full SET id = 3 WHERE id = 2")
	mustExec("DELETE FROM rf_full WHERE id = 3")
	mustExec("INSERT INTO rf_full VALUES (4, 'a')")
	mustExec("ALTER TABLE rf_full REPLICA IDENTITY FULL")
	mustExec("UPDATE rf_full SET v = 'b' WHERE id = 4")
	mustExec("DELETE FROM rf_full WHERE id = 4")
	// The partition at DEFAULT put at FULL.
	mustExec("ALTER TABLE rf_parted_2 REPLICA IDENTITY FULL")
	mustExec("UPDATE rf_parted SET v = 'c' WHERE id = 1001")
	evs = untilEnd()
	noPartialImage(t, evs)
	full := of(evs, "rf_full")
	var got []string
	for _, ev := range full {
		got = append(got, fmt.Sprintf("%s:%s", rowKinds([]event.Event{ev}), ev.PKValues))
	}
	if want := []string{"I:2", "U:4", "D:4"}; !slices.Equal(got, want) {
		t.Errorf("rf_full across DEFAULT and back: events %v, want %v (what was made at DEFAULT is not captured)", got, want)
	} else if full[1].RowBefore["v"] != "a" || full[2].RowBefore["v"] != "b" {
		t.Errorf("back at FULL the earlier rows are whole: %v / %v", full[1].RowBefore, full[2].RowBefore)
	}
	if n := r.skips.Count(reason); n != 9+4 {
		t.Errorf("want rf_full's 4 changes made at DEFAULT counted (13 in all), got %d", n)
	}
	// What PostgreSQL does for a partition put at FULL is printed: the table
	// is captured again once it is described with every partition at FULL.
	parted := of(evs, "rf_parted")
	t.Logf("rf_parted after its partition was put at FULL, no restart: %q", rowKinds(parted))
	if len(parted) == 1 && parted[0].RowBefore["v"] != "b" {
		t.Errorf("rf_parted UPDATE at FULL: the earlier row is whole, got %v", parted[0].RowBefore)
	}
	r.stop()
}
