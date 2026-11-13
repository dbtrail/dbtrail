//go:build integration

package pgverifysource

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/consistency"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2198: the live read is pinned to a temporary slot's exported snapshot.

const slotTbl = "pgvs_slot_it_2198"

// TestLiveSourcePinsAtTemporarySlot_2198: the read carries the slot's
// consistent point as an exact anchor, a write committed after the read
// opened is not in it, and the temporary slot is gone once the read is done.
func TestLiveSourcePinsAtTemporarySlot_2198(t *testing.T) {
	dsn := testutil.SkipIfNoPostgres(t)
	ctx := context.Background()
	setup := slotTable(t, dsn)
	slots := recordSlots(t)

	checksum, closeSource, err := LiveSource(ctx, dsn)
	if err != nil {
		t.Fatalf("LiveSource: %v", err)
	}
	t.Cleanup(func() { _ = closeSource() })
	var before uint64
	if err := setup.QueryRow(ctx, "SELECT (pg_current_wal_lsn() - '0/0')::bigint").Scan(&before); err != nil {
		t.Fatal(err)
	}
	got, err := checksum(ctx, "public", slotTbl, nil)
	if err != nil {
		t.Fatalf("checksum: %v", err)
	}
	if got.Anchor != consistency.AnchorPGSlot || got.AnchorUnpinned != "" {
		t.Fatalf("Anchor %q, AnchorUnpinned %q; want the slot's exact anchor", got.Anchor, got.AnchorUnpinned)
	}
	if got.LSN < before || got.RowCount != 3 {
		t.Fatalf("LSN %d (WAL was at %d before the read), rows %d; want the consistent point of a slot made for this read over the 3 rows", got.LSN, before, got.RowCount)
	}
	waitNoVerifySlot(t, setup, *slots)
}

// TestLiveSourceWaitsForOpenTransaction_2198: creating the slot waits for the
// write transactions open at that moment; one that outlives the wait makes
// the read refuse with consistency.ErrPGSnapshotWait naming it, and leaves no
// slot behind. Once it ends the read pins again.
func TestLiveSourceWaitsForOpenTransaction_2198(t *testing.T) {
	dsn := testutil.SkipIfNoPostgres(t)
	ctx := context.Background()
	setup := slotTable(t, dsn)
	slots := recordSlots(t)
	defer func(w time.Duration) { snapshotWait = w }(snapshotWait)
	snapshotWait = 2 * time.Second

	holder, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { holder.Close(context.Background()) })
	tx, err := holder.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(ctx, "UPDATE "+slotTbl+" SET v = v || '!' WHERE id = 1"); err != nil {
		t.Fatal(err)
	}

	checksum, closeSource, err := LiveSource(ctx, dsn)
	if err != nil {
		t.Fatalf("LiveSource: %v", err)
	}
	t.Cleanup(func() { _ = closeSource() })
	start := time.Now()
	_, err = checksum(ctx, "public", slotTbl, nil)
	t.Logf("after %s: %v", time.Since(start).Round(time.Millisecond), err)
	if !errors.Is(err, consistency.ErrPGSnapshotWait) {
		t.Fatalf("err = %v; want ErrPGSnapshotWait", err)
	}
	// The oldest open write transaction on a shared server may be another
	// test's; the message names one, by pid.
	if !strings.Contains(err.Error(), "pid ") && !strings.Contains(err.Error(), "prepared transaction") {
		t.Fatalf("err = %v; want the oldest open transaction named", err)
	}
	if time.Since(start) > 30*time.Second {
		t.Fatalf("the read took %s; want it bounded by the wait", time.Since(start))
	}
	waitNoVerifySlot(t, setup, *slots)

	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	snapshotWait = 30 * time.Second
	got, err := checksum(ctx, "public", slotTbl, nil)
	if err != nil {
		t.Fatalf("checksum after the transaction ended: %v", err)
	}
	if got.Anchor != consistency.AnchorPGSlot {
		t.Fatalf("Anchor %q (%s); want the slot's exact anchor once the transaction ended", got.Anchor, got.AnchorUnpinned)
	}
}

// TestLiveSourceWithoutReplication_2198: a role without the REPLICATION
// attribute cannot open the slot's connection; the read still runs, with the
// old anchor, and says why it is not pinned.
func TestLiveSourceWithoutReplication_2198(t *testing.T) {
	dsn := testutil.SkipIfNoPostgres(t)
	ctx := context.Background()
	setup := slotTable(t, dsn)
	slots := recordSlots(t)
	checksum, closeSource, err := LiveSource(ctx, roleWithoutReplication(t, setup, dsn, slotTbl))
	if err != nil {
		t.Fatalf("LiveSource: %v", err)
	}
	t.Cleanup(func() { _ = closeSource() })
	got, err := checksum(ctx, "public", slotTbl, nil)
	if err != nil {
		t.Fatalf("checksum: %v", err)
	}
	t.Logf("unpinned: %s", got.AnchorUnpinned)
	if got.Anchor != "" || got.LSN == 0 || got.RowCount != 3 {
		t.Fatalf("Anchor %q LSN %d rows %d; want the unpinned read with its WAL anchor", got.Anchor, got.LSN, got.RowCount)
	}
	if !strings.Contains(got.AnchorUnpinned, "REPLICATION") && !strings.Contains(got.AnchorUnpinned, "pg_hba.conf") {
		t.Fatalf("AnchorUnpinned %q; want it to say the role lacks REPLICATION (or has no replication entry)", got.AnchorUnpinned)
	}
	if len(*slots) != 0 {
		waitNoVerifySlot(t, setup, *slots)
	}
}

func slotTable(t *testing.T, dsn string) *pgx.Conn {
	t.Helper()
	ctx := context.Background()
	setup, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { setup.Close(context.Background()) })
	drop := func() { _, _ = setup.Exec(context.Background(), "DROP TABLE IF EXISTS "+slotTbl) }
	drop()
	t.Cleanup(drop)
	for _, q := range []string{
		"CREATE TABLE " + slotTbl + " (id int PRIMARY KEY, v text)",
		"INSERT INTO " + slotTbl + " VALUES (1, 'a'), (2, 'b'), (3, 'c')",
	} {
		if _, err := setup.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	return setup
}

// recordSlots records the name of every temporary slot this test's reads
// create.
func recordSlots(t *testing.T) *[]string {
	t.Helper()
	var names []string
	prev := slotNamed
	slotNamed = func(n string) { names = append(names, n) }
	t.Cleanup(func() { slotNamed = prev })
	return &names
}

// waitNoVerifySlot fails unless every slot in names is gone, give or take
// the server ending the session that held it, and at least one was made.
func waitNoVerifySlot(t *testing.T, conn *pgx.Conn, names []string) {
	t.Helper()
	if len(names) == 0 {
		t.Fatal("the read created no temporary slot")
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		var n int
		if err := conn.QueryRow(context.Background(), "SELECT count(*) FROM pg_replication_slots WHERE slot_name = ANY($1)", names).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d of the temporary slots %v still exist after the read", n, names)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// roleWithoutReplication creates a login role with SELECT on table and no
// REPLICATION attribute, dropped at cleanup, and returns dsn with it.
func roleWithoutReplication(t *testing.T, setup *pgx.Conn, dsn, table string) string {
	t.Helper()
	ctx := context.Background()
	u, err := url.Parse(dsn)
	if err != nil || u.Scheme == "" {
		t.Skipf("the test DSN is not a URL, cannot swap its user: %v", err)
	}
	role := fmt.Sprintf("pgvs_norepl_%d", time.Now().UnixNano()%1e9)
	const pw = "norepl2198"
	drop := func() {
		bg := context.Background()
		_, _ = setup.Exec(bg, "REVOKE ALL ON "+table+" FROM "+role)
		_, _ = setup.Exec(bg, "DROP ROLE IF EXISTS "+role)
	}
	t.Cleanup(drop)
	for _, q := range []string{
		"CREATE ROLE " + role + " LOGIN NOREPLICATION PASSWORD '" + pw + "'",
		"GRANT SELECT ON " + table + " TO " + role,
	} {
		if _, err := setup.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	u.User = url.UserPassword(role, pw)
	return u.String()
}
