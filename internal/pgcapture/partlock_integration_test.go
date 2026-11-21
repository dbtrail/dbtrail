//go:build integration

package pgcapture

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestPartitionCatalogReadsTakeNoLock_Integration holds an ACCESS EXCLUSIVE
// lock on one partition, as a VACUUM FULL, a rewriting ALTER or a long TRUNCATE
// does, and runs the two reads capture and the doctor make of a partitioned
// table. They read pg_inherits and take no lock on any partition, so they
// answer at once. pg_partition_tree(), which they do not use, locks every
// partition and waits: with it, one such statement on one partition made the
// read time out and took capture of every table down with it. The log prints
// what this server does with each.
func TestPartitionCatalogReadsTakeNoLock_Integration(t *testing.T) {
	dsn := testutil.SkipIfNoPostgres(t)
	ctx := context.Background()
	const pub = "bintrail_partlock_pub"
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { conn.Close(context.Background()) })
	locker, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { locker.Close(context.Background()) })
	drop := func() {
		bg := context.Background()
		_, _ = conn.Exec(bg, "DROP PUBLICATION IF EXISTS "+pub)
		_, _ = conn.Exec(bg, "DROP TABLE IF EXISTS partlock_t")
	}
	drop()
	exec := func(c *pgx.Conn, q string) {
		t.Helper()
		if _, err := c.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	exec(conn, "CREATE TABLE partlock_t (id int PRIMARY KEY, v text) PARTITION BY RANGE (id)")
	exec(conn, "CREATE TABLE partlock_t_1 PARTITION OF partlock_t FOR VALUES FROM (0) TO (1000)")
	exec(conn, "CREATE TABLE partlock_t_2 PARTITION OF partlock_t FOR VALUES FROM (1000) TO (2000)")
	exec(conn, "ALTER TABLE partlock_t REPLICA IDENTITY FULL")
	exec(conn, "ALTER TABLE partlock_t_2 REPLICA IDENTITY FULL")
	exec(conn, "CREATE PUBLICATION "+pub+" FOR TABLE partlock_t WITH (publish_via_partition_root = true)")
	var oid uint32
	if err := conn.QueryRow(ctx, "SELECT 'partlock_t'::regclass::oid").Scan(&oid); err != nil {
		t.Fatal(err)
	}

	exec(locker, "BEGIN")
	exec(locker, "LOCK TABLE partlock_t_1 IN ACCESS EXCLUSIVE MODE")
	// Registered after drop's own cleanup would run: the lock must go first.
	t.Cleanup(drop)
	t.Cleanup(func() { _, _ = locker.Exec(context.Background(), "ROLLBACK") })

	const wait = 2 * time.Second
	timed := func(what string, run func(context.Context) error) (time.Duration, error) {
		qctx, cancel := context.WithTimeout(ctx, wait)
		defer cancel()
		start := time.Now()
		err := run(qctx)
		took := time.Since(start)
		t.Logf("%s, with a partition locked ACCESS EXCLUSIVE: took %s, error: %v", what, took.Round(time.Millisecond), err)
		return took, err
	}

	// What these reads replaced. A fresh connection: a query cancelled by its
	// deadline closes the connection it ran on.
	old, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { old.Close(context.Background()) })
	if _, treeErr := timed("pg_partition_tree()", func(c context.Context) error {
		var n int
		return old.QueryRow(c, "SELECT count(*) FROM pg_partition_tree('partlock_t')").Scan(&n)
	}); treeErr == nil {
		t.Log("pg_partition_tree() did not wait for the lock on this server")
	}

	var parts Partitions
	took, err := timed("capture's read of the partitions (pg_inherits)", func(c context.Context) error {
		var e error
		parts, e = queryPartitions(c, conn, oid)
		return e
	})
	if err != nil || took > wait/2 {
		t.Fatalf("capture's read of the partitions waited for the lock: %s, %v", took, err)
	}
	if want := (Partitions{Known: true, Partitioned: true, BelowFull: 1}); parts != want {
		t.Errorf("partitions = %+v, want %+v", parts, want)
	}

	var ids []TableIdentity
	took, err = timed("the doctor's read of the tables (pg_inherits)", func(c context.Context) error {
		var e error
		ids, e = QueryTableIdentities(c, conn, pub)
		return e
	})
	if err != nil || took > wait/2 {
		t.Fatalf("the doctor's read waited for the lock: %s, %v", took, err)
	}
	if len(ids) != 1 || len(ids[0].PartitionsBelowFull) != 1 || ids[0].PartitionsBelowFull[0] != `"public"."partlock_t_1"` {
		t.Errorf("identities = %+v, want the root listed for partlock_t_1", ids)
	}

	// A relation the catalog no longer holds is not known, never "not
	// partitioned": it may have been.
	gone, err := queryPartitions(ctx, conn, 4_000_000_000)
	if err != nil || gone != (Partitions{}) {
		t.Errorf("a relation that is gone: %+v, %v; want nothing known", gone, err)
	}
}
