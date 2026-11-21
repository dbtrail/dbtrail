//go:build integration

package pgcapture

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestQueryPartitions_Integration is what capture's read of a relation's
// partitions answers on a live PostgreSQL, one table per shape the decoder
// decides on (Decoder.judgePartitions). The log prints each answer.
func TestQueryPartitions_Integration(t *testing.T) {
	dsn := testutil.SkipIfNoPostgres(t)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { conn.Close(context.Background()) })

	for _, tc := range []struct {
		name  string
		table string
		setup []string
		want  Partitions
	}{
		{"a table that is not partitioned", "pq_plain",
			[]string{"CREATE TABLE pq_plain (id int PRIMARY KEY, v text)"},
			Partitions{Known: true}},
		{"every partition at FULL", "pq_allfull",
			[]string{
				"CREATE TABLE pq_allfull (id int PRIMARY KEY, v text) PARTITION BY RANGE (id)",
				"CREATE TABLE pq_allfull_1 PARTITION OF pq_allfull FOR VALUES FROM (0) TO (10)",
				"ALTER TABLE pq_allfull_1 REPLICA IDENTITY FULL",
			},
			Partitions{Known: true, Partitioned: true}},
		{"one partition at DEFAULT under the root's key, one at FULL, one at NOTHING", "pq_mixed",
			[]string{
				"CREATE TABLE pq_mixed (id int PRIMARY KEY, v text) PARTITION BY RANGE (id)",
				"CREATE TABLE pq_mixed_1 PARTITION OF pq_mixed FOR VALUES FROM (0) TO (10)",
				"CREATE TABLE pq_mixed_2 PARTITION OF pq_mixed FOR VALUES FROM (10) TO (20)",
				"ALTER TABLE pq_mixed_2 REPLICA IDENTITY FULL",
				"CREATE TABLE pq_mixed_3 PARTITION OF pq_mixed FOR VALUES FROM (20) TO (30)",
				"ALTER TABLE pq_mixed_3 REPLICA IDENTITY NOTHING",
			},
			Partitions{Known: true, Partitioned: true, BelowFull: 2}},
		{"a partition of a partition, at DEFAULT", "pq_sub",
			[]string{
				"CREATE TABLE pq_sub (id int PRIMARY KEY, v text) PARTITION BY RANGE (id)",
				"CREATE TABLE pq_sub_1 PARTITION OF pq_sub FOR VALUES FROM (0) TO (100) PARTITION BY RANGE (id)",
				"CREATE TABLE pq_sub_1_1 PARTITION OF pq_sub_1 FOR VALUES FROM (0) TO (10)",
			},
			Partitions{Known: true, Partitioned: true, BelowFull: 1}},
		// The root has no key, so the partition's own key is outside it.
		{"a root without a primary key over a partition with its own, at DEFAULT", "pq_nokey",
			[]string{
				"CREATE TABLE pq_nokey (id int NOT NULL, v text) PARTITION BY RANGE (id)",
				"CREATE TABLE pq_nokey_1 PARTITION OF pq_nokey FOR VALUES FROM (0) TO (10)",
				"ALTER TABLE pq_nokey_1 ADD PRIMARY KEY (id)",
				"CREATE TABLE pq_nokey_2 PARTITION OF pq_nokey FOR VALUES FROM (10) TO (20)",
				"ALTER TABLE pq_nokey_2 REPLICA IDENTITY FULL",
			},
			Partitions{Known: true, Partitioned: true, BelowFull: 1, Wide: 1}},
		{"a root without a primary key over a partition with a unique index as its identity", "pq_nokeyidx",
			[]string{
				"CREATE TABLE pq_nokeyidx (id int NOT NULL, v text) PARTITION BY RANGE (id)",
				"CREATE TABLE pq_nokeyidx_1 PARTITION OF pq_nokeyidx FOR VALUES FROM (0) TO (10)",
				"CREATE UNIQUE INDEX pq_nokeyidx_1_uk ON pq_nokeyidx_1 (id)",
				"ALTER TABLE pq_nokeyidx_1 REPLICA IDENTITY USING INDEX pq_nokeyidx_1_uk",
			},
			Partitions{Known: true, Partitioned: true, BelowFull: 1, Wide: 1}},
		{"a partition whose identity index is wider than the primary key", "pq_wide",
			[]string{
				"CREATE TABLE pq_wide (id int, id2 int NOT NULL, u int NOT NULL, PRIMARY KEY (id, id2)) PARTITION BY RANGE (id2)",
				"CREATE TABLE pq_wide_1 PARTITION OF pq_wide FOR VALUES FROM (0) TO (10)",
				"CREATE UNIQUE INDEX pq_wide_1_uk ON pq_wide_1 (id, id2, u)",
				"ALTER TABLE pq_wide_1 REPLICA IDENTITY USING INDEX pq_wide_1_uk",
				"CREATE TABLE pq_wide_2 PARTITION OF pq_wide FOR VALUES FROM (10) TO (20)",
			},
			Partitions{Known: true, Partitioned: true, BelowFull: 2, Wide: 1}},
		// Whatever the root's identity (here DEFAULT). The index's u is outside
		// the root's key too.
		{"a partition whose identity index leaves the primary key out, root at DEFAULT", "pq_miss",
			[]string{
				"CREATE TABLE pq_miss (id int, id2 int NOT NULL, u int NOT NULL, PRIMARY KEY (id, id2)) PARTITION BY RANGE (id2)",
				"CREATE TABLE pq_miss_1 PARTITION OF pq_miss FOR VALUES FROM (0) TO (10)",
				"CREATE UNIQUE INDEX pq_miss_1_uk ON pq_miss_1 (u, id2)",
				"ALTER TABLE pq_miss_1 REPLICA IDENTITY USING INDEX pq_miss_1_uk",
			},
			Partitions{Known: true, Partitioned: true, BelowFull: 1, Wide: 1, MissesKey: 1}},
		// The identity index is the primary key's columns in another order.
		{"a partition whose identity index is the primary key again", "pq_same",
			[]string{
				"CREATE TABLE pq_same (id int, id2 int NOT NULL, PRIMARY KEY (id, id2)) PARTITION BY RANGE (id2)",
				"CREATE TABLE pq_same_1 PARTITION OF pq_same FOR VALUES FROM (0) TO (10)",
				"CREATE UNIQUE INDEX pq_same_1_uk ON pq_same_1 (id2, id)",
				"ALTER TABLE pq_same_1 REPLICA IDENTITY USING INDEX pq_same_1_uk",
			},
			Partitions{Known: true, Partitioned: true, BelowFull: 1}},
		// The partition's key column has another attribute number than the
		// root's (a dropped column before it, and another column order): the
		// keys are compared by name, and are the same key.
		{"a partition attached with its columns numbered otherwise", "pq_attnum",
			[]string{
				"CREATE TABLE pq_attnum (id int PRIMARY KEY, v text) PARTITION BY RANGE (id)",
				"CREATE TABLE pq_attnum_1 (junk int, v text, id int NOT NULL)",
				"ALTER TABLE pq_attnum_1 DROP COLUMN junk",
				"ALTER TABLE pq_attnum ATTACH PARTITION pq_attnum_1 FOR VALUES FROM (0) TO (10)",
			},
			Partitions{Known: true, Partitioned: true, BelowFull: 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			drop := func() { _, _ = conn.Exec(context.Background(), "DROP TABLE IF EXISTS "+tc.table+" CASCADE") }
			drop()
			t.Cleanup(drop)
			for _, q := range tc.setup {
				if _, err := conn.Exec(ctx, q); err != nil {
					t.Fatalf("%s: %v", q, err)
				}
			}
			var oid uint32
			if err := conn.QueryRow(ctx, "SELECT '"+tc.table+"'::regclass::oid").Scan(&oid); err != nil {
				t.Fatal(err)
			}
			got, err := queryPartitions(ctx, conn, oid)
			t.Logf("%s -> %+v, %v", tc.table, got, err)
			if err != nil || got != tc.want {
				t.Errorf("partitions = %+v, %v; want %+v", got, err, tc.want)
			}
		})
	}
}
