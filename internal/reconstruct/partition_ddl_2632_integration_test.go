//go:build integration

package reconstruct_test

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// An ALTER TABLE that changes rows with no row events (#2632), through the
// real refresh path and the real schema_changes table, whose query sqlmock
// cannot run. The rows are written by the writer capture uses.
func recordAlter(t *testing.T, db *sql.DB, table string, endPos uint64, ranAt time.Time, query string) {
	t.Helper()
	if err := indexer.InsertSchemaChange(db, event.Event{
		BinlogFile: "binlog.000003",
		EndPos:     endPos,
		Timestamp:  ranAt,
		Schema:     "shop",
		Table:      table,
		EventType:  event.EventDDL,
		DDLType:    event.DDLAlterTable,
		DDLQuery:   query,
	}, nil); err != nil {
		t.Fatalf("InsertSchemaChange: %v", err)
	}
}

// A DROP PARTITION after the snapshot, and no row change since: the table
// would be carried forward as it was, every row of the partition with it.
func TestReconstructParquet_aDroppedPartitionRefuses(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dsn, root, base := lateDDLIndex(t)

	recordAlter(t, db, "orders", 600, base.Add(10*time.Second), "ALTER TABLE `orders` DROP PARTITION p2020")
	err := foldOrders(dsn, root, root, reconstruct.OutputFormatParquet, base.Add(30*time.Second))
	if !errors.Is(err, reconstruct.ErrDestructiveDDL) {
		t.Fatalf("the refresh after a DROP PARTITION: %v, want a destructive-DDL refusal", err)
	}
	for _, want := range []string{"ALTER TABLE ... DROP PARTITION on shop.orders", "binlog.000003:600"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not say %q: %v", want, err)
		}
	}
}

// An EXCHANGE recorded under another table, and in upper case, refuses the
// table it names after WITH TABLE.
func TestReconstructParquet_anExchangeRefusesTheTableItNames(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dsn, root, base := lateDDLIndex(t)

	recordAlter(t, db, "orders_parted", 600, base.Add(10*time.Second),
		"ALTER TABLE orders_parted EXCHANGE PARTITION P0 WITH TABLE ORDERS WITHOUT VALIDATION")
	err := foldOrders(dsn, root, root, reconstruct.OutputFormatParquet, base.Add(30*time.Second))
	if !errors.Is(err, reconstruct.ErrDestructiveDDL) || !strings.Contains(err.Error(), "EXCHANGE PARTITION") {
		t.Fatalf("the refresh of the table an EXCHANGE named: %v, want a refusal naming it", err)
	}
}

// What keeps every row refuses nothing: an ADD PARTITION on the table, a
// DROP PARTITION on another table, an EXCHANGE with a table whose name only
// starts the same, a CREATE that partitions. The refresh publishes.
func TestReconstructParquet_partitionDDLThatKeepsTheRowsRefusesNothing(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dsn, root, base := lateDDLIndex(t)

	recordAlter(t, db, "orders", 600, base.Add(5*time.Second),
		"ALTER TABLE orders ADD PARTITION (PARTITION p2031 VALUES LESS THAN (2032))")
	recordAlter(t, db, "events", 610, base.Add(6*time.Second), "ALTER TABLE events DROP PARTITION p2020")
	recordAlter(t, db, "t", 620, base.Add(7*time.Second), "ALTER TABLE t EXCHANGE PARTITION p0 WITH TABLE orders_archive")
	if err := indexer.InsertSchemaChange(db, event.Event{
		BinlogFile: "binlog.000003", EndPos: 630, Timestamp: base.Add(8 * time.Second),
		Schema: "shop", Table: "orders", EventType: event.EventDDL, DDLType: event.DDLCreateTable,
		DDLQuery: "CREATE TABLE IF NOT EXISTS orders (id INT PRIMARY KEY) PARTITION BY HASH (id) PARTITIONS 4",
	}, nil); err != nil {
		t.Fatalf("InsertSchemaChange: %v", err)
	}
	rowChange(t, db, "binlog.000003", 700, 800, base.Add(10*time.Second), "1", `{"id":1,"status":"A"}`)
	if err := foldOrders(dsn, root, root, reconstruct.OutputFormatParquet, base.Add(30*time.Second)); err != nil {
		t.Fatalf("the refresh refused on a statement that keeps every row: %v", err)
	}
	out, _, _, err := reconstruct.FindBaseline(context.Background(), root, "shop", "orders", base.Add(31*time.Second))
	if err != nil {
		t.Fatalf("FindBaseline: %v", err)
	}
	if got, want := readOrders(t, out), []string{"1=A", "2=paid", "3=shipped"}; !equalStrings(got, want) {
		t.Fatalf("snapshot = %v, want %v", got, want)
	}
}

// An ALTER the index column cannot hold as sent, too long or not UTF-8,
// used to fail its INSERT under strict mode, and the refusal was lost with
// the row. It is stored cut and repaired, and still refuses.
func TestReconstructParquet_aLongOrNonUTF8DropPartitionStillRefuses(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	for name, query := range map[string]string{
		"longer than the column": "ALTER TABLE orders DROP PARTITION " + strings.Repeat("p_2024010100, ", 6000) + "p_last",
		"a latin1 byte":          "ALTER TABLE orders /* migraci\xf3n */ DROP PARTITION p2020",
	} {
		t.Run(name, func(t *testing.T) {
			db, dsn, root, base := lateDDLIndex(t)
			recordAlter(t, db, "orders", 600, base.Add(10*time.Second), query)
			err := foldOrders(dsn, root, root, reconstruct.OutputFormatParquet, base.Add(30*time.Second))
			if !errors.Is(err, reconstruct.ErrDestructiveDDL) || !strings.Contains(err.Error(), "DROP PARTITION") {
				t.Fatalf("the refresh after the DROP PARTITION: %v, want a refusal naming it", err)
			}
		})
	}
}
