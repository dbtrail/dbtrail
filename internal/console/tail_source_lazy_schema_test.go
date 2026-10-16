package console

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/views"
)

// The router opens each server's index lazily (Config.LazyIndex), so its
// bundles carry no schema resolver. A tail needs one for the table's key:
// without loading it here, every table of every server was refused as if the
// index had no schema snapshot, and the router's copy never answered newer
// than its snapshot.

const (
	latestSnapshotRE = `SELECT COALESCE\(MAX\(snapshot_id\), 0\) FROM schema_snapshots`
	snapshotRowsRE   = `SELECT schema_name, table_name, column_name, ordinal_position`
	snapshotTimeRE   = `SELECT MIN\(snapshot_time\) FROM schema_snapshots WHERE snapshot_id = \?`
	exclusionsRE     = `FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE\(\) AND TABLE_NAME = 'snapshot_exclusions'`
)

var snapshotCols = []string{"schema_name", "table_name", "column_name", "ordinal_position", "column_key", "data_type",
	"column_type", "is_generated", "is_identity_always", "character_set_name", "is_nullable"}

// expectSnapshotLoad expects metadata.NewResolver(db, id) reading tables, each
// with an int id key and one more column.
func expectSnapshotLoad(mock sqlmock.Sqlmock, id int, tables ...string) {
	rows := sqlmock.NewRows(snapshotCols)
	for _, tbl := range tables {
		schema, table, _ := strings.Cut(tbl, ".")
		rows.AddRow(schema, table, "id", 1, "PRI", "int", "int", false, false, "", "NO")
		rows.AddRow(schema, table, "status", 2, "", "varchar", "varchar(32)", false, false, "utf8mb4", "YES")
	}
	mock.ExpectQuery(snapshotRowsRE).WithArgs(id).WillReturnRows(rows)
	mock.ExpectQuery(snapshotTimeRE).WithArgs(id).WillReturnRows(sqlmock.NewRows([]string{"t"}).AddRow(time.Date(2026, 10, 2, 11, 50, 25, 0, time.UTC)))
	mock.ExpectQuery(exclusionsRE).WillReturnRows(sqlmock.NewRows([]string{"e"}).AddRow(false))
}

func expectLatest(mock sqlmock.Sqlmock, id int) {
	mock.ExpectQuery(latestSnapshotRE).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(id))
}

// lazyTailRig is a router's tail source: a bundle with no resolver, marked
// unavailable as a lazily opened index marks it, and a copy file per table
// whose footer passes every rule before the key is read.
func lazyTailRig(t *testing.T) (*indexTailSource, sqlmock.Sqlmock, func(schema, table string) views.BaselineTable) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	mock.MatchExpectationsInOrder(true)
	t.Cleanup(func() { db.Close() })
	x := newIndexTailSource(&Server{}, &bundle{db: db, resolverUnavailable: true}).capturing(nil)
	root := t.TempDir()
	table := func(schema, table string) views.BaselineTable {
		path := filepath.Join(root, schema, table+".parquet")
		stamp := "2026-10-16T01:00:25Z"
		writeFooterFile(t, path, map[string]string{
			baseline.MetaKeyBinlogFile: "mysql-bin-changelog.004194", baseline.MetaKeyBinlogPos: "1447",
			baseline.MetaKeySnapshotTimestamp: stamp, baseline.MetaKeyLastDumpAt: stamp, baseline.MetaKeyFoldGeneration: "0",
			baseline.MetaKeySnapshotProducer: baseline.ProducerDump, baseline.MetaKeyLockMode: string(baseline.LockModeFTWRL),
			baseline.MetaKeyCreateTableSQL: "CREATE TABLE `" + table + "` (\n  `id` int NOT NULL,\n  `status` varchar(32) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n",
		})
		return views.BaselineTable{Schema: schema, Table: table, Path: path}
	}
	return x, mock, table
}

func TestIndexTailSource_lazyIndexLoadsTheSchemaSnapshot(t *testing.T) {
	ctx := context.Background()

	t.Run("a table in the snapshot gets a tail", func(t *testing.T) {
		x, mock, table := lazyTailRig(t)
		expectLatest(mock, 12)
		expectSnapshotLoad(mock, 12, "sbtest.sbtest1", "demo.customers")
		tail, why, err := x.Start(ctx, table("sbtest", "sbtest1"))
		if err != nil || why != "" || tail == nil {
			t.Fatalf("Start = %v, %q, %v; want a tail", tail, why, err)
		}
		if got := tail.PKColumns(); len(got) != 1 || got[0] != "id" {
			t.Errorf("key = %v, want [id]", got)
		}
		// The second table of the same snapshot: asked which snapshot is the
		// newest, and not read again.
		expectLatest(mock, 12)
		if tail, why, err := x.Start(ctx, table("demo", "customers")); err != nil || why != "" || tail == nil {
			t.Fatalf("second table: %v, %q, %v", tail, why, err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
	})

	t.Run("a newer snapshot is read, and a table added in it is found", func(t *testing.T) {
		x, mock, table := lazyTailRig(t)
		expectLatest(mock, 12)
		expectSnapshotLoad(mock, 12, "sbtest.sbtest1")
		if _, why, err := x.Start(ctx, table("demo", "orders2")); err != nil || !strings.Contains(why, "could not be found in the index's schema snapshot") {
			t.Fatalf("a table the snapshot lacks: %q, %v", why, err)
		}
		expectLatest(mock, 13)
		expectSnapshotLoad(mock, 13, "sbtest.sbtest1", "demo.orders2")
		if tail, why, err := x.Start(ctx, table("demo", "orders2")); err != nil || why != "" || tail == nil {
			t.Fatalf("after snapshot 13: %v, %q, %v", tail, why, err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
	})

	t.Run("an index with no snapshot says so", func(t *testing.T) {
		x, mock, table := lazyTailRig(t)
		expectLatest(mock, 0)
		if _, why, err := x.Start(ctx, table("sbtest", "sbtest1")); err != nil || !strings.Contains(why, "the index has no schema snapshot to read sbtest.sbtest1's key from") {
			t.Fatalf("no snapshot: %q, %v", why, err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
	})

	t.Run("a snapshot that cannot be read is an error, not a refusal, and is asked again", func(t *testing.T) {
		x, mock, table := lazyTailRig(t)
		mock.ExpectQuery(latestSnapshotRE).WillReturnError(errors.New("Error 1146: Table 'schema_snapshots' doesn't exist"))
		// An error, so the keeper asks again at the next statement; a
		// refusal would stand until the copy's next snapshot.
		_, why, err := x.Start(ctx, table("sbtest", "sbtest1"))
		if err == nil || why != "" || !strings.Contains(err.Error(), "1146") {
			t.Fatalf("unreadable: why %q, err %v; want an error naming 1146 and no refusal", why, err)
		}
		expectLatest(mock, 12)
		expectSnapshotLoad(mock, 12, "sbtest.sbtest1")
		if tail, why, err := x.Start(ctx, table("sbtest", "sbtest1")); err != nil || why != "" || tail == nil {
			t.Fatalf("after the index answers: %v, %q, %v", tail, why, err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
	})

	t.Run("a newer snapshot whose rows cannot be read is an error, and is read again", func(t *testing.T) {
		x, mock, table := lazyTailRig(t)
		expectLatest(mock, 12)
		expectSnapshotLoad(mock, 12, "sbtest.sbtest1")
		if tail, why, err := x.Start(ctx, table("sbtest", "sbtest1")); err != nil || why != "" || tail == nil {
			t.Fatalf("snapshot 12: %v, %q, %v", tail, why, err)
		}
		expectLatest(mock, 13)
		mock.ExpectQuery(snapshotRowsRE).WithArgs(13).WillReturnError(errors.New("connection reset"))
		if _, why, err := x.Start(ctx, table("sbtest", "sbtest1")); err == nil || why != "" || !strings.Contains(err.Error(), "connection reset") {
			t.Fatalf("rows of 13 unreadable: why %q, err %v", why, err)
		}
		// 13 is not taken as loaded: the next Start reads it.
		expectLatest(mock, 13)
		expectSnapshotLoad(mock, 13, "sbtest.sbtest1")
		if tail, why, err := x.Start(ctx, table("sbtest", "sbtest1")); err != nil || why != "" || tail == nil {
			t.Fatalf("13 read again: %v, %q, %v", tail, why, err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
	})
}

// A bundle that loaded its resolver (watch: the index is opened eagerly)
// keeps using it, and nothing is asked of the index.
func TestIndexTailSource_eagerBundleKeepsItsResolver(t *testing.T) {
	x, mock, table := lazyTailRig(t)
	x.b.resolverUnavailable = false
	x.b.resolver = metadata.NewResolverFromTables(3, map[string]*metadata.TableMeta{
		"sbtest.sbtest1": {Schema: "sbtest", Table: "sbtest1", Columns: []metadata.ColumnMeta{{Name: "id", OrdinalPosition: 1, IsPK: true, DataType: "int"}}},
	})
	if tail, why, err := x.Start(context.Background(), table("sbtest", "sbtest1")); err != nil || why != "" || tail == nil {
		t.Fatalf("an eager bundle's own resolver: %v, %q, %v", tail, why, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// A bundle opened eagerly before its index had any snapshot (watch, a server
// just added) reads the snapshot that appeared since.
func TestIndexTailSource_eagerBundleWithNoSnapshotYetReadsOne(t *testing.T) {
	x, mock, table := lazyTailRig(t)
	x.b.resolverUnavailable = false
	expectLatest(mock, 1)
	expectSnapshotLoad(mock, 1, "sbtest.sbtest1")
	if tail, why, err := x.Start(context.Background(), table("sbtest", "sbtest1")); err != nil || why != "" || tail == nil {
		t.Fatalf("a snapshot taken after the bundle opened: %v, %q, %v", tail, why, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}
