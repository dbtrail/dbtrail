//go:build integration

package verify

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// writeLockedTestBaseline is writeTestBaseline with a lock record in the
// footer (none when lock is empty).
func writeLockedTestBaseline(t *testing.T, baseDir string, ts time.Time, dbName, createSQL string,
	cols []baseline.Column, rows [][]string, anchorPos int64, lock string) {
	t.Helper()
	snapDir := filepath.Join(baseDir, strings.ReplaceAll(ts.Format(time.RFC3339), ":", "-"))
	if err := os.MkdirAll(filepath.Join(snapDir, dbName), 0o755); err != nil {
		t.Fatal(err)
	}
	md := readFooter(ts, createSQL)
	md[baseline.MetaKeyBinlogFile] = "binlog.000001"
	md[baseline.MetaKeyBinlogPos] = strconv.FormatInt(anchorPos, 10)
	if lock != "" {
		md[baseline.MetaKeyLockMode] = lock
	}
	bw, err := baseline.NewWriter(filepath.Join(snapDir, dbName, "orders.parquet"), cols,
		baseline.WriterConfig{Compression: "zstd", RowGroupSize: 100, Metadata: md})
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if err := bw.WriteRow(row, make([]bool, len(row))); err != nil {
			t.Fatal(err)
		}
	}
	if err := bw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := baseline.WriteSuccessMarker(snapDir); err != nil {
		t.Fatal(err)
	}
}

// #1380, through the real comparison: the same difference between two
// snapshots is a mismatch when both were locked, a mismatch that says so
// when one has no record, and inconclusive when one was read with no locks.
// A pair that agrees is a match whatever the locks were.
func TestVerifyBaselinePair_snapshotLock(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	for _, c := range []struct {
		name, key, dt, colType string
		ord                    int
	}{
		{"id", "PRI", "int", "int", 1},
		{"status", "", "varchar", "varchar(64)", 2},
	} {
		testutil.MustExec(t, db, `INSERT INTO schema_snapshots
			(snapshot_id, snapshot_time, schema_name, table_name, column_name,
			 ordinal_position, column_key, data_type, column_type, is_nullable, is_generated)
			VALUES (1, UTC_TIMESTAMP(), ?, 'orders', ?, ?, ?, ?, ?, 'YES', 0)`,
			dbName, c.name, c.ord, c.key, c.dt, c.colType)
	}
	now := time.Now().UTC()
	prevTS := now.Truncate(time.Hour).Add(-2 * time.Hour)
	newTS := prevTS.Add(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{prevTS, newTS, now.Truncate(time.Hour)})
	ets := prevTS.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300, ets, nil, dbName, "orders", 2 /*UPDATE*/, "2", nil,
		[]byte(`{"id":2,"status":"b"}`), []byte(`{"id":2,"status":"shipped"}`))

	createSQL := "CREATE TABLE `orders` (\n  `id` INT NOT NULL,\n  `status` VARCHAR(64),\n  PRIMARY KEY (`id`)\n);\n"
	cols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "status", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	}
	prevRows := [][]string{{"1", "a"}, {"2", "b"}}
	agrees := [][]string{{"1", "a"}, {"2", "shipped"}}
	differs := [][]string{{"1", "changed"}, {"2", "shipped"}}

	resolver, err := metadata.NewResolver(db, 1)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	cfg := BaselineConfig{IndexDB: db, Resolver: resolver, IndexDBName: dbName, NoArchive: true}

	for _, tc := range []struct {
		name              string
		prevLock, newLock string
		newRows           [][]string
		want              Status
		lock              string
		says, neverSays   string
	}{
		{"locked, and they differ", "ftwrl", "lock-all", differs, StatusMismatch, "consistent", "content digest differs", "locks"},
		{"no record, and they differ", "", "", differs, StatusMismatch, "unknown", "records how it was locked, so they may have been taken with no locks", "was taken with no locks"},
		{"no record on the read only, and they differ", "ftwrl", "", differs, StatusMismatch, "unknown", "does not record how it was locked, so it may have been taken with no locks", "Neither"},
		{"the read took no locks, and they differ", "ftwrl", "no-lock", differs, StatusInconclusive, "torn", "was taken with no locks", "does not record"},
		{"the older one took no locks, and they differ", "no-lock", "ftwrl", differs, StatusInconclusive, "torn", "was taken with no locks", "does not record"},
		{"no locks, and they agree", "no-lock", "no-lock", agrees, StatusMatch, "torn", "", "locks"},
		{"no record, and they agree", "", "", agrees, StatusMatch, "unknown", "", "locks"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			baseDir := t.TempDir()
			writeLockedTestBaseline(t, baseDir, prevTS, dbName, createSQL, cols, prevRows, 200, tc.prevLock)
			writeLockedTestBaseline(t, baseDir, newTS, dbName, createSQL, cols, tc.newRows, 300, tc.newLock)
			pairs, _, err := FindBaselinePair(context.Background(), baseDir)
			if err != nil || len(pairs) != 1 || pairs[0].Settled != nil {
				t.Fatalf("pairs = %+v, err = %v", pairs, err)
			}
			got, err := VerifyBaselinePair(context.Background(), cfg, pairs[0])
			if err != nil {
				t.Fatalf("VerifyBaselinePair: %v", err)
			}
			if got.Status != tc.want || got.SnapshotLock != tc.lock {
				t.Fatalf("%s with lock %q (%s); want %s with lock %q", got.Status, got.SnapshotLock, got.Detail, tc.want, tc.lock)
			}
			if (got.Status == StatusInconclusive) != (got.InconclusiveKind == InconclusiveTornSnapshot) {
				t.Errorf("%s with kind %q", got.Status, got.InconclusiveKind)
			}
			if tc.says != "" && !strings.Contains(got.Detail, tc.says) {
				t.Errorf("the reason does not say %q: %q", tc.says, got.Detail)
			}
			if strings.Contains(got.Detail, tc.neverSays) {
				t.Errorf("the reason says %q: %q", tc.neverSays, got.Detail)
			}
		})
	}
}
