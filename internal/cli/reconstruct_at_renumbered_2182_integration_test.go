//go:build integration

package cli

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2182 through the command: `bintrail reconstruct --at <past>` bounds the
// binlog-renumbering check by --at, and without --at it keeps today's check
// over the whole index. The numbering starts over at a time still in the
// future, so the run without --at (target: now) is the one place the two
// checks disagree.
func TestRunReconstruct_atBeforeABinlogRenumbering_2182(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	for i, col := range []struct{ name, key, typ string }{{"id", "PRI", "int"}, {"status", "", "varchar"}} {
		testutil.MustExec(t, db, `INSERT INTO schema_snapshots
			(snapshot_id, snapshot_time, schema_name, table_name, column_name, ordinal_position, column_key, data_type, is_nullable, is_generated)
			VALUES (1, UTC_TIMESTAMP() - INTERVAL 1 DAY, 'testdb', 'orders', ?, ?, ?, ?, 'NO', 0)`, col.name, i+1, col.key, col.typ)
	}
	testutil.MustExec(t, db, `INSERT INTO stream_state (id, mode, binlog_file, binlog_position, last_checkpoint, server_id)
		VALUES (1, 'position', 'binlog.000001', 4, NOW(), 1)`)

	N := time.Now().UTC().Truncate(time.Hour)
	var hours []time.Time
	for h := N.Add(-4 * time.Hour); !h.After(N.Add(3 * time.Hour)); h = h.Add(time.Hour) {
		hours = append(hours, h)
	}
	testutil.SetupPartitionedTable(t, db, dbName, hours)
	ins := func(id uint64, file string, start uint64, at time.Time, pk, after string) {
		t.Helper()
		testutil.MustExec(t, db, `INSERT INTO binlog_events
			(event_id, binlog_file, start_pos, end_pos, event_timestamp, schema_name, table_name, event_type, pk_values, row_before, row_after)
			VALUES (?, ?, ?, ?, ?, 'testdb', 'orders', 2, ?, ?, ?)`,
			id, file, start, start+100, at.Format("2006-01-02 15:04:05"), pk,
			[]byte(`{"id":`+pk+`,"status":"before"}`), []byte(after))
	}
	snapAt := N.Add(-2 * time.Hour)
	// The snapshot's mark: the last change before it, binlog.000007:100-200.
	ins(10, "binlog.000007", 100, snapAt.Add(-30*time.Minute), "1", `{"id":1,"status":"A"}`)
	// A change in the same numbering, then the numbering starts over 90
	// minutes from the top of this hour.
	ins(15, "binlog.000007", 300, N.Add(-time.Hour), "2", `{"id":2,"status":"X"}`)
	ins(20, "binlog.000001", 300, N.Add(90*time.Minute), "1", `{"id":1,"status":"B"}`)

	createSQL := "CREATE TABLE `orders` (\n  `id` INT NOT NULL,\n  `status` VARCHAR(64) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n"
	baselineDir := t.TempDir()
	parquetDir := filepath.Join(baselineDir, strings.ReplaceAll(snapAt.Format(time.RFC3339), ":", "-"), "testdb")
	if err := os.MkdirAll(parquetDir, 0o755); err != nil {
		t.Fatal(err)
	}
	cols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "status", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	}
	bw, err := baseline.NewWriter(filepath.Join(parquetDir, "orders.parquet"), cols, baseline.WriterConfig{
		Compression: "none", RowGroupSize: 100,
		Metadata: map[string]string{
			baseline.MetaKeyCreateTableSQL: createSQL,
			baseline.MetaKeyBinlogFile:     "binlog.000007",
			baseline.MetaKeyBinlogPos:      strconv.Itoa(200),
			baseline.MetaKeyEventMark:      reconstruct.EventMark{ID: 10, File: "binlog.000007", End: 200}.Encode(),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range [][]string{{"1", "A"}, {"2", "paid"}, {"3", "shipped"}} {
		if err := bw.WriteRow(row, []bool{false, false}); err != nil {
			t.Fatal(err)
		}
	}
	if err := bw.Close(); err != nil {
		t.Fatal(err)
	}

	orig := captureRecFlags()
	savedOutputFormat, savedOutputDir, savedTables := recOutputFormat, recOutputDir, recTables
	savedChunkSize, savedParallelism, savedFetchBatch := recChunkSize, recParallelism, recFetchBatch
	t.Cleanup(func() {
		applyRecFlags(orig)
		recOutputFormat, recOutputDir, recTables = savedOutputFormat, savedOutputDir, savedTables
		recChunkSize, recParallelism, recFetchBatch = savedChunkSize, savedParallelism, savedFetchBatch
	})
	recIndexDSN = testutil.SnapshotDSN(dbName)
	recBaselineDir = baselineDir
	recBaselineS3 = ""
	recAllowGaps = false
	recNoArchive = true
	recOutputFormat = "mydumper"
	recTables = "testdb.orders"
	recChunkSize = "256MB"
	recParallelism = 1
	recFetchBatch = 0
	reconstructCmd.SetContext(context.Background())
	t.Cleanup(func() { reconstructCmd.SetContext(nil) })

	run := func(at string) (map[string]string, error) {
		recAt = at
		recOutputDir = t.TempDir()
		err := runReconstruct(reconstructCmd, nil)
		return readDumpDir(t, recOutputDir), err
	}

	for _, tc := range []struct{ name, at string }{
		{"no --at: today's check over the whole index", ""},
		{"--at after the restart", N.Add(2 * time.Hour).Format(time.RFC3339)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := run(tc.at); !errors.Is(err, reconstruct.ErrBinlogRenumbered) {
				t.Fatalf("err = %v, want ErrBinlogRenumbered", err)
			}
		})
	}
	t.Run("--at before the restart", func(t *testing.T) {
		dump, err := run(N.Add(-30 * time.Minute).Format(time.RFC3339))
		if err != nil {
			t.Fatalf("--at before the restart: %v", err)
		}
		var all strings.Builder
		for _, body := range dump {
			all.WriteString(body)
		}
		for _, want := range []string{"'A'", "'X'", "'shipped'"} {
			if !strings.Contains(all.String(), want) {
				t.Errorf("the dump does not hold %s", want)
			}
		}
		for _, bad := range []string{"'B'", "'paid'"} {
			if strings.Contains(all.String(), bad) {
				t.Errorf("the dump holds %s", bad)
			}
		}
	})
}
