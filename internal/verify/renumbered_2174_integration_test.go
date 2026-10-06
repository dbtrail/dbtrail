//go:build integration

package verify

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/serverid"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2174: the source's binary log started again after a snapshot (RESET
// BINARY LOGS AND GTIDS, a replaced server). verify reads the changes since
// the snapshot from its position, and every later change sorts below it: the
// reconstruction misses them and the table read as a mismatch with no cause.
// Both verify modes now run the check a refresh runs (#2160) and say the
// renumbering is the cause. The restart is simulated the way #2160's tests do
// it: changes indexed after the snapshot's event mark in a lower-numbered
// file.

const (
	renumberedOldUUID = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	renumberedNewUUID = "4f22ab58-71ca-11e1-9e33-c80aa9429563"
)

type renumberCase struct {
	name string
	// mark is the (previous) snapshot's event mark; "" writes none.
	mark string
	// startOver puts the change after the snapshot in binlog.000001.
	startOver bool
	// startOverLater indexes a change in binlog.000001 after the window's
	// end (the newer snapshot, or now).
	startOverLater bool
	// backfilled leaves a row in index_state, as `bintrail index` does.
	backfilled bool
	// captureReads, when set, is the server_uuid capture reads now.
	captureReads string
	// want is the status; refused cases are inconclusive.
	want Status
}

func renumberCases(withoutCheck Status) []renumberCase {
	sameServer := reconstruct.EventMark{ID: 10, File: "binlog.000007", End: 200}.Encode()
	withServer := reconstruct.EventMark{ID: 10, File: "binlog.000007", End: 200, ServerUUID: renumberedOldUUID}.Encode()
	return []renumberCase{
		// Today's behavior: the change is missed, which reads as withoutCheck.
		{name: "no event mark", startOver: true, want: withoutCheck},
		{name: "mark, same numbering", mark: sameServer, want: StatusMatch},
		{name: "mark, numbering started over", mark: sameServer, startOver: true, want: StatusInconclusive},
		{name: "mark, numbering started over after the window", mark: sameServer, startOverLater: true, want: StatusMatch},
		{name: "mark, backfilled index", mark: sameServer, startOver: true, backfilled: true, want: withoutCheck},
		{name: "mark names the server, capture reads it", mark: withServer, captureReads: renumberedOldUUID, want: StatusMatch},
		{name: "mark names the server, capture reads another", mark: withServer, captureReads: renumberedNewUUID, want: StatusInconclusive},
	}
}

// renumberIndex seeds the index side shared by both modes: the orders schema,
// the mark's own row (event 10, before the snapshot), the change after it
// (event 20, id 1 a→zzz), and what the case says about backfill and capture.
func renumberIndex(t *testing.T, db *sql.DB, dbName string, tc renumberCase, hours []time.Time, markAt, changeAt, laterAt time.Time) {
	t.Helper()
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
	testutil.SetupPartitionedTable(t, db, dbName, hours)
	insert := func(id uint64, file string, start uint64, at time.Time, pk, before, after string) {
		testutil.MustExec(t, db, `INSERT INTO binlog_events
			(event_id, binlog_file, start_pos, end_pos, event_timestamp, schema_name, table_name, event_type, pk_values, row_before, row_after)
			VALUES (?, ?, ?, ?, ?, ?, 'orders', 2, ?, ?, ?)`,
			id, file, start, start+100, at.Format("2006-01-02 15:04:05"), dbName, pk,
			[]byte(`{"id":`+pk+`,"status":"`+before+`"}`), []byte(`{"id":`+pk+`,"status":"`+after+`"}`))
	}
	insert(10, "binlog.000007", 100, markAt, "2", "x", "b")
	insert(20, changeFile(tc), 300, changeAt, "1", "a", "zzz")
	if tc.startOverLater {
		insert(30, "binlog.000001", 100, laterAt, "2", "b", "later")
	}
	if tc.backfilled {
		testutil.MustExec(t, db, `INSERT INTO index_state (binlog_file, file_size, last_position, events_indexed, status, started_at, completed_at)
			VALUES ('binlog.000005', 1, 150, 1, 'completed', UTC_TIMESTAMP(), UTC_TIMESTAMP())`)
	}
	if tc.captureReads != "" {
		testutil.MustExec(t, db, serverid.DDLBintrailServers)
		testutil.MustExec(t, db, `INSERT INTO bintrail_servers (bintrail_id, server_uuid, host, port, username) VALUES ('b1', ?, 'db', 3306, 'u')`, tc.captureReads)
	}
}

func changeFile(tc renumberCase) string {
	if tc.startOver {
		return "binlog.000001"
	}
	return "binlog.000007"
}

func writeMarkedBaseline(t *testing.T, baseDir string, ts time.Time, dbName string, rows [][]string, file string, pos int64, mark string) {
	t.Helper()
	snapDir := filepath.Join(baseDir, strings.ReplaceAll(ts.Format(time.RFC3339), ":", "-"))
	if err := os.MkdirAll(filepath.Join(snapDir, dbName), 0o755); err != nil {
		t.Fatal(err)
	}
	md := readFooter(ts, "CREATE TABLE `orders` (\n  `id` INT NOT NULL,\n  `status` VARCHAR(64),\n  PRIMARY KEY (`id`)\n);\n")
	md[baseline.MetaKeyBinlogFile] = file
	md[baseline.MetaKeyBinlogPos] = strconv.FormatInt(pos, 10)
	if mark != "" {
		md[baseline.MetaKeyEventMark] = mark
	}
	cols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "status", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	}
	bw, err := baseline.NewWriter(filepath.Join(snapDir, dbName, "orders.parquet"), cols,
		baseline.WriterConfig{Compression: "zstd", RowGroupSize: 100, Metadata: md})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		if err := bw.WriteRow(r, []bool{false, false}); err != nil {
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

func checkRenumberedVerdict(t *testing.T, tc renumberCase, got TableResult, extra ...string) {
	t.Helper()
	if got.Status != tc.want {
		t.Fatalf("status = %q (%s); want %q", got.Status, got.Detail, tc.want)
	}
	if tc.want != StatusInconclusive {
		return
	}
	if got.InconclusiveKind != "" {
		t.Errorf("inconclusive kind = %q; a renumbering needs attention, so it must not be a benign kind", got.InconclusiveKind)
	}
	want := append([]string{"new full snapshot is needed"}, extra...)
	if tc.captureReads != "" {
		want = append(want, renumberedNewUUID)
	} else {
		want = append(want, "binary log started again")
	}
	for _, w := range want {
		if !strings.Contains(got.Detail, w) {
			t.Errorf("the detail does not say %q: %s", w, got.Detail)
		}
	}
}

// Baseline-pair mode: the previous snapshot's mark against the changes read
// from its position up to the newer snapshot's.
func TestVerifyBaselinePair_afterTheBinlogNumberingStartsOver_2174(t *testing.T) {
	for _, tc := range renumberCases(StatusMismatch) {
		t.Run(tc.name, func(t *testing.T) {
			db, dbName := testutil.CreateTestDB(t)
			prevTS := time.Now().UTC().Truncate(time.Hour).Add(-2 * time.Hour)
			newTS := prevTS.Add(time.Hour)
			renumberIndex(t, db, dbName, tc, []time.Time{prevTS.Add(-time.Hour), prevTS, newTS}, prevTS.Add(-5*time.Minute), prevTS.Add(30*time.Minute), newTS.Add(10*time.Minute))
			if tc.captureReads != "" {
				testutil.MustExec(t, db, `INSERT INTO stream_state (id, mode, binlog_file, binlog_position, last_checkpoint, server_id, bintrail_id)
					VALUES (1, 'position', 'binlog.000007', 400, UTC_TIMESTAMP(), 1, 'b1')`)
			}

			baseDir := t.TempDir()
			writeMarkedBaseline(t, baseDir, prevTS, dbName, [][]string{{"1", "a"}, {"2", "b"}}, "binlog.000007", 200, tc.mark)
			// The newer snapshot was read from the source after the change, in
			// whatever numbering the source had by then.
			writeMarkedBaseline(t, baseDir, newTS, dbName, [][]string{{"1", "zzz"}, {"2", "b"}}, changeFile(tc), 400, "")

			resolver, err := metadata.NewResolver(db, 1)
			if err != nil {
				t.Fatal(err)
			}
			cfg := BaselineConfig{IndexDB: db, Resolver: resolver, IndexDBName: dbName, NoArchive: true}
			ctx := context.Background()
			pairs, _, err := FindBaselinePair(ctx, baseDir)
			if err != nil || len(pairs) != 1 {
				t.Fatalf("FindBaselinePair = %d pairs, %v; want 1", len(pairs), err)
			}
			got, err := VerifyBaselinePair(ctx, cfg, pairs[0])
			if err != nil {
				t.Fatalf("VerifyBaselinePair: %v", err)
			}
			checkRenumberedVerdict(t, tc, got, "second full snapshot")

			// The drill-down reads the same window, so it refuses the same way.
			if tc.want == StatusInconclusive {
				if _, err := ExplainBaselinePairMismatch(ctx, cfg, pairs[0]); !errors.Is(err, reconstruct.ErrBinlogRenumbered) {
					t.Errorf("explain = %v; want ErrBinlogRenumbered", err)
				}
			}
		})
	}
}

// Live mode: the newest snapshot's mark against the changes read from its
// position up to now, compared with the source's own table.
func TestVerifyTable_afterTheBinlogNumberingStartsOver_2174(t *testing.T) {
	for _, tc := range renumberCases(StatusMismatch) {
		t.Run(tc.name, func(t *testing.T) {
			db, dbName := testutil.CreateTestDB(t)
			now := time.Now().UTC()
			h1 := now.Truncate(time.Hour).Add(-time.Hour)
			if tc.startOverLater {
				// Live mode reads up to now: there is no "after the window".
				t.Skip("live mode reads up to now")
			}
			renumberIndex(t, db, dbName, tc, []time.Time{h1.Add(-time.Hour), h1, h1.Add(time.Hour)}, h1.Add(-5*time.Minute), now.Add(-time.Minute), time.Time{})

			// The source table holds the change.
			testutil.MustExec(t, db, fmt.Sprintf("CREATE TABLE `%s`.`orders` (`id` INT PRIMARY KEY, `status` VARCHAR(64))", dbName))
			testutil.MustExec(t, db, fmt.Sprintf("INSERT INTO `%s`.`orders` VALUES (1,'zzz'),(2,'b')", dbName))
			var uuid string
			if err := db.QueryRow("SELECT @@server_uuid").Scan(&uuid); err != nil {
				t.Fatal(err)
			}
			testutil.MustExec(t, db, `INSERT INTO stream_state (id, mode, gtid_set, last_checkpoint, server_id, bintrail_id)
				VALUES (1, 'gtid', ?, UTC_TIMESTAMP(), 1, 'b1')`, uuid+":1-1000000")

			baseDir := t.TempDir()
			writeMarkedBaseline(t, baseDir, h1, dbName, [][]string{{"1", "a"}, {"2", "b"}}, "binlog.000007", 200, tc.mark)

			resolver, err := metadata.NewResolver(db, 1)
			if err != nil {
				t.Fatal(err)
			}
			cfg := Config{SourceDB: db, IndexDB: db, Resolver: resolver, BaselineSource: baseDir, IndexDBName: dbName, NoArchive: true}
			got, err := VerifyTable(context.Background(), cfg, dbName, "orders")
			if err != nil {
				t.Fatalf("VerifyTable: %v", err)
			}
			checkRenumberedVerdict(t, tc, got)
		})
	}
}
