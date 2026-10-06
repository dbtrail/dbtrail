//go:build integration

package streamrun

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/shim"
	"github.com/dbtrail/dbtrail/internal/testutil"
	"github.com/dbtrail/dbtrail/internal/verify"
)

// #2156, the "latest change per row" readers, on changes captured from a real
// server through the real capture: `verify` against the live source, and the
// shim's full-table `_snapshot`.
//
// The shapes are order2156Shapes: rows 1, 2 and 3 are each changed by two
// sessions where the change the binary log holds LAST started its statement
// FIRST (it waited on a row lock), and row 9 is changed twice by one session.
// A snapshot of the table taken before the shapes holds every row as "seed".
// Both readers take that snapshot and put on it the latest change of each row
// after it. Taken by statement time, the latest change of rows 1-3 is the
// first session's, which the database does not hold.

const latestPerKey2156After = "1=wait-B,3=ins-B,9=nine-2"

// latestPerKey2156Baseline writes the snapshot the two readers start from: the
// table as it was before the shapes, in the layout `bintrail baseline` writes.
func latestPerKey2156Baseline(t *testing.T, snapTime time.Time, schema string) string {
	t.Helper()
	root := t.TempDir()
	dir := filepath.Join(root, snapTime.UTC().Format("2006-01-02T15-04-05")+"Z", schema)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir baseline: %v", err)
	}
	cols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "v", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	}
	w, err := baseline.NewWriter(filepath.Join(dir, "t.parquet"), cols, baseline.WriterConfig{
		Compression: "none", RowGroupSize: 100,
		Metadata: map[string]string{baseline.MetaKeyCreateTableSQL: "CREATE TABLE `t` (\n  `id` INT NOT NULL,\n  `v` VARCHAR(32) NOT NULL,\n  PRIMARY KEY (`id`)\n);\n"},
	})
	if err != nil {
		t.Fatalf("baseline writer: %v", err)
	}
	for _, id := range []string{"1", "2", "3", "9"} {
		if err := w.WriteRow([]string{id, "seed"}, []bool{false, false}); err != nil {
			t.Fatalf("WriteRow: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("baseline close: %v", err)
	}
	return root
}

// latestPerKey2156Snapshot reads the whole table through the shim's
// `_snapshot`, as a MySQL client does, and returns it in order2156Table's form
// together with what SHOW WARNINGS answers afterwards.
func latestPerKey2156Snapshot(t *testing.T, h *shim.Handler, asOf time.Time) (string, []string) {
	t.Helper()
	res, err := h.HandleQuery("SELECT * FROM _snapshot.t AS OF '" + asOf.UTC().Format("2006-01-02 15:04:05") + "'")
	if err != nil {
		t.Fatalf("_snapshot: %v", err)
	}
	cells := latestPerKey2156Cells(t, res.Resultset)
	var out []string
	for _, c := range cells {
		out = append(out, c[0]+"="+c[1])
	}
	slices.Sort(out)
	wres, err := h.HandleQuery("SHOW WARNINGS")
	if err != nil {
		t.Fatalf("SHOW WARNINGS: %v", err)
	}
	var warnings []string
	if wres != nil && wres.Resultset != nil {
		for _, c := range latestPerKey2156Cells(t, wres.Resultset) {
			warnings = append(warnings, c[2])
		}
	}
	return strings.Join(out, ","), warnings
}

func latestPerKey2156Cells(t *testing.T, rs *gomysql.Resultset) [][]string {
	t.Helper()
	var out [][]string
	for _, rd := range rs.RowDatas {
		fvs, err := rd.Parse(rs.Fields, false, nil)
		if err != nil {
			t.Fatalf("parse row: %v", err)
		}
		cells := make([]string, len(fvs))
		for i := range fvs {
			switch v := fvs[i].Value().(type) {
			case nil:
				cells[i] = "NULL"
			case []byte:
				cells[i] = string(v)
			default:
				cells[i] = fmt.Sprint(v)
			}
		}
		out = append(out, cells)
	}
	return out
}

func runLatestPerKey2156(t *testing.T, flavor, sourceDSN string, sourceDB, indexDB *sql.DB, sourceName, indexName string) {
	ctx := context.Background()
	testutil.MustExec(t, sourceDB, "CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(32) NOT NULL) ENGINE=InnoDB")
	testutil.MustExec(t, sourceDB, "INSERT INTO t VALUES (1,'seed'),(2,'seed'),(3,'seed'),(9,'seed')")

	// Hourly partitions around now, so the reads' coverage check finds the
	// hours of the window live, as on an index `bintrail init` created.
	hour := time.Now().UTC().Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, indexDB, indexName, []time.Time{hour.Add(-time.Hour), hour, hour.Add(time.Hour)})

	snapTime := time.Now().UTC().Truncate(time.Second).Add(-time.Second)
	baselineDir := latestPerKey2156Baseline(t, snapTime, sourceName)

	captureWhile(t, flavor, sourceDSN, sourceDB, indexDB, sourceName, 8, func() { order2156Shapes(t, sourceDB) })
	if got := order2156Table(t, sourceDB); got != latestPerKey2156After {
		t.Fatalf("after the shapes the table is %s, want %s: the shapes did not happen", got, latestPerKey2156After)
	}

	// The stream's checkpoint, which the capture above stops before: it is
	// what tells the index is one a stream writes, and what verify reads as
	// the index's coverage of the source.
	gtidVar := "@@global.gtid_executed"
	if flavor == gomysql.MariaDBFlavor {
		gtidVar = "@@global.gtid_binlog_pos"
	}
	var gtid string
	if err := sourceDB.QueryRow("SELECT " + gtidVar).Scan(&gtid); err != nil {
		t.Fatalf("read %s: %v", gtidVar, err)
	}
	testutil.MustExec(t, indexDB, `INSERT INTO stream_state (id, mode, binlog_file, binlog_position, gtid_set, last_checkpoint, server_id)
		VALUES (1, 'gtid', 'binlog.000001', 4, ?, UTC_TIMESTAMP(), 1)`, gtid)
	if proof, err := query.IDsFollowBinlog(ctx, indexDB, snapTime); err != nil || proof != query.IDsFollowStream {
		t.Fatalf("IDsFollowBinlog on the captured index = %v, %v; want IDsFollowStream", proof, err)
	}

	resolver, err := metadata.NewResolver(indexDB, 0)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	cfg := verify.Config{
		SourceDB: sourceDB, IndexDB: indexDB, Resolver: resolver,
		BaselineSource: baselineDir, IndexDBName: indexName, NoArchive: true,
	}
	newShim := func() *shim.Handler {
		h := shim.NewHandlerWithConfig(indexDB, shim.Config{
			AllowGaps: true, NoArchive: true, IndexDBName: indexName, BaselineDir: baselineDir,
		}, slog.Default())
		if err := h.UseDB(sourceName); err != nil {
			t.Fatalf("UseDB: %v", err)
		}
		return h
	}
	asOf := time.Now().UTC().Add(time.Second)

	t.Run("verify", func(t *testing.T) {
		res, err := verify.VerifyTable(ctx, cfg, sourceName, "t")
		if err != nil {
			t.Fatalf("VerifyTable: %v", err)
		}
		t.Logf("verify: status=%s detail=%q source rows=%d reconstructed rows=%d", res.Status, res.Detail, res.SourceRows, res.ReconstructRows)
		if res.Status != verify.StatusMatch {
			t.Fatalf("verify says %s (%s) on a table whose snapshot and changes are right", res.Status, res.Detail)
		}
		if strings.Contains(res.Detail, "order of changes") {
			t.Fatalf("a proven order carries the unproven note: %q", res.Detail)
		}
	})

	t.Run("shim _snapshot", func(t *testing.T) {
		got, warnings := latestPerKey2156Snapshot(t, newShim(), asOf)
		if got != latestPerKey2156After {
			t.Fatalf("_snapshot answers %s, the database holds %s", got, latestPerKey2156After)
		}
		if len(warnings) != 0 {
			t.Fatalf("a proven order raised warnings: %q", warnings)
		}
	})

	// The same changes on an index that a stream writes and that also had a
	// binary log file indexed into it by `bintrail index` just now: its ids
	// no longer say in which order the changes were written. Both readers keep
	// statement-time order, as before, and say so.
	testutil.MustExec(t, indexDB, `INSERT INTO index_state (binlog_file, file_size, last_position, status, started_at, completed_at)
		VALUES ('binlog.000001', 1, 1, 'completed', UTC_TIMESTAMP(), UTC_TIMESTAMP())`)

	t.Run("verify, order unproven", func(t *testing.T) {
		res, err := verify.VerifyTable(ctx, cfg, sourceName, "t")
		if err != nil {
			t.Fatalf("VerifyTable: %v", err)
		}
		t.Logf("verify: status=%s detail=%q", res.Status, res.Detail)
		if res.Status != verify.StatusMismatch {
			t.Fatalf("status = %s, want the statement-time answer (mismatch) on an index whose order is unproven", res.Status)
		}
		if !strings.Contains(res.Detail, "order of changes unproven") {
			t.Fatalf("the mismatch does not say the order of the changes is unproven: %q", res.Detail)
		}
	})

	t.Run("shim _snapshot, order unproven", func(t *testing.T) {
		got, warnings := latestPerKey2156Snapshot(t, newShim(), asOf)
		if want := "1=wait-A,2=upd-A,9=nine-2"; got != want {
			t.Fatalf("_snapshot answers %s, want the statement-time answer %s", got, want)
		}
		if len(warnings) != 1 || !strings.Contains(warnings[0], "order of changes unproven") {
			t.Fatalf("SHOW WARNINGS = %q, want one note that the order is unproven", warnings)
		}
		t.Logf("SHOW WARNINGS: %s", warnings[0])
	})
}

func TestIntegrationLatestPerKeyBinlogOrder2156_mysql(t *testing.T) {
	indexDB, indexName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	sourceDB, sourceName := testutil.CreateTestDB(t)
	var logBin string
	if err := sourceDB.QueryRow("SELECT @@log_bin").Scan(&logBin); err != nil || logBin != "1" {
		t.Skip("skipping: binary logging not enabled on test MySQL")
	}
	runLatestPerKey2156(t, gomysql.MySQLFlavor, testutil.IntegrationDSN(sourceName), sourceDB, indexDB, sourceName, indexName)
}

func TestIntegrationLatestPerKeyBinlogOrder2156_mariadb(t *testing.T) {
	sourceDB, sourceName := testutil.CreateTestMariaDB(t)
	indexDB, indexName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	runLatestPerKey2156(t, gomysql.MariaDBFlavor,
		testutil.MariaDBBaseDSN()+"/"+sourceName+"?parseTime=true", sourceDB, indexDB, sourceName, indexName)
}
