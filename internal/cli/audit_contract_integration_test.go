//go:build integration

package cli

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/audittest"
	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// resetReconstructGlobals does the same for the reconstruct command.
func resetReconstructGlobals(t *testing.T) {
	t.Helper()
	sIndexDSN, sSchema, sTable, sPK, sPKCols := recIndexDSN, recSchema, recTable, recPK, recPKColumns
	sAt, sBaselineDir, sBaselineS3, sBaselineOnly := recAt, recBaselineDir, recBaselineS3, recBaselineOnly
	sHistory, sSQL, sFormat, sNoArchive, sAllowGaps := recHistory, recSQL, recFormat, recNoArchive, recAllowGaps
	sOutFormat, sOutDir, sTables := recOutputFormat, recOutputDir, recTables
	t.Cleanup(func() {
		recIndexDSN, recSchema, recTable, recPK, recPKColumns = sIndexDSN, sSchema, sTable, sPK, sPKCols
		recAt, recBaselineDir, recBaselineS3, recBaselineOnly = sAt, sBaselineDir, sBaselineS3, sBaselineOnly
		recHistory, recSQL, recFormat, recNoArchive, recAllowGaps = sHistory, sSQL, sFormat, sNoArchive, sAllowGaps
		recOutputFormat, recOutputDir, recTables = sOutFormat, sOutDir, sTables
	})
	recIndexDSN, recSchema, recTable, recPK, recPKColumns = "", "", "", "", ""
	recAt, recBaselineDir, recBaselineS3, recBaselineOnly = "", "", "", false
	recHistory, recSQL, recFormat, recNoArchive, recAllowGaps = false, "", "json", true, true
	recOutputFormat, recOutputDir, recTables = "", "", ""
}

// auditOrdersBaseline seeds an index whose `orders` table has a baseline
// snapshot plus one indexed UPDATE after it — the minimum fixture the
// reconstruct and verify --explain contract cases need. It returns the index
// handle, the schema name, its DSN and the baseline directory.
func auditOrdersBaseline(t *testing.T, twoBaselines bool) (db *sql.DB, dbName, dsn, baseDir string, snapTime time.Time) {
	t.Helper()
	db, dbName = testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	now := time.Now().UTC()
	prevTS := now.Truncate(time.Hour).Add(-2 * time.Hour)
	newTS := prevTS.Add(time.Hour)
	for _, c := range []struct {
		name, key, dt string
		ord           int
	}{{"id", "PRI", "int", 1}, {"status", "", "varchar", 2}} {
		testutil.MustExec(t, db, `INSERT INTO schema_snapshots
			(snapshot_id, snapshot_time, schema_name, table_name, column_name,
			 ordinal_position, column_key, data_type, column_type, is_nullable, is_generated)
			VALUES (1, ?, ?, 'orders', ?, ?, ?, ?, ?, 'YES', 0)`,
			prevTS.Format("2006-01-02 15:04:05"), dbName, c.name, c.ord, c.key, c.dt, c.dt)
	}

	baseDir = t.TempDir()
	createSQL := "CREATE TABLE `orders` (\n  `id` INT NOT NULL,\n  `status` VARCHAR(64),\n  PRIMARY KEY (`id`)\n);\n"
	cols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "status", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	}
	writeCLIBaseline(t, baseDir, prevTS, dbName, "orders", createSQL, cols, [][]string{{"1", "a"}, {"2", "b"}}, 200)
	if twoBaselines {
		// The second baseline disagrees with what the deltas replay to, so
		// verify reports a mismatch and --explain has something to drill into.
		writeCLIBaseline(t, baseDir, newTS, dbName, "orders", createSQL, cols, [][]string{{"1", "a"}, {"2", "shipped"}}, 300)
	}

	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{prevTS, newTS, now.Truncate(time.Hour)})
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300,
		prevTS.Add(30*time.Minute).Format("2006-01-02 15:04:05"), nil,
		dbName, "orders", 2 /* UPDATE */, "2", nil,
		[]byte(`{"id":2,"status":"b"}`), []byte(`{"id":2,"status":"cancelled"}`))

	return db, dbName, testutil.IntegrationDSN(dbName), baseDir, prevTS
}

// TestIntegrationAuditContract_CLI is the CLI's #945 audit contract: every
// command that reads historical row images or emits a reversal script must
// record it on the ext.AuditSink seam.
//
// It is behavioural rather than source-level: each case runs the real command
// entry point against the integration MySQL with a recording sink installed,
// so an emission that is deleted — or quietly moved onto a branch the command
// never takes — fails here. CI runs the integration matrix on every pull
// request as a required check, so this gates merges.
//
// No t.Parallel(): the commands read package-level flag globals AND ext's
// sink is process-wide (audittest.Install).
func TestIntegrationAuditContract_CLI(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	rec := audittest.Install(t)

	cases := []struct {
		name   string
		action string
		table  string
		// wantMode, when set, pins Detail["mode"] — the per-mode contract the
		// pair-level CheckCoverage cannot see (#1123: --baseline-only was an
		// unaudited fifth mode behind an already-covered pair).
		wantMode string
		call     func(t *testing.T)
	}{
		{
			name:   "query",
			action: "query.run",
			table:  "orders",
			call: func(t *testing.T) {
				dbName, dsn := seedRecoverUpdates(t)
				resetQueryGlobals(t)
				qIndexDSN, qSchema, qTable = dsn, dbName, "orders"
				if err := runQuery(newQueryTestCmd(), nil); err != nil {
					t.Fatalf("runQuery: %v", err)
				}
			},
		},
		{
			name:   "recover",
			action: "recover.generate",
			table:  "orders",
			call: func(t *testing.T) {
				dbName, dsn := seedRecoverUpdates(t)
				resetRecoverGlobals(t)
				rIndexDSN, rSchema, rTable = dsn, dbName, "orders"
				rOutput = filepath.Join(t.TempDir(), "undo.sql")
				if err := runRecover(newRecoverTestCmd(), nil); err != nil {
					t.Fatalf("runRecover: %v", err)
				}
			},
		},
		{
			name:   "recover-cascade",
			action: "recover.cascade",
			table:  "parent",
			call: func(t *testing.T) {
				_, dbName, dsn := seedCascadeIndex(t)
				cleanCascadeFlags(dsn, dbName, filepath.Join(t.TempDir(), "cascade.sql"))
				if err := runCascadeCmd(t); err != nil {
					t.Fatalf("runRecoverCascade: %v", err)
				}
			},
		},
		{
			name:     "reconstruct",
			action:   "reconstruct.run",
			table:    "orders",
			wantMode: "row",
			call: func(t *testing.T) {
				_, dbName, dsn, baseDir, snapTime := auditOrdersBaseline(t, false)
				resetReconstructGlobals(t)
				recIndexDSN, recSchema, recTable, recPK = dsn, dbName, "orders", "2"
				// The fixture's schema_snapshots rows predate the event, so the
				// resolver cannot infer the PK; name it explicitly (the same
				// escape hatch the flag exists for).
				recPKColumns = "id"
				recBaselineDir = baseDir
				recAt = snapTime.Add(time.Hour).Format("2006-01-02 15:04:05")
				if err := runReconstruct(newQueryTestCmd(), nil); err != nil {
					t.Fatalf("runReconstruct: %v", err)
				}
			},
		},
		{
			// The fifth reconstruct mode: --baseline-only prints the raw
			// baseline row (no deltas, no index connection) and used to return
			// before the emission (#1123).
			name:     "reconstruct --baseline-only",
			action:   "reconstruct.run",
			table:    "orders",
			wantMode: "baseline-only",
			call: func(t *testing.T) {
				_, dbName, _, baseDir, _ := auditOrdersBaseline(t, false)
				resetReconstructGlobals(t)
				recSchema, recTable, recPK, recPKColumns = dbName, "orders", "1", "id"
				recBaselineDir, recBaselineOnly = baseDir, true
				if err := runReconstruct(newQueryTestCmd(), nil); err != nil {
					t.Fatalf("runReconstruct --baseline-only: %v", err)
				}
			},
		},
		{
			name:   "verify --explain",
			action: "verify.explain",
			table:  "orders",
			call: func(t *testing.T) {
				db, dbName, _, baseDir, _ := auditOrdersBaseline(t, true)
				resolver, err := metadata.NewResolver(db, 1)
				if err != nil {
					t.Fatalf("NewResolver: %v", err)
				}
				vfyNoArchive, vfyExplain = true, true
				t.Cleanup(func() { vfyNoArchive, vfyExplain = false, false })
				c := &cobra.Command{}
				c.SetContext(context.Background())
				c.SetOut(os.Stdout)
				// A mismatch is the point: --explain only drills down (and only
				// emits) when the fingerprints disagree, and the command then
				// exits non-zero by contract.
				if err := runVerifyBaselinePair(c, db, resolver, dbName, baseDir, "", duckdbutil.Tuning{}, ""); err == nil {
					t.Fatal("expected a mismatch so --explain drills down")
				}
			},
		},
		{
			// drill materializes historical row state into an external scratch
			// server — the newest data-serving mode (#1195), emitted per table.
			name:   "drill",
			action: "drill.run",
			table:  "orders",
			call: func(t *testing.T) {
				srcSchema := fmt.Sprintf("drillsrc_%d", time.Now().UnixNano())
				dsn, baseDir, at := drillContractFixture(t, srcSchema)
				resetDrillGlobals(t)
				drlIndexDSN, drlBaselineDir = dsn, baseDir
				drlTables = srcSchema + ".orders"
				// Inside the fixture's partition coverage — the default (now)
				// would trip the strict gap check on hours that never existed.
				drlAt = at.Format("2006-01-02 15:04:05")
				// The scratch is the SAME test server: srcSchema does not exist
				// there, so the emptiness guard passes; drill creates and loads
				// it, and the cleanup below drops it.
				drlTargetDSN = testutil.BaseDSN() + "/"
				drlFormat = "json"
				if err := runDrill(newQueryTestCmd(), nil); err != nil {
					t.Fatalf("runDrill: %v", err)
				}
			},
		},
	}

	var observed []audittest.Pair
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec.Reset()
			tc.call(t)
			evs := rec.Events()
			if len(evs) != 1 {
				t.Fatalf("recorded %d audit events, want exactly 1: %+v", len(evs), evs)
			}
			ev := evs[0]
			if ev.Surface != "cli" || ev.Action != tc.action {
				t.Errorf("event = %s/%s, want cli/%s", ev.Surface, ev.Action, tc.action)
			}
			if ev.Table != tc.table {
				t.Errorf("table = %q, want %q", ev.Table, tc.table)
			}
			if tc.wantMode != "" && ev.Detail["mode"] != tc.wantMode {
				t.Errorf("detail[mode] = %q, want %q", ev.Detail["mode"], tc.wantMode)
			}
			// A locally invoked command has no authenticated caller, so the
			// actor is the process identity — "os:<user>" per ext.ProcessActor.
			if ev.Actor == "" {
				t.Error("actor must not be empty")
			}
			if ev.Time.IsZero() {
				t.Error("event Time not stamped")
			}
			observed = append(observed, audittest.Pair{Surface: ev.Surface, Action: ev.Action})
		})
	}

	audittest.CheckCoverage(t, audittest.OwnerCLI, observed)
}

// resetDrillGlobals saves and clears the drill command's flag globals.
func resetDrillGlobals(t *testing.T) {
	t.Helper()
	sIdx, sTgt, sTables, sAt := drlIndexDSN, drlTargetDSN, drlTables, drlAt
	sDir, sS3, sOut, sFmt := drlBaselineDir, drlBaselineS3, drlOutput, drlFormat
	t.Cleanup(func() {
		drlIndexDSN, drlTargetDSN, drlTables, drlAt = sIdx, sTgt, sTables, sAt
		drlBaselineDir, drlBaselineS3, drlOutput, drlFormat = sDir, sS3, sOut, sFmt
	})
	drlIndexDSN, drlTargetDSN, drlTables, drlAt = "", "", "", ""
	drlBaselineDir, drlBaselineS3, drlOutput, drlFormat = "", "", "", "text"
}

// drillContractFixture seeds a full-table-reconstructable index for a
// SYNTHETIC source schema that does not exist as a database on the shared
// test server — so the same server can serve as drill's scratch target. It
// registers a cleanup dropping the database drill creates there.
func drillContractFixture(t *testing.T, srcSchema string) (dsn, baselineDir string, at time.Time) {
	t.Helper()
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	t.Cleanup(func() {
		testutil.MustExec(t, db, "DROP DATABASE IF EXISTS `"+srcSchema+"`")
	})

	testutil.MustExec(t, db, `INSERT INTO schema_snapshots
		(snapshot_id, snapshot_time, schema_name, table_name, column_name, ordinal_position, column_key, data_type, is_nullable, is_generated)
		VALUES (1, UTC_TIMESTAMP(), ?, 'orders', 'id', 1, 'PRI', 'int', 'NO', 0)`, srcSchema)
	testutil.MustExec(t, db, `INSERT INTO schema_snapshots
		(snapshot_id, snapshot_time, schema_name, table_name, column_name, ordinal_position, column_key, data_type, is_nullable, is_generated)
		VALUES (1, UTC_TIMESTAMP(), ?, 'orders', 'status', 2, '', 'varchar', 'NO', 0)`, srcSchema)

	// VERBATIM mydumper shape — the /*!…*/ SET preamble makes the schema
	// file multi-statement, exactly what a real `bintrail baseline` stores
	// (a single-statement synthetic here would hide load-session bugs; it
	// did once).
	createSQL := "/*!40101 SET NAMES binary*/;\n/*!40014 SET FOREIGN_KEY_CHECKS=0*/;\nCREATE TABLE `orders` (\n  `id` INT NOT NULL,\n  `status` VARCHAR(64) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n"
	baselineDir = t.TempDir()
	h1 := time.Now().UTC().Add(-48 * time.Hour).Truncate(time.Hour)
	h2 := h1.Add(time.Hour)
	snapshotTSDir := strings.ReplaceAll(h1.Format(time.RFC3339), ":", "-")
	parquetDir := filepath.Join(baselineDir, snapshotTSDir, srcSchema)
	if err := os.MkdirAll(parquetDir, 0o755); err != nil {
		t.Fatalf("mkdir baseline: %v", err)
	}
	cols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "status", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	}
	bw, err := baseline.NewWriter(filepath.Join(parquetDir, "orders.parquet"), cols, baseline.WriterConfig{
		Compression:  "zstd",
		RowGroupSize: 100,
		Metadata:     map[string]string{baseline.MetaKeyCreateTableSQL: createSQL},
	})
	if err != nil {
		t.Fatalf("baseline.NewWriter: %v", err)
	}
	for _, row := range [][]string{{"1", "start-1"}, {"2", "start-2"}, {"3", "start-3"}} {
		if err := bw.WriteRow(row, []bool{false, false}); err != nil {
			t.Fatalf("WriteRow: %v", err)
		}
	}
	if err := bw.Close(); err != nil {
		t.Fatalf("writer close: %v", err)
	}

	// Deltas: update id=2, delete id=3, insert id=4 → final rows {1,2,4}.
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h1, h2})
	ts1 := h1.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
	ts2 := h2.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, ts1, nil,
		srcSchema, "orders", 2, "2", nil,
		[]byte(`{"id":2,"status":"start-2"}`), []byte(`{"id":2,"status":"paid"}`))
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300, ts1, nil,
		srcSchema, "orders", 3, "3", nil,
		[]byte(`{"id":3,"status":"start-3"}`), nil)
	testutil.InsertEvent(t, db, "binlog.000001", 300, 400, ts2, nil,
		srcSchema, "orders", 1, "4", nil,
		nil, []byte(`{"id":4,"status":"new-4"}`))

	return testutil.IntegrationDSN(dbName), baselineDir, h2.Add(45 * time.Minute)
}
