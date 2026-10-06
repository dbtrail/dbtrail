//go:build integration

package streamrun

import (
	"bytes"
	"context"
	"database/sql"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/dbtrail/internal/cascade"
	"github.com/dbtrail/dbtrail/internal/cascaderecover"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/recovery"
	"github.com/dbtrail/dbtrail/internal/shim"
	"github.com/dbtrail/dbtrail/internal/testutil"
	"github.com/dbtrail/dbtrail/internal/verify"
)

// #2156, the last sites, on changes captured from a real server through the
// real capture: `verify --check recover` (the chain walk), the shim's
// single-row `_flashback` and `_snapshot` and its full-table `_flashback`,
// and `recover-cascade` (script applied, rows compared).
//
// The shapes are order2156Shapes: rows 1, 2 and 3 are each changed by two
// sessions where the change the binary log holds LAST started its statement
// FIRST (it waited on a row lock), and row 9 is changed twice by one session.

// remaining2156Index records the capture as a stream's, the way
// runLatestPerKey2156 does: the capture stops before its first checkpoint.
func remaining2156Index(t *testing.T, flavor string, sourceDB, indexDB *sql.DB) {
	t.Helper()
	gtidVar, sflavor := "@@global.gtid_executed", "mysql"
	if flavor == gomysql.MariaDBFlavor {
		gtidVar, sflavor = "@@global.gtid_binlog_pos", "mariadb"
	}
	var gtid string
	if err := sourceDB.QueryRow("SELECT " + gtidVar).Scan(&gtid); err != nil {
		t.Fatalf("read %s: %v", gtidVar, err)
	}
	testutil.MustExec(t, indexDB, `INSERT INTO stream_state (id, mode, binlog_file, binlog_position, gtid_set, flavor, last_checkpoint, server_id)
		VALUES (1, 'gtid', 'binlog.000001', 4, ?, ?, UTC_TIMESTAMP(), 1)
		ON DUPLICATE KEY UPDATE mode = 'gtid', gtid_set = VALUES(gtid_set), flavor = VALUES(flavor)`, gtid, sflavor)
}

// remaining2156MakeUnproven indexes a file into the stream's index by hand:
// its ids no longer say in which order the changes were written.
func remaining2156MakeUnproven(t *testing.T, indexDB *sql.DB) {
	t.Helper()
	testutil.MustExec(t, indexDB, `INSERT INTO index_state (binlog_file, file_size, last_position, status, started_at, completed_at)
		VALUES ('binlog.000001', 1, 1, 'completed', UTC_TIMESTAMP(), UTC_TIMESTAMP())`)
}

// remaining2156Read runs one statement through the shim and returns its rows
// as "id=v" (sorted), with what SHOW WARNINGS answers afterwards.
func remaining2156Read(t *testing.T, h *shim.Handler, stmt string) (string, []string) {
	t.Helper()
	res, err := h.HandleQuery(stmt)
	if err != nil {
		t.Fatalf("%s: %v", stmt, err)
	}
	var out []string
	if res != nil && res.Resultset != nil {
		for _, c := range latestPerKey2156Cells(t, res.Resultset) {
			out = append(out, c[0]+"="+c[1])
		}
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

func runRemainingSites2156(t *testing.T, flavor, sourceDSN string, sourceDB, indexDB *sql.DB, sourceName, indexName string) {
	ctx := context.Background()
	testutil.MustExec(t, sourceDB, "CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(32) NOT NULL) ENGINE=InnoDB")
	testutil.MustExec(t, sourceDB, "INSERT INTO t VALUES (1,'seed'),(2,'seed'),(3,'seed'),(9,'seed')")
	hour := time.Now().UTC().Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, indexDB, indexName, []time.Time{hour.Add(-time.Hour), hour, hour.Add(time.Hour)})
	since := time.Now().UTC().Add(-time.Minute)
	// A snapshot of the table before the shapes, so the one-row `_snapshot`
	// read takes its baseline path (without one it is the `_flashback` read).
	snapTime := time.Now().UTC().Truncate(time.Second).Add(-time.Second)
	baselineDir := latestPerKey2156Baseline(t, snapTime, sourceName)

	captureWhile(t, flavor, sourceDSN, sourceDB, indexDB, sourceName, 8, func() { order2156Shapes(t, sourceDB) })
	if got := order2156Table(t, sourceDB); got != latestPerKey2156After {
		t.Fatalf("after the shapes the table is %s, want %s: the shapes did not happen", got, latestPerKey2156After)
	}
	remaining2156Index(t, flavor, sourceDB, indexDB)
	if proof, err := query.IDsFollowBinlog(ctx, indexDB, since); err != nil || proof != query.IDsFollowStream {
		t.Fatalf("IDsFollowBinlog on the captured index = %v, %v; want IDsFollowStream", proof, err)
	}
	resolver, err := metadata.NewResolver(indexDB, 0)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	asOf := time.Now().UTC().Add(time.Second)
	at := asOf.Format("2006-01-02 15:04:05")
	newShim := func() *shim.Handler {
		h := shim.NewHandlerWithConfig(indexDB, shim.Config{AllowGaps: true, NoArchive: true, IndexDBName: indexName, BaselineDir: baselineDir}, slog.Default())
		if err := h.UseDB(sourceName); err != nil {
			t.Fatalf("UseDB: %v", err)
		}
		return h
	}
	recoverInputs := func(t *testing.T) verify.TableResult {
		t.Helper()
		res, err := verify.VerifyRecoverInputs(ctx, verify.RecoverInputsConfig{
			IndexDB: indexDB, Resolver: resolver, IndexDBName: indexName, NoArchive: true,
			Since: since, Until: asOf,
		}, sourceName, "t")
		if err != nil {
			t.Fatalf("VerifyRecoverInputs: %v", err)
		}
		t.Logf("verify --check recover: status=%s detail=%q", res.Status, res.Detail)
		return res
	}
	// Single-row reads: row 1 ends on B's update, row 2 on B's delete (no
	// row), row 3 on B's insert.
	singleRows := func(t *testing.T, schema string) (string, []string) {
		t.Helper()
		var got []string
		var warnings []string
		h := newShim()
		for _, id := range []string{"1", "2", "3", "9"} {
			r, w := remaining2156Read(t, h, "SELECT * FROM "+schema+".t AS OF '"+at+"' WHERE id = "+id)
			if r == "" {
				r = id + "=<none>"
			}
			got = append(got, r)
			warnings = append(warnings, w...)
		}
		return strings.Join(got, ","), warnings
	}

	const singleAfter = "1=wait-B,2=<none>,3=ins-B,9=nine-2"
	t.Run("verify --check recover", func(t *testing.T) {
		if res := recoverInputs(t); res.Status != verify.StatusMatch {
			t.Fatalf("status %s (%s): a chain that is whole in the binary log reads as broken", res.Status, res.Detail)
		}
	})
	for _, schema := range []string{"_flashback", "_snapshot"} {
		t.Run(schema+" single row", func(t *testing.T) {
			got, warnings := singleRows(t, schema)
			if got != singleAfter || len(warnings) != 0 {
				t.Fatalf("%s answers %s with warnings %q; the database held %s", schema, got, warnings, singleAfter)
			}
		})
	}
	t.Run("_flashback full table", func(t *testing.T) {
		got, warnings := remaining2156Read(t, newShim(), "SELECT * FROM _flashback.t AS OF '"+at+"'")
		if got != latestPerKey2156After || len(warnings) != 0 {
			t.Fatalf("_flashback answers %s with warnings %q; the database holds %s", got, warnings, latestPerKey2156After)
		}
		// A LIMIT under the cap still bounds the read.
		h := shim.NewHandlerWithConfig(indexDB, shim.Config{AllowGaps: true, NoArchive: true, IndexDBName: indexName, FullTableRowCap: 2}, slog.Default())
		if err := h.UseDB(sourceName); err != nil {
			t.Fatal(err)
		}
		if _, err := h.HandleQuery("SELECT * FROM _flashback.t AS OF '" + at + "'"); err == nil || !strings.Contains(err.Error(), "more than 2 rows") {
			t.Fatalf("over the cap: err = %v, want the 1104 refusal", err)
		}
		// LIMIT 2 reads two rows' latest changes; a row whose latest change
		// is a DELETE (row 2) takes its place and is not answered, as before.
		got, _ = remaining2156Read(t, h, "SELECT * FROM _flashback.t AS OF '"+at+"' LIMIT 2")
		rows := strings.Split(got, ",")
		if got == "" || len(rows) > 2 {
			t.Fatalf("LIMIT 2 answered %q", got)
		}
		for _, r := range rows {
			if !slices.Contains(strings.Split(latestPerKey2156After, ","), r) {
				t.Fatalf("LIMIT 2 answered %q: %s is not what the database holds", got, r)
			}
		}
	})

	// The same changes on an index whose ids no longer prove the order: every
	// reader keeps statement-time order, as before, and says so.
	remaining2156MakeUnproven(t, indexDB)
	t.Run("verify --check recover, order unproven", func(t *testing.T) {
		res := recoverInputs(t)
		if res.Status != verify.StatusMismatch || !strings.Contains(res.Detail, "order of changes unproven") {
			t.Fatalf("status %s (%s): want the statement-time answer with the order note", res.Status, res.Detail)
		}
	})
	t.Run("_flashback single row, order unproven", func(t *testing.T) {
		got, warnings := singleRows(t, "_flashback")
		if want := "1=wait-A,2=upd-A,3=<none>,9=nine-2"; got != want {
			t.Fatalf("answers %s, want the statement-time answer %s", got, want)
		}
		if len(warnings) != 3 || !strings.HasPrefix(warnings[0], "order of changes unproven: ") {
			t.Fatalf("SHOW WARNINGS = %q, want one note for each of rows 1-3", warnings)
		}
	})
	t.Run("_snapshot single row, order unproven", func(t *testing.T) {
		got, warnings := singleRows(t, "_snapshot")
		if want := "1=wait-A,2=upd-A,3=<none>,9=nine-2"; got != want || len(warnings) != 3 {
			t.Fatalf("answers %s with warnings %q, want the statement-time answer %s and one note for each of rows 1-3", got, warnings, want)
		}
	})
	t.Run("_flashback full table, order unproven", func(t *testing.T) {
		got, warnings := remaining2156Read(t, newShim(), "SELECT * FROM _flashback.t AS OF '"+at+"'")
		if want := "1=wait-A,2=upd-A,9=nine-2"; got != want {
			t.Fatalf("answers %s, want the statement-time answer %s", got, want)
		}
		if len(warnings) != 1 || !strings.HasPrefix(warnings[0], "order of changes unproven: for 3 row(s)") {
			t.Fatalf("SHOW WARNINGS = %q", warnings)
		}
	})
}

func TestIntegrationRemainingSites2156_mysql(t *testing.T) {
	indexDB, indexName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	sourceDB, sourceName := testutil.CreateTestDB(t)
	var logBin string
	if err := sourceDB.QueryRow("SELECT @@log_bin").Scan(&logBin); err != nil || logBin != "1" {
		t.Skip("skipping: binary logging not enabled on test MySQL")
	}
	runRemainingSites2156(t, gomysql.MySQLFlavor, testutil.IntegrationDSN(sourceName), sourceDB, indexDB, sourceName, indexName)
}

func TestIntegrationRemainingSites2156_mariadb(t *testing.T) {
	sourceDB, sourceName := testutil.CreateTestMariaDB(t)
	indexDB, indexName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	runRemainingSites2156(t, gomysql.MariaDBFlavor,
		testutil.MariaDBBaseDSN()+"/"+sourceName+"?parseTime=true", sourceDB, indexDB, sourceName, indexName)
}

// plainFetcher hides query.LatestRepicker: the scan for children as it was
// before #2156 took it, for the premise.
type plainFetcher struct{ f query.Fetcher }

func (p plainFetcher) Fetch(ctx context.Context, o query.Options) ([]query.ResultRow, error) {
	return p.f.Fetch(ctx, o)
}

// recover-cascade: child 10 of parent 1 is changed by two sessions (B waited
// on A's row lock, so B is in the binary log last and started first), then
// parent 1 is deleted and InnoDB deletes child 10 below the binary log. The
// script must put child 10 back with B's value.
func runCascade2156(t *testing.T, flavor, sourceDSN, applyDSN string, sourceDB, indexDB *sql.DB, sourceName, indexName string) {
	ctx := context.Background()
	testutil.MustExec(t, sourceDB, "CREATE TABLE parent (id INT PRIMARY KEY, v VARCHAR(32) NOT NULL) ENGINE=InnoDB")
	testutil.MustExec(t, sourceDB, `CREATE TABLE t (id INT PRIMARY KEY, pid INT NOT NULL, v VARCHAR(32) NOT NULL,
		CONSTRAINT fk_t FOREIGN KEY (pid) REFERENCES parent(id) ON DELETE CASCADE) ENGINE=InnoDB`)
	testutil.MustExec(t, sourceDB, "INSERT INTO parent VALUES (1,'p1')")
	testutil.MustExec(t, sourceDB, "INSERT INTO t VALUES (10,1,'seed')")
	hour := time.Now().UTC().Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, indexDB, indexName, []time.Time{hour.Add(-time.Hour), hour, hour.Add(time.Hour)})

	captureWhile(t, flavor, sourceDSN, sourceDB, indexDB, sourceName, 3, func() {
		a, err := sourceDB.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer a.Close()
		b, err := sourceDB.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer b.Close()
		stmtTimeExec(t, ctx, a, "BEGIN")
		stmtTimeExec(t, ctx, a, "SELECT v FROM t WHERE id = 10 FOR UPDATE")
		done := make(chan error, 1)
		go func() {
			_, err := b.ExecContext(ctx, "UPDATE t SET v = 'wait-B' WHERE id = 10")
			done <- err
		}()
		waitStatementAge(t, sourceDB, "wait-B", 2)
		stmtTimeExec(t, ctx, a, "UPDATE t SET v = 'wait-A' WHERE id = 10")
		stmtTimeExec(t, ctx, a, "COMMIT")
		if err := <-done; err != nil {
			t.Fatalf("B: %v", err)
		}
		time.Sleep(1100 * time.Millisecond)
		testutil.MustExec(t, sourceDB, "DELETE FROM parent WHERE id = 1")
	})
	remaining2156Index(t, flavor, sourceDB, indexDB)
	var left int
	if err := sourceDB.QueryRow("SELECT COUNT(*) FROM t").Scan(&left); err != nil || left != 0 {
		t.Fatalf("the cascade did not happen: %d child rows left (%v)", left, err)
	}

	resolver, err := metadata.NewResolver(indexDB, 0)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	del := event.EventDelete
	parents, err := query.New(indexDB).Fetch(ctx, query.Options{Schema: sourceName, Table: "parent", EventType: &del})
	if err != nil || len(parents) != 1 {
		t.Fatalf("parent deletes = %d (%v), want 1", len(parents), err)
	}
	fks, err := cascade.LoadCascadeFKs(ctx, indexDB, []string{sourceName}, time.Now())
	if err != nil {
		t.Fatalf("LoadCascadeFKs: %v", err)
	}

	script := func(t *testing.T, f query.Fetcher) (string, cascade.Result) {
		t.Helper()
		res, err := cascade.SynthesizeVictims(ctx, f, fks, parents, cascade.Options{})
		if err != nil {
			t.Fatalf("SynthesizeVictims: %v", err)
		}
		roots, order := cascaderecover.MergeParentRoots(parents, res.KeyUpdateParents, query.BinlogOrderProof(ctx, indexDB))
		rows := append(append([]query.ResultRow{}, roots...), res.Victims...)
		var buf bytes.Buffer
		if _, err := cascaderecover.EmitSQL(&buf, recovery.New(indexDB, resolver), rows, res.SetNullRows, res.KeyUpdates, resolver, cascaderecover.Header{
			Schema: sourceName, Table: "parent", Parents: len(roots), Children: len(res.Victims),
			Warnings: append(res.Warnings, cascaderecover.OrderNotes(order)...),
		}); err != nil {
			t.Fatalf("EmitSQL: %v", err)
		}
		return buf.String(), res
	}
	childAfterApply := func(t *testing.T, s string) string {
		t.Helper()
		if err := order2156Apply(t, applyDSN, s); err != nil {
			t.Fatalf("apply: %v\n%s", err, s)
		}
		var v string
		if err := sourceDB.QueryRow("SELECT v FROM t WHERE id = 10").Scan(&v); err != nil {
			t.Fatalf("read child 10: %v", err)
		}
		// Back to the state after the cascade (the capture has stopped).
		testutil.MustExec(t, sourceDB, "DELETE FROM parent")
		return v
	}

	t.Run("premise: the statement-time pick puts back A's value", func(t *testing.T) {
		s, _ := script(t, plainFetcher{query.New(indexDB)})
		if got := childAfterApply(t, s); got != "wait-A" {
			t.Fatalf("child 10 = %s; the premise expects wait-A", got)
		}
	})
	for name, f := range map[string]query.Fetcher{
		"live index": query.New(indexDB),
		"merged":     &query.MergedFetcher{DB: indexDB, Engine: query.New(indexDB), DBName: indexName, NoArchive: true},
	} {
		t.Run(name+": child put back with the value the database held", func(t *testing.T) {
			s, res := script(t, f)
			for _, w := range res.Warnings {
				if strings.Contains(w, "order of changes") {
					t.Fatalf("a proven order carries a warning: %q", w)
				}
			}
			if got := childAfterApply(t, s); got != "wait-B" {
				t.Fatalf("child 10 = %s, want wait-B:\n%s", got, s)
			}
		})
	}
	remaining2156MakeUnproven(t, indexDB)
	t.Run("order unproven: statement time, said in the script", func(t *testing.T) {
		s, _ := script(t, query.New(indexDB))
		if !strings.Contains(s, "order of changes unproven") {
			t.Fatalf("the script does not say the order is unproven:\n%s", s)
		}
		if got := childAfterApply(t, s); got != "wait-A" {
			t.Fatalf("child 10 = %s, want the statement-time answer wait-A", got)
		}
	})
}

func TestIntegrationCascadeBinlogOrder2156_mysql(t *testing.T) {
	indexDB, indexName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	sourceDB, sourceName := testutil.CreateTestDB(t)
	var logBin string
	if err := sourceDB.QueryRow("SELECT @@log_bin").Scan(&logBin); err != nil || logBin != "1" {
		t.Skip("skipping: binary logging not enabled on test MySQL")
	}
	dsn := testutil.IntegrationDSN(sourceName)
	runCascade2156(t, gomysql.MySQLFlavor, dsn, dsn+"&multiStatements=true", sourceDB, indexDB, sourceName, indexName)
}

func TestIntegrationCascadeBinlogOrder2156_mariadb(t *testing.T) {
	sourceDB, sourceName := testutil.CreateTestMariaDB(t)
	indexDB, indexName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	dsn := testutil.MariaDBBaseDSN() + "/" + sourceName + "?parseTime=true"
	runCascade2156(t, gomysql.MariaDBFlavor, dsn, dsn+"&multiStatements=true", sourceDB, indexDB, sourceName, indexName)
}
