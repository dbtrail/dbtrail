//go:build integration

package reconstruct_test

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestChangesOnly_whatEachCreateShapeRecords runs every shape of statement
// that makes or remakes a table against the real MySQL, reads its binary log
// through the real parser into schema_changes, and asks the refusal of a
// table with no snapshot whether it would tell the operator "the index
// recorded a CREATE TABLE for it at ...". That sentence leads an operator to
// write the table from its changes alone, so it must appear only where the
// table really was new at that moment.
//
// The case that started this: an application that runs CREATE TABLE IF NOT
// EXISTS at every start. The server writes the statement to the binary log
// although the table has existed all along (asserted below, from the binary
// log itself), so its record says nothing about when the table was created.
func TestChangesOnly_whatEachCreateShapeRecords(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()

	sourceDB, src := testutil.CreateTestDB(t)
	indexDB, indexName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	if err := indexer.EnsureSchema(indexDB); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	const cols = "(id INT PRIMARY KEY, v VARCHAR(16))"
	// Tables that exist, with rows, before anything is recorded.
	for _, table := range []string{"t_ine", "t_swap", "t_re"} {
		testutil.MustExec(t, sourceDB, "CREATE TABLE "+table+" "+cols)
		testutil.MustExec(t, sourceDB, "INSERT INTO "+table+" VALUES (1,'a'),(2,'b'),(3,'c')")
	}
	stats, err := metadata.TakeSnapshot(sourceDB, indexDB, []string{src})
	if err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}
	res, err := metadata.NewResolver(indexDB, stats.SnapshotID)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}

	testutil.MustExec(t, sourceDB, "FLUSH BINARY LOGS")
	binlog, _, err := config.CurrentBinlogPosition(sourceDB)
	if err != nil {
		t.Fatalf("CurrentBinlogPosition: %v", err)
	}
	for _, stmt := range []string{
		"CREATE TABLE t_plain " + cols,
		"INSERT INTO t_plain VALUES (1,'a'),(2,'b')",
		// The application's start-up statement, on a table that exists.
		"CREATE TABLE IF NOT EXISTS t_ine " + cols,
		// The same statement on a table that does not exist yet.
		"CREATE TABLE IF NOT EXISTS t_ine_new " + cols,
		"CREATE TABLE t_like LIKE t_plain",
		"CREATE TABLE t_ctas AS SELECT * FROM t_ine",
		// An online schema change: copy under another name, then swap.
		"CREATE TABLE _t_swap_new LIKE t_swap",
		"INSERT INTO _t_swap_new SELECT * FROM t_swap",
		"RENAME TABLE t_swap TO _t_swap_old, _t_swap_new TO t_swap",
		"DROP TABLE t_re",
		"CREATE TABLE t_re " + cols,
	} {
		testutil.MustExec(t, sourceDB, stmt)
	}
	// A temporary table lives on one connection.
	conn, err := sourceDB.Conn(ctx)
	if err != nil {
		t.Fatalf("Conn: %v", err)
	}
	for _, stmt := range []string{"CREATE TEMPORARY TABLE t_tmp " + cols, "INSERT INTO t_tmp VALUES (1,'a')"} {
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	conn.Close()
	testutil.MustExec(t, sourceDB, "FLUSH BINARY LOGS")

	// ── What the binary log holds, from the server itself ────────────────
	rows, err := sourceDB.QueryContext(ctx, "SHOW BINLOG EVENTS IN '"+binlog+"'")
	if err != nil {
		t.Fatalf("SHOW BINLOG EVENTS: %v", err)
	}
	var statements []string
	rowEvents := map[string]int{} // table → Write_rows events
	lastMapped := ""
	for rows.Next() {
		var logName, kind, info string
		var pos, serverID, endPos int64
		if err := rows.Scan(&logName, &pos, &kind, &serverID, &endPos, &info); err != nil {
			t.Fatalf("scan binlog event: %v", err)
		}
		switch kind {
		case "Query":
			if stmt := strings.TrimPrefix(info, "use `"+src+"`; "); stmt != info {
				statements = append(statements, strings.Join(strings.Fields(stmt), " "))
			}
		case "Table_map":
			if i := strings.Index(info, "("+src+"."); i >= 0 {
				lastMapped = strings.TrimSuffix(info[i+len(src)+2:], ")")
			}
		case "Write_rows":
			rowEvents[lastMapped]++
		}
	}
	rows.Close()
	t.Logf("statements in the binary log:\n  %s", strings.Join(statements, "\n  "))
	t.Logf("row-insert events in the binary log, by table: %v", rowEvents)

	logged := func(part string) bool {
		for _, s := range statements {
			if strings.Contains(s, part) {
				return true
			}
		}
		return false
	}
	if !logged("CREATE TABLE IF NOT EXISTS t_ine (") {
		t.Fatal("the server did not log CREATE TABLE IF NOT EXISTS on a table that exists: the premise of this test is gone, read it again")
	}
	if logged("t_tmp") {
		t.Errorf("the server logged the temporary table's statements")
	}
	if rowEvents["t_ctas"] == 0 {
		t.Errorf("CREATE TABLE ... SELECT left no row events for t_ctas: its rows are not in the recorded changes, so its CREATE TABLE's date would promise rows the index does not hold")
	}
	if rowEvents["t_swap"] != 0 {
		t.Errorf("the swap's copy was logged under t_swap: %v", rowEvents)
	}
	if rowEvents["_t_swap_new"] == 0 {
		t.Errorf("the swap's copy left no row events under _t_swap_new: %v", rowEvents)
	}

	// ── What the index records: the real parser over the real file ───────
	tmpDir := t.TempDir()
	cp := exec.Command("docker", "cp",
		fmt.Sprintf("bintrail-test-mysql:/var/lib/mysql/%s", binlog), filepath.Join(tmpDir, binlog))
	if out, err := cp.CombinedOutput(); err != nil {
		t.Fatalf("docker cp %s: %v\n%s", binlog, err, out)
	}
	p := parser.New(tmpDir, res, parser.Filters{Schemas: map[string]bool{src: true}}, nil)
	evCh := make(chan event.Event, 100)
	parseErr := make(chan error, 1)
	go func() {
		defer close(evCh)
		parseErr <- p.ParseFile(ctx, binlog, evCh)
	}()
	for ev := range evCh {
		if ev.EventType != event.EventDDL {
			continue
		}
		if err := indexer.InsertSchemaChange(indexDB, ev, nil); err != nil {
			t.Fatalf("InsertSchemaChange: %v", err)
		}
	}
	if err := <-parseErr; err != nil {
		t.Logf("ParseFile ended with: %v", err)
	}
	recorded, err := indexDB.QueryContext(ctx, `SELECT table_name, ddl_type, LEFT(ddl_query, 70) FROM schema_changes WHERE schema_name = ? ORDER BY id`, src)
	if err != nil {
		t.Fatalf("read schema_changes: %v", err)
	}
	var lines []string
	for recorded.Next() {
		var table, kind, text string
		if err := recorded.Scan(&table, &kind, &text); err != nil {
			t.Fatalf("scan schema_changes: %v", err)
		}
		lines = append(lines, fmt.Sprintf("%-12s %-13s %s", table, kind, strings.Join(strings.Fields(text), " ")))
	}
	recorded.Close()
	t.Logf("schema_changes:\n  %s", strings.Join(lines, "\n  "))

	// ── What the refusal says for each table ─────────────────────────────
	want := map[string]bool{ // table → the refusal carries a creation date
		"t_plain":   true,  // created inside the recorded changes
		"t_like":    true,  // created empty
		"t_ctas":    true,  // its rows follow as row events
		"t_re":      true,  // dropped and created again: new at that moment
		"t_ine":     false, // IF NOT EXISTS on a table that already had rows
		"t_ine_new": false, // IF NOT EXISTS on a new table: the text cannot tell it from the one above
		"t_swap":    false, // its rows were written under another name
		"t_tmp":     false, // never recorded
	}
	var tables []string
	for table := range want {
		tables = append(tables, src+"."+table)
	}
	_, failures, err := reconstruct.ReconstructTablesDetailed(ctx, reconstruct.FullTableConfig{
		IndexDSN:     testutil.BaseDSN() + "/" + indexName,
		BaselineSrc:  t.TempDir(),
		Tables:       tables,
		At:           time.Now().UTC().Add(time.Hour),
		OutputDir:    t.TempDir(),
		OutputFormat: reconstruct.OutputFormatMydumper,
	})
	if err == nil {
		t.Fatal("tables with no snapshot were not refused")
	}
	seen := map[string]bool{}
	for _, f := range failures {
		var refusal *reconstruct.ChangesOnlyError
		if !errors.As(f.Err, &refusal) {
			t.Errorf("%s failed for another reason: %v", f.Table, f.Err)
			continue
		}
		seen[f.Table] = true
		got := !refusal.CreateRecordedAt.IsZero()
		t.Logf("%-10s refusal carries a creation date: %v", f.Table, got)
		if got != want[f.Table] {
			t.Errorf("%s: refusal carries a creation date = %v, want %v", f.Table, got, want[f.Table])
		}
	}
	if len(seen) != len(want) {
		t.Errorf("refused %d tables, want %d", len(seen), len(want))
	}
}
