//go:build integration

package reconstruct_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestReconstructMydumper_charsetNarrowing is #2735 against a real MySQL:
// each baseline embeds the table's own SHOW CREATE TABLE and rows, each ALTER
// runs on the server, and capture's record of it is what capture writes (a
// real TakeSnapshot after the statement, indexer.InsertSchemaChange with its
// id). The ALTERs write no row events, so the full-table reconstruct carries
// every baseline value through the merge.
//
//   - lossy: utf8mb4 to latin1 under an empty sql_mode, over 'Ñandú ✓'. The
//     server keeps 'Ñandú ?'; the reconstruct must refuse.
//   - roundtrip: the same, then back to utf8mb4. Both ends say utf8mb4 and the
//     value is still 'Ñandú ?'; the reconstruct must refuse.
//   - lossless: utf8mb4 to latin1 under the strict default, over 'Ñandú €',
//     which latin1 holds. Nothing changed; it must answer.
//   - widened: latin1 to utf8mb4. Nothing changed; it must answer.
func TestReconstructMydumper_charsetNarrowing(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()

	indexDB, dbName := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(ctx, indexDB, 48, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	if err := indexer.EnsureSchema(indexDB); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	src := dbName + "_src"
	testutil.MustExec(t, indexDB, "DROP DATABASE IF EXISTS `"+src+"`")
	testutil.MustExec(t, indexDB, "CREATE DATABASE `"+src+"`")
	t.Cleanup(func() { indexDB.Exec("DROP DATABASE IF EXISTS `" + src + "`") })

	conn, err := indexDB.Conn(ctx)
	if err != nil {
		t.Fatalf("Conn: %v", err)
	}
	defer conn.Close()
	exec := func(stmts ...string) {
		t.Helper()
		for _, s := range stmts {
			if _, err := conn.ExecContext(ctx, s); err != nil {
				t.Fatalf("%s: %v", s, err)
			}
		}
	}
	exec("USE `"+src+"`", "SET NAMES utf8mb4",
		"CREATE TABLE lossy (id INT PRIMARY KEY, name VARCHAR(20)) DEFAULT CHARSET=utf8mb4",
		"INSERT INTO lossy VALUES (1, 'Ñandú ✓')",
		"CREATE TABLE roundtrip (id INT PRIMARY KEY, name VARCHAR(20)) DEFAULT CHARSET=utf8mb4",
		"INSERT INTO roundtrip VALUES (1, 'Ñandú ✓')",
		"CREATE TABLE lossless (id INT PRIMARY KEY, name VARCHAR(20)) DEFAULT CHARSET=utf8mb4",
		"INSERT INTO lossless VALUES (1, 'Ñandú €')",
		"CREATE TABLE widened (id INT PRIMARY KEY, name VARCHAR(20)) DEFAULT CHARSET=latin1",
		"INSERT INTO widened VALUES (1, 'Ñandú')",
	)
	tables := []string{"lossy", "roundtrip", "lossless", "widened"}
	createSQL := map[string]string{}
	rows := map[string][]string{}
	for _, tbl := range tables {
		var name, create, id, val string
		if err := conn.QueryRowContext(ctx, "SHOW CREATE TABLE `"+tbl+"`").Scan(&name, &create); err != nil {
			t.Fatalf("SHOW CREATE TABLE %s: %v", tbl, err)
		}
		createSQL[tbl] = create + ";\n"
		if err := conn.QueryRowContext(ctx, "SELECT id, name FROM `"+tbl+"`").Scan(&id, &val); err != nil {
			t.Fatalf("read %s: %v", tbl, err)
		}
		rows[tbl] = []string{id, val}
	}

	base := time.Now().UTC().Truncate(time.Hour)
	cut := base.Add(50 * time.Second)
	// The snapshot in effect at the baseline, then one per ALTER, as capture
	// takes them, each dated when its statement ran.
	snapshotAt := func(at time.Time) int {
		t.Helper()
		st, err := metadata.TakeSnapshot(indexDB, indexDB, []string{src})
		if err != nil {
			t.Fatalf("TakeSnapshot: %v", err)
		}
		testutil.MustExec(t, indexDB, "UPDATE `"+dbName+"`.schema_snapshots SET snapshot_time = ? WHERE snapshot_id = ?", at, st.SnapshotID)
		return st.SnapshotID
	}
	snapshotAt(base.Add(-time.Minute))
	pos := uint64(1000)
	alter := func(tbl string, offset time.Duration, stmts ...string) {
		t.Helper()
		exec(stmts...)
		at := base.Add(offset)
		id := snapshotAt(at)
		pos += 500
		ev := event.Event{
			DDLType: event.DDLAlterTable, Schema: src, Table: tbl, DDLQuery: stmts[len(stmts)-1],
			Timestamp: at, BinlogFile: "binlog.000002", EndPos: pos,
		}
		if err := indexer.InsertSchemaChange(indexDB, ev, &id); err != nil {
			t.Fatalf("InsertSchemaChange: %v", err)
		}
	}
	alter("lossy", 5*time.Second, "SET SESSION sql_mode = ''", "ALTER TABLE lossy MODIFY name VARCHAR(20) CHARACTER SET latin1")
	alter("roundtrip", 6*time.Second, "SET SESSION sql_mode = ''", "ALTER TABLE roundtrip MODIFY name VARCHAR(20) CHARACTER SET latin1")
	alter("roundtrip", 7*time.Second, "ALTER TABLE roundtrip MODIFY name VARCHAR(20) CHARACTER SET utf8mb4")
	alter("lossless", 8*time.Second, "SET SESSION sql_mode = DEFAULT", "ALTER TABLE lossless CONVERT TO CHARACTER SET latin1")
	alter("widened", 9*time.Second, "ALTER TABLE widened CONVERT TO CHARACTER SET utf8mb4")
	for tbl, want := range map[string]string{"lossy": "Ñandú ?", "roundtrip": "Ñandú ?", "lossless": "Ñandú €", "widened": "Ñandú"} {
		var got string
		if err := conn.QueryRowContext(ctx, "SELECT CONVERT(name USING utf8mb4) FROM `"+tbl+"`").Scan(&got); err != nil || got != want {
			t.Fatalf("%s after its ALTER: %q, %v; want %q", tbl, got, err, want)
		}
	}

	root := t.TempDir()
	snapDir := filepath.Join(root, strings.ReplaceAll(base.Format(time.RFC3339), ":", "-"))
	for _, tbl := range tables {
		cols, err := baseline.ParseSchemaText(createSQL[tbl])
		if err != nil {
			t.Fatalf("ParseSchemaText %s: %v", tbl, err)
		}
		w, err := baseline.NewWriter(filepath.Join(snapDir, src, tbl+".parquet"), cols, baseline.WriterConfig{
			Compression: "none", RowGroupSize: 100,
			Metadata: map[string]string{
				baseline.MetaKeyCreateTableSQL:    createSQL[tbl],
				baseline.MetaKeyBinlogFile:        "binlog.000001",
				baseline.MetaKeyBinlogPos:         "4",
				baseline.MetaKeySnapshotTimestamp: base.Format(time.RFC3339),
			},
		})
		if err != nil {
			t.Fatalf("NewWriter %s: %v", tbl, err)
		}
		if err := w.WriteRow(rows[tbl], make([]bool, len(rows[tbl]))); err != nil {
			t.Fatalf("WriteRow %s: %v", tbl, err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("Close %s: %v", tbl, err)
		}
	}
	if err := baseline.WriteSuccessMarker(snapDir); err != nil {
		t.Fatalf("WriteSuccessMarker: %v", err)
	}

	qualified := make([]string, len(tables))
	for i, tbl := range tables {
		qualified[i] = src + "." + tbl
	}
	out := filepath.Join(t.TempDir(), "dump")
	_, failures, err := reconstruct.ReconstructTablesDetailed(ctx, reconstruct.FullTableConfig{
		IndexDSN: testutil.BaseDSN() + "/" + dbName, BaselineSrc: root,
		Tables: qualified, At: cut, OutputDir: out,
		OutputFormat: reconstruct.OutputFormatMydumper,
	})
	if err == nil {
		t.Fatal("the reconstruct answered across two ALTERs that replaced a letter with '?'")
	}
	var refused []string
	for _, f := range failures {
		refused = append(refused, f.Table)
		if !errors.Is(f.Err, reconstruct.ErrDestructiveDDL) || !strings.Contains(f.Err.Error(), "from utf8mb4 to latin1") {
			t.Errorf("%s: refused with %v, want ErrDestructiveDDL naming the narrowing", f.Table, f.Err)
		}
		if strings.Contains(f.Err.Error(), "✓") {
			t.Errorf("%s: the refusal carries the row's value: %v", f.Table, f.Err)
		}
	}
	slices.Sort(refused)
	if !slices.Equal(refused, []string{"lossy", "roundtrip"}) {
		t.Fatalf("refused %v, want exactly lossy and roundtrip (lossless and widened kept every letter)", refused)
	}
	// The dump loads under SET NAMES binary into the CREATE TABLE it carries,
	// the baseline's: widened's still says latin1, so its text is latin1 bytes.
	for tbl, want := range map[string]string{"lossless": "Ñandú €", "widened": "\xd1and\xfa"} {
		data, err := os.ReadFile(filepath.Join(out, src+"."+tbl+".00000.sql"))
		if err != nil {
			t.Fatalf("%s: read the dump: %v", tbl, err)
		}
		if !strings.Contains(string(data), want) {
			t.Errorf("%s: the dump does not hold %q:\n%s", tbl, want, data)
		}
	}

	// A statement whose schema snapshot was not recorded and that may change a
	// character set is said, not guessed.
	ev := event.Event{
		DDLType: event.DDLAlterTable, Schema: src, Table: "widened", DDLQuery: "ALTER TABLE widened MODIFY name VARCHAR(30)",
		Timestamp: base.Add(20 * time.Second), BinlogFile: "binlog.000002", EndPos: pos + 500,
	}
	if err := indexer.InsertSchemaChange(indexDB, ev, nil); err != nil {
		t.Fatalf("InsertSchemaChange: %v", err)
	}
	n, err := reconstruct.FindCharsetNarrowings(ctx, indexDB, src, "widened", reconstruct.DDLWindow{Since: base, Until: cut})
	if err != nil {
		t.Fatalf("FindCharsetNarrowings: %v", err)
	}
	if n.Checked() || len(n.Notes()) != 1 || !strings.Contains(n.Notes()[0], "no schema snapshot with character sets recorded") {
		t.Errorf("an ALTER with no snapshot: checked=%v notes=%q, want one note and nothing to check", n.Checked(), n.Notes())
	}
	if n, err := reconstruct.FindCharsetNarrowings(ctx, indexDB, src, "lossy", reconstruct.DDLWindow{Since: base.Add(10 * time.Second), Until: cut}); err != nil || n.Checked() {
		t.Errorf("the narrowing before the window was counted: checked=%v, %v", n.Checked(), err)
	}

	// An index whose schema snapshots record no character set (before #756)
	// cannot check, and says so for each ALTER whose text may change one.
	testutil.MustExec(t, indexDB, "ALTER TABLE `"+dbName+"`.schema_snapshots DROP COLUMN character_set_name")
	n, err = reconstruct.FindCharsetNarrowings(ctx, indexDB, src, "roundtrip", reconstruct.DDLWindow{Since: base, Until: cut})
	if err != nil || n.Checked() || len(n.Notes()) != 2 {
		t.Errorf("an index with no character sets recorded: checked=%v notes=%q, %v; want a note per ALTER of roundtrip", n.Checked(), n.Notes(), err)
	}
}
