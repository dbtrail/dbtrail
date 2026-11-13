//go:build integration

package reconstruct_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestRefresh_charsetChange is #2723 against what a real MySQL prints: each
// baseline embeds the table's own SHOW CREATE TABLE, and the schema snapshot
// is a real `bintrail snapshot` of the table after the ALTER.
//
//   - narrowed: utf8mb4 to latin1 under a non-strict sql_mode. MySQL turns the
//     character latin1 cannot hold into '?' and writes no row event, so a
//     refresh that published would keep the old value.
//   - widened: CONVERT TO CHARACTER SET utf8mb4 on a latin1 table. Every value
//     is kept, and a refresh that published would carry CHARACTER SET latin1
//     forward into every restore.
//
// Both must refuse with ErrSchemaChanged naming the column and both character
// sets. The controls are tables with mixed character sets and no ALTER: the
// columns SHOW CREATE TABLE writes a character set for and the ones it leaves
// to the table's default, a collation alone, utf8mb3 and ENUM. They must
// refresh, which is what pins that the resolution matches the server's own
// output and that no refusal is invented.
func TestRefresh_charsetChange(t *testing.T) {
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
		"CREATE TABLE narrowed (id INT PRIMARY KEY, name VARCHAR(20)) DEFAULT CHARSET=utf8mb4",
		"INSERT INTO narrowed VALUES (1, 'Ñandú ✓')",
		"CREATE TABLE widened (id INT PRIMARY KEY, name VARCHAR(20)) DEFAULT CHARSET=latin1",
		"INSERT INTO widened VALUES (1, 'Ñandú')",
		"CREATE TABLE mixed (id INT PRIMARY KEY, a VARCHAR(10), b VARCHAR(10) CHARACTER SET latin1, "+
			"c VARCHAR(10) COLLATE utf8mb4_bin, d VARCHAR(10) CHARACTER SET utf8mb3, "+
			"e ENUM('x','y') CHARACTER SET latin1, f TEXT CHARACTER SET ascii, i INT) DEFAULT CHARSET=utf8mb4",
		"INSERT INTO mixed VALUES (1, 'a', 'b', 'c', 'd', 'x', 'f', 1)",
		"CREATE TABLE mixed_latin1 (id INT PRIMARY KEY, a VARCHAR(10), b VARCHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci, "+
			"c CHAR(2) COLLATE latin1_bin) DEFAULT CHARSET=latin1",
		"INSERT INTO mixed_latin1 VALUES (1, 'a', 'b', 'c')",
	)

	tables := []string{"narrowed", "widened", "mixed", "mixed_latin1"}
	createSQL := make(map[string]string, len(tables))
	rows := make(map[string][]string, len(tables))
	for _, tbl := range tables {
		var name, create string
		if err := conn.QueryRowContext(ctx, "SHOW CREATE TABLE `"+tbl+"`").Scan(&name, &create); err != nil {
			t.Fatalf("SHOW CREATE TABLE %s: %v", tbl, err)
		}
		createSQL[tbl] = create + ";\n"
		r, err := conn.QueryContext(ctx, "SELECT * FROM `"+tbl+"`")
		if err != nil {
			t.Fatalf("read %s: %v", tbl, err)
		}
		cols, _ := r.Columns()
		vals := make([]string, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		for r.Next() {
			if err := r.Scan(ptrs...); err != nil {
				t.Fatalf("scan %s: %v", tbl, err)
			}
		}
		r.Close()
		rows[tbl] = vals
	}

	exec("SET SESSION sql_mode = ''",
		"ALTER TABLE narrowed MODIFY name VARCHAR(20) CHARACTER SET latin1",
		"ALTER TABLE widened CONVERT TO CHARACTER SET utf8mb4")
	var after string
	if err := conn.QueryRowContext(ctx, "SELECT CONVERT(name USING utf8mb4) FROM narrowed WHERE id = 1").Scan(&after); err != nil || after != "Ñandú ?" {
		t.Fatalf("narrowed after the ALTER: %q, %v; want the ✓ replaced by '?'", after, err)
	}

	// The schema snapshot after both ALTERs, dated between the baseline and the
	// target so it is the one in effect there.
	base := time.Now().UTC().Truncate(time.Hour)
	cut := base.Add(30 * time.Second)
	if _, err := metadata.TakeSnapshot(indexDB, indexDB, []string{src}); err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}
	testutil.MustExec(t, indexDB, "UPDATE `"+dbName+"`.schema_snapshots SET snapshot_time = ?", base.Add(5*time.Second))
	var narrowedCharset string
	if err := indexDB.QueryRow("SELECT character_set_name FROM `"+dbName+"`.schema_snapshots WHERE table_name = 'narrowed' AND column_name = 'name'").
		Scan(&narrowedCharset); err != nil || narrowedCharset != "latin1" {
		t.Fatalf("the snapshot's character set for narrowed.name: %q, %v; want latin1", narrowedCharset, err)
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

	refresh := func(tbls ...string) ([]reconstruct.TableFailure, error) {
		qualified := make([]string, len(tbls))
		for i, tbl := range tbls {
			qualified[i] = src + "." + tbl
		}
		_, failures, err := reconstruct.ReconstructTablesDetailed(ctx, reconstruct.FullTableConfig{
			IndexDSN: testutil.BaseDSN() + "/" + dbName, BaselineSrc: root,
			Tables: qualified, At: cut, OutputDir: root,
			OutputFormat: reconstruct.OutputFormatParquet,
		})
		return failures, err
	}

	t.Run("a changed character set refuses, naming the column and both sets", func(t *testing.T) {
		failures, err := refresh(tables...)
		if err == nil {
			t.Fatal("the refresh published across two character set changes")
		}
		want := map[string]string{
			"narrowed": "name (varchar(20) CHARACTER SET utf8mb4 -> varchar(20) CHARACTER SET latin1)",
			"widened":  "name (varchar(20) CHARACTER SET latin1 -> varchar(20) CHARACTER SET utf8mb4)",
		}
		var refused []string
		for _, f := range failures {
			refused = append(refused, f.Table)
			if !errors.Is(f.Err, reconstruct.ErrSchemaChanged) {
				t.Errorf("%s: refused with %v, want ErrSchemaChanged", f.Table, f.Err)
			}
			if w, ok := want[f.Table]; !ok || !strings.Contains(f.Err.Error(), w) {
				t.Errorf("%s: refusal %q, want it to name %q", f.Table, f.Err, w)
			}
		}
		slices.Sort(refused)
		if !slices.Equal(refused, []string{"narrowed", "widened"}) {
			t.Errorf("refused %v, want exactly narrowed and widened (the mixed tables had no ALTER)", refused)
		}
	})

	t.Run("tables with mixed character sets and no ALTER refresh", func(t *testing.T) {
		before := map[string]string{}
		for _, tbl := range []string{"mixed", "mixed_latin1"} {
			p, _, _, err := reconstruct.FindBaseline(ctx, root, src, tbl, cut)
			if err != nil {
				t.Fatalf("FindBaseline %s: %v", tbl, err)
			}
			before[tbl] = p
		}
		failures, err := refresh("mixed", "mixed_latin1")
		if err != nil {
			t.Fatalf("the refresh refused tables whose character sets did not change: %v\n%s", err, fmt.Sprint(failures))
		}
		for tbl, p := range before {
			newest, _, _, err := reconstruct.FindBaseline(ctx, root, src, tbl, cut.Add(time.Second))
			if err != nil || newest == p {
				t.Errorf("%s: no snapshot published (newest %s, source %s, %v)", tbl, newest, p, err)
			}
		}
	})
}
