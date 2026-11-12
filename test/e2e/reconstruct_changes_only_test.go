//go:build integration

package e2e_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestReconstructFullTable_changesOnlyIsNeverAQuietSuccess runs the real
// binary over real files. A table with no snapshot at or before the moment
// can only be rebuilt from its recorded changes, which leaves out every row
// no change touched. That used to be written as the table's dump with exit
// status 0 and one warning; it is now refused unless --allow-changes-only is
// passed, and with the flag the summary and the schema file say what the
// output is.
//
// The fixture: testdb.orders has rows 1, 2, 3 in its snapshot, then row 2 is
// updated and row 4 inserted, so rows 1 and 3 exist only in the snapshot.
// testdb.items has rows 10, 11 and one update. "both" holds a snapshot of the
// two tables; "itemsOnly" holds a snapshot of testdb.items alone, which is
// what a run sees for testdb.orders when its snapshot is gone.
func TestReconstructFullTable_changesOnlyIsNeverAQuietSuccess(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()

	binPath := filepath.Join(t.TempDir(), "bintrail")
	build := exec.Command("go", "build", "-o", binPath, "./cmd/bintrail")
	build.Dir = projectRoot(t)
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("go build failed: %v\n%s", err, out)
	}

	db, dbName := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(ctx, db, 48, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	indexDSN := testutil.SnapshotDSN(dbName)

	base := time.Now().UTC().Truncate(time.Hour)
	at := base.Add(50 * time.Minute)
	ts := func(tm time.Time) string { return tm.Format("2006-01-02 15:04:05") }
	for _, table := range []string{"orders", "items"} {
		testutil.InsertSnapshot(t, db, 1, ts(base), "testdb", table, "id", 1, "PRI", "int", "NO")
		testutil.InsertSnapshot(t, db, 1, ts(base), "testdb", table, "status", 2, "", "varchar", "NO")
	}

	both, itemsOnly := t.TempDir(), t.TempDir()
	writeSnapshotTable(t, both, base, "orders", [][]string{{"1", "start-1"}, {"2", "start-2"}, {"3", "start-3"}})
	writeSnapshotTable(t, both, base, "items", [][]string{{"10", "ten"}, {"11", "eleven"}})
	writeSnapshotTable(t, itemsOnly, base, "items", [][]string{{"10", "ten"}, {"11", "eleven"}})
	for _, root := range []string{both, itemsOnly} {
		if err := baseline.WriteSuccessMarker(snapshotDir(root, base)); err != nil {
			t.Fatalf("WriteSuccessMarker: %v", err)
		}
	}

	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, ts(base.Add(10*time.Minute)), nil,
		"testdb", "orders", 2 /* UPDATE */, "2", nil,
		[]byte(`{"id":2,"status":"start-2"}`), []byte(`{"id":2,"status":"paid"}`))
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300, ts(base.Add(20*time.Minute)), nil,
		"testdb", "orders", 1 /* INSERT */, "4", nil,
		nil, []byte(`{"id":4,"status":"new-4"}`))
	testutil.InsertEvent(t, db, "binlog.000001", 300, 400, ts(base.Add(30*time.Minute)), nil,
		"testdb", "items", 2 /* UPDATE */, "10", nil,
		[]byte(`{"id":10,"status":"ten"}`), []byte(`{"id":10,"status":"TEN"}`))

	// run executes `bintrail reconstruct` in full-table mode and returns the
	// output directory, what it printed and its exit status.
	run := func(t *testing.T, snapshots, format, tables string, extra ...string) (outDir, output string, status int) {
		t.Helper()
		outDir = t.TempDir()
		args := append([]string{"reconstruct",
			"--index-dsn", indexDSN, "--baseline-dir", snapshots,
			"--tables", tables, "--at", at.Format(time.RFC3339),
			"--output-format", format, "--output", outDir, "--parallelism", "1"}, extra...)
		cmd := exec.Command(binPath, args...)
		var buf bytes.Buffer
		cmd.Stdout, cmd.Stderr = &buf, &buf
		err := cmd.Run()
		var exitErr *exec.ExitError
		switch {
		case err == nil:
		case errors.As(err, &exitErr):
			status = exitErr.ExitCode()
		default:
			t.Fatalf("run bintrail: %v", err)
		}
		output = strings.ReplaceAll(buf.String(), indexDSN, "<index-dsn>")
		t.Logf("bintrail reconstruct --tables %s --output-format %s %s\nexit status: %d\nfiles: %v\n%s",
			tables, format, strings.Join(extra, " "), status, listFiles(t, outDir), output)
		return outDir, output, status
	}

	t.Run("with a snapshot the table is complete", func(t *testing.T) {
		outDir, _, status := run(t, both, "mydumper", "testdb.orders")
		if status != 0 {
			t.Fatalf("exit status = %d, want 0", status)
		}
		data := tableData(t, outDir, "testdb.orders")
		for _, want := range []string{"(1, 'start-1')", "(2, 'paid')", "(3, 'start-3')", "(4, 'new-4')"} {
			if !strings.Contains(data, want) {
				t.Errorf("dump is missing row %s:\n%s", want, data)
			}
		}
	})

	t.Run("no snapshot, mydumper output: refused, nothing written for the table", func(t *testing.T) {
		outDir, output, status := run(t, itemsOnly, "mydumper", "testdb.orders")
		if status == 0 {
			t.Errorf("exit status = 0: a dump holding only the changed rows was reported as a success")
		}
		if files := tableFiles(t, outDir, "testdb.orders"); len(files) != 0 {
			t.Errorf("refused table left files a loader would take for the table: %v", files)
		}
		if _, err := os.Stat(filepath.Join(outDir, baseline.SuccessMarker)); err == nil {
			t.Errorf("the output directory is marked %s", baseline.SuccessMarker)
		}
		for _, want := range []string{"testdb.orders", "every other row", "--allow-changes-only", "does not include testdb.orders"} {
			if !strings.Contains(output, want) {
				t.Errorf("refusal does not say %q", want)
			}
		}
	})

	t.Run("no snapshot, parquet output: refused as before", func(t *testing.T) {
		_, output, status := run(t, itemsOnly, "parquet", "testdb.orders")
		if status == 0 {
			t.Errorf("exit status = 0, want the refusal to write a snapshot from changes alone")
		}
		if !strings.Contains(output, "cannot be re-emitted as one") {
			t.Errorf("the Parquet refusal changed its wording")
		}
		if strings.Contains(output, "--allow-changes-only") {
			t.Errorf("the Parquet refusal offers --allow-changes-only, which it never honors")
		}
	})

	t.Run("two tables, one without its snapshot: the run fails and names it", func(t *testing.T) {
		outDir, output, status := run(t, itemsOnly, "mydumper", "testdb.items,testdb.orders")
		if status == 0 {
			t.Errorf("exit status = 0 with testdb.orders rebuilt from its changes alone")
		}
		if files := tableFiles(t, outDir, "testdb.orders"); len(files) != 0 {
			t.Errorf("refused table left files: %v", files)
		}
		if data := tableData(t, outDir, "testdb.items"); !strings.Contains(data, "(10, 'TEN')") || !strings.Contains(data, "(11, 'eleven')") {
			t.Errorf("testdb.items, which has its snapshot, was not written in full:\n%s", data)
		}
		if _, err := os.Stat(filepath.Join(outDir, baseline.SuccessMarker)); err == nil {
			t.Errorf("the output directory is marked %s", baseline.SuccessMarker)
		}
		if !strings.Contains(output, "testdb.orders") || !strings.Contains(output, "--allow-changes-only") {
			t.Errorf("refusal does not name the table and the flag")
		}
	})

	t.Run("--allow-changes-only writes the changed rows and says so", func(t *testing.T) {
		outDir, output, status := run(t, itemsOnly, "mydumper", "testdb.items,testdb.orders", "--allow-changes-only")
		if status != 0 {
			t.Fatalf("exit status = %d, want 0 with the flag", status)
		}
		data := tableData(t, outDir, "testdb.orders")
		for _, want := range []string{"(2, 'paid')", "(4, 'new-4')"} {
			if !strings.Contains(data, want) {
				t.Errorf("changes-only dump is missing changed row %s:\n%s", want, data)
			}
		}
		for _, absent := range []string{"start-1", "start-3"} {
			if strings.Contains(data, absent) {
				t.Errorf("changes-only dump holds a row no change touched (%s):\n%s", absent, data)
			}
		}
		schemaFile, err := os.ReadFile(filepath.Join(outDir, "testdb.orders-schema.sql"))
		if err != nil {
			t.Fatalf("read schema file: %v", err)
		}
		t.Logf("testdb.orders-schema.sql:\n%s", schemaFile)
		if !strings.HasPrefix(string(schemaFile), "-- bintrail: CHANGES ONLY.") {
			t.Errorf("the schema file does not open with the changes-only marker:\n%s", schemaFile)
		}
		var summary string
		for _, line := range strings.Split(output, "\n") {
			if strings.Contains(line, "changes only") && strings.Contains(line, "table=orders") {
				summary = line
			}
			if strings.Contains(line, "changes only") && strings.Contains(line, "table=items") {
				t.Errorf("testdb.items was built from its snapshot but the summary calls it changes only: %s", line)
			}
		}
		if summary == "" {
			t.Errorf("the summary does not mark testdb.orders as changes only")
		}
		if !strings.Contains(output, "changes_only_tables=testdb.orders") {
			t.Errorf("the closing line does not list the changes-only table")
		}
	})

	t.Run("--allow-changes-only is refused with parquet output", func(t *testing.T) {
		_, output, status := run(t, itemsOnly, "parquet", "testdb.orders", "--allow-changes-only")
		if status == 0 || !strings.Contains(output, "--allow-changes-only applies only to --output-format mydumper") {
			t.Errorf("exit status = %d, want the flag refused with Parquet output", status)
		}
	})
}

func snapshotDir(root string, at time.Time) string {
	return filepath.Join(root, strings.ReplaceAll(at.Format(time.RFC3339), ":", "-"))
}

// writeSnapshotTable writes testdb.<table> with (id, status) rows into the
// snapshot taken at `at` under root.
func writeSnapshotTable(t *testing.T, root string, at time.Time, table string, rows [][]string) {
	t.Helper()
	createSQL := "CREATE TABLE `" + table + "` (\n  `id` int NOT NULL,\n  `status` varchar(64) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB;\n"
	cols, err := baseline.ParseSchemaText(createSQL)
	if err != nil {
		t.Fatalf("ParseSchemaText: %v", err)
	}
	w, err := baseline.NewWriter(filepath.Join(snapshotDir(root, at), "testdb", table+".parquet"), cols, baseline.WriterConfig{
		Compression: "none", RowGroupSize: 100,
		Metadata: map[string]string{
			baseline.MetaKeyCreateTableSQL: createSQL,
			baseline.MetaKeyBinlogFile:     "binlog.000001",
			baseline.MetaKeyBinlogPos:      "4",
		},
	})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	for _, row := range rows {
		if err := w.WriteRow(row, make([]bool, len(row))); err != nil {
			t.Fatalf("WriteRow: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func listFiles(t *testing.T, dir string) []string {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	var names []string
	for _, e := range ents {
		names = append(names, e.Name())
	}
	sort.Strings(names)
	return names
}

// tableFiles is every file in dir a loader reads for the table: its schema
// file and its data chunks.
func tableFiles(t *testing.T, dir, name string) []string {
	t.Helper()
	var out []string
	for _, f := range listFiles(t, dir) {
		if strings.HasPrefix(f, name+".") || strings.HasPrefix(f, name+"-") {
			out = append(out, f)
		}
	}
	return out
}

// tableData is the table's data chunks, concatenated.
func tableData(t *testing.T, dir, name string) string {
	t.Helper()
	var b strings.Builder
	for _, f := range tableFiles(t, dir, name) {
		if strings.HasSuffix(f, "-schema.sql") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(dir, f))
		if err != nil {
			t.Fatalf("read %s: %v", f, err)
		}
		b.Write(raw)
	}
	return b.String()
}
