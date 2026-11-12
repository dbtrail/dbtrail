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

	// bintrail runs the binary and returns what it printed and its exit status.
	bintrail := func(t *testing.T, args ...string) (output string, status int) {
		t.Helper()
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
		return strings.ReplaceAll(buf.String(), indexDSN, "<index-dsn>"), status
	}
	// run executes `bintrail reconstruct` in full-table mode and returns the
	// output directory, what it printed and its exit status.
	run := func(t *testing.T, snapshots, format, tables string, extra ...string) (outDir, output string, status int) {
		t.Helper()
		outDir = t.TempDir()
		output, status = bintrail(t, append([]string{"reconstruct",
			"--index-dsn", indexDSN, "--baseline-dir", snapshots,
			"--tables", tables, "--at", at.Format(time.RFC3339),
			"--output-format", format, "--output", outDir, "--parallelism", "1"}, extra...)...)
		t.Logf("bintrail reconstruct --tables %s --output-format %s %s\nexit status: %d\nfiles: %v\n%s",
			tables, format, strings.Join(extra, " "), status, listFiles(t, outDir), output)
		return outDir, output, status
	}
	// convert executes `bintrail baseline` over a dump folder and returns the
	// new snapshots folder, what it printed and its exit status.
	convert := func(t *testing.T, dump string, extra ...string) (snapshots, output string, status int) {
		t.Helper()
		snapshots = t.TempDir()
		output, status = bintrail(t, append([]string{"baseline", "--input", dump, "--output", snapshots}, extra...)...)
		output = strings.ReplaceAll(output, dump, "<dump>")
		t.Logf("bintrail baseline --input <dump> %s\nexit status: %d\nfiles: %v\n%s",
			strings.Join(extra, " "), status, listFiles(t, snapshots), output)
		return snapshots, output, status
	}
	// marked fails unless dir holds the marker file `have` and lacks `lack`.
	// Both are checked: a folder with neither reads as complete by the rule
	// for folders older than the markers.
	marked := func(t *testing.T, dir, have, lack string) {
		t.Helper()
		if _, err := os.Stat(filepath.Join(dir, have)); err != nil {
			t.Errorf("the output directory is not marked %s: %v", have, err)
		}
		if _, err := os.Stat(filepath.Join(dir, lack)); err == nil {
			t.Errorf("the output directory is marked %s", lack)
		}
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
		marked(t, outDir, baseline.IncompleteMarker, baseline.SuccessMarker)
		for _, want := range []string{"testdb.orders: there is no snapshot", "every other row", "--allow-changes-only", "does not include testdb.orders"} {
			if !strings.Contains(output, want) {
				t.Errorf("refusal does not say %q", want)
			}
		}
		// The binary prints the error inside JSON: one refused table must be
		// one line there, with no line break shown as \n, and the table is
		// not named twice in a row.
		if i := strings.Index(output, `{"error"`); i < 0 {
			t.Errorf("no error was printed")
		} else {
			for _, bad := range []string{`\n`, "testdb.orders: testdb.orders"} {
				if strings.Contains(output[i:], bad) {
					t.Errorf("the printed error holds %q", bad)
				}
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
		marked(t, outDir, baseline.IncompleteMarker, baseline.SuccessMarker)
		if !strings.Contains(output, "testdb.orders") || !strings.Contains(output, "--allow-changes-only") {
			t.Errorf("refusal does not name the table and the flag")
		}
		// The folder holds one complete table and is not finished: it is not
		// converted into a snapshot, which would be published as complete.
		snapshots, converted, status := convert(t, outDir)
		if status == 0 || !strings.Contains(converted, "_INCOMPLETE") {
			t.Errorf("exit status = %d: a folder its run did not finish was converted into a snapshot", status)
		}
		if files := listFiles(t, snapshots); len(files) != 0 {
			t.Errorf("the refused conversion wrote %v", files)
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
			if strings.Contains(line, "table dump holds changes only") && strings.Contains(line, "table=orders") {
				summary = line
			}
			if strings.Contains(line, "table dump complete") && strings.Contains(line, "table=orders") {
				t.Errorf("testdb.orders is reported as a complete dump: %s", line)
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
		// As documented: the run wrote what was asked for, so the folder is
		// marked complete and carries the position of the tables that had a
		// snapshot. It also lists the table that is changes only.
		marked(t, outDir, baseline.SuccessMarker, baseline.IncompleteMarker)
		if _, err := os.Stat(filepath.Join(outDir, "metadata")); err != nil {
			t.Errorf("no metadata file: %v", err)
		}
		listed, err := baseline.ReadChangesOnlyTables(outDir)
		if err != nil || len(listed) != 1 || !listed["testdb.orders"] {
			t.Errorf("%s lists %v (err %v), want testdb.orders alone", baseline.ChangesOnlyMarkerFile, listed, err)
		}

		// That folder looks like any dump. Converted into a snapshot, the
		// two-row testdb.orders would be what every later reconstruct starts
		// from, with no flag and no warning. `bintrail baseline` refuses it.
		snapshots, converted, status := convert(t, outDir)
		if status == 0 {
			t.Errorf("exit status = 0: the changes-only table was converted into a snapshot")
		}
		for _, want := range []string{"testdb.orders in <dump>", "missing every other row", "Nothing was converted", "--tables"} {
			if !strings.Contains(converted, want) {
				t.Errorf("the conversion's refusal does not say %q", want)
			}
		}
		if files := listFiles(t, snapshots); len(files) != 0 {
			t.Errorf("the refused conversion wrote %v", files)
		}
		// The complete table of the same folder converts when it is named.
		if _, _, status := convert(t, outDir, "--tables", "testdb.items"); status != 0 {
			t.Errorf("exit status = %d: testdb.items, built from its snapshot, was not converted when named", status)
		}
		// A copy of the folder that lost the marker file is still refused,
		// by the first line of the table's schema file.
		if err := os.Remove(filepath.Join(outDir, baseline.ChangesOnlyMarkerFile)); err != nil {
			t.Fatal(err)
		}
		if _, converted, status := convert(t, outDir); status == 0 || !strings.Contains(converted, "testdb.orders in <dump>") {
			t.Errorf("exit status = %d: without the marker file the changes-only table was converted", status)
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
