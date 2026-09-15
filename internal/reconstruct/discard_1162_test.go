package reconstruct

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/query"
)

// orderedBinaryPKBaseline is binaryPKBaseline with a deterministic row order:
// the #1162 tests below need clean rows to be scanned — and therefore written
// to the chunk file — BEFORE the row that trips the #1158 guard, and a map
// fixture cannot promise that.
func orderedBinaryPKBaseline(t *testing.T, dir string, rows [][2]string) string {
	t.Helper()
	cols := []baseline.Column{
		{Name: "k", MySQLType: "binary", ParquetType: baseline.MysqlToParquetNode("binary")},
		{Name: "val", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	}
	path := filepath.Join(dir, "bp.parquet")
	w, err := baseline.NewWriter(path, cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 100})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	for _, r := range rows {
		if err := w.WriteRow([]string{"0x" + r[0], r[1]}, []bool{false, false}); err != nil {
			t.Fatalf("WriteRow: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return path
}

// listSQLFiles returns every *.sql file in dir whose name starts with prefix.
func listSQLFiles(t *testing.T, dir, prefix string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read output dir: %v", err)
	}
	var out []string
	for _, e := range entries {
		name := e.Name()
		if strings.HasPrefix(name, prefix) && strings.HasSuffix(name, ".sql") {
			out = append(out, name)
		}
	}
	return out
}

// TestMergeBaselineIntoWriter_discardsArtifactsOnMidScanError is #1162, driven
// through the real production path: the #1158 mis-keyed-row guard is the first
// PER-ROW guard in mergeBaselineIntoWriter's merge, so — unlike the pre-writer
// #602/#843 guards — it fires after the schema file exists and after earlier
// clean rows have already been written to the current chunk. Before the fix,
// the deferred Close FINALIZED that chunk (terminating ";\n", flush, file
// kept), leaving a syntactically valid, silently-truncated <db>.<table>.NNNNN.sql
// plus the schema file on disk; myloader and `cat out/*.sql | mysql` read
// neither the run-level _INCOMPLETE marker nor anything else that flags it.
//
// The fixture scans two clean rows first (so rows really are in the chunk when
// the guard fires) and then a row whose pending DELETE event is keyed under the
// padded spelling — the #1158 shape.
func TestMergeBaselineIntoWriter_discardsArtifactsOnMidScanError(t *testing.T) {
	const (
		cleanKey1 = "0102030405060708090A0B0C0D0E0F10" // full width, no event
		cleanKey2 = "AA02030405060708090A0B0C0D0E0F10" // full width, no event
		paddedKey = "11223344556677889900AABB00000000" // its event is mis-keyed
	)
	baselinePath := orderedBinaryPKBaseline(t, t.TempDir(), [][2]string{
		{cleanKey1, "row1"},
		{cleanKey2, "row2"},
		{paddedKey, "doomed"},
	})
	outDir := t.TempDir()

	rep := &TableReport{Schema: "db", Table: "bp"}
	err := mergeBaselineIntoWriter(context.Background(), mergeInput{
		LocalBaselinePath: baselinePath,
		CreateTableSQL:    "CREATE TABLE `bp` (`k` BINARY(16) PRIMARY KEY, `val` VARCHAR(32));\n",
		Schema:            "db",
		Table:             "bp",
		PKCols:            binaryPKCols(),
		Changes: map[string]*query.ResultRow{
			// Keyed by the PADDED spelling — NOT what canonicalization produces.
			"0x" + paddedKey: {
				EventType: event.EventDelete, SchemaName: "db", TableName: "bp",
				PKValues: "0x" + paddedKey,
			},
		},
		OutputDir: outDir,
		ChunkSize: 0,
	}, rep)

	// The error must still reach the caller — the discard is cleanup, not a
	// swallow.
	if err == nil {
		t.Fatal("mergeBaselineIntoWriter must fail on the mis-keyed row")
	}
	if !strings.Contains(err.Error(), "baseline merge") {
		t.Fatalf("expected the #1158 guard error, got: %v", err)
	}

	// The failed table must leave NOTHING behind: no truncated chunk, no
	// orphan schema file. WriteSchema ran before the merge and clean rows were
	// written before the guard fired, so these files existed and must have
	// been unlinked, not finalized.
	if left := listSQLFiles(t, outDir, "db.bp"); len(left) != 0 {
		for _, name := range left {
			b, _ := os.ReadFile(filepath.Join(outDir, name))
			t.Logf("surviving file %s:\n%s", name, b)
		}
		t.Fatalf("failed table left %d artifact(s) on disk: %v", len(left), left)
	}
	if len(rep.Files) != 0 {
		t.Errorf("rep.Files must stay empty on failure, got %v", rep.Files)
	}
}

// TestMergeBaselineIntoWriter_siblingTableOutputSurvivesDiscard pins the
// multi-table composition: ReconstructTables writes every table into ONE
// OutputDir and collects per-table errors, so table B failing must remove only
// B's artifacts — table A's completed, finalized output stays intact and
// loadable.
func TestMergeBaselineIntoWriter_siblingTableOutputSurvivesDiscard(t *testing.T) {
	outDir := t.TempDir()

	// Table A: completes cleanly.
	repA := &TableReport{Schema: "mydb", Table: "orders"}
	if err := mergeBaselineIntoWriter(context.Background(), mergeInput{
		LocalBaselinePath: writeTestBaseline(t, [][]string{{"1", "new"}, {"2", "paid"}}),
		CreateTableSQL:    "-- table A schema",
		Schema:            "mydb",
		Table:             "orders",
		PKCols:            pkColsIntID(),
		Changes:           map[string]*query.ResultRow{},
		OutputDir:         outDir,
	}, repA); err != nil {
		t.Fatalf("table A merge: %v", err)
	}
	if len(repA.Files) == 0 {
		t.Fatal("table A produced no files; fixture is broken")
	}

	// Table B: fails mid-scan on the #1158 guard, same OutputDir.
	const paddedKey = "11223344556677889900AABB00000000"
	repB := &TableReport{Schema: "db", Table: "bp"}
	err := mergeBaselineIntoWriter(context.Background(), mergeInput{
		LocalBaselinePath: orderedBinaryPKBaseline(t, t.TempDir(), [][2]string{{paddedKey, "doomed"}}),
		CreateTableSQL:    "-- table B schema",
		Schema:            "db",
		Table:             "bp",
		PKCols:            binaryPKCols(),
		Changes: map[string]*query.ResultRow{
			"0x" + paddedKey: {
				EventType: event.EventDelete, SchemaName: "db", TableName: "bp",
				PKValues: "0x" + paddedKey,
			},
		},
		OutputDir: outDir,
	}, repB)
	if err == nil {
		t.Fatal("table B merge must fail")
	}

	// B's artifacts are gone; A's are untouched and still finalized.
	if left := listSQLFiles(t, outDir, "db.bp"); len(left) != 0 {
		t.Errorf("failed table B left artifacts: %v", left)
	}
	for _, name := range repA.Files {
		b, rerr := os.ReadFile(filepath.Join(outDir, name))
		if rerr != nil {
			t.Fatalf("table A file %s vanished after table B's discard: %v", name, rerr)
		}
		if strings.HasSuffix(name, "-schema.sql") {
			continue
		}
		if !strings.HasSuffix(string(b), ";\n") {
			t.Errorf("table A chunk %s lost its terminator:\n%s", name, b)
		}
	}
}

// TestMydumperWriter_discard covers the writer-level contract directly:
// Discard removes everything the writer created — schema file, rotated chunks,
// and the in-progress chunk — clears Files(), and leaves the writer terminal.
func TestMydumperWriter_discard(t *testing.T) {
	dir := t.TempDir()
	// 1-byte chunkSize rotates after every row, so two rows leave two
	// finalized chunks on disk before the third opens a fresh in-progress one.
	w, err := NewMydumperWriter(dir, "mydb", "users", []string{"id"}, 1)
	if err != nil {
		t.Fatalf("NewMydumperWriter: %v", err)
	}
	if err := w.WriteSchema("-- schema"); err != nil {
		t.Fatalf("WriteSchema: %v", err)
	}
	for i := int64(1); i <= 2; i++ {
		if err := w.WriteRow([]any{i}); err != nil {
			t.Fatalf("WriteRow %d: %v", i, err)
		}
	}
	if got := len(w.Files()); got != 3 { // schema + 2 rotated chunks
		t.Fatalf("fixture expected 3 files before the in-progress row, got %d: %v", got, w.Files())
	}
	// Third row opens (but does not finalize) chunk 00002.
	if err := w.WriteRow([]any{int64(3)}); err != nil {
		t.Fatalf("WriteRow 3: %v", err)
	}

	if err := w.Discard(); err != nil {
		t.Fatalf("Discard: %v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	if len(entries) != 0 {
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Errorf("Discard left files behind: %v", names)
	}
	if got := w.Files(); len(got) != 0 {
		t.Errorf("Files() after Discard = %v, want empty", got)
	}
	if err := w.WriteRow([]any{int64(4)}); err != ErrWriterClosed {
		t.Errorf("WriteRow after Discard = %v, want ErrWriterClosed", err)
	}
	// Idempotent.
	if err := w.Discard(); err != nil {
		t.Errorf("second Discard: %v", err)
	}
}

// TestWriteBinlogOnlyChanges_errorLeavesNoArtifacts covers the binlog-only
// fallback's error path, which got the same deferred discard: that path has no
// pre-writer guards at all, so any failure must also leave nothing behind. The
// injectable failure through the real code here is the schema write (the first
// file operation); the mid-write mechanics are pinned by the merge-path test
// above and TestMydumperWriter_discard, which share the same Discard.
func TestWriteBinlogOnlyChanges_errorLeavesNoArtifacts(t *testing.T) {
	outDir := t.TempDir()
	if err := os.Chmod(outDir, 0o555); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(outDir, 0o755) })

	rep := &TableReport{Schema: "mydb", Table: "orders"}
	err := writeBinlogOnlyChanges(outDir, "mydb", "orders", pkColsIntID(), []string{"id", "status"}, 0, nil,
		"-- schema", map[string]*query.ResultRow{}, rep)
	if err == nil {
		t.Skip("running as a user unaffected by directory permissions; nothing to assert")
	}
	if left := listSQLFiles(t, outDir, "mydb.orders"); len(left) != 0 {
		t.Errorf("failed binlog-only table left artifacts: %v", left)
	}
	if len(rep.Files) != 0 {
		t.Errorf("rep.Files must stay empty on failure, got %v", rep.Files)
	}
}

// TestMydumperWriter_discardAfterSuccessfulCloseIsNoOp pins the finalized
// flag: once Close has fully succeeded the table's output is complete, and a
// later Discard — e.g. a caller's deferred error path firing because some
// step AFTER the writer finalized failed — must remove nothing. Without this,
// "Discard only runs before a successful Close" is a property of statement
// ordering at the call sites, not of the writer.
func TestMydumperWriter_discardAfterSuccessfulCloseIsNoOp(t *testing.T) {
	dir := t.TempDir()
	w, err := NewMydumperWriter(dir, "mydb", "orders", []string{"id"}, 0)
	if err != nil {
		t.Fatalf("NewMydumperWriter: %v", err)
	}
	if err := w.WriteSchema("-- schema"); err != nil {
		t.Fatalf("WriteSchema: %v", err)
	}
	if err := w.WriteRow([]any{int64(1)}); err != nil {
		t.Fatalf("WriteRow: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	wantFiles := append([]string(nil), w.Files()...)
	if len(wantFiles) != 2 {
		t.Fatalf("fixture expected schema + 1 chunk, got %v", wantFiles)
	}

	if err := w.Discard(); err != nil {
		t.Fatalf("Discard after successful Close: %v", err)
	}
	for _, name := range wantFiles {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Errorf("Discard after successful Close removed %s: %v", name, err)
		}
	}
	if got := w.Files(); len(got) != len(wantFiles) {
		t.Errorf("Files() after no-op Discard = %v, want %v", got, wantFiles)
	}
}
