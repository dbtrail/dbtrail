package reconstruct

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/query"
)

// writeTestBaseline creates a small Parquet baseline on disk with two
// columns (id INT, status VARCHAR) and the caller-provided rows. Returns
// the local path.
func writeTestBaseline(t *testing.T, rows [][]string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "baseline.parquet")
	cols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "status", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	}
	w, err := baseline.NewWriter(path, cols, baseline.WriterConfig{
		Compression:  "none",
		RowGroupSize: 10,
	})
	if err != nil {
		t.Fatalf("baseline.NewWriter: %v", err)
	}
	for _, r := range rows {
		if err := w.WriteRow(r, []bool{false, false}); err != nil {
			t.Fatalf("WriteRow: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("writer close: %v", err)
	}
	return path
}

// pkColsIntID returns a minimal PK column descriptor for the test table
// (primary key is the single `id` INT column at ordinal position 1).
func pkColsIntID() []metadata.ColumnMeta {
	return []metadata.ColumnMeta{
		{Name: "id", OrdinalPosition: 1, IsPK: true, DataType: "int"},
	}
}

// pkStrForInt returns the canonical pk_values encoding for an integer id,
// matching what the binlog parser would produce at index time.
func pkStrForInt(id int) string {
	return strings.TrimSpace(
		// parser.BuildPKValues does fmt.Sprintf("%v", ...) so int → "42".
		// Construct the same way for test determinism.
		parser.BuildPKValues(pkColsIntID(), map[string]any{"id": id}),
	)
}

// TestMergeBaseline_passthroughOnly verifies the "no events" case: every
// baseline row flows through unchanged.
func TestMergeBaseline_passthroughOnly(t *testing.T) {
	baselinePath := writeTestBaseline(t, [][]string{
		{"1", "new"},
		{"2", "paid"},
		{"3", "shipped"},
	})
	outDir := t.TempDir()

	rep := &TableReport{Schema: "mydb", Table: "orders"}
	err := mergeBaselineIntoWriter(context.Background(), mergeInput{
		LocalBaselinePath: baselinePath,
		CreateTableSQL:    "-- test",
		Schema:            "mydb",
		Table:             "orders",
		PKCols:            pkColsIntID(),
		Changes:           map[string]*query.ResultRow{},
		OutputDir:         outDir,
		ChunkSize:         0,
	}, rep)
	if err != nil {
		t.Fatalf("mergeBaselineIntoWriter: %v", err)
	}

	if rep.BaselineRows != 3 {
		t.Errorf("BaselineRows = %d, want 3", rep.BaselineRows)
	}
	if rep.UpdatesApplied != 0 || rep.InsertsEmitted != 0 || rep.DeletesSkipped != 0 {
		t.Errorf("unexpected event counters: %+v", rep)
	}

	chunk := mustReadOnlyChunk(t, outDir)
	for _, want := range []string{"(1, 'new')", "(2, 'paid')", "(3, 'shipped')"} {
		if !strings.Contains(chunk, want) {
			t.Errorf("chunk missing %q:\n%s", want, chunk)
		}
	}
}

// TestFoldPage_unresolvedToastMarker is the #592 guard on the full-table
// mydumper path: an event carrying the residual unchanged-TOAST marker must
// refuse the whole table BEFORE any output exists — writing the marker's JSON
// into a reconstructed dump is silent corruption.
//
// The check runs per event in foldPage since the window became paged (#1097),
// upstream of both merge entry points, which is why it is asserted there and
// not against a finished change map: retainEvent blanks the before-image, so a
// map-level TOAST check would have nothing left to inspect. The shim's
// full-table _snapshot entry point keeps its own map-level call and its own
// test (TestSnapshotFullTableImages_unresolvedToastMarker below) — its map is
// built by a different caller and still carries before-images.
func TestFoldPage_unresolvedToastMarker(t *testing.T) {
	res := &foldResult{Changes: map[string]*query.ResultRow{}}
	err := foldPage([]query.ResultRow{{
		EventType:  event.EventUpdate,
		SchemaName: "mydb", TableName: "orders", PKValues: pkStrForInt(2),
		RowAfter: map[string]any{"id": "2", "status": toastMarker()},
	}}, "mydb", "orders", pkColsIntID(), res)
	if err == nil {
		t.Fatal("expected a loud error for a marker-carrying change")
	}
	for _, want := range []string{"unresolved unchanged-TOAST marker", "capture invariant violated", "mydb.orders", "status"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error missing %q:\n%s", want, err)
		}
	}
	if len(res.Changes) != 0 {
		t.Errorf("a refused event must not be folded into the change map, got %d entries", len(res.Changes))
	}
}

func TestSnapshotFullTableImages_unresolvedToastMarker(t *testing.T) {
	emitted := 0
	err := SnapshotFullTableImages(context.Background(), SnapshotFullTableInput{
		BaselinePath: "/nonexistent/never-touched/baseline.parquet",
		Schema:       "mydb",
		Table:        "orders",
		PKCols:       pkColsIntID(),
		Changes: map[string]*query.ResultRow{
			pkStrForInt(1): {
				EventType:  event.EventUpdate,
				SchemaName: "mydb", TableName: "orders", PKValues: pkStrForInt(1),
				RowAfter: map[string]any{"id": "1", "status": toastMarker()},
			},
		},
	}, func(map[string]any) error {
		emitted++
		return nil
	})
	if err == nil {
		t.Fatal("expected a loud error for a marker-carrying change")
	}
	for _, want := range []string{"unresolved unchanged-TOAST marker", "capture invariant violated", "mydb.orders", "status"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error missing %q:\n%s", want, err)
		}
	}
	if strings.Contains(err.Error(), "materialize baseline") {
		t.Errorf("refusal must fire BEFORE baseline materialization, got: %v", err)
	}
	if emitted != 0 {
		t.Errorf("refusal must emit no rows, emitted %d", emitted)
	}
}

// writeBlobTextBaseline creates a baseline (id INT, tx TEXT, bl BLOB). TEXT maps
// to parquet.String() (DuckDB scans it back as a Go string), BLOB to
// ByteArrayType (DuckDB []byte) — the type asymmetry behind #660.
func writeBlobTextBaseline(t *testing.T, rows [][]string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "baseline.parquet")
	cols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "tx", MySQLType: "text", ParquetType: baseline.MysqlToParquetNode("text")},
		{Name: "bl", MySQLType: "blob", ParquetType: baseline.MysqlToParquetNode("blob")},
	}
	w, err := baseline.NewWriter(path, cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 10})
	if err != nil {
		t.Fatalf("baseline.NewWriter: %v", err)
	}
	for _, r := range rows {
		if err := w.WriteRow(r, []bool{false, false, false}); err != nil {
			t.Fatalf("WriteRow: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("writer close: %v", err)
	}
	return path
}

// TestMergeBaseline_blobTextProvenance is the #660/#668 anchor across both
// value branches: an UNTOUCHED baseline TEXT whose content is valid base64
// must survive VERBATIM (the corruption case, since DuckDB delivers it as a Go
// string), and an already-decoded delta-event TEXT/BLOB (decode now happens
// upstream, epoch-aware, via DecodeEventBinaries before mergeBaselineIntoWriter
// ever sees the Changes map — #668) must reach the writer untouched, not
// decoded a second time. Re-introducing a decode in rowAfterOrdered makes this
// fail directly; the delta-event "tx" fixture is itself valid base64 (like the
// baseline fixture above it) so a reintroduced decode anywhere on the Changes
// path — e.g. inside mergeBaselineIntoWriter — corrupts it and trips the
// assertion below too, instead of silently no-op'ing on an already-decoded
// value that doesn't happen to look like base64.
func TestMergeBaseline_blobTextProvenance(t *testing.T) {
	baselinePath := writeBlobTextBaseline(t, [][]string{
		{"1", "YWJjZA==", "RAW"}, // untouched; tx is valid base64 (of "abcd"); bl bytes "RAW"
	})
	outDir := t.TempDir()

	changes := map[string]*query.ResultRow{
		pkStrForInt(2): {
			EventType: event.EventInsert,
			PKValues:  pkStrForInt(2),
			// Already decoded, as DecodeEventBinaries would leave it. "test" is
			// itself valid base64 (decodes to garbage bytes), on purpose: it's
			// what catches a reintroduced double-decode on this path.
			RowAfter: map[string]any{"id": float64(2), "tx": "test", "bl": []byte("BIN")},
		},
	}

	rep := &TableReport{Schema: "mydb", Table: "t"}
	if err := mergeBaselineIntoWriter(context.Background(), mergeInput{
		LocalBaselinePath: baselinePath,
		CreateTableSQL:    "-- test",
		Schema:            "mydb",
		Table:             "t",
		PKCols:            pkColsIntID(),
		Changes:           changes,
		OutputDir:         outDir,
		ChunkSize:         0,
	}, rep); err != nil {
		t.Fatalf("mergeBaselineIntoWriter: %v", err)
	}

	chunk := mustReadOnlyChunk(t, outDir)
	// Columns emit in the baseline's order (bl, id, tx). Baseline pass-through:
	// TEXT verbatim (not 'abcd'), BLOB as the hex of its bytes.
	if !strings.Contains(chunk, "(X'524157', 1, 'YWJjZA==')") {
		t.Errorf("baseline row must emit hex BLOB + verbatim TEXT, got:\n%s", chunk)
	}
	if strings.Contains(chunk, "'abcd'") {
		t.Errorf("baseline TEXT was wrongly base64-decoded (corruption, #660):\n%s", chunk)
	}
	// Delta event: already-decoded values must reach the writer unchanged. "test"
	// is valid base64, so a reintroduced decode here would corrupt it — this is
	// what makes the assertion an actual double-decode regression guard.
	if !strings.Contains(chunk, "(X'42494e', 2, 'test')") {
		t.Errorf("delta-event BLOB/TEXT must reach the writer already decoded, got:\n%s", chunk)
	}
}

// TestMergeBaseline_updateSubstitution verifies that an UPDATE event on a
// baseline row produces the row_after values in the output and bumps the
// UpdatesApplied counter, not BaselineRows.
func TestMergeBaseline_updateSubstitution(t *testing.T) {
	baselinePath := writeTestBaseline(t, [][]string{
		{"1", "new"},
		{"2", "paid"},
	})
	outDir := t.TempDir()

	changes := map[string]*query.ResultRow{
		pkStrForInt(2): {
			EventType: parser.EventUpdate,
			PKValues:  pkStrForInt(2),
			RowBefore: map[string]any{"id": float64(2), "status": "paid"},
			RowAfter:  map[string]any{"id": float64(2), "status": "shipped"},
		},
	}

	rep := &TableReport{}
	err := mergeBaselineIntoWriter(context.Background(), mergeInput{
		LocalBaselinePath: baselinePath,
		CreateTableSQL:    "-- test",
		Schema:            "mydb",
		Table:             "orders",
		PKCols:            pkColsIntID(),
		Changes:           changes,
		OutputDir:         outDir,
		ChunkSize:         0,
	}, rep)
	if err != nil {
		t.Fatalf("mergeBaselineIntoWriter: %v", err)
	}

	if rep.BaselineRows != 1 {
		t.Errorf("BaselineRows = %d, want 1 (id=1 passes through)", rep.BaselineRows)
	}
	if rep.UpdatesApplied != 1 {
		t.Errorf("UpdatesApplied = %d, want 1", rep.UpdatesApplied)
	}

	chunk := mustReadOnlyChunk(t, outDir)
	if !strings.Contains(chunk, "(1, 'new')") {
		t.Errorf("chunk missing passthrough row:\n%s", chunk)
	}
	if !strings.Contains(chunk, "(2, 'shipped')") {
		t.Errorf("chunk missing updated row:\n%s", chunk)
	}
	if strings.Contains(chunk, "(2, 'paid')") {
		t.Errorf("chunk still contains the pre-update baseline value:\n%s", chunk)
	}
}

// TestMergeBaseline_deleteSkipsRow verifies that a DELETE event removes the
// matching baseline row from the output (it is skipped, not substituted)
// and bumps DeletesSkipped.
func TestMergeBaseline_deleteSkipsRow(t *testing.T) {
	baselinePath := writeTestBaseline(t, [][]string{
		{"1", "new"},
		{"2", "paid"},
		{"3", "shipped"},
	})
	outDir := t.TempDir()

	changes := map[string]*query.ResultRow{
		pkStrForInt(2): {
			EventType: parser.EventDelete,
			PKValues:  pkStrForInt(2),
			RowBefore: map[string]any{"id": float64(2), "status": "paid"},
		},
	}

	rep := &TableReport{}
	err := mergeBaselineIntoWriter(context.Background(), mergeInput{
		LocalBaselinePath: baselinePath,
		CreateTableSQL:    "-- test",
		Schema:            "mydb",
		Table:             "orders",
		PKCols:            pkColsIntID(),
		Changes:           changes,
		OutputDir:         outDir,
		ChunkSize:         0,
	}, rep)
	if err != nil {
		t.Fatalf("mergeBaselineIntoWriter: %v", err)
	}

	if rep.DeletesSkipped != 1 {
		t.Errorf("DeletesSkipped = %d, want 1", rep.DeletesSkipped)
	}
	if rep.BaselineRows != 2 {
		t.Errorf("BaselineRows = %d, want 2", rep.BaselineRows)
	}

	chunk := mustReadOnlyChunk(t, outDir)
	if strings.Contains(chunk, "(2,") {
		t.Errorf("chunk still contains deleted row (id=2):\n%s", chunk)
	}
	if !strings.Contains(chunk, "(1, 'new')") || !strings.Contains(chunk, "(3, 'shipped')") {
		t.Errorf("chunk missing surviving rows:\n%s", chunk)
	}
}

// TestMergeBaseline_insertAppendsAfterBaseline verifies that an INSERT event
// for a PK that is NOT in the baseline appends a new row after the baseline
// pass, and bumps InsertsEmitted.
func TestMergeBaseline_insertAppendsAfterBaseline(t *testing.T) {
	baselinePath := writeTestBaseline(t, [][]string{
		{"1", "new"},
	})
	outDir := t.TempDir()

	changes := map[string]*query.ResultRow{
		pkStrForInt(42): {
			EventType: parser.EventInsert,
			PKValues:  pkStrForInt(42),
			RowAfter:  map[string]any{"id": float64(42), "status": "just-inserted"},
		},
		pkStrForInt(43): {
			EventType: parser.EventInsert,
			PKValues:  pkStrForInt(43),
			RowAfter:  map[string]any{"id": float64(43), "status": "also-inserted"},
		},
	}

	rep := &TableReport{}
	err := mergeBaselineIntoWriter(context.Background(), mergeInput{
		LocalBaselinePath: baselinePath,
		CreateTableSQL:    "-- test",
		Schema:            "mydb",
		Table:             "orders",
		PKCols:            pkColsIntID(),
		Changes:           changes,
		OutputDir:         outDir,
		ChunkSize:         0,
	}, rep)
	if err != nil {
		t.Fatalf("mergeBaselineIntoWriter: %v", err)
	}

	if rep.BaselineRows != 1 {
		t.Errorf("BaselineRows = %d, want 1", rep.BaselineRows)
	}
	if rep.InsertsEmitted != 2 {
		t.Errorf("InsertsEmitted = %d, want 2", rep.InsertsEmitted)
	}

	chunk := mustReadOnlyChunk(t, outDir)
	for _, want := range []string{"(1, 'new')", "(42, 'just-inserted')", "(43, 'also-inserted')"} {
		if !strings.Contains(chunk, want) {
			t.Errorf("chunk missing %q:\n%s", want, chunk)
		}
	}

	// New inserts must be appended AFTER the baseline rows, not before.
	baselineIdx := strings.Index(chunk, "(1, 'new')")
	insert1Idx := strings.Index(chunk, "(42, ")
	insert2Idx := strings.Index(chunk, "(43, ")
	if baselineIdx < 0 || insert1Idx < 0 || insert2Idx < 0 {
		t.Fatalf("missing expected rows in chunk:\n%s", chunk)
	}
	if baselineIdx > insert1Idx || baselineIdx > insert2Idx {
		t.Errorf("baseline row should appear BEFORE inserted rows; got positions %d / %d / %d",
			baselineIdx, insert1Idx, insert2Idx)
	}
	// Deterministic ordering between new inserts (sorted by PK).
	if insert1Idx > insert2Idx {
		t.Errorf("new inserts should be sorted by PK; got id=42 at %d, id=43 at %d", insert1Idx, insert2Idx)
	}
}

// TestMergeBaseline_mixedAllEventTypes combines INSERT, UPDATE and DELETE
// in one run to verify the counters and output are correct together.
func TestMergeBaseline_mixedAllEventTypes(t *testing.T) {
	baselinePath := writeTestBaseline(t, [][]string{
		{"1", "baseline-1"},
		{"2", "baseline-2"},
		{"3", "baseline-3"},
	})
	outDir := t.TempDir()

	changes := map[string]*query.ResultRow{
		pkStrForInt(2): {
			EventType: parser.EventUpdate,
			PKValues:  pkStrForInt(2),
			RowAfter:  map[string]any{"id": float64(2), "status": "updated-2"},
		},
		pkStrForInt(3): {
			EventType: parser.EventDelete,
			PKValues:  pkStrForInt(3),
		},
		pkStrForInt(10): {
			EventType: parser.EventInsert,
			PKValues:  pkStrForInt(10),
			RowAfter:  map[string]any{"id": float64(10), "status": "inserted-10"},
		},
	}

	rep := &TableReport{}
	err := mergeBaselineIntoWriter(context.Background(), mergeInput{
		LocalBaselinePath: baselinePath,
		CreateTableSQL:    "-- test",
		Schema:            "mydb",
		Table:             "orders",
		PKCols:            pkColsIntID(),
		Changes:           changes,
		OutputDir:         outDir,
		ChunkSize:         0,
	}, rep)
	if err != nil {
		t.Fatalf("mergeBaselineIntoWriter: %v", err)
	}

	if rep.BaselineRows != 1 {
		t.Errorf("BaselineRows = %d, want 1 (only id=1 is a passthrough)", rep.BaselineRows)
	}
	if rep.UpdatesApplied != 1 {
		t.Errorf("UpdatesApplied = %d, want 1", rep.UpdatesApplied)
	}
	if rep.DeletesSkipped != 1 {
		t.Errorf("DeletesSkipped = %d, want 1", rep.DeletesSkipped)
	}
	if rep.InsertsEmitted != 1 {
		t.Errorf("InsertsEmitted = %d, want 1", rep.InsertsEmitted)
	}

	chunk := mustReadOnlyChunk(t, outDir)
	// Expected final rows: 1 (passthrough), 2 (updated), 10 (new insert).
	// Row 3 was deleted and must not appear.
	if !strings.Contains(chunk, "(1, 'baseline-1')") {
		t.Errorf("missing passthrough row:\n%s", chunk)
	}
	if !strings.Contains(chunk, "(2, 'updated-2')") {
		t.Errorf("missing updated row:\n%s", chunk)
	}
	if strings.Contains(chunk, "(3, ") {
		t.Errorf("deleted row still present:\n%s", chunk)
	}
	if !strings.Contains(chunk, "(10, 'inserted-10')") {
		t.Errorf("missing new insert:\n%s", chunk)
	}
}

// TestSplitSchemaTable covers the pure helper for parsing --tables entries.
func TestSplitSchemaTable(t *testing.T) {
	cases := []struct {
		in     string
		ok     bool
		schema string
		table  string
	}{
		{"mydb.orders", true, "mydb", "orders"},
		{"schema_with_underscore.table_name", true, "schema_with_underscore", "table_name"},
		{"", false, "", ""},
		{"nodot", false, "", ""},
		{".notable", false, "", ""},
		{"noschema.", false, "", ""},
		{"too.many.dots", false, "", ""},
	}
	for _, c := range cases {
		s, tbl, ok := splitSchemaTable(c.in)
		if ok != c.ok {
			t.Errorf("splitSchemaTable(%q) ok = %v, want %v", c.in, ok, c.ok)
			continue
		}
		if ok && (s != c.schema || tbl != c.table) {
			t.Errorf("splitSchemaTable(%q) = (%q, %q), want (%q, %q)",
				c.in, s, tbl, c.schema, c.table)
		}
	}
}

// TestWriteBinlogOnlyChanges_insertsSkipsDeletes covers the #766 ErrNoBaseline
// fallback core: with no baseline at all, every surviving (non-DELETE)
// change is emitted as a row, in deterministic PK order, and the schema file
// carries an explanatory placeholder instead of a fabricated CREATE TABLE.
func TestWriteBinlogOnlyChanges_insertsSkipsDeletes(t *testing.T) {
	outDir := t.TempDir()
	colNames := []string{"id", "status"}

	changes := map[string]*query.ResultRow{
		pkStrForInt(1): {
			EventType: parser.EventInsert,
			PKValues:  pkStrForInt(1),
			RowAfter:  map[string]any{"id": float64(1), "status": "binlog-only-1"},
		},
		pkStrForInt(2): {
			EventType: parser.EventDelete,
			PKValues:  pkStrForInt(2),
			RowBefore: map[string]any{"id": float64(2), "status": "deleted"},
		},
		pkStrForInt(3): {
			EventType: parser.EventUpdate,
			PKValues:  pkStrForInt(3),
			RowAfter:  map[string]any{"id": float64(3), "status": "binlog-only-3"},
		},
	}

	rep := &TableReport{Schema: "mydb", Table: "orders"}
	if err := writeBinlogOnlyChanges(outDir, "mydb", "orders", pkColsIntID(), colNames, 0, nil,
		binlogOnlySchemaPlaceholder("mydb", "orders"), changes, rep); err != nil {
		t.Fatalf("writeBinlogOnlyChanges: %v", err)
	}

	if rep.InsertsEmitted != 2 {
		t.Errorf("InsertsEmitted = %d, want 2", rep.InsertsEmitted)
	}
	if rep.DeletesSkipped != 1 {
		t.Errorf("DeletesSkipped = %d, want 1", rep.DeletesSkipped)
	}
	if rep.BaselineRows != 0 || rep.UpdatesApplied != 0 {
		t.Errorf("unexpected non-zero baseline/update counters: %+v", rep)
	}
	if len(rep.Files) == 0 {
		t.Fatal("rep.Files is empty; expected at least the schema + one data chunk")
	}

	chunk := mustReadOnlyChunk(t, outDir)
	if !strings.Contains(chunk, "(1, 'binlog-only-1')") || !strings.Contains(chunk, "(3, 'binlog-only-3')") {
		t.Errorf("chunk missing surviving rows:\n%s", chunk)
	}
	if strings.Contains(chunk, "(2,") {
		t.Errorf("chunk contains deleted row (id=2):\n%s", chunk)
	}

	schemaPath := filepath.Join(outDir, "mydb.orders-schema.sql")
	b, err := os.ReadFile(schemaPath)
	if err != nil {
		t.Fatalf("read schema file: %v", err)
	}
	if !strings.Contains(string(b), "no baseline snapshot exists") || !strings.Contains(string(b), "#766") {
		t.Errorf("schema file placeholder missing expected explanation:\n%s", b)
	}
}

// TestWriteBinlogOnlyChanges_nilRowAfterSkipped verifies a corrupted event
// (nil RowAfter on a non-DELETE) is dropped rather than emitting an all-NULL
// row, mirroring mergeBaselineImages' own defensive skip.
func TestWriteBinlogOnlyChanges_nilRowAfterSkipped(t *testing.T) {
	outDir := t.TempDir()
	colNames := []string{"id", "status"}

	changes := map[string]*query.ResultRow{
		pkStrForInt(1): {
			EventType: parser.EventInsert,
			PKValues:  pkStrForInt(1),
			RowAfter:  nil,
		},
	}

	rep := &TableReport{Schema: "mydb", Table: "orders"}
	if err := writeBinlogOnlyChanges(outDir, "mydb", "orders", pkColsIntID(), colNames, 0, nil,
		binlogOnlySchemaPlaceholder("mydb", "orders"), changes, rep); err != nil {
		t.Fatalf("writeBinlogOnlyChanges: %v", err)
	}
	if rep.InsertsEmitted != 0 {
		t.Errorf("InsertsEmitted = %d, want 0 (nil RowAfter must be skipped)", rep.InsertsEmitted)
	}
}

// TestFindCapturedCreateTableDDL_found verifies the happy path: a captured
// CREATE TABLE row in schema_changes is returned verbatim.
func TestFindCapturedCreateTableDDL_found(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	at := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	mock.ExpectQuery("SELECT ddl_query FROM schema_changes").
		WithArgs("mydb", "orders", string(event.DDLCreateTable), at).
		WillReturnRows(sqlmock.NewRows([]string{"ddl_query"}).
			AddRow("CREATE TABLE `orders` (`id` int NOT NULL, PRIMARY KEY (`id`))"))

	ddl, found, err := findCapturedCreateTableDDL(context.Background(), db, "mydb", "orders", at)
	if err != nil {
		t.Fatalf("findCapturedCreateTableDDL: %v", err)
	}
	if !found {
		t.Fatal("found = false, want true")
	}
	if !strings.Contains(ddl, "CREATE TABLE `orders`") {
		t.Errorf("ddl = %q, want the captured CREATE TABLE text", ddl)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestFindCapturedCreateTableDDL_notFound verifies that no matching row
// (sql.ErrNoRows) reports found=false with a nil error, so the caller falls
// back to the placeholder rather than treating "never captured" as a fault.
func TestFindCapturedCreateTableDDL_notFound(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SELECT ddl_query FROM schema_changes").
		WillReturnRows(sqlmock.NewRows([]string{"ddl_query"}))

	ddl, found, err := findCapturedCreateTableDDL(context.Background(), db, "mydb", "orders", time.Now())
	if err != nil {
		t.Fatalf("findCapturedCreateTableDDL: %v", err)
	}
	if found {
		t.Errorf("found = true, want false; ddl = %q", ddl)
	}
}

// TestFindCapturedCreateTableDDL_realErrorSurfaces verifies a genuine query
// failure (not just "no rows") is returned to the caller, not swallowed.
func TestFindCapturedCreateTableDDL_realErrorSurfaces(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	forcedErr := errors.New("connection reset")
	mock.ExpectQuery("SELECT ddl_query FROM schema_changes").WillReturnError(forcedErr)

	_, found, err := findCapturedCreateTableDDL(context.Background(), db, "mydb", "orders", time.Now())
	if err == nil {
		t.Fatal("expected the underlying query error to surface")
	}
	if found {
		t.Error("found = true on a real error, want false")
	}
}

// ─── helpers ─────────────────────────────────────────────────────────────────

// mustReadOnlyChunk reads the single .sql chunk file in dir (other than
// schema files) and returns its contents, failing the test if zero or
// multiple chunk files are present.
func mustReadOnlyChunk(t *testing.T, dir string) string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read output dir: %v", err)
	}
	var chunks []string
	for _, e := range entries {
		name := e.Name()
		if strings.HasSuffix(name, ".sql") && !strings.Contains(name, "-schema.sql") {
			chunks = append(chunks, name)
		}
	}
	if len(chunks) != 1 {
		t.Fatalf("expected exactly 1 chunk file in %s, got %d: %v", dir, len(chunks), chunks)
	}
	b, err := os.ReadFile(filepath.Join(dir, chunks[0]))
	if err != nil {
		t.Fatalf("read chunk: %v", err)
	}
	return string(b)
}

// TestBinlogOnlySchemaPlaceholder_commentInjection pins #1120 for the
// binlog-only schema file: its entire contents are a "--" comment block, and it
// is written for the operator to apply as-is, so a newline in either identifier
// would leave the rest of the name executing as SQL.
func TestBinlogOnlySchemaPlaceholder_commentInjection(t *testing.T) {
	out := binlogOnlySchemaPlaceholder("my\ndb", "or\nders")
	for _, line := range strings.Split(strings.TrimRight(out, "\n"), "\n") {
		if !strings.HasPrefix(line, "--") {
			t.Errorf("placeholder line is not a comment and would execute: %q\nfull text:\n%s", line, out)
		}
	}
}
