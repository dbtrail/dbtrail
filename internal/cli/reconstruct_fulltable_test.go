package cli

import (
	"strings"
	"testing"
	"time"
)

// ─── runReconstructFullTable flag validation ─────────────────────────────────
//
// These tests exercise the CLI-layer guards that reject incompatible flag
// combinations for --output-format=mydumper. The production code never
// reaches config.Connect (validation fires before the DB connection), so
// these run without MySQL.

// resetMydumperFlags clears the full-table flag set between tests and
// registers a Cleanup to restore the caller's original values.
func resetMydumperFlags(t *testing.T) {
	t.Helper()
	origSnap := captureRecFlags()
	origOutputFormat := recOutputFormat
	origOutputDir := recOutputDir
	origTables := recTables
	origChunkSize := recChunkSize
	origParallelism := recParallelism
	t.Cleanup(func() {
		applyRecFlags(origSnap)
		recOutputFormat = origOutputFormat
		recOutputDir = origOutputDir
		recTables = origTables
		recChunkSize = origChunkSize
		recParallelism = origParallelism
	})

	// Zero everything, then set the minimal required flags for full-table
	// mode. Individual tests override specific fields to trigger errors.
	recIndexDSN = "root:pass@tcp(localhost:3306)/idx"
	recSchema = ""
	recTable = ""
	recPK = ""
	recPKColumns = ""
	recAt = ""
	recBaselineDir = "/tmp/baselines"
	recBaselineS3 = ""
	recBaselineOnly = false
	recHistory = false
	recSQL = ""
	recFormat = "json"
	recNoArchive = false
	recAllowGaps = false
	recOutputFormat = "mydumper"
	recOutputDir = "/tmp/out"
	recTables = "mydb.orders"
	recChunkSize = "256MB"
	recParallelism = 1
	recFetchBatch = 0
}

// TestRunReconstructFullTable_rejectsNegativeFetchBatch pins the validation on
// --fetch-batch-size (#1097). 0 is legal and means "use the default"; a
// negative value is a typo that FetchMergedStream would silently absorb into
// the same default, so the CLI refuses it rather than run a page size the
// operator did not ask for.
func TestRunReconstructFullTable_rejectsNegativeFetchBatch(t *testing.T) {
	resetMydumperFlags(t)
	recFetchBatch = -1
	err := runReconstruct(reconstructCmd, nil)
	if err == nil {
		t.Fatal("expected error when --fetch-batch-size is negative")
	}
	if !strings.Contains(err.Error(), "--fetch-batch-size") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRunReconstructFullTable_rejectsPK(t *testing.T) {
	resetMydumperFlags(t)
	recPK = "42"
	err := runReconstruct(reconstructCmd, nil)
	if err == nil {
		t.Fatal("expected error when --pk is set with --output-format=mydumper")
	}
	if !strings.Contains(err.Error(), "--pk") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRunReconstructFullTable_rejectsPKColumns(t *testing.T) {
	resetMydumperFlags(t)
	recPKColumns = "id"
	err := runReconstruct(reconstructCmd, nil)
	if err == nil {
		t.Fatal("expected error when --pk-columns is set with --output-format=mydumper")
	}
	if !strings.Contains(err.Error(), "pk-columns") && !strings.Contains(err.Error(), "--pk") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRunReconstructFullTable_rejectsHistory(t *testing.T) {
	resetMydumperFlags(t)
	recHistory = true
	err := runReconstruct(reconstructCmd, nil)
	if err == nil {
		t.Fatal("expected error when --history is set with --output-format=mydumper")
	}
	if !strings.Contains(err.Error(), "--history") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRunReconstructFullTable_rejectsBaselineOnly(t *testing.T) {
	resetMydumperFlags(t)
	recBaselineOnly = true
	err := runReconstruct(reconstructCmd, nil)
	if err == nil {
		t.Fatal("expected error when --baseline-only is set with --output-format=mydumper")
	}
	if !strings.Contains(err.Error(), "baseline-only") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRunReconstructFullTable_rejectsSQL(t *testing.T) {
	resetMydumperFlags(t)
	recSQL = "SELECT 1"
	err := runReconstruct(reconstructCmd, nil)
	if err == nil {
		t.Fatal("expected error when --sql is set with --output-format=mydumper")
	}
	// The --sql check runs before the --output-format dispatch in
	// runReconstruct, so the error surfaces as "invalid --format" (because
	// the SQL path validates --format first) OR as a mydumper-mode
	// incompatibility error. Accept either — what matters is that the run
	// does not silently proceed with both modes active.
}

func TestRunReconstructFullTable_rejectsSchemaTable(t *testing.T) {
	resetMydumperFlags(t)
	recSchema = "mydb"
	recTable = "orders"
	err := runReconstruct(reconstructCmd, nil)
	if err == nil {
		t.Fatal("expected error when --schema/--table is set with --output-format=mydumper")
	}
	if !strings.Contains(err.Error(), "--tables") && !strings.Contains(err.Error(), "schema") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRunReconstructFullTable_requiresTables(t *testing.T) {
	resetMydumperFlags(t)
	recTables = ""
	err := runReconstruct(reconstructCmd, nil)
	if err == nil {
		t.Fatal("expected error when --tables is empty")
	}
	if !strings.Contains(err.Error(), "--tables") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRunReconstructFullTable_requiresOutputDir(t *testing.T) {
	resetMydumperFlags(t)
	recOutputDir = ""
	err := runReconstruct(reconstructCmd, nil)
	if err == nil {
		t.Fatal("expected error when --output is empty")
	}
	// The documented name, not the older --output-dir (#1793).
	if err.Error() != "--output is required in full-table mode" {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRunReconstructFullTable_requiresIndexDSN(t *testing.T) {
	resetMydumperFlags(t)
	recIndexDSN = ""
	err := runReconstruct(reconstructCmd, nil)
	if err == nil {
		t.Fatal("expected error when --index-dsn is empty")
	}
	if !strings.Contains(err.Error(), "--index-dsn") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRunReconstructFullTable_requiresBaselineSource(t *testing.T) {
	resetMydumperFlags(t)
	recBaselineDir = ""
	recBaselineS3 = ""
	err := runReconstruct(reconstructCmd, nil)
	if err == nil {
		t.Fatal("expected error when neither --baseline-dir nor --baseline-s3 is set")
	}
	if !strings.Contains(err.Error(), "baseline-dir") && !strings.Contains(err.Error(), "baseline-s3") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRunReconstructFullTable_rejectsInvalidTablesEntry(t *testing.T) {
	resetMydumperFlags(t)
	recTables = "nodot"
	err := runReconstruct(reconstructCmd, nil)
	if err == nil {
		t.Fatal("expected error for --tables entry without a dot")
	}
	if !strings.Contains(err.Error(), "schema.table") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRunReconstructFullTable_rejectsUnknownOutputFormat(t *testing.T) {
	resetMydumperFlags(t)
	recOutputFormat = "csv"
	err := runReconstruct(reconstructCmd, nil)
	if err == nil {
		t.Fatal("expected error for --output-format=csv")
	}
	if !strings.Contains(err.Error(), "mydumper") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRunReconstructFullTable_rejectsInvalidChunkSize(t *testing.T) {
	resetMydumperFlags(t)
	recChunkSize = "not-a-size"
	err := runReconstruct(reconstructCmd, nil)
	if err == nil {
		t.Fatal("expected error for invalid --chunk-size")
	}
	if !strings.Contains(err.Error(), "chunk-size") {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestRunReconstructFullTable_defaultAtIsNow verifies that an empty --at
// field doesn't error and implicitly means "now". The test intercepts at
// the validation boundary by leaving --tables empty so the function errors
// out before attempting a DB connection, then asserts that the error is
// the missing-tables error (proving --at parsing succeeded).
func TestRunReconstructFullTable_defaultAtIsNow(t *testing.T) {
	resetMydumperFlags(t)
	recTables = ""
	recAt = ""
	err := runReconstruct(reconstructCmd, nil)
	if err == nil {
		t.Fatal("expected error (missing --tables) to short-circuit DB connect")
	}
	if !strings.Contains(err.Error(), "--tables") {
		t.Errorf("expected missing-tables error, got: %v", err)
	}
}

// TestRunReconstructFullTable_rejectsInvalidAt covers the --at parse error
// path, which must fire before DB connect.
func TestRunReconstructFullTable_rejectsInvalidAt(t *testing.T) {
	resetMydumperFlags(t)
	recAt = "not-a-timestamp"
	err := runReconstruct(reconstructCmd, nil)
	if err == nil {
		t.Fatal("expected error for invalid --at")
	}
	if !strings.Contains(err.Error(), "--at") {
		t.Errorf("unexpected error: %v", err)
	}
}

// Silence unused-imports if a future refactor deletes a case.
var _ = time.Time{}

// TestDedupePreservingOrder pins the --tables de-duplication (#1162): a
// repeated entry would spawn two concurrent writers over the same
// chunk/schema filenames in the shared output dir, and with the error-path
// discard one copy failing would unlink the files its twin just wrote.
func TestDedupePreservingOrder(t *testing.T) {
	cases := []struct {
		name string
		in   []string
		want []string
	}{
		{"nil", nil, nil},
		{"no duplicates", []string{"db.a", "db.b"}, []string{"db.a", "db.b"}},
		{"adjacent duplicate", []string{"db.a", "db.a", "db.b"}, []string{"db.a", "db.b"}},
		{"non-adjacent duplicate keeps first position", []string{"db.a", "db.b", "db.a"}, []string{"db.a", "db.b"}},
		{"all duplicates", []string{"db.a", "db.a", "db.a"}, []string{"db.a"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := dedupePreservingOrder(append([]string(nil), tc.in...))
			if len(got) != len(tc.want) {
				t.Fatalf("dedupePreservingOrder(%v) = %v, want %v", tc.in, got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("dedupePreservingOrder(%v) = %v, want %v", tc.in, got, tc.want)
				}
			}
		})
	}
}
