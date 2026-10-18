package reconstruct

import (
	"context"
	"errors"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// writeGateBaseline writes a minimal (id,status) baseline Parquet into the
// Hive layout FindBaseline expects (<dir>/<ts>/<schema>/<table>.parquet),
// stamping the supplied Parquet file metadata. Returns the baseline root dir.
func writeGateBaseline(t *testing.T, schema, table string, meta map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "2026-01-01T00-00-00Z", schema, table+".parquet")
	cols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "status", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	}
	w, err := baseline.NewWriter(path, cols, baseline.WriterConfig{
		Compression:  "none",
		RowGroupSize: 10,
		Metadata:     meta,
	})
	if err != nil {
		t.Fatalf("baseline.NewWriter: %v", err)
	}
	if err := w.WriteRow([]string{"1", "a"}, []bool{false, false}); err != nil {
		t.Fatalf("WriteRow: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("writer close: %v", err)
	}
	return dir
}

// TestReconstructTable_pgBaselineRemediation is the #830 guard: a full-table
// reconstruct over a PostgreSQL baseline (LSN-stamped, no CreateTableSQL) must
// fail with the "#1596 not yet supported for PostgreSQL" message, NOT the
// MySQL-only "re-run `bintrail baseline`" remediation (which no PG baseline can
// ever satisfy — pgbaseline deliberately omits CreateTableSQL). The LSN!=0
// branch short-circuits before any DB read, so a nil DB is enough here.
func TestReconstructTable_pgBaselineRemediation(t *testing.T) {
	dir := writeGateBaseline(t, "shop", "orders", map[string]string{
		baseline.MetaKeyLSN: strconv.FormatUint(42, 10),
	})
	cfg := FullTableConfig{BaselineSrc: dir, At: time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)}

	_, err := ReconstructTable(context.Background(), cfg, "shop", "orders", nil, nil, nil, nil, "")
	if err == nil {
		t.Fatal("expected the PG-not-supported error, got nil")
	}
	if !errors.Is(err, ErrPGFullTableMydumper) {
		t.Fatalf("PG baseline got the wrong error: %v", err)
	}
	if strings.Contains(err.Error(), "re-run `bintrail baseline`") {
		t.Fatalf("PG baseline must NOT prescribe re-running bintrail baseline: %v", err)
	}
	// To Parquet, over an index that records no PostgreSQL stream (no run
	// end was read): no LSN order to fold in.
	cfg.OutputFormat = OutputFormatParquet
	if _, err := ReconstructTable(context.Background(), cfg, "shop", "orders", nil, nil, nil, nil, ""); err == nil ||
		!strings.Contains(err.Error(), "records no PostgreSQL source") {
		t.Fatalf("PG baseline to Parquet with no PostgreSQL index: %v", err)
	}
}

// TestReconstructTable_mysqlMissingCreateTableSQLUnchanged pins the no-regress
// half of #830: a genuine MySQL baseline missing CreateTableSQL (LSN==0, no PG
// flavor — nil DB resolves SourceFlavor to "") must still get the original
// "re-run `bintrail baseline`" guidance, never the PostgreSQL message.
func TestReconstructTable_mysqlMissingCreateTableSQLUnchanged(t *testing.T) {
	dir := writeGateBaseline(t, "shop", "orders", map[string]string{})
	cfg := FullTableConfig{BaselineSrc: dir, At: time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)}

	_, err := ReconstructTable(context.Background(), cfg, "shop", "orders", nil, nil, nil, nil, "")
	if err == nil {
		t.Fatal("expected the missing-CreateTableSQL error, got nil")
	}
	if !strings.Contains(err.Error(), "re-run `bintrail baseline`") {
		t.Fatalf("MySQL missing-CreateTableSQL must keep its guidance: %v", err)
	}
	if strings.Contains(err.Error(), "PostgreSQL") {
		t.Fatalf("MySQL baseline must not get the PG message: %v", err)
	}
}

// TestReconstructTable_pgNoBaselineRefused is the #916 secondary guard: a
// full-table reconstruct of a PG-flavored source (stream_state.flavor='postgres')
// for a table with NO baseline must refuse (#1596), not fall through to a
// binlog-only report mislabeled as full-table. With no baseline there is no LSN
// anchor, so detection is by the recorded source flavor alone.
func TestReconstructTable_pgNoBaselineRefused(t *testing.T) {
	dir := t.TempDir() // empty baseline source → FindBaseline returns ErrNoBaseline

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectQuery("SELECT flavor FROM stream_state").
		WillReturnRows(sqlmock.NewRows([]string{"flavor"}).AddRow("postgres"))

	cfg := FullTableConfig{BaselineSrc: dir, At: time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)}
	_, err = ReconstructTable(context.Background(), cfg, "shop", "orders", db, nil, nil, nil, "")
	if err == nil {
		t.Fatal("expected the PG no-baseline refusal, got nil")
	}
	if !errors.Is(err, ErrPGFullTableMydumper) {
		t.Fatalf("PG no-baseline reconstruct got the wrong error: %v", err)
	}
	if merr := mock.ExpectationsWereMet(); merr != nil {
		t.Errorf("unmet sqlmock expectations: %v", merr)
	}
}
