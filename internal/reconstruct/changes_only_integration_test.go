//go:build integration

package reconstruct_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestChangesOnly_refusedByDefaultWrittenAndMarkedOnRequest runs the engine
// against a real index. shop.t has no snapshot and two recorded inserts.
// Without AllowChangesOnly the run fails, the table has no file and the output
// directory is not marked complete. With it the table is written, reported as
// BinlogOnly, and its schema file opens with the marker.
func TestChangesOnly_refusedByDefaultWrittenAndMarkedOnRequest(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Hour)
	at := base.Add(50 * time.Minute)
	created := base.Add(5 * time.Minute)

	db, dbName := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(ctx, db, 48, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	ts := func(tm time.Time) string { return tm.Format("2006-01-02 15:04:05") }
	testutil.InsertSnapshot(t, db, 1, ts(base), "shop", "t", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, ts(base), "shop", "t", "v", 2, "", "varchar", "YES")
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, ts(base.Add(10*time.Minute)), nil,
		"shop", "t", 1, "1", nil, nil, []byte(`{"id":1,"v":"one"}`))
	testutil.InsertEvent(t, db, "binlog.000001", 300, 400, ts(base.Add(30*time.Minute)), nil,
		"shop", "t", 1, "2", nil, nil, []byte(`{"id":2,"v":"two"}`))

	run := func(t *testing.T, allow bool) (string, []*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		t.Helper()
		out := t.TempDir()
		reports, failures, err := reconstruct.ReconstructTablesDetailed(ctx, reconstruct.FullTableConfig{
			IndexDSN:         testutil.BaseDSN() + "/" + dbName,
			BaselineSrc:      t.TempDir(),
			Tables:           []string{"shop.t"},
			At:               at,
			OutputDir:        out,
			OutputFormat:     reconstruct.OutputFormatMydumper,
			AllowChangesOnly: allow,
		})
		return out, reports, failures, err
	}
	tableFiles := func(t *testing.T, dir string) []string {
		t.Helper()
		ents, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("ReadDir: %v", err)
		}
		var names []string
		for _, e := range ents {
			if strings.HasPrefix(e.Name(), "shop.t") {
				names = append(names, e.Name())
			}
		}
		return names
	}

	t.Run("refused by default", func(t *testing.T) {
		out, reports, failures, err := run(t, false)
		if err == nil || !errors.Is(err, reconstruct.ErrChangesOnly) {
			t.Fatalf("err = %v, want the run to fail with ErrChangesOnly", err)
		}
		t.Logf("rendered: %s", err)
		if len(reports) != 0 {
			t.Errorf("a refused table has a report: %+v", reports[0])
		}
		var refusal *reconstruct.ChangesOnlyError
		if len(failures) != 1 || !errors.As(failures[0].Err, &refusal) {
			t.Fatalf("failures = %+v, want the one refusal", failures)
		}
		if !refusal.CreateRecordedAt.IsZero() {
			t.Errorf("CreateRecordedAt = %s with no CREATE TABLE on record", refusal.CreateRecordedAt)
		}
		if files := tableFiles(t, out); len(files) != 0 {
			t.Errorf("refused table left files: %v", files)
		}
		if _, err := os.Stat(filepath.Join(out, baseline.SuccessMarker)); err == nil {
			t.Errorf("the output directory is marked %s", baseline.SuccessMarker)
		}
	})

	t.Run("written and marked on request, placeholder schema", func(t *testing.T) {
		out, reports, _, err := run(t, true)
		if err != nil {
			t.Fatalf("run: %v", err)
		}
		if len(reports) != 1 || !reports[0].BinlogOnly || reports[0].RowsWritten != 2 {
			t.Fatalf("reports = %+v, want one BinlogOnly report with both rows", reports)
		}
		schemaFile, err := os.ReadFile(filepath.Join(out, "shop.t-schema.sql"))
		if err != nil {
			t.Fatalf("read schema file: %v", err)
		}
		if !strings.HasPrefix(string(schemaFile), "-- bintrail: CHANGES ONLY.") {
			t.Errorf("schema file does not open with the marker:\n%s", schemaFile)
		}
	})

	// From here on the index holds the table's CREATE TABLE.
	if _, err := db.Exec(`INSERT INTO schema_changes
		(detected_at, binlog_file, binlog_pos, schema_name, table_name, ddl_type, ddl_query)
		VALUES (?, 'binlog.000001', 50, 'shop', 't', 'CREATE TABLE', 'CREATE TABLE t (id int NOT NULL, v varchar(16), PRIMARY KEY (id))')`, created); err != nil {
		t.Fatalf("seed schema_changes: %v", err)
	}

	t.Run("the refusal carries when the CREATE TABLE was recorded", func(t *testing.T) {
		_, _, failures, err := run(t, false)
		var refusal *reconstruct.ChangesOnlyError
		if err == nil || len(failures) != 1 || !errors.As(failures[0].Err, &refusal) {
			t.Fatalf("err = %v, failures = %+v", err, failures)
		}
		if !refusal.CreateRecordedAt.Equal(created) {
			t.Errorf("CreateRecordedAt = %s, want %s", refusal.CreateRecordedAt, created)
		}
	})

	t.Run("written and marked on request, recorded CREATE TABLE", func(t *testing.T) {
		out, _, _, err := run(t, true)
		if err != nil {
			t.Fatalf("run: %v", err)
		}
		schemaFile, err := os.ReadFile(filepath.Join(out, "shop.t-schema.sql"))
		if err != nil {
			t.Fatalf("read schema file: %v", err)
		}
		t.Logf("rendered:\n%s", schemaFile)
		if !strings.HasPrefix(string(schemaFile), "-- bintrail: CHANGES ONLY.") {
			t.Errorf("schema file does not open with the marker:\n%s", schemaFile)
		}
		if !strings.Contains(string(schemaFile), "CREATE TABLE t (") {
			t.Errorf("schema file lost the recorded CREATE TABLE:\n%s", schemaFile)
		}
	})
}
