package sqlsandbox

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/dbtrail/dbtrail/internal/archive"
	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/views"
)

// copyFixture is a Parquet copy the way the daemon lays one out: one archived
// partition in the Hive layout rotation writes, and one baseline snapshot.
// Two roots on purpose: the console's archive directory and its backup
// directory are separate settings, so the sandbox must admit more than one
// directory at once.
type copyFixture struct {
	archiveRoot  string
	baselineRoot string
	// views is the generator output the console serves as /api/views.sql:
	// the same text the worker installs before the user's SQL runs.
	views string
}

func newCopyFixture(t *testing.T) copyFixture {
	t.Helper()
	f := copyFixture{archiveRoot: t.TempDir(), baselineRoot: t.TempDir()}
	const id = "11111111-2222-3333-4444-555555555555"
	writeFixtureArchive(t, f.archiveRoot, id)
	baselinePath := writeFixtureBaseline(t, f.baselineRoot)
	f.views = views.Generate(views.Input{
		GeneratedAt:      time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC),
		Version:          "test",
		ArchiveSources:   []string{filepath.Join(f.archiveRoot, "bintrail_id="+id)},
		BaselineSource:   f.baselineRoot,
		BaselineSnapshot: time.Date(2026, 4, 30, 3, 0, 0, 0, time.UTC),
		Baselines:        []views.BaselineTable{{Schema: "shop", Table: "orders", Path: baselinePath}},
	})
	return f
}

func (f copyFixture) job(sqlText string) Job {
	return Job{CopyDirs: []string{f.archiveRoot, f.baselineRoot}, ViewsSQL: f.views, SQL: sqlText}
}

// writeFixtureArchive mirrors internal/views' fixture: one UPDATE through the
// real archive column set and the real Parquet writer.
func writeFixtureArchive(t *testing.T, root, id string) {
	t.Helper()
	path := filepath.Join(root, "bintrail_id="+id, "event_date=2026-05-01", "event_hour=03", "events.parquet")
	w, err := baseline.NewWriter(path, archive.BinlogEventColumns, baseline.WriterConfig{
		Compression: "none", RowGroupSize: 100,
	})
	if err != nil {
		t.Fatalf("archive writer: %v", err)
	}
	values := []string{
		"1", "binlog.000001", "100", "200", "2026-05-01 03:00:00", "",
		"42", "shop", "orders", "2", "1",
		`["status"]`, `{"id":1,"status":"new"}`, `{"id":1,"status":"paid"}`,
		"1", "", "", "1777000000000000",
	}
	nulls := make([]bool, len(archive.BinlogEventColumns))
	nulls[5] = true  // gtid
	nulls[15] = true // query_text
	nulls[16] = true // query_hash
	if len(values) != len(archive.BinlogEventColumns) {
		t.Fatalf("fixture has %d values for %d columns: update the fixture with the column set",
			len(values), len(archive.BinlogEventColumns))
	}
	if err := w.WriteRow(values, nulls); err != nil {
		t.Fatalf("write archive row: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close archive writer: %v", err)
	}
}

func writeFixtureBaseline(t *testing.T, root string) string {
	t.Helper()
	path := filepath.Join(root, "2026-04-30T03-00-00Z", "shop", "orders.parquet")
	schemaFile := filepath.Join(t.TempDir(), "shop.orders-schema.sql")
	ddl := "CREATE TABLE `orders` (\n  `id` int NOT NULL,\n  `status` varchar(32) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n"
	if err := os.WriteFile(schemaFile, []byte(ddl), 0o644); err != nil {
		t.Fatalf("write schema file: %v", err)
	}
	cols, err := baseline.ParseSchema(schemaFile)
	if err != nil {
		t.Fatalf("ParseSchema: %v", err)
	}
	w, err := baseline.NewWriter(path, cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 100})
	if err != nil {
		t.Fatalf("baseline writer: %v", err)
	}
	for _, r := range [][]string{{"1", "new"}, {"2", "paid"}} {
		if err := w.WriteRow(r, []bool{false, false}); err != nil {
			t.Fatalf("write baseline row: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close baseline writer: %v", err)
	}
	return path
}

// writeOutsideParquet writes a real Parquet file OUTSIDE every copy directory,
// so a read of it fails on the sandbox and not on a missing file.
func writeOutsideParquet(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "outside.parquet")
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec("COPY (SELECT range AS i FROM range(3)) TO '" + path + "' (FORMAT PARQUET)"); err != nil {
		t.Fatalf("write outside parquet: %v", err)
	}
	return path
}

// listFiles returns every file under root, relative, for the "never writes"
// assertions.
func listFiles(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	err := filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			rel, _ := filepath.Rel(root, p)
			out = append(out, rel)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// dirDigest hashes every file under root, path and content, for the
// byte-for-byte "the copy did not change" assertions.
func dirDigest(t *testing.T, root string) string {
	t.Helper()
	h := sha256.New()
	for _, rel := range listFiles(t, root) {
		io.WriteString(h, rel+"\x00")
		f, err := os.Open(filepath.Join(root, rel))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.Copy(h, f); err != nil {
			t.Fatal(err)
		}
		f.Close()
		io.WriteString(h, "\x00")
	}
	return hex.EncodeToString(h.Sum(nil))
}
