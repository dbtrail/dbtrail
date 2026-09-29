//go:build integration

package cli

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// A MariaDB UUID key is stored in the index as its bytes, so it can only be
// looked up once the index's schema snapshot says the column is a UUID. When
// that snapshot cannot be read, the key used to be looked up as text: the
// baseline row matched, no event did, and reconstruct printed the
// snapshot-era row as the state at --at, exit 0. With the baseline's own
// CREATE TABLE showing a UUID key, it must refuse instead.
func TestRunReconstruct_MariaDBUUIDKeyWithoutIndexTypesRefuses(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	h1 := time.Now().UTC().Add(-48 * time.Hour).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h1})
	// No schema snapshot for mdb.devices: the index cannot type the key.

	baselineDir := t.TempDir()
	parquetDir := filepath.Join(baselineDir, strings.ReplaceAll(h1.Format(time.RFC3339), ":", "-"), "mdb")
	if err := os.MkdirAll(parquetDir, 0o755); err != nil {
		t.Fatal(err)
	}
	create := "CREATE TABLE `devices` (\n  `u` uuid NOT NULL,\n  `n` int(11) DEFAULT NULL,\n  PRIMARY KEY (`u`)\n) ENGINE=InnoDB;\n"
	cols := []baseline.Column{
		{Name: "u", MySQLType: "uuid", ParquetType: baseline.MysqlToParquetNode("uuid")},
		{Name: "n", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
	}
	w, err := baseline.NewWriter(filepath.Join(parquetDir, "devices.parquet"), cols, baseline.WriterConfig{
		Compression: "zstd", RowGroupSize: 100, Metadata: map[string]string{baseline.MetaKeyCreateTableSQL: create},
	})
	if err != nil {
		t.Fatal(err)
	}
	const key = "00000000-0000-0000-0000-000000000001"
	if err := w.WriteRow([]string{key, "1"}, []bool{false, false}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	kb, _ := hex.DecodeString("00000000000000000000000000000001")
	pk := event.BuildPKValues([]metadata.ColumnMeta{{Name: "u", DataType: "uuid"}}, map[string]any{"u": kb})
	u := base64.StdEncoding.EncodeToString(kb)
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, h1.Add(30*time.Minute).Format("2006-01-02 15:04:05"), nil,
		"mdb", "devices", 2, pk, nil, []byte(`{"u":"`+u+`","n":1}`), []byte(`{"u":"`+u+`","n":2}`))

	orig := captureRecFlags()
	t.Cleanup(func() { applyRecFlags(orig) })
	recIndexDSN = testutil.SnapshotDSN(dbName)
	recSchema, recTable, recPK, recPKColumns = "mdb", "devices", key, "u"
	recBaselineDir, recBaselineS3 = baselineDir, ""
	recBaselineOnly, recHistory, recSQL = false, false, ""
	recFormat, recNoArchive, recAllowGaps = "json", true, true
	recAt = h1.Add(45 * time.Minute).Format(time.RFC3339)
	reconstructCmd.SetContext(context.Background())
	t.Cleanup(func() { reconstructCmd.SetContext(nil) })

	oldStdout := os.Stdout
	t.Cleanup(func() { os.Stdout = oldStdout })
	r, wPipe, _ := os.Pipe()
	os.Stdout = wPipe
	runErr := runReconstruct(reconstructCmd, nil)
	wPipe.Close()
	os.Stdout = oldStdout
	out, _ := io.ReadAll(r)
	if runErr == nil {
		t.Fatalf("reconstruct answered without the index's column types for a UUID key: %s", out)
	}
	if !strings.Contains(runErr.Error(), "uuid") || !strings.Contains(runErr.Error(), "bintrail snapshot") {
		t.Errorf("refusal should name the UUID key and the fix (bintrail snapshot): %v", runErr)
	}
}
