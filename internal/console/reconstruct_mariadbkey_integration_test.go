//go:build integration

package console

import (
	"encoding/base64"
	"encoding/hex"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// Time-travel on a MariaDB UUID key whose table is not typed by the schema
// snapshot in effect at the baseline (it joined the snapshots later). The key
// cannot be spelled the way the index stores it, so the endpoint must refuse
// rather than return the baseline-era row as the state at `at`.
func TestIntegrationReconstruct_MariaDBUUIDKeyWithoutIndexTypesRefuses(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	testutil.InsertSnapshot(t, db, 1, "2026-06-01 00:00:00", "app", "users", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 2, "2026-06-01 06:00:00", "mdb", "devices", "u", 1, "PRI", "uuid", "NO")
	testutil.InsertSnapshot(t, db, 2, "2026-06-01 06:00:00", "mdb", "devices", "n", 2, "", "int", "YES")

	baseDir := t.TempDir()
	dir := filepath.Join(baseDir, strings.ReplaceAll(time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339), ":", "-"), "mdb")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	create := "CREATE TABLE `devices` (\n  `u` uuid NOT NULL,\n  `n` int(11) DEFAULT NULL,\n  PRIMARY KEY (`u`)\n) ENGINE=InnoDB;\n"
	cols := []baseline.Column{
		{Name: "u", MySQLType: "uuid", ParquetType: baseline.MysqlToParquetNode("uuid")},
		{Name: "n", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
	}
	w, err := baseline.NewWriter(filepath.Join(dir, "devices.parquet"), cols, baseline.WriterConfig{
		Compression: "none", RowGroupSize: 100, Metadata: map[string]string{baseline.MetaKeyCreateTableSQL: create}})
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
	testutil.InsertEvent(t, db, "bin.000001", 4, 40, "2026-06-01 12:00:00", nil, "mdb", "devices", 2, pk,
		nil, []byte(`{"u":"`+u+`","n":1}`), []byte(`{"u":"`+u+`","n":2}`))

	srv, err := New(Config{DB: db, DBName: dbName, Listen: "127.0.0.1:8090", Token: intToken, BaselineDir: baseDir})
	if err != nil {
		t.Fatal(err)
	}
	rec, body := doReq(t, srv, "GET", "/api/reconstruct?schema=mdb&table=devices&pk="+url.QueryEscape(key)+
		"&at="+url.QueryEscape("2026-06-01 13:30:00")+"&allow_gaps=true", "")
	if rec.Code == 200 {
		t.Fatalf("the endpoint answered a UUID key without the index's column types: %s", body)
	}
	if !strings.Contains(string(body), "bintrail snapshot") {
		t.Errorf("refusal should name the fix (bintrail snapshot): %d %s", rec.Code, body)
	}
}
