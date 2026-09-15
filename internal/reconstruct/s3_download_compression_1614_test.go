package reconstruct

import (
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
)

// The local copy of an S3 baseline is what the daemon's disk check sizes a
// table on (#1614), so it is written with the snapshot writer's compression.
// Runs the real statement on a local source: parquet_scan reads either.
func TestS3DownloadCopySQL_writesTheSnapshotCompression(t *testing.T) {
	src := writeTestBaseline(t, [][]string{{"1", "new"}, {"2", "paid"}})
	dst := filepath.Join(t.TempDir(), "baseline.parquet")
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(s3DownloadCopySQL(src, dst)); err != nil {
		t.Fatalf("copy: %v", err)
	}
	rows, err := db.Query("SELECT DISTINCT compression FROM parquet_metadata('" + dst + "')")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			t.Fatal(err)
		}
		got = append(got, c)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || !strings.EqualFold(got[0], ParquetWriterCompression) {
		t.Fatalf("the local copy is compressed %v, want %q like the snapshot it is compared against", got, ParquetWriterCompression)
	}
}
