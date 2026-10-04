package query

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

// TestArchiveRoutingsAgreeOnCount is the invariant that lets a capability gate
// use one routing while its handler uses the other (#1456): for every registry
// shape, ResolveArchiveSources and PortableArchiveSources return the SAME
// number of sources. Each includes a row exactly when it has a parseable local
// base or an S3 root; drop that "never omit" rule from either and a button
// would advertise a file the handler answers 404 for.
func TestArchiveRoutingsAgreeOnCount(t *testing.T) {
	dir := t.TempDir()
	withData := filepath.Join(dir, "bintrail_id=with-data")
	if err := os.MkdirAll(filepath.Join(withData, "event_date=2026-06-05", "event_hour=10"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(withData, "event_date=2026-06-05", "event_hour=10", "e.parquet"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	emptyTree := filepath.Join(dir, "bintrail_id=empty-tree")
	if err := os.MkdirAll(emptyTree, 0o755); err != nil {
		t.Fatal(err)
	}
	cols := []string{"bintrail_id", "sample_local", "sample_bucket", "sample_key"}
	shapes := func() *sqlmock.Rows {
		return sqlmock.NewRows(cols).
			AddRow("with-data", filepath.Join(withData, "e.parquet"), "bkt", "events/bintrail_id=with-data/f.parquet").
			AddRow("empty-tree", filepath.Join(emptyTree, "e.parquet"), "bkt", "events/bintrail_id=empty-tree/f.parquet").
			AddRow("gone-local", filepath.Join(dir, "bintrail_id=gone-local", "e.parquet"), nil, nil).
			AddRow("s3-only", nil, "bkt", "events/bintrail_id=s3-only/f.parquet").
			AddRow("odd-key-local", filepath.Join(dir, "bintrail_id=odd-key-local", "e.parquet"), "bkt", "events/no-id/f.parquet").
			AddRow("odd-key-only", nil, "bkt", "events/no-id/f.parquet").
			AddRow("nulls", nil, nil, nil)
	}
	const wantCount = 5 // odd-key-only and nulls have nothing either routing can name

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery("FROM archive_state").WillReturnRows(shapes())
	// with-data has a local copy with data and an S3 one: the resolver
	// checks its files (#2078). One source either way, so the count holds.
	mock.ExpectQuery(`SELECT local_path, s3_bucket, s3_key FROM archive_state WHERE bintrail_id = \?`).WithArgs("with-data").
		WillReturnRows(sqlmock.NewRows([]string{"local_path", "s3_bucket", "s3_key"}).
			AddRow(filepath.Join(withData, "event_date=2026-06-05", "event_hour=10", "e.parquet"), "bkt", "events/bintrail_id=with-data/f.parquet"))
	mock.ExpectQuery("FROM archive_state").WillReturnRows(shapes())

	local, err := ResolveArchiveSources(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}
	portable, err := PortableArchiveSources(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}
	if len(local) != wantCount || len(portable) != wantCount {
		t.Fatalf("local-first = %d %v, portable = %d %v, want %d each", len(local), local, len(portable), portable, wantCount)
	}
}
