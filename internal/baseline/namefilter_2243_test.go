package baseline

import (
	"database/sql"
	"testing"
)

// TestTableDeltaNameFilter_cutsTheNameAtASlashOnly (#2243): a backslash is a
// legal character of a file name. Cut there too, the filter of a table named
// `a\orders` was the filter of "orders" and let that table's chain in.
func TestTableDeltaNameFilter_cutsTheNameAtASlashOnly(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	filter := TableDeltaNameFilter(`/snap/shop/a\orders.parquet`, TableDeltaUpsertsSuffix)
	for name, want := range map[string]bool{
		`/snap/shop/a\orders.000000.upserts`:        true,
		`/snap/shop/a\orders.000000-000003.upserts`: true,
		`/snap/shop/orders.000000.upserts`:          false,
		`/snap/shop/b\orders.000000.upserts`:        false,
	} {
		var got bool
		if err := db.QueryRow("SELECT "+filter+" FROM (SELECT ? AS filename)", name).Scan(&got); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got != want {
			t.Errorf("the filter of a\\orders on %s = %v, want %v", name, got, want)
		}
	}
}
