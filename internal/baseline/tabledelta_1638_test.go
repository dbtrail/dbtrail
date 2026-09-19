package baseline

import (
	"database/sql"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
)

// TestDeltaProbePattern pins the two properties the S3 probe stands on.
//
// It must hold a wildcard. Verified against DuckDB 1.5.5 and a real S3 API: a
// glob over an s3:// pattern with no wildcard makes no request and returns the
// pattern as its one row, so an exact-key probe says "exists" for every key.
// The first version of this probe did exactly that, which would have made every
// S3 baseline lookup fail looking for a delta that was never there.
//
// And it must match the table's own files only, run here through DuckDB's glob
// (the same matcher, over a local directory): a sibling whose name merely
// starts the same way is another table.
func TestDeltaProbePattern(t *testing.T) {
	dir := t.TempDir()
	// A name with glob metacharacters, to cover the escaping as well.
	for _, n := range []string{"or[d]ers.parquet", "or[d]ers.000000.posdel", "or[d]ers.000000.upserts", "or[d]ers_archive.000000.posdel", "orders.000000.posdel"} {
		if err := os.WriteFile(filepath.Join(dir, n), nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	pattern := deltaProbePattern(filepath.Join(dir, "or[d]ers.parquet"))
	if !strings.Contains(pattern, "*") {
		t.Fatalf("probe pattern %q holds no wildcard: over S3 it would report every key as present", pattern)
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.Query("SELECT file FROM glob('" + strings.ReplaceAll(pattern, "'", "''") + "')")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var f string
		if err := rows.Scan(&f); err != nil {
			t.Fatal(err)
		}
		got = append(got, filepath.Base(f))
	}
	sort.Strings(got)
	want := []string{"or[d]ers.000000.posdel", "or[d]ers.000000.upserts", "or[d]ers.parquet"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("probe matched %v, want exactly %v", got, want)
	}
}

func TestSnapshotTableDeltas(t *testing.T) {
	snap := t.TempDir()
	touch := func(rel string) {
		p := filepath.Join(snap, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	touch("shop/orders.parquet")
	touch("shop/orders.000000.posdel")
	touch("shop/orders.000000.upserts")
	touch("shop/plain.parquet")
	got, err := SnapshotTableDeltas(t.Context(), snap)
	if err != nil {
		t.Fatal(err)
	}
	if !got[filepath.Join(snap, "shop/orders.parquet")] || got[filepath.Join(snap, "shop/plain.parquet")] || len(got) != 1 {
		t.Fatalf("SnapshotTableDeltas = %v, want only shop/orders", got)
	}
	touch("shop/half.000000.posdel")
	if _, err := SnapshotTableDeltas(t.Context(), snap); err == nil || !strings.Contains(err.Error(), "half.parquet") {
		t.Fatalf("half a pair: err = %v, want ErrHalfTableDelta naming the table", err)
	}
}
