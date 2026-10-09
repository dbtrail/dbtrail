package query

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/dbtrail/dbtrail/internal/event"
)

// TestFetchSnapshot_aTableNamedLikeAGlob (#2243): parquet_scan takes a path
// as a glob. "order[st]" as a pattern matches "orders" and not itself, so a
// snapshot read of that table returned the rows of the other, stamped with
// the other's snapshot time; alone in its directory it found no file.
func TestFetchSnapshot_aTableNamedLikeAGlob(t *testing.T) {
	if os.Getenv("CGO_ENABLED") == "0" {
		t.Skip("DuckDB requires CGO")
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()
	write := func(p, values, stamp string) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		// The target of a COPY is a plain path, not a pattern.
		if _, err := db.Exec(`COPY (SELECT * FROM (VALUES ` + values + `) t(id)) TO '` + p +
			`' (FORMAT PARQUET, KV_METADATA {'bintrail.snapshot_timestamp': '` + stamp + `'})`); err != nil {
			t.Fatalf("write fixture: %v", err)
		}
	}
	et := event.EventSnapshot
	for _, withNeighbour := range []bool{false, true} {
		dir := filepath.Join(t.TempDir(), "shop")
		own := filepath.Join(dir, "order[st].parquet")
		write(own, "(1),(2),(3)", "2026-07-01T00:00:00Z")
		if withNeighbour {
			write(filepath.Join(dir, "orders.parquet"), "(7),(8)", "2025-01-01T00:00:00Z")
		}
		rows, err := FetchSnapshot(context.Background(), own, Options{Schema: "shop", Table: "order[st]", EventType: &et})
		if err != nil {
			t.Fatalf("neighbour=%v: FetchSnapshot: %v", withNeighbour, err)
		}
		if len(rows) != 3 {
			t.Fatalf("neighbour=%v: %d rows, want the table's 3", withNeighbour, len(rows))
		}
		if y := rows[0].EventTimestamp.Year(); y != 2026 {
			t.Errorf("neighbour=%v: rows stamped %v, want the table's own snapshot time", withNeighbour, rows[0].EventTimestamp)
		}
	}
}
