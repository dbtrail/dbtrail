package cliapp

import "testing"

// TestUploadable: a table's delta is uploaded with it. Leaving the pair behind
// publishes a table file that is older than its directory says (#1638).
func TestUploadable(t *testing.T) {
	for path, want := range map[string]bool{
		"/b/2026-05-01T12-00-00Z/shop/orders.parquet":    true,
		"/b/2026-05-01T12-00-00Z/shop/orders.posdel":     true,
		"/b/2026-05-01T12-00-00Z/shop/orders.upserts":    true,
		"/b/2026-05-01T12-00-00Z/shop/ORDERS.PARQUET":    true,
		"/b/2026-05-01T12-00-00Z/_SUCCESS":               false,
		"/b/2026-05-01T12-00-00Z/views.sql":              false,
		"/b/2026-05-01T12-00-00Z/shop/orders.posdel.tmp": false,
	} {
		if got := uploadable(path); got != want {
			t.Errorf("uploadable(%q) = %v, want %v", path, got, want)
		}
	}
}
