package console

import (
	"reflect"
	"slices"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// TestTableDeltaNames: each table is handed only its own delta files, by the
// same parser the chain reader filters with, so the chain it reads is the one
// it read when it was handed the whole schema directory.
func TestTableDeltaNames(t *testing.T) {
	const ts = "2026-06-01T15-00-00Z"
	names := []string{
		// Two tables sharing a prefix.
		"shop/orders.parquet", "shop/orders.000000.posdel", "shop/orders.000000.upserts",
		"shop/orders.000001-000003.posdel", "shop/orders.000001-000003.upserts",
		"shop/orders_2024.parquet", "shop/orders_2024.000001.posdel", "shop/orders_2024.000001.upserts",
		// A table whose own name ends in a sequence-shaped segment.
		"shop/t.000001.parquet", "shop/t.000001.000002.posdel", "shop/t.000001.000002.upserts",
		"shop/t.parquet", "shop/t.000003.posdel", "shop/t.000003.upserts",
		// The v0.83.0 shape.
		"shop/legacy.parquet", "shop/legacy.posdel", "shop/legacy.upserts",
		// The same table name in another schema.
		"crm/orders.parquet", "crm/orders.000004.posdel", "crm/orders.000004.upserts",
		// Never a delta: a dotfile, a table with no chain.
		"shop/.posdel", "shop/plain.parquet",
	}
	var files []baselineSnapshotFile
	for _, n := range names {
		files = append(files, baselineSnapshotFile{RelPath: ts + "/" + n})
	}
	// Not a table file: markers and anything deeper.
	files = append(files,
		baselineSnapshotFile{RelPath: ts + "/_SUCCESS"},
		baselineSnapshotFile{RelPath: ts + "/shop/sub/orders.000009.posdel"})

	got := tableDeltaNames(files)
	want := map[string][]string{
		ts + "/shop/orders":      {"orders.000000.posdel", "orders.000000.upserts", "orders.000001-000003.posdel", "orders.000001-000003.upserts"},
		ts + "/shop/orders_2024": {"orders_2024.000001.posdel", "orders_2024.000001.upserts"},
		ts + "/shop/t.000001":    {"t.000001.000002.posdel", "t.000001.000002.upserts"},
		ts + "/shop/t":           {"t.000003.posdel", "t.000003.upserts"},
		ts + "/shop/legacy":      {"legacy.posdel", "legacy.upserts"},
		ts + "/crm/orders":       {"orders.000004.posdel", "orders.000004.upserts"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("tableDeltaNames:\n got %v\nwant %v", got, want)
	}
	if n := len(tableDeltaNames(nil)); n != 0 {
		t.Errorf("no files gave %d tables", n)
	}

	// The chain each table gets is the chain it got from its whole directory.
	byDir := map[string][]string{}
	for _, n := range names {
		schema, file, _ := cutSlash(n)
		byDir[schema] = append(byDir[schema], file)
	}
	for _, n := range names {
		schema, file, _ := cutSlash(n)
		stem, ok := trimParquet(file)
		if !ok {
			continue
		}
		dir := "/snap/" + schema
		whole, errW := baseline.TableDeltaChainIn(dir, byDir[schema], stem)
		mine, errM := baseline.TableDeltaChainIn(dir, got[ts+"/"+schema+"/"+stem], stem)
		if !reflect.DeepEqual(whole, mine) || (errW == nil) != (errM == nil) {
			t.Errorf("%s: chain from its own files %+v (%v), from the directory %+v (%v)", n, mine, errM, whole, errW)
		}
	}
}

func cutSlash(s string) (string, string, bool) {
	i := slices.Index([]byte(s), '/')
	if i < 0 {
		return "", s, false
	}
	return s[:i], s[i+1:], true
}

func trimParquet(name string) (string, bool) {
	const suf = ".parquet"
	if len(name) <= len(suf) || name[len(name)-len(suf):] != suf {
		return "", false
	}
	return name[:len(name)-len(suf)], true
}
