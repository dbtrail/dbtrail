package views

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

func prefetchInput() Input {
	snap := "s3://b/base/2026-04-30T03-00-00Z/"
	return Input{
		GeneratedAt: time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC), Version: "t",
		BaselineSource: "s3://b/base/", BaselineSnapshot: time.Date(2026, 4, 30, 3, 0, 0, 0, time.UTC),
		Baselines: []BaselineTable{
			{Schema: "shop", Table: "orders", Path: snap + "shop/orders.parquet", Rel: "shop/orders.parquet",
				Delta: true, DeltaFiles: []baseline.TableDeltaFile{
					{Seq: 0, SeqLo: 0, Posdel: snap + "shop/orders.000000.posdel", Upserts: snap + "shop/orders.000000.upserts"},
					{Seq: 1, SeqLo: 1, Posdel: snap + "shop/orders.000001.posdel", Upserts: snap + "shop/orders.000001.upserts"},
				}},
			{Schema: "shop", Table: "items", Path: snap + "shop/items.parquet", Rel: "shop/items.parquet"},
			{Schema: "shop", Table: "odd", Path: snap + "shop/log[2024].parquet", Rel: "shop/log[2024].parquet"},
			{Schema: "shop", Table: "legacy", Path: snap + "shop/legacy.parquet", Rel: "shop/legacy.parquet",
				Delta: true, DeltaLegacy: true},
		},
	}
}

// #2053: the prefetch names exactly the files the views open when they are
// created, ahead of the first view, and only in a file that turns the HTTP
// metadata cache on (without it each bind HEADs again and nothing is saved).
func TestFooterPrefetch_namesEveryFileTheViewsOpen_2053(t *testing.T) {
	in := prefetchInput()
	snap := "s3://b/base/2026-04-30T03-00-00Z/shop/"
	want := []string{
		snap + "orders.parquet", snap + "orders.000000.posdel", snap + "orders.000000.upserts",
		snap + "orders.000001.upserts",
		snap + "items.parquet", snap + "legacy.parquet",
	}
	if got := prefetchFiles(in); !slices.Equal(slices.Sorted(slices.Values(got)), slices.Sorted(slices.Values(want))) {
		t.Errorf("prefetchFiles = %v\nwant %v", got, want)
	}
	sql := Generate(in)
	pre := strings.Index(sql, "parquet_file_metadata(CASE")
	view := strings.Index(sql, "CREATE OR REPLACE VIEW")
	if pre < 0 || view < 0 || pre > view {
		t.Fatalf("prefetch at %d, first view at %d; want the prefetch first", pre, view)
	}
	// Every literal file a view names is in the prefetch list.
	list := sql[pre : strings.Index(sql[pre:], "] ELSE")+pre]
	if strings.Contains(list, "log[2024]") {
		t.Error("a path with a glob character is prefetched; it would abort the file before any view")
	}
	if strings.Contains(list, "orders.000001.posdel") {
		t.Error("a second posdel file is prefetched; the bind never opens it")
	}
	for _, f := range want {
		if !strings.Contains(list, "'"+f+"'") {
			t.Errorf("prefetch list misses %s", f)
		}
	}

	// A following file prefetches from its own listing of the snapshot, never
	// from the files named at generation (#2063): it meets later snapshots.
	in.Follow = FollowNewest
	if got := Generate(in); !strings.Contains(got, "parquet_file_metadata(") || strings.Contains(got, "'"+want[0]+"',") {
		t.Error("a following file must prefetch from the session's listing, with no file named at generation")
	}
	local := prefetchInput()
	local.BaselineSource = "/data/base"
	for i := range local.Baselines {
		local.Baselines[i].Path = strings.Replace(local.Baselines[i].Path, "s3://b/base", "/data/base", 1)
		local.Baselines[i].DeltaFiles = nil
		local.Baselines[i].Delta = false
	}
	if strings.Contains(Generate(local), "parquet_file_metadata(") {
		t.Error("a local file prefetches; there is no round trip to save")
	}
}

// The emitted statements run, print nothing, and leave the views reading the
// same rows: run against local Parquet, which DuckDB reads through the same
// statements.
func TestFooterPrefetch_runsAndChangesNothing_2053(t *testing.T) {
	root := t.TempDir()
	path := writeSnapshot(t, root, "2026-04-30T03-00-00Z", true, "a", "b")
	var b strings.Builder
	in := Input{Baselines: []BaselineTable{{Schema: "shop", Table: "orders", Path: path}}}
	writeFooterPrefetch(&b, in)
	db := execViews(t, b.String()+"CREATE SCHEMA shop; CREATE VIEW shop.orders AS SELECT * FROM read_parquet("+sqlString(path)+");")
	var n int
	if err := db.QueryRow(`SELECT getvariable('` + prefetchVar + `')`).Scan(&n); err != nil || n != 1 {
		t.Errorf("prefetch variable = %d, %v; want 1 file read", n, err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM shop.orders`).Scan(&n); err != nil || n != 2 {
		t.Errorf("view rows = %d, %v; want 2", n, err)
	}
}
