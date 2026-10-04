package views

import (
	"strings"
	"testing"
	"time"
)

const httpCacheSetting = "SET enable_http_metadata_cache = true;"

// #2051: a file pinned to one snapshot over S3 reads files that never
// change, so it turns on DuckDB's HTTP metadata cache: without it every query
// re-checks every cached file with a HEAD (~0.5 s each from outside AWS, one
// after another). The setting exists since DuckDB 1.1, so the file keeps
// loading on every version it loaded on (validate_external_file_cache, which
// does the same, only exists from 1.5). A file that follows the newest
// snapshot, or reads only local files, does not get it.
func TestHTTPMetadataCache_onlyPinnedS3_2051(t *testing.T) {
	s3Table := []BaselineTable{{Schema: "shop", Table: "orders", Path: "s3://b/base/2026-04-30T03-00-00Z/shop/orders.parquet", Rel: "shop/orders.parquet"}}
	localTable := []BaselineTable{{Schema: "shop", Table: "orders", Path: "/data/base/2026-04-30T03-00-00Z/shop/orders.parquet", Rel: "shop/orders.parquet"}}
	base := Input{GeneratedAt: time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC), Version: "t",
		BaselineSnapshot: time.Date(2026, 4, 30, 3, 0, 0, 0, time.UTC)}
	cases := []struct {
		name string
		in   func() Input
		want bool
	}{
		{"pinned S3 snapshot", func() Input { in := base; in.BaselineSource = "s3://b/base/"; in.Baselines = s3Table; return in }, true},
		// #2063: a following file turns it on too, after choosing the snapshot.
		{"S3 snapshot following the newest", func() Input {
			in := base
			in.BaselineSource = "s3://b/base/"
			in.Baselines = s3Table
			in.Follow = FollowNewest
			return in
		}, true},
		{"S3 snapshot following the newest, with the S3 events view", func() Input {
			in := base
			in.BaselineSource = "s3://b/base/"
			in.Baselines = s3Table
			in.Follow = FollowNewest
			in.ArchiveSources = []string{"s3://b/arch/bintrail_id=x"}
			return in
		}, false},
		{"pinned local snapshot", func() Input { in := base; in.BaselineSource = "/data/base"; in.Baselines = localTable; return in }, false},
		// The events view's archive files can be rewritten under the same key
		// (a partition archived again before it was dropped), and the setting
		// covers the whole DuckDB session: a file that reads archives from S3
		// never gets it, pinned state views or not.
		{"S3 archives only", func() Input { in := base; in.ArchiveSources = []string{"s3://b/arch/bintrail_id=x"}; return in }, false},
		{"pinned S3 snapshot with the S3 events view", func() Input {
			in := base
			in.BaselineSource = "s3://b/base/"
			in.Baselines = s3Table
			in.ArchiveSources = []string{"s3://b/arch/bintrail_id=x"}
			return in
		}, false},
		{"pinned S3 snapshot, events left out", func() Input {
			in := base
			in.BaselineSource = "s3://b/base/"
			in.Baselines = s3Table
			in.ArchiveSources = []string{"s3://b/arch/bintrail_id=x"}
			in.OmitEvents = true
			return in
		}, true},
	}
	for _, c := range cases {
		got := Generate(c.in())
		if has := strings.Contains(got, httpCacheSetting); has != c.want {
			t.Errorf("%s: carries the HTTP metadata cache = %v, want %v", c.name, has, c.want)
		}
		if !c.want {
			continue
		}
		set, secret, view := strings.Index(got, httpCacheSetting), strings.Index(got, "CREATE OR REPLACE SECRET"), strings.Index(got, "CREATE OR REPLACE VIEW")
		if secret < 0 || view < 0 || !(secret < set && set < view) {
			t.Errorf("%s: setting at %d, secret at %d, first view at %d; want after the secret and before any view", c.name, set, secret, view)
		}
		// The setting outlives the file in the reader's DuckDB: the file says so,
		// and says how to turn it off.
		if !strings.Contains(got, "RESET enable_http_metadata_cache") {
			t.Errorf("%s: the file does not say how to turn the setting off", c.name)
		}
	}
}

// #2063: in a following file the cache is OFF while the snapshot is chosen and
// ON only after it. With it on, DuckDB 1.4 and 1.5 answer a second read of
// _NEWEST in one session from the first (measured), so a file read again
// would never see a refresh.
func TestHTTPMetadataCache_offWhileTheSnapshotIsChosen_2063(t *testing.T) {
	in := newestInput()
	in.ArchiveSources = nil
	for _, ptr := range []string{"", "2026-04-30T03-00-00Z"} {
		in.NewestPointer = ptr
		got := Generate(in)
		off := strings.Index(got, "SET enable_http_metadata_cache = false;")
		choose := strings.Index(got, "SET VARIABLE "+newestVar)
		list := strings.Index(got, "SET VARIABLE "+filesVar)
		on := strings.Index(got, httpCacheSetting)
		view := strings.Index(got, "CREATE OR REPLACE VIEW")
		if off < 0 || !(off < choose && choose < list && list < on && on < view) {
			t.Errorf("pointer=%q: order off=%d choose=%d list=%d on=%d view=%d; want off < choose < list < on < first view",
				ptr, off, choose, list, on, view)
		}
	}
}
