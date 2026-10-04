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
		{"S3 snapshot following the newest", func() Input {
			in := base
			in.BaselineSource = "s3://b/base/"
			in.Baselines = s3Table
			in.Follow = FollowNewest
			return in
		}, false},
		{"pinned local snapshot", func() Input { in := base; in.BaselineSource = "/data/base"; in.Baselines = localTable; return in }, false},
		{"pinned S3 archive only", func() Input { in := base; in.ArchiveSources = []string{"s3://b/arch/bintrail_id=x"}; return in }, true},
	}
	for _, c := range cases {
		got := Generate(c.in())
		if has := strings.Contains(got, httpCacheSetting); has != c.want {
			t.Errorf("%s: carries the HTTP metadata cache = %v, want %v", c.name, has, c.want)
		}
		if c.want && strings.Index(got, httpCacheSetting) > strings.Index(got, "CREATE OR REPLACE VIEW") {
			t.Errorf("%s: the setting comes after the first view; it must be on before anything is read", c.name)
		}
	}
}
