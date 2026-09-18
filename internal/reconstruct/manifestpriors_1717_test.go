package reconstruct

import (
	"path/filepath"
	"reflect"
	"testing"
)

// #1717: the priors handed to the manifest writer are the local snapshot
// directories the run's tables were read from, once each, in report order;
// S3 sources and baseline-less tables contribute nothing.
func TestManifestPriorDirs(t *testing.T) {
	a := filepath.Join("root", "2026-05-01T09-00-00Z")
	b := filepath.Join("root", "2026-05-01T08-00-00Z")
	reps := []*TableReport{
		{SourceSnapshotDir: a},
		nil,
		{SourceSnapshotDir: ""},
		{SourceSnapshotDir: b},
		{SourceSnapshotDir: a},
	}
	if got := manifestPriorDirs(reps); !reflect.DeepEqual(got, []string{a, b}) {
		t.Fatalf("priors = %v", got)
	}
	if got := manifestPriorDirs(nil); got != nil {
		t.Fatalf("no reports: %v", got)
	}
}

func TestLocalSnapshotDir(t *testing.T) {
	cases := map[string]string{
		filepath.Join("root", "2026-05-01T09-00-00Z", "shop", "orders.parquet"): filepath.Join("root", "2026-05-01T09-00-00Z"),
		"s3://bucket/prefix/2026-05-01T09-00-00Z/shop/orders.parquet":           "",
		"": "",
	}
	for in, want := range cases {
		if got := localSnapshotDir(in); got != want {
			t.Errorf("localSnapshotDir(%q) = %q, want %q", in, got, want)
		}
	}
}
