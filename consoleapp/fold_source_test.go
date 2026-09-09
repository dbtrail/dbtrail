package consoleapp

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// TestResolveFoldSource pins #1626: on a server that backs up to S3 AND keeps
// a local directory, the fold reads the local copy only when it is the
// bucket's newest snapshot, table for table. Every other state keeps the
// bucket, so a stale local directory is never folded from.
func TestResolveFoldSource(t *testing.T) {
	t0 := time.Date(2026, 9, 9, 21, 45, 35, 0, time.UTC)
	t1 := t0.Add(5 * time.Minute)
	files := func(at time.Time, tables ...string) []reconstruct.BaselineFile {
		var out []reconstruct.BaselineFile
		for _, tb := range tables {
			out = append(out, reconstruct.BaselineFile{SnapshotTime: at, Schema: "demo", Table: tb})
		}
		return out
	}
	both := refreshRequest{ServerName: "abirds", BaselineDir: "/b", BaselineS3: "s3://bucket/abirds/"}
	for _, tc := range []struct {
		name   string
		req    refreshRequest
		remote []reconstruct.BaselineFile
		local  []reconstruct.BaselineFile
		errAt  string
		want   string
	}{
		{"same newest snapshot, same tables: the local copy",
			both, files(t0, "orders", "customers"), files(t0, "orders", "customers"), "", "/b"},
		{"local holds an extra table too: still the local copy",
			both, files(t0, "orders"), files(t0, "orders", "customers"), "", "/b"},
		{"local is older: the bucket",
			both, files(t1, "orders"), files(t0, "orders"), "", "s3://bucket/abirds/"},
		{"local is newer than the bucket (upload pending): the bucket",
			both, files(t0, "orders"), files(t1, "orders"), "", "s3://bucket/abirds/"},
		{"local copy is missing a table the bucket has: the bucket",
			both, files(t0, "orders", "customers"), files(t0, "orders"), "", "s3://bucket/abirds/"},
		{"no local snapshot yet: the bucket",
			both, files(t0, "orders"), nil, "", "s3://bucket/abirds/"},
		{"bucket listing fails: the bucket, as before",
			both, files(t0, "orders"), files(t0, "orders"), "s3://bucket/abirds/", "s3://bucket/abirds/"},
		{"local listing fails: the bucket",
			both, files(t0, "orders"), files(t0, "orders"), "/b", "s3://bucket/abirds/"},
		{"no destination: the local directory, no listing needed",
			refreshRequest{BaselineDir: "/b"}, nil, nil, "", "/b"},
		{"no local directory: the bucket, no listing needed",
			refreshRequest{BaselineS3: "s3://bucket/x/"}, nil, nil, "", "s3://bucket/x/"},
		{"already resolved: left alone",
			refreshRequest{BaselineDir: "/b", BaselineS3: "s3://bucket/x/", FoldSource: "/elsewhere"}, nil, nil, "", "/elsewhere"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			orig := listBaselines
			t.Cleanup(func() { listBaselines = orig })
			var calls []string
			listBaselines = func(_ context.Context, source string) ([]reconstruct.BaselineFile, error) {
				calls = append(calls, source)
				if source == tc.errAt {
					return nil, errors.New("listing failed")
				}
				if source == tc.req.BaselineS3 {
					return tc.remote, nil
				}
				return tc.local, nil
			}
			got := resolveFoldSource(context.Background(), tc.req)
			if got != tc.want {
				t.Fatalf("resolveFoldSource = %q, want %q (listed %v)", got, tc.want, calls)
			}
			if (tc.req.BaselineS3 == "" || tc.req.BaselineDir == "" || tc.req.FoldSource != "") && len(calls) != 0 {
				t.Fatalf("no listing may run when the answer needs none, got %v", calls)
			}
		})
	}
}

// TestBaselineFoldSource_honoursTheResolvedSource: every consumer of the fold
// source in one run (the fold config, the reuse log, the listing) reads the
// same resolved value, or the fold would read one place and the log describe
// another.
func TestBaselineFoldSource_honoursTheResolvedSource(t *testing.T) {
	req := refreshRequest{IndexDSN: "dsn", BaselineDir: "/b", BaselineS3: "s3://bucket/x/", FoldSource: "/b"}
	if got := baselineFoldSource(req); got != "/b" {
		t.Fatalf("baselineFoldSource = %q, want the resolved local copy", got)
	}
	if cfg := refreshFoldConfig(req, time.Now(), []string{"demo.orders"}); cfg.BaselineSrc != "/b" || cfg.OutputDir != "/b" {
		t.Fatalf("fold config = src %q out %q, want both /b", cfg.BaselineSrc, cfg.OutputDir)
	}
	req.FoldSource = ""
	if got := baselineFoldSource(req); got != "s3://bucket/x/" {
		t.Fatalf("unresolved request must keep the standing rule, got %q", got)
	}
}
