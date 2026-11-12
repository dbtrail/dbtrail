package duckdbutil

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/dbtrail/dbtrail/internal/storage"
)

// TestMain keeps this package's tests off the network: a session that reads
// s3:// paths asks where each bucket lives (#2688), and a test about
// something else must not depend on what AWS answers for a made-up bucket.
func TestMain(m *testing.M) {
	restore := storage.SetBucketRegionDiscoveryForTest(func(context.Context, string, string) (string, error) {
		return "", errors.New("bucket region discovery is disabled in this package's tests")
	})
	code := m.Run()
	restore()
	os.Exit(code)
}

type regionAnswers struct {
	mu    sync.Mutex
	m     map[string]string
	asked map[string]int
}

func withRegions(t *testing.T, m map[string]string) *regionAnswers {
	t.Helper()
	a := &regionAnswers{m: m, asked: map[string]int{}}
	restore := storage.SetBucketRegionDiscoveryForTest(func(_ context.Context, _, bucket string) (string, error) {
		a.mu.Lock()
		defer a.mu.Unlock()
		a.asked[bucket]++
		if r, ok := a.m[bucket]; ok {
			return r, nil
		}
		return "", errors.New("unreachable")
	})
	t.Cleanup(restore) // back to this package's offline default
	return a
}

func (a *regionAnswers) count(b string) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.asked[b]
}

func TestWithDiscoveredRegions_2688(t *testing.T) {
	ctx := context.Background()
	none := storage.S3Endpoint{}
	pinned := store(t, "", "", "eu-west-1")

	t.Run("same region: no pin", func(t *testing.T) {
		t.Setenv("AWS_REGION", "us-west-2")
		withRegions(t, map[string]string{"near": "us-west-2"})
		if got := withDiscoveredRegions(ctx, "", none, []string{"s3://near/a.parquet"}, nil); len(got) != 0 {
			t.Errorf("a same-region bucket got a pin: %v", got)
		}
	})
	t.Run("other region: pinned", func(t *testing.T) {
		t.Setenv("AWS_REGION", "us-west-2")
		withRegions(t, map[string]string{"far": "us-east-1"})
		got := withDiscoveredRegions(ctx, "", none, []string{"s3://far/snap/*.parquet"}, nil)
		if got["far"].Region != "us-east-1" || got["far"].Endpoint.Set() || got["far"].HasKeys() {
			t.Errorf("got %v, want a region-only pin to us-east-1", got)
		}
	})
	t.Run("AWS_DEFAULT_REGION counts when AWS_REGION is empty", func(t *testing.T) {
		t.Setenv("AWS_REGION", "")
		t.Setenv("AWS_DEFAULT_REGION", "us-east-1")
		withRegions(t, map[string]string{"far": "us-east-1"})
		if got := withDiscoveredRegions(ctx, "", none, []string{"s3://far/a"}, nil); len(got) != 0 {
			t.Errorf("got %v, want no pin", got)
		}
	})
	t.Run("empty environment region: pinned", func(t *testing.T) {
		t.Setenv("AWS_REGION", "")
		t.Setenv("AWS_DEFAULT_REGION", "")
		withRegions(t, map[string]string{"far": "us-east-1"})
		if got := withDiscoveredRegions(ctx, "", none, []string{"s3://far/a"}, nil); got["far"].Region != "us-east-1" {
			t.Errorf("got %v, want a pin (nothing says what the chain signs for)", got)
		}
	})
	t.Run("discovery fails: no pin", func(t *testing.T) {
		t.Setenv("AWS_REGION", "us-west-2")
		withRegions(t, nil)
		if got := withDiscoveredRegions(ctx, "", none, []string{"s3://dark/a"}, nil); len(got) != 0 {
			t.Errorf("got %v", got)
		}
	})
	t.Run("caller pinned a region: not asked", func(t *testing.T) {
		t.Setenv("AWS_REGION", "us-west-2")
		a := withRegions(t, map[string]string{"far": "us-east-1"})
		if got := withDiscoveredRegions(ctx, "ap-south-1", none, []string{"s3://far/a"}, nil); len(got) != 0 || a.count("far") != 0 {
			t.Errorf("got %v after %d questions", got, a.count("far"))
		}
	})
	t.Run("custom endpoint: not asked", func(t *testing.T) {
		t.Setenv("AWS_REGION", "us-west-2")
		a := withRegions(t, map[string]string{"far": "us-east-1"})
		ep := storage.S3Endpoint{URL: "http://minio:9000", PathStyle: true, Source: storage.EnvS3Endpoint}
		if got := withDiscoveredRegions(ctx, "", ep, []string{"s3://far/a"}, nil); len(got) != 0 || a.count("far") != 0 {
			t.Errorf("got %v after %d questions", got, a.count("far"))
		}
	})
	t.Run("bucket with its own store: kept, not asked", func(t *testing.T) {
		t.Setenv("AWS_REGION", "us-west-2")
		a := withRegions(t, map[string]string{"own": "us-east-1"})
		stores := map[string]storage.BucketStore{"own": pinned}
		got := withDiscoveredRegions(ctx, "", none, []string{"s3://own/a"}, stores)
		if len(got) != 1 || got["own"].Region != "eu-west-1" || a.count("own") != 0 {
			t.Errorf("got %v after %d questions", got, a.count("own"))
		}
	})
	t.Run("two buckets, two regions, one question each, stores untouched", func(t *testing.T) {
		t.Setenv("AWS_REGION", "us-west-2")
		a := withRegions(t, map[string]string{"a": "us-east-1", "b": "eu-central-1"})
		stores := map[string]storage.BucketStore{"own": pinned}
		paths := []string{"s3://a/x.parquet", "s3://b/y.parquet", "s3://a/z.parquet", "/local/f.parquet", "s3://bare"}
		got := withDiscoveredRegions(ctx, "", none, paths, stores)
		if got["a"].Region != "us-east-1" || got["b"].Region != "eu-central-1" || got["own"].Region != "eu-west-1" || len(got) != 3 {
			t.Errorf("got %v", got)
		}
		if len(stores) != 1 {
			t.Errorf("the caller's store map was modified: %v", stores)
		}
		if a.count("a") != 1 || a.count("b") != 1 || a.count("bare") != 0 {
			t.Errorf("questions: a=%d b=%d bare=%d", a.count("a"), a.count("b"), a.count("bare"))
		}
		// A second session is answered from the cache.
		withDiscoveredRegions(ctx, "", none, paths, stores)
		if a.count("a") != 1 || a.count("b") != 1 {
			t.Errorf("second session asked again: a=%d b=%d", a.count("a"), a.count("b"))
		}
	})
}

// The pin reaches DuckDB: a secret scoped to the far bucket carries its
// region, and DuckDB picks it for that bucket's paths and not for another's.
func TestEnableS3CredentialChainFor_pinsTheBucketsRegion_2688(t *testing.T) {
	t.Setenv("AWS_REGION", "us-west-2")
	t.Setenv("BINTRAIL_DUCKDB_NO_AWS_EXT", "")
	t.Setenv(storage.EnvS3Endpoint, "")
	t.Setenv(storage.EnvS3PathStyle, "")
	t.Setenv("AWS_ENDPOINT_URL_S3", "")
	t.Setenv("AWS_ENDPOINT_URL", "")
	withStores(t, nil)
	withRegions(t, map[string]string{"far-bucket": "us-east-1", "near-bucket": "us-west-2"})
	ctx := context.Background()

	for _, noExt := range []string{"", "1"} {
		t.Run("BINTRAIL_DUCKDB_NO_AWS_EXT="+noExt, func(t *testing.T) {
			t.Setenv("BINTRAIL_DUCKDB_NO_AWS_EXT", noExt)
			db, err := sql.Open("duckdb", "")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { db.Close() })
			if err := LoadHTTPFS(ctx, db); err != nil {
				t.Skip("httpfs unavailable (offline host)")
			}
			if err := EnableS3CredentialChainFor(ctx, db, "", "s3://far-bucket/snap/t.parquet", "s3://near-bucket/snap/u.parquet"); err != nil {
				t.Fatal(err)
			}
			var name, desc string
			if err := db.QueryRow("SELECT name, secret_string FROM duckdb_secrets() WHERE name = (SELECT name FROM which_secret('s3://far-bucket/snap/t.parquet', 's3'))").Scan(&name, &desc); err != nil {
				t.Fatalf("no secret for the far bucket: %v", err)
			}
			if !strings.Contains(desc, "region=us-east-1") || !strings.Contains(desc, "s3://far-bucket/") {
				t.Errorf("the far bucket's secret %s does not carry its region and scope: %s", name, desc)
			}
			var n int
			if err := db.QueryRow("SELECT count(*) FROM which_secret('s3://near-bucket/snap/u.parquet', 's3') WHERE name LIKE '%near%'").Scan(&n); err != nil || n != 0 {
				t.Errorf("a same-region bucket got a secret of its own (n=%d, err=%v)", n, err)
			}
		})
	}
}
