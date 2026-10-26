package duckdbutil

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/storage"
)

// A session whose every read goes to buckets with keys of their own never
// asks the AWS credential chain (#2568): off AWS that lookup waits on the
// instance-metadata address for seconds, and it can only ever produce a
// secret those buckets' own scoped secrets outrank.

func TestTargetsNeedChain(t *testing.T) {
	stores := map[string]storage.BucketStore{
		"keyed":          keyedStore(t, "https://s3.example.test", "AKIASTORE", "secret"),
		"keyed.with.dot": keyedStore(t, "https://s3.example.test", "AKIASTORE", "secret"),
		"keysonly":       func() storage.BucketStore { s, _ := store(t, "", "", "eu-west-1").WithKeys("AKIA", "s"); return s }(),
		"endpointonly":   store(t, "https://s3.example.test", "", ""),
	}
	cases := []struct {
		name  string
		paths []string
		want  bool
	}{
		{"no paths: unknown targets keep the chain", nil, true},
		{"empty list", []string{}, true},
		{"keyed bucket", []string{"s3://keyed/snap/t.parquet"}, false},
		{"bare bucket: its scoped secret does not match", []string{"s3://keyed"}, true},
		{"keyed bucket, trailing slash", []string{"s3://keyed/"}, false},
		{"keyed bucket, glob", []string{"s3://keyed/snap/*.parquet"}, false},
		{"bucket name with dots", []string{"s3://keyed.with.dot/a.parquet"}, false},
		{"keys-only store", []string{"s3://keysonly/a.parquet"}, false},
		{"same prefix as a keyed bucket is another bucket", []string{"s3://keyed-archive/a.parquet"}, true},
		{"keyed bucket is a prefix of the name", []string{"s3://keyedx/a.parquet"}, true},
		{"store with an endpoint and no keys", []string{"s3://endpointonly/a.parquet"}, true},
		{"bucket with no store", []string{"s3://plain/a.parquet"}, true},
		{"one keyed, one not", []string{"s3://keyed/a.parquet", "s3://plain/b.parquet"}, true},
		{"two keyed", []string{"s3://keyed/a.parquet", "s3://keyed.with.dot/b.parquet"}, false},
		{"keyed plus a local file", []string{"s3://keyed/a.parquet", "/var/lib/x/b.parquet"}, false},
		{"empty bucket", []string{"s3:///a.parquet"}, true},
		{"uppercase scheme is not ours to read", []string{"S3://keyed/a.parquet"}, true},
		{"other scheme", []string{"s3a://keyed/a.parquet"}, true},
		{"case differs from the stored bucket", []string{"s3://KEYED/a.parquet"}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := targetsNeedChain(c.paths, stores); got != c.want {
				t.Errorf("targetsNeedChain(%q) = %v, want %v", c.paths, got, c.want)
			}
		})
	}
}

func TestEnableS3CredentialChainFor_keyedTargetsSkipTheChain(t *testing.T) {
	withStores(t, map[string]storage.BucketStore{
		"keyed-bucket": keyedStore(t, "https://s3.example.test", "AKIASTORE", "secret"),
	})
	t.Setenv("BINTRAIL_DUCKDB_NO_AWS_EXT", "")
	t.Setenv(storage.EnvS3Endpoint, "")
	t.Setenv(storage.EnvS3PathStyle, "")
	ctx := context.Background()

	open := func(t *testing.T) *sql.DB {
		t.Helper()
		db, err := sql.Open("duckdb", "")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { db.Close() })
		if err := LoadHTTPFS(ctx, db); err != nil {
			t.Skip("httpfs unavailable (offline host)")
		}
		return db
	}
	awsLoaded := func(t *testing.T, db *sql.DB) bool {
		t.Helper()
		var loaded bool
		if err := db.QueryRow("SELECT loaded FROM duckdb_extensions() WHERE extension_name = 'aws'").Scan(&loaded); err != nil {
			t.Fatal(err)
		}
		return loaded
	}

	t.Run("keyed only", func(t *testing.T) {
		db := open(t)
		if err := EnableS3CredentialChainFor(ctx, db, "", "s3://keyed-bucket/snap/t.parquet"); err != nil {
			t.Fatal(err)
		}
		if awsLoaded(t, db) {
			t.Error("the aws extension was loaded for a session that reads only a keyed bucket")
		}
		var n int
		if err := db.QueryRow("SELECT count(*) FROM duckdb_secrets() WHERE name = 'bintrail_s3_chain'").Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 0 {
			t.Error("a credential_chain secret exists for a session that reads only a keyed bucket")
		}
		var got string
		if err := db.QueryRow("SELECT name FROM which_secret('s3://keyed-bucket/snap/t.parquet', 's3')").Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != bucketSecretName("keyed-bucket") {
			t.Errorf("the keyed bucket resolves to %q, not its own secret", got)
		}
	})
	t.Run("keyed and unkeyed", func(t *testing.T) {
		db := open(t)
		if err := EnableS3CredentialChainFor(ctx, db, "", "s3://keyed-bucket/a.parquet", "s3://other-bucket/b.parquet"); err != nil {
			t.Fatal(err)
		}
		if !awsLoaded(t, db) {
			t.Skip("aws extension unavailable (offline host)")
		}
	})
}

// A routing statement that failed because time ran out says so: "is httpfs
// loaded?" sent the operator after an extension that was loaded fine.
func TestApplyBucketStoreSecrets_outOfTimeSaysSo(t *testing.T) {
	stores := map[string]storage.BucketStore{
		"keyed": keyedStore(t, "https://s3.example.test", "AKIASTORE", "secret"),
		"plain": store(t, "https://s3.example.test", "", ""),
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	exec := func(ctx context.Context, stmt string) error {
		return errors.New("Interrupted: " + stmt)
	}
	for _, only := range []string{"keyed", "plain"} {
		err := applyBucketStoreSecrets(ctx, exec, map[string]storage.BucketStore{only: stores[only]}, false)
		if err == nil {
			t.Fatalf("%s: no error", only)
		}
		if strings.Contains(err.Error(), "httpfs") || !strings.Contains(err.Error(), "ran out of time") {
			t.Errorf("%s: %v", only, err)
		}
		if strings.Contains(err.Error(), "AKIASTORE") || strings.Contains(err.Error(), "'secret'") {
			t.Errorf("%s: keys in the error: %v", only, err)
		}
		if !errors.Is(err, context.Canceled) {
			t.Errorf("%s: the context's error is not kept: %v", only, err)
		}
	}
}
