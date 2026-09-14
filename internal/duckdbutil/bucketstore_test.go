package duckdbutil

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/dbtrail/dbtrail/internal/storage"
)

// Per-bucket stores on the DuckDB half (#1575).

func withStores(t *testing.T, m map[string]storage.BucketStore) {
	t.Helper()
	storage.SetBucketStores(m)
	t.Cleanup(func() { storage.SetBucketStores(nil) })
}

func store(t *testing.T, endpoint, style, region string) storage.BucketStore {
	t.Helper()
	s, err := storage.NewBucketStore(endpoint, style, region)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestBucketStoreSecretStatements(t *testing.T) {
	if got := BucketStoreSecretStatements(nil); got != nil {
		t.Fatalf("no stores must render nothing, got %q", got)
	}
	stores := map[string]storage.BucketStore{
		"zeta.minio":  store(t, "http://minio:9000", "", ""),
		"alpha-wasab": store(t, "https://s3.eu-central-1.wasabisys.com", "vhost", "eu-central-1"),
		"pinned":      store(t, "", "", "ap-south-1"),
	}
	got := BucketStoreSecretStatements(stores)
	want := []string{
		"CREATE OR REPLACE SECRET bintrail_s3_bucket_alpha_wasab_" + hashOf("alpha-wasab") +
			" (TYPE s3, PROVIDER credential_chain, SCOPE 's3://alpha-wasab/', REGION 'eu-central-1', ENDPOINT 's3.eu-central-1.wasabisys.com', URL_STYLE 'vhost', USE_SSL true)",
		"CREATE OR REPLACE SECRET bintrail_s3_bucket_pinned_" + hashOf("pinned") +
			" (TYPE s3, PROVIDER credential_chain, SCOPE 's3://pinned/', REGION 'ap-south-1')",
		"CREATE OR REPLACE SECRET bintrail_s3_bucket_zeta_minio_" + hashOf("zeta.minio") +
			" (TYPE s3, PROVIDER credential_chain, SCOPE 's3://zeta.minio/', REGION 'us-east-1', ENDPOINT 'minio:9000', URL_STYLE 'path', USE_SSL false)",
	}
	if len(got) != len(want) {
		t.Fatalf("got %d statements, want %d: %q", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("statement %d:\n got %s\nwant %s", i, got[i], want[i])
		}
		if strings.Contains(got[i], "KEY_ID") || strings.Contains(got[i], "SECRET '") {
			t.Errorf("statement %d carries a key: %s", i, got[i])
		}
	}
	// Two names that fold to the same identifier stay distinct.
	if bucketSecretName("a-b") == bucketSecretName("a.b") {
		t.Error("a-b and a.b collide on the secret name")
	}
}

func hashOf(bucket string) string {
	n := bucketSecretName(bucket)
	return n[strings.LastIndex(n, "_")+1:]
}

func TestBucketStoreConfigSecretStatements(t *testing.T) {
	stores := map[string]storage.BucketStore{"b": store(t, "http://minio:9000", "", "")}
	got := bucketStoreConfigSecretStatements(stores, "AKIA", "s'cret", "tok")
	if len(got) != 1 {
		t.Fatalf("got %q", got)
	}
	for _, part := range []string{"PROVIDER config", "KEY_ID 'AKIA'", "SECRET 's''cret'", "SESSION_TOKEN 'tok'", "SCOPE 's3://b/'", "ENDPOINT 'minio:9000'"} {
		if !strings.Contains(got[0], part) {
			t.Errorf("missing %q in %s", part, got[0])
		}
	}
	// Without keys the secret still routes: the store's own auth error is the
	// honest outcome, not a read of the ambient endpoint.
	bare := bucketStoreConfigSecretStatements(stores, "", "", "")
	if strings.Contains(bare[0], "KEY_ID") || strings.Contains(bare[0], "SESSION_TOKEN") || !strings.Contains(bare[0], "ENDPOINT 'minio:9000'") {
		t.Errorf("keyless statement wrong: %s", bare[0])
	}
}

// The precedence the whole design rests on, measured against the embedded
// DuckDB rather than assumed: for a path under the bucket, the bucket-scoped
// secret is chosen over bintrail_s3_chain (default scope s3://), and any
// other bucket keeps the chain secret. Both secrets use PROVIDER config here
// so the test needs httpfs only, never the aws extension download.
func TestBucketStoreScopedSecretWins_DuckDB(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := LoadHTTPFS(ctx, db); err != nil {
		t.Skip("httpfs unavailable (offline host)")
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, "CREATE OR REPLACE SECRET bintrail_s3_chain (TYPE s3, PROVIDER config, KEY_ID 'k', SECRET 's', ENDPOINT 'env-store:9000', URL_STYLE 'path', USE_SSL false)"); err != nil {
		t.Fatal(err)
	}
	stores := map[string]storage.BucketStore{"minio-b": store(t, "http://minio:9000", "", "")}
	for _, stmt := range bucketStoreConfigSecretStatements(stores, "k", "s", "") {
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	for path, want := range map[string]string{
		"s3://minio-b/x/y.parquet": bucketSecretName("minio-b"),
		"s3://other/x/y.parquet":   "bintrail_s3_chain",
		// A bucket whose name merely STARTS with the scoped one is not it:
		// the scope is a string prefix, which is why it ends in '/'.
		"s3://minio-bucket/x.parquet": "bintrail_s3_chain",
	} {
		var got string
		if err := conn.QueryRowContext(ctx, "SELECT name FROM which_secret(?, 's3')", path).Scan(&got); err != nil {
			t.Fatalf("which_secret(%s): %v", path, err)
		}
		if got != want {
			t.Errorf("which_secret(%s) = %s, want %s", path, got, want)
		}
	}
}

// The session setup applies the stores in both branches: with the escape
// hatch (no aws extension) as PROVIDER config secrets from the environment's
// keys, and, where the extension loads, as credential_chain secrets.
func TestEnableS3CredentialChain_bucketStores(t *testing.T) {
	withStores(t, map[string]storage.BucketStore{"minio-b": store(t, "http://minio:9000", "", "us-west-1")})
	t.Setenv("AWS_ACCESS_KEY_ID", "testdummykey")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "testdummysecret")
	t.Setenv("AWS_SESSION_TOKEN", "")
	t.Setenv(storage.EnvS3PathStyle, "")
	t.Setenv(storage.EnvS3Endpoint, "")
	ctx := context.Background()

	check := func(t *testing.T, db *sql.DB, wantProvider string) {
		t.Helper()
		var desc, provider string
		err := db.QueryRowContext(ctx, "SELECT secret_string, provider FROM duckdb_secrets() WHERE name = ?", bucketSecretName("minio-b")).Scan(&desc, &provider)
		if err != nil {
			t.Fatalf("scoped secret missing: %v", err)
		}
		if provider != wantProvider {
			t.Errorf("provider = %q, want %q", provider, wantProvider)
		}
		for _, want := range []string{"minio:9000", "us-west-1", "s3://minio-b/"} {
			if !strings.Contains(desc, want) {
				t.Errorf("secret does not carry %q: %s", want, desc)
			}
		}
		var got string
		if err := db.QueryRowContext(ctx, "SELECT name FROM which_secret('s3://minio-b/x.parquet', 's3')").Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != bucketSecretName("minio-b") {
			t.Errorf("the bucket resolves to %s, not its own secret", got)
		}
	}

	t.Run("escape hatch", func(t *testing.T) {
		t.Setenv("BINTRAIL_DUCKDB_NO_AWS_EXT", "1")
		db, err := sql.Open("duckdb", "")
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		if err := LoadHTTPFS(ctx, db); err != nil {
			t.Skip("httpfs unavailable (offline host)")
		}
		if err := EnableS3CredentialChain(ctx, db); err != nil {
			t.Fatal(err)
		}
		check(t, db, "config")
	})
	t.Run("aws extension", func(t *testing.T) {
		t.Setenv("BINTRAIL_DUCKDB_NO_AWS_EXT", "")
		db, err := sql.Open("duckdb", "")
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		if err := LoadHTTPFS(ctx, db); err != nil {
			t.Skip("httpfs unavailable (offline host)")
		}
		if err := EnableS3CredentialChain(ctx, db); err != nil {
			t.Fatal(err)
		}
		var loaded bool
		if err := db.QueryRow("SELECT loaded FROM duckdb_extensions() WHERE extension_name = 'aws'").Scan(&loaded); err != nil || !loaded {
			t.Skip("aws extension unavailable (offline host)")
		}
		check(t, db, "credential_chain")
	})
}

// With the aws extension loaded but no credentials ANYWHERE, DuckDB refuses
// the credential_chain secret at CREATE (measured: "Secret Validation
// Failure"). The general secret warns and moves on; the bucket must not lose
// its routing over the same condition: it gets an endpoint-only secret, so
// the read reaches the right store and fails there with that store's own
// authentication error instead of succeeding against the ambient one.
func TestEnableS3CredentialChain_bucketStoreWithoutCredentialsStillRoutes(t *testing.T) {
	withStores(t, map[string]storage.BucketStore{"minio-b": store(t, "http://minio:9000", "", "")})
	for k, v := range map[string]string{
		"AWS_CONFIG_FILE": "/nonexistent", "AWS_SHARED_CREDENTIALS_FILE": "/nonexistent", "AWS_PROFILE": "",
		"AWS_REGION": "", "AWS_DEFAULT_REGION": "", "AWS_EC2_METADATA_DISABLED": "true",
		"AWS_ACCESS_KEY_ID": "", "AWS_SECRET_ACCESS_KEY": "", "AWS_SESSION_TOKEN": "",
		"AWS_CONTAINER_CREDENTIALS_RELATIVE_URI": "", "AWS_WEB_IDENTITY_TOKEN_FILE": "",
		"BINTRAIL_DUCKDB_NO_AWS_EXT": "", storage.EnvS3Endpoint: "", storage.EnvS3PathStyle: "",
	} {
		t.Setenv(k, v)
	}
	ctx := context.Background()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := LoadHTTPFS(ctx, db); err != nil {
		t.Skip("httpfs unavailable (offline host)")
	}
	if err := EnableS3CredentialChain(ctx, db); err != nil {
		t.Fatalf("a session with no credentials must still be usable (the general secret's contract): %v", err)
	}
	var loaded bool
	if err := db.QueryRow("SELECT loaded FROM duckdb_extensions() WHERE extension_name = 'aws'").Scan(&loaded); err != nil || !loaded {
		t.Skip("aws extension unavailable (offline host)")
	}
	var provider string
	if err := db.QueryRowContext(ctx, "SELECT provider FROM duckdb_secrets() WHERE name = ?", bucketSecretName("minio-b")).Scan(&provider); err != nil {
		t.Fatalf("no scoped secret at all: the bucket's reads would go to the ambient endpoint: %v", err)
	}
	if provider != "config" {
		t.Errorf("provider = %q, want the endpoint-only fallback (config)", provider)
	}
	var got string
	if err := db.QueryRowContext(ctx, "SELECT name FROM which_secret('s3://minio-b/x.parquet', 's3')").Scan(&got); err != nil || got != bucketSecretName("minio-b") {
		t.Errorf("which_secret = %q, %v; want the bucket's own secret", got, err)
	}
}

// The hatch branch with stores and a session that never loaded httpfs: the
// secret cannot be created, and that is an error, not a session that reads
// the ambient endpoint for a bucket that lives elsewhere.
func TestEnableS3CredentialChain_bucketStoreNeedsHTTPFS(t *testing.T) {
	withStores(t, map[string]storage.BucketStore{"minio-b": store(t, "http://minio:9000", "", "")})
	t.Setenv("BINTRAIL_DUCKDB_NO_AWS_EXT", "1")
	t.Setenv(storage.EnvS3PathStyle, "")
	t.Setenv(storage.EnvS3Endpoint, "")
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// Autoload may still bring httpfs in for CREATE SECRET; either outcome
	// is acceptable EXCEPT a nil error with no secret.
	err = EnableS3CredentialChain(context.Background(), db)
	var n int
	_ = db.QueryRow("SELECT count(*) FROM duckdb_secrets() WHERE name = ?", bucketSecretName("minio-b")).Scan(&n)
	if err == nil && n == 0 {
		t.Fatal("no error and no scoped secret: the bucket's reads would go to the ambient endpoint")
	}
}

// One bucket whose credential-chain secret fails falls back alone: the
// buckets whose secret was created keep it.
func TestApplyBucketStoreSecrets_fallsBackPerBucket(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "AKIAEXAMPLE")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "sup3rs3cret")
	t.Setenv("AWS_SESSION_TOKEN", "")
	stores := map[string]storage.BucketStore{
		"good": store(t, "http://minio:9000", "", ""),
		"bad":  store(t, "https://s3.wasabisys.com", "", "eu-central-1"),
	}
	var ran []string
	exec := func(_ context.Context, stmt string) error {
		if strings.Contains(stmt, "credential_chain") && strings.Contains(stmt, "'s3://bad/'") {
			return errors.New("Secret Validation Failure")
		}
		ran = append(ran, stmt)
		return nil
	}
	if err := applyBucketStoreSecrets(context.Background(), exec, stores, true); err != nil {
		t.Fatal(err)
	}
	got := strings.Join(ran, "\n")
	if len(ran) != 2 {
		t.Fatalf("ran %d statements, want one per bucket:\n%s", len(ran), got)
	}
	for _, want := range []string{
		"PROVIDER credential_chain, SCOPE 's3://good/'",
		"PROVIDER config, KEY_ID 'AKIAEXAMPLE', SECRET 'sup3rs3cret', SCOPE 's3://bad/'",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("no statement carries %q:\n%s", want, got)
		}
	}
}

// DuckDB echoes a statement it cannot parse, and the fallback statement
// carries the environment's keys: the error must name the bucket, never them.
func TestApplyBucketStoreSecrets_errorNeverCarriesTheKeys(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "AKIAEXAMPLE")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "sup3rs3cret")
	t.Setenv("AWS_SESSION_TOKEN", "tok3n")
	stores := map[string]storage.BucketStore{"b": store(t, "http://minio:9000", "", "")}
	exec := func(_ context.Context, stmt string) error {
		return fmt.Errorf("Parser Error: syntax error at or near %q", stmt)
	}
	err := applyBucketStoreSecrets(context.Background(), exec, stores, true)
	if err == nil {
		t.Fatal("a fallback that cannot be created must fail the session")
	}
	t.Log(err)
	for _, leak := range []string{"AKIAEXAMPLE", "sup3rs3cret", "tok3n"} {
		if strings.Contains(err.Error(), leak) {
			t.Errorf("the error carries %s: %v", leak, err)
		}
	}
	if !strings.Contains(err.Error(), `"b"`) {
		t.Errorf("the error does not name the bucket: %v", err)
	}

	// DuckDB can echo a WINDOW of the statement, cutting a key in the middle,
	// which no whole-value replacement recognizes.
	fragment := func(_ context.Context, stmt string) error {
		return errors.New("Parser Error: syntax error at end of input\nLINE 1: ...KEY_ID 'AKIAEX', SECRET 'sup3rs")
	}
	err = applyBucketStoreSecrets(context.Background(), fragment, stores, true)
	if err == nil || strings.Contains(err.Error(), "sup3rs") || strings.Contains(err.Error(), "AKIAEX") {
		t.Errorf("a truncated echo of the statement reaches the error: %v", err)
	}
}

// Measured: a scoped secret with no REGION signs with whatever s3_region the
// session has, while the SDK signs the same store's uploads as us-east-1.
func TestBucketStoreSecret_signsLikeTheSDK_DuckDB(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := LoadHTTPFS(ctx, db); err != nil {
		t.Skip("httpfs unavailable (offline host)")
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, "SET GLOBAL s3_region='ap-south-1'"); err != nil {
		t.Fatal(err)
	}
	stores := map[string]storage.BucketStore{"minio-b": store(t, "http://minio:9000", "", "")}
	for _, stmt := range bucketStoreConfigSecretStatements(stores, "k", "s", "") {
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			t.Fatal(err)
		}
	}
	var desc string
	if err := conn.QueryRowContext(ctx, "SELECT secret_string FROM duckdb_secrets() WHERE name = ?", bucketSecretName("minio-b")).Scan(&desc); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(desc, "region=us-east-1") {
		t.Errorf("the scoped secret does not sign as us-east-1, the region the same store's uploads sign with: %s", desc)
	}
}
