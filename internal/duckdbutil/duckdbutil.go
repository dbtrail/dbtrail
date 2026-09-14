// Package duckdbutil holds small helpers shared by the DuckDB sessions
// bintrail opens (baseline reads, snapshot queries, S3 footer probes).
package duckdbutil

import (
	"context"
	"database/sql"
	"fmt"
	"hash/fnv"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/dbtrail/dbtrail/internal/storage"
)

// execer is the subset of *sql.DB / *sql.Conn / *sql.Tx that LoadHTTPFS needs,
// so callers can load the extension on whichever handle they already hold.
type execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// LoadHTTPFS installs and loads the DuckDB httpfs extension on db, first making
// sure $HOME points at a writable directory. DuckDB extracts and caches
// extensions under $HOME/.duckdb; a process running as a homeless user — a
// container user created with `useradd --no-create-home`, so $HOME points at a
// directory that does not exist — otherwise fails INSTALL with "IO Error: Can't
// find the home directory at '/home/<user>'". Callers wrap the returned error
// with their own context.
func LoadHTTPFS(ctx context.Context, db execer) error {
	ensureWritableHome()
	_, err := db.ExecContext(ctx, "INSTALL httpfs; LOAD httpfs;")
	return err
}

// ensureWritableHome points $HOME at an existing, writable directory when it is
// not already, so DuckDB can install AND autoload its extensions on every pooled
// connection. The env is the only lever that reaches all of them: a session
// `SET home_directory` is connection-scoped and — critically — only covers an
// explicit INSTALL, NOT the autoload that a `CREATE SECRET` / `parquet_scan`
// triggers on its own connection (that autoload resolves the home from $HOME, so
// it would still fail with "Can't find the home directory"). No-op when $HOME
// already resolves to an existing directory, so a valid setup is never touched —
// only an already-broken $HOME is changed. Best-effort: if no writable directory
// can be provisioned at all, it logs at WARN and leaves $HOME as-is so DuckDB
// surfaces its own error rather than this masking it.
func ensureWritableHome() {
	if h, err := os.UserHomeDir(); err == nil && h != "" {
		if fi, statErr := os.Stat(h); statErr == nil && fi.IsDir() {
			return // $HOME is usable — leave it alone.
		}
	}
	dir := filepath.Join(os.TempDir(), "bintrail-duckdb")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		// $TMPDIR itself is broken/unwritable. Don't blindly trust os.TempDir()
		// (it returns $TMPDIR verbatim with no existence check). MkdirTemp creates
		// AND validates a writable dir; if even that fails the host has no usable
		// temp space, so log loudly and leave $HOME untouched.
		td, tmpErr := os.MkdirTemp("", "bintrail-duckdb-")
		if tmpErr != nil {
			slog.Warn("duckdb: could not provision a writable home directory for extension install; S3 reads may fail",
				"tried", dir, "mkdir_error", err, "mkdtemp_error", tmpErr)
			return
		}
		dir = td
	}
	if err := os.Setenv("HOME", dir); err != nil {
		slog.Warn("duckdb: could not set HOME for extension install; S3 reads may fail", "dir", dir, "error", err)
	}
}

// EnableS3CredentialChain gives a DuckDB session AWS-SDK credentials for
// s3:// access: the aws extension's credential_chain provider resolves the
// AWS SDK default chain — env keys, config profiles, and EC2/ECS/EKS IAM
// roles (SSO-session profiles have open gaps upstream: duckdb-aws#125).
// Without it, plain httpfs resolves static env keys at best, so role-only
// environments failed exactly on the DuckDB read paths while every
// SDK-backed upload worked (#459).
//
// Best-effort BY DESIGN, but never silent where it matters: when the aws
// extension cannot install/load (offline hosts — it is cached in ~/.duckdb,
// per DuckDB version and platform, after one connected run) the session
// proceeds with plain httpfs env-key resolution. That fallback is logged at
// debug when AWS env keys exist (it genuinely works) and at WARN when they
// don't — the upcoming S3 read is then doomed to a generic 403 that never
// mentions the chain, so this warn is the only diagnostic the operator gets.
// A failed CREATE SECRET (the chain resolved no usable credentials — broken
// profile, expired SSO, unreachable IMDS) always warns: the SDK upload paths
// report that state loudly and the read paths must not bury it.
//
// BINTRAIL_DUCKDB_NO_AWS_EXT=1 skips the aws extension and the secret, NOT
// the endpoint routing: that is applied as DuckDB global settings (SET
// GLOBAL, scoped to the instance), and an unrouted read does not fail, it
// reaches AWS. Escape hatch for
// proxies that BLACKHOLE (rather than refuse) the DuckDB extension registry:
// there the INSTALL attempt can stall for minutes, ignores context
// cancellation, and recurs every session because failures are never cached.
//
// The secret resolves credentials at CREATE time, not per request — fine
// here because every caller opens a short-lived session per operation; do
// not reuse this on long-lived pooled sessions under expiring roles.
//
// Only CREDENTIALS are best-effort. A non-nil error means the session could
// not be pointed at the right store — a bad BINTRAIL_S3_ENDPOINT, or a SET
// that would not apply — and the caller must not read: an unrouted session
// succeeds against AWS instead of failing.
//
// Call it after `INSTALL httpfs; LOAD httpfs;` on sessions that will touch
// s3:// paths.
func EnableS3CredentialChain(ctx context.Context, db *sql.DB) error {
	return EnableS3CredentialChainRegion(ctx, db, "")
}

// EnableS3CredentialChainRegion is EnableS3CredentialChain that also pins the
// secret's REGION when region is non-empty. DuckDB's secrets manager can take
// precedence over the session `SET s3_region` for matching paths, and a
// credential_chain secret otherwise resolves region from the AWS SDK config
// (e.g. AWS_REGION) — not the bucket's actual location. Putting the detected
// bucket region IN the secret pins it so a cross-region read avoids a
// 301/PermanentRedirect regardless of that precedence (#511). region "" reproduces
// EnableS3CredentialChain exactly (no REGION clause), so existing same-region
// callers are unchanged.
func EnableS3CredentialChainRegion(ctx context.Context, db *sql.DB, region string) error {
	// ROUTING FIRST, and outside every gate below (#1454). Which store a read
	// reaches is not best-effort: an unrouted session does not fail, it
	// succeeds against AWS with the ambient credentials, and an operator whose
	// bucket name exists in both places gets someone else's data or a
	// "missing" baseline that is sitting in their store. The gates below can
	// all skip the secret — the escape hatch is set, the extension will not
	// install on an air-gapped host, the chain resolves nothing — and each of
	// those is a plausible MinIO deployment.
	ep, err := storage.S3EndpointFromEnv()
	if err != nil {
		return err
	}
	if err := applyS3Routing(ctx, db, region, ep); err != nil {
		return err
	}

	stores := storage.BucketStores()
	if os.Getenv("BINTRAIL_DUCKDB_NO_AWS_EXT") != "" {
		return applyBucketStoreSecrets(ctx, execOn(db), stores, false)
	}
	ensureWritableHome()
	if _, err := db.ExecContext(ctx, "INSTALL aws; LOAD aws;"); err != nil {
		if os.Getenv("AWS_ACCESS_KEY_ID") != "" {
			slog.Debug("duckdb: aws extension unavailable; S3 reads fall back to env-key resolution",
				"error", err)
		} else {
			slog.Warn("duckdb: aws extension unavailable and no AWS env keys set — profile/role credentials will NOT apply to this S3 read; expect an authentication failure",
				"error", err)
		}
		return applyBucketStoreSecrets(ctx, execOn(db), stores, false)
	}
	// The secret repeats the endpoint the settings applied above already carry.
	// DuckDB's secrets manager can take precedence over a SET for matching
	// paths, so a secret that named only credentials would put the endpoint
	// back to AWS for exactly the paths it matches.
	secret := "CREATE OR REPLACE SECRET bintrail_s3_chain (TYPE s3, PROVIDER credential_chain" +
		S3SecretClauses(region, ep) + ")"
	// ensureWritableHome (above) has already pointed $HOME at a writable dir, so
	// the httpfs autoload this CREATE SECRET triggers resolves its home correctly
	// even under a homeless user.
	if _, err := db.ExecContext(ctx, secret); err != nil {
		// Credentials stay best-effort: a read that cannot be signed fails at
		// the read, with the store's own error. Routing was applied above and
		// is unaffected.
		slog.Warn("duckdb: AWS credential chain resolved no usable credentials for S3 reads",
			"error", err)
	}
	return applyBucketStoreSecrets(ctx, execOn(db), stores, true)
}

// execOn adapts a *sql.DB to the statement runner applyBucketStoreSecrets
// takes (a seam, so a per-bucket failure can be tested without a DuckDB that
// fails for one bucket and not another).
func execOn(db *sql.DB) func(context.Context, string) error {
	return func(ctx context.Context, stmt string) error {
		_, err := db.ExecContext(ctx, stmt)
		return err
	}
}

// applyBucketStoreSecrets creates one secret per bucket with its own store
// (#1575), bucket by bucket. With tryChain each bucket first gets a
// credential_chain secret; a bucket whose chain secret fails (DuckDB refuses
// one that resolves no credentials, at CREATE) falls back ALONE to an
// endpoint-only PROVIDER config secret carrying whatever keys the environment
// holds, so the buckets that did get a chain secret keep it. Without tryChain
// (no aws extension) every bucket gets the config secret directly.
//
// A bucket whose store has keys of its own gets a PROVIDER config secret with
// exactly those keys, and nothing else: no credential_chain attempt first (it
// would sign with the daemon's credentials and win), no environment keys, no
// session token (one from the environment belongs to the environment's keys).
//
// Credentials are best-effort, routing is not: a bucket whose fallback cannot
// be created fails the session, because its reads would otherwise go to the
// ambient endpoint (AWS, for a MinIO bucket). The error names the bucket and
// never carries DuckDB's message when the statement holds keys, the store's
// or the environment's.
func applyBucketStoreSecrets(ctx context.Context, exec func(context.Context, string) error, stores map[string]storage.BucketStore, tryChain bool) error {
	keyID, secret, token := os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY"), os.Getenv("AWS_SESSION_TOKEN")
	for _, b := range sortedBuckets(stores) {
		st := stores[b]
		if st.HasKeys() {
			if err := exec(ctx, bucketStoreSecret(b, st, configProvider(st.AccessKeyID, st.SecretKey, ""))); err != nil {
				return fmt.Errorf("route DuckDB S3 reads for bucket %q to its own store with its own keys (is httpfs loaded?): %s",
					b, withholdIfKeyed(err, "the S3 store's keys", st.AccessKeyID, st.SecretKey))
			}
			continue
		}
		if tryChain {
			err := exec(ctx, bucketStoreSecret(b, st, chainProvider))
			if err == nil {
				continue
			}
			slog.Warn("duckdb: AWS credential chain resolved no usable credentials for a bucket with its own store; routing it by endpoint alone",
				"bucket", b, "error", err)
		}
		if err := exec(ctx, bucketStoreSecret(b, st, configProvider(keyID, secret, token))); err != nil {
			return fmt.Errorf("route DuckDB S3 reads for bucket %q to its own store (is httpfs loaded?): %s",
				b, withholdIfKeyed(err, "AWS keys from the environment", keyID, secret, token))
		}
	}
	return nil
}

// withholdIfKeyed renders the error of a statement that may carry keys; source
// says whose, for the message. Key values are removed wherever they appear whole; and a
// message that echoes the statement at all is withheld, because DuckDB can
// echo a WINDOW of it that cuts a key in the middle, where no replacement of
// the whole value finds it. Anything else passes: the usual failure (httpfs
// not loaded) names no key and is the one diagnostic the operator has.
func withholdIfKeyed(err error, source string, keys ...string) string {
	msg := err.Error()
	keyed := false
	for _, k := range keys {
		if k == "" {
			continue
		}
		keyed = true
		msg = strings.ReplaceAll(msg, strings.ReplaceAll(k, "'", "''"), "<redacted>")
		msg = strings.ReplaceAll(msg, k, "<redacted>")
	}
	if !keyed {
		return err.Error()
	}
	for _, marker := range []string{"LINE ", "KEY_ID", "SECRET", "SESSION_TOKEN", "SCOPE", "PROVIDER"} {
		if strings.Contains(msg, marker) {
			return "DuckDB's message is withheld because it echoes the statement, which carries " + source
		}
	}
	// Nothing of the statement is echoed: the message is safe, and it is the diagnostic.
	return msg
}

const chainProvider = ", PROVIDER credential_chain"

// configProvider is PROVIDER config with whichever environment keys are set.
func configProvider(keyID, secret, token string) string {
	provider := ", PROVIDER config"
	if keyID != "" {
		provider += ", KEY_ID " + sqlQuote(keyID)
	}
	if secret != "" {
		provider += ", SECRET " + sqlQuote(secret)
	}
	if token != "" {
		provider += ", SESSION_TOKEN " + sqlQuote(token)
	}
	return provider
}

// BucketStoreSecretStatements renders one credential_chain secret per bucket
// with its own store, in bucket order: what views.sql carries (never keys).
func BucketStoreSecretStatements(stores map[string]storage.BucketStore) []string {
	return renderBucketStoreSecrets(stores, chainProvider)
}

// bucketStoreConfigSecretStatements is the same set with PROVIDER config and
// the given keys: the endpoint-only fallback.
func bucketStoreConfigSecretStatements(stores map[string]storage.BucketStore, keyID, secret, token string) []string {
	return renderBucketStoreSecrets(stores, configProvider(keyID, secret, token))
}

func renderBucketStoreSecrets(stores map[string]storage.BucketStore, provider string) []string {
	if len(stores) == 0 {
		return nil
	}
	stmts := make([]string, 0, len(stores))
	for _, b := range sortedBuckets(stores) {
		stmts = append(stmts, bucketStoreSecret(b, stores[b], provider))
	}
	return stmts
}

func sortedBuckets(stores map[string]storage.BucketStore) []string {
	buckets := make([]string, 0, len(stores))
	for b := range stores {
		buckets = append(buckets, b)
	}
	sort.Strings(buckets)
	return buckets
}

// bucketStoreSecret renders one bucket's secret. The SCOPE ends in "/": the
// match is a string prefix, and s3://prod would otherwise capture every read
// of s3://prod-archive. REGION is the store's SigningRegion, never left out:
// a secret without one signs with the session's s3_region, not the us-east-1
// the SDK signs the same store's uploads with.
func bucketStoreSecret(bucket string, st storage.BucketStore, provider string) string {
	return "CREATE OR REPLACE SECRET " + bucketSecretName(bucket) + " (TYPE s3" + provider +
		", SCOPE " + sqlQuote("s3://"+bucket+"/") + S3SecretClauses(st.SigningRegion(), st.Endpoint) + ")"
}

// bucketSecretName is a stable identifier for a bucket's secret: the bucket
// with everything but [a-z0-9] folded to '_' (bucket names may carry dots
// and hyphens, identifiers may not), plus a short hash so "a-b" and "a.b"
// do not collide.
func bucketSecretName(bucket string) string {
	var b strings.Builder
	b.WriteString("bintrail_s3_bucket_")
	for _, r := range bucket {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	h := fnv.New32a()
	h.Write([]byte(bucket))
	fmt.Fprintf(&b, "_%08x", h.Sum32())
	return b.String()
}

// applyS3Routing pins WHERE this session's s3:// requests go, as session
// settings rather than as part of the secret, because settings need httpfs
// alone: the aws extension is a download that an air-gapped or proxied host
// does not get, and that host is a likely S3-compatible-store deployment.
//
// With no endpoint configured this does nothing at all, so an AWS session is
// left exactly as it was: httpfs's own defaults are already correct there.
// (The enclosing function can still refuse earlier, for a malformed
// BINTRAIL_S3_PATH_STYLE, which is a typo worth failing on either way.)
func applyS3Routing(ctx context.Context, db *sql.DB, region string, ep storage.S3Endpoint) error {
	if !ep.Set() {
		return nil
	}
	for _, stmt := range S3SettingStatements(region, ep) {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			// Refusing here is the point: continuing would read AWS.
			return fmt.Errorf("route DuckDB S3 reads to %s (is httpfs loaded?): %w", ep.URL, err)
		}
	}
	return nil
}

// S3SettingStatements renders the settings that decide WHERE a DuckDB
// instance's s3:// requests go. It is the settings half of what
// S3SecretClauses does for the secret, shared for the same reason: the
// downloadable views.sql must configure exactly what this process configures,
// and a hand-copied second list drifts (it did: s3_region was missing from
// the file's copy, so a cross-region store signed against an empty region
// whenever the reader's secret did not apply).
//
// SET GLOBAL, not SET: a plain SET binds to the CONNECTION that ran it, so on
// a *sql.DB pool the next connection reads with s3_endpoint unset and goes to
// AWS (measured: a sibling connection sees "" after SET, and the configured
// host after SET GLOBAL). Most callers use one connection, which is exactly
// why this would have stayed invisible.
//
// GLOBAL is not process-wide: it is scoped to the DuckDB INSTANCE, and every
// caller opens its own via sql.Open (measured: two handles hold two different
// s3_endpoint values at once). Two sessions in one process, a console daemon
// serving several servers among them, do not interfere.
//
// Returns nil when no endpoint is configured: httpfs's own defaults are
// already correct for AWS, and touching them would be a new failure mode for
// every existing user. Statements carry no trailing semicolon.
func S3SettingStatements(region string, ep storage.S3Endpoint) []string {
	if !ep.Set() {
		return nil
	}
	stmts := []string{
		"SET GLOBAL s3_endpoint=" + sqlQuote(ep.Host()),
		"SET GLOBAL s3_url_style=" + sqlQuote(ep.URLStyle()),
		fmt.Sprintf("SET GLOBAL s3_use_ssl=%t", ep.UseSSL()),
	}
	if region != "" {
		stmts = append(stmts, "SET GLOBAL s3_region="+sqlQuote(region))
	}
	return stmts
}

// S3SecretClauses renders the optional clauses of bintrail's S3 secret, each
// with its leading ", ": REGION when pinned, and ENDPOINT / URL_STYLE /
// USE_SSL when a custom endpoint is configured. Shared with the downloadable
// views.sql so a file generated here reads the same store this process does.
func S3SecretClauses(region string, ep storage.S3Endpoint) string {
	var b strings.Builder
	if region != "" {
		b.WriteString(", REGION " + sqlQuote(region))
	}
	if ep.Set() {
		b.WriteString(", ENDPOINT " + sqlQuote(ep.Host()))
		style := "vhost"
		if ep.PathStyle {
			style = "path"
		}
		b.WriteString(", URL_STYLE " + sqlQuote(style))
		if ep.UseSSL() {
			b.WriteString(", USE_SSL true")
		} else {
			b.WriteString(", USE_SSL false")
		}
	}
	return b.String()
}

func sqlQuote(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }
