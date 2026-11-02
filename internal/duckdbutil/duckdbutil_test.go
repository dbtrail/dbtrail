package duckdbutil

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
)

// TestEnsureWritableHome: a usable $HOME is left untouched (the normal case),
// and a broken $HOME (the homeless `useradd --no-create-home` case) is pointed
// at an existing, writable directory so DuckDB's extension install/autoload can
// find a home.
func TestEnsureWritableHome(t *testing.T) {
	good := t.TempDir()
	t.Setenv("HOME", good)
	ensureWritableHome()
	if h := os.Getenv("HOME"); h != good {
		t.Errorf("a usable $HOME must be left untouched, got %q want %q", h, good)
	}

	t.Setenv("HOME", filepath.Join(t.TempDir(), "does-not-exist"))
	ensureWritableHome()
	h := os.Getenv("HOME")
	fi, err := os.Stat(h)
	if err != nil || !fi.IsDir() {
		t.Errorf("a broken $HOME must be repointed at an existing directory, got %q (stat err %v)", h, err)
	}
}

// TestLoadHTTPFSHomelessUser is the regression guard for the reported bug: a
// process whose $HOME points at a non-existent directory (a container user
// created with `useradd --no-create-home`) failed httpfs INSTALL with "Can't
// find the home directory at '/home/...'". With the home_directory pin,
// LoadHTTPFS must never surface that error — it may still fail on an offline
// host (extension download), but never on the home directory — and the session
// must stay usable.
func TestLoadHTTPFSHomelessUser(t *testing.T) {
	t.Setenv("HOME", filepath.Join(t.TempDir(), "no-home-here"))

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := LoadHTTPFS(context.Background(), db); err != nil &&
		strings.Contains(err.Error(), "home directory") {
		t.Fatalf("LoadHTTPFS must not fail on the home directory for a homeless user: %v", err)
	}

	var one int
	if err := db.QueryRow("SELECT 1").Scan(&one); err != nil || one != 1 {
		t.Fatalf("session unusable after LoadHTTPFS: %v (got %d)", err, one)
	}
}

// TestEnableS3CredentialChainHomelessUser closes the asymmetry the httpfs test
// alone leaves open: the aws-extension INSTALL is just as home-directory
// sensitive as httpfs, and EnableS3CredentialChainRegion got the same pin. Under
// a homeless $HOME it must still load (or best-effort skip) without breaking the
// session, and — when the extension is available — actually create the chain
// secret, proving the pin let `INSTALL aws` succeed and not just no-op.
func TestEnableS3CredentialChainHomelessUser(t *testing.T) {
	t.Setenv("HOME", filepath.Join(t.TempDir(), "no-home-here"))
	t.Setenv("AWS_ACCESS_KEY_ID", "AKIATESTDUMMY0000000")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "testdummysecret")
	t.Setenv("BINTRAIL_DUCKDB_NO_AWS_EXT", "")

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	EnableS3CredentialChain(context.Background(), db) // must not panic/wedge

	var one int
	if err := db.QueryRow("SELECT 1").Scan(&one); err != nil || one != 1 {
		t.Fatalf("session unusable after EnableS3CredentialChain under homeless $HOME: %v (got %d)", err, one)
	}

	var loaded bool
	if err := db.QueryRow("SELECT loaded FROM duckdb_extensions() WHERE extension_name = 'aws'").Scan(&loaded); err != nil || !loaded {
		t.Skip("aws extension unavailable (offline host) — homeless pin not exercised")
	}
	var n int
	if err := db.QueryRow("SELECT count(*) FROM duckdb_secrets() WHERE name = 'bintrail_s3_chain'").Scan(&n); err != nil || n != 1 {
		t.Fatalf("chain secret missing under homeless $HOME — the aws INSTALL did not get the home-dir pin: n=%d err=%v", n, err)
	}
}

// TestEnsureWritableHome_brokenTmpdir exercises the last-resort branch: when both
// the stable temp dir and MkdirTemp fail to provision a writable home (here
// $TMPDIR is a regular file, so neither can create a subdirectory),
// ensureWritableHome must leave $HOME untouched — never repoint it at a directory
// that doesn't exist, which would just resurface the original error — and never
// panic.
func TestEnsureWritableHome_brokenTmpdir(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing")
	t.Setenv("HOME", missing)
	// Point TMPDIR at a file, not a directory, so MkdirAll AND MkdirTemp (both
	// rooted at os.TempDir()) fail.
	notADir := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(notADir, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TMPDIR", notADir)

	ensureWritableHome()
	if h := os.Getenv("HOME"); h != missing {
		t.Errorf("with no writable dir provisionable, $HOME must be left as-is; got %q want %q", h, missing)
	}
}

// TestEnableS3CredentialChainKeepsSessionUsable: the helper is best-effort —
// whether the aws extension installs (network) or not (offline), it must
// never break the session it was called on.
func TestEnableS3CredentialChainKeepsSessionUsable(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	EnableS3CredentialChain(context.Background(), db)

	var one int
	if err := db.QueryRowContext(context.Background(), "SELECT 1").Scan(&one); err != nil || one != 1 {
		t.Fatalf("session unusable after EnableS3CredentialChain: %v (got %d)", err, one)
	}
}

// TestEnableS3CredentialChainCreatesSecret pins the success path: when the
// aws extension is available, the chain secret must actually exist — without
// this, renaming the secret, typoing the SQL, or deleting the CREATE entirely
// keeps the suite green. Dummy env creds make the chain always resolvable
// (creds-less CI would otherwise hit the legitimate validation failure).
func TestEnableS3CredentialChainCreatesSecret(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "AKIATESTDUMMY0000000")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "testdummysecret")
	t.Setenv("BINTRAIL_DUCKDB_NO_AWS_EXT", "")

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	EnableS3CredentialChain(context.Background(), db)

	var loaded bool
	if err := db.QueryRow("SELECT loaded FROM duckdb_extensions() WHERE extension_name = 'aws'").Scan(&loaded); err != nil || !loaded {
		t.Skip("aws extension unavailable (offline host) — the best-effort fallback branch applies")
	}
	var n int
	if err := db.QueryRow("SELECT count(*) FROM duckdb_secrets() WHERE name = 'bintrail_s3_chain'").Scan(&n); err != nil || n != 1 {
		t.Fatalf("chain secret missing after a successful extension load: n=%d err=%v", n, err)
	}
}

// TestEnableS3CredentialChainEscapeHatch: BINTRAIL_DUCKDB_NO_AWS_EXT must
// skip the setup entirely (no INSTALL attempt that could stall behind a
// blackholing proxy) — no secret, session untouched.
func TestEnableS3CredentialChainEscapeHatch(t *testing.T) {
	t.Setenv("BINTRAIL_DUCKDB_NO_AWS_EXT", "1")

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	EnableS3CredentialChain(context.Background(), db)

	var n int
	if err := db.QueryRow("SELECT count(*) FROM duckdb_secrets() WHERE name = 'bintrail_s3_chain'").Scan(&n); err != nil || n != 0 {
		t.Fatalf("escape hatch did not skip the secret setup: n=%d err=%v", n, err)
	}
}

// TestEnableS3CredentialChainRegion: pinning a region must keep the session
// usable and, when the aws extension is available, still create the chain
// secret (now carrying REGION). The region travels with the secret so a
// cross-region httpfs read doesn't 301 regardless of secret-vs-SET precedence
// (#511). Dummy env creds make the chain resolvable on creds-less CI.
func TestEnableS3CredentialChainRegion(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "AKIATESTDUMMY0000000")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "testdummysecret")
	t.Setenv("BINTRAIL_DUCKDB_NO_AWS_EXT", "")

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	EnableS3CredentialChainRegion(context.Background(), db, "eu-west-1")

	// Session must remain usable whether or not the aws extension loaded.
	var one int
	if err := db.QueryRow("SELECT 1").Scan(&one); err != nil || one != 1 {
		t.Fatalf("session unusable after EnableS3CredentialChainRegion: %v (got %d)", err, one)
	}

	var loaded bool
	if err := db.QueryRow("SELECT loaded FROM duckdb_extensions() WHERE extension_name = 'aws'").Scan(&loaded); err != nil || !loaded {
		t.Skip("aws extension unavailable (offline host) — region pin not exercised")
	}
	// Assert the region actually TRAVELS WITH the secret — not just that a
	// secret exists (which TestEnableS3CredentialChainCreatesSecret already
	// covers). secret_string carries the region in plaintext, so this pins the
	// commit's actual change: stripping the REGION clause would fail here.
	var secretStr string
	if err := db.QueryRow("SELECT secret_string FROM duckdb_secrets() WHERE name = 'bintrail_s3_chain'").Scan(&secretStr); err != nil {
		t.Fatalf("region-pinned chain secret missing after a successful extension load: %v", err)
	}
	if !strings.Contains(secretStr, "region=eu-west-1") {
		t.Fatalf("chain secret does not carry the pinned region; got secret_string=%q", secretStr)
	}
}

// TestDuckDBEngineOnPinnedLine pins the engine to the DuckDB 1.5 line. The
// rationale lives in go.mod above the duckdb-go require; this is the part
// that can actually fail a build, because a comment cannot: `go get -u`
// rewrites the version and leaves the surrounding comment byte-identical, so
// the pin would otherwise be an argument the repo makes against itself.
//
// It also catches a bindings-only bump, which moves the engine without
// touching the duckdb-go line at all.
//
// This test SHOULD fail the day we deliberately move to 2.x. That is the
// point: the move becomes an explicit edit here rather than a silent side
// effect of a dependency update.
func TestDuckDBEngineOnPinnedLine(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open in-memory duckdb: %v", err)
	}
	defer db.Close()

	var got string
	if err := db.QueryRow("SELECT version()").Scan(&got); err != nil {
		t.Fatalf("SELECT version(): %v", err)
	}
	const wantLine = "v1.5."
	if !strings.HasPrefix(got, wantLine) {
		t.Fatalf("DuckDB engine is %s, want the %sx line.\n"+
			"If this is a deliberate move, update this test AND the pin "+
			"rationale in go.mod together — and run the off-line pre-flight "+
			"(real-S3 --ultrafast scan, cross-region read, IAM role without "+
			"GetBucketLocation).", got, wantLine)
	}
}
