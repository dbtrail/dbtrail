package cli

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/archive"
	"github.com/dbtrail/dbtrail/internal/duckdbutil"
)

// TestReconcileDryRunErrFlagsDeepUnverified pins #469: a --deep dry-run with
// a zero-drift report but failed footer probes must still exit non-zero, so a
// "green" cron run can't hide objects it was asked to verify.
func TestReconcileDryRunErrFlagsDeepUnverified(t *testing.T) {
	// Zero-drift report: in sync, no actions. Before #469 this returned nil
	// even though some S3 objects could not be deep-verified.
	rep := &archive.Report{InSync: 3}

	if err := reconcileDryRunErr(rep, 0); err != nil {
		t.Fatalf("clean report with no footer failures should exit zero, got: %v", err)
	}

	err := reconcileDryRunErr(rep, 2)
	if err == nil {
		t.Fatal("dry-run must exit non-zero when --deep footer probes failed, got nil")
	}
	if !strings.Contains(err.Error(), "could not be deep-verified") {
		t.Fatalf("error must name the deep-verify failure, got: %v", err)
	}
}

// TestReconcileExecuteErrFlagsDeepUnverified is the execute-mode (--repair/
// --prune) sibling of the dry-run test: --deep/--repair/--prune are
// independent flags, so a `reconcile --deep --repair` run with no remaining
// drift but failed footer probes must STILL exit non-zero — --repair cannot
// fix a footer it cannot read, and a scheduled auto-remediation keys on the
// exit code. Without the shared deepUnverified guard this path returned nil
// (silent exit 0), reintroducing #469 in execute mode.
func TestReconcileExecuteErrFlagsDeepUnverified(t *testing.T) {
	// Zero unaddressed drift (in sync, no pending actions), --repair --prune.
	rep := &archive.Report{InSync: 3}

	if err := reconcileExecuteErr(rep, 0, true, true); err != nil {
		t.Fatalf("clean report with no footer failures should exit zero, got: %v", err)
	}

	err := reconcileExecuteErr(rep, 2, true, true)
	if err == nil {
		t.Fatal("execute mode must exit non-zero when --deep footer probes failed, got nil")
	}
	if !strings.Contains(err.Error(), "could not be deep-verified") {
		t.Fatalf("error must name the deep-verify failure, got: %v", err)
	}

	// The pre-existing drift rule is preserved: unaddressed drift still wins,
	// and a deep failure alongside it never lets the run exit 0.
	drifted := &archive.Report{Inserts: 1}
	if err := reconcileExecuteErr(drifted, 0, false, false); err == nil {
		t.Fatal("pending insert without --repair must exit non-zero")
	}
}

// TestReconcileReportJSONIncludesDeepUnverified pins #469: the footer-probe
// failure count appears in --format json so a cron consumer can see it.
func TestReconcileReportJSONIncludesDeepUnverified(t *testing.T) {
	rep := &archive.Report{InSync: 1}

	var buf bytes.Buffer
	if err := writeReconcileReport(&buf, "json", rep, 4, 0, nil, false, false); err != nil {
		t.Fatalf("writeReconcileReport: %v", err)
	}

	var got reconcileReportJSON
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal report JSON: %v\noutput: %s", err, buf.String())
	}
	if got.DeepUnverified != 4 {
		t.Fatalf("deep_unverified = %d, want 4 (raw: %s)", got.DeepUnverified, buf.String())
	}
	// The field must be present (not omitted) even when zero — stable shape
	// for the cron consumer.
	if !strings.Contains(buf.String(), `"deep_unverified"`) {
		t.Fatalf("JSON output missing deep_unverified field: %s", buf.String())
	}
}

// TestReconcileReportTextSurfacesDeepUnverified pins the text-mode surface of
// #469: a non-zero count produces a WARNING line; zero stays quiet.
func TestReconcileReportTextSurfacesDeepUnverified(t *testing.T) {
	rep := &archive.Report{InSync: 1}

	var loud bytes.Buffer
	if err := writeReconcileReport(&loud, "text", rep, 3, 0, nil, false, false); err != nil {
		t.Fatalf("writeReconcileReport: %v", err)
	}
	if !strings.Contains(loud.String(), "could not be deep-verified") {
		t.Fatalf("text output must warn on deep-unverified files: %s", loud.String())
	}

	var quiet bytes.Buffer
	if err := writeReconcileReport(&quiet, "text", rep, 0, 0, nil, false, false); err != nil {
		t.Fatalf("writeReconcileReport: %v", err)
	}
	if strings.Contains(quiet.String(), "deep-verified") {
		t.Fatalf("healthy run must not mention deep-verify: %s", quiet.String())
	}
}

// TestReconcileWiringFromReportDeepUnverified pins the command-layer wiring of
// the decision-layer count: runArchiveReconcile sources deepUnverified from
// report.DeepUnverified (the dual-backend / picked-Invalid signal), so a
// zero-DRIFT report that nonetheless has DeepUnverified>0 still fails the
// dry-run and shows up in both output modes. This is the integration point of
// the review fix — the count now originates in archive.Diff, not a scan-time
// probe counter.
func TestReconcileWiringFromReportDeepUnverified(t *testing.T) {
	// In sync (no actions), but one pair could not be deep-verified — the
	// dual-backend silent-downgrade state archive.Diff now reports.
	rep := &archive.Report{InSync: 2, DeepUnverified: 1}
	deepUnverified := rep.DeepUnverified // mirrors runArchiveReconcile

	// Dry-run must exit non-zero on the count even with zero diff actions.
	if err := reconcileDryRunErr(rep, deepUnverified); err == nil {
		t.Fatal("dry-run must fail when report.DeepUnverified>0 with no other drift")
	} else if !strings.Contains(err.Error(), "could not be deep-verified") {
		t.Fatalf("error must name the deep-verify failure, got: %v", err)
	}

	var jsonBuf bytes.Buffer
	if err := writeReconcileReport(&jsonBuf, "json", rep, deepUnverified, 0, nil, false, false); err != nil {
		t.Fatalf("writeReconcileReport json: %v", err)
	}
	var got reconcileReportJSON
	if err := json.Unmarshal(jsonBuf.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, jsonBuf.String())
	}
	if got.DeepUnverified != 1 {
		t.Fatalf("deep_unverified = %d, want 1 (raw: %s)", got.DeepUnverified, jsonBuf.String())
	}

	var textBuf bytes.Buffer
	if err := writeReconcileReport(&textBuf, "text", rep, deepUnverified, 0, nil, false, false); err != nil {
		t.Fatalf("writeReconcileReport text: %v", err)
	}
	if !strings.Contains(textBuf.String(), "could not be deep-verified") {
		t.Fatalf("text output must warn: %s", textBuf.String())
	}
}

// TestDuckDBParquetRowCountSharedSession pins the #807 refactor: footer probes
// run on ONE caller-owned DuckDB session (no per-object open/INSTALL/secret),
// so successive probes — including a path needing quote-escaping — must all
// work on the same handle. Local files stand in for S3 objects; only the
// transport differs (the parquetquery queryFileList precedent).
func TestDuckDBParquetRowCountSharedSession(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	ctx := context.Background()

	dir := t.TempDir()
	files := map[string]int64{"a.parquet": 3, "b.parquet": 5, "it's.parquet": 1}
	for name, n := range files {
		p := strings.ReplaceAll(filepath.Join(dir, name), "'", "''")
		if _, err := db.ExecContext(ctx,
			fmt.Sprintf("COPY (SELECT * FROM range(%d)) TO '%s' (FORMAT PARQUET)", n, p)); err != nil {
			t.Fatalf("write test parquet %s: %v", name, err)
		}
	}
	for name, want := range files {
		got, cols, err := duckdbParquetFooter(ctx, db, filepath.Join(dir, name))
		if err != nil {
			t.Fatalf("probe %s on the shared session: %v", name, err)
		}
		if got != want {
			t.Errorf("%s: row count %d, want %d", name, got, want)
		}
		// The same footer read yields the column set the #1535 backfill
		// records; the fixture is COPY (SELECT * FROM range(n)), whose single
		// column is named "range".
		if cols != "range" {
			t.Errorf("%s: column set %q, want %q", name, cols, "range")
		}
	}
}

// TestOpenS3FooterSessionRegionPin pins the #807 region fix: the shared --deep
// session must come up usable and carry the scan's region IN the chain secret
// (EnableS3CredentialChainRegion, #511) — without the pin, every probe on a
// cross-region bucket 301s into permanent DeepUnverified. Dummy env creds make
// the chain resolvable on creds-less CI; offline hosts skip (extension
// download), mirroring the duckdbutil test convention.
func TestOpenS3FooterSessionRegionPin(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "AKIATESTDUMMY0000000")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "testdummysecret")
	t.Setenv("BINTRAIL_DUCKDB_NO_AWS_EXT", "")

	db, err := openS3FooterSession(context.Background(), "eu-west-1")
	if err != nil {
		t.Skipf("httpfs unavailable (offline host?): %v", err)
	}
	defer db.Close()

	var one int
	if err := db.QueryRow("SELECT 1").Scan(&one); err != nil || one != 1 {
		t.Fatalf("session unusable after openS3FooterSession: %v (got %d)", err, one)
	}
	var loaded bool
	if err := db.QueryRow("SELECT loaded FROM duckdb_extensions() WHERE extension_name = 'aws'").Scan(&loaded); err != nil || !loaded {
		t.Skip("aws extension unavailable (offline host) — region pin not exercised")
	}
	var secretStr string
	if err := db.QueryRow("SELECT secret_string FROM duckdb_secrets() WHERE name = 'bintrail_s3_chain'").Scan(&secretStr); err != nil {
		t.Fatalf("chain secret missing after openS3FooterSession: %v", err)
	}
	if !strings.Contains(secretStr, "region=eu-west-1") {
		t.Fatalf("chain secret does not carry the pinned region; got %q", secretStr)
	}
}

// TestDueForS3SecretRefresh pins the follow-up to #807: the shared --deep
// footer session's credential-chain secret must not be frozen for the whole
// scan (duckdbutil.EnableS3CredentialChain explicitly warns it resolves
// credentials at CREATE time, not per request), or an expiring IMDS/STS role
// 403s every remaining probe partway through a long scan — the same
// DeepUnverified symptom #807 was filed to fix.
func TestDueForS3SecretRefresh(t *testing.T) {
	last := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	interval := 10 * time.Minute

	cases := []struct {
		name string
		now  time.Time
		want bool
	}{
		{"just issued", last, false},
		{"well within interval", last.Add(5 * time.Minute), false},
		{"one second before due", last.Add(interval - time.Second), false},
		{"exactly at interval", last.Add(interval), true},
		{"well past interval", last.Add(45 * time.Minute), true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := dueForS3SecretRefresh(last, c.now, interval); got != c.want {
				t.Errorf("dueForS3SecretRefresh(%v, %v, %v) = %v, want %v", last, c.now, interval, got, c.want)
			}
		})
	}
}

// TestS3FooterSessionSecretReissue pins the mechanism the #807 follow-up
// relies on: re-issuing the credential-chain secret on an ALREADY-OPEN shared
// session (not a fresh one) must succeed and leave the session usable, so a
// long --deep scan can pick up rotated credentials mid-scan the same way the
// pre-#807 per-object sessions did.
func TestS3FooterSessionSecretReissue(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "<redacted>")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "testdummysecret")
	t.Setenv("BINTRAIL_DUCKDB_NO_AWS_EXT", "")

	db, err := openS3FooterSession(context.Background(), "eu-west-1")
	if err != nil {
		t.Skipf("httpfs unavailable (offline host?): %v", err)
	}
	defer db.Close()

	var loaded bool
	if err := db.QueryRow("SELECT loaded FROM duckdb_extensions() WHERE extension_name = 'aws'").Scan(&loaded); err != nil || !loaded {
		t.Skip("aws extension unavailable (offline host) — secret reissue not exercised")
	}

	// Simulate the scan loop's periodic refresh on the SAME session.
	duckdbutil.EnableS3CredentialChainRegion(context.Background(), db, "eu-west-1")

	var one int
	if err := db.QueryRow("SELECT 1").Scan(&one); err != nil || one != 1 {
		t.Fatalf("session unusable after re-issuing the secret: %v (got %d)", err, one)
	}
	var secretStr string
	if err := db.QueryRow("SELECT secret_string FROM duckdb_secrets() WHERE name = 'bintrail_s3_chain'").Scan(&secretStr); err != nil {
		t.Fatalf("chain secret missing after re-issue: %v", err)
	}
	if !strings.Contains(secretStr, "region=eu-west-1") {
		t.Fatalf("re-issued chain secret does not carry the pinned region; got %q", secretStr)
	}
}
