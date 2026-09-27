package cliapp

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestResolveBaselineRetain pins the three --baseline-retain outcomes without the
// dump→convert pipeline: unset → off, malformed → error, set-without-upload →
// loud no-op (the only-copy guard), set-with-upload → prune.
func TestResolveBaselineRetain(t *testing.T) {
	if r, doPrune, err := resolveBaselineRetain("", "s3://b/p"); err != nil || doPrune || r != 0 {
		t.Errorf("unset: got (%v, %v, %v), want (0, false, nil)", r, doPrune, err)
	}
	if _, _, err := resolveBaselineRetain("garbage", "s3://b/p"); err == nil {
		t.Error("malformed --baseline-retain must error")
	}
	if r, doPrune, err := resolveBaselineRetain("7d", ""); err != nil || doPrune || r != 7*24*time.Hour {
		t.Errorf("retain without --upload: got (%v, %v, %v), want (7d, false, nil)", r, doPrune, err)
	}
	if r, doPrune, err := resolveBaselineRetain("7d", "s3://b/p"); err != nil || !doPrune || r != 7*24*time.Hour {
		t.Errorf("retain with --upload: got (%v, %v, %v), want (7d, true, nil)", r, doPrune, err)
	}
}

// ─── cobra command wiring ─────────────────────────────────────────────────────

func TestBaselineCmd_registered(t *testing.T) {
	found := false
	for _, cmd := range rootCmd.Commands() {
		if cmd.Use == "baseline" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected 'baseline' command to be registered under rootCmd")
	}
}

func TestBaselineCmd_requiredFlags(t *testing.T) {
	for _, name := range []string{"input", "output"} {
		flag := baselineCmd.Flag(name)
		if flag == nil {
			t.Fatalf("flag --%s not registered", name)
		}
		if flag.Annotations["cobra_annotation_bash_completion_one_required_flag"] == nil {
			t.Errorf("flag --%s is not marked required", name)
		}
	}
}

func TestBaselineCmd_allFlagsRegistered(t *testing.T) {
	for _, name := range []string{
		"input", "output", "timestamp", "tables",
		"compression", "row-group-size", "upload", "upload-region",
		"retry", "encrypt", "encrypt-key",
	} {
		if baselineCmd.Flag(name) == nil {
			t.Errorf("flag --%s not registered on baselineCmd", name)
		}
	}
}

func TestBaselineCmd_defaults(t *testing.T) {
	cases := []struct {
		flag string
		want string
	}{
		{"compression", "zstd"},
		{"row-group-size", "500000"},
	}
	for _, tc := range cases {
		f := baselineCmd.Flag(tc.flag)
		if f == nil {
			t.Fatalf("flag --%s not registered", tc.flag)
		}
		if f.DefValue != tc.want {
			t.Errorf("flag --%s: expected default %q, got %q", tc.flag, tc.want, f.DefValue)
		}
	}
}

func TestBaselineCmd_emptyStringDefaults(t *testing.T) {
	for _, name := range []string{"timestamp", "tables", "upload", "upload-region"} {
		f := baselineCmd.Flag(name)
		if f == nil {
			t.Errorf("flag --%s not registered", name)
			continue
		}
		if f.DefValue != "" {
			t.Errorf("flag --%s: expected empty default, got %q", name, f.DefValue)
		}
	}
}

func TestBaselineCmd_retryDefaultFalse(t *testing.T) {
	f := baselineCmd.Flag("retry")
	if f == nil {
		t.Fatal("flag --retry not registered")
	}
	if f.DefValue != "false" {
		t.Errorf("expected default retry=false, got %q", f.DefValue)
	}
}

// ─── parseTableFilter ─────────────────────────────────────────────────────────

func TestParseTableFilter(t *testing.T) {
	cases := []struct {
		input string
		want  []string
	}{
		{"", nil},
		{"mydb.orders", []string{"mydb.orders"}},
		{"mydb.orders, mydb.users", []string{"mydb.orders", "mydb.users"}},
		{"  mydb.orders  ,  ", []string{"mydb.orders"}},
	}
	for _, tc := range cases {
		got := parseTableFilter(tc.input)
		if len(got) != len(tc.want) {
			t.Errorf("parseTableFilter(%q) = %v, want %v", tc.input, got, tc.want)
			continue
		}
		for i := range tc.want {
			if got[i] != tc.want[i] {
				t.Errorf("parseTableFilter(%q)[%d] = %q, want %q", tc.input, i, got[i], tc.want[i])
			}
		}
	}
}

func TestRunBaselineTimestampParsing(t *testing.T) {
	origInput, origOutput, origTS := bslInput, bslOutput, bslTimestamp
	t.Cleanup(func() {
		bslInput = origInput
		bslOutput = origOutput
		bslTimestamp = origTS
	})

	// Real (empty) directories: post-#461 a dump with zero tables is an
	// error, so the valid-timestamp cases below assert exactly that error —
	// which doubles as the cmd-level propagation check for it.
	bslInput = t.TempDir()
	bslOutput = t.TempDir()

	// Invalid format must return the "expected ISO 8601" error before calling Run.
	bslTimestamp = "not-a-timestamp"
	if err := runBaseline(baselineCmd, nil); err == nil || !strings.Contains(err.Error(), "expected ISO 8601") {
		t.Errorf("invalid timestamp: want ISO 8601 error, got: %v", err)
	}

	// Valid formats: each must get past timestamp parsing and surface the
	// zero-tables refusal from baseline.Run instead.
	validCases := []struct {
		name string
		ts   string
	}{
		{"RFC3339", "2025-02-28T00:00:00Z"},
		{"T-no-TZ", "2025-02-28T00:00:00"},
		{"space-fmt", "2025-02-28 00:00:00"},
	}
	for _, tc := range validCases {
		bslTimestamp = tc.ts
		err := runBaseline(baselineCmd, nil)
		if err == nil || !strings.Contains(err.Error(), "no tables found") {
			t.Errorf("%s: want the zero-tables error after a parsed timestamp, got: %v", tc.name, err)
		}
	}
}

// TestParseTableFilter_onlyCommasAndSpaces verifies that a string containing
// only commas and whitespace (no actual table names) returns nil — the same
// result as an empty string, but via a different code path (SplitSeq iterates
// but every trimmed part is "").
func TestParseTableFilter_onlyCommasAndSpaces(t *testing.T) {
	for _, input := range []string{",", "  ,  ", ", , ,", " , "} {
		if got := parseTableFilter(input); got != nil {
			t.Errorf("parseTableFilter(%q) = %v, want nil", input, got)
		}
	}
}

// TestParseTableFilter_threeEntries verifies the split loop handles n>2 tables.
func TestParseTableFilter_threeEntries(t *testing.T) {
	got := parseTableFilter("db.a, db.b, db.c")
	want := []string{"db.a", "db.b", "db.c"}
	if len(got) != len(want) {
		t.Fatalf("expected %v, got %v", want, got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("[%d] expected %q, got %q", i, want[i], got[i])
		}
	}
}

// TestParseTableFilter_trailingComma verifies that a trailing comma does not
// produce an empty string entry — the TrimSpace+empty-check drops it.
func TestParseTableFilter_trailingComma(t *testing.T) {
	got := parseTableFilter("db.orders,")
	if len(got) != 1 || got[0] != "db.orders" {
		t.Errorf("expected [db.orders], got %v", got)
	}
}

func TestRunBaselineMissingInput(t *testing.T) {
	origInput, origOutput, origTS := bslInput, bslOutput, bslTimestamp
	t.Cleanup(func() {
		bslInput = origInput
		bslOutput = origOutput
		bslTimestamp = origTS
	})

	// Non-existent input directory should produce an error about reading the dir.
	bslInput = "/nonexistent/path-does-not-exist"
	bslOutput = t.TempDir()
	bslTimestamp = "2025-01-01T00:00:00Z" // valid timestamp, skips metadata parsing

	if err := runBaseline(baselineCmd, nil); err == nil {
		t.Error("expected error for nonexistent input directory, got nil")
	}
}

// TestRunBaseline_invalidUploadURL verifies that an invalid --upload value
// (not starting with s3://) is caught by parseS3URL inside uploadBaselineToS3
// and surfaces as an "S3 upload" error — without requiring AWS credentials.
// A minimal real dump carries execution past baseline.Run (post-#461 an
// empty input dir errors on zero tables) so the upload block's URL
// validation is what fires.
func TestRunBaseline_invalidUploadURL(t *testing.T) {
	origInput, origOutput, origTS, origUpload :=
		bslInput, bslOutput, bslTimestamp, bslUpload
	t.Cleanup(func() {
		bslInput = origInput
		bslOutput = origOutput
		bslTimestamp = origTS
		bslUpload = origUpload
	})

	// A minimal 1-table dump: an EMPTY input dir no longer reaches the
	// upload validation — zero discovered tables is an error since #461.
	bslInput = t.TempDir()
	writeMinimalDump(t, bslInput)
	bslOutput = t.TempDir()
	// Outside cobra's Execute the command context is nil, and baseline.Run's
	// workers (now actually reached — the dump has a table) call ctx.Err().
	baselineCmd.SetContext(context.Background())
	t.Cleanup(func() { baselineCmd.SetContext(nil) })
	bslTimestamp = "2025-02-28T00:00:00Z"
	bslUpload = "http://not-s3.example.com/bucket" // invalid: not s3://

	err := runBaseline(baselineCmd, nil)
	if err == nil {
		t.Fatal("expected error for invalid --upload URL, got nil")
	}
	if !strings.Contains(err.Error(), "S3 upload") {
		t.Errorf("expected 'S3 upload' in error, got: %v", err)
	}
}

// TestRunBaseline_emptyTimestamp verifies that when --timestamp is omitted,
// runBaseline delegates timestamp resolution to baseline.Run → ParseMetadata.
// A temp dir with no metadata file causes a "parse mydumper metadata" error —
// proving the empty-timestamp code path is reached.
func TestRunBaseline_emptyTimestamp(t *testing.T) {
	origInput, origOutput, origTS := bslInput, bslOutput, bslTimestamp
	t.Cleanup(func() {
		bslInput = origInput
		bslOutput = origOutput
		bslTimestamp = origTS
	})

	bslInput = t.TempDir() // valid dir but no metadata file
	bslOutput = t.TempDir()
	bslTimestamp = "" // triggers ParseMetadata inside baseline.Run

	err := runBaseline(baselineCmd, nil)
	if err == nil {
		t.Fatal("expected error when metadata file is absent, got nil")
	}
	if !strings.Contains(err.Error(), "metadata") {
		t.Errorf("expected 'metadata' in error, got: %v", err)
	}
}

// ─── encryption ───────────────────────────────────────────────────────────────

func TestBaselineCmd_encryptDefaultFalse(t *testing.T) {
	f := baselineCmd.Flag("encrypt")
	if f == nil {
		t.Fatal("flag --encrypt not registered")
	}
	if f.DefValue != "false" {
		t.Errorf("expected default encrypt=false, got %q", f.DefValue)
	}
}

func TestDecryptDumpFiles_noEncFiles(t *testing.T) {
	dir := t.TempDir()
	// Create a non-.enc file to verify it's left alone.
	os.WriteFile(filepath.Join(dir, "test.sql"), []byte("data"), 0o644)

	keyPath := filepath.Join(dir, "test.key")
	os.WriteFile(keyPath, []byte("testkey"), 0o600)

	cleanup, err := decryptDumpFiles(dir, keyPath)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer cleanup()
	// Should log a warning but not fail.
}

func TestDecryptDumpFiles_roundTrip(t *testing.T) {
	// Skip if openssl is not available.
	if _, err := exec.LookPath("openssl"); err != nil {
		t.Skip("openssl not available on $PATH")
	}

	dir := t.TempDir()
	keyPath := filepath.Join(dir, "test.key")
	os.WriteFile(keyPath, []byte("testpassphrase"), 0o600)

	// Encrypt a file using the same openssl command that dump would use.
	plaintext := "CREATE TABLE test (id INT PRIMARY KEY);\n"
	plainFile := filepath.Join(dir, "mydb.test-schema.sql")
	os.WriteFile(plainFile, []byte(plaintext), 0o644)

	encFile := plainFile + ".enc"
	cmd := exec.Command("openssl", "enc", "-aes-256-cbc", "-pbkdf2",
		"-pass", "file:"+keyPath, "-in", plainFile, "-out", encFile)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("encrypt failed: %v\n%s", err, out)
	}

	// Remove the plain file to simulate what mydumper would produce.
	os.Remove(plainFile)

	// Decrypt.
	cleanup, err := decryptDumpFiles(dir, keyPath)
	if err != nil {
		t.Fatalf("decryptDumpFiles failed: %v", err)
	}

	// Verify decrypted file exists and has correct content.
	data, err := os.ReadFile(plainFile)
	if err != nil {
		t.Fatalf("decrypted file not found: %v", err)
	}
	if string(data) != plaintext {
		t.Errorf("decrypted content mismatch: got %q, want %q", string(data), plaintext)
	}

	// Cleanup should remove the decrypted file.
	cleanup()
	if _, err := os.Stat(plainFile); !os.IsNotExist(err) {
		t.Error("cleanup should have removed the decrypted file")
	}
}

// writeMinimalDump writes the smallest mydumper output that converts: one
// table with a schema file and a single-row INSERT.
func writeMinimalDump(t *testing.T, dir string) {
	t.Helper()
	schema := "CREATE TABLE `orders` (\n  `id` int NOT NULL,\n  PRIMARY KEY (`id`)\n);\n"
	for name, content := range map[string]string{
		"shop.orders-schema.sql": schema,
		"shop.orders.00000.sql":  "INSERT INTO `orders` VALUES(1);\n",
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func TestViewsSkippedLine(t *testing.T) {
	many := []string{"a.v1", "a.v2", "a.v3", "a.v4", "a.v5", "a.v6", "a.v7"}
	tests := []struct {
		name  string
		views []string
		want  string
	}{
		{"none", nil, ""},
		{"empty", []string{}, ""},
		{"one", []string{"shop.big_orders"}, "1 skipped (shop.big_orders)"},
		{"as many as are shown", many[:5], "5 skipped (a.v1, a.v2, a.v3, a.v4, a.v5)"},
		{"more than are shown", many, "7 skipped (a.v1, a.v2, a.v3, a.v4, a.v5 and 2 more)"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := viewsSkippedLine(tt.views); got != tt.want {
				t.Errorf("viewsSkippedLine = %q, want %q", got, tt.want)
			}
		})
	}
}
