package cliapp

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/pflag"

	"github.com/dbtrail/dbtrail/internal/icebergexport"
)

func resetCatalogGlobals(t *testing.T) {
	t.Helper()
	resetExportGlobals(t)
	sKind, sURI, sWh, sV4, sV4R, sV4S, sCred, sTok := eiCatalog, eiCatalogURI, eiCatalogWarehouse, eiSigV4, eiSigV4Region, eiSigV4Service, eiCredentialFile, eiTokenFile
	t.Cleanup(func() {
		eiCatalog, eiCatalogURI, eiCatalogWarehouse, eiSigV4, eiSigV4Region, eiSigV4Service, eiCredentialFile, eiTokenFile = sKind, sURI, sWh, sV4, sV4R, sV4S, sCred, sTok
	})
	eiCatalog, eiCatalogURI, eiCatalogWarehouse, eiSigV4, eiSigV4Region, eiSigV4Service, eiCredentialFile, eiTokenFile = "hadoop", "", "", false, "", "", "", ""
	t.Setenv(envCatalogCredential, "")
	t.Setenv(envCatalogToken, "")
}

func secretFile(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// TestRunExportIceberg_catalogFlags: each combination is refused before
// anything is opened, and the positive controls prove the checks were
// passed (a glue run without a region stops at the region, a rest run at
// the unreachable catalog, a hadoop run at the unreachable index).
func TestRunExportIceberg_catalogFlags(t *testing.T) {
	valid := func(t *testing.T) {
		eiIndexDSN = "u:p@tcp(127.0.0.1:1)/db?timeout=200ms"
		eiBaselineDir = t.TempDir()
		eiWarehouse = t.TempDir()
		eiTables = "shop.orders"
		eiFormat = "text"
	}
	cases := []struct {
		name string
		mut  func(t *testing.T)
		want string
	}{
		{"unknown catalog", func(t *testing.T) { eiCatalog = "hive" }, `unknown catalog "hive"`},
		{"hadoop with s3", func(t *testing.T) { eiWarehouse = "s3://b/p" }, "object-store URL"},
		{"hadoop with a uri", func(t *testing.T) { eiCatalogURI = "https://c" }, "--catalog-uri only applies to --catalog rest"},
		{"hadoop with a token file", func(t *testing.T) { eiTokenFile = secretFile(t, "S3CR3T-tok3") }, "--catalog-token-file only applies to --catalog rest"},
		{"glue without warehouse", func(t *testing.T) { eiCatalog, eiWarehouse = "glue", "" }, "--catalog glue needs --warehouse s3://bucket/prefix"},
		{"glue with a local path", func(t *testing.T) { eiCatalog = "glue" }, "must be an s3:// location"},
		{"glue with gs", func(t *testing.T) { eiCatalog, eiWarehouse = "glue", "gs://b/p" }, "must be an s3:// location"},
		{"glue with a credential file", func(t *testing.T) {
			eiCatalog, eiWarehouse, eiCredentialFile = "glue", "s3://b/p", secretFile(t, "S3CR3T-cred:x")
		}, "--catalog-credential-file only applies to --catalog rest"},
		{"rest without uri", func(t *testing.T) { eiCatalog, eiWarehouse = "rest", "" }, "--catalog rest needs --catalog-uri"},
		{"rest with credential and token env", func(t *testing.T) {
			eiCatalog, eiWarehouse, eiCatalogURI = "rest", "", "http://127.0.0.1:1"
			t.Setenv(envCatalogCredential, "S3CR3T-cred:x")
			t.Setenv(envCatalogToken, "S3CR3T-tok3")
		}, "pick one way to authenticate"},
		{"rest with token env and token file", func(t *testing.T) {
			eiCatalog, eiWarehouse, eiCatalogURI = "rest", "", "http://127.0.0.1:1"
			t.Setenv(envCatalogToken, "S3CR3T-tok3")
			eiTokenFile = secretFile(t, "S3CR3T-tok2")
		}, "both " + envCatalogToken + " and --catalog-token-file"},
		{"rest with sigv4 and a token", func(t *testing.T) {
			eiCatalog, eiWarehouse, eiCatalogURI, eiSigV4 = "rest", "", "http://127.0.0.1:1", true
			t.Setenv(envCatalogToken, "S3CR3T-tok3")
		}, "pick one way to authenticate"},
		{"rest with an empty token file", func(t *testing.T) {
			eiCatalog, eiWarehouse, eiCatalogURI = "rest", "", "http://127.0.0.1:1"
			eiTokenFile = secretFile(t, " \n")
		}, "--catalog-token-file is empty"},
		{"positive control: glue stops at the region", func(t *testing.T) {
			eiCatalog, eiWarehouse = "glue", "s3://b/p"
			isolateAWS(t)
		}, "no AWS region"},
		{"positive control: rest stops at the catalog", func(t *testing.T) {
			eiCatalog, eiWarehouse, eiCatalogURI = "rest", "", "http://127.0.0.1:1"
			eiTokenFile = secretFile(t, "S3CR3T-tok1\n")
		}, "open the REST catalog"},
		{"positive control: hadoop ignores catalog secrets in the environment", func(t *testing.T) {
			t.Setenv(envCatalogToken, "S3CR3T-tok3")
			t.Setenv(envCatalogCredential, "S3CR3T-cred:x")
		}, "connect to index"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resetCatalogGlobals(t)
			valid(t)
			tc.mut(t)
			exportIcebergCmd.SetContext(context.Background())
			exportIcebergCmd.SetOut(&bytes.Buffer{})
			err := runExportIceberg(exportIcebergCmd, nil)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want containing %q", err, tc.want)
			}
			if strings.Contains(err.Error(), "S3CR3T") {
				t.Fatalf("error echoes a secret value: %v", err)
			}
		})
	}
}

// TestExportIcebergFlags_noSecretOnTheCommandLine: argv is visible to every
// local user (ps, /proc/<pid>/cmdline) and lands in shell history, so no
// flag of the command may carry a secret. A flag named like one must be a
// path to a file.
func TestExportIcebergFlags_noSecretOnTheCommandLine(t *testing.T) {
	n := 0
	exportIcebergCmd.Flags().VisitAll(func(f *pflag.Flag) {
		name := strings.ToLower(f.Name)
		for _, w := range []string{"credential", "token", "secret", "password", "key"} {
			if strings.Contains(name, w) {
				n++
				if !strings.HasSuffix(name, "-file") {
					t.Errorf("--%s looks like it takes a secret on the command line; read it from an environment variable or a file", f.Name)
				}
			}
		}
	})
	if n < 2 {
		t.Fatalf("found %d secret-bearing flags; want at least the credential and token file flags (the scan is broken)", n)
	}
	for _, flag := range []string{"--catalog-credential", "--catalog-token"} {
		if err := exportIcebergCmd.ParseFlags([]string{flag, "x"}); err == nil || !strings.Contains(err.Error(), "unknown flag") {
			t.Errorf("%s parsed (%v); a secret must not be accepted on the command line", flag, err)
		}
	}
}

// TestReadSecret: values are trimmed (a file ends in a newline), an
// unreadable file never echoes what was passed, because a secret pasted
// where the PATH belongs is the likely mistake.
func TestReadSecret(t *testing.T) {
	t.Setenv("BT_TEST_SECRET", "")
	if v, err := readSecret("BT_TEST_SECRET", "--x-file", secretFile(t, "  s3cr3t \n")); err != nil || v != "s3cr3t" {
		t.Fatalf("file: %q, %v", v, err)
	}
	t.Setenv("BT_TEST_SECRET", " fromenv\n")
	if v, err := readSecret("BT_TEST_SECRET", "--x-file", ""); err != nil || v != "fromenv" {
		t.Fatalf("env: %q, %v", v, err)
	}
	t.Setenv("BT_TEST_SECRET", "")
	if v, err := readSecret("BT_TEST_SECRET", "--x-file", ""); err != nil || v != "" {
		t.Fatalf("neither: %q, %v", v, err)
	}
	_, err := readSecret("BT_TEST_SECRET", "--x-file", "eyJhbGciOiJIUzI1NiJ9.pasted-token")
	if err == nil || strings.Contains(err.Error(), "eyJ") {
		t.Fatalf("unreadable file: err = %v; it must refuse without echoing the value", err)
	}
}

func isolateAWS(t *testing.T) {
	t.Helper()
	empty := filepath.Join(t.TempDir(), "empty")
	if err := os.WriteFile(empty, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AWS_CONFIG_FILE", empty)
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", empty)
	t.Setenv("AWS_REGION", "")
	t.Setenv("AWS_DEFAULT_REGION", "")
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
}

func TestExportTarget_redactsTheURI(t *testing.T) {
	got := exportTarget(icebergexport.CatalogConfig{Kind: icebergexport.CatalogREST, URI: "https://host/x?token=S3CR3T"}, "")
	if strings.Contains(got, "S3CR3T") || !strings.Contains(got, "https://host/x") {
		t.Fatalf("exportTarget = %q", got)
	}
}
