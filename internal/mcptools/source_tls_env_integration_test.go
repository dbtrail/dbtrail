//go:build integration

package mcptools

import (
	"context"
	"testing"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// The standalone resolver itself hands the environment's source TLS to the
// target extension tools read (not just the helper that reads it).
func TestIntegrationDSNTargetCarriesEnvSourceTLS(t *testing.T) {
	_, dbName := testutil.CreateTestDB(t)
	t.Setenv("BINTRAIL_SOURCE_DSN", "u:p@tcp(src.example:3306)/")
	t.Setenv("BINTRAIL_SSL_MODE", "verify-identity")
	t.Setenv("BINTRAIL_SSL_CA", "/i/ca.pem")
	t.Setenv("BINTRAIL_SSL_CERT", "/i/cert.pem")
	t.Setenv("BINTRAIL_SSL_KEY", "/i/key.pem")

	tgt, err := DSNTarget(func(string) (string, error) { return testutil.SnapshotDSN(dbName), nil })(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	defer tgt.DB.Close()
	want := ext.SourceTLS{Mode: "verify-identity", CA: "/i/ca.pem", Cert: "/i/cert.pem", Key: "/i/key.pem"}
	if tgt.SourceDSN != "u:p@tcp(src.example:3306)/" || tgt.SourceTLS != want {
		t.Fatalf("SourceDSN %q SourceTLS %+v, want %+v", tgt.SourceDSN, tgt.SourceTLS, want)
	}
}
