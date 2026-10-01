//go:build integration

package consoleapp

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/doctor"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// tlsOnlyDSN creates a user the test server only lets in over TLS (REQUIRE
// SSL), the per-user form of require_secure_transport=ON.
func tlsOnlyDSN(t *testing.T) string {
	t.Helper()
	testutil.SkipIfNoMySQL(t)
	admin, err := sql.Open("mysql", testutil.BaseDSN()+"/")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { admin.Close() })
	user := fmt.Sprintf("ctlsonly_%d", time.Now().UnixNano()%1_000_000_000)
	const pass = "tlsonly-pass"
	for _, q := range []string{
		fmt.Sprintf("CREATE USER '%s'@'%%' IDENTIFIED BY '%s' REQUIRE SSL", user, pass),
		fmt.Sprintf("GRANT SELECT, REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO '%s'@'%%'", user),
	} {
		if _, err := admin.Exec(q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	t.Cleanup(func() { _, _ = admin.Exec(fmt.Sprintf("DROP USER '%s'@'%%'", user)) })
	cfg, err := mysql.ParseDSN(testutil.BaseDSN() + "/")
	if err != nil {
		t.Fatal(err)
	}
	cfg.User, cfg.Passwd, cfg.DBName = user, pass, ""
	return cfg.FormatDSN()
}

// The connect flow's startup checks (DoctorUnsaved) must reach the source with
// the TLS its capture will use: an entry with no ssl_mode is "preferred", so a
// source that only accepts encrypted connections passes, as capture would.
func TestDoctorUnsaved_UsesTheEntrysSourceTLS(t *testing.T) {
	dsn := tlsOnlyDSN(t)
	m := newMonitorSupervisor(context.Background(), "", nil, 0)
	m.loopbackRetry = func(string, string) string { return "" }

	for _, tc := range []struct {
		mode string
		want string
	}{
		{"", "pass"},
		{"preferred", "pass"},
		{"required", "pass"},
		{"disabled", "fail"},
	} {
		t.Run("ssl_mode="+tc.mode, func(t *testing.T) {
			r, err := m.DoctorUnsaved(t.Context(), console.ServerEntry{Name: "tls-only", SourceDSN: dsn, SSLMode: tc.mode})
			if err != nil {
				t.Fatal(err)
			}
			for _, c := range r.Checks {
				if c.Name == doctor.SourceConnectionCheckName {
					if c.Status != tc.want {
						t.Fatalf("source connection = %s (%s), want %s", c.Status, c.Detail, tc.want)
					}
					return
				}
			}
			t.Fatal("no source connection check")
		})
	}
}

// connectSource is what every console read of a saved source goes through:
// the zero SSL a request carries by default is preferred, which reaches a
// TLS-only source; disabled does not.
func TestConnectSource_TLSOnlySource(t *testing.T) {
	dsn := tlsOnlyDSN(t)
	db, err := connectSource(dsn, config.SSL{})
	if err != nil {
		t.Fatalf("zero SSL (preferred): %v", err)
	}
	db.Close()
	if db, err := connectSource(dsn, config.SSL{Mode: "disabled"}); err == nil {
		db.Close()
		t.Fatal("disabled reached a source that only accepts TLS")
	}
}

// The schema snapshot reads the source with the request's TLS.
func TestTakeSchemaSnapshot_TLSOnlySource(t *testing.T) {
	src := tlsOnlyDSN(t)
	idxDB, idxName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, idxDB)
	req := console.SchemaSnapshotRequest{ServerID: "tls", SourceDSN: src,
		IndexDSN: testutil.DefaultDSN + "/" + idxName, Schemas: []string{"mysql"}, SourceSSL: config.SSL{Mode: "preferred"}}
	if _, err := takeSchemaSnapshot(req); err != nil {
		t.Fatalf("preferred: %v", err)
	}
	req.SourceSSL = config.SSL{Mode: "disabled"}
	if _, err := takeSchemaSnapshot(req); err == nil {
		t.Fatal("disabled took a snapshot of a source that only accepts TLS")
	}
}
