//go:build integration

package consoleapp

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
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

// gtidIndex is an index database whose capture checkpoint is a GTID set, so
// the capture reads below go on to ask the source.
func gtidIndex(t *testing.T, flavor string) string {
	t.Helper()
	db, name := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	testutil.MustExec(t, db, `REPLACE INTO stream_state (id, mode, binlog_file, binlog_position, gtid_set, flavor, last_checkpoint, server_id, capture_skips)
		VALUES (1, 'gtid', 'binlog.000001', 4, ?, ?, UTC_TIMESTAMP(), 1, '{}')`, gtidFor(flavor), flavor)
	return testutil.DefaultDSN + "/" + name
}

func gtidFor(flavor string) string {
	if flavor == console.FlavorMariaDB {
		return "0-1-10"
	}
	return "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-10"
}

// Each repeating capture read reaches a TLS-only source with the entry's
// default TLS. Reverted to a cleartext connect, each answers "the source did
// not answer".
func TestCaptureReads_TLSOnlySource(t *testing.T) {
	src := tlsOnlyDSN(t)
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		read func() captureProbeResult
	}{
		{"capture status", func() captureProbeResult {
			return captureHeadFromDBs(ctx, gtidIndex(t, "mysql"), src, config.SSL{})
		}},
		{"capture status, MariaDB", func() captureProbeResult {
			return captureHeadFromDBsMariaDB(ctx, gtidIndex(t, console.FlavorMariaDB), src, config.SSL{})
		}},
		{"window probe (#1791)", func() captureProbeResult {
			return captureFromDBs(ctx, gtidIndex(t, "mysql"), src, config.SSL{}, time.Now().Add(-time.Hour), time.Time{})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := tc.read()
			t.Logf("%+v", r)
			if r.detail == "the source did not answer" {
				t.Fatalf("the read did not reach the TLS-only source: %+v", r)
			}
		})
	}
}

// The replica check at Start reads the candidate and every saved MariaDB
// peer with their own TLS.
func TestReplicaReads_TLSOnlySource(t *testing.T) {
	src := tlsOnlyDSN(t)
	if p := readMariaDBPeer(context.Background(), console.ServerEntry{Name: "peer", SourceDSN: src}); p.unreachable {
		t.Fatalf("readMariaDBPeer did not reach the TLS-only peer: %+v", p)
	}
	reg, err := console.LoadRegistry("")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reg.Add(console.ServerEntry{Name: "peer", DSN: testutil.DefaultDSN + "/peer_idx", SourceDSN: src}); err != nil {
		t.Fatal(err)
	}
	m := newMonitorSupervisor(context.Background(), "", reg, 0)
	c := m.replicaOverlapCheck(context.Background(), console.ServerEntry{ID: "cand", Name: "cand", SourceDSN: src})
	if c == nil {
		t.Fatal("no replica check")
	}
	t.Logf("%s: %s", c.Status, c.Detail)
	if strings.HasPrefix(c.Detail, "could not connect to the source") {
		t.Fatalf("the replica check did not reach the TLS-only source: %s", c.Detail)
	}
}

// The live-source verify reaches a TLS-only source with the request's TLS.
func TestRunLiveSource_TLSOnlySource(t *testing.T) {
	src := tlsOnlyDSN(t)
	idxDB, idxName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, idxDB)
	s := &verifySupervisor{ctx: context.Background()}
	err := s.runLiveSource(console.VerifyRequest{ServerID: "tls", SourceDSN: src}, idxDB, nil, idxName)
	if err != nil && strings.HasPrefix(err.Error(), "connect source") {
		t.Fatalf("live verify did not reach the TLS-only source: %v", err)
	}
}
