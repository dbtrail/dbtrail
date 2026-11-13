//go:build integration

package consoleapp

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/testutil"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

// gtidModeDSNEnv names a THROWAWAY MySQL 8.0 server started with GTIDs on
// (--gtid-mode=ON --enforce-gtid-consistency=ON, binary logging on). The
// test below changes its gtid_mode, which nobody may do to a shared server,
// so it runs only when this is set, and puts the mode back to ON at the end.
const gtidModeDSNEnv = "BINTRAIL_TEST_GTIDMODE_DSN"

// #2674, point 3: the capture probe classifies a source whose gtid_mode is
// not ON, while capture follows it by GTID, as "capture cannot read from
// it" (a full read), on the MySQL manual's rule for auto-position. This
// confirms it against a real server in each mode: capture's own request (a
// binlog dump from a GTID set, StartSyncGTID with the syncer settings
// streamrun uses) is refused by the source in OFF, OFF_PERMISSIVE and
// ON_PERMISSIVE and served in ON, and the probe says which mode it found.
func TestIntegrationGTIDModeRefusesCaptureByGTID_2674(t *testing.T) {
	src := os.Getenv(gtidModeDSNEnv)
	if src == "" {
		t.Skip(gtidModeDSNEnv + " is not set: this test changes gtid_mode on the server it names, so it needs a throwaway one")
	}
	sdb, err := config.Connect(src)
	if err != nil {
		t.Fatal(err)
	}
	// Registered before the cleanup that puts the mode back, so it runs
	// after it (cleanups run last-registered first).
	t.Cleanup(func() { sdb.Close() })
	ctx := context.Background()
	ladder := []string{"OFF", "OFF_PERMISSIVE", "ON_PERMISSIVE", "ON"}
	rung := func(mode string) int {
		for i, m := range ladder {
			if m == mode {
				return i
			}
		}
		t.Fatalf("unknown gtid_mode %q", mode)
		return -1
	}
	// MySQL moves gtid_mode one step at a time.
	setMode := func(want string) {
		t.Helper()
		var cur string
		if err := sdb.QueryRowContext(ctx, "SELECT @@GLOBAL.gtid_mode").Scan(&cur); err != nil {
			t.Fatal(err)
		}
		for i := rung(cur); i != rung(want); {
			if i < rung(want) {
				i++
			} else {
				i--
			}
			if _, err := sdb.ExecContext(ctx, "SET GLOBAL gtid_mode = "+ladder[i]); err != nil {
				t.Fatalf("SET GLOBAL gtid_mode = %s: %v", ladder[i], err)
			}
		}
	}
	t.Cleanup(func() { setMode("ON") })
	setMode("ON")
	testutil.MustExec(t, sdb, "CREATE DATABASE IF NOT EXISTS p2674")
	testutil.MustExec(t, sdb, "CREATE TABLE IF NOT EXISTS p2674.t (id INT AUTO_INCREMENT PRIMARY KEY, at DATETIME(6))")
	var executed string
	if err := sdb.QueryRowContext(ctx, "SELECT @@GLOBAL.gtid_executed").Scan(&executed); err != nil {
		t.Fatal(err)
	}
	executed = strings.Join(strings.Fields(executed), "")

	// The index, on the shared test server: a GTID checkpoint at the set
	// the source had executed, as a capture that followed it would leave.
	db, name := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	indexDSN := testutil.DefaultDSN + "/" + name
	testutil.MustExec(t, db, `REPLACE INTO stream_state (id, mode, binlog_file, binlog_position, gtid_set, flavor, last_checkpoint, server_id, capture_skips)
		VALUES (1, 'gtid', 'binlog.000001', 4, ?, 'mysql', UTC_TIMESTAMP(), 1, '{}')`, executed)

	host, port, user, password, err := config.ParseSourceDSN(src)
	if err != nil {
		t.Fatal(err)
	}
	// captureByGTID is capture's request: the same syncer settings as
	// streamrun (no TLS here: a throwaway server), from the checkpoint's
	// set, and the first answer the source gives.
	captureByGTID := func() error {
		t.Helper()
		syncer := replication.NewBinlogSyncer(replication.BinlogSyncerConfig{
			ServerID: 92674, Flavor: "mysql", Host: host, Port: port, User: user, Password: password,
			HeartbeatPeriod: time.Second, MaxReconnectAttempts: 1, TimestampStringLocation: time.UTC,
		})
		defer syncer.Close()
		set, err := gomysql.ParseMysqlGTIDSet(executed)
		if err != nil {
			t.Fatal(err)
		}
		streamer, err := syncer.StartSyncGTID(set)
		if err != nil {
			return err
		}
		gctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		for {
			if _, err := streamer.GetEvent(gctx); err != nil {
				if errors.Is(err, context.DeadlineExceeded) {
					return nil // served: events, then waiting for more
				}
				return err
			}
		}
	}
	anchor := time.Now().Add(-3 * time.Hour)
	for _, c := range []struct {
		mode    string
		refused bool
		detail  string
	}{
		{"ON", false, captureDetailBehind},
		{"ON_PERMISSIVE", true, captureDetailGTIDOnPermissive},
		{"OFF_PERMISSIVE", true, captureDetailGTIDOffPermissive},
		{"OFF", true, captureDetailGTIDModeOff},
	} {
		t.Run(c.mode, func(t *testing.T) {
			setMode(c.mode)
			// The source keeps writing in every mode.
			testutil.MustExec(t, sdb, "INSERT INTO p2674.t (at) VALUES (NOW(6))")
			err := captureByGTID()
			t.Logf("gtid_mode=%s: the source answered capture's request with: %v", c.mode, err)
			if c.refused != (err != nil) {
				t.Fatalf("gtid_mode=%s: capture by GTID refused=%v (%v), want refused=%v", c.mode, err != nil, err, c.refused)
			}
			if c.refused && !strings.Contains(err.Error(), "AUTO_POSITION") {
				t.Fatalf("gtid_mode=%s: refused for another reason than auto-position: %v", c.mode, err)
			}
			r := captureFromDBs(ctx, indexDSN, src, config.SSL{}, anchor, time.Time{}, nil)
			t.Logf("gtid_mode=%s: the probe says verdict=%q detail=%q", c.mode, r.verdict, r.detail)
			if r.detail != c.detail {
				t.Fatalf("gtid_mode=%s: probe %+v, want the detail %q", c.mode, r, c.detail)
			}
			if c.refused && captureClassOf(r.detail) != captureEvidence {
				t.Fatalf("gtid_mode=%s: %q is not decided as evidence (the full read)", c.mode, r.detail)
			}
			if !c.refused && r.verdict != console.CaptureBehind {
				t.Fatalf("gtid_mode=ON with a write past the checkpoint: verdict %q, want behind", r.verdict)
			}
		})
	}
}
