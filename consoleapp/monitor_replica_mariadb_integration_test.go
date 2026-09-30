//go:build integration

package consoleapp

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestIntegrationReplicaCheckMariaDB runs the replica / duplicate card against
// real MariaDB servers: the source and the drill target of the MariaDB job,
// two unrelated servers that both run with the default server_id and write
// domain 0. The card must not call them related, must find one server added
// twice under two addresses, must read a replication channel, and must say
// when the user it connects with cannot read replication status.
func TestIntegrationReplicaCheckMariaDB(t *testing.T) {
	testutil.SkipIfNoMariaDB(t)
	scratch := os.Getenv("BINTRAIL_TEST_MARIADB_SCRATCH_DSN")
	if scratch == "" {
		testutil.SkipOrFailMariaDB(t, "BINTRAIL_TEST_MARIADB_SCRATCH_DSN is not set: this test needs a second MariaDB")
	}
	source := testutil.MariaDBBaseDSN()
	ctx := context.Background()

	check := func(t *testing.T, candDSN string, peers ...console.ServerEntry) *console.DoctorCheck {
		t.Helper()
		reg, err := console.LoadRegistry(filepath.Join(t.TempDir(), "servers.yaml"))
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range peers {
			p.DSN = "u:p@tcp(127.0.0.1:1)/idx" // never read by the MariaDB check
			if _, err := reg.Add(p); err != nil {
				t.Fatal(err)
			}
		}
		m := &monitorSupervisor{registry: reg}
		card := m.replicaOverlapCheck(ctx, console.ServerEntry{ID: "candidate", Name: "candidate", SourceDSN: candDSN, Flavor: console.FlavorMySQL})
		if card == nil {
			t.Fatal("no card")
		}
		t.Logf("%s: %s", card.Status, card.Detail)
		return card
	}

	t.Run("two unrelated servers", func(t *testing.T) {
		card := check(t, source+"/", console.ServerEntry{Name: "scratch", SourceDSN: scratch + "/"})
		if card.Status != "pass" || strings.Contains(card.Detail, "could not be verified") {
			t.Errorf("card = %+v, want a clean pass", *card)
		}
	})

	t.Run("one server under two addresses", func(t *testing.T) {
		cfg, err := mysql.ParseDSN(source + "/")
		if err != nil {
			t.Fatal(err)
		}
		host, port, _ := strings.Cut(cfg.Addr, ":")
		switch host {
		case "127.0.0.1":
			cfg.Addr = "localhost:" + port
		case "localhost":
			cfg.Addr = "127.0.0.1:" + port
		default:
			testutil.SkipOrFailMariaDB(t, "the MariaDB source is not on this machine (%s): no second address to reach it by", cfg.Addr)
		}
		card := check(t, source+"/", console.ServerEntry{Name: "again", SourceDSN: cfg.FormatDSN()})
		if card.Status != "warn" || !strings.Contains(card.Detail, `is the same server as already-monitored "again"`) {
			t.Errorf("card = %+v, want the same server found", *card)
		}
	})

	t.Run("a replication channel is read", func(t *testing.T) {
		db, err := config.Connect(scratch + "/")
		if err != nil {
			t.Fatal(err)
		}
		// Closed by a cleanup, not a defer: the cleanups below use it, and a
		// deferred Close runs before them, which fails them in silence.
		t.Cleanup(func() { db.Close() })
		// A channel toward the monitored source itself, by its own hostname
		// and port (what channelHostIs matches), never started: it never
		// connects, so Master_Server_Id stays 0. The host and port match; only
		// the server id rule keeps it from being called a replica.
		srcDB, err := config.Connect(source + "/")
		if err != nil {
			t.Fatal(err)
		}
		srcIdentity, err := loadMariaDBServer(ctx, srcDB, console.FlavorMariaDB)
		srcDB.Close()
		if err != nil || srcIdentity.hostname == "" || srcIdentity.port == 0 {
			t.Fatalf("source identity = %+v, err %v", srcIdentity, err)
		}
		testutil.MustExec(t, db, fmt.Sprintf("CHANGE MASTER 'dbtrail_probe' TO MASTER_HOST='%s', MASTER_PORT=%d, MASTER_USER='nobody', MASTER_PASSWORD='x'",
			srcIdentity.hostname, srcIdentity.port))
		t.Cleanup(func() {
			if _, err := db.Exec("RESET SLAVE 'dbtrail_probe' ALL"); err != nil {
				t.Errorf("remove the probe channel: %v", err)
			}
		})
		s, err := loadMariaDBServer(ctx, db, console.FlavorMariaDB)
		if err != nil {
			t.Fatal(err)
		}
		if !s.channelsRead || s.hostname == "" || s.port == 0 || s.startedAt == 0 || s.uuidNode == "" {
			t.Fatalf("read = %+v", s)
		}
		var seen bool
		for _, ch := range s.channels {
			if ch.host == srcIdentity.hostname && ch.port == srcIdentity.port && ch.serverID == 0 {
				seen = true
			}
		}
		if !seen {
			t.Errorf("channels = %+v, want the probe channel", s.channels)
		}
		card := check(t, scratch+"/", console.ServerEntry{Name: "source", SourceDSN: source + "/"})
		if card.Status != "pass" || !strings.Contains(card.Detail, "1 could not be verified") {
			t.Errorf("card = %+v, want pass with the source not verified: a channel toward it that never connected proves nothing either way", *card)
		}
	})

	t.Run("a user without SLAVE MONITOR", func(t *testing.T) {
		db, err := config.Connect(source + "/")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { db.Close() }) // after the DROP USER cleanup below

		testutil.MustExec(t, db, "DROP USER IF EXISTS 'dbtrail_nomon'@'%'")
		testutil.MustExec(t, db, "CREATE USER 'dbtrail_nomon'@'%' IDENTIFIED BY 'nomon'")
		testutil.MustExec(t, db, "GRANT REPLICATION SLAVE, REPLICATION CLIENT, SELECT ON *.* TO 'dbtrail_nomon'@'%'")
		t.Cleanup(func() {
			if _, err := db.Exec("DROP USER IF EXISTS 'dbtrail_nomon'@'%'"); err != nil {
				t.Errorf("drop the test user: %v", err)
			}
		})
		cfg, err := mysql.ParseDSN(source + "/")
		if err != nil {
			t.Fatal(err)
		}
		cfg.User, cfg.Passwd = "dbtrail_nomon", "nomon"
		card := check(t, cfg.FormatDSN(), console.ServerEntry{Name: "scratch", SourceDSN: scratch + "/"})
		if card.Status != "skip" || !strings.Contains(card.Remediation, "SLAVE MONITOR") {
			t.Errorf("card = %+v, want a skip that names SLAVE MONITOR", *card)
		}
	})
}
