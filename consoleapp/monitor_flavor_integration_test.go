//go:build integration

package consoleapp

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// startRegistrySource provisions and starts a supervised source through the
// real stream (no stub), and removes its derived index database afterwards.
func startRegistrySource(t *testing.T, ctx context.Context, sup *monitorSupervisor, bootDSN string, entry console.ServerEntry) console.ServerEntry {
	t.Helper()
	derived, err := sup.DeriveIndexDSN(entry.ID)
	if err != nil {
		t.Fatal(err)
	}
	entry.DSN = derived
	t.Cleanup(func() {
		if db, err := sql.Open("mysql", bootDSN); err == nil {
			_, _ = db.Exec("DROP DATABASE IF EXISTS bintrail_idx_" + entry.ID)
			_ = db.Close()
		}
	})
	if err := sup.Start(ctx, entry); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = sup.Stop(context.Background(), entry.ID) })
	return entry
}

// TestIntegrationMonitorRegistrySourceMariaDBDetected: a server added from the
// console with the form's default Source type ("mysql") that is really a
// MariaDB. The supervised stream asks the server, captures as mariadb, records
// it in stream_state, tells the source jobs mariadb, and warns on the status. Before, it captured
// as mysql: no statement text, and on 11.4+ a crash loop on zero positions.
func TestIntegrationMonitorRegistrySourceMariaDBDetected(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	sourceDB, sourceName := testutil.CreateTestMariaDB(t)
	var logBin string
	if err := sourceDB.QueryRow("SELECT @@log_bin").Scan(&logBin); err != nil || logBin != "1" {
		testutil.SkipOrFailMariaDB(t, "binary logging not enabled on test MariaDB")
	}
	testutil.MustExec(t, sourceDB, "CREATE TABLE t (id INT PRIMARY KEY)")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, bootName := testutil.CreateTestDB(t)
	bootDSN := testutil.IntegrationDSN(bootName)
	sup := newMonitorSupervisor(ctx, bootDSN, nil, 0)

	sentinel := fmt.Sprintf("flvmaria%d", time.Now().UnixNano()%1e9)
	infoCh, _ := probeSourceJob(t, sentinel)
	entry := startRegistrySource(t, ctx, sup, bootDSN, console.ServerEntry{
		ID:        sentinel,
		Name:      "flavor-mariadb",
		SourceDSN: testutil.MariaDBBaseDSN() + "/" + sourceName,
		Schemas:   sourceName,
		Flavor:    console.FlavorMySQL,
	})

	select {
	case got := <-infoCh:
		if got.Flavor != console.FlavorMariaDB {
			t.Fatalf("source job flavor = %q, want the detected mariadb", got.Flavor)
		}
	case <-time.After(90 * time.Second):
		t.Fatalf("source job never fired; monitor status: %+v", sup.Status(entry.ID))
	}

	indexDB, err := sql.Open("mysql", entry.DSN)
	if err != nil {
		t.Fatal(err)
	}
	defer indexDB.Close()
	deadline := time.Now().Add(60 * time.Second)
	for {
		var f string
		err := indexDB.QueryRow("SELECT flavor FROM stream_state WHERE id = 1").Scan(&f)
		if err == nil && f == console.FlavorMariaDB {
			// Saved with the form's default MySQL: captured as MariaDB, and
			// the status says the saved type is wrong, without failing.
			if st := sup.Status(entry.ID); !strings.Contains(st.FlavorWarning, "saved with Source type MySQL, but the server reports MariaDB") || st.State == "failed" {
				t.Errorf("status = %+v, want a warning and no failure", st)
			}
			break
		}
		if err == nil && f != "" && f != console.FlavorMariaDB {
			t.Fatalf("stream_state.flavor = %q, want mariadb", f)
		}
		if time.Now().After(deadline) {
			t.Fatalf("no mariadb checkpoint within 60s (last err %v); monitor status: %+v", err, sup.Status(entry.ID))
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// TestIntegrationMonitorRegistryFlavorHintContradicted: a server saved as
// MariaDB that is really MySQL. The saved Source type is only a hint: the
// supervised stream captures it as MySQL, the source jobs are told mysql, the
// server's status carries a warning, and the stream never fails for it.
func TestIntegrationMonitorRegistryFlavorHintContradicted(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, bootName := testutil.CreateTestDB(t)
	bootDSN := testutil.IntegrationDSN(bootName)
	sourceDB, sourceName := testutil.CreateTestDB(t)
	testutil.MustExec(t, sourceDB, "CREATE TABLE t (id INT PRIMARY KEY)")
	sup := newMonitorSupervisor(ctx, bootDSN, nil, 0)

	sentinel := fmt.Sprintf("flvmysql%d", time.Now().UnixNano()%1e9)
	infoCh, _ := probeSourceJob(t, sentinel)
	entry := startRegistrySource(t, ctx, sup, bootDSN, console.ServerEntry{
		ID:        sentinel,
		Name:      "flavor-hint",
		SourceDSN: testutil.BaseDSN() + "/" + sourceName,
		Schemas:   sourceName,
		Flavor:    console.FlavorMariaDB,
	})

	select {
	case got := <-infoCh:
		if got.Flavor != console.FlavorMySQL {
			t.Fatalf("source job flavor = %q, want the detected mysql", got.Flavor)
		}
	case <-time.After(90 * time.Second):
		t.Fatalf("source job never fired; monitor status: %+v", sup.Status(entry.ID))
	}

	indexDB, err := sql.Open("mysql", entry.DSN)
	if err != nil {
		t.Fatal(err)
	}
	defer indexDB.Close()
	deadline := time.Now().Add(60 * time.Second)
	for {
		st := sup.Status(entry.ID)
		if st.State == "failed" {
			t.Fatalf("a contradicted Source type failed the stream: %+v", st)
		}
		var f string
		qerr := indexDB.QueryRow("SELECT flavor FROM stream_state WHERE id = 1").Scan(&f)
		if qerr == nil && f == console.FlavorMySQL && st.State == "running" {
			t.Logf("warning shown: %s", st.FlavorWarning)
			if !strings.Contains(st.FlavorWarning, "saved with Source type MariaDB, but the server reports MySQL") {
				t.Errorf("FlavorWarning = %q, want the contradiction", st.FlavorWarning)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no running mysql capture within 60s (flavor %q, err %v); status %+v", f, qerr, st)
		}
		time.Sleep(250 * time.Millisecond)
	}
}
