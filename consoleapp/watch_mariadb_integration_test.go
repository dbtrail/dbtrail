//go:build integration

package consoleapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestIntegrationWatchMariaDBSource runs `bintrail-console watch` end to end
// with a MariaDB main source and no --source-flavor: runWatch itself, with
// its preflight, init, schema snapshot, stream and console, against the real
// MySQL index and the real MariaDB source. A row written on the source must
// come back through the console's own events API, and the console must label
// the daemon's source MariaDB: before, the boot entry of a MariaDB source had
// no flavor and read as MySQL on every screen.
func TestIntegrationWatchMariaDBSource(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	sourceDB, sourceName := testutil.CreateTestMariaDB(t)
	var logBin string
	if err := sourceDB.QueryRow("SELECT @@log_bin").Scan(&logBin); err != nil || logBin != "1" {
		testutil.SkipOrFailMariaDB(t, "binary logging not enabled on test MariaDB")
	}
	testutil.MustExec(t, sourceDB, "CREATE TABLE orders (id INT PRIMARY KEY, note VARCHAR(32))")

	watchWiringSetup(t) // index database, env, telemetry sink, flag globals
	origFlavor, origSchemas, origServerID := upSourceFlavor, upSchemas, upServerID
	t.Cleanup(func() { upSourceFlavor, upSchemas, upServerID = origFlavor, origSchemas, origServerID })
	upSourceDSN = testutil.MariaDBBaseDSN() + "/"
	upSourceFlavor = "" // detection decides, as on the compose install
	upSchemas = sourceName
	upServerID = 0

	// A free port for the console, so the test can reach its API.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	upConsoleListen = addr

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	watchCmd.SetContext(ctx)
	done := make(chan error, 1)
	go func() { done <- runWatch(watchCmd, nil) }()
	defer func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("runWatch returned an error on clean shutdown: %v", err)
			}
		case <-time.After(60 * time.Second):
			t.Error("runWatch did not return after cancel")
		}
	}()

	get := func(path string, out any) error {
		req, _ := http.NewRequest("GET", "http://"+addr+path, nil)
		req.Header.Set("Authorization", "Bearer "+upConsoleToken)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			return fmt.Errorf("%s: status %d", path, resp.StatusCode)
		}
		return json.NewDecoder(resp.Body).Decode(out)
	}
	waitFor := func(what string, within time.Duration, ok func() (bool, string)) {
		t.Helper()
		deadline := time.Now().Add(within)
		for {
			select {
			case err := <-done:
				t.Fatalf("runWatch exited while waiting for %s: %v", what, err)
			default:
			}
			good, last := ok()
			if good {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("no %s within %s (last: %s)", what, within, last)
			}
			time.Sleep(500 * time.Millisecond)
		}
	}

	// Capture runs as MariaDB, detected from the server.
	indexDB, err := sql.Open("mysql", upIndexDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer indexDB.Close()
	waitFor("MariaDB checkpoint", 120*time.Second, func() (bool, string) {
		var f string
		err := indexDB.QueryRow("SELECT flavor FROM stream_state WHERE id = 1").Scan(&f)
		return err == nil && f == "mariadb", fmt.Sprintf("flavor %q, err %v", f, err)
	})

	// A write on the source comes back through the console.
	testutil.MustExec(t, sourceDB, "INSERT INTO orders VALUES (1, 'from mariadb')")
	waitFor("the row in the console's events", 60*time.Second, func() (bool, string) {
		var resp struct {
			Events []struct {
				SchemaName string `json:"schema_name"`
				TableName  string `json:"table_name"`
				EventType  string `json:"event_type"`
			} `json:"events"`
		}
		err := get("/api/events?schema="+url.QueryEscape(sourceName)+"&table=orders", &resp)
		if err != nil {
			return false, err.Error()
		}
		for _, e := range resp.Events {
			if e.SchemaName == sourceName && e.TableName == "orders" && e.EventType == "INSERT" {
				return true, ""
			}
		}
		return false, fmt.Sprintf("%d events", len(resp.Events))
	})

	// The daemon's own source is labeled with the flavor capture detected.
	var servers struct {
		Servers []struct {
			ID     string `json:"id"`
			Flavor string `json:"flavor"`
		} `json:"servers"`
	}
	if err := get("/api/servers", &servers); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, s := range servers.Servers {
		if s.ID == bootCaptureServerID {
			found = true
			if s.Flavor != "mariadb" {
				t.Errorf("the daemon's source is listed with flavor %q, want mariadb", s.Flavor)
			}
		}
	}
	if !found {
		t.Errorf("the daemon's source is not in the server list: %+v", servers.Servers)
	}
}
