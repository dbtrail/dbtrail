//go:build integration

package consoleapp

import (
	"context"
	"database/sql"
	"net"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/readrouter"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// sslCipher asks a connection which TLS cipher it is on; "" is cleartext.
func sslCipher(t *testing.T, db *sql.DB) string {
	t.Helper()
	var name string
	var cipher sql.NullString
	if err := db.QueryRow("SHOW SESSION STATUS LIKE 'Ssl_cipher'").Scan(&name, &cipher); err != nil {
		t.Fatalf("read Ssl_cipher: %v", err)
	}
	return cipher.String
}

// TestIntegrationFlashbackForwardsOverTheServersTLS: what a client sends to
// the routed port reaches the source over the TLS that server's capture
// uses. The source here offers TLS, as MySQL does out of the box: with the
// entry's default mode (preferred) and with required the port's connection
// is encrypted, exactly like a capture-style connection to the same source;
// only an entry that says disabled goes in clear.
func TestIntegrationFlashbackForwardsOverTheServersTLS(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	now := time.Now().UTC().Truncate(time.Hour)
	indexDSN := seedFlashbackIndex(t, "alice", now)
	srcDB, srcName := testutil.CreateTestDB(t)
	if _, err := srcDB.Exec("CREATE TABLE orders (id INT NOT NULL PRIMARY KEY, status VARCHAR(32))"); err != nil {
		t.Fatal(err)
	}
	sourceDSN := testutil.IntegrationDSN(srcName)
	baseDir := t.TempDir()
	writeRoutingBaseline(t, baseDir, srcName, time.Minute, 2)
	reg, err := console.LoadRegistry(t.TempDir() + "/servers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	ids := map[string]string{}
	for name, mode := range map[string]string{"default": "", "required": "required", "disabled": "disabled"} {
		e, err := reg.Add(console.ServerEntry{Name: "tls-" + name, DSN: indexDSN, SourceDSN: sourceDSN, BaselineDir: baseDir, SSLMode: mode})
		if err != nil {
			t.Fatal(err)
		}
		ids[name] = e.ID
	}
	srv, err := console.New(console.Config{Listen: "127.0.0.1:0", Token: "tok", Registry: reg})
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan struct{})
	go func() {
		_ = serveFlashback(ctx, srv, ln, flashbackConfig{RouteMaxCopyAge: time.Hour, RoutePolicy: readrouter.Policy{ScanRows: 2}})
		close(served)
	}()
	t.Cleanup(func() { cancel(); <-served })

	// What capture's own kind of connection gets from this source.
	capture, fellBack, err := connectSourceCleartext(sourceDSN, console.ServerEntry{}.SourceSSL(), 0)
	if err != nil {
		t.Fatal(err)
	}
	captureCipher := sslCipher(t, capture)
	capture.Close()
	if captureCipher == "" || fellBack {
		t.Fatalf("the test source offers no TLS to a capture-style connection (cipher %q, fell back %v): this test cannot tell encrypted from clear", captureCipher, fellBack)
	}

	for name, wantTLS := range map[string]bool{"default": true, "required": true, "disabled": false} {
		conn := openFlashback(t, ln.Addr().String(), ids[name], "tok", srcName)
		got := sslCipher(t, conn)
		conn.Close()
		t.Logf("entry ssl_mode %-8s: the port's connection to the source has Ssl_cipher=%q (capture-style: %q)", name, got, captureCipher)
		if wantTLS != (got != "") {
			t.Errorf("entry ssl_mode %s: Ssl_cipher=%q through the port, want encrypted=%v", name, got, wantTLS)
		}
	}
}

// TestIntegrationForwarderTLSMariaDBSource: the same rule against a MariaDB
// source, at the forwarder itself.
func TestIntegrationForwarderTLSMariaDBSource(t *testing.T) {
	testutil.SkipIfNoMariaDB(t)
	dsn := testutil.MariaDBBaseDSN() + "/"
	for mode, wantTLS := range map[string]bool{"preferred": true, "required": true, "disabled": false} {
		fw, err := readrouter.NewForwarder(dsn, config.SSL{Mode: mode}, readrouter.DefaultPolicy(), 10*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		buf := &readrouter.BufferSink{}
		if _, err := fw.Forward(context.Background(), "SHOW SESSION STATUS LIKE 'Ssl_cipher'", buf); err != nil {
			t.Fatalf("%s: %v", mode, err)
		}
		fw.Close()
		cipher, _ := buf.Rows[0][1].([]byte)
		t.Logf("MariaDB, ssl_mode %-9s: Ssl_cipher=%q", mode, cipher)
		if wantTLS != (len(cipher) > 0) {
			t.Errorf("MariaDB, ssl_mode %s: Ssl_cipher=%q, want encrypted=%v", mode, cipher, wantTLS)
		}
	}
}
