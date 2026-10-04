//go:build integration

package consoleapp

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2033: a client that hangs up in the middle of a statement on the embedded
// port frees that server's slot at once, and a statement waiting in line for
// it runs. Before, nothing read the socket while the statement ran, so the
// worker kept going for a reader that was gone, and every other client of
// that server got 1203 (now: waited) until the 60-second cap.
func TestIntegrationFlashbackHangUpFreesTheSlot_2033(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	now := time.Now().UTC().Truncate(time.Hour)
	dsn := seedFlashbackIndex(t, "alice", now)
	baseDir := t.TempDir()
	writeFreeSQLBaseline(t, baseDir)
	reg, err := console.LoadRegistry(t.TempDir() + "/servers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	ent, err := reg.Add(console.ServerEntry{Name: "srva", DSN: dsn, SourceDSN: "r:p@tcp(x:3306)/shop", BaselineDir: baseDir})
	if err != nil {
		t.Fatal(err)
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
	go func() { _ = serveFlashback(ctx, srv, ln, flashbackConfig{}); close(served) }()
	defer func() { cancel(); <-served }()
	addr := ln.Addr().String()

	// A statement that runs far longer than this test: it holds the server's
	// one slot until its client leaves.
	a := openFlashback(t, addr, ent.ID, "tok", "")
	defer a.Close()
	aCtx, aLeave := context.WithCancel(context.Background())
	aDone := make(chan error, 1)
	aStart := time.Now()
	go func() {
		rows, err := a.QueryContext(aCtx, "SELECT sum(i * i) FROM range(100000000000) t(i)")
		if err == nil {
			rows.Close()
		}
		aDone <- err
	}()

	b := openFlashback(t, addr, ent.ID, "tok", "")
	defer b.Close()
	// Give a's statement the slot first.
	time.Sleep(2 * time.Second)
	// b's statement needs the same server's slot, so it waits in line
	// (#2033) for as long as a runs.
	bDone := make(chan error, 1)
	go func() {
		var n int
		bDone <- b.QueryRow("SELECT count(*) AS n FROM orders").Scan(&n)
	}()
	// Control: b is still waiting 3 s later, so a really holds the slot.
	select {
	case err := <-bDone:
		aState := "still running"
		select {
		case aErr := <-aDone:
			aState = fmt.Sprintf("ended after %v with %v", time.Since(aStart).Round(time.Millisecond), aErr)
		default:
		}
		t.Fatalf("control: b finished (%v) while a should hold the slot (a: %s); the test below would prove nothing", err, aState)
	case <-time.After(3 * time.Second):
	}

	// a's client hangs up: the driver closes the socket on cancel. b, which
	// was waiting, gets the slot at once and is served.
	aLeave()
	<-aDone
	freed := time.Now()
	select {
	case err := <-bDone:
		if err != nil {
			t.Fatalf("b after a left: %v, want it served", err)
		}
		t.Logf("b served %v after a's client left", time.Since(freed).Round(time.Millisecond))
	case <-time.After(10 * time.Second):
		t.Fatal("10 s after a's client hung up, b is still waiting: the slot was not freed")
	}
}
