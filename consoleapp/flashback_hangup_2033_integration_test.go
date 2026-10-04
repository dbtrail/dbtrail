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

// #2033 (prerequisite of a wait instead of a refusal): a client that hangs up
// in the middle of a statement on the embedded port frees that server's slot
// at once. Before, nothing read the socket while the statement ran, so the
// worker kept going for a reader that was gone, and every other client of
// that server got 1203 until the 60-second cap.
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
	ask := func() error {
		var n int
		return b.QueryRow("SELECT count(*) AS n FROM orders").Scan(&n)
	}
	// Control: while a runs, b is refused, so a really holds the slot.
	held := false
	var lastB error
	for deadline := time.Now().Add(20 * time.Second); time.Now().Before(deadline); time.Sleep(100 * time.Millisecond) {
		lastB = ask()
		if mysqlCode(lastB) == 1203 {
			held = true
			break
		}
	}
	if !held {
		aState := "still running"
		select {
		case err := <-aDone:
			aState = fmt.Sprintf("ended after %v with %v", time.Since(aStart).Round(time.Millisecond), err)
		default:
		}
		t.Fatalf("control: the long statement never held the slot (a: %s; b's last answer: %v); the test below would prove nothing", aState, lastB)
	}

	// a's client hangs up: the driver closes the socket on cancel.
	aLeave()
	<-aDone
	freed := time.Now()
	for deadline := freed.Add(10 * time.Second); ; time.Sleep(100 * time.Millisecond) {
		err := ask()
		if err == nil {
			t.Logf("slot free %v after the client left", time.Since(freed).Round(time.Millisecond))
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("10 s after the client hung up the slot is still held (last: %v)", err)
		}
	}
}
