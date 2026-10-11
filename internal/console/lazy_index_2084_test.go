package console

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// #2084: a process whose main work does not need a server's index can
// select that server while the index is away. Nothing listens on the index
// address below.
func lazyIndexServer(t *testing.T, lazy bool) *Server {
	t.Helper()
	clearStores(t)
	reg, err := LoadRegistry(filepath.Join(t.TempDir(), "console-servers.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reg.Add(ServerEntry{Name: "prod", DSN: "u:secretpw@tcp(127.0.0.1:1)/idx?timeout=2s",
		SourceDSN: "repl:pw@tcp(db.prod:3306)/app", BaselineDir: t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "t", Registry: reg, LazyIndex: lazy})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.cm.CloseAll)
	return srv
}

// Unset, a server whose index does not answer cannot be selected, as before.
func TestLazyIndex_unsetAnIndexThatIsAwayRefusesTheServer(t *testing.T) {
	srv := lazyIndexServer(t, false)
	_, err := srv.ResolveFlashback(context.Background(), "prod")
	if err == nil || errors.Is(err, ErrUnknownServer) || strings.Contains(err.Error(), "secretpw") {
		t.Fatalf("with the index away: %v, want a failure to open it that does not carry the password", err)
	}
}

// Set, the server is selected with everything the port needs that does not
// come from the index, and what does read the index fails with the reason.
func TestLazyIndex_theServerIsSelectedAndOnlyIndexReadsFail(t *testing.T) {
	srv := lazyIndexServer(t, true)
	tgt, err := srv.ResolveFlashback(context.Background(), "prod")
	if err != nil {
		t.Fatalf("with the index away: %v", err)
	}
	if tgt.ForwardDSN != "repl:pw@tcp(db.prod:3306)/app" || tgt.DefaultSchema != "app" || tgt.BaselineDir == "" || tgt.SQL == nil || tgt.IndexDBName != "idx" || tgt.IndexDB == nil {
		t.Errorf("the target: forward %q, schema %q, baseline %q, sql %v, index %q", tgt.ForwardDSN, tgt.DefaultSchema, tgt.BaselineDir, tgt.SQL != nil, tgt.IndexDBName)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = tgt.IndexDB.PingContext(ctx)
	if err == nil {
		t.Fatal("the index answered; nothing should listen on its address")
	}
	// Selected again, it is the same server and still opens nothing.
	again, err := srv.ResolveFlashback(context.Background(), "prod")
	if err != nil || again.IndexDB != tgt.IndexDB {
		t.Errorf("selected again: %v, a new connection pool = %v", err, again.IndexDB != tgt.IndexDB)
	}
	// A DSN that is not one is still refused at once, lazy or not.
	if _, err := srv.cm.reg.Add(ServerEntry{Name: "broken", DSN: "u:p@tcp(127.0.0.1:1)/"}); err != nil {
		t.Fatal(err)
	}
	if _, err := srv.ResolveFlashback(context.Background(), "broken"); err == nil || !strings.Contains(err.Error(), "database name") {
		t.Errorf("an index DSN with no database: %v", err)
	}
}
