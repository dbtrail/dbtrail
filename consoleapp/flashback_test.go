package consoleapp

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/shim"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// This file mutates up* package globals via save-and-restore. DO NOT add
// t.Parallel() here — see watch_test.go's note.

// newFlashbackConsole builds a console.Server with the given token for the
// serving-layer tests (creds, serveFlashback). A loopback listen keeps a
// no-token server legal (first-run setup).
func newFlashbackConsole(t *testing.T, token string) *console.Server {
	t.Helper()
	srv, err := console.New(console.Config{Listen: "127.0.0.1:0", Token: token})
	if err != nil {
		t.Fatal(err)
	}
	return srv
}

// TestFlashbackCreds: auth is the token alone. Every username — known or not —
// gets the same token, so the handshake error code cannot leak which usernames
// name a real server. An empty token uniformly denies.
func TestFlashbackCreds(t *testing.T) {
	creds := flashbackCreds{srv: newFlashbackConsole(t, "sekret")}
	for _, u := range []string{"someid", "alpha", "", "ghost"} {
		cred, found, _ := creds.GetCredential(u)
		if !found || len(cred.Passwords) != 1 || cred.Passwords[0] != "sekret" {
			t.Fatalf("GetCredential(%q) = (%q,%v), want ([sekret],true)", u, cred.Passwords, found)
		}
		// The credential's plugin must default to native — it has to match
		// the method shim.NewMySQLServer("") builds the port with, or
		// go-mysql auth-switches every client.
		if cred.AuthPluginName != gomysql.AUTH_NATIVE_PASSWORD {
			t.Fatalf("GetCredential(%q) plugin = %q, want %q", u, cred.AuthPluginName, gomysql.AUTH_NATIVE_PASSWORD)
		}
	}
	credsNoTok := flashbackCreds{srv: newFlashbackConsole(t, "")}
	if _, found, _ := credsNoTok.GetCredential("x"); found {
		t.Fatal("no token must never authorise a passwordless handshake")
	}
}

// TestRoutingHandlerUnresolved: with no bound handler, HandleQuery errors
// generically; once routing has set a fail, BOTH verbs return it verbatim.
func TestRoutingHandlerUnresolved(t *testing.T) {
	r := &routingHandler{}
	if _, err := r.HandleQuery("SELECT 1"); err == nil {
		t.Fatal("HandleQuery with nil inner must error")
	}
	want := gomysql.NewError(gomysql.ER_BAD_DB_ERROR, "boom")
	r.fail = want
	if _, err := r.HandleQuery("SELECT 1"); err != want {
		t.Fatalf("HandleQuery returned %v, want the stored fail", err)
	}
	if err := r.UseDB("x"); err != want {
		t.Fatalf("UseDB returned %v, want the stored fail (unresolvable connection must reject USE too)", err)
	}
}

// TestRoutingHandlerPendingDB: a UseDB before inner is bound (the go-mysql
// handshake path) is stashed and does not fail the connection.
func TestRoutingHandlerPendingDB(t *testing.T) {
	r := &routingHandler{}
	if err := r.UseDB("shopdb"); err != nil {
		t.Fatalf("pre-bind UseDB must not error (handshake would abort): %v", err)
	}
	if r.pendingDB != "shopdb" {
		t.Fatalf("pendingDB = %q, want shopdb", r.pendingDB)
	}
}

// TestFlashbackConfigWithDefaults pins the zero→default substitution — notably
// QueryTimeout, which must never reach the query path as 0 (the only runaway
// backstop) — and that explicit values are preserved.
func TestFlashbackConfigWithDefaults(t *testing.T) {
	got := flashbackConfig{}.withDefaults()
	if got.QueryTimeout != defaultFlashbackQueryTimeout {
		t.Fatalf("QueryTimeout default = %v, want %v", got.QueryTimeout, defaultFlashbackQueryTimeout)
	}
	if got.MaxFullTable != defaultFlashbackMaxFullTable {
		t.Fatalf("MaxFullTable default = %d, want %d", got.MaxFullTable, defaultFlashbackMaxFullTable)
	}
	// #2241: a port always has a bound on the results it holds while
	// sending them, of four of the largest result one statement returns.
	if got.ResultBudget.Max() != 4*sqlsandbox.DefaultLimits().MaxResultBytes {
		t.Fatalf("ResultBudget default = %d bytes, want four results of %d", got.ResultBudget.Max(), sqlsandbox.DefaultLimits().MaxResultBytes)
	}
	own := shim.NewResultBudget(1)
	custom := flashbackConfig{QueryTimeout: time.Second, MaxFullTable: 9, ResultBudget: own}.withDefaults()
	if custom.QueryTimeout != time.Second || custom.MaxFullTable != 9 || custom.ResultBudget != own {
		t.Fatalf("explicit values overwritten: %+v", custom)
	}
}

// budgetTestFreeSQL is a copy that answers one row.
type budgetTestFreeSQL struct{ routeTestFreeSQL }

func (budgetTestFreeSQL) Run(context.Context, string, string, sqlsandbox.Session) (sqlsandbox.Result, error) {
	return sqlsandbox.Result{Columns: []sqlsandbox.Column{{Name: "x", Type: "VARCHAR"}}, Rows: [][]any{{strings.Repeat("a", 1000)}}}, nil
}

// The port's serving loop asks the handler it was given to release the
// result it counted, and that handler is the routing wrapper: it must pass
// the call to the handler that did the counting, or the budget fills up for
// good and the port refuses every statement (#2241).
func TestRoutingHandlerReleasesTheInnerHandlersResult(t *testing.T) {
	b := shim.NewResultBudget(1 << 20)
	inner := shim.NewHandlerWithConfig(nil, shim.Config{ResultBudget: b}, nil)
	inner.BindFreeSQL(budgetTestFreeSQL{})
	proxy := &routingHandler{inner: inner}
	if _, err := proxy.HandleQuery("SELECT x FROM t"); err != nil {
		t.Fatal(err)
	}
	if b.Held() < 1000 {
		t.Fatalf("held = %d after a 1000-byte result, want it counted", b.Held())
	}
	rel, ok := any(proxy).(interface{ ReleaseResult() })
	if !ok {
		t.Fatal("routingHandler has no ReleaseResult: shim.Session would never release a result")
	}
	rel.ReleaseResult()
	if b.Held() != 0 {
		t.Errorf("held = %d after the release, want 0", b.Held())
	}
	(&routingHandler{}).ReleaseResult() // not bound yet: nothing to release
}

// TestServeFlashbackRequiresToken: the port refuses to start without a token,
// since MySQL-protocol auth cannot be driven by the bcrypt password store.
func TestServeFlashbackRequiresToken(t *testing.T) {
	err := serveFlashback(context.Background(), newFlashbackConsole(t, ""), nil, flashbackConfig{})
	if err == nil || !strings.Contains(err.Error(), "token") {
		t.Fatalf("empty token: err = %v, want a token-required error", err)
	}
}

// TestServeFlashbackDrainsActiveConn: serveFlashback returns only after an
// IN-FLIGHT connection has drained when ctx is cancelled — pinning the
// wg.Add/wg.Wait drain (a naive no-connection test would pass even without it).
func TestServeFlashbackDrainsActiveConn(t *testing.T) {
	srv := newFlashbackConsole(t, "tok")
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- serveFlashback(ctx, srv, ln, flashbackConfig{}) }()

	// Read the server's handshake greeting to confirm the accept loop spawned an
	// in-flight handler goroutine (wg > 0) before cancelling. The deadline and
	// the drain wait below are failure detectors only — on the green path both
	// resolve immediately — so they are generous: at 5s a machine loaded with a
	// full parallel suite plus build/vet starved them into spurious failures
	// (#1164).
	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	if _, err := conn.Read(make([]byte, 1)); err != nil {
		t.Fatalf("expected a handshake greeting from the flashback port: %v", err)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("serveFlashback did not drain the in-flight connection on ctx cancel")
	}
}

func TestNextFlashbackBackoff(t *testing.T) {
	if got := nextFlashbackBackoff(0); got != initialFlashbackBackoff {
		t.Fatalf("seed = %v, want %v", got, initialFlashbackBackoff)
	}
	if got := nextFlashbackBackoff(initialFlashbackBackoff); got != 2*initialFlashbackBackoff {
		t.Fatalf("double = %v, want %v", got, 2*initialFlashbackBackoff)
	}
	if got := nextFlashbackBackoff(maxFlashbackBackoff); got != maxFlashbackBackoff {
		t.Fatalf("at cap = %v, want %v", got, maxFlashbackBackoff)
	}
	if got := nextFlashbackBackoff(4 * time.Second); got != maxFlashbackBackoff {
		t.Fatalf("past cap = %v, want %v", got, maxFlashbackBackoff)
	}
}

// TestStartFlashbackPortDisabled: an empty --flashback-listen is a no-op that
// returns a usable drain func (callers defer it unconditionally).
func TestStartFlashbackPortDisabled(t *testing.T) {
	saved := upConsoleFlashbackListen
	t.Cleanup(func() { upConsoleFlashbackListen = saved })

	upConsoleFlashbackListen = ""
	srv, err := console.New(console.Config{Listen: "127.0.0.1:0", Token: "tok"})
	if err != nil {
		t.Fatal(err)
	}
	stop, err := startFlashbackPort(context.Background(), srv)
	if err != nil {
		t.Fatalf("disabled port must not error: %v", err)
	}
	stop() // must be safe to call
}

// TestStartFlashbackPortRequiresToken: enabling the port without a console token
// fails fast (MySQL-protocol auth cannot use the bcrypt password store).
func TestStartFlashbackPortRequiresToken(t *testing.T) {
	saved := upConsoleFlashbackListen
	t.Cleanup(func() { upConsoleFlashbackListen = saved })

	upConsoleFlashbackListen = "127.0.0.1:0"
	// A loopback bind with no token is legal for the console (first-run setup),
	// so New succeeds — but the flashback port must refuse it.
	srv, err := console.New(console.Config{Listen: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	if srv.Token() != "" {
		t.Fatal("precondition: expected an empty token")
	}
	_, err = startFlashbackPort(context.Background(), srv)
	if err == nil || !strings.Contains(err.Error(), "token") {
		t.Fatalf("no token: err = %v, want a token-required error", err)
	}
}

// TestStartFlashbackPortBindAndDrain: the enabled port binds, serves on the
// daemon context, and the returned drain func returns once ctx is cancelled —
// pinning the shutdown-ordering contract (drain before the deferred db.Close)
// against a regression that would hang the daemon.
func TestStartFlashbackPortBindAndDrain(t *testing.T) {
	saved := upConsoleFlashbackListen
	t.Cleanup(func() { upConsoleFlashbackListen = saved })

	upConsoleFlashbackListen = "127.0.0.1:0" // ephemeral
	srv, err := console.New(console.Config{Listen: "127.0.0.1:0", Token: "tok"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	stop, err := startFlashbackPort(ctx, srv)
	if err != nil {
		t.Fatalf("bind failed: %v", err)
	}

	cancel() // daemon shutdown → ServeFlashback closes the listener and returns
	drained := make(chan struct{})
	go func() { stop(); close(drained) }()
	select {
	case <-drained:
	// Failure detector, not a paced wait — generous so machine load cannot
	// starve the drain into a spurious failure (#1164).
	case <-time.After(30 * time.Second):
		t.Fatal("flashback drain did not return after ctx cancel (shutdown would hang)")
	}
}

// TestResolveFlashbackEnv locks the --flashback-listen env fallback and its
// flag > env precedence, guarding the Changed("flashback-listen") string against
// a rename that would silently let the env override an explicit flag.
func TestResolveFlashbackEnv(t *testing.T) {
	saved := upConsoleFlashbackListen
	t.Cleanup(func() { upConsoleFlashbackListen = saved })

	if watchCmd.Flags().Lookup("flashback-listen") == nil {
		t.Fatal("flag --flashback-listen not registered on watchCmd; resolveUpConsoleEnv's Changed would always be false")
	}

	newCmd := func() *cobra.Command {
		cmd := &cobra.Command{}
		cmd.Flags().StringVar(&upConsoleFlashbackListen, "flashback-listen", "", "")
		return cmd
	}

	t.Setenv("BINTRAIL_CONSOLE_FLASHBACK_LISTEN", "127.0.0.1:3308")

	// No flag set → env applies.
	upConsoleFlashbackListen = ""
	resolveUpConsoleEnv(newCmd())
	if upConsoleFlashbackListen != "127.0.0.1:3308" {
		t.Fatalf("env fallback: got %q, want 127.0.0.1:3308", upConsoleFlashbackListen)
	}

	// Explicit flag wins over env.
	upConsoleFlashbackListen = "127.0.0.1:9000"
	cmd := newCmd()
	if err := cmd.Flags().Set("flashback-listen", "127.0.0.1:9000"); err != nil {
		t.Fatal(err)
	}
	resolveUpConsoleEnv(cmd)
	if upConsoleFlashbackListen != "127.0.0.1:9000" {
		t.Fatalf("flag precedence: got %q, want 127.0.0.1:9000 (env leaked over an explicit flag)", upConsoleFlashbackListen)
	}
}
