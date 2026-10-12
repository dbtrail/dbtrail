package consoleapp

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/readrouter"
	"github.com/dbtrail/dbtrail/internal/shim"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// saveRouteGlobals restores the port's flag variables after a test that sets
// them, and clears the environment variables that would feed them.
func saveRouteGlobals(t *testing.T) {
	t.Helper()
	listen, age, ro, cost, rows := upConsoleFlashbackListen, upRouteMaxCopyAge, upRouteReadOnly, upRouteCostThreshold, upRouteScanRows
	t.Cleanup(func() {
		upConsoleFlashbackListen, upRouteMaxCopyAge, upRouteReadOnly, upRouteCostThreshold, upRouteScanRows = listen, age, ro, cost, rows
	})
	for _, k := range []string{"BINTRAIL_CONSOLE_FLASHBACK_LISTEN", "BINTRAIL_CONSOLE_ROUTE_MAX_COPY_AGE", "BINTRAIL_CONSOLE_ROUTE_READ_ONLY",
		"BINTRAIL_CONSOLE_ROUTE_COST_THRESHOLD", "BINTRAIL_CONSOLE_ROUTE_SCAN_ROWS"} {
		t.Setenv(k, "")
	}
	upConsoleFlashbackListen, upRouteMaxCopyAge, upRouteReadOnly = "", 0, false
	upRouteCostThreshold, upRouteScanRows = readrouter.DefaultPolicy().CostThreshold, readrouter.DefaultPolicy().ScanRows
}

// routeFlagsCmd is a command carrying the port's flags bound to the same
// variables watch binds them to, so a test can set one "on the command line".
func routeFlagsCmd() *cobra.Command {
	cmd := &cobra.Command{}
	cmd.Flags().StringVar(&upConsoleFlashbackListen, "flashback-listen", "", "")
	cmd.Flags().DurationVar(&upRouteMaxCopyAge, "route-max-copy-age", 0, "")
	cmd.Flags().BoolVar(&upRouteReadOnly, "route-read-only", false, "")
	return cmd
}

// routeTestFreeSQL is a copy that is never asked: the statements below are
// decided before it.
type routeTestFreeSQL struct{}

func (routeTestFreeSQL) Run(context.Context, string, string, sqlsandbox.Session) (sqlsandbox.Result, error) {
	return sqlsandbox.Result{}, errors.New("the copy was asked")
}
func (routeTestFreeSQL) CopyUpdatedAt(context.Context) time.Time { return time.Now() }
func (routeTestFreeSQL) RowCap() int                             { return 0 }

// TestBindReadRouterCarriesReadOnly: a connection bound from a port
// configured read-only refuses a write, and one bound from a read-write port
// sends it on. No database: the refusal comes before any connection, and the
// read-write side fails to connect to a port nothing listens on, which is
// the proof that it tried.
func TestBindReadRouterCarriesReadOnly(t *testing.T) {
	srv, err := console.New(console.Config{Listen: "127.0.0.1:0", Token: "tok"})
	if err != nil {
		t.Fatal(err)
	}
	tgt := console.FlashbackTarget{ID: "s1", SQL: &console.SQLOnCopy{}, ForwardDSN: "nobody:x@tcp(127.0.0.1:1)/none", SourceSSL: console.ServerEntry{}.SourceSSL()}
	bind := func(readOnly bool) *shim.Handler {
		h := shim.NewHandler(nil, nil)
		h.BindFreeSQL(routeTestFreeSQL{})
		bindReadRouter(h, srv, tgt, "s1", flashbackConfig{RouteMaxCopyAge: time.Hour, RoutePolicy: readrouter.DefaultPolicy(),
			RouteReadOnly: readOnly, QueryTimeout: 5 * time.Second}, slog.Default())
		t.Cleanup(h.Close)
		return h
	}
	code := func(err error) uint16 {
		var me *gomysql.MyError
		if errors.As(err, &me) {
			return me.Code
		}
		return 0
	}
	_, err = bind(true).HandleQuery("DELETE FROM t")
	if code(err) != gomysql.ER_OPTION_PREVENTS_STATEMENT || !strings.Contains(err.Error(), "--route-read-only") {
		t.Errorf("read-only port: DELETE got %v, want the 1290 refusal", err)
	}
	_, err = bind(false).HandleQuery("DELETE FROM t")
	if code(err) != readrouter.CodeUpstreamLost {
		t.Errorf("read-write port: DELETE got %v, want the forward to be attempted (2006 from the unreachable source)", err)
	}
	// Routing off: nothing is bound, whatever the read-only setting.
	h := shim.NewHandler(nil, nil)
	h.BindFreeSQL(routeTestFreeSQL{})
	bindReadRouter(h, srv, tgt, "s1", flashbackConfig{RouteReadOnly: true}, slog.Default())
	if _, err := h.HandleQuery("DELETE FROM t"); code(err) == gomysql.ER_OPTION_PREVENTS_STATEMENT || code(err) == readrouter.CodeUpstreamLost {
		t.Errorf("routing off: DELETE got %v, want the copy's own answer", err)
	}
}

// TestBindReadRouterUsesTheServersTLS: the connection's TLS comes from the
// target (the entry's ssl_* fields). A setting that cannot be used keeps the
// connection copy-only and tells the Connect page why, in the entry's own
// words, instead of connecting some other way.
func TestBindReadRouterUsesTheServersTLS(t *testing.T) {
	srv, err := console.New(console.Config{Listen: "127.0.0.1:0", Token: "tok", FlashbackListen: "127.0.0.1:3308",
		ReadRouting: console.ReadRoutingConfig{MaxCopyAge: time.Hour, ScanRows: 2}})
	if err != nil {
		t.Fatal(err)
	}
	unavailable := func() string {
		req := httptest.NewRequest("GET", "http://127.0.0.1/api/flashback", nil)
		req.Header.Set("Authorization", "Bearer tok")
		rec := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rec, req)
		var fb struct {
			Routing struct {
				Servers map[string]struct {
					Unavailable string `json:"unavailable"`
				} `json:"servers"`
			} `json:"routing"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &fb); err != nil {
			t.Fatalf("decode %s: %v", rec.Body.String(), err)
		}
		return fb.Routing.Servers["s1"].Unavailable
	}
	bind := func(ssl config.SSL) *shim.Handler {
		h := shim.NewHandler(nil, nil)
		h.BindFreeSQL(routeTestFreeSQL{})
		bindReadRouter(h, srv, console.FlashbackTarget{ID: "s1", SQL: &console.SQLOnCopy{}, ForwardDSN: "nobody:x@tcp(127.0.0.1:1)/none", SourceSSL: ssl},
			"s1", flashbackConfig{RouteMaxCopyAge: time.Hour, RoutePolicy: readrouter.DefaultPolicy(), QueryTimeout: 5 * time.Second}, slog.Default())
		t.Cleanup(h.Close)
		return h
	}
	// An unknown mode: not bound (the write gets the copy's answer, not the
	// source's 2006), and the page says why without a command-line flag.
	_, err = bind(config.SSL{Mode: "prefered"}).HandleQuery("DELETE FROM t")
	var me *gomysql.MyError
	if errors.As(err, &me) && me.Code == readrouter.CodeUpstreamLost {
		t.Errorf("an unusable TLS mode still bound a router: %v", err)
	}
	why := unavailable()
	t.Log(why)
	if !strings.Contains(why, "prefered") || !strings.Contains(why, "this server's TLS settings cannot be used") || strings.Contains(why, "--ssl") {
		t.Errorf("the page says %q, want the bad TLS mode named without a flag", why)
	}
	// A usable one: bound (the forward is attempted), and the note is gone.
	_, err = bind(config.SSL{Mode: "required"}).HandleQuery("DELETE FROM t")
	if !errors.As(err, &me) || me.Code != readrouter.CodeUpstreamLost {
		t.Errorf("a usable TLS mode did not bind a router: %v", err)
	}
	if why := unavailable(); why != "" {
		t.Errorf("after a connection bound, the page still says %q", why)
	}
}
