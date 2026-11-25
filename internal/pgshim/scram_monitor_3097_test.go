package pgshim

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// A username whose password is its own alone (the monitor login) costs the
// port what an unknown username costs: no key is derived at a login, for
// either, when the host gave WarmPasswords every password a login can be
// checked with. With the monitor's password left out of that set, each
// login under the monitor's username derives it again, which is why the
// host (consoleapp's pgPortLogin, console.Server.AllFlashbackPasswords)
// puts it in.
func TestSCRAM_aLoginWithItsOwnPasswordDerivesNothing(t *testing.T) {
	const monitorPassword = "monitor-pw"
	var derived atomic.Int64
	old := scramDerive
	scramDerive = func(pw string, salt []byte, n int) []byte { derived.Add(1); return old(pw, salt, n) }
	t.Cleanup(func() { scramDerive = old })

	host := func(warm []string) LoginFunc {
		return func(_ context.Context, user, _ string, password *Password, _ func()) (*Binding, *Error) {
			WarmPasswords(warm)
			candidates := []string{portPassword}
			if user == "monitor" {
				candidates = []string{monitorPassword}
			}
			if !password.Matches(candidates) {
				return nil, &Error{Code: "28P01", Message: "no"}
			}
			return &Binding{Run: one}, nil
		}
	}
	// work is the keys derived and the forms checked by five logins under
	// user with a wrong password, after a first one.
	work := func(addr, user string) (derivations, checks int64) {
		h, port, _ := net.SplitHostPort(addr)
		try := func() {
			_, err := pgconn.Connect(context.Background(), fmt.Sprintf("host=%s port=%s user=%s password=wrong dbname=orders sslmode=disable", h, port, user))
			var pe *pgconn.PgError
			if !errors.As(err, &pe) || pe.Code != "28P01" {
				t.Fatalf("%s with a wrong password: %v, want 28P01", user, err)
			}
		}
		try()
		d, c := derived.Load(), scramChecks.Load()
		for range 5 {
			try()
		}
		return derived.Load() - d, scramChecks.Load() - c
	}

	addr := serveAddrCfg(t, Config{Login: host([]string{portPassword, monitorPassword})})
	md, mc := work(addr, "monitor")
	nd, nc := work(addr, "nobody")
	if md != 0 || nd != 0 {
		t.Errorf("keys derived by five logins: %d under the monitor's username, %d under an unknown one, want 0 and 0", md, nd)
	}
	if mc != nc {
		t.Errorf("forms checked by five logins: %d under the monitor's username, %d under an unknown one", mc, nc)
	}
	// The monitor's own password still logs in.
	h, port, _ := net.SplitHostPort(addr)
	c, err := pgconn.Connect(context.Background(), fmt.Sprintf("host=%s port=%s user=monitor password=%s dbname=orders sslmode=disable", h, port, monitorPassword))
	if err != nil {
		t.Fatalf("the monitor's password: %v", err)
	}
	_ = c.Close(context.Background())

	// The set without the monitor's password: what this guards against.
	addr = serveAddrCfg(t, Config{Login: host([]string{portPassword})})
	if md, _ := work(addr, "monitor"); md == 0 {
		t.Error("with the monitor's password left out of the set, its logins derived nothing: this test would not see the defect")
	}
}
