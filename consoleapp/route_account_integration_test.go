//go:build integration

package consoleapp

import (
	"context"
	"database/sql"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	drivermysql "github.com/go-sql-driver/mysql"
)

var routeAccountSeq atomic.Uint64

// routeAccountDSN creates, on the source sourceDSN names, an account of its
// own for the MySQL port to forward with, and returns its DSN: the port
// never forwards with the account a server captures with (#2317), so a test
// server that routes needs one. It has every privilege, as the test source
// account does, so what a test learns about routing is not about grants;
// the tests about a forwarding account's grants make their own. Dropped
// when the test ends.
func routeAccountDSN(t *testing.T, sourceDSN string) string {
	t.Helper()
	cfg, err := drivermysql.ParseDSN(sourceDSN)
	if err != nil {
		t.Fatalf("the source DSN of a routed test server: %v", err)
	}
	admin := cfg.Clone()
	admin.DBName = ""
	db, err := sql.Open("mysql", admin.FormatDSN())
	if err != nil {
		t.Fatalf("open the source to create a forwarding account: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	user := fmt.Sprintf("fwd_%d_%d", time.Now().UnixNano()%1_000_000_000, routeAccountSeq.Add(1))
	const pass = "Fwd-pw-2317"
	for _, q := range []string{
		fmt.Sprintf("CREATE USER '%s'@'%%' IDENTIFIED BY '%s'", user, pass),
		fmt.Sprintf("GRANT ALL PRIVILEGES ON *.* TO '%s'@'%%' WITH GRANT OPTION", user),
	} {
		if _, err := db.ExecContext(ctx, q); err != nil {
			_ = db.Close()
			t.Fatalf("create the forwarding account %s: %v", user, err)
		}
	}
	t.Cleanup(func() {
		_, _ = db.Exec(fmt.Sprintf("DROP USER IF EXISTS '%s'@'%%'", user))
		_ = db.Close()
	})
	route := cfg.Clone()
	route.User, route.Passwd = user, pass
	return route.FormatDSN()
}

// otherAccountDSN is sourceDSN as another account, without creating it: the
// forwarding account of a test server whose source nothing listens on.
func otherAccountDSN(t *testing.T, sourceDSN string) string {
	t.Helper()
	cfg, err := drivermysql.ParseDSN(sourceDSN)
	if err != nil {
		t.Fatalf("the source DSN of a routed test server: %v", err)
	}
	route := cfg.Clone()
	route.User, route.Passwd = "fwd_nobody", "Fwd-pw-2317"
	return route.FormatDSN()
}
