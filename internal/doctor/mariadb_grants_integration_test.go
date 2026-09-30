//go:build integration

package doctor

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// mariaDBTestUser creates a user on the MariaDB source of the MariaDB CI job
// with exactly the given global privileges and returns a DSN that logs in as
// it. The CI jobs connect as root, which holds every privilege, so a check
// that misreads a grant never fails there; this is the user a real install
// has. Created at '%' because CI reaches the container over the docker
// bridge, not as localhost. Dropped at cleanup.
func mariaDBTestUser(t *testing.T, root *sql.DB, privs string) (user, dsn string) {
	t.Helper()
	user = fmt.Sprintf("bt_min_%d", time.Now().UnixNano()%1_000_000_000)
	const pw = "bt-min-pw-1"
	testutil.MustExec(t, root, fmt.Sprintf("CREATE USER '%s'@'%%' IDENTIFIED BY '%s'", user, pw))
	t.Cleanup(func() {
		if _, err := root.Exec(fmt.Sprintf("DROP USER IF EXISTS '%s'@'%%'", user)); err != nil {
			t.Logf("cleanup: drop user %s: %v", user, err)
		}
	})
	if privs != "" {
		testutil.MustExec(t, root, fmt.Sprintf("GRANT %s ON *.* TO '%s'@'%%'", privs, user))
	}
	cfg, err := mysql.ParseDSN(testutil.MariaDBBaseDSN() + "/?parseTime=true")
	if err != nil {
		t.Fatalf("parse MariaDB DSN: %v", err)
	}
	cfg.User, cfg.Passwd = user, pw
	return user, cfg.FormatDSN()
}

func mariaDBRoot(t *testing.T) *sql.DB {
	t.Helper()
	testutil.SkipIfNoMariaDB(t)
	root, err := sql.Open("mysql", testutil.MariaDBBaseDSN()+"/?parseTime=true")
	if err != nil {
		t.Fatalf("connect to MariaDB as root: %v", err)
	}
	t.Cleanup(func() { root.Close() })
	return root
}

func grantsRow(t *testing.T, r *Report) CheckResult {
	t.Helper()
	var found []CheckResult
	for _, c := range r.Checks {
		if c.Status != StatusPass {
			t.Logf("non-pass row: %s = %s (%s)", c.Name, c.Status, c.Detail)
		}
		if c.Name == ReplicationGrantsCheckName {
			found = append(found, c)
		}
	}
	if len(found) != 1 {
		t.Fatalf("want exactly one %q row, got %d. Checks: %+v", ReplicationGrantsCheckName, len(found), r.Checks)
	}
	return found[0]
}

// TestBuild_MariaDBMinimalPrivilegeUser runs doctor as the least-privilege
// user docs/mariadb.md asks for. MariaDB 10.5+ prints REPLICATION CLIENT as
// BINLOG MONITOR, and doctor used to look for the MySQL name only, so `up` and
// `watch` refused to start on every MariaDB whose user was not ALL PRIVILEGES
// (found on RDS for MariaDB, where no user is).
func TestBuild_MariaDBMinimalPrivilegeUser(t *testing.T) {
	root := mariaDBRoot(t)
	user, dsn := mariaDBTestUser(t, root, "REPLICATION SLAVE, REPLICATION CLIENT, SELECT, LOCK TABLES, SHOW VIEW")

	// The grant text alone must be enough. doctor would also pass on the
	// SHOW BINARY LOGS probe, which would hide a parser that misreads the
	// server's names, so the parser is checked on the server's own lines.
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.Query("SHOW GRANTS")
	if err != nil {
		t.Fatalf("SHOW GRANTS as %s: %v", user, err)
	}
	var grants []string
	for rows.Next() {
		var g string
		if err := rows.Scan(&g); err != nil {
			t.Fatal(err)
		}
		grants = append(grants, g)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	t.Logf("SHOW GRANTS: %q", grants)
	if slave, client := metadata.HasReplPrivileges(grants); !slave || !client {
		t.Errorf("HasReplPrivileges(%q) = slave %v, client %v; want both", grants, slave, client)
	}

	got := grantsRow(t, Build(t.Context(), dsn, "", "", 0))
	if got.Status != StatusPass {
		t.Errorf("%q = %q (%s), want pass for the documented minimal user", got.Name, got.Status, got.Detail)
	}
}

// TestBuild_MariaDBGrantRemediationWorks follows the loop the RDS test found
// broken: doctor refuses, the operator runs the GRANT doctor printed, word for
// word, and doctor runs again. Before the fix the second run refused exactly
// like the first.
func TestBuild_MariaDBGrantRemediationWorks(t *testing.T) {
	root := mariaDBRoot(t)
	user, dsn := mariaDBTestUser(t, root, "REPLICATION SLAVE, SELECT")

	first := grantsRow(t, Build(t.Context(), dsn, "", "", 0))
	if first.Status != StatusFail {
		t.Fatalf("%q = %q (%s) for a user without REPLICATION CLIENT, want fail", first.Name, first.Status, first.Detail)
	}
	var fix []string
	for _, line := range strings.Split(first.Remediation, "\n") {
		if l := strings.TrimSpace(line); strings.HasPrefix(l, "GRANT ") || strings.HasPrefix(l, "FLUSH ") {
			fix = append(fix, strings.TrimSuffix(l, ";"))
		}
	}
	if len(fix) == 0 || !strings.Contains(fix[0], user) {
		t.Fatalf("no GRANT for %s in the remediation:\n%s", user, first.Remediation)
	}
	for _, stmt := range fix {
		t.Logf("running the printed fix: %s", stmt)
		testutil.MustExec(t, root, stmt)
	}

	second := grantsRow(t, Build(t.Context(), dsn, "", "", 0))
	if second.Status != StatusPass {
		t.Errorf("after running doctor's own fix, %q = %q (%s), want pass", second.Name, second.Status, second.Detail)
	}
}
