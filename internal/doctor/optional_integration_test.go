//go:build integration

package doctor

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestRDSRetentionGrantAccountAgainstRealMySQL connects as accounts whose
// names need quoting, lets the retention check hit its probe-error branch (a
// user with no privileges is refused mysql.rds_configuration with 1142), and
// then asks the server itself, as root, for the grants of the account the fix
// names. SHOW GRANTS FOR fails on an account that does not exist, so a GRANT
// that names the wrong account, or quotes it wrong, fails here.
func TestRDSRetentionGrantAccountAgainstRealMySQL(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := t.Context()
	root, err := sql.Open("mysql", testutil.BaseDSN()+"/")
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()

	suffix := fmt.Sprintf("%d", time.Now().UnixNano()%1_000_000)
	const pw = "Doctor-Grant-1"
	users := []string{"o'b`q_" + suffix, "a@b_" + suffix}
	for _, u := range users {
		acct := grantAccount(u + "@%")
		if acct == "" {
			t.Fatalf("setup: grantAccount(%q) = empty", u+"@%")
		}
		if _, err := root.ExecContext(ctx, "CREATE USER "+acct+" IDENTIFIED BY '"+pw+"'"); err != nil {
			t.Fatalf("create %s: %v", acct, err)
		}
		t.Cleanup(func() { _, _ = root.Exec("DROP USER IF EXISTS " + acct) })
	}
	// An IPv6 host is quoted too; SHOW GRANTS FOR is enough to prove it.
	v6 := grantAccount("u6_" + suffix + "@::1")
	if _, err := root.ExecContext(ctx, "CREATE USER "+v6); err != nil {
		t.Fatalf("create %s: %v", v6, err)
	}
	t.Cleanup(func() { _, _ = root.Exec("DROP USER IF EXISTS " + v6) })
	if _, err := root.ExecContext(ctx, "SHOW GRANTS FOR "+v6); err != nil {
		t.Errorf("SHOW GRANTS FOR %s: %v", v6, err)
	}

	base, err := mysql.ParseDSN(testutil.BaseDSN() + "/")
	if err != nil {
		t.Fatal(err)
	}
	for _, u := range users {
		t.Run(u, func(t *testing.T) {
			cfg := base.Clone()
			cfg.User, cfg.Passwd = u, pw
			db, err := sql.Open("mysql", cfg.FormatDSN())
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()

			got := checkBinlogRetention(ctx, db)
			t.Logf("%s\n%s", got.Detail, got.Remediation)
			const prefix = "GRANT SELECT ON mysql.rds_configuration TO "
			i := strings.Index(got.Remediation, prefix)
			if i < 0 {
				t.Fatalf("no GRANT in the fix (did the probe not fail?): %+v", got)
			}
			acct := strings.TrimSuffix(got.Remediation[i+len(prefix):], ";")
			if acct == placeholderAccount {
				t.Fatalf("the fix kept the placeholder for %q", u)
			}
			var line string
			if err := root.QueryRowContext(ctx, "SHOW GRANTS FOR "+acct).Scan(&line); err != nil {
				t.Fatalf("SHOW GRANTS FOR %s (the account the fix names): %v", acct, err)
			}
			if !strings.Contains(line, "@`%`") {
				t.Errorf("grants for %s: %q", acct, line)
			}
		})
	}
}
