//go:build integration

package cliapp

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/doctor"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// `bintrail up` must run its preflight with the TLS its stream uses
// (preferred unless set): before, the preflight connected in cleartext and
// refused to boot on a server that only accepts encrypted connections.
func TestUpPreflight_UsesTheStreamsSourceTLS(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	admin, err := sql.Open("mysql", testutil.BaseDSN()+"/")
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	user := fmt.Sprintf("uptlsonly_%d", time.Now().UnixNano()%1_000_000_000)
	for _, q := range []string{
		fmt.Sprintf("CREATE USER '%s'@'%%' IDENTIFIED BY 'x-pass' REQUIRE SSL", user),
		fmt.Sprintf("GRANT SELECT, REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO '%s'@'%%'", user),
	} {
		if _, err := admin.Exec(q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	t.Cleanup(func() { _, _ = admin.Exec(fmt.Sprintf("DROP USER '%s'@'%%'", user)) })
	cfg, err := mysql.ParseDSN(testutil.BaseDSN() + "/")
	if err != nil {
		t.Fatal(err)
	}
	cfg.User, cfg.Passwd = user, "x-pass"

	prevSrc, prevIdx := upSourceDSN, upIndexDSN
	t.Cleanup(func() { upSourceDSN, upIndexDSN = prevSrc, prevIdx })
	upSourceDSN, upIndexDSN = cfg.FormatDSN(), ""

	r := upPreflight(context.Background())
	for _, c := range r.Checks {
		if c.Name == doctor.SourceConnectionCheckName {
			if c.Status != doctor.StatusPass {
				t.Fatalf("source connection = %s: %s", c.Status, c.Detail)
			}
			return
		}
	}
	t.Fatal("no source connection check")
}
