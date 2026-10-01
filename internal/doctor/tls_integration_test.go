//go:build integration

package doctor

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// tlsOnlySourceDSN creates a user the server only lets in over TLS (REQUIRE
// SSL): the per-user form of require_secure_transport, which cannot be turned
// on globally on the shared test server without breaking every other test.
func tlsOnlySourceDSN(t *testing.T) string {
	t.Helper()
	testutil.SkipIfNoMySQL(t)
	admin, err := sql.Open("mysql", testutil.BaseDSN()+"/")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { admin.Close() })
	user := fmt.Sprintf("tlsonly_%d", time.Now().UnixNano()%1_000_000_000)
	const pass = "tlsonly-pass"
	for _, q := range []string{
		fmt.Sprintf("CREATE USER '%s'@'%%' IDENTIFIED BY '%s' REQUIRE SSL", user, pass),
		fmt.Sprintf("GRANT SELECT, REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO '%s'@'%%'", user),
	} {
		if _, err := admin.Exec(q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	t.Cleanup(func() { _, _ = admin.Exec(fmt.Sprintf("DROP USER '%s'@'%%'", user)) })
	base, err := mysql.ParseDSN(testutil.BaseDSN() + "/")
	if err != nil {
		t.Fatal(err)
	}
	base.User, base.Passwd, base.DBName = user, pass, ""
	return base.FormatDSN()
}

func sourceConnCheck(t *testing.T, r *Report) CheckResult {
	t.Helper()
	for _, c := range r.Checks {
		if c.Name == SourceConnectionCheckName {
			return c
		}
	}
	t.Fatalf("no %q check in the report", SourceConnectionCheckName)
	return CheckResult{}
}

// The console's checks must connect the way capture does. Before
// WithSourceSSL, Build always connected in cleartext, so a source that only
// accepts encrypted connections failed here while capture would have worked.
func TestBuild_SourceSSLReachesTheConnection(t *testing.T) {
	dsn := tlsOnlySourceDSN(t)

	t.Run("preferred connects over TLS", func(t *testing.T) {
		r := Build(t.Context(), dsn, "", "", 0, ForUnsavedServer(), WithSourceSSL(config.SSL{Mode: "preferred"}))
		if c := sourceConnCheck(t, r); c.Status != StatusPass {
			t.Fatalf("source connection = %s: %s", c.Status, c.Detail)
		}
	})
	t.Run("required connects over TLS", func(t *testing.T) {
		r := Build(t.Context(), dsn, "", "", 0, ForUnsavedServer(), WithSourceSSL(config.SSL{Mode: "required"}))
		if c := sourceConnCheck(t, r); c.Status != StatusPass {
			t.Fatalf("source connection = %s: %s", c.Status, c.Detail)
		}
	})
	t.Run("disabled is refused", func(t *testing.T) {
		r := Build(t.Context(), dsn, "", "", 0, ForUnsavedServer(), WithSourceSSL(config.SSL{Mode: "disabled"}))
		if c := sourceConnCheck(t, r); c.Status != StatusFail {
			t.Fatalf("source connection = %s, want fail with TLS disabled", c.Status)
		}
	})
	t.Run("without WithSourceSSL the DSN alone decides (cleartext here)", func(t *testing.T) {
		r := Build(t.Context(), dsn, "", "", 0, ForUnsavedServer())
		if c := sourceConnCheck(t, r); c.Status != StatusFail {
			t.Fatalf("source connection = %s, want fail: no TLS was asked for", c.Status)
		}
		r = Build(t.Context(), dsn+"?tls=preferred", "", "", 0, ForUnsavedServer())
		if c := sourceConnCheck(t, r); c.Status != StatusPass {
			t.Fatalf("tls=preferred in the DSN: source connection = %s: %s", c.Status, c.Detail)
		}
	})
	t.Run("a tls=false in the DSN wins over preferred", func(t *testing.T) {
		r := Build(t.Context(), dsn+"?tls=false", "", "", 0, ForUnsavedServer(), WithSourceSSL(config.SSL{Mode: "preferred"}))
		c := sourceConnCheck(t, r)
		if c.Status != StatusFail || strings.Contains(c.Detail, "cleartext retry") {
			t.Fatalf("source connection = %s (%s), want a plain fail with no retry", c.Status, c.Detail)
		}
	})
}
