//go:build integration

package consoleapp

import (
	"errors"
	"testing"

	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/internal/config"
)

// An extension opening a source that only accepts encrypted connections
// (REQUIRE SSL, the per-user form of require_secure_transport=ON) with the
// seam's DSN and ext.OpenSource gets in; the DSN-only path every extension
// had before (a plain connect) is refused. A per-user REQUIRE SSL refuses a
// cleartext login with 1045 (access denied); the server-wide setting answers
// 3159. Either is the refusal this test means.
func TestExtOpenSource_TLSOnlySource(t *testing.T) {
	dsn := tlsOnlyDSN(t)

	ping := func(t *testing.T, dsn string, tls ext.SourceTLS) error {
		t.Helper()
		db, err := ext.OpenSource(dsn, tls)
		if err != nil {
			return err
		}
		defer db.Close()
		var one int
		return db.QueryRowContext(t.Context(), "SELECT 1").Scan(&one)
	}
	refused := func(err error) bool {
		var me *mysql.MySQLError
		return errors.As(err, &me) && (me.Number == 1045 || me.Number == 3159)
	}

	t.Run("the old plaintext path is refused", func(t *testing.T) {
		db, err := config.Connect(dsn)
		if err == nil {
			db.Close()
			t.Fatal("a cleartext connection reached a REQUIRE SSL user")
		}
		if !refused(err) {
			t.Fatalf("err = %v, want the server's refusal", err)
		}
	})
	for _, tc := range []struct {
		name string
		tls  ext.SourceTLS
	}{
		{"zero value (preferred)", ext.SourceTLS{}},
		{"preferred", ext.SourceTLS{Mode: "preferred"}},
		{"required", ext.SourceTLS{Mode: "required"}},
	} {
		t.Run(tc.name+" gets in", func(t *testing.T) {
			if err := ping(t, dsn, tc.tls); err != nil {
				t.Fatal(err)
			}
		})
	}
	t.Run("disabled is refused", func(t *testing.T) {
		if err := ping(t, dsn, ext.SourceTLS{Mode: "disabled"}); !refused(err) {
			t.Fatalf("err = %v, want the server's refusal", err)
		}
	})
	t.Run("a tls= in the DSN wins over disabled", func(t *testing.T) {
		if err := ping(t, dsn+"?tls=skip-verify", ext.SourceTLS{Mode: "disabled"}); err != nil {
			t.Fatal(err)
		}
	})
}
