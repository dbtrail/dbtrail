//go:build integration

package consoleapp

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/mydumperlock"
)

// #1996: every connection a full read makes before mydumper runs reaches a
// source that only accepts encrypted connections, with the entry's TLS (an
// empty mode is preferred), as capture does. Each goes red when its wiring is
// reverted to a plain config.Connect: the REQUIRE SSL user is refused in
// cleartext.
func TestFullReadPreChecks_UseTheSourceTLS(t *testing.T) {
	dsn := tlsOnlyDSN(t)
	ctx := t.Context()
	def := config.SSL{} // an entry with no ssl_mode

	t.Run("disk estimate", func(t *testing.T) {
		if _, err := estimateDumpSize(ctx, dsn, def, nil); err != nil {
			t.Fatalf("estimateDumpSize: %v", err)
		}
	})
	t.Run("server version", func(t *testing.T) {
		if v, err := sourceServerVersion(ctx, dsn, def); err != nil || v == "" {
			t.Fatalf("sourceServerVersion = %q, %v", v, err)
		}
	})
	t.Run("new tables of an update", func(t *testing.T) {
		if _, _, err := listSourceTables(ctx, dsn, def, nil); err != nil {
			t.Fatalf("listSourceTables: %v", err)
		}
	})
	t.Run("privilege preflight reaches the grants", func(t *testing.T) {
		// The user holds no LOCK TABLES: a refusal NAMING the missing
		// privilege proves the check connected and read SHOW GRANTS.
		err := realCheckMydumperPrivileges(ctx, dsn, def, baseline.LockModeLockAll, mydumperlock.RemedyConsole, nil)
		var mp *mydumperlock.MissingPrivilegesError
		if !errors.As(err, &mp) {
			t.Fatalf("err = %v, want a missing-privileges refusal (a connection error means the check went out in cleartext)", err)
		}
	})
	t.Run("preferred probe reports TLS", func(t *testing.T) {
		sourceEncrypted = realSourceEncrypted
		t.Cleanup(func() {
			sourceEncrypted = func(context.Context, string, config.SSL) (bool, error) { return false, nil }
		})
		enc, err := sourceEncrypted(ctx, dsn, def)
		if err != nil || !enc {
			t.Fatalf("sourceEncrypted = %v, %v; want true", enc, err)
		}
		got, err := resolveDumpTLS(ctx, dsn, def)
		if err != nil || !got.encrypt {
			t.Fatalf("resolveDumpTLS = %+v, %v; want an encrypted dump", got, err)
		}
	})
	t.Run("no-lock table count", func(t *testing.T) {
		var buf bytes.Buffer
		prev := slog.Default()
		slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
		t.Cleanup(func() { slog.SetDefault(prev) })
		warnIfMultiTableNoLock(ctx, dsn, def, nil)
		if strings.Contains(buf.String(), "could not") {
			t.Fatalf("the no-lock table count did not run: %s", buf.String())
		}
	})
	t.Run("disabled is refused in plain words", func(t *testing.T) {
		_, err := estimateDumpSize(ctx, dsn, config.SSL{Mode: "disabled"}, nil)
		if err == nil {
			t.Fatal("a cleartext connection reached a REQUIRE SSL user")
		}
	})
}
