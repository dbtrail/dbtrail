//go:build integration

package consoleapp

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/shim"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TLS and SCRAM on the PostgreSQL port (#2766), end to end: the port as the
// command runs it, with the operator's certificate, and the clients people
// use (psql, pgx, psycopg 3, JDBC) in every sslmode. Each one logs in with
// SCRAM and reads the copy; the ones that ask for TLS get it, and the ones
// that verify accept the certificate by its CA.

// pgTLSModes are the sslmodes every client is run in, with whether the
// connection ends up encrypted.
var pgTLSModes = []struct {
	mode      string
	encrypted bool
}{
	{"disable", false},
	{"prefer", true},
	{"require", true},
	{"verify-ca", true},
	{"verify-full", true},
}

func TestPGRouterTLS_realClients(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	cert := testutil.WriteTestCert(t, testutil.TestCertOptions{})
	pt, err := shim.LoadPortTLS(cert.CertFile, cert.KeyFile)
	if err != nil {
		t.Fatal(err)
	}
	env := startPGExtPortTLS(t, ctx, pt.Config)
	const count = "SELECT count(*) AS n FROM " + pgExtTable
	// libpq's form: localhost is the name the certificate holds, hostaddr
	// where the port listens.
	libpq := func(mode, extra string) string {
		return fmt.Sprintf("host=localhost hostaddr=%s port=%s user=anyone password=tok dbname=pgsrv sslmode=%s sslrootcert=%s %s",
			env.host, env.port, mode, cert.CAFile, extra)
	}

	t.Run("psql", func(t *testing.T) {
		psql, err := exec.LookPath("psql")
		if err != nil {
			if os.Getenv("BINTRAIL_REQUIRE_PSQL") != "" {
				t.Fatal("psql is not installed, and BINTRAIL_REQUIRE_PSQL says it must be")
			}
			t.Skip("psql is not installed here")
		}
		run := func(dsn string) (string, error) {
			out, err := exec.CommandContext(ctx, psql, "-X", "-At", "-c", `\conninfo`, "-c", count, dsn).CombinedOutput()
			return string(out), err
		}
		for _, m := range pgTLSModes {
			for _, cb := range []string{"", "channel_binding=require"} {
				if cb != "" && !m.encrypted {
					continue
				}
				out, err := run(libpq(m.mode, cb))
				if err != nil {
					t.Errorf("psql sslmode=%s %s: %v\n%s", m.mode, cb, err, out)
					continue
				}
				if !strings.HasSuffix(strings.TrimSpace(out), "\n5") {
					t.Errorf("psql sslmode=%s %s: %q, want the count 5", m.mode, cb, out)
				}
				if got := strings.Contains(out, "SSL connection"); got != m.encrypted {
					t.Errorf("psql sslmode=%s %s: encrypted %v, want %v:\n%s", m.mode, cb, got, m.encrypted, out)
				}
			}
		}
		// The two refusals a client sees: a wrong password, and channel
		// binding asked for in clear text.
		if out, err := run(strings.Replace(libpq("require", ""), "password=tok", "password=wrong", 1)); err == nil || !strings.Contains(out, "password authentication failed") {
			t.Errorf("psql with a wrong password: %v\n%s", err, out)
		}
		if out, err := run(libpq("disable", "channel_binding=require")); err == nil {
			t.Errorf("psql with channel binding required and no TLS connected:\n%s", out)
		}
	})

	t.Run("pgx", func(t *testing.T) {
		for _, m := range pgTLSModes {
			for _, extra := range []string{"", "channel_binding=require", "sslnegotiation=direct"} {
				if extra != "" && !m.encrypted {
					continue
				}
				// pgx takes no hostaddr: localhost alone, which it tries
				// address by address.
				dsn := strings.Replace(libpq(m.mode, extra), "hostaddr="+env.host+" ", "", 1)
				conn := pgxConn(t, ctx, dsn, pgx.QueryExecModeCacheStatement)
				if got := pgxAnswer(ctx, conn, count); got != `[n] ["5"]` {
					t.Errorf("pgx sslmode=%s %s: %s", m.mode, extra, got)
				}
				if _, encrypted := conn.PgConn().Conn().(*tls.Conn); encrypted != m.encrypted {
					t.Errorf("pgx sslmode=%s %s: encrypted %v, want %v", m.mode, extra, encrypted, m.encrypted)
				}
			}
		}
	})

	t.Run("psycopg", func(t *testing.T) {
		python, err := exec.LookPath("python3")
		if err == nil {
			err = exec.CommandContext(ctx, python, "-c", "import psycopg").Run()
		}
		if err != nil {
			if os.Getenv("BINTRAIL_REQUIRE_PSYCOPG") != "" {
				t.Fatal("psycopg 3 is not installed, and BINTRAIL_REQUIRE_PSYCOPG says it must be")
			}
			t.Skip("psycopg 3 is not installed here")
		}
		script := filepath.Join(t.TempDir(), "tls.py")
		if err := os.WriteFile(script, []byte(psycopgTLSScript), 0o600); err != nil {
			t.Fatal(err)
		}
		for _, m := range pgTLSModes {
			out, err := exec.CommandContext(ctx, python, script, libpq(m.mode, "")).CombinedOutput()
			if err != nil {
				t.Errorf("psycopg sslmode=%s: %v\n%s", m.mode, err, out)
				continue
			}
			want := fmt.Sprintf("ssl=%v n=5", m.encrypted)
			if got := strings.TrimSpace(string(out)); got != want {
				t.Errorf("psycopg sslmode=%s: %q, want %q", m.mode, got, want)
			}
		}
	})

	t.Run("jdbc", func(t *testing.T) {
		jar := os.Getenv("BINTRAIL_PGJDBC_JAR")
		java, err := exec.LookPath("java")
		if jar == "" || err != nil {
			if os.Getenv("BINTRAIL_REQUIRE_JDBC") != "" {
				t.Fatal("java or BINTRAIL_PGJDBC_JAR is missing, and BINTRAIL_REQUIRE_JDBC says they must be there")
			}
			t.Skip("JDBC is not set up here (java and BINTRAIL_PGJDBC_JAR)")
		}
		src := filepath.Join(t.TempDir(), "TLS.java")
		if err := os.WriteFile(src, []byte(jdbcTLSProgram), 0o600); err != nil {
			t.Fatal(err)
		}
		for _, m := range pgTLSModes {
			url := fmt.Sprintf("jdbc:postgresql://localhost:%s/pgsrv?user=anyone&password=tok&sslmode=%s&sslrootcert=%s", env.port, m.mode, cert.CAFile)
			out, err := exec.CommandContext(ctx, java, "-cp", jar, src, url).CombinedOutput()
			if err != nil {
				t.Errorf("JDBC sslmode=%s: %v\n%s", m.mode, err, out)
				continue
			}
			if got := strings.TrimSpace(string(out)); got != "n=5" {
				t.Errorf("JDBC sslmode=%s: %q, want n=5", m.mode, got)
			}
		}
	})
}

const psycopgTLSScript = `import sys
import psycopg

with psycopg.connect(sys.argv[1]) as conn:
    n = conn.execute("SELECT count(*) AS n FROM ` + pgExtTable + `").fetchone()[0]
    print(f"ssl={'true' if conn.pgconn.ssl_in_use else 'false'} n={n}")
`

const jdbcTLSProgram = `import java.sql.*;

public class TLS {
    public static void main(String[] args) throws Exception {
        try (Connection c = DriverManager.getConnection(args[0]);
             ResultSet rs = c.createStatement().executeQuery("SELECT count(*) AS n FROM ` + pgExtTable + `")) {
            rs.next();
            System.out.println("n=" + rs.getString(1));
        }
    }
}
`
