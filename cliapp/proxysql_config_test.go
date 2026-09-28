package cliapp

import (
	"bytes"
	"database/sql"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

const (
	pcTestSourceDSN = "user:pass@tcp(db.example.com:3306)/myapp"
	pcTestUser1     = "app_user"
	// Cleartext used by tests; the SHA1 ProxySQL stores is derived from
	// this at SQL-generation time. pcTestSHA1_1 is computed lazily so
	// the assertion in tests stays in lockstep with the production
	// derivation in nativePasswordHash().
	pcTestPassword1 = "testpw1"
)

var pcTestSHA1_1 = nativePasswordHash(pcTestPassword1)

func writeShimYAML(t *testing.T, dir string, body string) string {
	t.Helper()
	path := filepath.Join(dir, "shim.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func resetPCFlags() {
	pcOut = "proxysql-setup.sql"
	pcShimConfig = "shim.yaml"
	pcMySQLPort = 3306
	pcShimPort = 3308
	pcProxySQLMySQLPort = 6033
	pcForce = false
	pcBackendAuthPlugin = backendAuthNative
	pcValidate = false
}

const validShimYAML = `listen: ':3308'
tenants:
  - server_id: '1'
    source_dsn: 'user:pass@tcp(db:3306)/myapp'
    agent_url: 'http://localhost:8600'
    agent_token: 'btk_abc'
    mysql_user: app_user
    mysql_password: 'testpw1'
`

func TestRunProxySQLConfig(t *testing.T) {
	t.Run("happy path single tenant", func(t *testing.T) {
		dir := t.TempDir()
		orig, _ := os.Getwd()
		t.Cleanup(func() { os.Chdir(orig) })
		os.Chdir(dir)

		t.Setenv("BINTRAIL_SOURCE_DSN", pcTestSourceDSN)
		writeShimYAML(t, dir, validShimYAML)
		resetPCFlags()

		if err := runProxySQLConfig(proxysqlConfigCmd, nil); err != nil {
			t.Fatal(err)
		}

		data, err := os.ReadFile(filepath.Join(dir, "proxysql-setup.sql"))
		if err != nil {
			t.Fatalf("expected output file: %v", err)
		}
		out := string(data)

		wants := []string{
			"/*",
			"* Bintrail time-travel SQL",
			"docs/time-travel-sql.md",
			"*/",
			"BEGIN;",
			"DELETE FROM mysql_servers WHERE hostgroup_id IN (990, 991);",
			"INSERT INTO mysql_servers (hostgroup_id, hostname, port) VALUES (990, 'db.example.com', 3306);",
			"INSERT INTO mysql_servers (hostgroup_id, hostname, port) VALUES (991, '127.0.0.1', 3308);",
			"DELETE FROM mysql_users WHERE default_hostgroup = 990;",
			"INSERT INTO mysql_users (username, password, default_hostgroup, active) VALUES ('app_user', '" + pcTestSHA1_1 + "', 990, 1);",
			"DELETE FROM mysql_query_rules WHERE rule_id IN (990001, 990002, 990003, 990004, 990005, 990006);",
			"VALUES (990001, 1, '\\b_flashback\\.', 991, 1);",
			"VALUES (990002, 1, '\\b_diff\\.', 991, 1);",
			"VALUES (990003, 1, '\\b_snapshot\\.', 991, 1);",
			"VALUES (990004, 1, '/\\*\\+\\s*DBTRAIL_AT', 991, 1);",
			"VALUES (990005, 1, '^\\s*SHOW\\s+(FULL\\s+)?TABLES\\s+(FROM|IN)\\s+`?_(flashback|diff|snapshot)`?', 991, 1);",
			"VALUES (990006, 1, '\\bAS\\s+OF\\s+(TIMESTAMP\\s+)?''[^'']*''\\s*\\x3b?\\s*$', 991, 1);",
			"COMMIT;",
			"LOAD MYSQL SERVERS TO RUNTIME;",
			"LOAD MYSQL USERS TO RUNTIME;",
			"LOAD MYSQL QUERY RULES TO RUNTIME;",
			"SAVE MYSQL SERVERS TO DISK;",
			"SAVE MYSQL USERS TO DISK;",
			"SAVE MYSQL QUERY RULES TO DISK;",
		}
		for _, w := range wants {
			if !strings.Contains(out, w) {
				t.Errorf("output missing %q; full output:\n%s", w, out)
			}
		}

		info, _ := os.Stat(filepath.Join(dir, "proxysql-setup.sql"))
		if perm := info.Mode().Perm(); perm != 0o600 {
			t.Errorf("perm = %o, want 0600", perm)
		}
	})

	t.Run("happy path two tenants", func(t *testing.T) {
		two := validShimYAML + `  - server_id: '2'
    source_dsn: 'user:pass@tcp(db:3306)/myapp2'
    agent_url: 'http://localhost:8600'
    agent_token: 'btk_xyz'
    mysql_user: app_user2
    mysql_password: 'testpw2'
`
		dir := t.TempDir()
		orig, _ := os.Getwd()
		t.Cleanup(func() { os.Chdir(orig) })
		os.Chdir(dir)

		t.Setenv("BINTRAIL_SOURCE_DSN", pcTestSourceDSN)
		writeShimYAML(t, dir, two)
		resetPCFlags()

		if err := runProxySQLConfig(proxysqlConfigCmd, nil); err != nil {
			t.Fatal(err)
		}

		data, _ := os.ReadFile(filepath.Join(dir, "proxysql-setup.sql"))
		out := string(data)

		if !strings.Contains(out, "DELETE FROM mysql_users WHERE default_hostgroup = 990;") {
			t.Errorf("expected hostgroup-scoped DELETE; got:\n%s", out)
		}
		if !strings.Contains(out, "INSERT INTO mysql_users (username, password, default_hostgroup, active) VALUES ('app_user', ") {
			t.Error("expected INSERT for app_user")
		}
		if !strings.Contains(out, "INSERT INTO mysql_users (username, password, default_hostgroup, active) VALUES ('app_user2', ") {
			t.Error("expected INSERT for app_user2")
		}
	})

	t.Run("error when source DSN missing", func(t *testing.T) {
		t.Setenv("BINTRAIL_SOURCE_DSN", "")
		resetPCFlags()
		err := runProxySQLConfig(proxysqlConfigCmd, nil)
		if err == nil {
			t.Fatal("expected error")
		}
		if !strings.Contains(err.Error(), "BINTRAIL_SOURCE_DSN") {
			t.Errorf("error should name the env var, got %v", err)
		}
	})

	t.Run("error when DSN invalid", func(t *testing.T) {
		t.Setenv("BINTRAIL_SOURCE_DSN", "not-a-valid-dsn-format")
		resetPCFlags()
		err := runProxySQLConfig(proxysqlConfigCmd, nil)
		if err == nil {
			t.Fatal("expected error for invalid DSN")
		}
		if !strings.Contains(err.Error(), "BINTRAIL_SOURCE_DSN") {
			t.Errorf("error should mention the var, got %v", err)
		}
	})

	t.Run("error when DSN uses unix socket", func(t *testing.T) {
		t.Setenv("BINTRAIL_SOURCE_DSN", "user:pass@unix(/tmp/mysql.sock)/myapp")
		resetPCFlags()
		err := runProxySQLConfig(proxysqlConfigCmd, nil)
		if err == nil {
			t.Fatal("expected error for unix socket")
		}
		if !strings.Contains(err.Error(), "unix socket") {
			t.Errorf("error should mention unix socket, got %v", err)
		}
	})

	t.Run("error when shim.yaml missing", func(t *testing.T) {
		dir := t.TempDir()
		orig, _ := os.Getwd()
		t.Cleanup(func() { os.Chdir(orig) })
		os.Chdir(dir)

		t.Setenv("BINTRAIL_SOURCE_DSN", pcTestSourceDSN)
		resetPCFlags()

		err := runProxySQLConfig(proxysqlConfigCmd, nil)
		if err == nil {
			t.Fatal("expected error when shim.yaml missing")
		}
		if !strings.Contains(err.Error(), "shim config not found") {
			t.Errorf("expected 'shim config not found' in error, got %v", err)
		}
	})

	t.Run("error when tenant missing mysql_user", func(t *testing.T) {
		dir := t.TempDir()
		orig, _ := os.Getwd()
		t.Cleanup(func() { os.Chdir(orig) })
		os.Chdir(dir)

		t.Setenv("BINTRAIL_SOURCE_DSN", pcTestSourceDSN)
		writeShimYAML(t, dir, "tenants:\n  - mysql_password: 'p'\n")
		resetPCFlags()

		err := runProxySQLConfig(proxysqlConfigCmd, nil)
		if err == nil {
			t.Fatal("expected error")
		}
		if !strings.Contains(err.Error(), "mysql_user") {
			t.Errorf("error should name the missing field, got %v", err)
		}
	})

	t.Run("error when tenant missing mysql_password", func(t *testing.T) {
		dir := t.TempDir()
		orig, _ := os.Getwd()
		t.Cleanup(func() { os.Chdir(orig) })
		os.Chdir(dir)

		t.Setenv("BINTRAIL_SOURCE_DSN", pcTestSourceDSN)
		writeShimYAML(t, dir, "tenants:\n  - mysql_user: app_user\n")
		resetPCFlags()

		err := runProxySQLConfig(proxysqlConfigCmd, nil)
		if err == nil {
			t.Fatal("expected error")
		}
		if !strings.Contains(err.Error(), "mysql_password") {
			t.Errorf("error should name the missing field, got %v", err)
		}
	})

	t.Run("legacy mysql_pass_sha1 alone rejected with migration hint", func(t *testing.T) {
		// Operators upgrading from 0.7.0 / 0.7.1 see this clearly so they
		// can mechanically replace the field without digging through the
		// changelog.
		dir := t.TempDir()
		orig, _ := os.Getwd()
		t.Cleanup(func() { os.Chdir(orig) })
		os.Chdir(dir)

		t.Setenv("BINTRAIL_SOURCE_DSN", pcTestSourceDSN)
		writeShimYAML(t, dir, "tenants:\n  - mysql_user: app_user\n    mysql_pass_sha1: '*ABC'\n")
		resetPCFlags()

		err := runProxySQLConfig(proxysqlConfigCmd, nil)
		if err == nil {
			t.Fatal("expected error")
		}
		if !strings.Contains(err.Error(), "mysql_password is required") {
			t.Errorf("error should explain the migration, got %v", err)
		}
	})

	t.Run("both mysql_password and mysql_pass_sha1 set: cleartext wins", func(t *testing.T) {
		// Half-migrated shim.yaml: operator added mysql_password but
		// forgot to delete the legacy mysql_pass_sha1. The cleartext
		// must win and the SHA1 emitted in the SQL must be derived
		// from the cleartext, NOT the stale legacy hash. A regression
		// here would silently use a stale hash for the new ProxySQL
		// row.
		dir := t.TempDir()
		orig, _ := os.Getwd()
		t.Cleanup(func() { os.Chdir(orig) })
		os.Chdir(dir)

		t.Setenv("BINTRAIL_SOURCE_DSN", pcTestSourceDSN)
		writeShimYAML(t, dir, `tenants:
  - mysql_user: app_user
    mysql_password: 'fresh'
    mysql_pass_sha1: '*STALEHASH'
`)
		resetPCFlags()
		if err := runProxySQLConfig(proxysqlConfigCmd, nil); err != nil {
			t.Fatal(err)
		}
		out, _ := os.ReadFile(filepath.Join(dir, "proxysql-setup.sql"))
		body := string(out)
		freshHash := nativePasswordHash("fresh")
		if !strings.Contains(body, freshHash) {
			t.Errorf("expected SQL to embed the fresh-cleartext SHA1 %q; got:\n%s", freshHash, body)
		}
		if strings.Contains(body, "*STALEHASH") {
			t.Errorf("stale legacy hash leaked into SQL; got:\n%s", body)
		}
	})

	t.Run("error when shim.yaml has no tenants", func(t *testing.T) {
		dir := t.TempDir()
		orig, _ := os.Getwd()
		t.Cleanup(func() { os.Chdir(orig) })
		os.Chdir(dir)

		t.Setenv("BINTRAIL_SOURCE_DSN", pcTestSourceDSN)
		writeShimYAML(t, dir, "tenants: []\n")
		resetPCFlags()

		err := runProxySQLConfig(proxysqlConfigCmd, nil)
		if err == nil {
			t.Fatal("expected error")
		}
		if !strings.Contains(err.Error(), "no tenants") {
			t.Errorf("expected 'no tenants' in error, got %v", err)
		}
	})

	t.Run("error when tenant credential contains newline", func(t *testing.T) {
		dir := t.TempDir()
		orig, _ := os.Getwd()
		t.Cleanup(func() { os.Chdir(orig) })
		os.Chdir(dir)

		t.Setenv("BINTRAIL_SOURCE_DSN", pcTestSourceDSN)
		writeShimYAML(t, dir, "tenants:\n  - mysql_user: \"bad\\nuser\"\n    mysql_password: 'p'\n")
		resetPCFlags()

		err := runProxySQLConfig(proxysqlConfigCmd, nil)
		if err == nil {
			t.Fatal("expected error for newline")
		}
		if !strings.Contains(err.Error(), "control character") {
			t.Errorf("expected 'control character' in error, got %v", err)
		}
	})

	t.Run("--output - writes to stdout", func(t *testing.T) {
		dir := t.TempDir()
		orig, _ := os.Getwd()
		t.Cleanup(func() { os.Chdir(orig) })
		os.Chdir(dir)

		t.Setenv("BINTRAIL_SOURCE_DSN", pcTestSourceDSN)
		writeShimYAML(t, dir, validShimYAML)
		resetPCFlags()
		pcOut = "-"

		r, w, _ := os.Pipe()
		origStdout := os.Stdout
		os.Stdout = w
		t.Cleanup(func() { os.Stdout = origStdout })

		done := make(chan []byte)
		go func() {
			var buf bytes.Buffer
			io.Copy(&buf, r)
			done <- buf.Bytes()
		}()

		if err := runProxySQLConfig(proxysqlConfigCmd, nil); err != nil {
			t.Fatal(err)
		}
		w.Close()

		out := string(<-done)
		if !strings.Contains(out, "INSERT INTO mysql_users") {
			t.Errorf("stdout missing expected SQL:\n%s", out)
		}
	})

	t.Run("refuses to overwrite", func(t *testing.T) {
		dir := t.TempDir()
		orig, _ := os.Getwd()
		t.Cleanup(func() { os.Chdir(orig) })
		os.Chdir(dir)

		t.Setenv("BINTRAIL_SOURCE_DSN", pcTestSourceDSN)
		writeShimYAML(t, dir, validShimYAML)
		os.WriteFile(filepath.Join(dir, "proxysql-setup.sql"), []byte("existing"), 0o644)
		resetPCFlags()

		err := runProxySQLConfig(proxysqlConfigCmd, nil)
		if err == nil {
			t.Fatal("expected error when output file exists")
		}
		if !strings.Contains(err.Error(), "already exists") {
			t.Errorf("expected 'already exists' in error, got %v", err)
		}
	})
}

func TestGenerateProxySQLSetupSQLDeterministic(t *testing.T) {
	tenants := []shimTenant{{MySQLUser: pcTestUser1, MySQLPassword: pcTestPassword1}}
	a := generateProxySQLSetupSQL("db.example.com", 3306, 3308, 6033, tenants, backendAuthNative)
	b := generateProxySQLSetupSQL("db.example.com", 3306, 3308, 6033, tenants, backendAuthNative)
	if a != b {
		t.Errorf("generateProxySQLSetupSQL must be deterministic; got two different outputs")
	}
}

func TestGenerateProxySQLSetupSQLSQLInjection(t *testing.T) {
	// A user containing single quotes must be safely escaped. The
	// password is hashed before being written into SQL so the worst it
	// can do at the SQL layer is alter the hash output — still safe to
	// quote, and we cover that path here too.
	tenants := []shimTenant{
		{MySQLUser: "ev'il", MySQLPassword: "p'p"},
	}
	out := generateProxySQLSetupSQL("db", 3306, 3308, 6033, tenants, backendAuthNative)

	// Username is quoted with the doubled single-quote.
	if !strings.Contains(out, "VALUES ('ev''il', '") {
		t.Errorf("expected escaped username; got:\n%s", out)
	}
	// Password is the SHA1 of the cleartext, also quoted.
	wantHash := nativePasswordHash("p'p")
	if !strings.Contains(out, "'"+wantHash+"', 990, 1);") {
		t.Errorf("expected hashed password %q in output; got:\n%s", wantHash, out)
	}
}

// TestGenerateProxySQLSetupSQLRenameIdempotent verifies that renaming a
// tenant in shim.yaml between runs leaves no orphan row: the second
// run's DELETE WHERE default_hostgroup = 990 catches the previous
// tenant's row even though its username is no longer in the current
// list. This locks in the design rationale for scoping the DELETE by
// hostgroup rather than by username.
func TestGenerateProxySQLSetupSQLRenameIdempotent(t *testing.T) {
	first := generateProxySQLSetupSQL("db", 3306, 3308, 6033,
		[]shimTenant{{MySQLUser: "old_user", MySQLPassword: "oldpw"}}, backendAuthNative)
	second := generateProxySQLSetupSQL("db", 3306, 3308, 6033,
		[]shimTenant{{MySQLUser: "new_user", MySQLPassword: "newpw"}}, backendAuthNative)

	// Both runs emit the same blanket DELETE, scoped only by hostgroup,
	// so the second apply also removes 'old_user' even though the name
	// no longer appears anywhere in the second SQL file.
	wantDelete := "DELETE FROM mysql_users WHERE default_hostgroup = 990;"
	if !strings.Contains(first, wantDelete) || !strings.Contains(second, wantDelete) {
		t.Errorf("both runs must contain hostgroup-scoped DELETE %q", wantDelete)
	}
	if strings.Contains(second, "old_user") {
		t.Error("second-run SQL must not reference the renamed-away tenant")
	}
	if !strings.Contains(second, "INSERT INTO mysql_users (username, password, default_hostgroup, active) VALUES ('new_user',") {
		t.Errorf("second-run SQL must INSERT the new tenant; got:\n%s", second)
	}
}

// TestGenerateProxySQLSetupSQLHostgroupPairing locks in the
// destination_hostgroup for each rule_id so a future swap of
// `passthroughHostgroup` and `shimHostgroup` would be caught even if
// individual fragment assertions still pass.
func TestGenerateProxySQLSetupSQLHostgroupPairing(t *testing.T) {
	tenants := []shimTenant{{MySQLUser: pcTestUser1, MySQLPassword: pcTestPassword1}}
	out := generateProxySQLSetupSQL("db", 3306, 3308, 6033, tenants, backendAuthNative)

	wants := []string{
		// passthrough server lives in passthrough hostgroup
		"INSERT INTO mysql_servers (hostgroup_id, hostname, port) VALUES (990,",
		// shim server lives in shim hostgroup
		"INSERT INTO mysql_servers (hostgroup_id, hostname, port) VALUES (991,",
		// users default to passthrough hostgroup (real MySQL by default)
		"default_hostgroup, active) VALUES ('app_user', '" + pcTestSHA1_1 + "', 990, 1);",
		// virtual-schema rules route to shim hostgroup, never passthrough
		"VALUES (990001, 1, '\\b_flashback\\.', 991, 1);",
		"VALUES (990002, 1, '\\b_diff\\.', 991, 1);",
		"VALUES (990003, 1, '\\b_snapshot\\.', 991, 1);",
		// hint-comment form (#288) also routes to shim hostgroup
		"VALUES (990004, 1, '/\\*\\+\\s*DBTRAIL_AT', 991, 1);",
		// bare AS OF on a real table (#385) also routes to shim hostgroup
		"VALUES (990006, 1, '\\bAS\\s+OF\\s+(TIMESTAMP\\s+)?''[^'']*''\\s*\\x3b?\\s*$', 991, 1);",
	}
	for _, w := range wants {
		if !strings.Contains(out, w) {
			t.Errorf("hostgroup pairing missing %q; full SQL:\n%s", w, out)
		}
	}
	// And explicitly: no rule should ever route a virtual schema to the
	// passthrough hostgroup.
	for _, bad := range []string{
		"VALUES (990001, 1, '\\b_flashback\\.', 990, 1)",
		"VALUES (990002, 1, '\\b_diff\\.', 990, 1)",
		"VALUES (990003, 1, '\\b_snapshot\\.', 990, 1)",
		"VALUES (990004, 1, '/\\*\\+\\s*DBTRAIL_AT', 990, 1)",
		"VALUES (990006, 1, '\\bAS\\s+OF\\s+(TIMESTAMP\\s+)?''[^'']*''\\s*\\x3b?\\s*$', 990, 1)",
	} {
		if strings.Contains(out, bad) {
			t.Errorf("virtual-schema rule must not target passthrough hostgroup, found %q", bad)
		}
	}
}

func TestRunProxySQLConfigStrictYAML(t *testing.T) {
	// A typo in shim.yaml (mysql_user_name vs mysql_user) used to silently
	// parse as empty, surfacing as the misleading "mysql_user is empty" error.
	// UnmarshalStrict now reports the unknown key directly.
	dir := t.TempDir()
	orig, _ := os.Getwd()
	t.Cleanup(func() { os.Chdir(orig) })
	os.Chdir(dir)

	t.Setenv("BINTRAIL_SOURCE_DSN", pcTestSourceDSN)
	writeShimYAML(t, dir, "tenants:\n  - mysql_user_name: app_user\n    mysql_password: 'p'\n")
	resetPCFlags()

	err := runProxySQLConfig(proxysqlConfigCmd, nil)
	if err == nil {
		t.Fatal("expected error for unknown YAML field")
	}
	if !strings.Contains(err.Error(), "mysql_user_name") {
		t.Errorf("error should name the unknown field, got %v", err)
	}
}

func TestParseProxySQLBackendIPv6(t *testing.T) {
	// Bracketed IPv6 with port.
	host, port, err := parseProxySQLBackend("u:p@tcp([2001:db8::1]:3306)/x", 3306)
	if err != nil {
		t.Fatal(err)
	}
	if host != "2001:db8::1" {
		t.Errorf("got host %q, want '2001:db8::1' (without brackets)", host)
	}
	if port != 3306 {
		t.Errorf("got port %d", port)
	}
}

func TestParseProxySQLBackendEmptyHost(t *testing.T) {
	_, _, err := parseProxySQLBackend("u:p@tcp(:3306)/x", 3306)
	if err == nil {
		t.Fatal("expected error for empty host")
	}
	if !strings.Contains(err.Error(), "empty host") {
		t.Errorf("expected 'empty host' in error, got %v", err)
	}
}

func TestRunProxySQLConfigPortRangeValidation(t *testing.T) {
	dir := t.TempDir()
	orig, _ := os.Getwd()
	t.Cleanup(func() { os.Chdir(orig) })
	os.Chdir(dir)

	t.Setenv("BINTRAIL_SOURCE_DSN", pcTestSourceDSN)
	writeShimYAML(t, dir, validShimYAML)

	t.Run("zero port rejected", func(t *testing.T) {
		resetPCFlags()
		pcMySQLPort = 0
		err := runProxySQLConfig(proxysqlConfigCmd, nil)
		if err == nil || !strings.Contains(err.Error(), "out of range") {
			t.Errorf("expected out-of-range error for port 0, got %v", err)
		}
	})

	t.Run("uint16-overflow port rejected", func(t *testing.T) {
		// 70000 used to silently truncate to uint16 → 4464, generating broken SQL.
		resetPCFlags()
		pcShimPort = 70000
		err := runProxySQLConfig(proxysqlConfigCmd, nil)
		if err == nil || !strings.Contains(err.Error(), "out of range") {
			t.Errorf("expected out-of-range error for port 70000, got %v", err)
		}
	})
}

func TestLoadShimTenantsControlChars(t *testing.T) {
	// Reject control chars beyond plain \r\n: \t in mysql_user, \0 in
	// mysql_password. Both would corrupt the generated SQL output.
	cases := []struct {
		name     string
		yamlBody string
		wantSub  string
	}{
		{
			name:     "tab in mysql_user",
			yamlBody: "tenants:\n  - mysql_user: \"app\\tuser\"\n    mysql_password: 'p'\n",
			wantSub:  "mysql_user contains control character",
		},
		{
			name:     "null byte in pass",
			yamlBody: "tenants:\n  - mysql_user: app_user\n    mysql_password: \"p\\u0000q\"\n",
			wantSub:  "mysql_password contains control character",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := writeShimYAML(t, dir, tc.yamlBody)
			_, err := loadShimTenants(path)
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("expected %q in error, got %v", tc.wantSub, err)
			}
		})
	}
}

func TestNativePasswordHash(t *testing.T) {
	// Pinned vectors against `SELECT PASSWORD(...)` in MySQL 5.7 /
	// ProxySQL — these are byte-identity checks against the canonical
	// mysql_native_password storage form. The "password" vector is
	// well-known across the MySQL ecosystem; the empty and UTF-8
	// vectors guard against accidental fixes that special-case empty
	// input or normalise input bytes (both would silently diverge
	// from MySQL's literal SHA1).
	cases := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "well-known vector \"password\"",
			in:   "password",
			want: "*2470C0C06DEE42FD1618BB99005ADCA2EC9D1E19",
		},
		{
			name: "empty cleartext (loadShimTenants rejects this; pinned for completeness)",
			in:   "",
			want: "*BE1BDEC0AA74B4DCB079943E70528096CCA985F8",
		},
		{
			// Multi-byte UTF-8: MySQL hashes the raw bytes, not
			// runes or NFC-normalised input. A well-meaning fix
			// using `[]rune` would silently produce a different
			// digest and break interop with any client that knows
			// the cleartext. The expected hash is the byte-level
			// SHA1(SHA1) of the UTF-8 encoding (10 bytes).
			name: "UTF-8 multi-byte \"pässwörd\"",
			in:   "pässwörd",
			want: "*0225EC5004ABB0B8CB557541FE53DE1A5D8CC825",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := nativePasswordHash(tc.in)
			if got != tc.want {
				t.Errorf("nativePasswordHash(%q) = %s, want %s", tc.in, got, tc.want)
			}
			if !strings.HasPrefix(got, "*") || len(got) != 41 {
				t.Errorf("expected `*` + 40 hex chars; got %q (len %d)", got, len(got))
			}
		})
	}
}

func TestParseProxySQLBackend(t *testing.T) {
	t.Run("DSN with port", func(t *testing.T) {
		host, port, err := parseProxySQLBackend("u:p@tcp(db.example.com:3307)/x", 3306)
		if err != nil {
			t.Fatal(err)
		}
		if host != "db.example.com" || port != 3307 {
			t.Errorf("got %s:%d", host, port)
		}
	})

	t.Run("DSN missing port falls back to flag", func(t *testing.T) {
		// go-sql-driver normalises the address to host:3306 if port is missing,
		// but we still verify the fallback logic works for an addr without ':'.
		host, port, err := parseProxySQLBackend("u:p@tcp(db.example.com)/x", 3306)
		if err != nil {
			t.Fatal(err)
		}
		if host != "db.example.com" {
			t.Errorf("got host %s", host)
		}
		if port != 3306 {
			t.Errorf("got port %d, want 3306", port)
		}
	})
}

// TestGenerateProxySQLSetupSQLNoLineComments locks in the #309 fix:
// every non-blank line of the generated SQL must NOT start with `-- `,
// because ProxySQL's admin parser treats each such line as its own
// statement and rejects the file with "ProxySQL Admin Error: not an
// error". Block comments (`/* ... */`) parse correctly.
func TestGenerateProxySQLSetupSQLNoLineComments(t *testing.T) {
	tenants := []shimTenant{{MySQLUser: pcTestUser1, MySQLPassword: pcTestPassword1}}
	out := generateProxySQLSetupSQL("db.example.com", 3306, 3308, 6033, tenants, backendAuthNative)

	for i, line := range strings.Split(out, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "--") {
			t.Errorf("line %d starts with `--` (ProxySQL admin rejects this); line=%q\nfull SQL:\n%s",
				i+1, line, out)
		}
	}
	if !strings.Contains(out, "/*") {
		t.Errorf("expected block-comment header `/*`; got:\n%s", out)
	}
	if !strings.Contains(out, "*/") {
		t.Errorf("expected block-comment terminator `*/`; got:\n%s", out)
	}
}

// TestGenerateProxySQLSetupSQLBackendAuthPlugin locks in the #310 fix:
// the password column emitted into mysql_users depends on the chosen
// backend auth plugin. native_password stores the SHA1 hash (default,
// preserves pre-#310 behaviour); caching_sha2_password stores the
// cleartext so ProxySQL can complete the SHA2 challenge against the
// MySQL 8.0+ backend.
func TestGenerateProxySQLSetupSQLBackendAuthPlugin(t *testing.T) {
	tenants := []shimTenant{{MySQLUser: "app_user", MySQLPassword: "s3cret!"}}

	t.Run("native_password stores SHA1 hash", func(t *testing.T) {
		out := generateProxySQLSetupSQL("db", 3306, 3308, 6033, tenants, backendAuthNative)
		wantHash := nativePasswordHash("s3cret!")
		if !strings.Contains(out, "'app_user', '"+wantHash+"', 990, 1") {
			t.Errorf("expected SHA1 hash %q in INSERT; got:\n%s", wantHash, out)
		}
		// Cleartext must NOT appear in the SQL — only the hash should.
		if strings.Contains(out, "'s3cret!'") {
			t.Errorf("cleartext leaked into native_password output:\n%s", out)
		}
	})

	t.Run("caching_sha2_password stores cleartext", func(t *testing.T) {
		out := generateProxySQLSetupSQL("db", 3306, 3308, 6033, tenants, backendAuthCaching)
		if !strings.Contains(out, "'app_user', 's3cret!', 990, 1") {
			t.Errorf("expected cleartext 's3cret!' in INSERT for caching_sha2_password; got:\n%s", out)
		}
		// The SHA1 hash must NOT appear when caching_sha2 is selected.
		hash := nativePasswordHash("s3cret!")
		if strings.Contains(out, hash) {
			t.Errorf("SHA1 hash %q leaked into caching_sha2 output:\n%s", hash, out)
		}
	})
}

// TestRunProxySQLConfigBackendPluginInvalid rejects unsupported plugin
// values rather than emitting hash-or-cleartext-or-nothing ambiguous SQL.
func TestRunProxySQLConfigBackendPluginInvalid(t *testing.T) {
	dir := t.TempDir()
	orig, _ := os.Getwd()
	t.Cleanup(func() { os.Chdir(orig) })
	os.Chdir(dir)

	t.Setenv("BINTRAIL_SOURCE_DSN", pcTestSourceDSN)
	writeShimYAML(t, dir, validShimYAML)
	resetPCFlags()
	pcBackendAuthPlugin = "sha256_password" // not supported

	err := runProxySQLConfig(proxysqlConfigCmd, nil)
	if err == nil {
		t.Fatal("expected error for unsupported plugin")
	}
	if !strings.Contains(err.Error(), "--backend-auth-plugin") {
		t.Errorf("error should name the flag, got %v", err)
	}
	if !strings.Contains(err.Error(), "sha256_password") {
		t.Errorf("error should echo the bad value, got %v", err)
	}
}

// TestRunProxySQLConfigForceOverwrites locks in the #311 fix:
// without --force, an existing output file still errors (preserving
// the safe default); with --force, the file is overwritten in place.
func TestRunProxySQLConfigForceOverwrites(t *testing.T) {
	t.Run("default still refuses to overwrite", func(t *testing.T) {
		dir := t.TempDir()
		orig, _ := os.Getwd()
		t.Cleanup(func() { os.Chdir(orig) })
		os.Chdir(dir)

		t.Setenv("BINTRAIL_SOURCE_DSN", pcTestSourceDSN)
		writeShimYAML(t, dir, validShimYAML)
		os.WriteFile(filepath.Join(dir, "proxysql-setup.sql"), []byte("existing"), 0o644)
		resetPCFlags()

		err := runProxySQLConfig(proxysqlConfigCmd, nil)
		if err == nil {
			t.Fatal("expected error when output exists and --force not set")
		}
		if !strings.Contains(err.Error(), "already exists") {
			t.Errorf("expected 'already exists' in error, got %v", err)
		}
		if !strings.Contains(err.Error(), "--force") {
			t.Errorf("error should mention --force as an escape hatch, got %v", err)
		}
		data, _ := os.ReadFile(filepath.Join(dir, "proxysql-setup.sql"))
		if string(data) != "existing" {
			t.Errorf("file was modified without --force; got %q", string(data))
		}
	})

	t.Run("--force overwrites in place", func(t *testing.T) {
		dir := t.TempDir()
		orig, _ := os.Getwd()
		t.Cleanup(func() { os.Chdir(orig) })
		os.Chdir(dir)

		t.Setenv("BINTRAIL_SOURCE_DSN", pcTestSourceDSN)
		writeShimYAML(t, dir, validShimYAML)
		os.WriteFile(filepath.Join(dir, "proxysql-setup.sql"), []byte("existing"), 0o644)
		resetPCFlags()
		pcForce = true

		if err := runProxySQLConfig(proxysqlConfigCmd, nil); err != nil {
			t.Fatalf("expected --force to overwrite cleanly, got %v", err)
		}
		data, _ := os.ReadFile(filepath.Join(dir, "proxysql-setup.sql"))
		body := string(data)
		if body == "existing" {
			t.Error("file was not overwritten with --force")
		}
		if !strings.Contains(body, "INSERT INTO mysql_users") {
			t.Errorf("--force output is missing expected SQL:\n%s", body)
		}
	})

	// Guards against a leak: O_TRUNC preserves the pre-existing inode's
	// permissions, so a previously 0o644 proxysql-setup.sql would stay
	// world-readable after --force even though OpenFile is called with
	// 0o600. With --backend-auth-plugin=caching_sha2_password the file
	// holds cleartext credentials — must be 0o600 after overwrite.
	t.Run("--force tightens permissions to 0o600", func(t *testing.T) {
		dir := t.TempDir()
		orig, _ := os.Getwd()
		t.Cleanup(func() { os.Chdir(orig) })
		os.Chdir(dir)

		t.Setenv("BINTRAIL_SOURCE_DSN", pcTestSourceDSN)
		writeShimYAML(t, dir, validShimYAML)
		outPath := filepath.Join(dir, "proxysql-setup.sql")
		if err := os.WriteFile(outPath, []byte("existing"), 0o644); err != nil {
			t.Fatalf("seed existing file: %v", err)
		}
		resetPCFlags()
		pcForce = true
		pcBackendAuthPlugin = backendAuthCaching

		if err := runProxySQLConfig(proxysqlConfigCmd, nil); err != nil {
			t.Fatalf("--force run failed: %v", err)
		}
		info, err := os.Stat(outPath)
		if err != nil {
			t.Fatalf("stat overwritten file: %v", err)
		}
		if mode := info.Mode().Perm(); mode != 0o600 {
			t.Errorf("expected mode 0o600 after --force overwrite, got 0o%o", mode)
		}
	})
}

// captureWarnLogs swaps slog.Default() with a text-handler writing to a
// bytes.Buffer for the duration of the test. Returns the buffer; the
// cleanup is registered with t. Use buf.String() in assertions.
func captureWarnLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return &buf
}

// TestValidateBackendAuthPlugin covers the pure SQL helper that probes
// mysql.user. The integration-side warn-emission decisions are tested
// separately in TestRunProxySQLConfigValidate_*.
//
// The helper drops the bogus host filter the previous version applied
// (mysql.user.host is the client-host pattern, not the server's
// hostname — the old `host = ?` branch was effectively dead code in
// production). All host rows for the requested user are returned.
func TestValidateBackendAuthPlugin(t *testing.T) {
	t.Run("returns all (host, plugin) rows for the user", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock: %v", err)
		}
		defer db.Close()

		mock.ExpectQuery("SELECT host, plugin FROM mysql.user").
			WithArgs("app_user").
			WillReturnRows(sqlmock.NewRows([]string{"host", "plugin"}).
				AddRow("%", "mysql_native_password").
				AddRow("localhost", "mysql_native_password"))

		got, err := validateBackendAuthPlugin(db, "app_user")
		if err != nil {
			t.Fatalf("validate: %v", err)
		}
		want := map[string]string{"%": "mysql_native_password", "localhost": "mysql_native_password"}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unmet expectations: %v", err)
		}
	})

	t.Run("returns split-plugin grants verbatim", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock: %v", err)
		}
		defer db.Close()

		// The realistic conflict case: same user, different plugins
		// per host pattern. QueryRow + LIMIT 1 in the previous impl
		// hid this; Query + iterate surfaces it for the orchestrator
		// to warn about.
		mock.ExpectQuery("SELECT host, plugin FROM mysql.user").
			WithArgs("app_user").
			WillReturnRows(sqlmock.NewRows([]string{"host", "plugin"}).
				AddRow("localhost", "mysql_native_password").
				AddRow("%", "caching_sha2_password"))

		got, err := validateBackendAuthPlugin(db, "app_user")
		if err != nil {
			t.Fatalf("validate: %v", err)
		}
		if got["localhost"] != "mysql_native_password" || got["%"] != "caching_sha2_password" {
			t.Errorf("split-plugin grants not preserved: %v", got)
		}
	})

	t.Run("propagates query error", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock: %v", err)
		}
		defer db.Close()

		mock.ExpectQuery("SELECT host, plugin FROM mysql.user").
			WillReturnError(errors.New("Access denied; you need (at least one of) the SELECT privilege(s)"))

		_, err = validateBackendAuthPlugin(db, "app_user")
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !strings.Contains(err.Error(), "Access denied") {
			t.Errorf("expected privilege error to propagate, got %v", err)
		}
	})

	t.Run("returns empty map when user not present", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock: %v", err)
		}
		defer db.Close()

		mock.ExpectQuery("SELECT host, plugin FROM mysql.user").
			WillReturnRows(sqlmock.NewRows([]string{"host", "plugin"}))

		got, err := validateBackendAuthPlugin(db, "app_user")
		if err != nil {
			t.Fatalf("validate: %v", err)
		}
		if len(got) != 0 {
			t.Errorf("expected empty map, got %v", got)
		}
	})
}

// TestRunProxySQLConfigValidate_pluginMatch: the tenant user's plugin in
// mysql.user matches --backend-auth-plugin; no warn log is emitted.
// Note we probe the TENANT user (app_user from validShimYAML), not the
// DSN user (user from pcTestSourceDSN) — this is the load-bearing fix
// over the original #327 implementation. ProxySQL re-handshakes as the
// tenant; probing the DSN user checked the wrong identity.
func TestRunProxySQLConfigValidate_pluginMatch(t *testing.T) {
	dir := t.TempDir()
	orig, _ := os.Getwd()
	t.Cleanup(func() { os.Chdir(orig) })
	os.Chdir(dir)

	t.Setenv("BINTRAIL_SOURCE_DSN", pcTestSourceDSN)
	writeShimYAML(t, dir, validShimYAML)
	resetPCFlags()
	pcValidate = true

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	mock.ExpectQuery("SELECT host, plugin FROM mysql.user").
		WithArgs("app_user"). // tenant user from validShimYAML
		WillReturnRows(sqlmock.NewRows([]string{"host", "plugin"}).
			AddRow("%", "mysql_native_password"))

	prevConnect := pcConnect
	pcConnect = func(string) (*sql.DB, error) { return db, nil }
	t.Cleanup(func() { pcConnect = prevConnect })

	buf := captureWarnLogs(t)

	if err := runProxySQLConfig(proxysqlConfigCmd, nil); err != nil {
		t.Fatalf("runProxySQLConfig: %v", err)
	}
	logs := buf.String()
	if strings.Contains(logs, "mismatch") {
		t.Errorf("unexpected mismatch warn for matching plugin; logs:\n%s", logs)
	}
	if strings.Contains(logs, "could not run") || strings.Contains(logs, "not found") {
		t.Errorf("unexpected skip/missing warn for clean path; logs:\n%s", logs)
	}
}

// TestRunProxySQLConfigValidate_pluginMismatch: the tenant user has
// caching_sha2_password but operator specified mysql_native_password;
// expect a warn log naming both plugins and the tenant_user.
func TestRunProxySQLConfigValidate_pluginMismatch(t *testing.T) {
	dir := t.TempDir()
	orig, _ := os.Getwd()
	t.Cleanup(func() { os.Chdir(orig) })
	os.Chdir(dir)

	t.Setenv("BINTRAIL_SOURCE_DSN", pcTestSourceDSN)
	writeShimYAML(t, dir, validShimYAML)
	resetPCFlags()
	pcValidate = true
	pcBackendAuthPlugin = backendAuthNative // operator says native

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	mock.ExpectQuery("SELECT host, plugin FROM mysql.user").
		WithArgs("app_user").
		WillReturnRows(sqlmock.NewRows([]string{"host", "plugin"}).
			AddRow("%", "caching_sha2_password"))

	prevConnect := pcConnect
	pcConnect = func(string) (*sql.DB, error) { return db, nil }
	t.Cleanup(func() { pcConnect = prevConnect })

	buf := captureWarnLogs(t)

	if err := runProxySQLConfig(proxysqlConfigCmd, nil); err != nil {
		t.Fatalf("runProxySQLConfig: %v", err)
	}
	logs := buf.String()
	if !strings.Contains(logs, "mismatch") {
		t.Errorf("expected mismatch warn; logs:\n%s", logs)
	}
	if !strings.Contains(logs, "caching_sha2_password") || !strings.Contains(logs, backendAuthNative) {
		t.Errorf("warn should name both plugins; logs:\n%s", logs)
	}
	if !strings.Contains(logs, "app_user") {
		t.Errorf("warn should name the tenant_user (app_user); logs:\n%s", logs)
	}
	// SQL must still be generated despite the warning.
	if _, err := os.Stat(filepath.Join(dir, "proxysql-setup.sql")); err != nil {
		t.Errorf("expected SQL file generated despite warn; stat: %v", err)
	}
}

// TestRunProxySQLConfigValidate_splitPluginGrants: same tenant user has
// different plugins per host pattern (legitimate setup: native locally,
// caching_sha2 remotely). The previous LIMIT 1 query silently picked
// one row in storage-engine order; this fix surfaces the conflict.
func TestRunProxySQLConfigValidate_splitPluginGrants(t *testing.T) {
	dir := t.TempDir()
	orig, _ := os.Getwd()
	t.Cleanup(func() { os.Chdir(orig) })
	os.Chdir(dir)

	t.Setenv("BINTRAIL_SOURCE_DSN", pcTestSourceDSN)
	writeShimYAML(t, dir, validShimYAML)
	resetPCFlags()
	pcValidate = true

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	mock.ExpectQuery("SELECT host, plugin FROM mysql.user").
		WithArgs("app_user").
		WillReturnRows(sqlmock.NewRows([]string{"host", "plugin"}).
			AddRow("localhost", "mysql_native_password").
			AddRow("%", "caching_sha2_password"))

	prevConnect := pcConnect
	pcConnect = func(string) (*sql.DB, error) { return db, nil }
	t.Cleanup(func() { pcConnect = prevConnect })

	buf := captureWarnLogs(t)

	if err := runProxySQLConfig(proxysqlConfigCmd, nil); err != nil {
		t.Fatalf("runProxySQLConfig: %v", err)
	}
	logs := buf.String()
	if !strings.Contains(logs, "split-plugin") {
		t.Errorf("expected split-plugin warn; logs:\n%s", logs)
	}
	if !strings.Contains(logs, "app_user") {
		t.Errorf("warn should name the tenant_user; logs:\n%s", logs)
	}
}

// TestRunProxySQLConfigValidate_userNotFound: the tenant user doesn't
// exist in mysql.user — distinct from a query failure (this is a hard
// operator error: the SQL we're about to write references a user that
// won't exist on the backend, and ProxySQL will fail at handshake).
func TestRunProxySQLConfigValidate_userNotFound(t *testing.T) {
	dir := t.TempDir()
	orig, _ := os.Getwd()
	t.Cleanup(func() { os.Chdir(orig) })
	os.Chdir(dir)

	t.Setenv("BINTRAIL_SOURCE_DSN", pcTestSourceDSN)
	writeShimYAML(t, dir, validShimYAML)
	resetPCFlags()
	pcValidate = true

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	mock.ExpectQuery("SELECT host, plugin FROM mysql.user").
		WithArgs("app_user").
		WillReturnRows(sqlmock.NewRows([]string{"host", "plugin"})) // empty

	prevConnect := pcConnect
	pcConnect = func(string) (*sql.DB, error) { return db, nil }
	t.Cleanup(func() { pcConnect = prevConnect })

	buf := captureWarnLogs(t)

	if err := runProxySQLConfig(proxysqlConfigCmd, nil); err != nil {
		t.Fatalf("runProxySQLConfig: %v", err)
	}
	logs := buf.String()
	if !strings.Contains(logs, "not found in mysql.user") {
		t.Errorf("expected 'not found in mysql.user' warn; logs:\n%s", logs)
	}
	if !strings.Contains(logs, "app_user") {
		t.Errorf("warn should name the tenant_user; logs:\n%s", logs)
	}
}

// TestRunProxySQLConfigValidate_queryError: probing mysql.user fails
// (e.g. permission denied); validation warns and continues — SQL still
// generated.
func TestRunProxySQLConfigValidate_queryError(t *testing.T) {
	dir := t.TempDir()
	orig, _ := os.Getwd()
	t.Cleanup(func() { os.Chdir(orig) })
	os.Chdir(dir)

	t.Setenv("BINTRAIL_SOURCE_DSN", pcTestSourceDSN)
	writeShimYAML(t, dir, validShimYAML)
	resetPCFlags()
	pcValidate = true

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	mock.ExpectQuery("SELECT host, plugin FROM mysql.user").
		WillReturnError(errors.New("Access denied for user 'user'@'%' to database 'mysql'"))

	prevConnect := pcConnect
	pcConnect = func(string) (*sql.DB, error) { return db, nil }
	t.Cleanup(func() { pcConnect = prevConnect })

	buf := captureWarnLogs(t)

	if err := runProxySQLConfig(proxysqlConfigCmd, nil); err != nil {
		t.Fatalf("runProxySQLConfig must not error on validation query failure: %v", err)
	}
	logs := buf.String()
	if !strings.Contains(logs, "could not run for tenant") {
		t.Errorf("expected 'could not run for tenant' warn; logs:\n%s", logs)
	}
	if !strings.Contains(logs, "query failed") {
		t.Errorf("expected 'query failed' in skip warn; logs:\n%s", logs)
	}
	if _, err := os.Stat(filepath.Join(dir, "proxysql-setup.sql")); err != nil {
		t.Errorf("expected SQL file generated despite validation failure; stat: %v", err)
	}
}

// TestRunProxySQLConfigValidate_connectError: connecting to the source
// fails entirely; we warn and continue. Locks in that validation never
// blocks SQL gen even when the DB is unreachable.
func TestRunProxySQLConfigValidate_connectError(t *testing.T) {
	dir := t.TempDir()
	orig, _ := os.Getwd()
	t.Cleanup(func() { os.Chdir(orig) })
	os.Chdir(dir)

	t.Setenv("BINTRAIL_SOURCE_DSN", pcTestSourceDSN)
	writeShimYAML(t, dir, validShimYAML)
	resetPCFlags()
	pcValidate = true

	prevConnect := pcConnect
	pcConnect = func(string) (*sql.DB, error) {
		return nil, errors.New("dial tcp: connection refused")
	}
	t.Cleanup(func() { pcConnect = prevConnect })

	buf := captureWarnLogs(t)

	if err := runProxySQLConfig(proxysqlConfigCmd, nil); err != nil {
		t.Fatalf("runProxySQLConfig must not error when connect fails: %v", err)
	}
	logs := buf.String()
	if !strings.Contains(logs, "could not connect") {
		t.Errorf("expected 'could not connect' warn; logs:\n%s", logs)
	}
	if _, err := os.Stat(filepath.Join(dir, "proxysql-setup.sql")); err != nil {
		t.Errorf("expected SQL file generated despite connect failure; stat: %v", err)
	}
}

// TestRunProxySQLConfigValidate_optInOnly verifies the default (no
// --validate) does NOT touch the DB. Guards against accidentally
// making the probe default-on.
func TestRunProxySQLConfigValidate_optInOnly(t *testing.T) {
	dir := t.TempDir()
	orig, _ := os.Getwd()
	t.Cleanup(func() { os.Chdir(orig) })
	os.Chdir(dir)

	t.Setenv("BINTRAIL_SOURCE_DSN", pcTestSourceDSN)
	writeShimYAML(t, dir, validShimYAML)
	resetPCFlags()
	// pcValidate stays false (default)

	called := false
	prevConnect := pcConnect
	pcConnect = func(string) (*sql.DB, error) {
		called = true
		return nil, errors.New("should never be called")
	}
	t.Cleanup(func() { pcConnect = prevConnect })

	if err := runProxySQLConfig(proxysqlConfigCmd, nil); err != nil {
		t.Fatalf("runProxySQLConfig: %v", err)
	}
	if called {
		t.Error("pcConnect was called when --validate was off; validation must be opt-in")
	}
}
