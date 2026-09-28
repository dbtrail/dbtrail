package cliapp

import (
	"crypto/sha1"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"unicode"

	"github.com/dbtrail/dbtrail/internal/cli"
	"github.com/dbtrail/dbtrail/internal/config"
	drivermysql "github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"
	yaml "go.yaml.in/yaml/v2"
)

// Hostgroups and rule IDs are placed in the 990* range so they are
// extremely unlikely to collide with operator-managed ProxySQL config.
// The DELETE-then-INSERT pattern in the generated SQL only touches
// rows in this range plus the named tenant users.
const (
	passthroughHostgroup = 990
	shimHostgroup        = 991
	ruleIDFlashback      = 990001
	ruleIDDiff           = 990002
	ruleIDSnapshot       = 990003
	// ruleIDHint matches the optimizer-hint comment form (#288):
	//   SELECT /*+ DBTRAIL_AT='<ts>' */ * FROM <table> ...
	// The shim parser detects the hint, strips it, and rewrites the
	// query as a TypeFlashback point-lookup against the real table
	// name. Without this rule, queries using the docs-advertised
	// hint form route to the real MySQL (which silently ignores the
	// unknown hint) instead of the shim — the feature is undelivered
	// end-to-end. The rule is intentionally ordered AFTER the three
	// virtual-schema rules so that a query like
	//   SELECT /*+ DBTRAIL_AT='...' */ * FROM _flashback.t AS OF ...
	// (no real-world need but defensible) still routes via the more
	// specific virtual-schema rule first.
	ruleIDHint = 990004
	// ruleIDShowTables matches `SHOW [FULL] TABLES FROM _flashback/_diff/_snapshot`
	// (#315). The virtual schemas have no backend MySQL counterpart, so the
	// query would otherwise route to passthrough and return ER_BAD_DB
	// (1049 "Unknown database"). With this rule, the shim handler answers
	// from the latest schema_snapshots row. Ordered AFTER the three virtual-
	// schema rules + the hint rule so any future overlap (e.g. a SHOW
	// containing a dotted virtual reference) routes via the more specific
	// rule first.
	ruleIDShowTables = 990005
	// ruleIDAsOf matches the bare time-travel form on a real table (#385):
	//   SELECT * FROM [<schema>.]<table> [WHERE <col> = <val>] AS OF '<ts>'
	// — the README-tagline shape. The pattern is END-ANCHORED ($ = end of
	// statement; ProxySQL match_pattern is PCRE with multiline OFF by
	// default, so $ means end-of-string): only statements that FINISH with
	// the AS OF clause route to the shim. That anchor is the load-bearing
	// false-positive defense — the shim has no passthrough, so a benign
	// query mis-routed here breaks for the client. Residual surface: a
	// statement whose final token is a string literal of the exact form
	// `AS OF '<text>'` (rare; documented in docs/time-travel-sql.md).
	// Ordered LAST so the virtual-schema forms (which can also end in
	// AS OF '<ts>') route via their more specific rules first.
	ruleIDAsOf = 990006
)

var proxysqlConfigCmd = &cobra.Command{
	Use:   "proxysql-config",
	Short: "Generate ProxySQL setup SQL from .bintrail.env and shim.yaml",
	Long: `Emits a SQL script that, when applied to a ProxySQL admin port (default
6032), configures ProxySQL to route _flashback / _diff / _snapshot virtual
schemas to the dbtrail-shim hostgroup and everything else to the customer's
real MySQL.

Reads BINTRAIL_SOURCE_DSN from .bintrail.env (host of the passthrough
backend) and shim.yaml (tenant credentials). The SQL is idempotent;
re-running it produces the same final state.

Use --output - to write to stdout instead of a file.`,
	RunE: runProxySQLConfig,
}

var (
	pcOut               string
	pcShimConfig        string
	pcMySQLPort         uint
	pcShimPort          uint
	pcProxySQLMySQLPort uint
	pcForce             bool
	pcBackendAuthPlugin string
	pcValidate          bool
)

// pcConnect is the indirection seam for opening the source DB during
// --validate pre-flight. Tests override it to inject a sqlmock DB without
// requiring a live MySQL. Defaults to config.Connect.
var pcConnect = config.Connect

// Valid values for --backend-auth-plugin. Default is the SHA1
// (mysql_native_password) form because that preserves pre-#310 behaviour
// and avoids silently weakening security posture on installs that
// already use native_password. Operators on caching_sha2_password
// (MySQL 8.0+ default) must opt in explicitly — ProxySQL needs the
// cleartext to complete the backend handshake under that plugin.
const (
	backendAuthNative  = "mysql_native_password"
	backendAuthCaching = "caching_sha2_password"
)

func init() {
	cli.AddOutputFlag(proxysqlConfigCmd, &pcOut, "proxysql-setup.sql", "Output path for the generated SQL (use - for stdout)", cli.OutAlias)
	proxysqlConfigCmd.Flags().StringVar(&pcShimConfig, "shim-config", "shim.yaml", "Path to the shim.yaml produced by 'bintrail init-shim' and edited by you")
	proxysqlConfigCmd.Flags().UintVar(&pcMySQLPort, "mysql-port", 3306, "Fallback MySQL port if BINTRAIL_SOURCE_DSN does not include one")
	proxysqlConfigCmd.Flags().UintVar(&pcShimPort, "shim-port", 3308, "Port the dbtrail-shim is listening on (matches shim.yaml's listen)")
	proxysqlConfigCmd.Flags().UintVar(&pcProxySQLMySQLPort, "proxysql-mysql-port", 6033, "ProxySQL's client-facing MySQL protocol port (used in the help comment)")
	proxysqlConfigCmd.Flags().BoolVarP(&pcForce, "force", "f", false, "Overwrite the output file if it already exists")
	proxysqlConfigCmd.Flags().StringVar(&pcBackendAuthPlugin, "backend-auth-plugin", backendAuthNative,
		"Backend MySQL auth plugin: 'mysql_native_password' stores SHA1 (default, current behaviour); "+
			"'caching_sha2_password' stores cleartext. NOTE: caching_sha2_password requires TLS between "+
			"ProxySQL and the MySQL backend, OR the SHA2 auth cache already primed for that user. Without "+
			"either, the ProxySQL→backend handshake fails on first connect; see ProxySQL docs.")
	proxysqlConfigCmd.Flags().BoolVar(&pcValidate, "validate", false,
		"Opt-in pre-flight: connect to BINTRAIL_SOURCE_DSN and verify the auth plugin in mysql.user for each "+
			"tenant from shim.yaml matches --backend-auth-plugin. ProxySQL re-handshakes as the tenant user "+
			"(NOT the DSN user), so we probe tenants. Warns (does not error) on mismatch, on split-plugin "+
			"grants (same user with different plugins per host), or when the probe itself fails (e.g. "+
			"SELECT on mysql.user denied, common for non-admin DSN users; the warning then says validation "+
			"could not run). Never blocks SQL generation.")
	bindCommandEnv(proxysqlConfigCmd)
	rootCmd.AddCommand(proxysqlConfigCmd)
}

// shimTenant declares every field bintrail init-shim emits in a tenant
// block. The strict YAML decoder used by loadShimTenants requires the
// struct to know about every key — so we declare ServerID, SourceDSN,
// AgentURL, AgentToken (legacy, kept for parse-compat), and MySQLPassSHA1
// (legacy, kept for parse-compat) just to satisfy strict mode and let
// it catch real typos like "mysql_user_name".
type shimTenant struct {
	ServerID      string `yaml:"server_id"`
	SourceDSN     string `yaml:"source_dsn"`
	AgentURL      string `yaml:"agent_url"`
	AgentToken    string `yaml:"agent_token"`
	MySQLUser     string `yaml:"mysql_user"`
	MySQLPassword string `yaml:"mysql_password"`
	// MySQLPassSHA1 is the deprecated < 0.7.2 field. It used to be the
	// only way to declare a tenant's password, but the shim cannot use
	// it (go-mysql/server's mysql_native_password handler needs the
	// cleartext to validate the client's scrambled response). Now the
	// SHA1 is recomputed from MySQLPassword at proxysql-config time.
	// Kept here purely so UnmarshalStrict accepts shim.yaml files that
	// still have the old field; loadShimTenants emits a clear error
	// pointing to the migration path.
	MySQLPassSHA1 string `yaml:"mysql_pass_sha1"`
}

type shimConfig struct {
	Listen  string       `yaml:"listen"`
	Tenants []shimTenant `yaml:"tenants"`
}

func runProxySQLConfig(cmd *cobra.Command, args []string) error {
	sourceDSN := os.Getenv("BINTRAIL_SOURCE_DSN")
	if sourceDSN == "" {
		return fmt.Errorf("missing required env var: BINTRAIL_SOURCE_DSN\nRun 'bintrail config init' to scaffold .bintrail.env, then set this value.")
	}
	for _, p := range []struct {
		name string
		val  uint
	}{
		{"--mysql-port", pcMySQLPort},
		{"--shim-port", pcShimPort},
		{"--proxysql-mysql-port", pcProxySQLMySQLPort},
	} {
		if p.val == 0 || p.val > 65535 {
			return fmt.Errorf("%s=%d is out of range (1..65535)", p.name, p.val)
		}
	}

	switch pcBackendAuthPlugin {
	case backendAuthNative, backendAuthCaching:
	default:
		return fmt.Errorf("--backend-auth-plugin=%q is not supported (allowed: %s, %s)",
			pcBackendAuthPlugin, backendAuthNative, backendAuthCaching)
	}

	host, port, err := parseProxySQLBackend(sourceDSN, uint16(pcMySQLPort))
	if err != nil {
		return err
	}

	tenants, err := loadShimTenants(pcShimConfig)
	if err != nil {
		return err
	}

	// Opt-in pre-flight: probe mysql.user for EACH TENANT's plugin and
	// warn-and-continue when it doesn't match --backend-auth-plugin
	// (#327). ProxySQL re-handshakes as the tenant user, NOT the DSN
	// user — so iterating tenants from shim.yaml is the only meaningful
	// signal. Validation failures never block SQL generation (the
	// operator can re-apply later).
	if pcValidate {
		runBackendAuthPluginValidation(sourceDSN, tenants, pcBackendAuthPlugin)
	}

	content := generateProxySQLSetupSQL(host, port, uint16(pcShimPort), uint16(pcProxySQLMySQLPort), tenants, pcBackendAuthPlugin)

	if pcOut == "-" {
		_, err := io.WriteString(os.Stdout, content)
		return err
	}

	// Default (no --force): O_EXCL closes the stat-then-write TOCTOU
	// window so a file appearing between check and write does not get
	// silently overwritten. --force trades that guarantee for
	// idempotent re-runs in automation (#311).
	flags := os.O_WRONLY | os.O_CREATE | os.O_EXCL
	if pcForce {
		flags = os.O_WRONLY | os.O_CREATE | os.O_TRUNC
		if _, statErr := os.Stat(pcOut); statErr == nil {
			fmt.Printf("overwriting existing %s\n", pcOut)
		}
	}
	f, err := os.OpenFile(pcOut, flags, 0o600)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return fmt.Errorf("file already exists: %s\nRemove it first, edit it directly, or pass --force to overwrite.", pcOut)
		}
		return fmt.Errorf("create %s: %w", pcOut, err)
	}
	// OpenFile only applies the mode arg on file *creation*. On --force
	// overwrite the existing inode keeps its prior permissions, so a
	// previously world-readable proxysql-setup.sql would stay world-readable
	// even though we passed 0o600 — a real leak when --backend-auth-plugin=
	// caching_sha2_password puts cleartext credentials in the output. Chmod
	// unconditionally so both new and overwritten files end up at 0o600.
	if err := f.Chmod(0o600); err != nil {
		f.Close()
		return fmt.Errorf("chmod %s: %w", pcOut, err)
	}
	if _, err := f.WriteString(content); err != nil {
		f.Close()
		return fmt.Errorf("write %s: %w", pcOut, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close %s: %w", pcOut, err)
	}

	fmt.Printf("ProxySQL setup SQL written to %s\n", pcOut)
	fmt.Printf("Apply it: mysql -u admin -P 6032 -h <proxysql-host> < %s\n", pcOut)
	return nil
}

// runBackendAuthPluginValidation orchestrates the --validate path:
// open a connection, then for each tenant from shim.yaml probe
// mysql.user for ALL (host, plugin) grants matching its mysql_user
// and emit the appropriate warn log on mismatch, split-plugin, or
// missing-row. Any failure short-circuits per-tenant with a warn —
// SQL generation always proceeds regardless (#327).
//
// Why tenants, not the DSN user: ProxySQL re-handshakes against the
// backend as the tenant user (the one stored in mysql_users at apply
// time). Probing the DSN user (typically the bintrail admin/indexer
// account) checks the wrong identity entirely.
func runBackendAuthPluginValidation(sourceDSN string, tenants []shimTenant, expected string) {
	if len(tenants) == 0 {
		slog.Warn("backend auth-plugin validation skipped: no tenants in shim.yaml")
		return
	}
	db, err := pcConnect(sourceDSN)
	if err != nil {
		slog.Warn("backend auth-plugin validation skipped: could not connect to source", "err", err)
		return
	}
	defer db.Close()
	for _, t := range tenants {
		grants, err := validateBackendAuthPlugin(db, t.MySQLUser)
		if err != nil {
			// Most common cause: DSN user lacks SELECT on mysql.user
			// (security best practice for non-admin DSNs). Either way
			// we couldn't verify this tenant — warn and continue.
			slog.Warn("backend auth-plugin validation could not run for tenant: query failed",
				"err", err, "tenant_user", t.MySQLUser)
			continue
		}
		if len(grants) == 0 {
			// User not present in mysql.user. Distinct from a query
			// failure — this is a HARD operator error (the generated
			// ProxySQL config will reference a user that doesn't
			// exist on the backend; the handshake will fail). Worth
			// a separate, louder warn so it doesn't get filed under
			// "probably permissions".
			slog.Warn(
				"backend auth-plugin validation: tenant user not found in mysql.user — ProxySQL handshake will fail",
				"tenant_user", t.MySQLUser,
			)
			continue
		}
		// Collect distinct plugins across all host patterns for this
		// user. A split (same user, different plugins per host) is its
		// own diagnostic — operators with both `@'localhost'` and
		// `@'%'` rows configured asymmetrically need to know.
		hostPlugins := make(map[string]string, len(grants))
		distinctPlugins := make(map[string]struct{}, 2)
		for host, plugin := range grants {
			hostPlugins[host] = plugin
			distinctPlugins[plugin] = struct{}{}
		}
		if len(distinctPlugins) > 1 {
			slog.Warn(
				"backend auth-plugin validation: split-plugin grants for tenant — ProxySQL handshake outcome depends on which host pattern matches the connection",
				"tenant_user", t.MySQLUser, "host_plugins", hostPlugins, "expected", expected,
			)
			continue
		}
		// Single plugin across all hosts — compare to expected.
		var actual string
		for p := range distinctPlugins {
			actual = p
		}
		if actual != expected {
			hosts := make([]string, 0, len(hostPlugins))
			for host := range hostPlugins {
				hosts = append(hosts, host)
			}
			slog.Warn(
				"backend auth plugin mismatch — ProxySQL backend handshake will fail at runtime",
				"expected", expected, "actual", actual,
				"tenant_user", t.MySQLUser, "hosts", hosts,
			)
		}
	}
}

// validateBackendAuthPlugin returns a map of host-pattern → auth plugin
// for every mysql.user row matching the given user. Empty map means no
// rows (the user doesn't exist on this server). Non-nil error means the
// query itself failed (typically: DSN user lacks SELECT on mysql.user).
//
// Returning a map (vs. a single string from QueryRow + LIMIT 1) is the
// load-bearing change vs. the original #327 implementation. The
// previous code silently picked one row in non-deterministic
// storage-engine order, hiding split-plugin grants and giving operators
// false confidence after a passing --validate run.
//
// Note: mysql.user.host is the CLIENT host pattern (where the user may
// connect FROM), not the server's hostname. The original code passed
// the backend's hostname as a filter, which was a category error — the
// match condition was effectively dead in production. This version
// drops the host filter entirely; the caller iterates all matches.
func validateBackendAuthPlugin(db *sql.DB, user string) (grants map[string]string, err error) {
	rows, err := db.Query("SELECT host, plugin FROM mysql.user WHERE user = ?", user)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	grants = make(map[string]string)
	for rows.Next() {
		var host, plugin string
		if err := rows.Scan(&host, &plugin); err != nil {
			return nil, err
		}
		grants[host] = plugin
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return grants, nil
}

// parseProxySQLBackend extracts the host and port from a go-sql-driver
// DSN. Uses net.SplitHostPort so bracketed IPv6 addresses ("[::1]:3306")
// are handled correctly. If the DSN address has no port, fallbackPort
// is used; an empty host is rejected.
func parseProxySQLBackend(dsn string, fallbackPort uint16) (host string, port uint16, err error) {
	cfg, parseErr := drivermysql.ParseDSN(dsn)
	if parseErr != nil {
		return "", 0, fmt.Errorf("invalid BINTRAIL_SOURCE_DSN: %w", parseErr)
	}
	if strings.EqualFold(cfg.Net, "unix") {
		return "", 0, fmt.Errorf("BINTRAIL_SOURCE_DSN uses a unix socket; ProxySQL routing requires a TCP host:port")
	}
	addr := cfg.Addr
	if addr == "" {
		return "", 0, fmt.Errorf("BINTRAIL_SOURCE_DSN has no address")
	}
	h, p, splitErr := net.SplitHostPort(addr)
	if splitErr != nil {
		// No port in addr — treat the whole thing as host (and reject if it
		// itself looks like a bracketed IPv6 with no port: "[::1]").
		h = strings.Trim(addr, "[]")
		p = ""
	}
	if h == "" {
		return "", 0, fmt.Errorf("BINTRAIL_SOURCE_DSN has an empty host: %q", addr)
	}
	if p == "" {
		return h, fallbackPort, nil
	}
	portN, convErr := strconv.ParseUint(p, 10, 16)
	if convErr != nil {
		return "", 0, fmt.Errorf("invalid port in BINTRAIL_SOURCE_DSN: %w", convErr)
	}
	return h, uint16(portN), nil
}

// loadShimTenants reads shim.yaml from path, validates each tenant has
// non-empty mysql_user and mysql_password free of newlines, and returns
// the resulting list.
func loadShimTenants(path string) ([]shimTenant, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("shim config not found at %s\nRun 'bintrail init-shim' to scaffold one, then fill in mysql_user / mysql_password.", path)
		}
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	var cfg shimConfig
	// Strict mode rejects unknown YAML keys so a typo like "mysql_user_name:"
	// surfaces as a clear parse error rather than silently parsing as an
	// empty mysql_user.
	if err := yaml.UnmarshalStrict(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	if len(cfg.Tenants) == 0 {
		return nil, fmt.Errorf("%s has no tenants", path)
	}
	for i, t := range cfg.Tenants {
		if t.MySQLUser == "" {
			return nil, fmt.Errorf("%s tenant #%d: mysql_user is empty (uncomment and fill in the TODO line)", path, i+1)
		}
		if t.MySQLPassword == "" {
			if t.MySQLPassSHA1 != "" {
				return nil, fmt.Errorf(
					"%s tenant #%d (mysql_user=%s): mysql_password is required; mysql_pass_sha1 alone is no longer accepted (>= 0.7.2). "+
						"Replace mysql_pass_sha1 with mysql_password: '<cleartext>'; the SHA1 is recomputed here automatically",
					path, i+1, t.MySQLUser)
			}
			return nil, fmt.Errorf("%s tenant #%d (mysql_user=%s): mysql_password is empty (set the cleartext password your application uses to connect)", path, i+1, t.MySQLUser)
		}
		if r := firstControlRune(t.MySQLUser); r >= 0 {
			return nil, fmt.Errorf("%s tenant #%d: mysql_user contains control character U+%04X", path, i+1, r)
		}
		if r := firstControlRune(t.MySQLPassword); r >= 0 {
			return nil, fmt.Errorf("%s tenant #%d: mysql_password contains control character U+%04X", path, i+1, r)
		}
		// Mirror internal/shim/auth.go's behaviour: when both fields
		// are set, mysql_password wins and the legacy SHA1 is silently
		// dropped. Warn so an operator who half-migrated (added the
		// new field but forgot to delete the old) can clean up rather
		// than leaving stale state in shim.yaml.
		if t.MySQLPassSHA1 != "" {
			slog.Warn(
				"shim.yaml: mysql_pass_sha1 is no longer used (the SHA1 is recomputed from mysql_password); the field is ignored — remove it to clean up",
				"tenant", i+1, "mysql_user", t.MySQLUser,
			)
		}
	}
	return cfg.Tenants, nil
}

// nativePasswordHash returns ProxySQL's `*<UPPER_HEX>` storage form for
// a cleartext mysql_native_password — i.e. SHA1(SHA1(password)). This
// matches what `SELECT PASSWORD('<pw>')` produces in MySQL 5.7 and what
// ProxySQL stores in mysql_users.password under default config.
//
// We compute the hash here (rather than asking the operator to run a
// SHA1 recipe and paste it into shim.yaml) so shim.yaml has a single
// source of truth: the cleartext password. The shim itself needs that
// cleartext to validate ProxySQL-forwarded auth responses, so it has
// to live in shim.yaml regardless; emitting the SHA1 ourselves removes
// the manual derivation step the operator used to perform.
func nativePasswordHash(password string) string {
	first := sha1.Sum([]byte(password))
	second := sha1.Sum(first[:])
	return "*" + strings.ToUpper(fmt.Sprintf("%x", second))
}

// firstControlRune returns the first control rune in s (per
// unicode.IsControl), or -1 if none. Control characters in tenant
// credentials are rejected because they corrupt the SQL output:
// sqlQuote only escapes ', not '\n', '\t', '\0', etc.
func firstControlRune(s string) rune {
	for _, r := range s {
		if unicode.IsControl(r) {
			return r
		}
	}
	return -1
}

func generateProxySQLSetupSQL(host string, mysqlPort, shimPort, proxysqlMySQLPort uint16, tenants []shimTenant, backendAuthPlugin string) string {
	var sb strings.Builder
	// Block-comment form (/* ... */) instead of line comments (-- ...):
	// ProxySQL's admin SQL parser treats each `-- ...` line as its own
	// statement and rejects the whole file with the misleading
	// "ProxySQL Admin Error: not an error" (#309). Block comments are
	// parsed correctly and let us keep the human-readable header.
	sb.WriteString("/*\n")
	sb.WriteString(" * Bintrail time-travel SQL: ProxySQL setup\n")
	sb.WriteString(" * Generated by bintrail proxysql-config. See docs/time-travel-sql.md.\n")
	sb.WriteString(" *\n")
	sb.WriteString(" * This script manages the following ProxySQL resources, all in the\n")
	sb.WriteString(" * 990* numeric range to avoid colliding with operator-managed rules:\n")
	fmt.Fprintf(&sb, " *   * mysql_servers in hostgroups %d (passthrough) and %d (shim)\n", passthroughHostgroup, shimHostgroup)
	fmt.Fprintf(&sb, " *   * mysql_query_rules with rule_id in %d..%d\n", ruleIDFlashback, ruleIDAsOf)
	sb.WriteString(" *   * mysql_users named in shim.yaml (these become bintrail-managed)\n")
	sb.WriteString(" *\n")
	sb.WriteString(" * Apply this file to the ProxySQL admin port:\n")
	sb.WriteString(" *     mysql -u admin -P 6032 -h <proxysql-host> < proxysql-setup.sql\n")
	sb.WriteString(" *\n")
	fmt.Fprintf(&sb, " * Your application then connects to ProxySQL on port %d.\n", proxysqlMySQLPort)
	sb.WriteString(" * Re-running this script is idempotent.\n")
	fmt.Fprintf(&sb, " * Backend auth plugin: %s.\n", backendAuthPlugin)
	sb.WriteString(" */\n")
	sb.WriteString("\n")

	// Wrap the table edits in a transaction so a partial failure (e.g. a
	// constraint violation on one INSERT) rolls back the whole change set.
	// LOAD/SAVE statements are admin commands rather than DML and are
	// emitted after the COMMIT.
	sb.WriteString("BEGIN;\n")
	sb.WriteString("\n")

	fmt.Fprintf(&sb, "DELETE FROM mysql_servers WHERE hostgroup_id IN (%d, %d);\n", passthroughHostgroup, shimHostgroup)
	fmt.Fprintf(&sb, "INSERT INTO mysql_servers (hostgroup_id, hostname, port) VALUES (%d, %s, %d);\n", passthroughHostgroup, sqlQuote(host), mysqlPort)
	fmt.Fprintf(&sb, "INSERT INTO mysql_servers (hostgroup_id, hostname, port) VALUES (%d, '127.0.0.1', %d);\n", shimHostgroup, shimPort)
	sb.WriteString("\n")

	// DELETE is scoped strictly to bintrail-managed rows by default_hostgroup,
	// never by username alone. This:
	//   * cleans rows from a previous run whose username was renamed in
	//     shim.yaml between runs (the old row still lives in hostgroup 990).
	//   * does NOT destroy an operator's pre-existing user that happens to
	//     share a name with a tenant — if there is a collision, the INSERT
	//     below fails loudly with a PRIMARY KEY violation rather than
	//     silently overwriting operator config.
	fmt.Fprintf(&sb, "DELETE FROM mysql_users WHERE default_hostgroup = %d;\n", passthroughHostgroup)
	for _, t := range tenants {
		// Password storage depends on the backend's auth plugin (#310):
		//   * mysql_native_password — ProxySQL stores SHA1(SHA1(pw)) and
		//     can re-handshake from that. Default; preserves pre-#310
		//     behaviour for installs on native_password.
		//   * caching_sha2_password — ProxySQL needs the cleartext to
		//     complete the SHA2 challenge against the backend. MySQL
		//     8.0+ default; without this branch, every tenant's
		//     ProxySQL→MySQL leg silently fails with Access denied.
		var stored string
		if backendAuthPlugin == backendAuthCaching {
			stored = t.MySQLPassword
		} else {
			stored = nativePasswordHash(t.MySQLPassword)
		}
		fmt.Fprintf(&sb, "INSERT INTO mysql_users (username, password, default_hostgroup, active) VALUES (%s, %s, %d, 1);\n",
			sqlQuote(t.MySQLUser), sqlQuote(stored), passthroughHostgroup)
	}
	sb.WriteString("\n")

	fmt.Fprintf(&sb, "DELETE FROM mysql_query_rules WHERE rule_id IN (%d, %d, %d, %d, %d, %d);\n", ruleIDFlashback, ruleIDDiff, ruleIDSnapshot, ruleIDHint, ruleIDShowTables, ruleIDAsOf)
	fmt.Fprintf(&sb, "INSERT INTO mysql_query_rules (rule_id, active, match_pattern, destination_hostgroup, apply) VALUES (%d, 1, '\\b_flashback\\.', %d, 1);\n", ruleIDFlashback, shimHostgroup)
	fmt.Fprintf(&sb, "INSERT INTO mysql_query_rules (rule_id, active, match_pattern, destination_hostgroup, apply) VALUES (%d, 1, '\\b_diff\\.', %d, 1);\n", ruleIDDiff, shimHostgroup)
	fmt.Fprintf(&sb, "INSERT INTO mysql_query_rules (rule_id, active, match_pattern, destination_hostgroup, apply) VALUES (%d, 1, '\\b_snapshot\\.', %d, 1);\n", ruleIDSnapshot, shimHostgroup)
	// Hint-comment form (#288): the shim's parser detects
	// /*+ DBTRAIL_AT='<ts>' */ in the leading optimizer-hint position
	// and rewrites the query to a _flashback point-lookup. The
	// match_pattern intentionally anchors to /\*\+\s*DBTRAIL_AT so a
	// table or column literally named DBTRAIL_AT (or a string value
	// containing that text) doesn't get false-routed to the shim.
	fmt.Fprintf(&sb, "INSERT INTO mysql_query_rules (rule_id, active, match_pattern, destination_hostgroup, apply) VALUES (%d, 1, '/\\*\\+\\s*DBTRAIL_AT', %d, 1);\n", ruleIDHint, shimHostgroup)
	// SHOW TABLES FROM <virtual> (#315): routes interactive table-listing
	// against the three virtual schemas to the shim, which answers from
	// the latest schema snapshot. Without this rule the query would hit
	// the real MySQL and get ER_BAD_DB.
	fmt.Fprintf(&sb, "INSERT INTO mysql_query_rules (rule_id, active, match_pattern, destination_hostgroup, apply) VALUES (%d, 1, '^\\s*SHOW\\s+(FULL\\s+)?TABLES\\s+(FROM|IN)\\s+`?_(flashback|diff|snapshot)`?', %d, 1);\n", ruleIDShowTables, shimHostgroup)
	// Bare AS OF on a real table (#385): END-ANCHORED — only statements
	// that FINISH with the AS OF clause route to the shim, so "AS OF"
	// inside a string literal mid-query stays on passthrough. Matches the
	// shim parser's asOfRealProbeRE semantic exactly, so router and shim
	// always agree on which statements are this shape. (The '' doubling is
	// SQL string escaping; the stored PCRE contains single quotes. The
	// optional trailing semicolon is written \x3b — PCRE hex escape for
	// ';' — because a literal ';' inside the pattern splits the statement
	// in half under any per-';' statement splitter, e.g. the e2e harness
	// or an operator's apply script.)
	fmt.Fprintf(&sb, "INSERT INTO mysql_query_rules (rule_id, active, match_pattern, destination_hostgroup, apply) VALUES (%d, 1, '\\bAS\\s+OF\\s+(TIMESTAMP\\s+)?''[^'']*''\\s*\\x3b?\\s*$', %d, 1);\n", ruleIDAsOf, shimHostgroup)
	sb.WriteString("\n")

	sb.WriteString("COMMIT;\n")
	sb.WriteString("\n")

	sb.WriteString("LOAD MYSQL SERVERS TO RUNTIME;\n")
	sb.WriteString("LOAD MYSQL USERS TO RUNTIME;\n")
	sb.WriteString("LOAD MYSQL QUERY RULES TO RUNTIME;\n")
	sb.WriteString("SAVE MYSQL SERVERS TO DISK;\n")
	sb.WriteString("SAVE MYSQL USERS TO DISK;\n")
	sb.WriteString("SAVE MYSQL QUERY RULES TO DISK;\n")

	return sb.String()
}

// sqlQuote wraps s as a SQL single-quoted string literal, doubling any
// embedded single quotes. ProxySQL admin uses SQLite-style quoting so
// this is the safe escape.
func sqlQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}
