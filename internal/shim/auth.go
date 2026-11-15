package shim

import (
	"errors"
	"fmt"
	"log/slog"
	"os"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"
	yaml "go.yaml.in/yaml/v2"
)

// TenantAuth implements server.AuthenticationHandler against the
// per-tenant cleartext password stored in shim.yaml's mysql_password
// field. Both halves of the auth flow validate against this same
// value: ProxySQL's frontend (which derives mysql_pass_sha1 from it
// via bintrail proxysql-config) and the shim's backend (which hands
// the cleartext to go-mysql/server's challenge/response check via
// server.Credential.Passwords).
//
// Why the shim now stores cleartext: go-mysql/server's credential API
// requires the cleartext password to drive its challenge/response
// check (Credential.Passwords are raw and hashed on demand). Storing
// only mysql_pass_sha1 (as earlier versions did) made every ProxySQL →
// shim connection fail because the library could not validate
// ProxySQL-forwarded auth without the cleartext. shim.yaml is
// operator-owned and 0o600; it already holds source_dsn (which
// contains a MySQL password), so adding the tenant cleartext alongside
// is a no-op against the existing trust model.
//
// Validation is real (not "any password accepted"): the username must
// appear in the tenants block AND the client's wire-protocol response
// must scramble against the stored cleartext. ProxySQL is still the
// outer gate — the shim's role is to make sure direct connections to
// :3308 are not silently authenticated by virtue of the username
// alone.
type TenantAuth struct {
	users map[string]string // username → cleartext mysql_password
	// authPlugin is the MySQL auth plugin every returned Credential
	// advertises; "" means mysql_native_password. It must match the
	// method NewMySQLServer was built with: go-mysql compares the
	// client's response under the CREDENTIAL's plugin and auth-switches
	// the client to it when the negotiated plugin differs, so a
	// mismatch forces an auth switch on every handshake.
	authPlugin string
}

// NewTenantAuth builds a TenantAuth from a map of username →
// cleartext password. An empty map, or any tenant with an empty
// password, returns an error.
//
// Empty passwords are rejected at the type boundary (not just at the
// LoadTenants entry point) because GetCredential returns whatever is
// in the map: an empty string here would make the
// mysql_native_password handshake accept any client that also sent
// an empty password — recreating the exact #254 silent-auth bug for
// any future caller that bypasses LoadTenants. Pushing the invariant
// down into the type means tests and library callers cannot
// accidentally weaken the auth model.
func NewTenantAuth(users map[string]string) (TenantAuth, error) {
	if len(users) == 0 {
		return TenantAuth{}, errors.New("shim: no tenants in shim.yaml; cannot start with empty allowlist")
	}
	clean := make(map[string]string, len(users))
	for u, p := range users {
		if u == "" {
			continue
		}
		if p == "" {
			return TenantAuth{}, fmt.Errorf("shim: tenant %q has empty mysql_password; refusing to start with effective auth bypass", u)
		}
		clean[u] = p
	}
	if len(clean) == 0 {
		return TenantAuth{}, errors.New("shim: every tenant in shim.yaml has an empty mysql_user")
	}
	return TenantAuth{users: clean}, nil
}

// WithAuthMethod returns a copy whose credentials advertise the given
// MySQL auth plugin ("" keeps mysql_native_password). Pass the same
// value NewMySQLServer is built with — see the authPlugin field doc.
// Passwords is every tenant's password, in no order: what a SCRAM front end
// derives ahead of a login, so that no login derives one for its username
// alone (pgshim.WarmPasswords).
func (a TenantAuth) Passwords() []string {
	out := make([]string, 0, len(a.users))
	for _, p := range a.users {
		out = append(out, p)
	}
	return out
}

func (a TenantAuth) WithAuthMethod(method string) TenantAuth {
	a.authPlugin = method
	return a
}

func (a TenantAuth) plugin() string {
	if a.authPlugin == "" {
		return mysql.AUTH_NATIVE_PASSWORD
	}
	return a.authPlugin
}

// LookupPassword returns the tenant's stored cleartext password. The
// PostgreSQL shim validates its cleartext-password handshake against
// this directly (it speaks pgproto3, not the MySQL wire protocol, so
// go-mysql's Credential machinery does not apply there).
//
// Unknown usernames return server.ErrAccessDenied rather than
// (found=false, err=nil) — see GetCredential for why the explicit
// error is load-bearing on the MySQL side; the PG shim treats any
// error as a failed match, so sharing the semantics is harmless there.
func (a TenantAuth) LookupPassword(u string) (password string, found bool, err error) {
	p, ok := a.users[u]
	if !ok {
		return "", false, server.ErrAccessDenied
	}
	return p, true, nil
}

// GetCredential implements server.AuthenticationHandler. Returns the
// stored cleartext password (as Credential.Passwords) so go-mysql's
// handshake can validate the client's scrambled response against it.
//
// Unknown usernames return server.ErrAccessDenied rather than
// (found=false, err=nil). The library translates ErrAccessDenied into
// ER_ACCESS_DENIED_ERROR (1045) inside (*Conn).handshake; a plain
// found=false instead surfaces ER_NO_SUCH_USER (1449), which ProxySQL
// reads as "backend broken" and SHUNNs the hostgroup (issue #262).
// Returning the error explicitly routes us through the 1045 path so
// ProxySQL's monitor probes look like normal auth failures and the
// backend stays ONLINE.
func (a TenantAuth) GetCredential(u string) (server.Credential, bool, error) {
	p, found, err := a.LookupPassword(u)
	if err != nil || !found {
		return server.Credential{}, false, err
	}
	return server.Credential{Passwords: []string{p}, AuthPluginName: a.plugin()}, true, nil
}

// OnAuthSuccess and OnAuthFailure are server.AuthenticationHandler
// lifecycle hooks. The shim needs neither: post-auth work happens in
// the command Handler, and failure logging happens at the accept-loop
// call site (classifyHandshakeErr), which sees the translated 1045.
func (TenantAuth) OnAuthSuccess(*server.Conn) error  { return nil }
func (TenantAuth) OnAuthFailure(*server.Conn, error) {}

// TenantConfig is the validated form of one entry in shim.yaml's
// tenants block. Used by `bintrail shim` so callers can recover the
// per-tenant source_dsn (and from it, the source schema) in addition
// to the credentials needed for TenantAuth.
//
// Returned only by LoadTenantConfigs — instances obtained through
// that function carry the invariants below. Direct struct-literal
// construction (TenantConfig{...}) bypasses validation; consumers
// should treat that as a programmer error rather than a recoverable
// configuration shape.
type TenantConfig struct {
	// ServerID is the bintrail stream/agent server_id. May be empty
	// (the shim does not require it; only the streamer does).
	ServerID string
	// SourceDSN is the upstream MySQL DSN for the tenant's source
	// database. May be empty: when set with a /<db> path, `bintrail
	// shim` derives the schema from it and pre-seeds Handler.db so
	// fully qualified time-travel queries work without `USE <db>`
	// (issue #263). When empty or path-less, that tenant's clients
	// fall back to issuing `USE` themselves.
	SourceDSN string
	// MySQLUser is the cleartext username the application connects
	// with through ProxySQL. Guaranteed non-empty by LoadTenantConfigs.
	MySQLUser string
	// MySQLPassword is the cleartext password
	// `mysql_native_password` validates against. Guaranteed non-empty
	// by LoadTenantConfigs — empty here is the #254 silent-auth
	// regression and is rejected at the loader boundary.
	MySQLPassword string
	// AllowedSchemas is the opt-in per-tenant schema allowlist
	// (issue #824). Empty/nil means unrestricted — the pre-#824
	// behaviour, kept for single-tenant deployments and backwards
	// compatibility. Non-empty means the shim rejects `USE` of, and
	// any time-travel query resolving to, a schema outside this set
	// with ER_DBACCESS_DENIED_ERROR (1044). Entries are guaranteed
	// non-empty by LoadTenantConfigs.
	AllowedSchemas []string
}

// LoadTenants reads shim.yaml from path and returns username →
// cleartext password for each tenant. Thin wrapper over
// LoadTenantConfigs that exists so older callers (and the existing
// auth tests) keep working unchanged.
func LoadTenants(path string) (map[string]string, error) {
	cfgs, err := LoadTenantConfigs(path)
	if err != nil {
		return nil, err
	}
	users := make(map[string]string, len(cfgs))
	for _, t := range cfgs {
		users[t.MySQLUser] = t.MySQLPassword
	}
	return users, nil
}

// LoadTenantConfigs reads shim.yaml from path and returns the full
// validated tenants block. Used by `bintrail shim` so the per-tenant
// source DSN reaches the connection handler — without it, fully
// qualified time-travel queries like `SELECT * FROM _flashback.orders`
// would still need an explicit `USE <db>` first (issue #263).
//
// Strict YAML parsing rejects unknown fields, so legacy fields kept
// in the struct (agent_url, agent_token from < 0.7.0; mysql_pass_sha1
// from < 0.7.2) parse cleanly even though they are no longer
// load-bearing. Operators upgrading from 0.7.1 see a runtime warning
// pointing at mysql_password.
func LoadTenantConfigs(path string) ([]TenantConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	var cfg struct {
		Tenants []struct {
			ServerID       string   `yaml:"server_id"`
			SourceDSN      string   `yaml:"source_dsn"`
			AgentURL       string   `yaml:"agent_url"`
			AgentToken     string   `yaml:"agent_token"`
			MySQLUser      string   `yaml:"mysql_user"`
			MySQLPassword  string   `yaml:"mysql_password"`
			MySQLPassSHA1  string   `yaml:"mysql_pass_sha1"`
			AllowedSchemas []string `yaml:"allowed_schemas"`
		} `yaml:"tenants"`
		Listen string `yaml:"listen"`
	}
	if err := yaml.UnmarshalStrict(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	if len(cfg.Tenants) == 0 {
		return nil, fmt.Errorf("%s has no tenants", path)
	}
	out := make([]TenantConfig, 0, len(cfg.Tenants))
	for i, t := range cfg.Tenants {
		if t.MySQLUser == "" {
			return nil, fmt.Errorf("%s tenant #%d: mysql_user is empty (uncomment and fill in the TODO line)", path, i+1)
		}
		if t.MySQLPassword == "" {
			if t.MySQLPassSHA1 != "" {
				return nil, fmt.Errorf(
					"%s tenant #%d (mysql_user=%s): mysql_password is required; mysql_pass_sha1 alone is no longer accepted (>= 0.7.2). "+
						"Replace mysql_pass_sha1 with mysql_password: '<cleartext>' — the SHA1 is recomputed by `bintrail proxysql-config`",
					path, i+1, t.MySQLUser)
			}
			return nil, fmt.Errorf("%s tenant #%d (mysql_user=%s): mysql_password is empty (set the cleartext password your application uses to connect)", path, i+1, t.MySQLUser)
		}
		if t.MySQLPassSHA1 != "" {
			slog.Warn(
				"shim.yaml: mysql_pass_sha1 is no longer used by `bintrail shim` (the SHA1 is now recomputed by `bintrail proxysql-config` from mysql_password); the field is ignored",
				"tenant", i+1, "mysql_user", t.MySQLUser,
			)
		}
		for _, s := range t.AllowedSchemas {
			if s == "" {
				return nil, fmt.Errorf(
					"%s tenant #%d (mysql_user=%s): allowed_schemas contains an empty entry — remove it or list a schema name",
					path, i+1, t.MySQLUser)
			}
		}
		out = append(out, TenantConfig{
			ServerID:       t.ServerID,
			SourceDSN:      t.SourceDSN,
			MySQLUser:      t.MySQLUser,
			MySQLPassword:  t.MySQLPassword,
			AllowedSchemas: t.AllowedSchemas,
		})
	}
	return out, nil
}
