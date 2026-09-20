package consoleapp

import (
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/cli"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/telemetry"
)

var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Serve a read-only web UI over the index (browse events, generate undo SQL)",
	Long: `Starts a local, read-only, single-operator web console over the binlog index.

It is the MCP server with a web face: browse indexed row events with full
before/after diffs, and generate recovery (undo) SQL, all from a browser. The
console NEVER executes SQL; recover produces a script you review and apply
yourself.

Security:
  - Binds to loopback (127.0.0.1) by default. Username+password login is the
    primary credential: on a fresh loopback console the first visit creates
    the password in the browser (or set it up front with 'user set-password').
  - A non-loopback bind needs a credential: a configured password, an explicit
    --token (opt-in automation), or --allow-setup (assert the bind is
    access-controlled); otherwise it is refused.

Example:
  bintrail-console serve --index-dsn "user:pass@tcp(127.0.0.1:3306)/binlog_index"`,
	RunE: runServe,
}

var (
	conIndexDSN     string
	conListen       string
	conToken        string
	conNoArchive    bool
	conProfile      string
	conAllowedHosts []string
	conBaselineDir  string
	conBaselineS3   string
	conServersFile  string
	conAuthFile     string
	conMCPTokenFile string
	conTLSCert      string
	conTLSKey       string
	conAllowSetup   bool
)

func init() {
	serveCmd.Flags().StringVar(&conIndexDSN, "index-dsn", "", "DSN for the index MySQL database (required unless the server registry has entries)")
	serveCmd.Flags().StringVar(&conListen, "listen", "127.0.0.1:8090", "Address to listen on (host:port)")
	serveCmd.Flags().StringVar(&conToken, "token", "", "Opt-in static token for API automation (never generated; humans use the password)")
	serveCmd.Flags().BoolVar(&conNoArchive, "no-archive", false, "Disable Parquet archive auto-discovery (MySQL-only)")
	serveCmd.Flags().StringVar(&conProfile, "profile", "", "RBAC profile: deny tables / redact columns; forces --no-archive")
	serveCmd.Flags().StringSliceVar(&conAllowedHosts, "allowed-hosts", nil, "Extra hostnames allowed in the Host header (for reverse-proxy setups; IP literals and localhost are always allowed)")
	serveCmd.Flags().StringVar(&conBaselineDir, "baseline-dir", "", "Local directory of baseline Parquet snapshots; enables the point-in-time Reconstruct surface")
	serveCmd.Flags().StringVar(&conBaselineS3, "baseline-s3", "", "S3 prefix of baseline Parquet snapshots (s3://bucket/prefix/); enables Reconstruct")
	serveCmd.Flags().StringVar(&conServersFile, "servers-file", "", "Path to the server registry YAML managed by the UI (default ~/.config/bintrail/console-servers.yaml)")
	serveCmd.Flags().StringVar(&conAuthFile, "auth-file", "", "Path to the console auth file enabling password login (default ~/.config/bintrail/console-auth.yaml; created with `bintrail-console user set-password`)")
	serveCmd.Flags().StringVar(&conMCPTokenFile, "mcp-token-file", "", "Path to the managed MCP token file written by Settings → Connect AI (default ~/.config/bintrail/console-mcp-token.yaml). Point it at persistent storage when the daemon runs in a container.")
	serveCmd.Flags().StringVar(&conTLSCert, "tls-cert", "", "TLS certificate file (PEM); serve the web interface over HTTPS (requires --tls-key)")
	serveCmd.Flags().StringVar(&conTLSKey, "tls-key", "", "TLS private key file (PEM; requires --tls-cert)")
	serveCmd.Flags().BoolVar(&conAllowSetup, "allow-setup", false, "Allow browser first-run password setup on a non-loopback bind (assert the bind is access-controlled, e.g. published only on the host loopback)")
	rootCmd.AddCommand(serveCmd)
}

func runServe(cmd *cobra.Command, args []string) error {
	// Load .bintrail.env / config.env once, mirroring the core CLI. The core
	// bintrail binary does this in each command's init() via bindCommandEnv;
	// this standalone binary loads it here, then reads the relevant vars below
	// with flag > env > default precedence.
	cli.LoadEnvFile()
	// After the env file, not before: the retired variable is as likely to sit
	// in .bintrail.env as in the environment, and warning before the load would
	// stay silent for the operator whose copy is in the file.
	warnSQLPanelRetired()

	// index-dsn falls back to BINTRAIL_INDEX_DSN, the one shared binding the
	// console uses (core bintrail wires this via bindCommandEnv/cli.EnvBindings).
	if !cmd.Flags().Changed("index-dsn") {
		if v := os.Getenv("BINTRAIL_INDEX_DSN"); v != "" {
			conIndexDSN = v
		}
	}
	// Console-specific env vars, with flag > env > default precedence.
	if !cmd.Flags().Changed("listen") {
		if v := os.Getenv("BINTRAIL_CONSOLE_LISTEN"); v != "" {
			conListen = v
		}
	}
	if !cmd.Flags().Changed("token") {
		if v := os.Getenv("BINTRAIL_CONSOLE_TOKEN"); v != "" {
			conToken = v
		}
	}
	if !cmd.Flags().Changed("baseline-dir") {
		if v := os.Getenv("BINTRAIL_CONSOLE_BASELINE_DIR"); v != "" {
			conBaselineDir = v
		}
	}
	if !cmd.Flags().Changed("baseline-s3") {
		if v := os.Getenv("BINTRAIL_CONSOLE_BASELINE_S3"); v != "" {
			conBaselineS3 = v
		}
	}
	if !cmd.Flags().Changed("servers-file") {
		if v := os.Getenv("BINTRAIL_CONSOLE_SERVERS"); v != "" {
			conServersFile = v
		}
	}
	if !cmd.Flags().Changed("auth-file") {
		if v := os.Getenv("BINTRAIL_CONSOLE_AUTH"); v != "" {
			conAuthFile = v
		}
	}
	if !cmd.Flags().Changed("mcp-token-file") {
		if v := os.Getenv("BINTRAIL_CONSOLE_MCP_TOKEN_FILE"); v != "" {
			conMCPTokenFile = v
		}
	}
	if !cmd.Flags().Changed("tls-cert") {
		if v := os.Getenv("BINTRAIL_CONSOLE_TLS_CERT"); v != "" {
			conTLSCert = v
		}
	}
	if !cmd.Flags().Changed("tls-key") {
		if v := os.Getenv("BINTRAIL_CONSOLE_TLS_KEY"); v != "" {
			conTLSKey = v
		}
	}
	if !cmd.Flags().Changed("allowed-hosts") {
		if v := os.Getenv("BINTRAIL_CONSOLE_ALLOWED_HOSTS"); v != "" {
			conAllowedHosts = strings.Split(v, ",")
		}
	}
	if !cmd.Flags().Changed("allow-setup") {
		if v := os.Getenv("BINTRAIL_CONSOLE_ALLOW_SETUP"); v == "1" || v == "true" {
			conAllowSetup = true
		}
	}

	// The server registry: the named connections managed from the UI. A
	// corrupt file fails loud — silently starting without the operator's
	// saved servers would look like data loss.
	serversPath := conServersFile
	if serversPath == "" {
		serversPath = console.DefaultRegistryPath()
	}
	registry, err := loadConsoleRegistry(serversPath, conBaselineS3)
	if err != nil {
		return err
	}

	// Either a command-line DSN or at least one saved server must exist.
	if conIndexDSN == "" && registry.Len() == 0 {
		return fmt.Errorf("either --index-dsn (or BINTRAIL_INDEX_DSN) or at least one server in %s is required", serversPath)
	}
	// Profile rules are loaded from the command-line index; without one there
	// is no DB to read them from.
	if conProfile != "" && conIndexDSN == "" {
		return fmt.Errorf("--profile requires --index-dsn: the profile's rules are loaded from that index database")
	}
	// The baseline flags describe the command-line entry; without a DSN there
	// is no boot index to merge deltas from (baseline + deltas → state), and
	// seeding a DB-less boot entry would crash its first query. Registry
	// servers carry their own per-server baseline settings instead.
	if (conBaselineDir != "" || conBaselineS3 != "") && conIndexDSN == "" {
		return fmt.Errorf("--baseline-dir/--baseline-s3 require --index-dsn: reconstruct merges the baseline with binlog deltas from that index (registry servers configure their baseline per entry in the UI)")
	}

	// The command-line DSN becomes the ephemeral "default" entry: connected
	// eagerly (fail-fast preserved) and schema-migrated here, at startup, on
	// the one DSN the operator typed in their shell. Servers added in the UI
	// are NEVER migrated — request handlers stay free of DDL, so the recover
	// path remains provably read-only.
	var db *sql.DB
	var dbName string
	if conIndexDSN != "" {
		cfg, err := mysql.ParseDSN(conIndexDSN)
		if err != nil {
			return fmt.Errorf("invalid --index-dsn: %w", err)
		}
		dbName = cfg.DBName
		if dbName == "" {
			return fmt.Errorf("--index-dsn must include a database name (e.g. user:pass@tcp(host:3306)/binlog_index)")
		}

		db, err = config.Connect(conIndexDSN)
		if err != nil {
			return fmt.Errorf("failed to connect to index database: %w", err)
		}
		defer db.Close()

		if err := indexer.EnsureSchema(db); err != nil {
			return fmt.Errorf("schema migration: %w", err)
		}
	}

	// Resolve profile RBAC rules up front. Archives don't enforce RBAC, so a
	// profile forces --no-archive to avoid leaking redacted columns.
	var denyTables []query.SchemaTable
	var redactCols []query.SchemaTableColumn
	if conProfile != "" {
		// LoadProfileRules resolves a nonexistent profile to zero rules WITHOUT
		// an error, so a typo would start the console with RBAC that enforces
		// nothing while the operator believes a profile is active. Refuse loudly
		// on an unknown name before loading rules (#838).
		exists, perr := query.ProfileExists(cmd.Context(), db, conProfile)
		if perr != nil {
			return fmt.Errorf("check profile %q: %w", conProfile, perr)
		}
		if !exists {
			return fmt.Errorf("profile %q does not exist in the index; create it (bintrail flag/profile/access) or fix the typo; refusing to start with an RBAC profile that enforces nothing", conProfile)
		}
		denyTables, redactCols, err = query.LoadProfileRules(cmd.Context(), db, conProfile)
		if err != nil {
			return fmt.Errorf("load profile %q: %w", conProfile, err)
		}
	}

	srv, err := console.New(console.Config{
		DB:      db,
		DBName:  dbName,
		BootDSN: conIndexDSN,
		// The consent-only adapter (serveTelemetry): serve beacons (#1362), so
		// the UI opt-out toggle must stop THIS running process and the state
		// endpoint must report the live decision — while console actions stay
		// unrecorded, per TELEMETRY.md.
		Telemetry:     serveTelemetry{},
		Registry:      registry,
		Listen:        conListen,
		Token:         conToken,

		NoArchive:     conNoArchive || conProfile != "",
		DenyTables:    denyTables,
		RedactColumns: redactCols,
		// A named profile — even one resolving to zero rules — forces query_text
		// withholding on every query (#699/#838).
		ProfileActive: conProfile != "",
		AllowedHosts:  conAllowedHosts,
		BaselineDir:   conBaselineDir,
		BaselineS3:    conBaselineS3,
		AuthPath:      conAuthFile,
		MCPTokenPath:  conMCPTokenFile,
		TLSCert:       conTLSCert,
		TLSKey:        conTLSKey,
		AllowSetup:    conAllowSetup,
		Version:       appVersion,
		// MonitorCtrl is intentionally left nil: bintrail-console serve is the
		// read-only standalone console. A write-capable control-plane daemon
		// wires a supervisor here instead; with nil, /api/capabilities reports
		// monitor:false and every monitor verb refuses at the endpoint with 403.
	})
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Usage telemetry for a months-lived process (#1362): Init's drain runs
	// once, at startup, so without this loop a daemon's beacons would spool
	// and age out undelivered. Own goroutine — never on a request path. serve
	// is as long-lived as `watch`; a read-only console counts toward daily
	// active daemons like any other daemon, and the beacon carries nothing a
	// command event doesn't (no run_id, one per UTC day).
	go tel.Client().RunDaemon(ctx, cmd.Name())

	printConsoleBanner(srv, "The DBTrail web interface (read-only) is running. Open:")
	slog.Info("console listening", "addr", conListen, "no_archive", conNoArchive || conProfile != "")

	if err := srv.Run(ctx); err != nil {
		return fmt.Errorf("console server: %w", err)
	}
	return nil
}

// printConsoleBanner prints the startup URL plus a credential hint keyed to
// the console's mode. The URL never carries a ?token= unless an explicit token
// is the only credential (URL() handles that); a live credential does not
// belong in logs or shell history.

func printConsoleBanner(srv *console.Server, headline string) {
	fmt.Fprintf(os.Stderr, "\n%s\n\n    %s\n\n", headline, srv.URL())
	switch {
	case srv.NeedsSetup():
		// First run, loopback, no credential: the browser creates the password.
		fmt.Fprintf(os.Stderr, "First run: open the URL and create your username and password.\n\n")
	case srv.PasswordLogin():
		fmt.Fprintf(os.Stderr, "Sign in with your username and password.\n")
		if srv.Token() != "" {
			fmt.Fprintf(os.Stderr, "(The configured access token also remains valid, for API automation.)\n")
		}
		fmt.Fprintln(os.Stderr)
	}
}

// serveTelemetry adapts the process's live telemetry client for the read-only
// console. It delegates the CONSENT surface — Enabled, Decision,
// SetRuntimeConsent — so the UI opt-out toggle stops a running serve's daily
// beacon immediately (not just on the next start), and so the telemetry state
// endpoint reports the LIVE decision, including a --telemetry launch flag
// (which the POST handler's 409 override guard keys on). Without this, serve
// would keep beaconing after the operator opted out, while GET /api/telemetry
// claimed reporting was off.
//
// It deliberately does NOT delegate RecordDaemonCommand: TELEMETRY.md
// promises the read-only serve records no console-action events, so actions
// return a nil *Span (inert by contract). Wiring tel.Client() directly, the
// way `watch` does, would silently break that promise.
type serveTelemetry struct{}

func (serveTelemetry) Enabled() bool                              { return tel.Client().Enabled() }
func (serveTelemetry) Decision() telemetry.Decision               { return tel.Client().Decision() }
func (serveTelemetry) SetRuntimeConsent(enabled bool)             { tel.Client().SetRuntimeConsent(enabled) }
func (serveTelemetry) RecordDaemonCommand(string) *telemetry.Span { return nil }

// warnSQLPanelRetired reports BINTRAIL_CONSOLE_SQL_PANEL as accepted and
// ignored. The page and POST /api/sql were removed in #1549; the variable is
// kept readable for one release so an operator who set it learns that it no
// longer does anything, instead of the setting silently becoming a no-op.
//
// The warning fires for ANY non-empty value, including "0". Someone who typed
// 0 asked for the page to be hidden and got that outcome, so it is not a fault
// — but it is the same stale line in a compose file or a unit, and leaving it
// unmentioned is what makes it survive to the release that stops reading it.
func warnSQLPanelRetired() {
	if strings.TrimSpace(os.Getenv("BINTRAIL_CONSOLE_SQL_PANEL")) == "" {
		return
	}
	// Page-neutral wording on purpose: one function warns for BOTH binaries,
	// and the card lives on Backups under `watch` and on Connect only for the
	// serve-only fallback (#1581) — naming a single page here sends half the
	// operators to a page without the card.
	slog.Warn("BINTRAIL_CONSOLE_SQL_PANEL is set but no longer does anything: " +
		"the SQL page and POST /api/sql were removed. Download a DuckDB schema " +
		"from the Backups page (Connect, on a read-only daemon) and query " +
		"the same Parquet in your own DuckDB. " +
		"Remove the variable; a future release stops reading it")
}
