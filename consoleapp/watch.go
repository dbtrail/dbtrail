package consoleapp

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/cli"
	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/doctor"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/observe"
	"github.com/dbtrail/dbtrail/internal/rotation"
	"github.com/dbtrail/dbtrail/internal/serverid"
	"github.com/dbtrail/dbtrail/internal/streamdeps"
	"github.com/dbtrail/dbtrail/internal/streamrun"
)

var watchCmd = &cobra.Command{
	Use:   "watch",
	Short: "Watch one or more MySQL servers: stream + console + control plane in one daemon",
	Long: `Runs the combined capture-and-observe daemon (the standalone successor to
'bintrail up --console'): preflight checks, index initialization, the live
replication stream, AND the read-only web console with its control plane,
all in one process sharing one SIGINT/SIGTERM lifecycle.

--source-dsn is optional: without it the daemon starts source-less (the
zero-config install) serving the console + control plane only, and sources
are added from the UI ("+ Add server" runs the preflight, provisions a
per-source index database, and starts streaming).

If --server-id is not provided, a deterministic ID is derived from
host:user:dbname of the source DSN, mapped into a high range to reduce
collision odds with existing replicas.

Examples:

  bintrail-console watch --index-dsn "$IDX"
  bintrail-console watch --source-dsn "$SRC" --index-dsn "$IDX"
  bintrail-console watch --source-dsn "$SRC" --index-dsn "$IDX" --schemas mydb`,
	RunE: runWatch,
}

var (
	upSourceDSN             string
	upIndexDSN              string
	upServerID              uint32
	upSchemas               string
	upTables                string
	upBatchSize             int
	upCheckpoint            int
	upSSLMode               string
	upSSLCA                 string
	upSSLCert               string
	upSSLKey                string
	upMetricsAddr           string
	upMetricsScrapeInterval int
	upPartitions            int
	upSkipDoctor            bool
	upFormat                string

	upConsoleListen         string
	upConsoleToken          string
	upConsoleBaselineDir    string
	upConsoleBaselineS3     string
	upConsoleBaselineRetain string
	upBaselineRefreshEvery  string
	upBaselineCarryForward  bool
	upConsoleServersFile    string
	upConsoleAuthFile       string
	upConsoleMCPTokenFile   string
	upConsoleTLSCert        string
	upConsoleTLSKey         string
	upConsoleAllowedHost    []string
	upConsoleAllowSetup     bool
	// upConsoleFlashbackListen opts into the embedded MySQL-protocol time-travel
	// port (#996): _flashback/_snapshot/_diff for every monitored server, routed
	// by the connection username, with no separate `bintrail shim` container.
	// Empty (default) = off. Requires a console token (MySQL-protocol auth can't
	// use the bcrypt password store). Env BINTRAIL_CONSOLE_FLASHBACK_LISTEN.
	upConsoleFlashbackListen string
	upArchiveStageDir        string
	// upConsoleBaselineTrigger opts into in-process baseline creation from the
	// console (#613). Env-only (BINTRAIL_CONSOLE_BASELINE_TRIGGER=1) — off by
	// default because it needs mydumper in the image and reaches the source DB.
	upConsoleBaselineTrigger bool
	// upBaselineStageDir is the local staging base for S3-destined baselines
	// (BINTRAIL_CONSOLE_BASELINE_STAGING); default a temp subdir.
	upBaselineStageDir string
	// upConsoleBaselineLockMode selects how a console-triggered MySQL/MariaDB
	// baseline synchronizes mydumper's threads onto one instant (#800, #1377).
	// Defaults to baseline.DefaultLockMode (FTWRL, point-consistent): a
	// baseline is the seed state reconstruct merges deltas onto, so a snapshot
	// that can be torn is not a backup, and that must not be something an
	// operator has to opt IN to. Env-only
	// (BINTRAIL_CONSOLE_BASELINE_LOCK_MODE); has no effect unless
	// baseline-trigger is also on.
	upConsoleBaselineLockMode = baseline.DefaultLockMode
	// upConsoleBaselineLockModeErr holds an invalid BINTRAIL_CONSOLE_BASELINE_LOCK_MODE
	// so the baseline supervisor can refuse with it. Startup is NOT failed:
	// see the parse site in resolveUpConsoleEnv.
	upConsoleBaselineLockModeErr error
	// upConsoleVerifyTrigger opts into in-process bintrail verify runs from the
	// console (#677). Env-only (BINTRAIL_CONSOLE_VERIFY_TRIGGER=1) — off by
	// default for a bare `watch` invocation; unlike baseline-trigger it starts
	// no subprocess and reads no live source in its default (baseline-anchored)
	// mode, so the bundled compose stack defaults it ON (see docker-compose.yml).
	upConsoleVerifyTrigger bool
	// upVerifyInterval enables the scheduled verification loop (#1191): every
	// interval, each registry server is verified in-process — baseline-anchored
	// when a baseline location is configured, the index-only recover-inputs
	// check otherwise — and the outcome lands in the persisted history.
	// Empty = off; setting it implies the verify supervisor (no separate
	// BINTRAIL_CONSOLE_VERIFY_TRIGGER needed).
	upVerifyInterval string
	// upVerifyTables optionally narrows scheduled verification to a
	// comma-separated schema.table list.
	upVerifyTables string
	// upNotifyWebhook (#1192) enables the outbound notification channel: a
	// generic JSON POST to this URL on continuity gap_lost, verify problems,
	// and rotation making no progress — edge-triggered with recovery events.
	// Empty = off.
	upNotifyWebhook string

	upRotateRetain    string
	upRotateInterval  string
	upRotateAddFuture int

	// upRotationCfg holds the parsed built-in-rotation settings from runWatch's
	// validation, read at the phase-3 start sites (the cobra-accumulator
	// pattern; the parsing and the loop itself live in internal/rotation).
	upRotationCfg rotation.Settings
)

// watchEnvBindings maps watch's flags to their BINTRAIL_ environment
// variables — the subset of the core CLI's cli.EnvBindings (internal/cli/
// env.go) that exists on this command. Applied by bindWatchEnv with the
// same semantics: an env-set flag is marked Changed, which both satisfies
// MarkFlagRequired and keeps rotation.ParseSettings' explicit-retention
// detection working for env-configured daemons (the compose path).
var watchEnvBindings = []struct {
	Flag   string
	EnvVar string
}{
	{"index-dsn", "BINTRAIL_INDEX_DSN"},
	{"source-dsn", "BINTRAIL_SOURCE_DSN"},
	{"schemas", "BINTRAIL_SCHEMAS"},
	{"tables", "BINTRAIL_TABLES"},
	{"server-id", "BINTRAIL_SERVER_ID"},
	{"batch-size", "BINTRAIL_BATCH_SIZE"},
	{"ssl-mode", "BINTRAIL_SSL_MODE"},
	{"ssl-ca", "BINTRAIL_SSL_CA"},
	{"ssl-cert", "BINTRAIL_SSL_CERT"},
	{"ssl-key", "BINTRAIL_SSL_KEY"},
	{"metrics-addr", "BINTRAIL_METRICS_ADDR"},
	{"metrics-scrape-interval", "BINTRAIL_METRICS_SCRAPE_INTERVAL"},
	{"rotate-retain", "BINTRAIL_ROTATE_RETAIN"},
	{"rotate-interval", "BINTRAIL_ROTATE_INTERVAL"},
	{"rotate-add-future", "BINTRAIL_ROTATE_ADD_FUTURE"},
}

// bindWatchEnv loads the env file (once) and applies BINTRAIL_* environment
// variables to watch's flags, mirroring the core CLI's bindCommandEnv. The
// console-specific BINTRAIL_CONSOLE_* vars are NOT bound here — they are read
// directly in resolveUpConsoleEnv, because --baseline-dir/--baseline-s3 also
// exist on core commands and the direct read keeps the precedence dance in
// one auditable place.
func bindWatchEnv(cmd *cobra.Command) {
	cli.LoadEnvFile()
	for _, b := range watchEnvBindings {
		v := os.Getenv(b.EnvVar)
		if v == "" {
			continue
		}
		if cmd.Flags().Lookup(b.Flag) == nil {
			continue
		}
		if err := cmd.Flags().Set(b.Flag, v); err != nil {
			fmt.Fprintf(os.Stderr, "warning: cannot set --%s from %s: %v\n", b.Flag, b.EnvVar, err)
		}
	}
}

func init() {
	watchCmd.Flags().StringVar(&upSourceDSN, "source-dsn", "", "DSN for the source MySQL server (omit to start source-less and add servers from the UI)")
	watchCmd.Flags().StringVar(&upIndexDSN, "index-dsn", "", "DSN for the index MySQL database (required)")
	watchCmd.Flags().Uint32Var(&upServerID, "server-id", 0, "MySQL replica server ID (default: hash of source host:user:dbname)")
	watchCmd.Flags().StringVar(&upSchemas, "schemas", "", "Comma-separated schemas to index (default: all user schemas)")
	watchCmd.Flags().StringVar(&upTables, "tables", "", "Comma-separated tables to index (default: all)")
	watchCmd.Flags().IntVar(&upBatchSize, "batch-size", 1000, "Events per batch INSERT")
	watchCmd.Flags().DurationVar(&indexer.WriteTimeout, "write-timeout", indexer.DefaultWriteTimeout, "Deadline for each index write (batch INSERT, checkpoint, digest lookup). A mid-statement network stall surfaces as an error within this window instead of freezing the daemon on kernel TCP retransmission (~13-16 min). Raise for very large batches over a slow link")
	watchCmd.Flags().IntVar(&upCheckpoint, "checkpoint", 10, "Checkpoint interval in seconds")
	watchCmd.Flags().StringVar(&upSSLMode, "ssl-mode", "preferred", "TLS mode for the source AND index connections: disabled, preferred, required, verify-ca, verify-identity")
	watchCmd.Flags().StringVar(&upSSLCA, "ssl-ca", "", "Path to CA certificate file for source TLS verification (omit to use system CAs)")
	watchCmd.Flags().StringVar(&upSSLCert, "ssl-cert", "", "Path to client certificate file for mutual TLS to the source")
	watchCmd.Flags().StringVar(&upSSLKey, "ssl-key", "", "Path to client private key file for mutual TLS to the source")
	watchCmd.Flags().StringVar(&upMetricsAddr, "metrics-addr", "", "Address to expose Prometheus metrics (e.g. :9090); empty = disabled")
	watchCmd.Flags().IntVar(&upMetricsScrapeInterval, "metrics-scrape-interval", 60, "How often (seconds) to refresh the bintrail_index_* gauges from a status snapshot")
	watchCmd.Flags().IntVar(&upPartitions, "partitions", 48, "Hourly partitions to create on first init")
	watchCmd.Flags().BoolVar(&upSkipDoctor, "skip-doctor", false, "Skip the preflight checks (useful when you've already verified with `bintrail doctor`)")
	watchCmd.Flags().StringVar(&upFormat, "format", "text", "Output format: text or json")
	watchCmd.Flags().StringVar(&upConsoleListen, "console-listen", "127.0.0.1:8090", "Console bind address")
	watchCmd.Flags().StringVar(&upConsoleToken, "console-token", "", "Opt-in static token for API automation (never generated; humans use the console password)")
	watchCmd.Flags().StringVar(&upConsoleBaselineDir, "baseline-dir", "", "Local directory of baseline Parquet snapshots; enables the console's point-in-time Reconstruct surface")
	watchCmd.Flags().StringVar(&upConsoleBaselineS3, "baseline-s3", "", "S3 prefix of baseline Parquet snapshots (s3://bucket/prefix/); enables Reconstruct")
	watchCmd.Flags().BoolVar(&upBaselineCarryForward, "baseline-carry-forward-unchanged", false,
		"When a refresh finds a table had no changes, publish its previous Parquet file instead of rewriting "+
			"it (hard link where possible). Off by default: the rows are identical either way, but it links two "+
			"snapshots to one file, so disk-usage and prune figures then count space they will not reclaim. "+
			"Editable from the console settings panel, which overrides this flag.")
	watchCmd.Flags().StringVar(&upBaselineRefreshEvery, "baseline-refresh-interval", "", "Periodically refresh each server's newest baseline snapshot from the index (Nm/Nh/Nd; default: off). Runs with the conservative DuckDB budget, folds at most 2 tables at a time, and never publishes over a known capture gap.")
	watchCmd.Flags().StringVar(&upConsoleBaselineRetain, "baseline-retain", "", "Periodically prune local --baseline-dir snapshots older than this (Nd/Nh) once a durable copy exists in --baseline-s3 (never deletes the only copy or the newest snapshot per table)")
	watchCmd.Flags().StringVar(&upConsoleServersFile, "console-servers-file", "", "Path to the console server registry YAML (default ~/.config/bintrail/console-servers.yaml)")
	watchCmd.Flags().StringVar(&upConsoleAuthFile, "console-auth-file", "", "Path to the console auth file enabling password login (default ~/.config/bintrail/console-auth.yaml; created with `bintrail-console user set-password`)")
	watchCmd.Flags().StringVar(&upConsoleMCPTokenFile, "console-mcp-token-file", "", "Path to the managed MCP token file written by Settings → Connect AI (default ~/.config/bintrail/console-mcp-token.yaml). Point it at persistent storage when the console runs in a container.")
	watchCmd.Flags().StringVar(&upConsoleTLSCert, "console-tls-cert", "", "TLS certificate file (PEM); serve the console over HTTPS (requires --console-tls-key)")
	watchCmd.Flags().StringVar(&upConsoleTLSKey, "console-tls-key", "", "TLS private key file (PEM; requires --console-tls-cert)")
	watchCmd.Flags().StringSliceVar(&upConsoleAllowedHost, "console-allowed-hosts", nil, "Extra hostnames allowed in the Host header (for a TLS-terminating reverse proxy); IP literals and localhost are always allowed")
	watchCmd.Flags().BoolVar(&upConsoleAllowSetup, "console-allow-setup", false, "Allow browser first-run password setup on a non-loopback bind (assert the bind is access-controlled, e.g. published only on the host loopback)")
	watchCmd.Flags().StringVar(&upConsoleFlashbackListen, "flashback-listen", "", "Serve an embedded MySQL-protocol time-travel port (_flashback/_snapshot/_diff) for every monitored server, routed by the connection username (server id or name); e.g. 127.0.0.1:3308. Requires --console-token. Empty = off. Env BINTRAIL_CONSOLE_FLASHBACK_LISTEN.")
	watchCmd.Flags().StringVar(&upArchiveStageDir, "archive-staging-dir", "", "Local staging directory for S3 archive uploads (default: OS temp dir). Rotated Parquet is written here, uploaded to a source's configured Archive S3 bucket, then pruned.")
	watchCmd.Flags().StringVar(&upRotateRetain, "rotate-retain", "30d", "Built-in rotation: drop index partitions older than this (Nd/Nh; \"off\" disables)")
	watchCmd.Flags().StringVar(&upRotateInterval, "rotate-interval", "1h", "Built-in rotation: how often to run a rotation cycle")
	watchCmd.Flags().StringVar(&upVerifyInterval, "verify-interval", "", "Scheduled verification: how often to verify every registry server (e.g. 24h, 7d); empty disables")
	watchCmd.Flags().StringVar(&upVerifyTables, "verify-tables", "", "Scheduled verification: comma-separated schema.table filter (default: all tables)")
	watchCmd.Flags().StringVar(&upNotifyWebhook, "notify-webhook", "", "Webhook URL for JSON notifications on lost capture continuity, verify problems, and unhealthy rotation; empty disables")
	watchCmd.Flags().IntVar(&upRotateAddFuture, "rotate-add-future", 3, "Built-in rotation: keep at least N future hourly partitions ready")
	// --source-dsn is deliberately NOT required: the daemon may start
	// source-less (zero-config install) and sources are added from the UI.
	_ = watchCmd.MarkFlagRequired("index-dsn")
	bindWatchEnv(watchCmd)
	rootCmd.AddCommand(watchCmd)
}

func runWatch(cmd *cobra.Command, args []string) error {
	// bindWatchEnv already loaded the env file in init(), so the variable is
	// visible here whichever way it was set. Here rather than in init() so the
	// binary warns once, for the command actually being run.
	warnSQLPanelRetired()
	if !cliutil.IsValidOutputFormat(upFormat) {
		return fmt.Errorf("invalid --format %q; must be text or json", upFormat)
	}
	// Validate the built-in rotation settings up front so a typo fails fast,
	// before any phase runs. The loop itself starts with phase 3. The Changed
	// check covers flag and env alike (bindWatchEnv marks env-set flags
	// Changed); an explicitly-chosen retention disables the upgrade guard.
	var err error
	upRotationCfg, err = rotation.ParseSettings(upRotateRetain, upRotateInterval, upRotateAddFuture,
		cmd.Flags().Changed("rotate-retain"))
	if err != nil {
		return err
	}

	// Containerized installs start the daemon and the index MySQL together,
	// and the official mysql image briefly accepts-then-drops connections
	// during its first initialization — a single connect attempt turns first
	// boot into a restart loop (regenerating the console token each time).
	// Wait for the index instead of dying.
	if err := waitForIndexMySQL(cmd.Context(), upIndexDSN, 90*time.Second); err != nil {
		return fmt.Errorf("index MySQL did not become reachable: %w", err)
	}

	// ── Phase 1: Preflight ──────────────────────────────────────────────────
	if upSourceDSN == "" {
		fmt.Fprintln(os.Stderr, "=== Phase 1/3: Preflight checks ===")
		fmt.Fprintln(os.Stderr, "No source configured yet; the preflight runs when you add a server from the console.")
		fmt.Fprintln(os.Stderr)
	} else if !upSkipDoctor {
		fmt.Fprintln(os.Stderr, "=== Phase 1/3: Preflight checks ===")
		// The capacity projection uses the daemon's actual rotation window (0
		// when built-in rotation is disabled → it reports unbounded growth).
		// Its FAIL is ADVISORY here: blocking the stream over a disk forecast
		// would manufacture the very forensic gap it warns about (an
		// unattended reboot would crash-loop instead of capturing while
		// there is still room). Standalone `bintrail doctor` keeps full FAIL
		// semantics for CI.
		preflight := doctor.Build(cmd.Context(), upSourceDSN, upIndexDSN, upSchemas, upRotationCfg.Retain)
		if err := preflight.Write(os.Stderr, "text"); err != nil {
			return fmt.Errorf("write preflight report: %w", err)
		}
		fatal, warnCapacity := upPreflightOutcome(preflight)
		if fatal != nil {
			return doctor.BootRefusal(fatal)
		}
		if warnCapacity {
			fmt.Fprintln(os.Stderr, "WARNING: the index disk capacity check FAILED: starting anyway (capturing beats not capturing), but act on its remediation before the volume fills.")
		}
		fmt.Fprintln(os.Stderr)
	}

	// ── Phase 2: Init ───────────────────────────────────────────────────────
	fmt.Fprintln(os.Stderr, "=== Phase 2/3: Initializing index database ===")
	if err := watchInit(cmd.Context()); err != nil {
		return fmt.Errorf("init failed: %w", err)
	}
	fmt.Fprintln(os.Stderr)

	// ── Phase 3: Stream + console (or console-only daemon when no source) ───
	if upSourceDSN == "" {
		fmt.Fprintln(os.Stderr, "=== Phase 3/3: Console + control plane ===")
		return runUpConsoleOnly(cmd)
	}
	fmt.Fprintln(os.Stderr, "=== Phase 3/3: Streaming ===")
	return runUpStreamWithConsole(cmd, args)
}

// upPreflightOutcome maps the preflight report to the daemon's boot decision:
// fatal is non-nil for any non-advisory failure (boot refused); warnCapacity
// is true when the capacity projection was the ONLY failure — boot proceeds,
// but the operator must hear about it (the caller prints the WARNING).
// Duplicated from cmd/bintrail/up.go (6 lines, the PR-C replication
// precedent): the advisory semantics are up-policy shared by both daemons.
func upPreflightOutcome(r *doctor.Report) (fatal error, warnCapacity bool) {
	if err := r.ErrExcluding(doctor.CapacityCheckName); err != nil {
		return err, false
	}
	return nil, r.Err() != nil
}

// watchInit provisions the boot index database directly via the indexer's
// provisioning API (the same triple the control-plane supervisor runs for
// per-source databases): CREATE DATABASE, the index table set, and the
// schema migration for a pre-existing legacy index. Idempotent — re-running
// `watch` skips work that's already done.
func watchInit(ctx context.Context) error {
	cfg, err := mysql.ParseDSN(upIndexDSN)
	if err != nil {
		return fmt.Errorf("invalid --index-dsn: %w", err)
	}
	if cfg.DBName == "" {
		return fmt.Errorf("--index-dsn must include a database name (e.g. user:pass@tcp(host:3306)/binlog_index)")
	}
	if err := indexer.EnsureDatabase(cfg, cfg.DBName, func(s string) { fmt.Fprintln(os.Stderr, s) }); err != nil {
		return err
	}
	db, err := config.Connect(upIndexDSN)
	if err != nil {
		return fmt.Errorf("connect index database: %w", err)
	}
	defer db.Close()
	if err := indexer.CreateIndexTables(ctx, db, upPartitions, false, func(name string) {
		fmt.Fprintf(os.Stderr, "  ✓ %s\n", name)
	}); err != nil {
		return err
	}
	return indexer.EnsureSchema(db)
}

// waitForIndexMySQL retries a server-level connection (database name
// stripped — init may not have created it yet) until the index MySQL accepts
// connections or the timeout elapses. Progress is logged so a compose
// first-boot reads as "waiting for MySQL", not as a crash loop.
func waitForIndexMySQL(ctx context.Context, dsn string, timeout time.Duration) error {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return fmt.Errorf("invalid --index-dsn: %w", err)
	}
	cfg.DBName = ""
	serverDSN := cfg.FormatDSN()

	deadline := time.Now().Add(timeout)
	var lastErr error
	for attempt := 0; ; attempt++ {
		db, err := config.Connect(serverDSN)
		if err == nil {
			db.Close()
			return nil
		}
		lastErr = err
		if time.Now().After(deadline) {
			return lastErr
		}
		if attempt%5 == 0 {
			fmt.Fprintf(os.Stderr, "Waiting for index MySQL at %s…\n", cfg.Addr)
		}
		select {
		case <-time.After(2 * time.Second):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// runUpConsoleOnly is the zero-config daemon: no initial source, just the
// index, the console, and the control-plane supervisor. Every source is added
// (and resumed at boot, via Reconcile) from the UI. Mirrors
// runUpStreamWithConsole minus the main stream.
func runUpConsoleOnly(cmd *cobra.Command) error {
	if err := resolveUpConsoleEnv(cmd); err != nil {
		return err
	}

	db, err := config.Connect(upIndexDSN)
	if err != nil {
		return fmt.Errorf("console: connect index database: %w", err)
	}
	defer db.Close()
	if err := indexer.EnsureSchema(db); err != nil {
		return fmt.Errorf("console: schema migration: %w", err)
	}

	serversPath := upConsoleServersFile
	if serversPath == "" {
		serversPath = console.DefaultRegistryPath()
	}
	registry, err := console.LoadRegistry(serversPath)
	if err != nil {
		return fmt.Errorf("console: %w", err)
	}

	cfg, err := upConsoleConfig(db, upIndexDSN, upConsoleOpts())
	if err != nil {
		return err
	}
	cfg.Registry = registry
	// Source-less daemon: nothing ever streams into the boot index (each
	// "+ Add server" source gets its own per-source database), so hide it
	// from the UI entirely — a fresh install must list no servers. The
	// console serves header-less requests from it underneath until the
	// first server is added.
	cfg.HideBoot = true

	ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Usage telemetry for a months-lived process: Init's drain runs once, so
	// without this loop a daemon's beacons would spool and age out undelivered.
	go tel.Client().RunDaemon(ctx, cmd.Name())

	supervisor := newMonitorSupervisor(ctx, upIndexDSN, registry, upRotationCfg.Retain)
	cfg.MonitorCtrl = supervisor
	// Refreshing a source's schema snapshot needs no opt-in of its own (#1296):
	// unlike the baseline trigger it starts no dump and copies no row data — it
	// re-reads information_schema and restarts that server's stream onto the
	// result. It is wired wherever the control plane is, because the remedy for
	// a degraded capture has to be reachable from the console that reports it.
	cfg.SchemaSnapshotCtrl = newSchemaSnapshotSupervisor(ctx, reloadStreamSchema(supervisor, registry))
	// One supervisor, TWO independently opt-in features: the manual dump-based
	// baseline trigger (#613, needs mydumper and BINTRAIL_CONSOLE_BASELINE_TRIGGER=1)
	// and the periodic refresh (#1171, needs neither — it exists precisely so a
	// fresher baseline does not require a dump). Build it when EITHER is asked
	// for, but wire the two Config fields separately: assigning cfg.BaselineCtrl
	// un-gates the Create-baseline button, so deriving one from the other would
	// either refuse to start a refresh-only daemon or silently switch on a
	// feature the operator did not enable.
	// The sweep runs regardless of the supervisor decision below: with both
	// baseline features off, no supervisor would ever remove a previous
	// process's staged dump.
	sweepSQLExportStaging(baselineStagingDir())
	var baselineSup *baselineSupervisor
	if upConsoleBaselineTrigger || upBaselineRefreshEvery != "" {
		baselineSup = newBaselineSupervisorFromConfig(ctx, baselineStagingDir())
	}
	if upConsoleBaselineTrigger {
		cfg.BaselineCtrl = baselineSup
	}
	if upBaselineRefreshEvery != "" {
		cfg.BaselineRefresh = baselineSup
		// Live target count for GET /api/baseline-refresh (#1579): the loop
		// recomputes its set per tick, so the page must too, or a fresh
		// install reports everything-running over zero refreshable servers.
		cfg.BaselineRefreshTargets = func() (int, int) {
			reqs, skipped := baselineRefreshTargets(registryEntries(registry), upIndexDSN, upConsoleBaselineDir)
			return len(reqs), len(skipped)
		}
	}
	wireBaselineExtras(&cfg, baselineSup, serversPath)
	// The per-server backup schedule (#1442) needs only the supervisor: the
	// chooser checks the creation opt-in per slot whenever a full backup is
	// the producer it picks.
	// The helper returns a true nil interface for a nil supervisor; assigning
	// a nil *backupScheduler here directly would advertise a loop that does
	// not exist.
	var backupSched *backupScheduler
	cfg.BackupSchedules, backupSched = newBackupScheduleReporter(baselineSup, registry, upConsoleBaselineTrigger, upBaselineCarryForward)
	notifier, err := newWatchNotifierFromFlags(ctx)
	if err != nil {
		return err
	}
	if err := wireVerify(ctx, &cfg, registry, serversPath, notifier); err != nil {
		return err
	}
	// The continuity watch serves two channels: webhook events (notifier) and
	// the Prometheus gauge (#1203). Either one being enabled starts it; with
	// neither, nothing runs.
	if notifier != nil || upMetricsAddr != "" {
		startContinuityWatch(ctx, notifier, registry, upIndexDSN)
	}
	// Baseline staleness (#1193) is webhook-only: status/console carry the
	// full verdict; the channel gets the broken transition.
	if notifier != nil {
		startStalenessWatch(ctx, notifier, registry, upIndexDSN, upConsoleBaselineDir, upConsoleBaselineS3)
	}

	// Built-in rotation covers the boot index plus every per-source database
	// the control plane provisions — the unattended quickstart's real data
	// lives in the latter. Settings are a live provider so the console can
	// retune retain/interval/add-future without a restart.
	rotation.StartLoop(ctx, rotationSettingsProvider(registry), func() []rotation.RotateTarget {
		return rotateTargets(upIndexDSN, supervisor, registry, archiveStagingDir())
	}, rotationCycleHooks(notifier)...)

	// Reclaim local baseline snapshots that already have a durable S3 copy (#616):
	// the global --baseline-dir plus every registry server's per-server dir.
	if err := startBaselinePruneLoop(ctx, registry, upConsoleBaselineDir, upConsoleBaselineS3, upConsoleBaselineRetain, upRotationCfg.Interval); err != nil {
		return err
	}

	// Keep each server's newest snapshot moving forward from the index alone
	// (#1171). Opt-in, and refused at startup when nothing can be refreshed.
	if err := startBaselineRefreshLoop(ctx, registry, baselineSup, upIndexDSN, upConsoleBaselineDir, upBaselineRefreshEvery, upBaselineCarryForward); err != nil {
		return err
	}
	// Per-server backup schedules from the Backups page (#1442). No flag:
	// the registry decides what runs, the loop only looks at the clock.
	startBackupScheduleLoop(ctx, backupSched)

	// Wire the live telemetry client so the console's opt-out toggle stops this
	// running daemon's beacons immediately, not just on the next start.
	cfg.Telemetry = tel.Client()
	srv, err := console.New(cfg)
	if err != nil {
		return err
	}
	ln, err := srv.Listen()
	if err != nil {
		return fmt.Errorf("console: cannot bind %s: %w", upConsoleListen, err)
	}

	// One daemon-level /metrics endpoint for ALL supervised streams — the
	// Prometheus registry is process-global and every stream metric carries
	// a "source" label (the entry ID), so per-stream servers are unnecessary
	// (and would fight over the bind). Synchronous bind: fails fast, like
	// the console bind.
	if upMetricsAddr != "" {
		stopMetrics, err := streamrun.StartMetricsServer(upMetricsAddr)
		if err != nil {
			return err
		}
		defer stopMetrics()
	}

	stopFlashback, err := startFlashbackPort(ctx, srv)
	if err != nil {
		return err
	}
	printConsoleBanner(srv, "Console is running: open it and add the MySQL servers to watch:")
	go supervisor.Reconcile(registry)

	serveErr := srv.Serve(ctx, ln)
	stop()                // cancel ctx so the flashback listener unblocks even if Serve returned a non-signal error
	stopFlashback()       // drain the flashback port before the deferred db.Close
	supervisor.Shutdown() // final checkpoints for every monitored stream
	return serveErr
}

// startFlashbackPort binds and serves the embedded MySQL-protocol time-travel
// port (#996) when --flashback-listen is set, routing each connection to a
// monitored server by its username. It returns a drain func (a no-op when the
// port is disabled) the caller must invoke before closing the shared index DB,
// so an in-flight time-travel query never races the deferred db.Close.
//
// The bind is synchronous so a port conflict or a missing token fails `watch`
// fast, exactly like the console bind. Serving runs on the daemon context: ctx
// cancellation closes the listener and drains open connections. A mid-run crash
// is logged, never propagated — the flashback port is strictly secondary to the
// console and the capture stream.
func startFlashbackPort(ctx context.Context, srv *console.Server) (func(), error) {
	if upConsoleFlashbackListen == "" {
		return func() {}, nil
	}
	if srv.Token() == "" {
		return nil, fmt.Errorf("--flashback-listen %s requires a console token: set --console-token or BINTRAIL_CONSOLE_TOKEN (MySQL-protocol auth cannot use the console password)", upConsoleFlashbackListen)
	}
	ln, err := net.Listen("tcp", upConsoleFlashbackListen)
	if err != nil {
		return nil, fmt.Errorf("flashback: cannot bind %s: %w", upConsoleFlashbackListen, err)
	}
	done := make(chan struct{})
	go func() {
		if err := serveFlashback(ctx, srv, ln, flashbackConfig{}); err != nil {
			slog.Warn("flashback port exited with error", "error", err)
		}
		close(done)
	}()
	fmt.Fprintf(os.Stderr, "Time-travel SQL (MySQL protocol) is listening on %s; connect a MySQL client with user=<server id or name>, password=<console token>.\n", ln.Addr())
	return func() { <-done }, nil
}

// runUpStreamWithConsole serves the read-only web console in this same process,
// alongside the live stream. Both share one SIGINT/SIGTERM lifecycle: the signal
// cancels the context the console runs on, and the main stream runs on the same
// context, so a single Ctrl-C drains both. The console gets its own index DB
// connection (the stream owns its own).
func runUpStreamWithConsole(cmd *cobra.Command, args []string) error {
	serverID := upServerID
	if serverID == 0 {
		id, err := serverid.DeriveServerID(upSourceDSN)
		if err != nil {
			return fmt.Errorf("cannot auto-derive --server-id from --source-dsn: %w (pass --server-id explicitly to bypass)", err)
		}
		serverID = id
		fmt.Fprintf(os.Stderr, "Auto-derived server-id from source DSN: %d\n", serverID)
	}

	if err := resolveUpConsoleEnv(cmd); err != nil {
		return err
	}

	db, err := config.Connect(upIndexDSN)
	if err != nil {
		return fmt.Errorf("console: connect index database: %w", err)
	}
	defer db.Close()

	// Bring the index schema up to date before serving — streamrun.One also
	// does this, but it runs after the console goroutine starts, so a legacy
	// index DB missing newer columns (e.g. connection_id) could fail early
	// /api/events requests in the startup window.
	if err := indexer.EnsureSchema(db); err != nil {
		return fmt.Errorf("console: schema migration: %w", err)
	}

	// The server registry gives `watch` the same UI-managed switcher as the
	// standalone console; the stream's own index is the ephemeral default.
	// A corrupt file fails loud — silently starting without the operator's
	// saved servers would look like data loss.
	serversPath := upConsoleServersFile
	if serversPath == "" {
		serversPath = console.DefaultRegistryPath()
	}
	registry, err := console.LoadRegistry(serversPath)
	if err != nil {
		return fmt.Errorf("console: %w", err)
	}

	cfg, err := upConsoleConfig(db, upIndexDSN, upConsoleOpts())
	if err != nil {
		return err
	}
	cfg.Registry = registry

	ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// See runUpConsoleOnly: the daemon telemetry loop, off the stream path.
	go tel.Client().RunDaemon(ctx, cmd.Name())

	// The control-plane supervisor: "+ Add server" in the console starts real
	// monitoring through it. Streams live on the daemon context (ctx), not on
	// the HTTP requests that start them.
	supervisor := newMonitorSupervisor(ctx, upIndexDSN, registry, upRotationCfg.Retain)
	cfg.MonitorCtrl = supervisor
	// Refreshing a source's schema snapshot needs no opt-in of its own (#1296):
	// unlike the baseline trigger it starts no dump and copies no row data — it
	// re-reads information_schema and restarts that server's stream onto the
	// result. It is wired wherever the control plane is, because the remedy for
	// a degraded capture has to be reachable from the console that reports it.
	cfg.SchemaSnapshotCtrl = newSchemaSnapshotSupervisor(ctx, reloadStreamSchema(supervisor, registry))
	// One supervisor, TWO independently opt-in features: the manual dump-based
	// baseline trigger (#613, needs mydumper and BINTRAIL_CONSOLE_BASELINE_TRIGGER=1)
	// and the periodic refresh (#1171, needs neither — it exists precisely so a
	// fresher baseline does not require a dump). Build it when EITHER is asked
	// for, but wire the two Config fields separately: assigning cfg.BaselineCtrl
	// un-gates the Create-baseline button, so deriving one from the other would
	// either refuse to start a refresh-only daemon or silently switch on a
	// feature the operator did not enable.
	// The sweep runs regardless of the supervisor decision below: with both
	// baseline features off, no supervisor would ever remove a previous
	// process's staged dump.
	sweepSQLExportStaging(baselineStagingDir())
	var baselineSup *baselineSupervisor
	if upConsoleBaselineTrigger || upBaselineRefreshEvery != "" {
		baselineSup = newBaselineSupervisorFromConfig(ctx, baselineStagingDir())
	}
	if upConsoleBaselineTrigger {
		cfg.BaselineCtrl = baselineSup
	}
	if upBaselineRefreshEvery != "" {
		cfg.BaselineRefresh = baselineSup
		// Live target count for GET /api/baseline-refresh (#1579): the loop
		// recomputes its set per tick, so the page must too, or a fresh
		// install reports everything-running over zero refreshable servers.
		cfg.BaselineRefreshTargets = func() (int, int) {
			reqs, skipped := baselineRefreshTargets(registryEntries(registry), upIndexDSN, upConsoleBaselineDir)
			return len(reqs), len(skipped)
		}
	}
	wireBaselineExtras(&cfg, baselineSup, serversPath)
	// The per-server backup schedule (#1442) needs only the supervisor: the
	// chooser checks the creation opt-in per slot whenever a full backup is
	// the producer it picks.
	// The helper returns a true nil interface for a nil supervisor; assigning
	// a nil *backupScheduler here directly would advertise a loop that does
	// not exist.
	var backupSched *backupScheduler
	cfg.BackupSchedules, backupSched = newBackupScheduleReporter(baselineSup, registry, upConsoleBaselineTrigger, upBaselineCarryForward)
	notifier, err := newWatchNotifierFromFlags(ctx)
	if err != nil {
		return err
	}
	if err := wireVerify(ctx, &cfg, registry, serversPath, notifier); err != nil {
		return err
	}
	// The continuity watch serves two channels: webhook events (notifier) and
	// the Prometheus gauge (#1203). Either one being enabled starts it; with
	// neither, nothing runs.
	if notifier != nil || upMetricsAddr != "" {
		startContinuityWatch(ctx, notifier, registry, upIndexDSN)
	}
	// Baseline staleness (#1193) is webhook-only: status/console carry the
	// full verdict; the channel gets the broken transition.
	if notifier != nil {
		startStalenessWatch(ctx, notifier, registry, upIndexDSN, upConsoleBaselineDir, upConsoleBaselineS3)
	}

	// Built-in rotation: boot index + every per-source database the control
	// plane provisions, on the daemon lifecycle. Live settings provider so the
	// console can retune retain/interval/add-future without a restart.
	rotation.StartLoop(ctx, rotationSettingsProvider(registry), func() []rotation.RotateTarget {
		return rotateTargets(upIndexDSN, supervisor, registry, archiveStagingDir())
	}, rotationCycleHooks(notifier)...)

	// Reclaim local baseline snapshots that already have a durable S3 copy (#616):
	// the global --baseline-dir plus every registry server's per-server dir.
	if err := startBaselinePruneLoop(ctx, registry, upConsoleBaselineDir, upConsoleBaselineS3, upConsoleBaselineRetain, upRotationCfg.Interval); err != nil {
		return err
	}

	// Keep each server's newest snapshot moving forward from the index alone
	// (#1171). Opt-in, and refused at startup when nothing can be refreshed.
	if err := startBaselineRefreshLoop(ctx, registry, baselineSup, upIndexDSN, upConsoleBaselineDir, upBaselineRefreshEvery, upBaselineCarryForward); err != nil {
		return err
	}
	// Per-server backup schedules from the Backups page (#1442). No flag:
	// the registry decides what runs, the loop only looks at the clock.
	startBackupScheduleLoop(ctx, backupSched)

	// With the console comes the multi-stream control plane, so /metrics is
	// served once at the daemon level (per-source "source" labels keep the
	// series apart). The main stream's config keeps MetricsAddr empty so it
	// never double-binds the same address inside streamrun.One. Synchronous
	// bind: fails fast, like the console bind below.
	if upMetricsAddr != "" {
		stopMetrics, err := streamrun.StartMetricsServer(upMetricsAddr)
		if err != nil {
			return err
		}
		defer stopMetrics()
	}

	// Wire the live telemetry client so the console's opt-out toggle reaches
	// this running daemon (see the other console.New site).
	cfg.Telemetry = tel.Client()
	srv, err := console.New(cfg)
	if err != nil {
		return err
	}

	// Bind synchronously so a port conflict fails `watch` fast — otherwise the
	// console would report "running" while the stream blocks for hours over a
	// server that never bound.
	ln, err := srv.Listen()
	if err != nil {
		return fmt.Errorf("console: cannot bind %s: %w", upConsoleListen, err)
	}
	// Bind the flashback port BEFORE starting the console goroutine: its error
	// (missing token, port conflict) must return before any goroutine touches
	// the shared index db, so the deferred db.Close can never race an in-flight
	// console request on a failed startup (runUpConsoleOnly binds it before its
	// synchronous Serve for the same reason).
	stopFlashback, err := startFlashbackPort(ctx, srv)
	if err != nil {
		return err
	}

	// The console is the secondary job: log a mid-run crash when it happens (not
	// only at shutdown), but NEVER let it take down the stream, which is the
	// primary data-capture job.
	consoleDone := make(chan struct{}, 1)
	go func() {
		if err := srv.Serve(ctx, ln); err != nil {
			slog.Warn("console server exited with error", "error", err)
		}
		consoleDone <- struct{}{}
	}()
	printConsoleBanner(srv, "Console (read-only) is running. Open:")

	// Resume whatever the operator had monitoring before the restart —
	// desired state lives in the registry, positions in each per-source
	// stream_state checkpoint.
	go supervisor.Reconcile(registry)

	// Extension source jobs (ext.RegisterSourceJob) for the daemon's MAIN source
	// run alongside its stream on the daemon context (ctx) — the same secondary,
	// never-fatal, daemon-scoped contract as `bintrail up` (cliapp/up.go) and the
	// built-in rotation loop above. The supervised registry sources get their own
	// jobs from the monitor supervisor (consoleapp/monitor.go); this call covers
	// only the single main source `watch --source-dsn` streams. Flavor is the
	// value the main stream below actually runs with (streamCfg.Flavor). No-op in
	// the stock binary.
	streamCfg := watchStreamConfig(serverID)
	ext.RunSourceJobs(ctx, mainSourceJobInfo(upSourceDSN, upIndexDSN, streamCfg.Flavor))

	streamErr := runMainStreamWithWriteDeadlineRetry(ctx, streamCfg)
	stop()                // drain the console even if the stream returned without a signal
	<-consoleDone         // order the console goroutine's exit before the deferred db.Close()
	stopFlashback()       // drain the flashback port before the deferred db.Close()
	supervisor.Shutdown() // final checkpoints for every monitored stream
	return streamErr
}

// watchStreamConfig snapshots watch's flag values into the main stream's
// streamrun.Config. The pinned values (StartPos 4, GapTimeout 30, …)
// replicate what core `up` produced via its populateStreamFlags →
// streamConfigFromFlags fan-out: `watch`, like `up`, deliberately exposes only
// the quickstart subset of stream's flags. The source TLS settings ARE
// configurable (--ssl-mode/--ssl-ca/--ssl-cert/--ssl-key or BINTRAIL_SSL_*,
// #879); SSLMode defaults to "preferred" only when left unset, so the previous
// behavior is unchanged. MetricsAddr stays empty on purpose — the daemon serves
// ONE /metrics endpoint for all streams (see runUpStreamWithConsole).
func watchStreamConfig(serverID uint32) streamrun.Config {
	return streamrun.Config{
		IndexDSN:   upIndexDSN,
		SourceDSN:  upSourceDSN,
		ServerID:   serverID,
		StartFile:  "",
		StartPos:   4,
		StartGTID:  "",
		BatchSize:  upBatchSize,
		Schemas:    upSchemas,
		Tables:     upTables,
		Checkpoint: upCheckpoint,
		SSLMode:    upSSLMode,
		SSLCA:      upSSLCA,
		SSLCert:    upSSLCert,
		SSLKey:     upSSLKey,
		Format:     upFormat,
		GapTimeout: 30,
		// The daemon serves /metrics centrally, so the primary stream sets
		// neither MetricsAddr nor MetricsSource — IndexMetrics turns the
		// bintrail_index_* scraper on for it when the daemon exposes metrics.
		IndexMetrics:          upMetricsAddr != "",
		MetricsScrapeInterval: upMetricsScrapeInterval,
		Deps:                  streamdeps.Default(),
	}
}

// mainSourceJobInfo builds the ext.SourceJobInfo for `watch`'s main (non-registry)
// source. Extracted from runUpStreamWithConsole so the flavor resolution is
// unit-testable without a live daemon. streamFlavor is watchStreamConfig's
// Flavor: `watch` exposes no --source-flavor for its main source, so it is empty
// and streamrun.One normalizes it to "mysql" internally; we default it to the
// same canonical value here so a registered job sees the non-empty flavor
// `bintrail up` supplies (never "").
func mainSourceJobInfo(sourceDSN, indexDSN, streamFlavor string) ext.SourceJobInfo {
	flavor := streamFlavor
	if flavor == "" {
		flavor = console.FlavorMySQL
	}
	return ext.SourceJobInfo{SourceDSN: sourceDSN, IndexDSN: indexDSN, Flavor: flavor}
}

// resolveUpConsoleEnv applies the console-specific env vars to the upConsole*
// globals with flag > env > default precedence (mirrors runServe). These are
// read directly rather than bound in watchEnvBindings: BINTRAIL_CONSOLE_* are
// console-only vars whose flags (--baseline-dir/--baseline-s3) also exist on
// core bintrail commands, and the direct read keeps the precedence dance in
// one unit-testable place.
// newBaselineSupervisorFromConfig builds the supervisor from the resolved
// console configuration. It exists so the two watch entry points cannot drift
// on the one wiring that matters: carrying an invalid-lock-mode error into the
// supervisor. Dropping that assignment is invisible at either call site — the
// daemon still boots and baselines still run, in a mode the operator did not
// ask for — so it is asserted here rather than duplicated there.
func newBaselineSupervisorFromConfig(ctx context.Context, stagingDir string) *baselineSupervisor {
	sup := newBaselineSupervisor(ctx, stagingDir, upConsoleBaselineLockMode)
	sup.configErr = upConsoleBaselineLockModeErr
	// The download TTL for staged .sql builds (#1448) needs a clock nobody
	// is polling: the Backups page expires lazily only while it is open.
	go sup.runSQLExportReaper()
	return sup
}

// envBoolOr reads a boolean environment variable, keeping fallback when the
// variable is unset or does not parse.
//
// strconv.ParseBool rather than a hand-written value list, because the repo
// already had two conventions for this (pflag's ParseBool behind
// BINTRAIL_ULTRAFAST, any-non-empty behind BINTRAIL_DUCKDB_NO_AWS_EXT) and a
// third one written inline would be the one nobody can predict. ParseBool is
// the same set pflag accepts: 1/t/T/TRUE/true/True and their false twins.
//
// An unparseable value keeps the fallback rather than erroring, and that is the
// conservative direction for every current caller: with no flag passed the
// fallback is the safe default, so a typo can only fail to turn something on.
func envBoolOr(name string, fallback bool) bool {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}
	v, err := strconv.ParseBool(raw)
	if err != nil {
		slog.Warn("environment variable is not a true/false value, so it was ignored",
			"variable", name, "value", raw, "using", fallback)
		return fallback
	}
	return v
}

func resolveUpConsoleEnv(cmd *cobra.Command) error {
	if !cmd.Flags().Changed("console-listen") {
		if v := os.Getenv("BINTRAIL_CONSOLE_LISTEN"); v != "" {
			upConsoleListen = v
		}
	}
	if !cmd.Flags().Changed("console-token") {
		if v := os.Getenv("BINTRAIL_CONSOLE_TOKEN"); v != "" {
			upConsoleToken = v
		}
	}
	if !cmd.Flags().Changed("baseline-dir") {
		if v := os.Getenv("BINTRAIL_CONSOLE_BASELINE_DIR"); v != "" {
			upConsoleBaselineDir = v
		}
	}
	if !cmd.Flags().Changed("baseline-s3") {
		if v := os.Getenv("BINTRAIL_CONSOLE_BASELINE_S3"); v != "" {
			upConsoleBaselineS3 = v
		}
	}
	if !cmd.Flags().Changed("baseline-retain") {
		if v := os.Getenv("BINTRAIL_CONSOLE_BASELINE_RETAIN"); v != "" {
			upConsoleBaselineRetain = v
		}
	}
	if !cmd.Flags().Changed("baseline-refresh-interval") {
		if v := os.Getenv("BINTRAIL_BASELINE_REFRESH_INTERVAL"); v != "" {
			upBaselineRefreshEvery = v
		}
	}
	if !cmd.Flags().Changed("baseline-carry-forward-unchanged") {
		upBaselineCarryForward = envBoolOr("BINTRAIL_BASELINE_CARRY_FORWARD_UNCHANGED", upBaselineCarryForward)
	}
	if !cmd.Flags().Changed("console-servers-file") {
		if v := os.Getenv("BINTRAIL_CONSOLE_SERVERS"); v != "" {
			upConsoleServersFile = v
		}
	}
	if !cmd.Flags().Changed("console-auth-file") {
		if v := os.Getenv("BINTRAIL_CONSOLE_AUTH"); v != "" {
			upConsoleAuthFile = v
		}
	}
	if !cmd.Flags().Changed("console-mcp-token-file") {
		if v := os.Getenv("BINTRAIL_CONSOLE_MCP_TOKEN_FILE"); v != "" {
			upConsoleMCPTokenFile = v
		}
	}
	if !cmd.Flags().Changed("console-tls-cert") {
		if v := os.Getenv("BINTRAIL_CONSOLE_TLS_CERT"); v != "" {
			upConsoleTLSCert = v
		}
	}
	if !cmd.Flags().Changed("console-tls-key") {
		if v := os.Getenv("BINTRAIL_CONSOLE_TLS_KEY"); v != "" {
			upConsoleTLSKey = v
		}
	}
	if !cmd.Flags().Changed("console-allowed-hosts") {
		if v := os.Getenv("BINTRAIL_CONSOLE_ALLOWED_HOSTS"); v != "" {
			upConsoleAllowedHost = strings.Split(v, ",")
		}
	}
	if !cmd.Flags().Changed("console-allow-setup") {
		if v := os.Getenv("BINTRAIL_CONSOLE_ALLOW_SETUP"); v == "1" || v == "true" {
			upConsoleAllowSetup = true
		}
	}
	if !cmd.Flags().Changed("flashback-listen") {
		if v := os.Getenv("BINTRAIL_CONSOLE_FLASHBACK_LISTEN"); v != "" {
			upConsoleFlashbackListen = v
		}
	}
	if !cmd.Flags().Changed("archive-staging-dir") {
		if v := os.Getenv("BINTRAIL_CONSOLE_ARCHIVE_STAGING"); v != "" {
			upArchiveStageDir = v
		}
	}
	// Baseline trigger is env-only (no flag): opt-in plus an optional staging dir.
	if v := os.Getenv("BINTRAIL_CONSOLE_BASELINE_TRIGGER"); v == "1" || v == "true" {
		upConsoleBaselineTrigger = true
	}
	if v := os.Getenv("BINTRAIL_CONSOLE_BASELINE_STAGING"); v != "" {
		upBaselineStageDir = v
	}
	// Lock mode defaults to point-consistent (baseline.DefaultLockMode). The
	// pre-#1377 BINTRAIL_CONSOLE_BASELINE_POINT_CONSISTENT opt-in is gone: it
	// selected what is now the default, so an operator who set it keeps the
	// behaviour they asked for and needs no migration. Only this variable can
	// select a WEAKER mode — a snapshot that can be torn has to be asked for.
	if v := os.Getenv("BINTRAIL_CONSOLE_BASELINE_LOCK_MODE"); v != "" {
		m, err := baseline.ParseLockMode(v)
		if err != nil {
			// Refuse BASELINES, not the daemon. Under `watch` this process is
			// also the capture plane, so failing startup over a baseline
			// setting would turn a typo into permanently lost events — the
			// same trap that made a refresh-only daemon refuse to boot. The
			// error is carried to the baseline supervisor, which returns it
			// from every Trigger, so it lands in baseline status where the
			// operator is looking.
			upConsoleBaselineLockModeErr = fmt.Errorf("BINTRAIL_CONSOLE_BASELINE_LOCK_MODE: %w", err)
			slog.Error("console: MySQL baseline DUMPS disabled by an invalid lock mode; capture and the periodic refresh are unaffected",
				"error", upConsoleBaselineLockModeErr)
		} else {
			upConsoleBaselineLockMode = m
		}
	}
	// Verify trigger is env-only (no flag), same shape as baseline trigger.
	if v := os.Getenv("BINTRAIL_CONSOLE_VERIFY_TRIGGER"); v == "1" || v == "true" {
		upConsoleVerifyTrigger = true
	}
	if !cmd.Flags().Changed("verify-interval") {
		if v := os.Getenv("BINTRAIL_CONSOLE_VERIFY_INTERVAL"); v != "" {
			upVerifyInterval = v
		}
	}
	if !cmd.Flags().Changed("verify-tables") {
		if v := os.Getenv("BINTRAIL_CONSOLE_VERIFY_TABLES"); v != "" {
			upVerifyTables = v
		}
	}
	if !cmd.Flags().Changed("notify-webhook") {
		if v := os.Getenv("BINTRAIL_CONSOLE_NOTIFY_WEBHOOK"); v != "" {
			upNotifyWebhook = v
		}
	}
	return nil
}

// baselineStagingDir resolves the local staging base for baselines destined for
// S3 (BINTRAIL_CONSOLE_BASELINE_STAGING). The dump and the staged Parquet are
// written under a fresh temp subdir here per run and removed after upload, so a
// leftover never causes a re-upload of an old snapshot. Default: an OS temp subdir.
func baselineStagingDir() string {
	if upBaselineStageDir != "" {
		return upBaselineStageDir
	}
	return filepath.Join(os.TempDir(), "bintrail-baseline-staging")
}

// wireVerify wires the in-process verify supervisor and, when
// --verify-interval is set, the scheduled verification loop (#1191). The
// supervisor (and with it the manual trigger endpoints) is enabled by either
// opt-in — BINTRAIL_CONSOLE_VERIFY_TRIGGER=1 or a schedule: scheduling verify
// implies wanting verify.
func wireVerify(ctx context.Context, cfg *console.Config, registry *console.Registry, serversPath string, notifier *watchNotifier) error {
	var interval time.Duration
	if upVerifyInterval != "" {
		var err error
		interval, err = cliutil.ParseRetain(upVerifyInterval)
		if err != nil {
			return fmt.Errorf("--verify-interval: %w", err)
		}
	}
	if !upConsoleVerifyTrigger && interval == 0 {
		return nil
	}
	history, err := console.OpenVerifyHistory(console.DefaultVerifyHistoryPath(serversPath))
	if err != nil {
		// Run without history rather than refusing to start the daemon — the
		// file is an observability aid, and NOT opening a store means nothing
		// ever overwrites the unreadable file it might still describe.
		slog.Error("verify history unavailable; runs will NOT be recorded and the history endpoint will refuse — fix or move the file and restart", "error", err)
		history = nil
	}
	sup := newVerifySupervisor(ctx, history, verifyFinishObservers(notifier))
	seedVerifyGauges(registry, history)
	cfg.VerifyCtrl = sup
	cfg.VerifyHistory = history
	if interval > 0 {
		startVerifyLoop(ctx, sup, registry, history, interval, splitVerifyTables(upVerifyTables))
	}
	return nil
}

// verifyFinishObservers composes the supervisor's finish hook: the health
// gauges always (#1203), the webhook notifier when configured. Both observe
// the same record history gets.
func verifyFinishObservers(notifier *watchNotifier) func(console.VerifyRunRecord) {
	return func(rec console.VerifyRunRecord) {
		setVerifyGauges(rec, rec.ServerName)
		if notifier != nil {
			notifier.VerifyFinished(rec)
		}
	}
}

// verifyRunPublishable reports whether a record carries a verdict the gauges
// may publish. Only a succeeded run that verified at least one table
// conclusively counts — a failed run, a zero-table run ("only one baseline
// yet"), or an all-inconclusive run must NOT overwrite the last real verdict:
// zeroed counts would auto-resolve a live mismatch alert, and a refreshed
// timestamp would keep the staleness alert quiet while verification is in
// fact broken. It recognizes the same degenerate-run shapes as the webhook's
// clean/problem split (watchNotifier.VerifyFinished) and Report.ExitError,
// but the VERDICTS differ by design: the webhook still notifies on
// failed/all-inconclusive runs, and the gauges publish mismatch runs (the
// alert must fire) — do not extract one shared predicate.
func verifyRunPublishable(rec console.VerifyRunRecord) bool {
	s := rec.Summary
	return rec.State == "succeeded" && s.Total > 0 && s.Inconclusive < s.Total
}

// setVerifyGauges publishes one finished run under the given server label
// (the CURRENT display name — the seed path must not resurrect a pre-rename
// name from an old record).
func setVerifyGauges(rec console.VerifyRunRecord, server string) {
	if !verifyRunPublishable(rec) {
		return
	}
	finished, err := time.Parse(time.RFC3339, rec.FinishedAt)
	if err != nil {
		return
	}
	s := rec.Summary
	observe.SetVerifyOutcome(server, finished, s.Match, s.Mismatch, s.Inconclusive, s.Error)
}

// seedVerifyGauges republishes each registry server's newest publishable run
// at startup (#1203) — the pull path survives restarts, reading the same
// history the console panel reads (the panel additionally shows failed runs;
// the gauges only carry conclusive verdicts).
func seedVerifyGauges(registry *console.Registry, history *console.VerifyHistory) {
	if registry == nil || history == nil {
		return
	}
	for _, e := range registry.List() {
		for _, rec := range history.List(e.ID) {
			if !verifyRunPublishable(rec) {
				continue
			}
			setVerifyGauges(rec, e.Name)
			break
		}
	}
}

// splitVerifyTables parses the comma-separated --verify-tables list; empty
// entries are dropped, an empty flag means no filter (nil).
func splitVerifyTables(raw string) []string {
	var out []string
	for _, t := range strings.Split(raw, ",") {
		if t = strings.TrimSpace(t); t != "" {
			out = append(out, t)
		}
	}
	return out
}

// startVerifyLoop runs one scheduled verification cycle per interval (#1191):
// every registry server, sequentially — one verify (one DuckDB budget) at a
// time. Mirrors startBaselinePruneLoop's shape: recover-guarded, first cycle
// shortly after startup, stops with the daemon context.
func startVerifyLoop(ctx context.Context, sup *verifySupervisor, registry *console.Registry, history *console.VerifyHistory, interval time.Duration, tables []string) {
	slog.Info("scheduled verification enabled", "interval", interval)
	go func() {
		if ctx.Err() == nil {
			runScheduledVerifyCycle(ctx, sup, registry, history, tables)
		}
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				runScheduledVerifyCycle(ctx, sup, registry, history, tables)
			}
		}
	}()
}

// runScheduledVerifyCycle is one pass of the scheduled verification loop —
// package-level so a unit test can drive registry→request→run→history without
// the goroutine/ticker plumbing.
func runScheduledVerifyCycle(ctx context.Context, sup *verifySupervisor, registry *console.Registry, history *console.VerifyHistory, tables []string) {
	// A panic must NEVER take down the daemon's primary capture — this
	// background check shares the process with the stream. Mirrors the
	// baseline-prune loop's guard.
	defer func() {
		if r := recover(); r != nil {
			slog.Error("scheduled verify cycle panicked; verification continues next tick", "panic", r)
		}
	}()
	var entries []console.ServerEntry
	if registry != nil {
		entries = registry.List()
	}
	if len(entries) == 0 {
		// Loud, every cycle: "loop running, verifying nothing" must not
		// look like "verifying everything". The schedule covers registry
		// servers; the command-line boot stream is not in the registry.
		slog.Warn("scheduled verify: no registry servers to verify — the schedule covers servers added in the console UI; a source configured only via command-line flags/env is not covered")
		return
	}
	for _, e := range entries {
		if ctx.Err() != nil {
			return
		}
		err := sup.RunScheduled(scheduledVerifyRequest(e, tables, upConsoleBaselineDir, upConsoleBaselineS3))
		if errors.Is(err, console.ErrVerifyRunning) {
			slog.Info("scheduled verify: skipped, a run is already in flight", "server", e.Name)
			recordVerifySkip(history, e, "a verify run was already in flight when the schedule fired")
		}
	}
}

// scheduledVerifyRequest picks the check a scheduled cycle runs for one
// server: baseline-anchored where a baseline location is configured — the
// entry's own, or the process-wide fallback, all-or-nothing exactly like
// withBaselineDefaults (#1010) — and the index-only recover-inputs check
// otherwise, so a server with no baseline is still verified rather than
// silently skipped.
func scheduledVerifyRequest(e console.ServerEntry, tables []string, globalDir, globalS3 string) console.VerifyRequest {
	dir, s3 := e.BaselineDir, e.BaselineS3
	if dir == "" && s3 == "" {
		dir, s3 = globalDir, globalS3
	}
	mode := console.VerifyModeBaselineAnchored
	if dir == "" && s3 == "" {
		mode = console.VerifyModeRecoverInputs
	}
	return console.VerifyRequest{
		ServerID: e.ID, ServerName: e.Name, Mode: mode, Tables: tables,
		IndexDSN: e.DSN, BaselineDir: dir, BaselineS3: s3, NoArchive: e.NoArchive,
	}
}

// recordVerifySkip persists a "skipped" record so a schedule that never gets
// to run stays visible in the history instead of silent.
func recordVerifySkip(history *console.VerifyHistory, e console.ServerEntry, reason string) {
	if history == nil {
		return
	}
	// One consecutive skip per cause: a wedged run plus a short interval would
	// otherwise append an identical skip every cycle, and the capped history
	// would evict the real verdicts — erasing exactly the "when did this last
	// actually verify" answer the history exists to keep.
	if recs := history.List(e.ID); len(recs) > 0 && recs[0].State == "skipped" && recs[0].SkipReason == reason {
		return
	}
	err := history.Append(console.VerifyRunRecord{
		ServerID: e.ID, ServerName: e.Name, Trigger: console.VerifyTriggerScheduled, SkipReason: reason,
		VerifyStatus: console.VerifyStatus{State: console.VerifyStateSkipped, Since: nowStamp(), FinishedAt: nowStamp()},
	})
	if err != nil {
		slog.Warn("scheduled verify: could not persist skip to history", "server", e.Name, "error", err)
	}
}

// baselinePruneTarget is one (local dir, durable S3) pair the prune loop reclaims.
type baselinePruneTarget struct {
	dir string
	s3  string
}

// baselinePruneTargets collects every baseline directory the daemon should prune,
// each paired with the S3 prefix that proves a snapshot is durable (#616):
//   - the daemon-global --baseline-dir/--baseline-s3 (the compose baseline profile
//     and the boot index write here), and
//   - every registry server's BaselineDir/BaselineS3 — the PER-SERVER dirs the
//     console "Create baseline" trigger (#613/#615) writes into (req.LocalDir =
//     entry.BaselineDir), which the global flag does NOT cover.
//
// A target needs BOTH a dir (to prune) and an S3 prefix (to confirm durability);
// dir-only or s3-only entries are skipped — a local snapshot with no S3 copy is
// the only copy and is never deleted. Deduped so a server that reuses the global
// dir is not pruned twice. Read fresh each cycle so a server added/edited from the
// console is covered on the next tick without a restart (mirrors rotateTargets).
func baselinePruneTargets(entries []console.ServerEntry, globalDir, globalS3 string) []baselinePruneTarget {
	var targets []baselinePruneTarget
	seen := map[string]bool{}
	add := func(dir, s3 string) {
		if dir == "" || s3 == "" {
			return
		}
		key := dir + "\x00" + s3
		if seen[key] {
			return
		}
		seen[key] = true
		targets = append(targets, baselinePruneTarget{dir: dir, s3: s3})
	}
	add(globalDir, globalS3)
	for _, e := range entries {
		add(e.BaselineDir, e.BaselineS3)
	}
	return targets
}

// runBaselinePruneCycle prunes each target via pruneFn, one (dir + S3) pair at a
// time. A failure on one target is logged and the rest still run — one bad dir
// must not strand the others. pruneFn is injected (baseline.PruneLocal in
// production) so the per-target iteration is unit-testable without S3.
func runBaselinePruneCycle(ctx context.Context, targets []baselinePruneTarget, retain time.Duration, pruneFn func(context.Context, baseline.PruneOptions) (baseline.PruneResult, error)) {
	for _, t := range targets {
		res, err := pruneFn(ctx, baseline.PruneOptions{
			LocalDir: t.dir,
			S3URL:    t.s3,
			Retain:   retain,
		})
		if err != nil {
			slog.Warn("baseline prune cycle failed", "dir", t.dir, "error", err)
			continue
		}
		if len(res.Pruned) > 0 {
			slog.Info("baseline prune cycle complete",
				"dir", t.dir, "pruned", len(res.Pruned), "reclaimed_bytes", res.ReclaimedBytes)
		}
		// Reported even when nothing was pruned, and that is the point: a
		// snapshot old enough to reclaim whose copy is NOT in the bucket stays
		// on this disk forever, and since #1539 a scheduled update whose upload
		// failed produces exactly that. Logging only successful prunes made the
		// accumulation invisible after the one line at the moment of failure,
		// while the page suppresses its disk warning for any server that HAS a
		// destination.
		if res.KeptNotDurable > 0 {
			slog.Warn("baseline prune: snapshots are old enough to reclaim but have no confirmed copy at the backup "+
				"destination, so they stay on this disk. Send them, or remove them by hand.",
				"dir", t.dir, "kept", res.KeptNotDurable, "destination", t.s3)
		}
		if res.ProbeErrors > 0 {
			slog.Warn("baseline prune: could not check whether some snapshots are at the backup destination, so they "+
				"were kept", "dir", t.dir, "unchecked", res.ProbeErrors)
		}
	}
}

// startBaselinePruneLoop launches a periodic prune of local baseline snapshots
// across every (dir + S3) target — the daemon-global pair plus every registry
// server's per-server baseline dir (#616). Each target is reclaimed only where a
// durable S3 copy exists; a dir with no S3 source is left untouched (its snapshots
// are the only copy — the same invariant rotation enforces with
// PruneLocalAfterUpload && ArchiveS3 != ""). The loop runs on the daemon context
// and stops when it is cancelled; it shares the rotation cadence (baselines change
// far less often than partitions, so this interval is ample). A bad
// --baseline-retain value is a fatal misconfiguration returned BEFORE the
// goroutine starts, so a typo fails the daemon fast rather than spinning.
func startBaselinePruneLoop(ctx context.Context, reg *console.Registry, globalDir, globalS3, retainRaw string, interval time.Duration) error {
	if retainRaw == "" {
		return nil // retention not configured — leave baselines untouched
	}
	retain, err := cliutil.ParseRetain(retainRaw)
	if err != nil {
		return fmt.Errorf("--baseline-retain: %w", err)
	}
	if globalDir != "" && globalS3 == "" {
		// The operator pointed retention at a local dir with no durable S3 source;
		// warn once so the global dir not being reclaimed isn't a silent surprise.
		// Per-server registry targets (added at runtime) may still have both.
		slog.Warn("--baseline-retain is set and --baseline-dir is configured but --baseline-s3 is not; the global baseline dir will not be pruned (its snapshots are the only copy)")
	}
	if interval <= 0 {
		interval = time.Hour
	}
	slog.Info("baseline prune loop enabled", "retain", retainRaw, "interval", interval)
	go func() {
		runOnce := func() {
			// Recover-guard the cycle: a panic in PruneLocal (live S3 calls, fs
			// walks) must NEVER take down the daemon's primary forensic capture —
			// this optional disk-reclaim feature shares the process with the
			// stream. Mirrors rotation.StartLoop's guard (internal/rotation).
			defer func() {
				if r := recover(); r != nil {
					slog.Error("baseline prune cycle panicked; retention continues next tick", "panic", r)
				}
			}()
			var entries []console.ServerEntry
			if reg != nil {
				entries = reg.List()
			}
			// A per-server local baseline dir with no S3 prefix is the only copy —
			// skipped, but warn (matching the global/CLI signal) so its unbounded
			// growth isn't silent.
			for _, e := range entries {
				if e.BaselineDir != "" && e.BaselineS3 == "" {
					slog.Warn("baseline-retain: server has a local baseline dir but no S3 prefix; its baselines are the only copy and will not be pruned",
						"server", e.Name, "dir", e.BaselineDir)
				}
			}
			runBaselinePruneCycle(ctx, baselinePruneTargets(entries, globalDir, globalS3), retain, baseline.PruneLocal)
		}
		// One sweep shortly after startup (the min-age floor protects any
		// just-created snapshot), then on the interval — unless the daemon is
		// already shutting down.
		if ctx.Err() == nil {
			runOnce()
		}
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				runOnce()
			}
		}
	}()
	return nil
}

// consoleOpts carries watch's console-surface settings into upConsoleConfig —
// a struct rather than a growing list of positional string params (it crossed
// six with auth+TLS; nine positionals is how arguments get transposed).
type consoleOpts struct {
	Listen       string
	Token        string
	BaselineDir  string
	BaselineS3   string
	AuthFile     string
	MCPTokenFile string
	// ServersFile is the console server registry path. It reaches
	// upConsoleConfig only so the stack-drift check reports the file this
	// daemon will actually open (#1529); the registry itself is loaded at the
	// call sites, which is why nothing else here reads it.
	ServersFile  string
	TLSCert      string
	TLSKey       string
	AllowedHosts []string
	AllowSetup   bool
	// FlashbackListen is the --flashback-listen address, reported to the
	// console so the Connect page can show the port (#1446). Empty = off.
	// startFlashbackPort binds it after console.New and aborts the daemon on
	// a bind failure, so a value the console reports was bound at startup;
	// a mid-run serve failure is logged and swallowed, not reflected here.
	FlashbackListen string
}

// upConsoleOpts snapshots the resolved upConsole* globals.
func upConsoleOpts() consoleOpts {
	return consoleOpts{
		Listen:          upConsoleListen,
		Token:           upConsoleToken,
		BaselineDir:     upConsoleBaselineDir,
		BaselineS3:      upConsoleBaselineS3,
		AuthFile:        upConsoleAuthFile,
		MCPTokenFile:    upConsoleMCPTokenFile,
		ServersFile:     upConsoleServersFile,
		TLSCert:         upConsoleTLSCert,
		TLSKey:          upConsoleTLSKey,
		AllowedHosts:    upConsoleAllowedHost,
		AllowSetup:      upConsoleAllowSetup,
		FlashbackListen: upConsoleFlashbackListen,
	}
}

// upConsoleConfig builds the console configuration for `watch`. It serves
// the Phase 1 surface (events/recover/status) over the live index, plus the
// baseline-gated Reconstruct (Time-travel) surface when a baseline source is
// supplied — still no profile or --no-archive, so the reconstruct gate
// (baselineConfigured in internal/console/server.go, which owns dir-over-s3
// precedence) reduces to baseline presence. Extracted for testability (dbName
// extraction + DSN validation).
// errString renders a possibly-nil error for a wire field.
func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func upConsoleConfig(db *sql.DB, indexDSN string, opts consoleOpts) (console.Config, error) {
	cfg, err := mysql.ParseDSN(indexDSN)
	if err != nil {
		return console.Config{}, fmt.Errorf("invalid --index-dsn: %w", err)
	}
	if cfg.DBName == "" {
		return console.Config{}, fmt.Errorf("--index-dsn must include a database name (e.g. user:pass@tcp(host:3306)/binlog_index)")
	}
	// Report a stack whose compose file is behind the images (#1529). Here
	// because both watch entry points reach this function and neither reaches
	// the other, and once per process: it is a startup line, not a monitor.
	composeDriftReporter(indexDSN, opts)
	return console.Config{
		DB:              db,
		DBName:          cfg.DBName,
		BootDSN:         indexDSN,
		Listen:          opts.Listen,
		Token:           opts.Token,

		BaselineDir:     opts.BaselineDir,
		BaselineS3:      opts.BaselineS3,
		AuthPath:        opts.AuthFile,
		MCPTokenPath:    opts.MCPTokenFile,
		TLSCert:         opts.TLSCert,
		TLSKey:          opts.TLSKey,
		AllowedHosts:    opts.AllowedHosts,
		FlashbackListen: opts.FlashbackListen,
		// The daemon's --rotate-* defaults, so GET /api/rotation can report the
		// effective policy (and the console panel prefill it) before the
		// operator saves an override.
		RotationDefaults: console.RotationDefaults{
			Retain:    upRotateRetain,
			Interval:  upRotateInterval,
			AddFuture: upRotateAddFuture,
			Enabled:   upRotationCfg.Enabled,
		},
		// Same role for the baseline-refresh panel: what the daemon itself was
		// told, reported when no console override is saved. Enabled is the
		// loop's boot-time liveness, so the panel can say a saved setting is
		// dormant instead of implying it is live.
		// The Backup settings page's read-only rows (#1582):
		// what this daemon was told, verbatim, each under the exact flag or
		// env name the page shows beside it. Values, not re-derivations — the
		// page's whole job is saying where the effective value came from.
		BackupSettingsDefaults: console.BackupSettingsDefaults{
			BaselineRetain: upConsoleBaselineRetain,
			RefreshEvery:   upBaselineRefreshEvery,
			LockMode:       string(upConsoleBaselineLockMode),
			LockModeErr:    errString(upConsoleBaselineLockModeErr),
			TriggerOn:      upConsoleBaselineTrigger,
			StagingDir:     upBaselineStageDir,
			VerifyInterval: upVerifyInterval,
			VerifyTables:   upVerifyTables,
		},
		BaselineRefreshDefaults: console.BaselineRefreshDefaults{
			CarryForwardUnchanged: upBaselineCarryForward,
			// Enabled is the OR because the restore consumes this setting too
			// and is wired off the supervisor, which --baseline-trigger alone
			// creates. Scheduled is the narrower interval-only fact. The two
			// must be computed from the same expressions that gate the two
			// consumers in runWatch, or the panel drifts from the daemon.
			Enabled:   upBaselineRefreshEvery != "" || upConsoleBaselineTrigger,
			Scheduled: upBaselineRefreshEvery != "",
		},
		AllowSetup: opts.AllowSetup,
		Version:    appVersion,
		// MonitorCtrl (the control-plane supervisor) is wired by the caller —
		// runUpStreamWithConsole / runUpConsoleOnly — because it needs the
		// registry and the daemon lifecycle context, which this config builder
		// doesn't have.
	}, nil
}

// archiveStagingDir resolves the local staging base for S3 archive uploads
// (--archive-staging-dir / BINTRAIL_CONSOLE_ARCHIVE_STAGING). A lost staging
// dir self-heals: an un-uploaded partition is never dropped, so the next cycle
// re-archives it from the still-present partition. Default: a temp subdir.
func archiveStagingDir() string {
	if upArchiveStageDir != "" {
		return upArchiveStageDir
	}
	return filepath.Join(os.TempDir(), "bintrail-archive-staging")
}

// rotateTargets assembles the built-in rotation's per-cycle targets: the boot
// index (drop-only — the ephemeral default entry has no registry archive
// config) plus every supervised source. A source whose registry entry carries
// an Archive S3 bucket archives-then-drops to it, but ONLY once its bintrail_id
// is resolved (read from stream_state) — until then it rotates drop-only and
// the engine's protect-unarchived guard keeps it from losing data.
func rotateTargets(bootDSN string, sup *monitorSupervisor, reg *console.Registry, stagingBase string) []rotation.RotateTarget {
	targets := []rotation.RotateTarget{{DSN: bootDSN}}
	for _, j := range sup.ActiveJobs() {
		t := rotation.RotateTarget{DSN: j.IndexDSN}
		if entry, ok := reg.Get(j.EntryID); ok && entry.ArchiveS3 != "" {
			id, err := resolveBintrailIDFunc(j.IndexDSN)
			switch {
			case err != nil:
				// A real DB error reading stream_state must not masquerade as
				// "identity not yet resolved": that would let a permanently
				// stalled archive (bad perms, unreachable index) rotate drop-only
				// forever with only a Debug line. Surface it at Warn so an
				// operator can tell archiving stalled from archiving being off.
				slog.Warn("archive-to-S3 configured but reading the source's bintrail_id failed; rotating drop-only this cycle",
					"entry", j.EntryID, "error", err)
			case id == "":
				slog.Debug("archive-to-S3 configured but the source's bintrail_id is not yet resolved; rotating drop-only this cycle", "entry", j.EntryID)
			default:
				t.ArchiveS3 = entry.ArchiveS3
				t.ArchiveDir = filepath.Join(stagingBase, j.EntryID)
				t.BintrailID = id
				t.ArchiveCompression = "zstd"
			}
		}
		targets = append(targets, t)
	}
	return targets
}

// rotationSettingsProvider returns the live built-in-rotation settings: the
// console-saved global policy (registry envelope) when present and valid, else
// the daemon's --rotate-* flag/env defaults (upRotationCfg). StartLoop reads it
// fresh each cycle, so an edit from the console applies on the next tick without
// a restart. A saved policy that fails to parse — a bad hand-edit of the file —
// falls back to the defaults with a warning rather than silently disabling
// rotation. The boot-index/per-source targets are unaffected: this governs the
// daemon-global retain/interval/add-future only (the loop is one shared ticker).
func rotationSettingsProvider(reg *console.Registry) func() rotation.Settings {
	return func() rotation.Settings {
		if rc, ok := reg.Rotation(); ok {
			s, err := rotation.ParseSettings(rc.Retain, rc.Interval, rc.AddFuture, true)
			if err == nil && s.Enabled {
				return s
			}
			slog.Warn("built-in rotation: ignoring invalid saved console policy; using daemon defaults", "error", err)
		}
		return upRotationCfg
	}
}

// resolveBintrailIDFunc is the seam tests stub to avoid a real DB.
var resolveBintrailIDFunc = resolveBintrailID

// resolveBintrailID reads a source's resolved server identity from its index
// stream_state — the UUID archives are partitioned under (bintrail_id=<uuid>).
// Returns ("", nil) when the stream has not resolved its identity yet (no
// stream_state row, or a NULL/empty bintrail_id) — archiving waits a cycle.
// Returns ("", err) on a genuine failure (connect or query error) so the caller
// can distinguish a transient/persistent fault from "not yet resolved" and log
// it loudly rather than letting a stalled archive look like a normal wait.
func resolveBintrailID(indexDSN string) (string, error) {
	db, err := config.Connect(indexDSN)
	if err != nil {
		return "", fmt.Errorf("connect: %w", err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var id sql.NullString
	if err := db.QueryRowContext(ctx, "SELECT bintrail_id FROM stream_state WHERE id = 1").Scan(&id); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil // no checkpoint row yet — identity not resolved, not a fault
		}
		return "", fmt.Errorf("read stream_state: %w", err)
	}
	return id.String, nil
}
