package consoleapp

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"sort"
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
	"github.com/dbtrail/dbtrail/internal/installid"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/observe"
	"github.com/dbtrail/dbtrail/internal/readrouter"
	"github.com/dbtrail/dbtrail/internal/rotation"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
	"github.com/dbtrail/dbtrail/internal/status"
	"github.com/dbtrail/dbtrail/internal/streamdeps"
	"github.com/dbtrail/dbtrail/internal/streamrun"
)

var watchCmd = &cobra.Command{
	Use:   "watch",
	Short: "Watch one or more MySQL servers: stream + web interface + control plane in one daemon",
	Long: `Runs the combined capture-and-observe daemon (the standalone successor to
'bintrail up --console'): preflight checks, index initialization, the live
replication stream, AND the read-only web interface with its control plane,
all in one process sharing one SIGINT/SIGTERM lifecycle.

--source-dsn is optional: without it the daemon starts source-less (the
zero-config install) serving the web interface + control plane only, and sources
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
	upSourceFlavor          string
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
	upBaselineTableDeltas   bool
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
	// upSQLMaxInFlight caps how many SQL-on-the-copy statements run at once
	// (#2030): the SQL card and the embedded port together, each a worker
	// with its own threads and memory beside capture. Env
	// BINTRAIL_CONSOLE_SQL_MAX_IN_FLIGHT; below 1 refuses to start.
	upSQLMaxInFlight = sqlsandbox.DefaultMaxInFlight
	// upSQLMemory is --sql-memory as given (#2210); upSQLMemoryLimit is what
	// resolveSQLMemory made of it for DuckDB, "" for the sandbox default.
	upSQLMemory, upSQLMemoryLimit string
	// upSQLPortMaxRows is --sql-port-max-rows: the most rows one statement
	// returns on the --flashback-listen port.
	upSQLPortMaxRows = console.DefaultSQLPortMaxRows
	// upRouteMaxCopyAge turns read routing on the port on (#2038): MySQL
	// answers by default, the copy takes expensive SELECTs while its snapshot
	// is at most this old, and past that the ones over tables unchanged since
	// their snapshot (#2085). Zero (default) = copy-only port, as before.
	// Env BINTRAIL_CONSOLE_ROUTE_MAX_COPY_AGE.
	upRouteMaxCopyAge time.Duration
	// upRouteCostThreshold / upRouteScanRows are the EXPLAIN thresholds read
	// routing decides on (readrouter.Policy). Env
	// BINTRAIL_CONSOLE_ROUTE_COST_THRESHOLD / BINTRAIL_CONSOLE_ROUTE_SCAN_ROWS.
	upRouteCostThreshold float64
	upRouteScanRows      int64
	// upRouteReadOnly makes the routed port read-only (#2079): a statement
	// that is not a read is refused and never sent to the source. Only
	// meaningful with read routing on. Env BINTRAIL_CONSOLE_ROUTE_READ_ONLY.
	upRouteReadOnly   bool
	upArchiveStageDir string
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
	// upConsoleBaselineLockModeSet says the variable set a valid mode: an
	// operator's choice, which every dump uses as is. Unset, the mode is
	// automatic (#1986): lock-all for an RDS/Aurora host, ftwrl elsewhere,
	// with one retry in lock-all when ftwrl is refused.
	upConsoleBaselineLockModeSet bool
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
	{"source-flavor", "BINTRAIL_SOURCE_FLAVOR"},
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
	watchCmd.Flags().StringVar(&upSourceFlavor, "source-flavor", "", "Flavor of the --source-dsn server: mysql or mariadb. Empty (default) detects it from the server; a value the server contradicts refuses to start")
	watchCmd.Flags().StringVar(&upIndexDSN, "index-dsn", "", "DSN for the index MySQL database (required)")
	watchCmd.Flags().Uint32Var(&upServerID, "server-id", 0, "MySQL replica server ID (default: hash of source host:user:dbname)")
	watchCmd.Flags().StringVar(&upSchemas, "schemas", "", "Comma-separated schemas to index (default: all user schemas)")
	watchCmd.Flags().StringVar(&upTables, "tables", "", "Comma-separated tables to index (default: all)")
	watchCmd.Flags().IntVar(&upBatchSize, "batch-size", 1000, indexer.BatchSizeHelp())
	watchCmd.Flags().DurationVar(&indexer.WriteTimeout, "write-timeout", indexer.DefaultWriteTimeout, "Deadline for each index write (batch INSERT, checkpoint, digest lookup). A mid-statement network stall surfaces as an error within this window instead of freezing the daemon on kernel TCP retransmission (~13-16 min). Raise for very large batches over a slow link")
	watchCmd.Flags().DurationVar(&streamrun.ResumeCleanupWait, "cleanup-wait-timeout", streamrun.DefaultResumeCleanupWait, "How long a start waits for a resume cleanup that a previous run left executing on the index, before it fails and names it. A second cleanup sent while the first still runs only fails on its locks. 0 starts the cleanup without looking")
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
	watchCmd.Flags().StringVar(&upConsoleListen, "console-listen", "127.0.0.1:8090", "Bind address for the web interface")
	watchCmd.Flags().StringVar(&upConsoleToken, "console-token", "", "Opt-in static token for API automation (never generated; humans use the password)")
	watchCmd.Flags().StringVar(&upConsoleBaselineDir, "baseline-dir", "", "Local directory of baseline Parquet snapshots; enables the web interface's point-in-time Reconstruct surface")
	watchCmd.Flags().StringVar(&upConsoleBaselineS3, "baseline-s3", "", "S3 prefix of baseline Parquet snapshots (s3://bucket/prefix/); enables Reconstruct")
	watchCmd.Flags().BoolVar(&upBaselineCarryForward, "baseline-carry-forward-unchanged", true,
		"When a refresh finds a table had no changes, publish its previous Parquet file instead of rewriting "+
			"it (hard link where possible). On by default since #1681: the rows are identical either way, and "+
			"reuse never publishes a table it should not — a destructive DDL or a stale schema snapshot refuses "+
			"the snapshot before it, a known capture gap makes the table ineligible so it is folded as usual, and a "+
			"failed _MANIFEST check fails the run. It links two snapshots to one file, so disk-usage and prune figures then "+
			"count space they will not reclaim while the newer snapshot references it. The web interface no "+
			"longer asks. --baseline-carry-forward-unchanged=false turns off THIS path; with "+
			"--baseline-table-deltas on (the default) a table that did not change is still published by linking "+
			"its previous file, so writing every table again needs both turned off.")
	watchCmd.Flags().BoolVar(&upBaselineTableDeltas, "baseline-table-deltas", true,
		"On by default (--baseline-table-deltas=false or BINTRAIL_BASELINE_TABLE_DELTAS=false turns it off). A refresh does not rewrite a table that changed: it keeps the previous Parquet file and writes that refresh's changed rows as one numbered "+
			"pair of small files beside it (<table>.000001.posdel, <table>.000001.upserts, then 000002, ...), linking the earlier pairs forward, and writes the table again in full when the chain's files together pass half of its size, the chain is a "+
			"day old, or the chain's start comes close to the oldest events the index keeps. The generated DuckDB views read the chain; every other reader uses the table file and the index, as before. A snapshot written this way "+
			"must not be read by a bintrail older than this one. Turning it off needs nothing else: the next refresh writes every table in full. A DuckDB view that follows the newest snapshot keeps working when a pair appears or goes away, except over a table whose schema it could not read, or when another table in the same schema is named after it plus a dot (orders and orders.old): the generated file names those, and they refuse once a pair appears, until the views are generated again.")
	watchCmd.Flags().StringVar(&upBaselineRefreshEvery, "baseline-refresh-interval", "", "Periodically refresh each server's newest baseline snapshot from the index (Nm/Nh/Nd; default: off). Runs with the conservative DuckDB budget, folds at most 2 tables at a time, and never publishes over a known capture gap.")
	watchCmd.Flags().StringVar(&upConsoleBaselineRetain, "baseline-retain", "", "Periodically prune local --baseline-dir snapshots older than this (Nd/Nh) once a durable copy exists in --baseline-s3 (never deletes the only copy or the newest snapshot per table)")
	watchCmd.Flags().StringVar(&upConsoleServersFile, "console-servers-file", "", "Path to the server registry YAML managed by the web interface (default ~/.config/bintrail/console-servers.yaml)")
	watchCmd.Flags().StringVar(&upConsoleAuthFile, "console-auth-file", "", "Path to the web interface auth file enabling password login (default ~/.config/bintrail/console-auth.yaml; created with `bintrail-console user set-password`)")
	watchCmd.Flags().StringVar(&upConsoleMCPTokenFile, "console-mcp-token-file", "", "Path to the managed MCP token file written by Settings → MCP Server (default ~/.config/bintrail/console-mcp-token.yaml). Point it at persistent storage when the daemon runs in a container.")
	watchCmd.Flags().StringVar(&upConsoleTLSCert, "console-tls-cert", "", "TLS certificate file (PEM); serve the web interface over HTTPS (requires --console-tls-key)")
	watchCmd.Flags().StringVar(&upConsoleTLSKey, "console-tls-key", "", "TLS private key file (PEM; requires --console-tls-cert)")
	watchCmd.Flags().StringSliceVar(&upConsoleAllowedHost, "console-allowed-hosts", nil, "Extra hostnames allowed in the Host header (for a TLS-terminating reverse proxy); IP literals and localhost are always allowed")
	watchCmd.Flags().BoolVar(&upConsoleAllowSetup, "console-allow-setup", false, "Allow browser first-run password setup on a non-loopback bind (assert the bind is access-controlled, e.g. published only on the host loopback)")
	watchCmd.Flags().IntVar(&upSQLMaxInFlight, "sql-max-in-flight", sqlsandbox.DefaultMaxInFlight, "How many SQL-on-the-copy statements run at once, the SQL card and the --flashback-listen port together; one more waits up to 30 s for a free slot, then is refused. Each runs as its own process with 2 threads and up to --sql-memory, on the host that captures, and every result is held in this process while it is sent, so raise it only with cores and memory to spare. Env BINTRAIL_CONSOLE_SQL_MAX_IN_FLIGHT.")
	watchCmd.Flags().IntVar(&upSQLPortMaxRows, "sql-port-max-rows", console.DefaultSQLPortMaxRows, "Most rows one statement returns on the --flashback-listen port; a result with more is refused with error 1104, not cut. The SQL page of the web interface shows 1,000 rows whatever this is. A result is also refused past 64 MB. A result is held in this process, which captures, while it is sent, taking a few times its size while it is prepared; the port holds at most 256 MB of results being sent, over all its connections, plus one result, and past that refuses a statement with error 1203 until an answer has been written to its client. Env BINTRAIL_CONSOLE_SQL_PORT_MAX_ROWS.")
	watchCmd.Flags().StringVar(&upSQLMemory, "sql-memory", "", "Memory each SQL-on-the-copy statement may use (the SQL card and the --flashback-listen port), e.g. 4GB; default 2GB, at least 512MB. It runs on the host that captures, times --sql-max-in-flight at once. A table with more changes not yet merged than this memory can merge (384 MB at 2 GB, in proportion) is answered from an earlier copy, or refused with a pointer to your own DuckDB when none fits. Past its memory a statement spills to a temporary folder, up to four times it. Env BINTRAIL_CONSOLE_SQL_MEMORY. Unset, the web interface can set it (Settings, MCP Server); set, it wins there.")
	watchCmd.Flags().StringVar(&upConsoleFlashbackListen, "flashback-listen", "", "Serve an embedded MySQL-protocol time-travel port (_flashback/_snapshot/_diff) for every monitored server, routed by the connection username (server id or name); e.g. 127.0.0.1:3308. Requires --console-token, which reads every schema of every server: the port does not filter by schema. Empty = the web interface decides (Connect turns the port on and off, with its own password; off until then). Set, this address decides and the web interface cannot change it. Env BINTRAIL_CONSOLE_FLASHBACK_LISTEN.")
	watchCmd.Flags().DurationVar(&upRouteMaxCopyAge, "route-max-copy-age", 0, "Experimental read routing on the --flashback-listen port: forward every statement, WRITES INCLUDED, to the server's source MySQL with the server's forwarding account when it has one (set on the server, in the web interface or as route_user / route_password in the API) and with its source account otherwise, except SELECTs whose EXPLAIN FORMAT=JSON says they are expensive (see --route-cost-threshold, --route-scan-rows), which run on the copy while its snapshot is at most this old; past that age the copy still answers one whose tables have had no change since their snapshot, as long as capture is known to have read everything the source had written at some moment within this same limit; a statement the copy rejects runs on MySQL. Anyone holding the access token can then do on the source whatever that account can: give the port its own account with SELECT only, and set --route-read-only. 0 = off (the port serves the copy only). Env BINTRAIL_CONSOLE_ROUTE_MAX_COPY_AGE.")
	watchCmd.Flags().BoolVar(&upRouteReadOnly, "route-read-only", false, "Read routing: refuse every statement that is not a read (INSERT, UPDATE, DELETE, DDL, GRANT, KILL, SET GLOBAL, SELECT ... INTO OUTFILE, SELECT ... FOR UPDATE, more than one statement in a line and anything else not recognised as a read) with an error that names this flag, and never send it to the source. SELECT, SHOW, DESCRIBE, EXPLAIN, USE, session SETs and transaction control keep working. It reads the statement's text, so it cannot see a stored function that writes: the source account's grants are the guard for that. Requires --route-max-copy-age. Env BINTRAIL_CONSOLE_ROUTE_READ_ONLY.")
	watchCmd.Flags().Float64Var(&upRouteCostThreshold, "route-cost-threshold", readrouter.DefaultPolicy().CostThreshold, "Read routing: a SELECT whose plan query_cost is at least this goes to the copy (a point lookup costs about 1; a full scan over 200k rows about 20000). 0 disables the cost rule. Env BINTRAIL_CONSOLE_ROUTE_COST_THRESHOLD.")
	watchCmd.Flags().Int64Var(&upRouteScanRows, "route-scan-rows", readrouter.DefaultPolicy().ScanRows, "Read routing: a SELECT whose plan has a full table scan over at least this many rows goes to the copy (on a MariaDB source, whose plans carry no comparable cost, a full index scan too, and a plan whose joins read at least this many rows in all). 0 disables the scan rule. Env BINTRAIL_CONSOLE_ROUTE_SCAN_ROWS.")
	watchCmd.Flags().StringVar(&upArchiveStageDir, "archive-staging-dir", "", "Local staging directory for S3 archive uploads (default: OS temp dir). Rotated Parquet is written here, uploaded to a source's configured Archive S3 bucket, then pruned.")
	watchCmd.Flags().StringVar(&upRotateRetain, "rotate-retain", indexer.DefaultRotateRetain, "Built-in rotation: drop index partitions older than this (Nd/Nh; \"off\" disables)")
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
	if _, err := metadata.NormalizeDeclaredFlavor(upSourceFlavor); err != nil {
		return fmt.Errorf("--source-flavor: %w", err)
	}
	var err error
	upRotationCfg, err = rotation.ParseSettings(upRotateRetain, upRotateInterval, upRotateAddFuture,
		cmd.Flags().Changed("rotate-retain"))
	if err != nil {
		return err
	}

	if err := resolveSQLMemory(cmd); err != nil {
		return err
	}
	if err := resolveSQLMaxInFlight(cmd); err != nil {
		return err
	}
	if err := resolveSQLPortMaxRows(cmd); err != nil {
		return err
	}
	// The port's flags, checked here, before the first connection: a
	// read-only flag that cannot apply, or an environment value that does
	// not parse, must not wait for the index to answer.
	if err := resolveRouteFlags(cmd); err != nil {
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
		fmt.Fprintln(os.Stderr, "No source configured yet; the preflight runs when you add a server from the web interface.")
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
		preflight := doctor.Build(cmd.Context(), upSourceDSN, upIndexDSN, upSchemas, upRotationCfg.Retain,
			doctor.WithSourceSSL(bootSourceSSL()))
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
		fmt.Fprintln(os.Stderr, "=== Phase 3/3: Web interface + control plane ===")
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
//
// A missing primary key or a table not on InnoDB is fatal here as in core
// `up`: it FAILS only while the index's first snapshot is pending (#1766), and
// then the main stream's own snapshot refuses a moment later, which ends this
// daemon too (only a write-deadline error restarts it). Refusing at the
// preflight says why, with the fix.
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
	registry, err := loadConsoleRegistry(serversPath, upIndexDSN != "" && upBaselineRefreshEvery != "", upConsoleBaselineDir, upConsoleBaselineS3)
	if err != nil {
		return fmt.Errorf("console: %w", err)
	}

	cfg, err := upConsoleConfig(db, upIndexDSN, upConsoleOpts(), registry)
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
	sweepSQLExportStaging(baselineStagingDirFor(registry))
	var baselineSup *baselineSupervisor
	if upConsoleBaselineTrigger || upBaselineRefreshEvery != "" {
		baselineSup = newBaselineSupervisorFromConfig(ctx, baselineStagingDirFor(registry), registry)
		// The retention the #1689 gate bounds a skipped cycle against. Wired
		// here rather than taken as a constructor argument because it is the
		// same provider rotation reads, and because a supervisor without one
		// still works: no policy cap, the observed partitions as the only
		// bound.
		// Before StartLoop below, which followRotation depends on: nothing
		// can save a policy in between, the console is not serving yet.
		baselineSup.followRotation(rotationSettingsProvider(registry))
	}
	if upConsoleBaselineTrigger {
		cfg.BaselineCtrl = baselineSup
	}
	if upBaselineRefreshEvery != "" {
		cfg.BaselineRefresh = baselineSup
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
		// No source streams into the boot index here (#1715).
		return rotateTargets(upIndexDSN, bootIdle, supervisor, registry, archiveStagingDir())
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
	// Per-server backup schedules from the Snapshots page (#1442). No flag:
	// the registry decides what runs, the loop only looks at the clock.
	startBackupScheduleLoop(ctx, backupSched)

	// Wire the live telemetry client so the console's opt-out toggle stops this
	// running daemon's beacons immediately, not just on the next start.
	cfg.Telemetry = tel.Client()
	srv, err := console.New(cfg)
	if err != nil {
		return err
	}
	wireSQLChainLine(baselineSup, srv)
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
	printConsoleBanner(srv, "The DBTrail web interface is running: open it and add the MySQL servers to watch:")
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
// validateRoutePolicy refuses a read-routing setup under which no statement
// could ever reach the copy: routing on (a max copy age) with both plan
// thresholds at 0. Every statement would land under cheap_plan, which reads
// as a tuning question rather than the misconfiguration it is.
// resolveRouteFlags applies the environment to the MySQL-protocol port's
// flags (--flashback-listen and the --route-* family) and checks them
// together. It opens nothing and starts nothing, so runWatch calls it before
// the first connection: a typo in a safety setting stops the daemon at once,
// not after the index answered. resolveUpConsoleEnv calls it again from the
// console paths; it is idempotent.
func resolveRouteFlags(cmd *cobra.Command) error {
	if !cmd.Flags().Changed("flashback-listen") {
		if v := os.Getenv("BINTRAIL_CONSOLE_FLASHBACK_LISTEN"); v != "" {
			upConsoleFlashbackListen = v
		}
	}
	if !cmd.Flags().Changed("route-max-copy-age") {
		if v := os.Getenv("BINTRAIL_CONSOLE_ROUTE_MAX_COPY_AGE"); v != "" {
			d, err := time.ParseDuration(v)
			if err != nil {
				return fmt.Errorf("BINTRAIL_CONSOLE_ROUTE_MAX_COPY_AGE: %w", err)
			}
			upRouteMaxCopyAge = d
		}
	}
	if !cmd.Flags().Changed("route-read-only") {
		if v := os.Getenv("BINTRAIL_CONSOLE_ROUTE_READ_ONLY"); v != "" {
			b, err := strconv.ParseBool(v)
			if err != nil {
				return fmt.Errorf("BINTRAIL_CONSOLE_ROUTE_READ_ONLY=%q is not a yes or no: use 1 or true to make the routed port read-only, 0 or false (or leave it unset) for read-write", v)
			}
			upRouteReadOnly = b
		}
	}
	if !cmd.Flags().Changed("route-cost-threshold") {
		if v := os.Getenv("BINTRAIL_CONSOLE_ROUTE_COST_THRESHOLD"); v != "" {
			f, err := strconv.ParseFloat(v, 64)
			if err != nil {
				return fmt.Errorf("BINTRAIL_CONSOLE_ROUTE_COST_THRESHOLD: %w", err)
			}
			upRouteCostThreshold = f
		}
	}
	if !cmd.Flags().Changed("route-scan-rows") {
		if v := os.Getenv("BINTRAIL_CONSOLE_ROUTE_SCAN_ROWS"); v != "" {
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return fmt.Errorf("BINTRAIL_CONSOLE_ROUTE_SCAN_ROWS: %w", err)
			}
			upRouteScanRows = n
		}
	}
	if err := validateRouteReadOnly(upRouteReadOnly, upConsoleFlashbackListen, upRouteMaxCopyAge); err != nil {
		return err
	}
	if upConsoleFlashbackListen != "" {
		return validateRoutePolicy(upRouteMaxCopyAge, upRouteCostThreshold, upRouteScanRows)
	}
	return nil
}

// flashbackConfigFromFlags is the port's serving configuration as the
// resolved flags give it: what startFlashbackPort serves with.
func flashbackConfigFromFlags() flashbackConfig {
	return flashbackConfig{
		RouteMaxCopyAge: upRouteMaxCopyAge,
		RoutePolicy:     readrouter.Policy{CostThreshold: upRouteCostThreshold, ScanRows: upRouteScanRows},
		RouteReadOnly:   upRouteReadOnly,
	}
}

// validateRouteReadOnly refuses --route-read-only where it would guard
// nothing: without the port, or with the port serving the copy only (routing
// off sends nothing to the source). A safety flag that is silently ignored
// reads as protection that is not there.
func validateRouteReadOnly(readOnly bool, flashbackListen string, maxCopyAge time.Duration) error {
	switch {
	case !readOnly:
		return nil
	case flashbackListen == "":
		return fmt.Errorf("--route-read-only applies to the MySQL-protocol port, which is off: set --flashback-listen and --route-max-copy-age too, or drop --route-read-only")
	case maxCopyAge <= 0:
		return fmt.Errorf("--route-read-only applies to read routing, which is off: set --route-max-copy-age too, or drop --route-read-only (with routing off the port serves the copy only and sends nothing to the source)")
	}
	return nil
}

func validateRoutePolicy(maxCopyAge time.Duration, costThreshold float64, scanRows int64) error {
	if maxCopyAge > 0 && costThreshold <= 0 && scanRows <= 0 {
		return fmt.Errorf("read routing: --route-cost-threshold and --route-scan-rows are both 0, so no statement could ever go to the copy; set one of them, or drop --route-max-copy-age to serve the copy only")
	}
	return nil
}

// routeStartupLine is the stderr line that says, at startup, what read
// routing sends to the source and whether the port is read-only.
func routeStartupLine(cfg flashbackConfig) string {
	rule := fmt.Sprintf("SELECTs with plan cost >= %.0f or a full scan over >= %d rows (on a MariaDB source, also joins that read that many rows in all) run on the copy while its snapshot is at most %s old, or over tables with no change since their snapshot", cfg.RoutePolicy.CostThreshold, cfg.RoutePolicy.ScanRows, cfg.RouteMaxCopyAge)
	if cfg.RouteReadOnly {
		return "Read routing (experimental) is on, read-only (--route-read-only): reads go to each server's source MySQL with that server's forwarding account when it has one, else with its source account; " + rule + ". A statement that is not a read is refused and never sent to the source. The check reads the statement's text: what a stored function does when a SELECT calls it is up to that account's grants.\n"
	}
	return "Read routing (experimental) is on, read-write: statements, writes included, go to each server's source MySQL with that server's forwarding account when it has one, else with its source account; " + rule + ". Anyone holding the access token can do on each source what that account can. Set --route-read-only to refuse writes, and a forwarding account on each server to bound the rest.\n"
}

// fast, exactly like the console bind. Serving runs on the daemon context: ctx
// cancellation closes the listener and drains open connections. A mid-run crash
// is logged, never propagated — the flashback port is strictly secondary to the
// console and the capture stream.
func startFlashbackPort(ctx context.Context, srv *console.Server) (func(), error) {
	cfg := flashbackConfigFromFlags()
	control := &flashbackControl{ctx: ctx, srv: srv, cfg: cfg}
	routeErr := validateRoutePolicy(upRouteMaxCopyAge, upRouteCostThreshold, upRouteScanRows)
	if upConsoleFlashbackListen == "" {
		// No address at startup: the web interface decides (#2101), so the
		// port is not "off" for --route-read-only. The flag still guards
		// nothing with routing off, whoever turns the port on.
		if err := validateRouteReadOnly(upRouteReadOnly, "decided in the web interface", upRouteMaxCopyAge); err != nil {
			return nil, err
		}
		// A port saved as on comes up here; one that cannot (its address was
		// taken, the routing thresholds contradict each other) is reported in
		// the web interface and the log and does NOT stop the daemon, which
		// is also the capture process.
		control.cfgErr = routeErr
		srv.ManageFlashback(control)
		if addr := control.boundAddr(); addr != nil {
			fmt.Fprintf(os.Stderr, "Time-travel SQL (MySQL protocol) is listening on %s, as saved in the web interface; connect a MySQL client with user=<server id or name> and the port password created there.\n", addr)
			if cfg.RouteMaxCopyAge > 0 {
				fmt.Fprint(os.Stderr, routeStartupLine(cfg))
			}
		}
		return control.Close, nil
	}
	if err := validateRouteReadOnly(upRouteReadOnly, upConsoleFlashbackListen, upRouteMaxCopyAge); err != nil {
		return nil, err
	}
	if srv.Token() == "" {
		return nil, fmt.Errorf("--flashback-listen %s requires the access token: set --console-token or BINTRAIL_CONSOLE_TOKEN (MySQL-protocol auth cannot use the web interface password)", upConsoleFlashbackListen)
	}
	if routeErr != nil {
		return nil, routeErr
	}
	if err := control.Apply(upConsoleFlashbackListen); err != nil {
		return nil, fmt.Errorf("flashback: %w", err)
	}
	fmt.Fprintf(os.Stderr, "Time-travel SQL (MySQL protocol) is listening on %s; connect a MySQL client with user=<server id or name>, password=<access token> (--console-token).\n", control.boundAddr())
	if cfg.RouteMaxCopyAge > 0 {
		fmt.Fprint(os.Stderr, routeStartupLine(cfg))
	}
	return control.Close, nil
}

// runUpStreamWithConsole serves the read-only web console in this same process,
// alongside the live stream. Both share one SIGINT/SIGTERM lifecycle: the signal
// cancels the context the console runs on, and the main stream runs on the same
// context, so a single Ctrl-C drains both. The console gets its own index DB
// connection (the stream owns its own).
func runUpStreamWithConsole(cmd *cobra.Command, args []string) error {
	serverID := upServerID
	if serverID == 0 {
		id, err := autoServerID(cmd.Context(), os.Stderr)
		if err != nil {
			return err
		}
		serverID = id
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
	registry, err := loadConsoleRegistry(serversPath, upIndexDSN != "" && upBaselineRefreshEvery != "", upConsoleBaselineDir, upConsoleBaselineS3)
	if err != nil {
		return fmt.Errorf("console: %w", err)
	}

	// The flavor the main stream runs as, for the console's boot entry.
	mainFlavor := newSourceFlavorCell(upSourceFlavor)
	cfg, err := upConsoleConfigFor(db, upIndexDSN, upConsoleOpts(), registry, mainFlavor)
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
	sweepSQLExportStaging(baselineStagingDirFor(registry))
	var baselineSup *baselineSupervisor
	if upConsoleBaselineTrigger || upBaselineRefreshEvery != "" {
		baselineSup = newBaselineSupervisorFromConfig(ctx, baselineStagingDirFor(registry), registry)
		// The retention the #1689 gate bounds a skipped cycle against. Wired
		// here rather than taken as a constructor argument because it is the
		// same provider rotation reads, and because a supervisor without one
		// still works: no policy cap, the observed partitions as the only
		// bound.
		// Before StartLoop below, which followRotation depends on: nothing
		// can save a policy in between, the console is not serving yet.
		baselineSup.followRotation(rotationSettingsProvider(registry))
	}
	if upConsoleBaselineTrigger {
		cfg.BaselineCtrl = baselineSup
	}
	if upBaselineRefreshEvery != "" {
		cfg.BaselineRefresh = baselineSup
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
		// The main stream writes the boot index.
		return rotateTargets(upIndexDSN, bootStreamed, supervisor, registry, archiveStagingDir())
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
	// Per-server backup schedules from the Snapshots page (#1442). No flag:
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
	wireSQLChainLine(baselineSup, srv)

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
	printConsoleBanner(srv, "The DBTrail web interface (read-only) is running. Open:")

	// Resume whatever the operator had monitoring before the restart —
	// desired state lives in the registry, positions in each per-source
	// stream_state checkpoint.
	go supervisor.Reconcile(registry)

	// Extension source jobs (ext.RegisterSourceJob) for the daemon's MAIN source
	// run alongside its stream — the same secondary, never-fatal contract as
	// `bintrail up` (cliapp/up.go). The supervised registry sources get their
	// own jobs from the monitor supervisor (consoleapp/monitor.go); this covers
	// only the single main source `watch --source-dsn` streams. They start once
	// the stream has asked the source what it is, with the flavor capture runs
	// as; FlavorOnce keeps a write-deadline restart from starting them again.
	// Every resolution also updates the flavor the console shows for the boot
	// entry. No-op in the stock binary.
	//
	// Only while this process holds the index database's capture lock: a
	// second daemon with the same source and index waits instead of capturing
	// beside this one (#2105). The jobs are bound to one tenure of that lock
	// (the ctx the closure receives), not to the daemon: a process that lost
	// the lock must stop them too, or they keep writing beside the process
	// that holds it now. A new tenure starts them again.
	baseStreamCfg := watchStreamConfig(serverID)
	streamErr := runMainStreamHoldingLock(ctx, upIndexDSN, func(ctx context.Context) error {
		streamCfg := baseStreamCfg
		streamCfg.Hooks = &streamrun.Hooks{OnFlavorResolved: mainStreamFlavorHook(mainFlavor, func(flavor string) {
			ext.RunSourceJobs(ctx, mainSourceJobInfo(upSourceDSN, upIndexDSN, flavor))
		})}
		return runMainStreamWithWriteDeadlineRetry(ctx, streamCfg)
	})
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
		Flavor:     upSourceFlavor,
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
// source. flavor is the one the main stream resolved (OnFlavorResolved), never
// the declared --source-flavor, which is empty when detection decides.
// Its SourceTLS is the TLS the main capture connects with (bootSourceSSL,
// from --ssl-*), so a job opens the source the way capture does.
func mainSourceJobInfo(sourceDSN, indexDSN, flavor string) ext.SourceJobInfo {
	return ext.SourceJobInfo{SourceDSN: sourceDSN, IndexDSN: indexDSN, Flavor: flavor, SourceTLS: ext.SourceTLS(bootSourceSSL())}
}

// entrySourceJobInfo builds the ext.SourceJobInfo for a supervised registry
// source: the entry's DSNs and its source TLS (SourceSSL), with the flavor
// capture runs as.
func entrySourceJobInfo(e console.ServerEntry, flavor string) ext.SourceJobInfo {
	return ext.SourceJobInfo{SourceDSN: e.SourceDSN, IndexDSN: e.DSN, Flavor: flavor, SourceTLS: ext.SourceTLS(e.SourceSSL())}
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
func newBaselineSupervisorFromConfig(ctx context.Context, stagingDir string, reg *console.Registry) *baselineSupervisor {
	sup := newBaselineSupervisor(ctx, stagingDir, upConsoleBaselineLockMode)
	sup.configErr = upConsoleBaselineLockModeErr
	sup.lockModeChosen = upConsoleBaselineLockModeSet
	// The settings store the lock mode is re-read from per job (#1682).
	sup.reg = reg
	sup.tableDeltas = upBaselineTableDeltas
	if upBaselineTableDeltas {
		// Once per boot, because the failure it prevents is silent: a DuckDB
		// view generated before this was on reads a table's file alone, and
		// from the first refresh that file is the table as it was when its
		// chain of deltas started.
		slog.Warn("table deltas are ON: a refresh keeps a changed table's file and writes its changes beside it. " +
			"DuckDB views generated before this was turned on must be generated again, or they show tables as of the last full rewrite. " +
			"The views.sql inside each snapshot, the download in the web interface and the SQL panel are generated per snapshot and need nothing.")
	}
	// Only the creation opt-in runs mydumper; a refresh-only daemon never does,
	// and a lock-mode typo already has its own refusal (configErr).
	if upConsoleBaselineTrigger && upConsoleBaselineLockModeErr == nil {
		src := lockModeAutomatic
		if upConsoleBaselineLockModeSet {
			src = lockModeFromEnv
		}
		if msg := mydumperBootWarning(upConsoleBaselineLockMode, src); msg != "" {
			slog.Warn("console: " + msg)
		}
	}
	// The download TTL for staged .sql builds (#1448) needs a clock nobody
	// is polling: the Snapshots page expires lazily only while it is open.
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
	if !cmd.Flags().Changed("baseline-table-deltas") {
		upBaselineTableDeltas = envBoolOr("BINTRAIL_BASELINE_TABLE_DELTAS", upBaselineTableDeltas)
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
	if err := resolveRouteFlags(cmd); err != nil {
		return err
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
	resolveBaselineLockModeEnv()
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

// baselineStagingDirFor resolves the working folder (#1682, #2255): a folder
// saved in the web interface wins, then BINTRAIL_CONSOLE_BASELINE_STAGING,
// and with neither set the default beside the servers file
// (defaultWorkingFolder, which keeps the temp folder where that one cannot
// hold a dump).
//
// Resolved at BOOT rather than per job on purpose: the directory is swept
// once at startup for what a previous process left behind, so a switch while
// running would leave those files with nothing looking at them again. The row
// on the page says "restart to change" for that reason, and saying it per row
// is what keeps the page honest about the difference.
func baselineStagingDirFor(reg *console.Registry) string {
	if set := effectiveStagingDir(reg, upBaselineStageDir); set != "" {
		return set
	}
	return defaultWorkingFolderOnce(reg.Path()).Dir
}

// bootWorkingFolderDefault is the default working folder for the page, and
// the startup line that says so when it is the temp folder. Empty when a
// folder is set: the default is then not in use, and working it out would
// create a folder nothing writes to.
func bootWorkingFolderDefault(reg *console.Registry) workingFolderDefault {
	if effectiveStagingDir(reg, upBaselineStageDir) != "" {
		return workingFolderDefault{}
	}
	d := defaultWorkingFolderOnce(reg.Path())
	const fix = "set the Working folder in the web interface's backup settings, or BINTRAIL_CONSOLE_BASELINE_STAGING, to a disk with room, and restart"
	switch {
	case d.InMemory:
		slog.Warn("working folder: no folder is set, and full reads will write their dump into memory: the system temp folder is RAM on this host",
			"folder", d.Dir, "why", d.Why, "fix", fix)
	case d.Unusable:
		slog.Warn("working folder: no folder is set, and the folder beside DBTrail's data cannot be used, so full reads write under the system temp folder",
			"folder", d.Dir, "why", d.Why, "fix", fix)
	case d.Why != "":
		slog.Info("working folder: no folder is set, and full reads write under the system temp folder",
			"folder", d.Dir, "why", d.Why, "to_change", fix)
	default:
		slog.Info("working folder: no folder is set, so full reads write beside DBTrail's data", "folder", d.Dir)
	}
	return d
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
		startVerifyLoop(ctx, sup, registry, history, interval, func() []string { return effectiveVerifyTables(registry, upVerifyTables) })
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
	// A run whose only findings are differences over a snapshot read with no
	// locks (#1380) is all-inconclusive and still publishes: a difference was
	// found, and the alert must see it.
	return rec.State == "succeeded" && s.Total > 0 && (s.Inconclusive < s.Total || s.InconclusiveDiffers > 0)
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
	// The tables that differ from a snapshot read with no locks count in the
	// mismatch series too (#1380), so a rule on mismatch does not read zero
	// while one stands; "differs" carries them on their own.
	observe.SetVerifyOutcome(server, finished, s.Match, s.Mismatch+s.InconclusiveDiffers, s.Inconclusive, s.Error)
	observe.SetVerifyDiffers(server, s.InconclusiveDiffers)
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
func splitVerifyTables(raw string) []string { return console.SplitVerifyTables(raw) }

// startVerifyLoop runs one scheduled verification cycle per interval (#1191):
// every registry server, sequentially — one verify (one DuckDB budget) at a
// time. Mirrors startBaselinePruneLoop's shape: recover-guarded, first cycle
// shortly after startup, stops with the daemon context.
func startVerifyLoop(ctx context.Context, sup *verifySupervisor, registry *console.Registry, history *console.VerifyHistory, interval time.Duration, tables func() []string) {
	slog.Info("scheduled verification enabled", "interval", interval)
	go func() {
		if ctx.Err() == nil {
			runScheduledVerifyCycle(ctx, sup, registry, history, tables())
		}
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				runScheduledVerifyCycle(ctx, sup, registry, history, tables())
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
		slog.Warn("scheduled verify: no registry servers to verify. The schedule covers servers added in the web interface; a source configured only via command-line flags/env is not covered")
		return
	}
	for _, e := range entries {
		if ctx.Err() != nil {
			return
		}
		err := sup.RunScheduled(scheduledVerifyRequest(e, tables))
		if errors.Is(err, console.ErrVerifyRunning) {
			slog.Info("scheduled verify: skipped, a run is already in flight", "server", e.Name)
			recordVerifySkip(history, e, "a verify run was already in flight when the schedule fired")
		}
	}
}

// scheduledVerifyRequest picks the check a scheduled cycle runs for one
// server: baseline-anchored where the server has a baseline location of its
// own (#1684: the process-wide one backs no registry server any more), and
// the index-only recover-inputs check otherwise, so a server with no
// baseline is still verified rather than silently skipped.
func scheduledVerifyRequest(e console.ServerEntry, tables []string) console.VerifyRequest {
	dir, s3 := e.BaselineDir, e.BaselineS3
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
	// keepNewest > 0 marks a local-only target (#1681): no destination, so
	// the prune keeps the newest keepNewest snapshots instead of confirming
	// copies. s3 is empty on such a target.
	keepNewest int
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

// localKeepPruneTargets are the local-only targets (#1681): every folder
// console.LocalKeepTargets names, with the daemon's own --baseline-dir
// excluded (its snapshots follow the global rule). Sorted for a stable order.
func localKeepPruneTargets(entries []console.ServerEntry, globalDir string) []baselinePruneTarget {
	keep := console.LocalKeepTargets(entries, globalDir)
	dirs := make([]string, 0, len(keep))
	for d := range keep {
		dirs = append(dirs, d)
	}
	sort.Strings(dirs)
	out := make([]baselinePruneTarget, 0, len(dirs))
	for _, d := range dirs {
		out = append(out, baselinePruneTarget{dir: d, keepNewest: keep[d]})
	}
	return out
}

// runBaselinePruneCycle prunes each target via pruneFn, one (dir + S3) pair at a
// time. A failure on one target is logged and the rest still run — one bad dir
// must not strand the others. pruneFn is injected (baseline.PruneLocal in
// production) so the per-target iteration is unit-testable without S3.
func runBaselinePruneCycle(ctx context.Context, targets []baselinePruneTarget, retain time.Duration, pruneFn func(context.Context, baseline.PruneOptions) (baseline.PruneResult, error)) {
	for _, t := range targets {
		res, err := pruneFn(ctx, baseline.PruneOptions{
			LocalDir:   t.dir,
			S3URL:      t.s3,
			Retain:     retain,
			KeepNewest: t.keepNewest,
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
			slog.Warn("baseline prune: snapshots are old enough to reclaim but have no confirmed copy at the snapshot "+
				"destination, so they stay on this disk. Send them, or remove them by hand.",
				"dir", t.dir, "kept", res.KeptNotDurable, "destination", t.s3)
		}
		if res.ProbeErrors > 0 {
			slog.Warn("baseline prune: could not check whether some snapshots are at the snapshot destination, so they "+
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
	// The FLAG is validated first and still fails the daemon fast: a typo on
	// the command line is a misconfiguration the operator can see and fix
	// there, and swallowing it into "retention off" would turn a typo into a
	// disk that silently never gets reclaimed.
	if retainRaw != "" {
		if _, err := cliutil.ParseRetain(retainRaw); err != nil {
			return fmt.Errorf("--baseline-retain: %w", err)
		}
	}
	// The BOOT gate: with a registry the loop always runs (#1681), because
	// a server's keep-newest count is saved per server and can appear at any
	// time; without one it runs only for a configured age retention. What it
	// removes is decided per cycle: nothing, where no server has a count and
	// no retention is set. pruneLoopStarts is the same expression, so the
	// page's "restart to change" and the listing's retention line follow
	// what actually runs. A saved value this build cannot read has already
	// warned inside effectiveRetain; it leaves the age rule off rather than
	// failing the daemon, because a file edited by hand must never stop
	// capture.
	bootRetain, _ := effectiveRetain(reg, retainRaw)
	if !pruneLoopStarts(reg, bootRetain) {
		return nil // no registry and no retention — leave baselines untouched
	}
	if bootRetain > 0 && globalDir != "" && globalS3 == "" {
		// The operator pointed retention at a local dir with no durable S3 source;
		// warn once so the global dir not being reclaimed isn't a silent surprise.
		// Per-server registry targets (added at runtime) may still have both.
		slog.Warn("--baseline-retain is set and --baseline-dir is configured but --baseline-s3 is not; the global baseline dir will not be pruned (its snapshots are the only copy)")
	}
	if interval <= 0 {
		interval = time.Hour
	}
	slog.Info("baseline prune loop enabled", "retain", bootRetain, "interval", interval)
	runOnce := func() { baselinePruneSweep(ctx, reg, globalDir, globalS3, retainRaw, baseline.PruneLocal) }
	go func() {
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
	// SQLMemoryLimit is the resolved --sql-memory as DuckDB takes it
	// ("4096MiB"), "" for the default (#2210).
	SQLMemoryLimit string
	// SQLMaxInFlight is the resolved --sql-max-in-flight (#2030).
	SQLMaxInFlight int
	// SQLPortMaxRows is the resolved --sql-port-max-rows.
	SQLPortMaxRows int
	// ReadRouting is the port's read-routing policy (--route-max-copy-age and
	// the threshold flags, #2038), reported by GET /api/flashback; the port
	// itself is bound with the same values in startFlashbackPort.
	ReadRouting console.ReadRoutingConfig
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
		ReadRouting: console.ReadRoutingConfig{
			MaxCopyAge:    upRouteMaxCopyAge,
			CostThreshold: upRouteCostThreshold,
			ScanRows:      upRouteScanRows,
			ReadOnly:      upRouteReadOnly,
		},
		SQLMaxInFlight: upSQLMaxInFlight,
		SQLMemoryLimit: upSQLMemoryLimit,
		SQLPortMaxRows: upSQLPortMaxRows,
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

// bootCaptureFilter is the scope THIS daemon's own capture runs with, for the
// uncaptured-tables report (#1802). It is the only place that scope exists:
// the schema snapshot records the SCHEMAS capture reads, nothing anywhere
// records --tables, and a table that filter drops has its events dropped by
// the parser with no counter, no log line and nothing in capture_skips. A
// report that counted the snapshot instead would state full coverage over
// tables nobody is watching.
//
// nil when this daemon runs no capture of its own (source-less): it knows
// nothing then, and the report claims no count rather than guessing. The
// lists are parsed by the same helper the stream parses them with, so the
// two cannot disagree about what is watched.
func bootCaptureFilter() *status.CaptureFilter {
	if upSourceDSN == "" {
		return nil
	}
	return &status.CaptureFilter{
		Known:   true,
		Schemas: cliutil.ParseSchemaList(upSchemas),
		Tables:  cliutil.ParseSchemaList(upTables),
	}
}

func upConsoleConfig(db *sql.DB, indexDSN string, opts consoleOpts, reg *console.Registry) (console.Config, error) {
	return upConsoleConfigFor(db, indexDSN, opts, reg, newSourceFlavorCell(upSourceFlavor))
}

// upConsoleConfigFor is upConsoleConfig with the cell the daemon's own
// capture records its flavor in (mainStreamFlavorHook): the source-ful watch
// passes the one its stream writes, so the boot entry's label and its
// capture status read follow what the source reports.
func upConsoleConfigFor(db *sql.DB, indexDSN string, opts consoleOpts, reg *console.Registry, bootFlavor *sourceFlavorCell) (console.Config, error) {
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
	workingDefault := bootWorkingFolderDefault(reg)
	// GOMAXPROCS, not NumCPU: since Go 1.25 it follows a container's CPU
	// limit, which is the number of cores the workers really share.
	if w := sqlMaxInFlightWarning(opts.SQLMaxInFlight, sqlsandbox.DefaultLimits().Threads, runtime.GOMAXPROCS(0)); w != "" {
		slog.Warn(w)
	}
	if w := sqlMemoryWarning(opts.SQLMemoryLimit, max(opts.SQLMaxInFlight, 1), console.HostMemoryBytes()); w != "" {
		slog.Warn(w)
	}
	// Only a daemon that captures its own source has a flavor to show.
	var bootSourceFlavor func() string
	if upSourceDSN != "" {
		bootSourceFlavor = bootFlavor.get
	}
	return console.Config{
		DB:      db,
		DBName:  cfg.DBName,
		BootDSN: indexDSN,
		Listen:  opts.Listen,
		Token:   opts.Token,
		// What this daemon's own capture watches (#1802).
		BootCaptureFilter: bootCaptureFilter(),
		// The Overview asks the source whether capture is caught up (#1794).
		// Here because both watch entry points reach this function; the
		// read-only serve does not, and answers unknown.
		CaptureStatus:    newCaptureStatusReporter(upSourceDSN).withBootFlavor(bootFlavor.get).withBootSSL(bootSourceSSL()).withBootFilters(upSchemas, upTables),
		BootSourceFlavor: bootSourceFlavor,

		BaselineDir:   opts.BaselineDir,
		BaselineS3:    opts.BaselineS3,
		AuthPath:      opts.AuthFile,
		MCPTokenPath:  opts.MCPTokenFile,
		FlashbackPath: flashbackFilePath(opts),
		// The SQL memory saved in the web interface (#2210), beside the
		// servers file like the port's setting.
		SQLSettingsPath: sqlSettingsFilePath(opts.ServersFile),
		TLSCert:         opts.TLSCert,
		TLSKey:          opts.TLSKey,
		AllowedHosts:    opts.AllowedHosts,
		FlashbackListen: opts.FlashbackListen,
		ReadRouting:     opts.ReadRouting,
		// Test connection tries a forwarding account with the port's own
		// client (#2079).
		RouteAccountProbe: probeRouteAccount,
		// A statement in flight on a connection dropped by an account
		// change is ended on the source too.
		KillSourceThreads: killSourceThreads,
		SQLMaxInFlight:    opts.SQLMaxInFlight,
		// --sql-memory (#2210); "" keeps the sandbox default.
		SQLLimits:      sqlsandbox.Limits{MemoryLimit: opts.SQLMemoryLimit},
		SQLPortMaxRows: opts.SQLPortMaxRows,
		// The daemon's --rotate-* defaults, so GET /api/rotation can report the
		// effective policy (and the console panel prefill it) before the
		// operator saves an override.
		RotationDefaults: console.RotationDefaults{
			Retain:    upRotateRetain,
			Interval:  upRotateInterval,
			AddFuture: upRotateAddFuture,
			Enabled:   upRotationCfg.Enabled,
		},
		// The Snapshots page's read-only rows (#1582):
		// what this daemon was told, verbatim, each under the exact flag or
		// env name the page shows beside it. Values, not re-derivations — the
		// page's whole job is saying where the effective value came from.
		BackupSettingsDefaults: console.BackupSettingsDefaults{
			BaselineRetain: upConsoleBaselineRetain,
			RefreshEvery:   upBaselineRefreshEvery,
			LockMode:       bootLockModeReport(),
			LockModeErr:    errString(upConsoleBaselineLockModeErr),
			TriggerOn:      upConsoleBaselineTrigger,
			StagingDir:     upBaselineStageDir,
			// What is in force when that is empty and nothing is saved
			// (#2255): worked out from the host, so the page cannot know it.
			StagingDirDefault:         workingDefault.Dir,
			StagingDirDefaultWhy:      workingDefault.Why,
			StagingDirDefaultInMemory: workingDefault.InMemory,
			VerifyInterval:            upVerifyInterval,
			VerifyTables:              upVerifyTables,
			// Which of those rows THIS daemon applies without a restart
			// (#1682). Computed from the same expressions that gate the
			// consumers below — the caveat BaselineRefreshDefaults already
			// carries: gate and report must move together, or the page
			// promises a live edit the daemon never picks up.
			Live: backupSettingsLive(
				pruneLoopRuns(reg),
				verifyLoopRuns(),
				upConsoleBaselineTrigger || upBaselineRefreshEvery != "",
			),
		},
		// The loop that applies each server's keep-newest count (#1681):
		// the listing reports a retention only where this is true.
		LocalPruneLoop: pruneLoopRuns(reg),
		// watch takes the snapshots, so it creates their folders (#1681);
		// the read-only serve does not set this and only checks them.
		MayCreateFolders: true,
		BaselineRefreshDefaults: console.BaselineRefreshDefaults{
			CarryForwardUnchanged: upBaselineCarryForward,
			// Enabled is the OR because the restore consumes this setting too
			// and is wired off the supervisor, which --baseline-trigger alone
			// creates. Scheduled is the narrower interval-only fact. The two
			// must be computed from the same expressions that gate the two
			// consumers in runWatch, or the panel drifts from the daemon.
			Enabled:   upBaselineRefreshEvery != "" || upConsoleBaselineTrigger,
			Scheduled: upBaselineRefreshEvery != "",
			// The other half of "what happens to a table that did not
			// change": with deltas on, its previous file is linked forward
			// whatever the flag above says.
			TableDeltas: upBaselineTableDeltas,
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

// bootRole says whether the main stream writes the boot index: a source-ful
// `watch` streams into it, a source-less one leaves it idle, and rotation
// logs an idle, empty boot index as housekeeping rather than freed space
// (#1715). A named type so the call sites read as a role, not a bare bool
// (an untyped literal still converts; the wiring test pins which runner
// passes which).
type bootRole bool

const (
	bootStreamed bootRole = true
	bootIdle     bootRole = false
)

// rotateTargets assembles the built-in rotation's per-cycle targets: the boot
// index (drop-only — the ephemeral default entry has no registry archive
// config; NoWriter when idle, #1715) plus every supervised source. A source
// whose registry entry carries
// an Archive S3 bucket archives-then-drops to it, but ONLY once its bintrail_id
// is resolved (read from stream_state) — until then it rotates drop-only and
// the engine's protect-unarchived guard keeps it from losing data.
func rotateTargets(bootDSN string, boot bootRole, sup *monitorSupervisor, reg *console.Registry, stagingBase string) []rotation.RotateTarget {
	targets := []rotation.RotateTarget{{DSN: bootDSN, NoWriter: boot == bootIdle}}
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
			slog.Warn("built-in rotation: ignoring the invalid policy saved in the web interface; using daemon defaults", "error", err)
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

// pruneLoopRuns and verifyLoopRuns mirror the boot gates of the two loops
// whose settings this daemon can re-read. They exist so the page's per-row
// "restart to change" is decided by the same condition that decides whether
// the goroutine exists — a loop that never started cannot pick anything up,
// and saying otherwise would be the one lie this page cannot afford.
func pruneLoopRuns(reg *console.Registry) bool {
	d, _ := effectiveRetain(reg, upConsoleBaselineRetain)
	return pruneLoopStarts(reg, d)
}

// pruneLoopStarts is startBaselinePruneLoop's gate: a registry (servers carry
// their own keep-newest counts, #1681) or an age retention.
func pruneLoopStarts(reg *console.Registry, retain time.Duration) bool {
	return reg != nil || retain > 0
}

func verifyLoopRuns() bool {
	if upVerifyInterval == "" {
		return false
	}
	d, err := cliutil.ParseRetain(upVerifyInterval)
	return err == nil && d > 0
}

// baselinePruneSweep is ONE prune cycle. A named function rather than a
// closure inside the loop so the wiring is testable: the thing that must hold
// is that the retention is resolved HERE, per cycle, and not captured when the
// goroutine started (#1682) — a closure over a parsed duration was exactly the
// shape that made the setting inert.
func baselinePruneSweep(ctx context.Context, reg *console.Registry, globalDir, globalS3, retainRaw string,
	pruneFn func(context.Context, baseline.PruneOptions) (baseline.PruneResult, error)) {
	// Recover-guard the cycle: a panic in PruneLocal (live S3 calls, fs walks)
	// must NEVER take down the daemon's primary forensic capture — this
	// optional disk-reclaim feature shares the process with the stream.
	// Mirrors rotation.StartLoop's guard (internal/rotation).
	defer func() {
		if r := recover(); r != nil {
			slog.Error("baseline prune cycle panicked; retention continues next tick", "panic", r)
		}
	}()
	// Re-read per cycle: a retention saved from the interface bounds the very
	// next sweep instead of waiting for a restart. A value saved as empty
	// stops pruning, which is a real answer — and why this reads the flag
	// rather than treating a zero duration as "unset".
	retain, on := effectiveRetain(reg, retainRaw)
	if !on {
		retain = 0
	}
	var entries []console.ServerEntry
	if reg != nil {
		entries = reg.List()
	}
	var targets []baselinePruneTarget
	if retain > 0 {
		// A per-server local baseline dir with no S3 prefix and no count is
		// the only copy — skipped, but warn (matching the global/CLI signal)
		// so its unbounded growth isn't silent.
		for _, e := range entries {
			if e.BaselineDir != "" && e.BaselineS3 == "" && e.LocalKeepNewest <= 0 {
				slog.Warn("baseline-retain: server has a local baseline dir, no S3 prefix and no number of snapshots to keep; its baselines are the only copy and will not be pruned",
					"server", e.Name, "dir", e.BaselineDir)
			}
		}
		targets = baselinePruneTargets(entries, globalDir, globalS3)
	}
	// Local-only folders prune to their count whether or not an age
	// retention is set (#1681); a set one still protects younger snapshots.
	targets = append(targets, localKeepPruneTargets(entries, globalDir)...)
	runBaselinePruneCycle(ctx, targets, retain, pruneFn)
}

// resolveBaselineLockModeEnv reads BINTRAIL_CONSOLE_BASELINE_LOCK_MODE. Empty
// is unset: the automatic mode (#1986), which the Compose file relies on by
// passing an empty value when BASELINE_LOCK_MODE is not in .env.
func resolveBaselineLockModeEnv() {
	// Lock mode defaults to point-consistent (baseline.DefaultLockMode). The
	// pre-#1377 BINTRAIL_CONSOLE_BASELINE_POINT_CONSISTENT opt-in is gone: it
	// selected what is now the default, so an operator who set it keeps the
	// behaviour they asked for and needs no migration. Only this variable can
	// select a WEAKER mode — a snapshot that can be torn has to be asked for.
	// All three, so a second read (tests call this repeatedly) cannot keep
	// an earlier run's mode or refusal.
	upConsoleBaselineLockMode, upConsoleBaselineLockModeSet, upConsoleBaselineLockModeErr = baseline.DefaultLockMode, false, nil
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
			upConsoleBaselineLockModeSet = true
		}
	}
}

// bootLockModeReport is the lock mode the settings API reports as the startup
// value: what the variable set, or "" when it is unset, which is the automatic
// mode (#1986). Reporting the internal default (ftwrl) there would say every
// dump runs ftwrl while an RDS host's run lock-all.
func bootLockModeReport() string {
	if !upConsoleBaselineLockModeSet {
		return ""
	}
	return string(upConsoleBaselineLockMode)
}

// bootSourceSSL is the TLS of the daemon's own capture, from --ssl-*: the
// boot server's console checks connect with it too.
func bootSourceSSL() config.SSL {
	return config.SSL{Mode: upSSLMode, CA: upSSLCA, Cert: upSSLCert, Key: upSSLKey}
}

// sqlMaxInFlightWarning says, once at startup, when the SQL statements
// allowed at once can take more threads than the host has cores (#2030).
// Not a refusal: the operator may know the host better (statements that
// rarely overlap). Only for a cap the operator raised: the default on a small
// host would otherwise warn at every boot about a choice nobody made. Empty
// when the cap fits.
func sqlMaxInFlightWarning(maxInFlight, threadsEach, cores int) string {
	if maxInFlight <= sqlsandbox.DefaultMaxInFlight || maxInFlight*threadsEach <= cores {
		return ""
	}
	return fmt.Sprintf("--sql-max-in-flight %d: %d statements at once can take %d threads on a host with %d cores, and they share them with capture; capture may fall behind while they run",
		maxInFlight, maxInFlight, maxInFlight*threadsEach, cores)
}

// resolveSQLMemory sets upSQLMemoryLimit from --sql-memory or, when the flag
// is not given, BINTRAIL_CONSOLE_SQL_MEMORY (#2210). Called with
// resolveSQLMaxInFlight, before the daemon waits for its index, so a typo
// fails at once.
func resolveSQLMemory(cmd *cobra.Command) error {
	limit, err := sqlMemoryFrom(cmd)
	if err != nil {
		return err
	}
	upSQLMemoryLimit = limit
	return nil
}

// sqlMemoryFrom is --sql-memory on cmd or, when not given,
// BINTRAIL_CONSOLE_SQL_MEMORY, as DuckDB takes it: in MiB, because its "GB"
// is decimal and ours (and the operator's) binary. "" when neither is set,
// and then the web interface's saved value or the default applies. watch and
// serve both run SQL on the copy, so both read it. The web interface reads a
// value with the same console.ParseSQLMemory, so both accept the same text.
func sqlMemoryFrom(cmd *cobra.Command) (string, error) {
	raw, name := "", "--sql-memory"
	if cmd.Flags().Changed("sql-memory") {
		raw, _ = cmd.Flags().GetString("sql-memory")
	} else {
		raw, name = strings.TrimSpace(os.Getenv("BINTRAIL_CONSOLE_SQL_MEMORY")), "BINTRAIL_CONSOLE_SQL_MEMORY"
		if raw == "" {
			return "", nil
		}
	}
	mib, err := console.ParseSQLMemory(raw)
	if err != nil {
		return "", fmt.Errorf("%s=%q: want a size of at least 512MB, e.g. 4GB (the memory each SQL statement on the copy may use; the default is 2GB)",
			name, raw)
	}
	return fmt.Sprintf("%dMiB", mib), nil
}

// sqlMemoryWarning says, once at startup, when the statements allowed at
// once can take more memory together than the host has (#2210). Not a
// refusal, as with the threads: statements rarely all run to the cap. Only
// for a memory the operator set here; a value saved in the web interface is
// warned about by the console when it loads it. Empty when it fits or the
// host is unknown.
func sqlMemoryWarning(memoryLimit string, maxInFlight int, hostBytes uint64) string {
	if memoryLimit == "" {
		return ""
	}
	total, over := console.SQLMemoryOverHost(memoryLimit, maxInFlight, hostBytes)
	if !over {
		return ""
	}
	each, _ := cliutil.ParseByteSize(memoryLimit)
	return fmt.Sprintf("--sql-memory %d MiB: %d statements at once can take %d MiB, more than the %d MiB this host lets DBTrail use (its memory, or a lower container limit), which capture shares; a statement may be killed by the kernel instead of refused",
		each>>20, maxInFlight, total, hostBytes>>20)
}

// resolveSQLMaxInFlight sets upSQLMaxInFlight from --sql-max-in-flight or,
// when the flag is not given, BINTRAIL_CONSOLE_SQL_MAX_IN_FLIGHT (#2030).
// Called before the daemon waits for its index, so a typo fails at once: a
// cap below 1 would refuse every statement, and falling back to the default
// would hide the mistake.
func resolveSQLMaxInFlight(cmd *cobra.Command) error {
	if !cmd.Flags().Changed("sql-max-in-flight") {
		if v := strings.TrimSpace(os.Getenv("BINTRAIL_CONSOLE_SQL_MAX_IN_FLIGHT")); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil || n < 1 {
				return fmt.Errorf("BINTRAIL_CONSOLE_SQL_MAX_IN_FLIGHT=%q: want a whole number of at least 1 (how many SQL statements run at once; the default is %d)", v, sqlsandbox.DefaultMaxInFlight)
			}
			upSQLMaxInFlight = n
		}
	} else if upSQLMaxInFlight < 1 {
		return fmt.Errorf("--sql-max-in-flight %d: want at least 1 (how many SQL statements run at once; the default is %d)", upSQLMaxInFlight, sqlsandbox.DefaultMaxInFlight)
	}
	return nil
}

// resolveSQLPortMaxRows sets upSQLPortMaxRows from --sql-port-max-rows or,
// when the flag is not given, BINTRAIL_CONSOLE_SQL_PORT_MAX_ROWS. A value
// below 1 fails here, at startup: it would refuse every result, and falling
// back to the default would hide the mistake.
func resolveSQLPortMaxRows(cmd *cobra.Command) error {
	const want = "want a whole number of at least 1 (the most rows one statement returns on the MySQL port; the default is %d)"
	if !cmd.Flags().Changed("sql-port-max-rows") {
		if v := strings.TrimSpace(os.Getenv("BINTRAIL_CONSOLE_SQL_PORT_MAX_ROWS")); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil || n < 1 {
				return fmt.Errorf("BINTRAIL_CONSOLE_SQL_PORT_MAX_ROWS=%q: "+want, v, console.DefaultSQLPortMaxRows)
			}
			upSQLPortMaxRows = n
		}
	} else if upSQLPortMaxRows < 1 {
		return fmt.Errorf("--sql-port-max-rows %d: "+want, upSQLPortMaxRows, console.DefaultSQLPortMaxRows)
	}
	return nil
}

// flashbackFilePath is where the web interface saves the MySQL port's setting
// (#2101): beside the servers file. That file's directory is the one place
// every working installation already keeps across restarts (the compose stack
// points it into its state volume), so the setting needs no path of its own.
func flashbackFilePath(opts consoleOpts) string {
	servers := opts.ServersFile
	if servers == "" {
		servers = console.DefaultRegistryPath()
	}
	return filepath.Join(filepath.Dir(servers), console.FlashbackFileName)
}

// sqlSettingsFilePath is where the web interface saves the SQL memory
// (#2210): beside the servers file, for the same reason as flashbackFilePath.
// serve and watch both use it.
func sqlSettingsFilePath(serversFile string) string {
	if serversFile == "" {
		serversFile = console.DefaultRegistryPath()
	}
	return filepath.Join(filepath.Dir(serversFile), console.SQLSettingsFileName)
}

// autoServerID derives the replication server-id when --server-id was not
// given: from the source connection AND this installation's index, so a
// second installation capturing the same source gets another id.
func autoServerID(ctx context.Context, w io.Writer) (uint32, error) {
	return installid.AutoDerive(ctx, w, upSourceDSN, upIndexDSN)
}

// wireSQLChainLine hands the refresh the console's live fold line (#2210,
// console.Server.SQLFoldLine), so a table's chain ends at half of it. Both
// watch entry points call it right after the console exists, before either
// starts serving.
func wireSQLChainLine(sup *baselineSupervisor, srv *console.Server) {
	if sup != nil && srv != nil {
		line := srv.SQLFoldLine
		sup.sqlChainLine.Store(&line)
	}
}
