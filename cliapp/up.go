package cliapp

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/doctor"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/rotation"
	"github.com/dbtrail/dbtrail/internal/serverid"
)

var upCmd = &cobra.Command{
	Use:   "up",
	Short: "One command: preflight + init + stream (the friction-free quickstart)",
	Long: `Runs preflight checks (equivalent to 'bintrail doctor'), creates the
index tables if they do not exist (equivalent to 'bintrail init'), and starts
the replication stream (equivalent to 'bintrail stream'). Re-running 'bintrail
up' is idempotent: it skips work that's already done and resumes the stream
from its saved checkpoint.

This is the friction-free entry point for new bintrail installations. The
underlying 'init', 'snapshot', 'index', and 'stream' commands remain available
for advanced workflows (e.g. running them on separate machines or for
debugging).

If --server-id is not provided, a deterministic ID is derived from
host:user:dbname of the source DSN, mapped into a high range to reduce
collision odds with existing replicas.

Examples:

  bintrail up --source-dsn "$SRC" --index-dsn "$IDX"
  bintrail up --source-dsn "$SRC" --index-dsn "$IDX" --skip-doctor
  bintrail up --source-dsn "$SRC" --index-dsn "$IDX" --schemas mydb,otherdb`,
	RunE: runUp,
}

var (
	upSourceDSN   string
	upIndexDSN    string
	upServerID    uint32
	upSchemas     string
	upTables      string
	upBatchSize   int
	upCheckpoint  int
	upMetricsAddr string
	upPartitions  int
	upSkipDoctor  bool
	upFormat      string

	upRotateRetain    string
	upRotateInterval  string
	upRotateAddFuture int

	// upRotationCfg holds the parsed built-in-rotation settings from runUp's
	// validation, read at the phase-3 start site (the cobra-accumulator
	// pattern; the parsing and the loop itself live in internal/rotation).
	upRotationCfg rotation.Settings
)

func init() {
	upCmd.Flags().StringVar(&upSourceDSN, "source-dsn", "", "DSN for the source MySQL server (required)")
	upCmd.Flags().StringVar(&upIndexDSN, "index-dsn", "", "DSN for the index MySQL database (required)")
	upCmd.Flags().Uint32Var(&upServerID, "server-id", 0, "MySQL replica server ID (default: hash of source host:user:dbname)")
	upCmd.Flags().StringVar(&upSchemas, "schemas", "", "Comma-separated schemas to index (default: all user schemas)")
	upCmd.Flags().StringVar(&upTables, "tables", "", "Comma-separated tables to index (default: all)")
	upCmd.Flags().IntVar(&upBatchSize, "batch-size", 1000, "Events per batch INSERT")
	upCmd.Flags().DurationVar(&indexer.WriteTimeout, "write-timeout", indexer.DefaultWriteTimeout, "Deadline for each index write (batch INSERT, checkpoint, digest lookup). A mid-statement network stall surfaces as an error within this window instead of freezing the daemon on kernel TCP retransmission (~13-16 min). Raise for very large batches over a slow link")
	upCmd.Flags().IntVar(&upCheckpoint, "checkpoint", 10, "Checkpoint interval in seconds")
	upCmd.Flags().StringVar(&upMetricsAddr, "metrics-addr", "", "Address to expose Prometheus metrics (e.g. :9090); empty = disabled")
	upCmd.Flags().IntVar(&upPartitions, "partitions", 48, "Hourly partitions to create on first init")
	upCmd.Flags().BoolVar(&upSkipDoctor, "skip-doctor", false, "Skip the preflight checks (useful when you've already verified with `bintrail doctor`)")
	upCmd.Flags().StringVar(&upFormat, "format", "text", "Output format: text or json")
	upCmd.Flags().StringVar(&upRotateRetain, "rotate-retain", indexer.DefaultRotateRetain, "Built-in rotation: drop index partitions older than this (Nd/Nh; \"off\" disables)")
	upCmd.Flags().StringVar(&upRotateInterval, "rotate-interval", "1h", "Built-in rotation: how often to run a rotation cycle")
	upCmd.Flags().IntVar(&upRotateAddFuture, "rotate-add-future", 3, "Built-in rotation: keep at least N future hourly partitions ready")
	// --source-dsn is validated in runUp instead of MarkFlagRequired so the
	// rotation settings parse first (fail-fast on a typo before any phase
	// runs) — see TestRunUp_explicitRetentionWiring, which relies on that
	// ordering. The combined daemon that could start source-less moved to
	// `bintrail-console watch`.
	_ = upCmd.MarkFlagRequired("index-dsn")
	bindCommandEnv(upCmd)
	rootCmd.AddCommand(upCmd)
}

func runUp(cmd *cobra.Command, args []string) error {
	if !cliutil.IsValidOutputFormat(upFormat) {
		return fmt.Errorf("invalid --format %q; must be text or json", upFormat)
	}
	// Validate the built-in rotation settings up front so a typo fails fast,
	// before any phase runs. The loop itself starts with phase 3. The Changed
	// check covers flag and env alike (bindCommandEnv marks env-set flags
	// Changed); an explicitly-chosen retention disables the upgrade guard.
	var err error
	upRotationCfg, err = rotation.ParseSettings(upRotateRetain, upRotateInterval, upRotateAddFuture,
		cmd.Flags().Changed("rotate-retain"))
	if err != nil {
		return err
	}
	if upSourceDSN == "" {
		return fmt.Errorf("--source-dsn is required")
	}

	// ── Phase 1: Preflight ──────────────────────────────────────────────────
	if !upSkipDoctor {
		fmt.Fprintln(os.Stderr, "=== Phase 1/3: Preflight checks ===")
		// The capacity projection uses up's actual rotation window (0 when
		// built-in rotation is disabled → it reports unbounded growth). Its
		// FAIL is ADVISORY here: blocking the stream over a disk forecast
		// would manufacture the very forensic gap it warns about (an
		// unattended reboot would crash-loop instead of capturing while
		// there is still room). Standalone `doctor` keeps full FAIL
		// semantics for CI.
		preflight := doctor.Build(cmd.Context(), upSourceDSN, upIndexDSN, upSchemas, upRotationCfg.Retain)
		appendExtDoctorChecks(cmd.Context(), preflight, upSourceDSN, upIndexDSN)
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
	if err := runUpInit(cmd); err != nil {
		return fmt.Errorf("init failed: %w", err)
	}
	fmt.Fprintln(os.Stderr)

	// ── Phase 3: Stream ─────────────────────────────────────────────────────
	fmt.Fprintln(os.Stderr, "=== Phase 3/3: Streaming ===")
	return runUpStream(cmd, args)
}

// upPreflightOutcome maps the preflight report to up's boot decision: fatal
// is non-nil for any non-advisory failure (boot refused); warnCapacity is
// true when the capacity projection was the ONLY failure — boot proceeds,
// but the operator must hear about it (the caller prints the WARNING).
// Extracted so the advisory semantics are unit-testable: losing either half
// would silently change what blocks `up` or swallow the disk-full signal.
func upPreflightOutcome(r *doctor.Report) (fatal error, warnCapacity bool) {
	if err := r.ErrExcluding(doctor.CapacityCheckName); err != nil {
		return err, false
	}
	return nil, r.Err() != nil
}

// runUpInit calls runInit with up's flag values. We share the parent context
// so Ctrl-C propagates during the init phase (table creation + optional S3
// bucket provisioning, both of which can block on remote IO).
func runUpInit(cmd *cobra.Command) error {
	initIndexDSN = upIndexDSN
	initPartitions = upPartitions
	initFormat = "text"
	initEncrypt = false
	initS3Bucket = ""
	initS3Region = "us-east-1"
	initS3ARN = ""

	subCmd := &cobra.Command{}
	subCmd.SetContext(cmd.Context())
	return runInit(subCmd, nil)
}

// runUpStream delegates to runStream after copying every up* flag value into
// the corresponding strm* package global. The snapshot step is handled inside
// runStream via auto-snapshot when no snapshot exists and --source-dsn is set.
func runUpStream(cmd *cobra.Command, args []string) error {
	serverID := upServerID
	if serverID == 0 {
		id, err := serverid.DeriveServerID(upSourceDSN)
		if err != nil {
			return fmt.Errorf("cannot auto-derive --server-id from --source-dsn: %w (pass --server-id explicitly to bypass)", err)
		}
		serverID = id
		fmt.Fprintf(os.Stderr, "Auto-derived server-id from source DSN: %d\n", serverID)
	}
	populateStreamFlags(serverID)

	// Rotate the boot index on the daemon lifecycle. The loop gets its own
	// signal-bound context (cmd's root context is never cancelled by SIGINT —
	// runStream installs its handler on a derived child), so rotation stops
	// when the stream starts draining.
	rotCtx, rotStop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
	defer rotStop()
	// Core `up` is single-source and has no control plane / registry, so its
	// rotation is drop-only (no per-source S3 archive config) and its settings
	// are static — a constant provider (no live console reconfiguration here).
	rotation.StartLoop(rotCtx, func() rotation.Settings { return upRotationCfg }, func() []rotation.RotateTarget {
		return []rotation.RotateTarget{{DSN: upIndexDSN}}
	})
	// Extension source jobs (ext.RegisterSourceJob) are NOT started here: they
	// share the stream's lifecycle, so runStream owns the one call site for
	// both `up` and plain `stream` (starting them here too would run every
	// registered job twice under `up`). populateStreamFlags above has already
	// copied up's DSNs into the strm* globals runStream reads, and flavor is
	// the value the stream actually runs with — `up` has no --source-flavor
	// flag of its own, so strmFlavor holds streamCmd's default ("mysql") or
	// the BINTRAIL_SOURCE_FLAVOR override that bindCommandEnv(streamCmd)
	// applied at flag-binding time.
	return runStream(cmd, args)
}

// populateStreamFlags copies every up* package global into the corresponding
// strm* global, plus the resolved server-id. Extracted from runUpStream so the
// up→strm fan-out is unit-testable.
//
// `up` has no --ssl-*/--start-gtid/--gap-timeout flags of its own, so
// strmSSLMode/strmSSLCA/strmSSLCert/strmSSLKey/strmStartGTID/strmGapTimeout
// are only pinned to up's hardcoded defaults when the operator hasn't
// already configured them on streamCmd — via an explicit flag or (the only
// channel `up` actually exposes) the BINTRAIL_SSL_MODE/BINTRAIL_SSL_CA/
// BINTRAIL_SSL_CERT/BINTRAIL_SSL_KEY/BINTRAIL_START_GTID/
// BINTRAIL_STREAM_GAP_TIMEOUT env vars, which bindCommandEnv(streamCmd)
// applies via Flags().Set (marking the flag Changed). Overwriting an
// already-Changed flag would silently downgrade e.g. BINTRAIL_SSL_MODE=
// verify-ca to the unauthenticated "preferred" default, or drop a
// configured mutual-TLS client cert/key (#808).
func populateStreamFlags(serverID uint32) {
	strmIndexDSN = upIndexDSN
	strmSourceDSN = upSourceDSN
	strmServerID = serverID
	strmStartFile = ""
	strmStartPos = 4
	if !streamCmd.Flags().Changed("start-gtid") {
		strmStartGTID = ""
	}
	strmBatchSize = upBatchSize
	strmSchemas = upSchemas
	strmTables = upTables
	strmCheckpoint = upCheckpoint
	strmMetricsAddr = upMetricsAddr
	if !streamCmd.Flags().Changed("ssl-mode") {
		strmSSLMode = "preferred"
	}
	if !streamCmd.Flags().Changed("ssl-ca") {
		strmSSLCA = ""
	}
	if !streamCmd.Flags().Changed("ssl-cert") {
		strmSSLCert = ""
	}
	if !streamCmd.Flags().Changed("ssl-key") {
		strmSSLKey = ""
	}
	strmFormat = upFormat
	strmReset = false
	strmNoGapFill = false
	if !streamCmd.Flags().Changed("gap-timeout") {
		strmGapTimeout = 30
	}
}
