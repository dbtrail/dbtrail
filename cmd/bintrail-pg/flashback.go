package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os/signal"
	"strings"
	"syscall"
	"time"

	drivermysql "github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/cli"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/pgshim"
	"github.com/dbtrail/dbtrail/internal/shim"
)

var flashbackCmd = &cobra.Command{
	Use:   "flashback",
	Short: "Serve single-row AS OF time-travel over the PostgreSQL wire protocol (psql)",
	Long: `Serves the bintrail time-travel engine over the PostgreSQL wire protocol so a
PostgreSQL operator can run single-row AS OF queries from psql or a PG driver,
reading the same out-of-band index (and optional baseline) the MySQL shim uses —
the original binlog/WAL files are never needed.

  psql "host=127.0.0.1 port=5433 user=<tenant> dbname=<schema>"
  SELECT * FROM _flashback.orders AS OF '5 minutes ago' WHERE id = 42;
  SELECT * FROM _snapshot.orders  AS OF '5 minutes ago' WHERE id = 42;
  SELECT * FROM orders AS OF '5 minutes ago' WHERE id = 42;   -- bare form

Addressing: connect with dbname=<your schema> (e.g. public). Choose flashback vs
snapshot semantics with the _flashback. / _snapshot. table prefix in the query,
exactly like the MySQL shim; _snapshot additionally consults --baseline-dir /
--baseline-s3 so a row untouched in the retained window still resolves.

Scope: single-row AS OF (WHERE <primary-key> = <value>) only. Full-table AS OF is
refused with remediation (PostgreSQL full-table time-travel is deferred). Both
the simple and the extended query protocol are served, so psql and a driver in
its default mode (pgx, JDBC, psycopg 3) connect as they are; an argument ($1)
cannot hold a single quote here.

Auth mirrors the shim: a cleartext credential validated per tenant from
--shim-config (shim.yaml). This is loopback-default; front a TLS terminator for a
non-loopback bind (same posture as the MySQL shim behind ProxySQL).`,
	RunE: runFlashback,
}

var (
	fbIndexDSN     string
	fbListen       string
	fbShimConfig   string
	fbNoArchive    bool
	fbAllowGaps    bool
	fbBaselineDir  string
	fbBaselineS3   string
	fbQueryTimeout time.Duration
	fbMaxConns     int
)

func init() {
	flashbackCmd.Flags().StringVar(&fbIndexDSN, "index-dsn", "", "DSN for the index MySQL database (required; env BINTRAIL_INDEX_DSN)")
	flashbackCmd.Flags().StringVar(&fbListen, "listen", "127.0.0.1:5433", "Address to listen on for PostgreSQL-protocol clients (env BINTRAIL_PG_FLASHBACK_LISTEN)")
	flashbackCmd.Flags().StringVar(&fbShimConfig, "shim-config", "shim.yaml", "Tenant auth config, same format as the MySQL shim (env BINTRAIL_PG_SHIM_CONFIG)")
	flashbackCmd.Flags().BoolVar(&fbNoArchive, "no-archive", false, "Disable archive auto-discovery (index-only results)")
	flashbackCmd.Flags().BoolVar(&fbAllowGaps, "allow-gaps", false, "Downgrade coverage gaps / archive failures to warnings and return partial results instead of aborting the query")
	flashbackCmd.Flags().StringVar(&fbBaselineDir, "baseline-dir", "", "Local baseline snapshot directory enabling _snapshot (env BINTRAIL_PG_BASELINE_DIR)")
	flashbackCmd.Flags().StringVar(&fbBaselineS3, "baseline-s3", "", "S3 baseline snapshot prefix enabling _snapshot; takes precedence over --baseline-dir (env BINTRAIL_PG_BASELINE_S3)")
	flashbackCmd.Flags().DurationVar(&fbQueryTimeout, "query-timeout", 30*time.Second, "Per-query deadline; 0 disables")
	flashbackCmd.Flags().IntVar(&fbMaxConns, "max-connections", 0, "Cap concurrent connections; 0 = unlimited")

	// index-dsn lives in cli.EnvBindings, so BindCommandEnv both loads the env
	// file and sets it from BINTRAIL_INDEX_DSN (marking it Changed so the env-only
	// path satisfies MarkFlagRequired). The other flags' BINTRAIL_PG_* fallback is
	// applied in runFlashback (the env file is loaded by then).
	_ = flashbackCmd.MarkFlagRequired("index-dsn")
	cli.BindCommandEnv(flashbackCmd)

	rootCmd.AddCommand(flashbackCmd)
}

// runFlashback is the `bintrail-pg flashback` entrypoint. It mirrors the MySQL
// shim's startup (load tenants → connect + ping + migrate the index → listen)
// minus the ProxySQL-specific machinery, then serves the PostgreSQL wire
// front-end. The engine and data layer are index-only, so no live PostgreSQL
// source is contacted here.
func runFlashback(cmd *cobra.Command, args []string) error {
	applyEnvFallback(&fbListen, "BINTRAIL_PG_FLASHBACK_LISTEN")
	applyEnvFallback(&fbShimConfig, "BINTRAIL_PG_SHIM_CONFIG")
	applyEnvFallback(&fbBaselineDir, "BINTRAIL_PG_BASELINE_DIR")
	applyEnvFallback(&fbBaselineS3, "BINTRAIL_PG_BASELINE_S3")

	tenantCfgs, err := shim.LoadTenantConfigs(fbShimConfig)
	if err != nil {
		return err
	}
	users := make(map[string]string, len(tenantCfgs))
	for _, t := range tenantCfgs {
		users[t.MySQLUser] = t.MySQLPassword
	}
	auth, err := shim.NewTenantAuth(users)
	if err != nil {
		return err
	}

	// Per-tenant schema isolation (#824/#1261), built from the same helper the
	// MySQL shim uses. The startup warning mirrors it too: an operator who set
	// allowed_schemas on some tenants can reasonably read the file as isolated,
	// and silence there is how an unrestricted tenant goes unnoticed.
	allowedSchemas := shim.UserAllowedSchemas(tenantCfgs)
	if unrestricted := shim.TenantsWithoutAllowedSchemas(tenantCfgs); len(tenantCfgs) > 1 && len(unrestricted) > 0 {
		slog.Warn(
			"flashback: cross-schema isolation is NOT enforced for some tenants; any of them can query every schema in the index. Add allowed_schemas to each tenant in shim.yaml to isolate them",
			"tenants", strings.Join(unrestricted, ", "))
	}

	db, err := config.Connect(fbIndexDSN)
	if err != nil {
		return fmt.Errorf("connect to index: %w", err)
	}
	defer db.Close()

	// Eager ping so a misconfigured DSN fails at startup, not at the first query.
	pingCtx, pingCancel := context.WithTimeout(context.Background(), 10*time.Second)
	err = db.PingContext(pingCtx)
	pingCancel()
	if err != nil {
		return fmt.Errorf("ping index DB: %w", err)
	}

	// Idempotent schema migration on the CLI-typed DSN — the engine SELECTs
	// post-initial-schema columns (query_text/query_hash, #699).
	if err := indexer.EnsureSchema(db); err != nil {
		return indexer.WrapSchemaMigrationErr(err)
	}

	dsnCfg, err := drivermysql.ParseDSN(fbIndexDSN)
	if err != nil {
		return fmt.Errorf("parse index DSN: %w", err)
	}
	if dsnCfg.DBName == "" {
		return fmt.Errorf("index DSN must include the database name (e.g. /bintrail_index)")
	}

	listener, err := net.Listen("tcp", fbListen)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", fbListen, err)
	}
	defer listener.Close()

	if !isLoopbackListenAddr(listener.Addr()) {
		slog.Warn("bintrail-pg flashback is bound to a non-loopback address and authenticates with a CLEARTEXT password; "+
			"front a TLS terminator or restrict network access to this port",
			"addr", listener.Addr().String())
	}

	slog.Info("bintrail-pg flashback listening",
		"addr", listener.Addr().String(),
		"tenants", len(tenantCfgs),
		"snapshot_baseline", fbBaselineDir != "" || fbBaselineS3 != "")

	ctx, cancel := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	go func() {
		<-ctx.Done()
		slog.Info("bintrail-pg flashback shutting down")
		listener.Close()
	}()

	// Usage telemetry for a months-lived process (#1362): Init's drain runs
	// once, at startup, so without this loop a daemon's beacons would spool
	// and age out undelivered. Own goroutine — never on a client connection's
	// path. Same seam as the MySQL shim, via this binary's own hook.
	go tel.Client().RunDaemon(ctx, cmd.Name())

	cfg := pgshim.Config{
		IndexDB: db,
		ShimConfig: shim.Config{
			AllowGaps:    fbAllowGaps,
			NoArchive:    fbNoArchive,
			IndexDBName:  dsnCfg.DBName,
			BaselineDir:  fbBaselineDir,
			BaselineS3:   fbBaselineS3,
			QueryTimeout: fbQueryTimeout,
		},
		Auth:           auth,
		AllowedSchemas: allowedSchemas,
		Logger:         slog.Default(),
		MaxConns:       fbMaxConns,
	}
	return pgshim.Serve(ctx, listener, cfg)
}

// isLoopbackListenAddr reports whether the bound address is loopback, so a
// non-loopback bind (where cleartext auth is exposed) can be warned about.
func isLoopbackListenAddr(addr net.Addr) bool {
	host, _, err := net.SplitHostPort(addr.String())
	if err != nil {
		return false
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
