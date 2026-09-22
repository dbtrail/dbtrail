package consoleapp

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/rotation"
)

// This file mutates up* package globals via save-and-restore. DO NOT add
// t.Parallel() to any test here — concurrent runs would cross-write each
// other's state (runWatch reads the globals at start).

func assertStr(t *testing.T, name, got, want string) {
	t.Helper()
	if got != want {
		t.Errorf("%s = %q, want %q", name, got, want)
	}
}

func TestUpConsoleConfig(t *testing.T) {
	cfg, err := upConsoleConfig(nil, "user:pass@tcp(127.0.0.1:3306)/binlog_index", consoleOpts{Listen: "127.0.0.1:8090", Token: "tok", BaselineDir: "/baselines", BaselineS3: "s3://bucket/prefix/", AuthFile: "/auth.yaml", TLSCert: "/c.pem", TLSKey: "/k.pem", AllowedHosts: []string{"console.internal"}, FlashbackListen: "127.0.0.1:3308"}, nil)
	if err != nil {
		t.Fatalf("upConsoleConfig: %v", err)
	}
	// The time-travel port's address reaches the console verbatim (#1446), so
	// GET /api/flashback reports the bind the daemon is serving; and the
	// resolved flag global is what upConsoleOpts snapshots into it.
	if cfg.FlashbackListen != "127.0.0.1:3308" {
		t.Errorf("FlashbackListen=%q, want verbatim pass-through", cfg.FlashbackListen)
	}
	prevFB := upConsoleFlashbackListen
	upConsoleFlashbackListen = "[::]:13308"
	if got := upConsoleOpts().FlashbackListen; got != "[::]:13308" {
		t.Errorf("upConsoleOpts().FlashbackListen=%q, want the --flashback-listen value", got)
	}
	upConsoleFlashbackListen = prevFB
	if cfg.DBName != "binlog_index" {
		t.Errorf("DBName = %q, want binlog_index", cfg.DBName)
	}
	if cfg.Listen != "127.0.0.1:8090" || cfg.Token != "tok" {
		t.Errorf("Listen=%q Token=%q, want 127.0.0.1:8090 / tok", cfg.Listen, cfg.Token)
	}
	// Baseline sources thread through verbatim (#379) — dir-over-s3 precedence
	// is owned by console.New, not here, so both must pass through raw.
	if cfg.BaselineDir != "/baselines" || cfg.BaselineS3 != "s3://bucket/prefix/" {
		t.Errorf("BaselineDir=%q BaselineS3=%q, want /baselines / s3://bucket/prefix/", cfg.BaselineDir, cfg.BaselineS3)
	}
	// Auth and TLS settings thread through verbatim too — console.New owns
	// their validation (both-or-neither TLS, auth-file probe).
	if cfg.AuthPath != "/auth.yaml" || cfg.TLSCert != "/c.pem" || cfg.TLSKey != "/k.pem" {
		t.Errorf("AuthPath=%q TLSCert=%q TLSKey=%q, want verbatim pass-through", cfg.AuthPath, cfg.TLSCert, cfg.TLSKey)
	}
	if len(cfg.AllowedHosts) != 1 || cfg.AllowedHosts[0] != "console.internal" {
		t.Errorf("AllowedHosts=%v, want [console.internal]", cfg.AllowedHosts)
	}
	// watch has no --profile/--no-archive, so NoArchive must stay false —
	// setting it would silently disable the reconstruct gate this wiring
	// enables.
	if cfg.NoArchive {
		t.Errorf("watch console config must not set NoArchive: %+v", cfg)
	}

	// The stack-drift check is wired into the same function, for the same
	// reason: it is silent on a healthy stack, so nothing else would notice the
	// call going missing.
	var gotDSN string
	var gotOpts consoleOpts
	prevReporter := composeDriftReporter
	composeDriftReporter = func(dsn string, opts consoleOpts) { gotDSN, gotOpts = dsn, opts }
	_, err = upConsoleConfig(nil, "user:pass@tcp(127.0.0.1:3306)/binlog_index",
		consoleOpts{Listen: "127.0.0.1:8090", AuthFile: "/auth.yaml", ServersFile: "/servers.yaml"}, nil)
	composeDriftReporter = prevReporter
	if err != nil {
		t.Fatalf("upConsoleConfig (drift): %v", err)
	}
	if gotDSN != "user:pass@tcp(127.0.0.1:3306)/binlog_index" {
		t.Errorf("the stack-drift check was not run with the index DSN in effect: %q", gotDSN)
	}
	if gotOpts.AuthFile != "/auth.yaml" || gotOpts.ServersFile != "/servers.yaml" {
		t.Errorf("the stack-drift check was not given the console state paths in effect: %+v", gotOpts)
	}

	// Without baseline flags the Phase 1 default is preserved: empty baselines
	// keep the reconstruct surface gated off.
	cfg, err = upConsoleConfig(nil, "user:pass@tcp(127.0.0.1:3306)/binlog_index", consoleOpts{Listen: "127.0.0.1:8090", Token: "tok"}, nil)
	if err != nil {
		t.Fatalf("upConsoleConfig (no baseline): %v", err)
	}
	if cfg.BaselineDir != "" || cfg.BaselineS3 != "" {
		t.Errorf("empty baseline args should pass through empty: %+v", cfg)
	}

	// Invalid DSN (no '/') must error, not silently produce an empty dbName.
	if _, err := upConsoleConfig(nil, "invalid", consoleOpts{Listen: "127.0.0.1:8090"}, nil); err == nil {
		t.Error("invalid --index-dsn should error")
	}
	// A DSN with no database name must error (parity with runServe) rather
	// than starting a console that feeds an empty schema to the planner.
	if _, err := upConsoleConfig(nil, "user:pass@tcp(127.0.0.1:3306)/", consoleOpts{Listen: "127.0.0.1:8090"}, nil); err == nil {
		t.Error("--index-dsn without a database name should error")
	}
}

// TestResolveUpConsoleEnv locks the flag > env > default precedence for the
// console-specific env vars. The trap this guards: the StringVar flag name and
// the Changed("<name>") string must match exactly — a mismatch compiles fine
// but makes Changed() return false for the real flag, so the env var would
// silently override an explicitly-passed flag.
func TestResolveUpConsoleEnv(t *testing.T) {
	saved := [4]string{upConsoleListen, upConsoleToken, upConsoleBaselineDir, upConsoleBaselineS3}
	t.Cleanup(func() {
		upConsoleListen, upConsoleToken, upConsoleBaselineDir, upConsoleBaselineS3 = saved[0], saved[1], saved[2], saved[3]
	})

	// The four flag names below must exist on the REAL watchCmd, or this test
	// would happily pass against a synthetic clone while production drifted
	// (a rename in init() not mirrored in resolveUpConsoleEnv would make
	// Changed() return false for the real flag → env silently overriding an
	// explicit flag). Pin the names to watchCmd before exercising the resolver.
	for _, name := range []string{"console-listen", "console-token", "baseline-dir", "baseline-s3"} {
		if watchCmd.Flags().Lookup(name) == nil {
			t.Fatalf("flag --%s not registered on watchCmd; resolveUpConsoleEnv's Changed(%q) would always be false", name, name)
		}
	}

	// newCmd registers the same flag names as watchCmd, bound to the same
	// globals, so Changed() reflects what cobra would see in a real run.
	newCmd := func() *cobra.Command {
		cmd := &cobra.Command{}
		cmd.Flags().StringVar(&upConsoleListen, "console-listen", "127.0.0.1:8090", "")
		cmd.Flags().StringVar(&upConsoleToken, "console-token", "", "")
		cmd.Flags().StringVar(&upConsoleBaselineDir, "baseline-dir", "", "")
		cmd.Flags().StringVar(&upConsoleBaselineS3, "baseline-s3", "", "")
		return cmd
	}

	t.Setenv("BINTRAIL_CONSOLE_LISTEN", "0.0.0.0:9999")
	t.Setenv("BINTRAIL_CONSOLE_TOKEN", "env-tok")
	t.Setenv("BINTRAIL_CONSOLE_BASELINE_DIR", "/env/baselines")
	t.Setenv("BINTRAIL_CONSOLE_BASELINE_S3", "s3://env-bucket/p/")

	// No flags set → env wins over defaults.
	upConsoleListen, upConsoleToken, upConsoleBaselineDir, upConsoleBaselineS3 = "127.0.0.1:8090", "", "", ""
	resolveUpConsoleEnv(newCmd())
	assertStr(t, "upConsoleListen (env)", upConsoleListen, "0.0.0.0:9999")
	assertStr(t, "upConsoleToken (env)", upConsoleToken, "env-tok")
	assertStr(t, "upConsoleBaselineDir (env)", upConsoleBaselineDir, "/env/baselines")
	assertStr(t, "upConsoleBaselineS3 (env)", upConsoleBaselineS3, "s3://env-bucket/p/")

	// Explicit flags → flag beats env.
	cmd := newCmd()
	for flag, val := range map[string]string{
		"console-listen": "127.0.0.1:7070",
		"console-token":  "flag-tok",
		"baseline-dir":   "/flag/baselines",
		"baseline-s3":    "s3://flag-bucket/p/",
	} {
		if err := cmd.Flags().Set(flag, val); err != nil {
			t.Fatalf("set --%s: %v", flag, err)
		}
	}
	resolveUpConsoleEnv(cmd)
	assertStr(t, "upConsoleListen (flag)", upConsoleListen, "127.0.0.1:7070")
	assertStr(t, "upConsoleToken (flag)", upConsoleToken, "flag-tok")
	assertStr(t, "upConsoleBaselineDir (flag)", upConsoleBaselineDir, "/flag/baselines")
	assertStr(t, "upConsoleBaselineS3 (flag)", upConsoleBaselineS3, "s3://flag-bucket/p/")

	// Exported-but-EMPTY env vars must be a no-op (the `v != ""` guard):
	// a refactor to os.LookupEnv-with-ok would make an empty export clobber
	// a non-empty default like console-listen's 127.0.0.1:8090 with "".
	// NOTE: newCmd() must run BEFORE seeding the globals — StringVar resets
	// each bound variable to its default at registration time.
	cmd = newCmd()
	t.Setenv("BINTRAIL_CONSOLE_LISTEN", "")
	t.Setenv("BINTRAIL_CONSOLE_TOKEN", "")
	t.Setenv("BINTRAIL_CONSOLE_BASELINE_DIR", "")
	t.Setenv("BINTRAIL_CONSOLE_BASELINE_S3", "")
	upConsoleListen, upConsoleToken, upConsoleBaselineDir, upConsoleBaselineS3 = "127.0.0.1:8090", "tok", "/dir", "s3://b/p/"
	resolveUpConsoleEnv(cmd)
	assertStr(t, "upConsoleListen (empty env)", upConsoleListen, "127.0.0.1:8090")
	assertStr(t, "upConsoleToken (empty env)", upConsoleToken, "tok")
	assertStr(t, "upConsoleBaselineDir (empty env)", upConsoleBaselineDir, "/dir")
	assertStr(t, "upConsoleBaselineS3 (empty env)", upConsoleBaselineS3, "s3://b/p/")
}

// TestWatchStreamConfig asserts the up* → streamrun.Config fan-out (the watch
// equivalent of core up's TestPopulateStreamFlags): a flag added to watch but
// not wired into watchStreamConfig fails here rather than silently dropping
// the value, and the pinned non-configurable defaults are locked in so a
// future "let me delete this unused field" refactor can't change behavior.
func TestWatchStreamConfig(t *testing.T) {
	orig := struct {
		src, idx, sch, tbl, fmtv, met string
		ssl, sslCA, sslCert, sslKey   string
		batch, chk, msi               int
	}{upSourceDSN, upIndexDSN, upSchemas, upTables, upFormat, upMetricsAddr, upSSLMode, upSSLCA, upSSLCert, upSSLKey, upBatchSize, upCheckpoint, upMetricsScrapeInterval}
	t.Cleanup(func() {
		upSourceDSN, upIndexDSN, upSchemas, upTables, upFormat = orig.src, orig.idx, orig.sch, orig.tbl, orig.fmtv
		upBatchSize, upCheckpoint = orig.batch, orig.chk
		upMetricsAddr, upMetricsScrapeInterval = orig.met, orig.msi
		upSSLMode, upSSLCA, upSSLCert, upSSLKey = orig.ssl, orig.sslCA, orig.sslCert, orig.sslKey
	})

	upSourceDSN = "user:pass@tcp(source.example.com:3306)/src"
	upIndexDSN = "ix:pw@tcp(127.0.0.1:3306)/binlog_index"
	upSchemas = "mydb,otherdb"
	upTables = "mydb.orders"
	upBatchSize = 2500
	upCheckpoint = 15
	upFormat = "json"
	upMetricsAddr = "" // primary stream gates index metrics on this being set
	upMetricsScrapeInterval = 33
	// Source TLS is user-configurable on watch (#879): set non-default values
	// so a regression that re-hardcodes SSLMode or drops the cert/key wiring
	// fails here rather than silently downgrading to unauthenticated TLS.
	upSSLMode = "verify-ca"
	upSSLCA = "/etc/ssl/ca.pem"
	upSSLCert = "/etc/ssl/client-cert.pem"
	upSSLKey = "/etc/ssl/client-key.pem"

	const passedServerID uint32 = 4242424242
	cfg := watchStreamConfig(passedServerID)

	assertStr(t, "IndexDSN", cfg.IndexDSN, upIndexDSN)
	assertStr(t, "SourceDSN", cfg.SourceDSN, upSourceDSN)
	assertStr(t, "Schemas", cfg.Schemas, upSchemas)
	assertStr(t, "Tables", cfg.Tables, upTables)
	assertStr(t, "Format", cfg.Format, upFormat)
	if cfg.ServerID != passedServerID {
		t.Errorf("ServerID = %d, want %d", cfg.ServerID, passedServerID)
	}
	if cfg.BatchSize != upBatchSize {
		t.Errorf("BatchSize = %d, want %d", cfg.BatchSize, upBatchSize)
	}
	if cfg.Checkpoint != upCheckpoint {
		t.Errorf("Checkpoint = %d, want %d", cfg.Checkpoint, upCheckpoint)
	}

	// Pinned defaults, not user-configurable from watch.
	assertStr(t, "StartFile", cfg.StartFile, "")
	assertStr(t, "StartGTID", cfg.StartGTID, "")
	// Source TLS is user-configurable (#879) — it must propagate verbatim.
	assertStr(t, "SSLMode", cfg.SSLMode, "verify-ca")
	assertStr(t, "SSLCA", cfg.SSLCA, "/etc/ssl/ca.pem")
	assertStr(t, "SSLCert", cfg.SSLCert, "/etc/ssl/client-cert.pem")
	assertStr(t, "SSLKey", cfg.SSLKey, "/etc/ssl/client-key.pem")
	// The daemon serves ONE /metrics endpoint for all streams; a per-stream
	// bind here would conflict with it.
	assertStr(t, "MetricsAddr", cfg.MetricsAddr, "")
	if cfg.MetricsScrapeInterval != 33 {
		t.Errorf("MetricsScrapeInterval = %d, want 33", cfg.MetricsScrapeInterval)
	}
	// IndexMetrics is the load-bearing wiring for the daemon's OWN primary
	// stream (it sets neither MetricsAddr nor MetricsSource): it must be ON iff
	// the daemon exposes /metrics (upMetricsAddr set), else the index gauges are
	// silently never scraped.
	if cfg.IndexMetrics {
		t.Error("IndexMetrics = true with --metrics-addr unset, want false")
	}
	upMetricsAddr = ":9090"
	if got := watchStreamConfig(passedServerID); !got.IndexMetrics {
		t.Error("IndexMetrics = false with --metrics-addr set, want true")
	}
	if cfg.StartPos != 4 {
		t.Errorf("StartPos = %d, want 4 (binlog magic-number header end)", cfg.StartPos)
	}
	if cfg.GapTimeout != 30 {
		t.Errorf("GapTimeout = %d, want 30", cfg.GapTimeout)
	}
	if cfg.Reset || cfg.NoGapFill {
		t.Errorf("Reset=%v NoGapFill=%v, want both false", cfg.Reset, cfg.NoGapFill)
	}
	if cfg.Deps.ValidateBinlogFormat == nil {
		t.Error("Deps must be wired (streamdeps.Default()), got zero-value Deps")
	}
}

// TestResolveUpConsoleEnvAuthTLS mirrors TestResolveUpConsoleEnv for the
// auth/TLS console vars — the duplicated direct-read blocks (serve vs watch)
// are the established silent-breakage trap for env-only installs; this is the
// watch-side tripwire.
func TestResolveUpConsoleEnvAuthTLS(t *testing.T) {
	saved := [3]string{upConsoleAuthFile, upConsoleTLSCert, upConsoleTLSKey}
	t.Cleanup(func() {
		upConsoleAuthFile, upConsoleTLSCert, upConsoleTLSKey = saved[0], saved[1], saved[2]
	})

	for _, name := range []string{"console-auth-file", "console-tls-cert", "console-tls-key"} {
		if watchCmd.Flags().Lookup(name) == nil {
			t.Fatalf("flag --%s not registered on watchCmd", name)
		}
	}
	newCmd := func() *cobra.Command {
		cmd := &cobra.Command{}
		cmd.Flags().StringVar(&upConsoleAuthFile, "console-auth-file", "", "")
		cmd.Flags().StringVar(&upConsoleTLSCert, "console-tls-cert", "", "")
		cmd.Flags().StringVar(&upConsoleTLSKey, "console-tls-key", "", "")
		return cmd
	}

	t.Setenv("BINTRAIL_CONSOLE_AUTH", "/env/auth.yaml")
	t.Setenv("BINTRAIL_CONSOLE_TLS_CERT", "/env/cert.pem")
	t.Setenv("BINTRAIL_CONSOLE_TLS_KEY", "/env/key.pem")

	// No flags set → env wins.
	upConsoleAuthFile, upConsoleTLSCert, upConsoleTLSKey = "", "", ""
	resolveUpConsoleEnv(newCmd())
	assertStr(t, "upConsoleAuthFile (env)", upConsoleAuthFile, "/env/auth.yaml")
	assertStr(t, "upConsoleTLSCert (env)", upConsoleTLSCert, "/env/cert.pem")
	assertStr(t, "upConsoleTLSKey (env)", upConsoleTLSKey, "/env/key.pem")

	// Explicit flags beat env.
	cmd := newCmd()
	for flag, val := range map[string]string{
		"console-auth-file": "/flag/auth.yaml",
		"console-tls-cert":  "/flag/cert.pem",
		"console-tls-key":   "/flag/key.pem",
	} {
		if err := cmd.Flags().Set(flag, val); err != nil {
			t.Fatalf("set --%s: %v", flag, err)
		}
	}
	resolveUpConsoleEnv(cmd)
	assertStr(t, "upConsoleAuthFile (flag)", upConsoleAuthFile, "/flag/auth.yaml")
	assertStr(t, "upConsoleTLSCert (flag)", upConsoleTLSCert, "/flag/cert.pem")
	assertStr(t, "upConsoleTLSKey (flag)", upConsoleTLSKey, "/flag/key.pem")
}

// TestWatchSSLEnvBinding locks the source-TLS override channel (#879): the
// --ssl-mode/--ssl-ca/--ssl-cert/--ssl-key flags exist on the real watchCmd,
// each is wired to its BINTRAIL_SSL_* env var in watchEnvBindings, and an env
// value propagates to the up* globals via bindWatchEnv. Without this, watch's
// source stream silently connects with ssl-mode=preferred and no override.
func TestWatchSSLEnvBinding(t *testing.T) {
	saved := [4]string{upSSLMode, upSSLCA, upSSLCert, upSSLKey}
	t.Cleanup(func() { upSSLMode, upSSLCA, upSSLCert, upSSLKey = saved[0], saved[1], saved[2], saved[3] })

	// The flags must exist on the REAL watchCmd — a rename in init() not
	// mirrored in watchEnvBindings would silently drop the env override.
	for _, name := range []string{"ssl-mode", "ssl-ca", "ssl-cert", "ssl-key"} {
		if watchCmd.Flags().Lookup(name) == nil {
			t.Fatalf("flag --%s not registered on watchCmd", name)
		}
	}
	// …and each is mapped to its BINTRAIL_SSL_* env var.
	wantEnv := map[string]string{
		"ssl-mode": "BINTRAIL_SSL_MODE",
		"ssl-ca":   "BINTRAIL_SSL_CA",
		"ssl-cert": "BINTRAIL_SSL_CERT",
		"ssl-key":  "BINTRAIL_SSL_KEY",
	}
	got := map[string]string{}
	for _, b := range watchEnvBindings {
		if _, ok := wantEnv[b.Flag]; ok {
			got[b.Flag] = b.EnvVar
		}
	}
	for flag, env := range wantEnv {
		if got[flag] != env {
			t.Errorf("watchEnvBindings[%q] = %q, want %q", flag, got[flag], env)
		}
	}

	// bindWatchEnv applies the env to the bound flags (a throwaway cmd bound to
	// the same globals, mirroring how init() binds watchCmd).
	cmd := &cobra.Command{}
	cmd.Flags().StringVar(&upSSLMode, "ssl-mode", "preferred", "")
	cmd.Flags().StringVar(&upSSLCA, "ssl-ca", "", "")
	cmd.Flags().StringVar(&upSSLCert, "ssl-cert", "", "")
	cmd.Flags().StringVar(&upSSLKey, "ssl-key", "", "")
	t.Setenv("BINTRAIL_SSL_MODE", "verify-identity")
	t.Setenv("BINTRAIL_SSL_CA", "/env/ca.pem")
	t.Setenv("BINTRAIL_SSL_CERT", "/env/cert.pem")
	t.Setenv("BINTRAIL_SSL_KEY", "/env/key.pem")
	bindWatchEnv(cmd)
	assertStr(t, "upSSLMode (env)", upSSLMode, "verify-identity")
	assertStr(t, "upSSLCA (env)", upSSLCA, "/env/ca.pem")
	assertStr(t, "upSSLCert (env)", upSSLCert, "/env/cert.pem")
	assertStr(t, "upSSLKey (env)", upSSLKey, "/env/key.pem")
}

// TestRotateTargets covers the per-source archive wiring: the boot index is
// drop-only; a source whose registry entry has an Archive S3 bucket archives
// to it (with a per-entry staging dir + resolved bintrail_id); a source with
// no bucket, or whose bintrail_id is unresolved, rotates drop-only.
func TestRotateTargets(t *testing.T) {
	reg, err := console.LoadRegistry("") // in-memory
	if err != nil {
		t.Fatal(err)
	}
	arch, err := reg.Add(console.ServerEntry{Name: "src-archived", ArchiveS3: "s3://bucket/prefix/"})
	if err != nil {
		t.Fatal(err)
	}
	plain, err := reg.Add(console.ServerEntry{Name: "src-plain"})
	if err != nil {
		t.Fatal(err)
	}
	pendingID, err := reg.Add(console.ServerEntry{Name: "src-archived-pending", ArchiveS3: "s3://bucket/pending/"})
	if err != nil {
		t.Fatal(err)
	}

	sup := &monitorSupervisor{
		registry: reg,
		jobs: map[string]*monitorJob{
			arch.ID:      {indexDSN: "dsn-arch"},
			plain.ID:     {indexDSN: "dsn-plain"},
			pendingID.ID: {indexDSN: "dsn-pending"},
			// A job whose registry entry was deleted between cycles (reg.Get
			// returns !ok). It must still rotate — drop-only — never vanish.
			"ghost": {indexDSN: "dsn-ghost"},
		},
	}

	prev := resolveBintrailIDFunc
	resolveBintrailIDFunc = func(dsn string) (string, error) {
		if dsn == "dsn-pending" {
			return "", nil // identity not yet resolved → archive waits
		}
		return "uuid-" + dsn, nil
	}
	t.Cleanup(func() { resolveBintrailIDFunc = prev })

	targets := rotateTargets("boot-dsn", bootIdle, sup, reg, "/stage")
	byDSN := map[string]rotation.RotateTarget{}
	for _, tg := range targets {
		byDSN[tg.DSN] = tg
	}

	if boot := byDSN["boot-dsn"]; boot.ArchiveS3 != "" || !boot.NoWriter {
		t.Errorf("boot index of a source-less watch must be drop-only and marked writerless, got %+v", boot)
	}
	// Every per-source index has a writer.
	for _, dsn := range []string{"dsn-arch", "dsn-plain", "dsn-pending", "dsn-ghost"} {
		if byDSN[dsn].NoWriter {
			t.Errorf("%s: a per-source index has a writer", dsn)
		}
	}
	// With the main stream writing the boot index, nothing is writerless.
	for _, tg := range rotateTargets("boot-dsn", bootStreamed, sup, reg, "/stage") {
		if tg.NoWriter {
			t.Errorf("source-ful watch: %s marked writerless", tg.DSN)
		}
	}
	a := byDSN["dsn-arch"]
	if a.ArchiveS3 != "s3://bucket/prefix/" || a.BintrailID != "uuid-dsn-arch" {
		t.Errorf("archived source target wrong: %+v", a)
	}
	if a.ArchiveDir != "/stage/"+arch.ID {
		t.Errorf("staging dir = %q, want /stage/%s", a.ArchiveDir, arch.ID)
	}
	if a.ArchiveCompression != "zstd" {
		t.Errorf("compression = %q, want zstd", a.ArchiveCompression)
	}
	if p := byDSN["dsn-plain"]; p.ArchiveS3 != "" {
		t.Errorf("no-bucket source must be drop-only, got %+v", p)
	}
	if pend := byDSN["dsn-pending"]; pend.ArchiveS3 != "" {
		t.Errorf("unresolved-bintrail_id source must rotate drop-only until resolved, got %+v", pend)
	}
	if ghost, ok := byDSN["dsn-ghost"]; !ok {
		t.Error("a job whose registry entry was deleted must still produce a (drop-only) target, not vanish")
	} else if ghost.ArchiveS3 != "" {
		t.Errorf("deleted-entry job must rotate drop-only, got %+v", ghost)
	}
}

// TestRotationSettingsProvider pins the live-settings precedence the console
// rotation panel relies on: a valid saved policy wins (and is marked Explicit so
// the upgrade guard is skipped for an operator-typed window), no policy falls
// back to the daemon defaults, and an invalid saved policy ALSO falls back
// (never silently disables rotation).
func TestRotationSettingsProvider(t *testing.T) {
	saved := upRotationCfg
	t.Cleanup(func() { upRotationCfg = saved })
	upRotationCfg = rotation.Settings{
		Enabled: true, Retain: 30 * 24 * time.Hour, RetainRaw: "30d",
		Interval: time.Hour, AddFuture: 3, Explicit: false,
	}

	reg, err := console.LoadRegistry("") // in-memory
	if err != nil {
		t.Fatal(err)
	}
	prov := rotationSettingsProvider(reg)

	// No saved policy → daemon defaults verbatim.
	if s := prov(); s.RetainRaw != "30d" || s.Interval != time.Hour || s.AddFuture != 3 {
		t.Errorf("no override should yield the daemon defaults, got %+v", s)
	}

	// A valid saved policy wins and is Explicit (so a long-lived index doesn't
	// trip the upgrade guard on an operator's deliberate window).
	if err := reg.SetRotation(console.RotationConfig{Retain: "7d", Interval: "15m", AddFuture: 5}); err != nil {
		t.Fatal(err)
	}
	s := prov()
	if s.RetainRaw != "7d" || s.Interval != 15*time.Minute || s.AddFuture != 5 {
		t.Errorf("valid override should win, got %+v", s)
	}
	if !s.Explicit {
		t.Error("a console-set policy must be Explicit so the upgrade guard is skipped")
	}

	// An invalid saved policy (bad retain) must fall back to the defaults, not
	// disable rotation.
	if err := reg.SetRotation(console.RotationConfig{Retain: "garbage", Interval: "15m", AddFuture: 5}); err != nil {
		t.Fatal(err)
	}
	if s := prov(); s.RetainRaw != "30d" {
		t.Errorf("an invalid override must fall back to the daemon defaults, got %+v", s)
	}
}

func TestArchiveStagingEnvFallback(t *testing.T) {
	saved := upArchiveStageDir
	t.Cleanup(func() { upArchiveStageDir = saved })
	if watchCmd.Flags().Lookup("archive-staging-dir") == nil {
		t.Fatal("flag --archive-staging-dir not registered on watchCmd")
	}
	cmd := &cobra.Command{}
	cmd.Flags().StringVar(&upArchiveStageDir, "archive-staging-dir", "", "")
	t.Setenv("BINTRAIL_CONSOLE_ARCHIVE_STAGING", "/env/staging")
	upArchiveStageDir = ""
	resolveUpConsoleEnv(cmd)
	if upArchiveStageDir != "/env/staging" {
		t.Errorf("archive-staging-dir from env = %q, want /env/staging", upArchiveStageDir)
	}
}

// TestStartBaselinePruneLoop_gating pins the pre-goroutine contract: a malformed
// --baseline-retain fails the daemon fast, retention-unset is a no-op, and a valid
// retain returns nil (the goroutine self-determines targets each cycle). It uses a
// nil registry and an immediately-cancelled ctx so no live prune cycle runs.
func TestStartBaselinePruneLoop_gating(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel up front: the goroutine's initial sweep is skipped (ctx.Err != nil)

	// A malformed retain value is fatal BEFORE the goroutine starts.
	if err := startBaselinePruneLoop(ctx, nil, "/d", "s3://b/p", "garbage", time.Hour); err == nil {
		t.Error("malformed --baseline-retain must error before the goroutine starts")
	}
	// Retention unset → nil, no loop.
	if err := startBaselinePruneLoop(ctx, nil, "/d", "s3://b/p", "", time.Hour); err != nil {
		t.Errorf("retention unset must be a nil no-op, got %v", err)
	}
	// Valid retain (even with no usable target yet) → nil; targets are resolved
	// per-cycle so a server added later is covered.
	if err := startBaselinePruneLoop(ctx, nil, "", "", "7d", time.Hour); err != nil {
		t.Errorf("valid retain must not error, got %v", err)
	}
}

// TestBaselinePruneTargets pins target collection: the global pair plus every
// registry server with BOTH a dir and an S3 prefix, dir/s3-only entries skipped,
// and a server reusing the global dir deduped.
func TestBaselinePruneTargets(t *testing.T) {
	entries := []console.ServerEntry{
		{BaselineDir: "/srv1/base", BaselineS3: "s3://b/srv1/"}, // both → target
		{BaselineDir: "/srv2/base"},                             // dir only → skip (only copy)
		{BaselineS3: "s3://b/srv3/"},                            // s3 only → skip (nothing local)
		{BaselineDir: "/global", BaselineS3: "s3://b/global/"},  // duplicate of the global pair
	}
	got := baselinePruneTargets(entries, "/global", "s3://b/global/")
	want := map[string]string{"/global": "s3://b/global/", "/srv1/base": "s3://b/srv1/"}
	if len(got) != len(want) {
		t.Fatalf("got %d targets %+v, want %d", len(got), got, len(want))
	}
	for _, tgt := range got {
		if want[tgt.dir] != tgt.s3 {
			t.Errorf("unexpected target %+v", tgt)
		}
	}

	if n := len(baselinePruneTargets(nil, "", "")); n != 0 {
		t.Errorf("no config → no targets, got %d", n)
	}
	if n := len(baselinePruneTargets(nil, "/d", "")); n != 0 {
		t.Errorf("global dir without S3 → no target (only copy), got %d", n)
	}
}

// TestRunBaselinePruneCycle pins the per-target CONSUMPTION (the point of commit
// 3, which TestBaselinePruneTargets only pins at the collection boundary): each
// deduped (dir, s3) target gets exactly one PruneLocal-shaped call with matching
// options, and a failure on one target does not stop the others.
func TestRunBaselinePruneCycle(t *testing.T) {
	entries := []console.ServerEntry{
		{BaselineDir: "/srv1", BaselineS3: "s3://b/srv1"},
		{BaselineDir: "/global", BaselineS3: "s3://b/global"}, // duplicate of the global pair
		{BaselineDir: "/srv2"},                                // dir-only → no target
	}
	targets := baselinePruneTargets(entries, "/global", "s3://b/global")

	var calls []baseline.PruneOptions
	pruneFn := func(_ context.Context, o baseline.PruneOptions) (baseline.PruneResult, error) {
		calls = append(calls, o)
		if o.LocalDir == "/srv1" {
			return baseline.PruneResult{}, errors.New("boom") // one target fails
		}
		return baseline.PruneResult{Pruned: []string{"x"}}, nil
	}
	runBaselinePruneCycle(context.Background(), targets, 7*24*time.Hour, pruneFn)

	// Two deduped targets (/global, /srv1) each called once; the /srv1 error did
	// not stop /global.
	want := map[string]string{"/global": "s3://b/global", "/srv1": "s3://b/srv1"}
	if len(calls) != len(want) {
		t.Fatalf("got %d prune calls %+v, want %d", len(calls), calls, len(want))
	}
	for _, c := range calls {
		if want[c.LocalDir] != c.S3URL {
			t.Errorf("unexpected prune call %+v", c)
		}
		if c.Retain != 7*24*time.Hour {
			t.Errorf("Retain = %v, want 7d", c.Retain)
		}
	}
}

// TestMainSourceJobInfo covers the pure flavor-resolution + SourceJobInfo
// construction for watch's main source (the wiring in runUpStreamWithConsole
// that fires ext.RunSourceJobs for the daemon's --source-dsn stream). The
// live-daemon firing itself is covered in monitor_integration_test.go for the
// supervised path; this pins the main-source construction without a daemon.
func TestMainSourceJobInfo(t *testing.T) {
	// Empty stream flavor (watchStreamConfig leaves Flavor unset; streamrun.One
	// normalizes it to mysql) → the canonical non-empty "mysql", matching the
	// value `bintrail up` supplies.
	got := mainSourceJobInfo("user:pass@tcp(h:3306)/db", "idx-dsn", "")
	want := ext.SourceJobInfo{SourceDSN: "user:pass@tcp(h:3306)/db", IndexDSN: "idx-dsn", Flavor: "mysql"}
	if got != want {
		t.Errorf("empty flavor: got %+v, want %+v", got, want)
	}

	// A non-empty stream flavor is carried through verbatim, so if watch ever
	// grows a --source-flavor for its main source the job sees it unchanged.
	got = mainSourceJobInfo("src", "idx", "mariadb")
	want = ext.SourceJobInfo{SourceDSN: "src", IndexDSN: "idx", Flavor: "mariadb"}
	if got != want {
		t.Errorf("mariadb flavor: got %+v, want %+v", got, want)
	}
}

// TestUpConsoleConfig_baselineRefreshDefaultsReachTheConsole pins the READ path
// of the reuse setting: daemon flag and env -> console.Config -> the panel.
//
// The write path (console toggle -> fold) is pinned hop by hop; this direction
// had nothing, and it is the one the panel's whole job depends on. Deleting the
// BaselineRefreshDefaults block in upConsoleConfig, or hardcoding Enabled to
// true, would leave the card reporting a setting the daemon is not running,
// which is worse than no card: the operator's consent would be recorded against
// the wrong behaviour.
//
// Enabled is asserted in BOTH directions on purpose. It is the difference
// between "this applies now" and "this applies after a restart", and a mutation
// that pins it to one value passes every test that only ever asks for that one.
func TestUpConsoleConfig_baselineRefreshDefaultsReachTheConsole(t *testing.T) {
	const dsn = "user:pass@tcp(127.0.0.1:3306)/binlog_index"
	opts := consoleOpts{Listen: "127.0.0.1:8090", Token: "tok"}

	prevCarry, prevEvery, prevTrig := upBaselineCarryForward, upBaselineRefreshEvery, upConsoleBaselineTrigger
	t.Cleanup(func() {
		upBaselineCarryForward, upBaselineRefreshEvery, upConsoleBaselineTrigger = prevCarry, prevEvery, prevTrig
	})

	for _, tc := range []struct {
		name          string
		carry         bool
		every         string
		trigger       bool
		wantCarry     bool
		wantEnabled   bool
		wantScheduled bool
	}{
		{"nothing on, reuse off", false, "", false, false, false, false},
		// Reuse asked for with no consumer at all. The panel must report it
		// dormant rather than live.
		{"nothing on, reuse on", true, "", false, true, false, false},
		{"scheduled, reuse off", false, "30m", false, false, true, true},
		{"scheduled, reuse on", true, "30m", false, true, true, true},
		// The case the second review found: --baseline-trigger alone wires the
		// point-in-time restore, which consumes this setting, while no refresh
		// loop runs. Enabled must be true (a restore WILL reuse files today)
		// and Scheduled false (nothing is on a timer). Reading liveness off the
		// interval alone told the operator nothing was live and then hard
		// linked their files.
		{"trigger only, reuse on", true, "", true, true, true, false},
		{"trigger only, reuse off", false, "", true, false, true, false},
		{"trigger and schedule", true, "30m", true, true, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			upBaselineCarryForward, upBaselineRefreshEvery = tc.carry, tc.every
			upConsoleBaselineTrigger = tc.trigger
			cfg, err := upConsoleConfig(nil, dsn, opts, nil)
			if err != nil {
				t.Fatalf("upConsoleConfig: %v", err)
			}
			got := cfg.BaselineRefreshDefaults
			if got.CarryForwardUnchanged != tc.wantCarry {
				t.Errorf("CarryForwardUnchanged=%v, want %v: the daemon flag did not reach the console",
					got.CarryForwardUnchanged, tc.wantCarry)
			}
			if got.Enabled != tc.wantEnabled {
				t.Errorf("Enabled=%v, want %v: the panel would misreport whether ANY consumer of the setting is live",
					got.Enabled, tc.wantEnabled)
			}
			if got.Scheduled != tc.wantScheduled {
				t.Errorf("Scheduled=%v, want %v: the panel would misreport whether a refresh loop is on a timer",
					got.Scheduled, tc.wantScheduled)
			}
		})
	}
}

// TestResolveUpConsoleEnv_carryForwardPrecedence: an explicit flag beats the
// environment, and an unreadable environment value beats nothing.
//
// Both halves are load-bearing and neither was covered. An env var that
// overrode an explicit --baseline-carry-forward-unchanged=false would change
// the on-disk shape of an operator's backups against their written instruction,
// and a value like "yes" silently turning it on is the "unreadable value must
// never be read as consent" rule this parse exists for.
func TestResolveUpConsoleEnv_carryForwardPrecedence(t *testing.T) {
	prev := upBaselineCarryForward
	t.Cleanup(func() { upBaselineCarryForward = prev })

	newCmd := func() *cobra.Command {
		cmd := &cobra.Command{Use: "watch"}
		cmd.Flags().BoolVar(&upBaselineCarryForward, "baseline-carry-forward-unchanged", false, "")
		return cmd
	}

	for _, tc := range []struct {
		name    string
		env     string
		flagSet string // "" = not passed
		want    bool
	}{
		{"env on, no flag", "true", "", true},
		{"env off, no flag", "false", "", false},
		{"unset, no flag", "", "", false},
		// Not a true/false value: keeps the default, never read as consent.
		{"env says yes, no flag", "yes", "", false},
		{"env says on, no flag", "on", "", false},
		{"env typo, no flag", "ture", "", false},
		// An explicit flag is the operator's written instruction; the
		// environment must not overrule it in either direction.
		{"env on, flag says false", "true", "false", false},
		{"env off, flag says true", "false", "true", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			upBaselineCarryForward = false
			t.Setenv("BINTRAIL_BASELINE_CARRY_FORWARD_UNCHANGED", tc.env)
			cmd := newCmd()
			if tc.flagSet != "" {
				if err := cmd.Flags().Set("baseline-carry-forward-unchanged", tc.flagSet); err != nil {
					t.Fatal(err)
				}
			}
			if err := resolveUpConsoleEnv(cmd); err != nil {
				t.Fatalf("resolveUpConsoleEnv: %v", err)
			}
			if upBaselineCarryForward != tc.want {
				t.Errorf("carry-forward = %v, want %v (env=%q flag=%q)",
					upBaselineCarryForward, tc.want, tc.env, tc.flagSet)
			}
		})
	}
}

// TestWatchCarryForwardFlagIsOffByDefault asserts the default on the REAL
// watchCmd declaration.
//
// TestResolveUpConsoleEnv_carryForwardPrecedence builds its own cobra command
// and re-declares the flag with a hardcoded false, which is right for testing
// precedence and blind to the shipped default: it substitutes its fixture for
// the production declaration rather than reading it. This reads the real one.
func TestWatchCarryForwardFlagIsOnByDefault(t *testing.T) {
	f := watchCmd.Flags().Lookup("baseline-carry-forward-unchanged")
	if f == nil {
		t.Fatal("--baseline-carry-forward-unchanged is gone from watch; it is the only way to turn reuse off now")
	}
	if f.DefValue != "true" {
		t.Fatalf("default = %q, want \"true\": since #1681 reusing a table that did not change is always on, "+
			"and the console no longer offers to turn it on", f.DefValue)
	}
}

// TestStartBaselineRefreshLoopCallSitesAgree: both watch entry paths must pass
// the same flag variable.
//
// The two calls are byte-identical including their context, which is exactly
// where a copy-paste divergence hides: `watch` with a source would ignore the
// flag while source-less `watch` honoured it, and each line can be mutated
// alone with the whole suite green. A source read is the cheapest thing that
// sees both.
func TestStartBaselineRefreshLoopCallSitesAgree(t *testing.T) {
	src, err := os.ReadFile("watch.go")
	if err != nil {
		t.Fatal(err)
	}
	const want = "upBaselineRefreshEvery, upBaselineCarryForward)"
	n := strings.Count(string(src), "startBaselineRefreshLoop(")
	if n != 2 {
		t.Fatalf("found %d startBaselineRefreshLoop call sites, expected 2; re-point this guard", n)
	}
	if got := strings.Count(string(src), want); got != n {
		t.Errorf("%d of %d call sites pass the daemon flag; the other passes something else, so one watch "+
			"entry path would silently ignore --baseline-carry-forward-unchanged", got, n)
	}
}

// A snapshot old enough to reclaim whose copy is not at the destination stays
// on this disk forever, and since #1539 a scheduled update whose upload failed
// produces exactly that. The prune cycle used to log only what it reclaimed,
// so the accumulation was invisible after the single line at the moment of
// failure — while the Backups page suppresses its disk warning for any server
// that HAS a destination.
func TestRunBaselinePruneCycle_reportsWhatItCouldNotReclaim(t *testing.T) {
	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })
	var buf bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))

	targets := []baselinePruneTarget{{dir: "/b", s3: "s3://bucket/backups/"}}
	runBaselinePruneCycle(context.Background(), targets, 7*24*time.Hour,
		func(context.Context, baseline.PruneOptions) (baseline.PruneResult, error) {
			return baseline.PruneResult{KeptNotDurable: 3, ProbeErrors: 1}, nil
		})

	out := buf.String()
	if !strings.Contains(out, "kept=3") {
		t.Errorf("the snapshots that could not be reclaimed were not reported: %s", out)
	}
	if !strings.Contains(out, "unchecked=1") {
		t.Errorf("the snapshots whose durability could not be checked were not reported: %s", out)
	}
	if !strings.Contains(out, "s3://bucket/backups/") {
		t.Errorf("the report does not name the destination the copies are missing from: %s", out)
	}
}

// TestWatchTableDeltasFlagIsOnByDefault: against the real command, as for the
// carry-forward flag above. Table deltas (#1638) are the default since #1729;
// the flag and the environment variable exist to turn them off.
func TestWatchTableDeltasFlagIsOnByDefault(t *testing.T) {
	f := watchCmd.Flags().Lookup("baseline-table-deltas")
	if f == nil {
		t.Fatal("--baseline-table-deltas is gone from watch; this guard covers nothing")
	}
	if f.DefValue != "true" {
		t.Fatalf("default = %q, want \"true\"", f.DefValue)
	}
}

// TestResolveUpConsoleEnv_tableDeltasPrecedence: the same table the
// carry-forward setting is held to, with the default now ON (#1729): a value
// that is not true/false keeps the default (it is never read as a refusal
// either), and an explicit flag beats the environment both ways.
func TestResolveUpConsoleEnv_tableDeltasPrecedence(t *testing.T) {
	prev := upBaselineTableDeltas
	t.Cleanup(func() { upBaselineTableDeltas = prev })

	newCmd := func() *cobra.Command {
		cmd := &cobra.Command{Use: "watch"}
		cmd.Flags().BoolVar(&upBaselineTableDeltas, "baseline-table-deltas", true, "")
		return cmd
	}
	for _, tc := range []struct {
		name    string
		env     string
		flagSet string
		want    bool
	}{
		{"env on, no flag", "true", "", true},
		{"env 1, no flag", "1", "", true},
		{"env off, no flag", "false", "", false},
		{"env 0, no flag", "0", "", false},
		{"unset, no flag", "", "", true},
		{"env says no, no flag", "no", "", true},
		{"env typo, no flag", "flase", "", true},
		{"env on, flag says false", "true", "false", false},
		{"env off, flag says true", "false", "true", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			upBaselineTableDeltas = true
			t.Setenv("BINTRAIL_BASELINE_TABLE_DELTAS", tc.env)
			cmd := newCmd()
			if tc.flagSet != "" {
				if err := cmd.Flags().Set("baseline-table-deltas", tc.flagSet); err != nil {
					t.Fatal(err)
				}
			}
			if err := resolveUpConsoleEnv(cmd); err != nil {
				t.Fatalf("resolveUpConsoleEnv: %v", err)
			}
			if upBaselineTableDeltas != tc.want {
				t.Errorf("table deltas = %v, want %v (env=%q flag=%q)", upBaselineTableDeltas, tc.want, tc.env, tc.flagSet)
			}
		})
	}
}

// TestRefreshFoldConfig_carriesTableDeltas: the last hop from the daemon flag
// to the fold. A restore shares foldSnapshot and must NOT inherit it, which is
// why the request carries it instead of the config reading the flag.
func TestRefreshFoldConfig_carriesTableDeltas(t *testing.T) {
	at := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	for _, on := range []bool{false, true} {
		cfg := refreshFoldConfig(refreshRequest{IndexDSN: "dsn", BaselineDir: "/b", TableDeltas: on}, at, []string{"s.t"})
		if cfg.TableDeltas != on {
			t.Errorf("TableDeltas = %v, want %v", cfg.TableDeltas, on)
		}
	}
}
