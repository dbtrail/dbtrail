package cliapp

import (
	"reflect"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/streamdeps"
	"github.com/dbtrail/dbtrail/internal/streamrun"
)

// ─── cobra command wiring ─────────────────────────────────────────────────────

// TestStreamCmd_registered verifies that streamCmd is wired into the root command.
func TestStreamCmd_registered(t *testing.T) {
	found := false
	for _, cmd := range rootCmd.Commands() {
		if cmd.Use == "stream" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected 'stream' command to be registered under rootCmd")
	}
}

// TestStreamCmd_requiredFlags verifies that the three required flags are marked
// as required so cobra enforces them before RunE is called.
func TestStreamCmd_requiredFlags(t *testing.T) {
	for _, flagName := range []string{"index-dsn", "source-dsn", "server-id"} {
		ann := streamCmd.Annotations
		_ = ann
		flag := streamCmd.Flag(flagName)
		if flag == nil {
			t.Errorf("flag --%s not registered", flagName)
			continue
		}
		// cobra marks required flags in the Annotations map.
		if streamCmd.Flag(flagName).Annotations["cobra_annotation_bash_completion_one_required_flag"] == nil {
			t.Errorf("flag --%s is not marked required", flagName)
		}
	}
}

// TestStreamCmd_allFlagsRegistered verifies that all expected flags are wired up.
func TestStreamCmd_allFlagsRegistered(t *testing.T) {
	for _, name := range []string{
		"index-dsn", "source-dsn", "source-flavor", "server-id",
		"start-file", "start-pos", "start-gtid",
		"batch-size", "schemas", "tables", "checkpoint", "metrics-addr",
		"ssl-mode", "ssl-ca", "ssl-cert", "ssl-key",
		"reset", "no-gap-fill",
	} {
		if streamCmd.Flag(name) == nil {
			t.Errorf("flag --%s not registered on streamCmd", name)
		}
	}
}

// TestStreamCmd_sourceFlavorDefault verifies --source-flavor defaults to
// empty: an operator who did not declare a flavor gets the one the server
// reports. A "mysql" default would count as a declaration and make every
// undeclared MariaDB refuse to start.
func TestStreamCmd_sourceFlavorDefault(t *testing.T) {
	f := streamCmd.Flag("source-flavor")
	if f == nil {
		t.Fatal("flag --source-flavor not registered")
	}
	if f.DefValue != "" {
		t.Errorf("expected an empty source-flavor default (detect), got %q", f.DefValue)
	}
}

// TestStreamSourceJobsHook pins that the stream's source jobs start from the
// flavor the stream resolved (not the declared, possibly empty, flag) and
// start once.
func TestStreamSourceJobsHook(t *testing.T) {
	var got []string
	hooks := streamSourceJobsHooks(func(f string) { got = append(got, f) })
	if hooks == nil || hooks.OnFlavorResolved == nil {
		t.Fatal("stream hooks must carry OnFlavorResolved")
	}
	hooks.OnFlavorResolved("mariadb")
	hooks.OnFlavorResolved("mariadb")
	if len(got) != 1 || got[0] != "mariadb" {
		t.Errorf("source jobs started %v, want exactly once with mariadb", got)
	}
}

// TestStreamCmd_resetDefaultFalse verifies the --reset flag defaults to false.
func TestStreamCmd_resetDefaultFalse(t *testing.T) {
	f := streamCmd.Flag("reset")
	if f == nil {
		t.Fatal("flag --reset not registered")
	}
	if f.DefValue != "false" {
		t.Errorf("expected default reset=false, got %q", f.DefValue)
	}
}

// TestStreamCmd_sslModeDefault verifies the default ssl-mode is "preferred".
func TestStreamCmd_sslModeDefault(t *testing.T) {
	f := streamCmd.Flag("ssl-mode")
	if f == nil {
		t.Fatal("flag --ssl-mode not registered")
	}
	if f.DefValue != "preferred" {
		t.Errorf("expected default ssl-mode=preferred, got %q", f.DefValue)
	}
}

// TestStreamCmd_sslFlagsEmptyDefaults verifies ssl-ca/cert/key default to "".
func TestStreamCmd_sslFlagsEmptyDefaults(t *testing.T) {
	for _, name := range []string{"ssl-ca", "ssl-cert", "ssl-key"} {
		f := streamCmd.Flag(name)
		if f == nil {
			t.Errorf("flag --%s not registered", name)
			continue
		}
		if f.DefValue != "" {
			t.Errorf("flag --%s: expected empty default, got %q", name, f.DefValue)
		}
	}
}

// TestStreamCmd_defaults verifies that optional flags have the expected defaults.
func TestStreamCmd_defaults(t *testing.T) {
	cases := []struct {
		flag string
		want string
	}{
		{"batch-size", "1000"},
		{"checkpoint", "10"},
		{"start-pos", "4"},
	}
	for _, tc := range cases {
		f := streamCmd.Flag(tc.flag)
		if f == nil {
			t.Errorf("flag --%s not registered", tc.flag)
			continue
		}
		if f.DefValue != tc.want {
			t.Errorf("flag --%s: expected default %q, got %q", tc.flag, tc.want, f.DefValue)
		}
	}
}

// TestStreamCmd_emptyStringDefaults verifies that optional string flags default to "".
func TestStreamCmd_emptyStringDefaults(t *testing.T) {
	for _, name := range []string{"start-file", "start-gtid", "schemas", "tables", "metrics-addr"} {
		f := streamCmd.Flag(name)
		if f == nil {
			t.Errorf("flag --%s not registered", name)
			continue
		}
		if f.DefValue != "" {
			t.Errorf("flag --%s: expected empty default, got %q", name, f.DefValue)
		}
	}
}

// TestStreamCmd_noGapFillFlagRegistered verifies the --no-gap-fill flag exists.
func TestStreamCmd_noGapFillFlagRegistered(t *testing.T) {
	f := streamCmd.Flag("no-gap-fill")
	if f == nil {
		t.Fatal("flag --no-gap-fill not registered on streamCmd")
	}
	if f.DefValue != "false" {
		t.Errorf("expected default no-gap-fill=false, got %q", f.DefValue)
	}
}

// TestStreamConfigFromFlags asserts the strm* → streamrun.Config snapshot — the
// single seam where the flag globals become a by-value config that streamrun.One
// (and, later, the control-plane supervisor) consumes. A flag added to stream
// but not wired through here would silently read its zero value inside
// streamrun.One, so every field is checked with a distinctive value. Mirrors
// TestPopulateStreamFlags' save-and-restore discipline: no t.Parallel().
func TestStreamConfigFromFlags(t *testing.T) {
	orig := struct {
		src, idx, file, gtid, sch, tbl, met, fmtv, ssl, ca, cert, key, flavor string
		sid, pos                                                              uint32
		batch, chk, gap, msi                                                  int
		reset, noGap                                                          bool
	}{
		src: strmSourceDSN, idx: strmIndexDSN, file: strmStartFile,
		gtid: strmStartGTID, sch: strmSchemas, tbl: strmTables,
		met: strmMetricsAddr, fmtv: strmFormat, ssl: strmSSLMode,
		ca: strmSSLCA, cert: strmSSLCert, key: strmSSLKey, flavor: strmFlavor,
		sid: strmServerID, pos: strmStartPos,
		batch: strmBatchSize, chk: strmCheckpoint, gap: strmGapTimeout,
		msi:   strmMetricsScrapeInterval,
		reset: strmReset, noGap: strmNoGapFill,
	}
	t.Cleanup(func() {
		strmSourceDSN, strmIndexDSN, strmStartFile = orig.src, orig.idx, orig.file
		strmStartGTID, strmSchemas, strmTables = orig.gtid, orig.sch, orig.tbl
		strmMetricsAddr, strmFormat, strmSSLMode = orig.met, orig.fmtv, orig.ssl
		strmSSLCA, strmSSLCert, strmSSLKey = orig.ca, orig.cert, orig.key
		strmServerID, strmStartPos = orig.sid, orig.pos
		strmBatchSize, strmCheckpoint, strmGapTimeout = orig.batch, orig.chk, orig.gap
		strmMetricsScrapeInterval = orig.msi
		strmReset, strmNoGapFill = orig.reset, orig.noGap
		strmFlavor = orig.flavor
	})

	strmIndexDSN = "ix:pw@tcp(127.0.0.1:3306)/binlog_index"
	strmSourceDSN = "user:pass@tcp(source.example.com:3306)/src"
	strmFlavor = "mariadb"
	strmServerID = 424242
	strmStartFile = "mysql-bin.000777"
	strmStartPos = 1234
	strmStartGTID = "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"
	strmBatchSize = 333
	strmSchemas = "mydb,otherdb"
	strmTables = "mydb.orders"
	strmCheckpoint = 17
	strmMetricsAddr = ":9191"
	strmMetricsScrapeInterval = 45
	strmSSLMode = "verify-ca"
	strmSSLCA = "/tmp/ca.pem"
	strmSSLCert = "/tmp/cert.pem"
	strmSSLKey = "/tmp/key.pem"
	strmFormat = "json"
	strmReset = true
	strmNoGapFill = true
	strmGapTimeout = 99

	got := streamConfigFromFlags()
	// The Deps seam must be wired (else streamrun.One nil-panics at runtime, and
	// only the Docker-gated monitor integration test would catch a dropped
	// streamdeps.Default() call). Spot-check two fields here so a unit run flags it.
	if got.Deps.BuildIndexFilters == nil || got.Deps.OutputJSON == nil {
		t.Error("streamConfigFromFlags did not wire Deps (streamdeps.Default())")
	}
	// Deps holds func values (not comparable / not == ); this test checks the
	// strm* → field snapshot, so zero it before the struct comparison.
	got.Deps = streamrun.Deps{}
	want := streamrun.Config{
		IndexDSN:              "ix:pw@tcp(127.0.0.1:3306)/binlog_index",
		SourceDSN:             "user:pass@tcp(source.example.com:3306)/src",
		Flavor:                "mariadb",
		ServerID:              424242,
		StartFile:             "mysql-bin.000777",
		StartPos:              1234,
		StartGTID:             "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5",
		BatchSize:             333,
		Schemas:               "mydb,otherdb",
		Tables:                "mydb.orders",
		Checkpoint:            17,
		MetricsAddr:           ":9191",
		MetricsScrapeInterval: 45,
		SSLMode:               "verify-ca",
		SSLCA:                 "/tmp/ca.pem",
		SSLCert:               "/tmp/cert.pem",
		SSLKey:                "/tmp/key.pem",
		Format:                "json",
		Reset:                 true,
		NoGapFill:             true,
		GapTimeout:            99,
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("streamConfigFromFlags mismatch:\n got: %+v\nwant: %+v", got, want)
	}
}

// TestStreamOneRejectsBadConfig: streamrun.One owns its own validation (it must
// not depend on cobra/flag-layer checks once a supervisor builds configs
// programmatically).
func TestStreamOneRejectsBadConfig(t *testing.T) {
	if err := streamrun.One(t.Context(), streamrun.Config{Format: "bogus", GapTimeout: 30}); err == nil ||
		!strings.Contains(err.Error(), "invalid --format") {
		t.Errorf("bad format: err = %v, want invalid --format", err)
	}
	if err := streamrun.One(t.Context(), streamrun.Config{Format: "text", GapTimeout: 0}); err == nil ||
		!strings.Contains(err.Error(), "gap-timeout") {
		t.Errorf("bad gap-timeout: err = %v, want gap-timeout error", err)
	}
	// An unsupported --source-flavor is rejected before any connection is opened.
	// Deps must be wired because the flavor check runs after Deps.validate().
	if err := streamrun.One(t.Context(), streamrun.Config{Deps: streamdeps.Default(), Format: "text", GapTimeout: 30, Flavor: "postgres"}); err == nil ||
		!strings.Contains(err.Error(), "invalid source flavor") {
		t.Errorf("bad flavor: err = %v, want invalid source flavor", err)
	}
}
