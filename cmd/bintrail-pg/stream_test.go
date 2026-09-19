package main

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"

	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/rotation"
)

// resetPGFlags restores the stream command's package-global flag vars to their
// declared defaults so each subtest starts clean — pgStreamConfigFromFlags
// writes the env fallback back into the globals, so they must be reset between
// cases. These subtests mutate shared globals and must NOT run in parallel.
func resetPGFlags() {
	pgIndexDSN = ""
	pgReplDSN = ""
	pgQueryDSN = ""
	pgSlot = ""
	pgPublication = ""
	pgServerID = 0
	pgStartLSN = ""
	pgSchemas = ""
	pgTables = ""
	pgBatchSize = 1000
	pgCheckpoint = 5
	pgPartitions = 48
}

// clearPGEnv blanks the BINTRAIL_PG_* vars for the test. t.Setenv to "" reads as
// unset in applyEnvFallback (which treats empty as absent), so a developer's
// shell environment cannot leak into these table-driven cases.
func clearPGEnv(t *testing.T) {
	t.Helper()
	for _, v := range []string{
		"BINTRAIL_PG_REPL_DSN", "BINTRAIL_PG_QUERY_DSN", "BINTRAIL_PG_SLOT",
		"BINTRAIL_PG_PUBLICATION", "BINTRAIL_PG_START_LSN",
	} {
		t.Setenv(v, "")
	}
}

func TestPGStreamConfigFromFlags_MissingRequired(t *testing.T) {
	clearPGEnv(t)
	resetPGFlags()
	// index-dsn/server-id are enforced by MarkFlagRequired at the cobra layer,
	// not here; this seam only validates the PG-specific connection settings.

	_, err := pgStreamConfigFromFlags()
	if err == nil {
		t.Fatal("expected an error when all PG connection settings are missing, got nil")
	}
	for _, want := range []string{"--repl-dsn", "--query-dsn", "--slot", "--publication"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q should name the missing flag %q", err, want)
		}
	}
}

func TestPGStreamConfigFromFlags_HappyPathFromFlags(t *testing.T) {
	clearPGEnv(t)
	resetPGFlags()
	pgIndexDSN = "user:pass@tcp(localhost:3306)/bintrail_index"
	pgReplDSN = "postgres://u@localhost/db?replication=database"
	pgQueryDSN = "postgres://u@localhost/db"
	pgSlot = "bintrail_slot"
	pgPublication = "bintrail_pub"
	pgServerID = 42
	pgSchemas = "public"
	pgTables = "public.orders"
	pgBatchSize = 500
	pgCheckpoint = 12
	pgPartitions = 24

	cfg, err := pgStreamConfigFromFlags()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.IndexDSN != pgIndexDSN || cfg.ReplDSN != pgReplDSN || cfg.QueryDSN != pgQueryDSN {
		t.Errorf("DSN passthrough mismatch: %+v", cfg)
	}
	if cfg.SlotName != "bintrail_slot" || cfg.Publication != "bintrail_pub" {
		t.Errorf("slot/publication mismatch: %+v", cfg)
	}
	if cfg.ServerID != 42 || cfg.Schemas != "public" || cfg.Tables != "public.orders" {
		t.Errorf("filter/server-id mismatch: %+v", cfg)
	}
	if cfg.BatchSize != 500 || cfg.Partitions != 24 {
		t.Errorf("batch/partitions mismatch: %+v", cfg)
	}
	if cfg.Checkpoint != 12*time.Second {
		t.Errorf("Checkpoint = %v, want 12s (the --checkpoint int is seconds)", cfg.Checkpoint)
	}
	if cfg.StartLSN != 0 {
		t.Errorf("StartLSN = %d, want 0 when --start-lsn is unset", cfg.StartLSN)
	}
}

func TestPGStreamConfigFromFlags_EnvFallback(t *testing.T) {
	clearPGEnv(t)
	resetPGFlags()
	pgIndexDSN = "idx"
	pgServerID = 7
	// All PG-specific settings supplied via env only (no CLI flag).
	t.Setenv("BINTRAIL_PG_REPL_DSN", "repl-from-env")
	t.Setenv("BINTRAIL_PG_QUERY_DSN", "query-from-env")
	t.Setenv("BINTRAIL_PG_SLOT", "slot-from-env")
	t.Setenv("BINTRAIL_PG_PUBLICATION", "pub-from-env")

	cfg, err := pgStreamConfigFromFlags()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.ReplDSN != "repl-from-env" || cfg.QueryDSN != "query-from-env" ||
		cfg.SlotName != "slot-from-env" || cfg.Publication != "pub-from-env" {
		t.Errorf("env fallback not applied: %+v", cfg)
	}
}

func TestPGStreamConfigFromFlags_FlagWinsOverEnv(t *testing.T) {
	clearPGEnv(t)
	resetPGFlags()
	pgIndexDSN = "idx"
	pgServerID = 7
	pgReplDSN = "repl-from-flag" // CLI flag set
	pgQueryDSN = "query-from-flag"
	pgSlot = "slot-from-flag"
	pgPublication = "pub-from-flag"
	t.Setenv("BINTRAIL_PG_REPL_DSN", "repl-from-env") // env also set — flag must win

	cfg, err := pgStreamConfigFromFlags()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.ReplDSN != "repl-from-flag" {
		t.Errorf("ReplDSN = %q, want the CLI flag value to win over env", cfg.ReplDSN)
	}
}

func TestPGStreamConfigFromFlags_StartLSN(t *testing.T) {
	clearPGEnv(t)

	t.Run("invalid", func(t *testing.T) {
		resetPGFlags()
		setPGRequired()
		pgStartLSN = "not-an-lsn"
		_, err := pgStreamConfigFromFlags()
		if err == nil || !strings.Contains(err.Error(), "start-lsn") {
			t.Fatalf("expected a start-lsn parse error, got %v", err)
		}
	})

	t.Run("valid", func(t *testing.T) {
		resetPGFlags()
		setPGRequired()
		pgStartLSN = "16/B374D848"
		cfg, err := pgStreamConfigFromFlags()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want, _ := pglogrepl.ParseLSN("16/B374D848")
		if cfg.StartLSN != uint64(want) {
			t.Errorf("StartLSN = %d, want %d", cfg.StartLSN, uint64(want))
		}
	})

	// start-lsn is the one connection setting whose env binding is otherwise
	// only exercised indirectly — drive it through BINTRAIL_PG_START_LSN so a
	// typo in that fallback's env-var name would fail this test.
	t.Run("valid-from-env", func(t *testing.T) {
		resetPGFlags()
		setPGRequired()
		t.Setenv("BINTRAIL_PG_START_LSN", "16/B374D848")
		cfg, err := pgStreamConfigFromFlags()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want, _ := pglogrepl.ParseLSN("16/B374D848")
		if cfg.StartLSN != uint64(want) {
			t.Errorf("StartLSN = %d, want %d (env fallback)", cfg.StartLSN, uint64(want))
		}
	})
}

// TestPGStreamCmd_defaults pins the registered cobra flag defaults so a drift in
// stream.go's IntVar registrations (e.g. --batch-size 1000→100) fails a test —
// resetPGFlags() hardcodes these same literals, so without this guard a default
// change would silently diverge from the documented user-facing behavior. Mirrors
// cmd/bintrail/stream_test.go's TestStreamCmd_defaults.
func TestPGStreamCmd_defaults(t *testing.T) {
	cases := []struct {
		flag string
		want string
	}{
		{"batch-size", "1000"},
		{"checkpoint", "5"},
		{"partitions", "48"},
		{"rotate-retain", indexer.DefaultRotateRetain},
		{"rotate-interval", "1h"},
		{"rotate-add-future", "3"},
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

// TestPGStreamCmd_rotationDefaultsEnableRetention pins the load-bearing #951
// contract: the registered --rotate-* flag defaults must parse into an ENABLED
// rotation, so a fresh `bintrail-pg stream` bounds its own index without any
// operator action (a PG install has no `up` command — stream is its only
// daemon). explicit=false (running on the built-in default, not operator-set)
// arms the upgrade guard that protects a pre-existing index's deep history.
//
// The WINDOW is asserted against indexer.DefaultRotateRetain rather than a
// literal (#1709): it moved from 30 days to 48 hours, and each index now
// records the one it was created under, so a literal here would pin this
// binary to a number the rotation loop no longer uses.
func TestPGStreamCmd_rotationDefaultsEnableRetention(t *testing.T) {
	retain := streamCmd.Flag("rotate-retain").DefValue
	interval := streamCmd.Flag("rotate-interval").DefValue
	addFuture, err := strconv.Atoi(streamCmd.Flag("rotate-add-future").DefValue)
	if err != nil {
		t.Fatalf("rotate-add-future default not an int: %v", err)
	}
	s, err := rotation.ParseSettings(retain, interval, addFuture, false)
	if err != nil {
		t.Fatalf("the registered rotation defaults must parse: %v", err)
	}
	if !s.Enabled {
		t.Error("built-in rotation must be enabled by default (safe-by-default retention, #951)")
	}
	wantRetain, err := cliutil.ParseRetain(indexer.DefaultRotateRetain)
	if err != nil {
		t.Fatalf("the built-in default must parse: %v", err)
	}
	if s.Retain != wantRetain {
		t.Errorf("default retain = %v, want %v (%s)", s.Retain, wantRetain, indexer.DefaultRotateRetain)
	}
	if s.AddFuture != 3 {
		t.Errorf("default add-future = %d, want 3", s.AddFuture)
	}
	if s.Explicit {
		t.Error("Explicit must be false for the built-in default so the upgrade guard stays armed")
	}
}

// TestPGStreamCmd_rotationOffDisables confirms `--rotate-retain off` fully
// disables the built-in loop, the documented opt-out for operators who run
// retention separately (e.g. a standalone `bintrail-pg rotate` daemon).
func TestPGStreamCmd_rotationOffDisables(t *testing.T) {
	s, err := rotation.ParseSettings("off", "1h", 3, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s.Enabled {
		t.Error(`--rotate-retain "off" must disable the built-in rotation loop`)
	}
}

// TestMaintenanceCommandsRegisteredOnRoot pins the #951 contract on the REAL
// bintrail-pg root: a PostgreSQL-only install must expose both `rotate` and
// `archive` so it can bound its index without the core MySQL binary. This walks
// the actual rootCmd (populated by main.go's init → cli.AddMaintenanceCommands),
// not a throwaway command, so dropping the registration call — or one of the two
// commands from it — fails here.
func TestMaintenanceCommandsRegisteredOnRoot(t *testing.T) {
	got := map[string]bool{}
	for _, c := range rootCmd.Commands() {
		got[c.Name()] = true
	}
	for _, want := range []string{"rotate", "archive"} {
		if !got[want] {
			t.Errorf("bintrail-pg root is missing the %q command (cli.AddMaintenanceCommands not wired?)", want)
		}
	}
}

// TestPGStreamCmd_rotateRetainExplicitWiring pins the flag `runPGStream` reads to
// arm the upgrade guard. Setting --rotate-retain on streamCmd must flip
// Changed("rotate-retain") to true, which is exactly the `explicit` argument
// passed to rotation.ParseSettings — a wrong flag name there would engage the
// guard even when the operator DID choose a retention, silently refusing drops.
// Mirrors cliapp/up_rotation_test.go's TestRunUp_explicitRetentionWiring.
func TestPGStreamCmd_rotateRetainExplicitWiring(t *testing.T) {
	flag := streamCmd.Flags().Lookup("rotate-retain")
	if flag == nil {
		t.Fatal("--rotate-retain not registered on streamCmd")
	}
	savedChanged, savedValue := flag.Changed, flag.Value.String()
	t.Cleanup(func() {
		flag.Changed = savedChanged
		_ = flag.Value.Set(savedValue)
	})

	// Never set → not explicit (guard armed, protects deep history).
	flag.Changed = false
	if s, _ := rotation.ParseSettings(flag.Value.String(), "1h", 3, streamCmd.Flags().Changed("rotate-retain")); s.Explicit {
		t.Error("Explicit must be false when --rotate-retain was never set")
	}

	// Set through the flag set, exactly like CLI/env (BindCommandEnv) would.
	if err := streamCmd.Flags().Set("rotate-retain", "7d"); err != nil {
		t.Fatalf("Set(rotate-retain): %v", err)
	}
	s, err := rotation.ParseSettings(flag.Value.String(), "1h", 3, streamCmd.Flags().Changed("rotate-retain"))
	if err != nil {
		t.Fatalf("ParseSettings: %v", err)
	}
	if !s.Explicit {
		t.Error(`Explicit must be true once --rotate-retain is set — the Changed("rotate-retain") call site is broken`)
	}
}

// setPGRequired fills the PG-specific required settings so a test can exercise
// downstream parsing (e.g. --start-lsn) without tripping the missing-settings
// guard. Callers must resetPGFlags() first.
func setPGRequired() {
	pgReplDSN = "repl"
	pgQueryDSN = "query"
	pgSlot = "slot"
	pgPublication = "pub"
}
