package consoleapp

import (
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/console"
)

func TestPGBaselineConfig(t *testing.T) {
	req := console.BaselineRequest{
		SourceDSN:   "postgres://repl:secret@pg:5432/appdb",
		Slot:        "bintrail_slot",
		Publication: "bintrail_pub",
		Schemas:     []string{"public", "shop"},
		Flavor:      "postgres",
	}
	cfg, err := pgBaselineConfig(req, "/out")
	if err != nil {
		t.Fatalf("pgBaselineConfig: %v", err)
	}
	if cfg.QueryDSN != req.SourceDSN {
		t.Errorf("QueryDSN = %q, want the source DSN", cfg.QueryDSN)
	}
	if !strings.Contains(cfg.ReplDSN, "replication=database") {
		t.Errorf("ReplDSN missing replication=database: %q", cfg.ReplDSN)
	}
	if cfg.SlotName != "bintrail_slot" || cfg.Publication != "bintrail_pub" {
		t.Errorf("slot/publication wrong: %q / %q", cfg.SlotName, cfg.Publication)
	}
	if cfg.OutputDir != "/out" || cfg.Compression != "zstd" {
		t.Errorf("output/compression wrong: %q / %q", cfg.OutputDir, cfg.Compression)
	}
}

// The full read the daemon starts copies four tables at once, as the MySQL
// one does (#3115). Zero would mean one connection to the source per CPU of
// the machine DBTrail runs on.
func TestPGBaselineConfig_tablesAtOnce(t *testing.T) {
	cfg, err := pgBaselineConfig(console.BaselineRequest{
		SourceDSN: "postgres://repl:secret@pg:5432/appdb", Flavor: "postgres",
	}, "/out")
	if err != nil {
		t.Fatalf("pgBaselineConfig: %v", err)
	}
	if cfg.Parallelism != 4 {
		t.Errorf("the daemon's PostgreSQL full read copies %d tables at once, want 4", cfg.Parallelism)
	}
	// The MySQL full read's number, which this one follows.
	args := buildConsoleMydumperArgs("h", 3306, "u", nil, "/dump", "", false, nil, false)
	if i := slices.Index(args, "--threads"); i < 0 || i+1 >= len(args) || args[i+1] != strconv.Itoa(cfg.Parallelism) {
		t.Errorf("the MySQL full read's --threads is not %d: %v", cfg.Parallelism, args)
	}
}

func TestPGBaselineConfig_badDSN(t *testing.T) {
	// A DSN already carrying replication is rejected by PGReplDSN.
	if _, err := pgBaselineConfig(console.BaselineRequest{SourceDSN: "postgres://h:5432/db?replication=database"}, "/out"); err == nil {
		t.Error("expected error for a repl-carrying source DSN")
	}
}
