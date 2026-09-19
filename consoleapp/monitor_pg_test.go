package consoleapp

import (
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
)

func TestSourcePGStreamConfig(t *testing.T) {
	e := console.ServerEntry{
		DSN:               "root:pw@tcp(idx:3306)/bintrail_idx_e1",
		SourceDSN:         "postgres://repl:secret@pg:5432/appdb",
		SourceSlot:        "bintrail_slot",
		SourcePublication: "bintrail_pub",
		Schemas:           "public",
		Flavor:            "postgres",
	}
	cfg, err := sourcePGStreamConfig(e, 42, 0)
	if err != nil {
		t.Fatalf("sourcePGStreamConfig: %v", err)
	}
	if cfg.IndexDSN != e.DSN || cfg.QueryDSN != e.SourceDSN {
		t.Errorf("dsn wiring wrong: index=%q query=%q", cfg.IndexDSN, cfg.QueryDSN)
	}
	if !strings.Contains(cfg.ReplDSN, "replication=database") {
		t.Errorf("repl DSN missing replication=database: %q", cfg.ReplDSN)
	}
	if cfg.SlotName != "bintrail_slot" || cfg.Publication != "bintrail_pub" {
		t.Errorf("slot/publication wrong: %q / %q", cfg.SlotName, cfg.Publication)
	}
	if cfg.ServerID != 42 || cfg.Schemas != "public" || cfg.BatchSize != 1000 {
		t.Errorf("config wrong: %+v", cfg)
	}
	if cfg.Checkpoint != 10*time.Second {
		t.Errorf("checkpoint = %v, want 10s", cfg.Checkpoint)
	}
}

func TestSourcePGStreamConfig_badDSN(t *testing.T) {
	// A DSN already carrying replication is rejected by PGReplDSN (would double it).
	if _, err := sourcePGStreamConfig(console.ServerEntry{SourceDSN: "postgres://h:5432/db?replication=database"}, 1, 0); err == nil {
		t.Error("expected error for a repl-carrying DSN")
	}
}

func TestDeriveSourceIdentity(t *testing.T) {
	m := &monitorSupervisor{}

	// An explicit SourceServerID wins for any flavor.
	if id, err := m.deriveSourceIdentity(console.ServerEntry{SourceServerID: 7}, console.FlavorPostgres); err != nil || id != 7 {
		t.Errorf("explicit server id should win: id=%d err=%v", id, err)
	}

	// PG with 0 → a stable non-zero hash of the (registry-unique) entry id.
	a, err := m.deriveSourceIdentity(console.ServerEntry{ID: "abc123"}, console.FlavorPostgres)
	if err != nil || a == 0 {
		t.Fatalf("pg identity: id=%d err=%v", a, err)
	}
	b, _ := m.deriveSourceIdentity(console.ServerEntry{ID: "abc123"}, console.FlavorPostgres)
	if a != b {
		t.Errorf("pg identity not stable across calls: %d vs %d", a, b)
	}
	if c, _ := m.deriveSourceIdentity(console.ServerEntry{ID: "xyz789"}, console.FlavorPostgres); a == c {
		t.Errorf("distinct entry ids collided on %d", a)
	}

	// MySQL with 0 delegates to DeriveServerID (needs a parseable MySQL DSN).
	if _, err := m.deriveSourceIdentity(console.ServerEntry{SourceDSN: "u:p@tcp(h:3306)/"}, console.FlavorMySQL); err != nil {
		t.Errorf("mysql identity: %v", err)
	}
}
