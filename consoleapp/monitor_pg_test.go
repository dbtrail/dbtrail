package consoleapp

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/serverid"
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

	// MySQL with 0 delegates to DeriveForInstall (needs a parseable MySQL DSN).
	if _, err := m.deriveSourceIdentity(console.ServerEntry{SourceDSN: "u:p@tcp(h:3306)/"}, console.FlavorMySQL); err != nil {
		t.Errorf("mysql identity: %v", err)
	}

	// The same source registered in two installations: the entry's own index
	// database is what tells them apart, so the ids differ. One installation
	// keeps its id from one start to the next.
	entry := console.ServerEntry{SourceDSN: "u:p@tcp(h:3306)/", DSN: "root:pw@tcp(index-mysql:3306)/bintrail_idx_ab12"}
	var asked []string
	derive := func(indexUUID string) uint32 {
		t.Helper()
		restore := serverid.SetIndexServerUUIDForTest(func(_ context.Context, dsn string) (string, error) {
			asked = append(asked, dsn)
			return indexUUID, nil
		})
		defer restore()
		id, err := m.deriveSourceIdentity(entry, console.FlavorMySQL)
		if err != nil {
			t.Fatalf("mysql identity: %v", err)
		}
		return id
	}
	here, again, there := derive("8b0c2f3e-7a11-4d5e-9c1a-000000000001"), derive("8b0c2f3e-7a11-4d5e-9c1a-000000000001"), derive("8b0c2f3e-7a11-4d5e-9c1a-000000000002")
	if here != again || here == there {
		t.Errorf("ids: this installation %d then %d, another installation %d; want stable here and different there", here, again, there)
	}
	if len(asked) != 3 || asked[0] != entry.DSN {
		t.Errorf("the index asked was %q; want this entry's own index DSN", asked)
	}
	// An id the operator chose is theirs: the index is not even asked.
	entry.SourceServerID = 4242
	if id := derive("8b0c2f3e-7a11-4d5e-9c1a-000000000003"); id != 4242 || len(asked) != 3 {
		t.Errorf("explicit id: got %d after %d index reads; want 4242 and no new read", id, len(asked))
	}
}

// TestAutoServerID_AsksThisDaemonsIndex: watch with no --server-id derives
// from ITS source and ITS index. A call that dropped the index would give
// every installation the same id again, and no other test would notice.
func TestAutoServerID_AsksThisDaemonsIndex(t *testing.T) {
	src, idx := upSourceDSN, upIndexDSN
	t.Cleanup(func() { upSourceDSN, upIndexDSN = src, idx })
	upSourceDSN, upIndexDSN = "u:p@tcp(h:3306)/", "root:pw@tcp(index-mysql:3306)/bintrail_index"
	var asked string
	t.Cleanup(serverid.SetIndexServerUUIDForTest(func(_ context.Context, dsn string) (string, error) {
		asked = dsn
		return "8b0c2f3e-7a11-4d5e-9c1a-000000000001", nil
	}))
	var out strings.Builder
	id, err := autoServerID(context.Background(), &out)
	want, sourceOnly, _ := serverid.DeriveForInstall(context.Background(), upSourceDSN, upIndexDSN)
	if err != nil || id != want || sourceOnly != nil || asked != upIndexDSN {
		t.Fatalf("autoServerID = %d, %v (index asked: %q); want %d from %q", id, err, asked, want, upIndexDSN)
	}
}
