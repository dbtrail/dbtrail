package pgstreamrun

import (
	"database/sql"
	"encoding/json"
	"github.com/dbtrail/dbtrail/internal/event"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jackc/pglogrepl"

	"github.com/dbtrail/dbtrail/internal/pgcapture"
)

// TestBuildSourceHealthJSON pins the wire shape written to stream_state.source_health
// (#599): SlotHealth's sql.NullInt64 and pglogrepl.LSN must flatten cleanly (NULL safe
// margin → JSON null, LSN → "X/Y" string), an empty not-full list must marshal as []
// (not null) for a stable frontend contract, and checked_at must be RFC3339 UTC.
func TestBuildSourceHealthJSON(t *testing.T) {
	checkedAt := time.Date(2026, 6, 23, 18, 30, 0, 0, time.UTC)

	// Unlimited retention (invalid SafeWalSize → null) + a non-FULL table.
	snap := pgcapture.HealthSnapshot{
		Slot: pgcapture.SlotHealth{
			Exists: true, Active: true, WalStatus: "reserved",
			RetainedBytes: 16384, RestartLSN: pglogrepl.LSN(0x1A2B3C4),
			SafeWalSize: sql.NullInt64{}, // invalid → null
		},
		ReplicaIdentityNotFull: []string{"app.t1 (relreplident=d)"},
	}
	var got map[string]any
	if b, err := buildSourceHealthJSON(snap, checkedAt); err != nil {
		t.Fatalf("buildSourceHealthJSON: %v", err)
	} else if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got["wal_status"] != "reserved" {
		t.Errorf("wal_status = %v", got["wal_status"])
	}
	// Pin the key NAMES the frontend (pgHealthCard) reads directly — a json-tag rename on
	// any of these would pass a looser test green and silently blank the panel.
	if got["exists"] != true {
		t.Errorf("exists = %v, want true", got["exists"])
	}
	if got["active"] != true {
		t.Errorf("active = %v, want true", got["active"])
	}
	if got["retained_bytes"] != float64(16384) {
		t.Errorf("retained_bytes = %v, want 16384", got["retained_bytes"])
	}
	if got["safe_wal_size"] != nil {
		t.Errorf("safe_wal_size must be null when SafeWalSize is invalid, got %v", got["safe_wal_size"])
	}
	if got["checked_at"] != "2026-06-23T18:30:00Z" {
		t.Errorf("checked_at = %v, want RFC3339 UTC", got["checked_at"])
	}
	if got["restart_lsn"] != pglogrepl.LSN(0x1A2B3C4).String() {
		t.Errorf("restart_lsn = %v, want %q", got["restart_lsn"], pglogrepl.LSN(0x1A2B3C4).String())
	}
	if nf, _ := got["replica_identity_not_full"].([]any); len(nf) != 1 || nf[0] != "app.t1 (relreplident=d)" {
		t.Errorf("replica_identity_not_full = %v", got["replica_identity_not_full"])
	}

	// Valid SafeWalSize → a number; empty not-full → [] (a non-nil slice), never null.
	snap2 := pgcapture.HealthSnapshot{
		Slot:                   pgcapture.SlotHealth{Exists: true, SafeWalSize: sql.NullInt64{Int64: 1 << 30, Valid: true}},
		ReplicaIdentityNotFull: nil,
	}
	var got2 map[string]any
	if b, err := buildSourceHealthJSON(snap2, checkedAt); err != nil {
		t.Fatalf("buildSourceHealthJSON(2): %v", err)
	} else if err := json.Unmarshal(b, &got2); err != nil {
		t.Fatalf("unmarshal(2): %v", err)
	}
	if got2["safe_wal_size"] == nil {
		t.Errorf("safe_wal_size must be a number when SafeWalSize is valid")
	}
	if nf, ok := got2["replica_identity_not_full"].([]any); !ok || nf == nil {
		t.Errorf("empty replica_identity_not_full must marshal as [], got %v", got2["replica_identity_not_full"])
	}
}

// TestBuildProbeErrorJSON pins the failed-probe wire shape (#599 review): a probe error
// is persisted as a snapshot carrying probe_error + checked_at (so the console shows
// "probe failing", not a blank panel), with replica_identity_not_full normalized to [];
// and a healthy snapshot must OMIT probe_error.
func TestBuildProbeErrorJSON(t *testing.T) {
	checkedAt := time.Date(2026, 6, 23, 18, 30, 0, 0, time.UTC)
	b, err := buildProbeErrorJSON("recovery is in progress", checkedAt)
	if err != nil {
		t.Fatalf("buildProbeErrorJSON: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got["probe_error"] != "recovery is in progress" {
		t.Errorf("probe_error = %v", got["probe_error"])
	}
	if got["checked_at"] != "2026-06-23T18:30:00Z" {
		t.Errorf("checked_at = %v", got["checked_at"])
	}
	if nf, ok := got["replica_identity_not_full"].([]any); !ok || nf == nil {
		t.Errorf("replica_identity_not_full must be [], got %v", got["replica_identity_not_full"])
	}

	// A successful snapshot must NOT carry probe_error (omitempty).
	healthy, _ := buildSourceHealthJSON(pgcapture.HealthSnapshot{}, checkedAt)
	var gm map[string]any
	if err := json.Unmarshal(healthy, &gm); err != nil {
		t.Fatalf("unmarshal healthy: %v", err)
	}
	if _, present := gm["probe_error"]; present {
		t.Errorf("a healthy snapshot must omit probe_error, got %v", gm["probe_error"])
	}
}

// ─── loadStreamStatePG (sqlmock; the #1082 fresh-start contract) ────────────────

func mockStateRows(flavor string, lsn uint64) *sqlmock.Rows {
	return sqlmock.NewRows([]string{"flavor", "binlog_position", "events_indexed", "server_id", "last_event_time"}).
		AddRow(flavor, lsn, 12, 9, nil)
}

// TestLoadStreamStatePG_ZeroPositionIsFresh (#1082): a stream_state row without a
// durable LSN cursor — seeded by a lost-slot stamp or a pre-commit health snapshot,
// or the cleared row `bintrail-pg reset` leaves behind so gap_lost_* survives — must
// read as "no checkpoint" (nil), so the capturer creates a fresh slot instead of
// demanding the one reset just dropped (ExpectExistingSlot stays false).
func TestLoadStreamStatePG_ZeroPositionIsFresh(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	mock.ExpectQuery("SELECT flavor, binlog_position").WillReturnRows(mockStateRows("postgres", 0))

	saved, err := loadStreamStatePG(db)
	if err != nil {
		t.Fatalf("loadStreamStatePG: %v", err)
	}
	if saved != nil {
		t.Fatalf("a zero-position row must read as first-run (nil), got %+v", saved)
	}
}

// TestLoadStreamStatePG_RealCheckpointResumes: the ordinary resume path is untouched
// by the #1082 change — a non-zero cursor loads as a resumable checkpoint.
func TestLoadStreamStatePG_RealCheckpointResumes(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	mock.ExpectQuery("SELECT flavor, binlog_position").WillReturnRows(mockStateRows("postgres", 27439044))

	saved, err := loadStreamStatePG(db)
	if err != nil || saved == nil {
		t.Fatalf("loadStreamStatePG = (%+v, %v), want a checkpoint", saved, err)
	}
	if saved.lsn != 27439044 || saved.eventsIndexed != 12 || saved.serverID != 9 {
		t.Errorf("checkpoint misloaded: %+v", saved)
	}
}

// TestLoadStreamStatePG_FlavorGuardBeatsZeroPosition: the flavor guard stays FIRST —
// even a zero-position row of another flavor refuses loud (a cleared MySQL checkpoint
// still marks a MySQL index; treating it as PG-fresh would clobber a foreign index).
func TestLoadStreamStatePG_FlavorGuardBeatsZeroPosition(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	mock.ExpectQuery("SELECT flavor, binlog_position").WillReturnRows(mockStateRows("mysql", 0))

	if _, err := loadStreamStatePG(db); err == nil || !strings.Contains(err.Error(), `"mysql"`) {
		t.Fatalf("a non-postgres row must refuse loud even at position 0, got %v", err)
	}
}

func TestResolveStartLSN(t *testing.T) {
	log := slog.New(slog.DiscardHandler)

	// A saved checkpoint wins over the flag (idempotent resume).
	if got := resolveStartLSN(&pgStreamState{lsn: 500}, 999, log); got != 500 {
		t.Errorf("saved checkpoint: got %d, want 500", got)
	}
	// No checkpoint → the explicit flag.
	if got := resolveStartLSN(nil, 999, log); got != 999 {
		t.Errorf("flag start: got %d, want 999", got)
	}
	// No checkpoint, no flag → 0: first run, the capturer starts from the slot's
	// ConsistentPoint (this must NOT be an error).
	if got := resolveStartLSN(nil, 0, log); got != 0 {
		t.Errorf("first run: got %d, want 0", got)
	}
}

func TestInferredOID_2441(t *testing.T) {
	for q, want := range map[string]uint32{
		event.InferredDDLPrefix + " oid=16411; instant=at-or-before; low=none; high=0/1@2026-01-01T00:00:00Z */ x": 16411,
		event.InferredDDLPrefix + " oid=0; low=none */ x":                                                          0,
		event.InferredDDLPrefix + " instant=at-or-before */ x":                                                     0,
		"ALTER TABLE t ADD c INT": 0,
	} {
		got, ok := inferredOID(q)
		if got != want || ok != (want != 0) {
			t.Errorf("inferredOID(%q) = %d %v, want %d", q, got, ok, want)
		}
	}
}

// Two ALTERs of one table in one transaction carry the same position, OID
// and kind. After a resend, the index already holding the first must skip
// only the first: the second is a real change.
func TestInferredCounter_twoChangesOfOneKindInOneTransaction_2441(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	alter := func(text string) event.Event {
		return event.Event{BinlogFile: "0/2", EndPos: 2, Schema: "s", Table: "a", EventType: event.EventDDL, DDLType: event.DDLAlterTable,
			DDLQuery: event.InferredDDLPrefix + " oid=10; instant=at-or-before; low=none; high=0/2@2026-01-01T00:00:00Z */ " + text}
	}
	held := sqlmock.NewRows([]string{"ddl_type"}).AddRow("ALTER TABLE")
	mock.ExpectQuery("FROM schema_changes").WithArgs(uint64(2), "0/2", "s", "a", event.InferredDDLPrefix+" oid=10;%").WillReturnRows(held)
	mock.ExpectQuery("FROM schema_changes").WithArgs(uint64(2), "0/2", "s", "a", event.InferredDDLPrefix+" oid=10;%").
		WillReturnRows(sqlmock.NewRows([]string{"ddl_type"}).AddRow("ALTER TABLE"))

	c := &inferredCounter{}
	if dup, err := c.recorded(db, alter("ALTER TABLE s.a: added x (text)")); err != nil || !dup {
		t.Fatalf("first ALTER: dup %v err %v, want recorded already", dup, err)
	}
	if dup, err := c.recorded(db, alter("ALTER TABLE s.a: added y (text)")); err != nil || dup {
		t.Fatalf("second ALTER: dup %v err %v, want a new row", dup, err)
	}
	c.reset()

	// A RENAME is counted on the row that carries its text, the old name's;
	// a DROP at the same position and OID is the same change.
	mock.ExpectQuery("FROM schema_changes").WithArgs(uint64(2), "0/2", "s", "old", event.InferredDDLPrefix+" oid=10;%").
		WillReturnRows(sqlmock.NewRows([]string{"ddl_type"}).AddRow("RENAME TABLE"))
	drop := event.Event{BinlogFile: "0/2", EndPos: 2, Schema: "s", Table: "old", EventType: event.EventDDL, DDLType: event.DDLDropTable,
		DDLQuery: event.InferredDDLPrefix + " oid=10; instant=at-or-before; low=none; high=0/2@2026-01-01T00:00:00Z */ DROP TABLE s.old"}
	if dup, err := c.recorded(db, drop); err != nil || !dup {
		t.Fatalf("DROP after a recorded RENAME of the same OID: dup %v err %v", dup, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// The text row of an inferred DROP or RENAME is a release; a rename's
// pointer row (its new name) and a row whose marker does not parse are not.
func TestLoadKnownReleases_2441(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	m := event.InferredDDLPrefix + " oid=7; instant=at-or-before; low=none; high=0/2@2026-01-01T00:00:00Z */ "
	mock.ExpectQuery("FROM schema_changes").WithArgs(event.InferredDDLPrefix + "%").WillReturnRows(
		sqlmock.NewRows([]string{"schema_name", "table_name", "binlog_pos", "head"}).
			AddRow("s", "old", 2, m+"RENAME TABLE s.old TO s.new").
			AddRow("s", "new", 2, m+"(same RENAME TABLE statement as the row for s.old)").
			AddRow("s", "gone", 3, m+"DROP TABLE s.gone (its name now belongs to a new table)").
			AddRow("s", "x", 4, event.InferredDDLPrefix+" instant=at-or-before */ DROP TABLE s.x"))
	got, err := loadKnownReleases(db)
	if err != nil {
		t.Fatal(err)
	}
	want := []pgcapture.KnownRelease{{OID: 7, Schema: "s", Table: "old", LSN: 2}, {OID: 7, Schema: "s", Table: "gone", LSN: 3}}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}
