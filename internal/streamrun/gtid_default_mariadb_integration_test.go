//go:build integration

package streamrun

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/testutil"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
)

// A new MariaDB capture starts in GTID mode. These drive the real One entry
// point (the one `stream`, `up`, `watch` and console sources share) against a
// real MariaDB, and are named *MariaDB* so the MariaDB-source CI job selects
// them on every supported version.

// mariadbBinlogPos reads @@gtid_binlog_pos, parsed.
func mariadbBinlogPos(t *testing.T, sourceDB *sql.DB) (string, *gomysql.MariadbGTIDSet) {
	t.Helper()
	s, err := config.CurrentMariaDBGTIDPos(sourceDB)
	if err != nil {
		t.Fatalf("read @@gtid_binlog_pos: %v", err)
	}
	set, err := parseMariadbSet(s)
	if err != nil {
		t.Fatalf("parse @@gtid_binlog_pos %q: %v", s, err)
	}
	return s, set
}

// checkpointCovers reports whether the durable checkpoint is a GTID set that
// has reached want in every domain.
func checkpointCovers(t *testing.T, indexDB *sql.DB, want *gomysql.MariadbGTIDSet) bool {
	t.Helper()
	s, err := loadStreamState(indexDB)
	if err != nil {
		t.Fatalf("poll stream_state: %v", err)
	}
	if s == nil || s.mode != "gtid" {
		return false
	}
	cp, err := parseMariadbSet(s.gtidSet)
	if err != nil {
		t.Fatalf("checkpoint gtid_set %q does not parse as MariaDB: %v", s.gtidSet, err)
	}
	return mariadbCheckpointCoversFloor(cp, want)
}

// gapLostStamped reports whether stream_state carries a continuity-loss stamp.
func gapLostStamped(t *testing.T, indexDB *sql.DB) (bool, string) {
	t.Helper()
	var at sql.NullTime
	var detail sql.NullString
	if err := indexDB.QueryRow("SELECT gap_lost_at, gap_lost_detail FROM stream_state WHERE id = 1").
		Scan(&at, &detail); err != nil {
		t.Fatalf("read gap_lost_at: %v", err)
	}
	return at.Valid, detail.String
}

// assertSanePositions is the #1117 probe: on MariaDB 11.4+ a stream that did
// not run as mariadb (no FillZeroLogPos) stores end_pos = 0 and an underflowed
// start_pos.
func assertSanePositions(t *testing.T, indexDB *sql.DB, schema string) {
	t.Helper()
	var bad int
	if err := indexDB.QueryRow(`SELECT COUNT(*) FROM binlog_events
		WHERE schema_name = ? AND (end_pos = 0 OR start_pos >= end_pos)`, schema).Scan(&bad); err != nil {
		t.Fatalf("position probe: %v", err)
	}
	if bad != 0 {
		t.Errorf("%d rows carry impossible positions (#1117)", bad)
	}
}

// TestOne_MariaDB_freshCaptureStartsInGTIDMode: no checkpoint and no flags. The
// capture starts in GTID mode at the source's current @@gtid_binlog_pos:
//   - a row written BEFORE the start is not captured (it starts from now, not
//     from the beginning of the binlog);
//   - every domain the source had is in the checkpoint from the first event,
//     including a domain the capture never sees a write in, which is what lets
//     the per-domain gap check tell "read past" from "never seen";
//   - the rows written after the start are captured exactly once, with sane
//     positions.
func TestOne_MariaDB_freshCaptureStartsInGTIDMode(t *testing.T) {
	indexDB, indexName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	sourceDB, sourceName := mariadbFlavorSource(t)

	// A write in a domain of its own, before the capture exists.
	ctx := context.Background()
	conn, err := sourceDB.Conn(ctx)
	if err != nil {
		t.Fatalf("open domain connection: %v", err)
	}
	defer conn.Close()
	const sideDomain = 7
	if _, err := conn.ExecContext(ctx, "SET SESSION gtid_domain_id = 7"); err != nil {
		testutil.SkipOrFailMariaDB(t, "SET SESSION gtid_domain_id = 7 failed (needs SUPER): %v", err)
	}
	if _, err := conn.ExecContext(ctx, "INSERT INTO orders (amount) VALUES (99)"); err != nil {
		t.Fatalf("pre-start insert: %v", err)
	}
	preStr, pre := mariadbBinlogPos(t, sourceDB)
	if _, ok := pre.Sets[sideDomain]; !ok || len(pre.Sets) < 2 {
		t.Fatalf("@@gtid_binlog_pos %q should carry domain %d and the default domain", preStr, sideDomain)
	}

	cfg := flavorTestConfig(indexName, mariadbSourceDSN(sourceName), sourceName, "", 99970)
	var post *gomysql.MariadbGTIDSet
	if err := runOneUntil(t, cfg, true,
		func() {
			for i := range 3 {
				testutil.MustExec(t, sourceDB, "INSERT INTO orders (amount) VALUES (?)", float64(i+1))
			}
			_, post = mariadbBinlogPos(t, sourceDB)
		},
		func() bool { return indexedOrders(t, indexDB, sourceName) >= 3 && checkpointCovers(t, indexDB, post) },
	); err != nil {
		t.Fatalf("One: %v", err)
	}

	saved, err := loadStreamState(indexDB)
	if err != nil || saved == nil {
		t.Fatalf("loadStreamState: %v (state %v)", err, saved)
	}
	if saved.mode != "gtid" || saved.flavor != gomysql.MariaDBFlavor {
		t.Fatalf("checkpoint mode/flavor = %q/%q, want gtid/mariadb", saved.mode, saved.flavor)
	}
	cp, err := parseMariadbSet(saved.gtidSet)
	if err != nil {
		t.Fatalf("checkpoint %q: %v", saved.gtidSet, err)
	}
	for domain, g := range pre.Sets {
		got, ok := cp.Sets[domain]
		if !ok {
			t.Errorf("checkpoint %q lost domain %d that the source had at start (%q)", saved.gtidSet, domain, preStr)
			continue
		}
		if got.SequenceNumber < g.SequenceNumber {
			t.Errorf("checkpoint %q is behind the start point %q in domain %d", saved.gtidSet, preStr, domain)
		}
	}
	// ids 2..4: id 1 is the pre-start row, which must not be captured.
	assertExactlyOnce(t, indexedPKs(t, indexDB, sourceName, "orders"), pkRange(2, 4))
	assertSanePositions(t, indexDB, sourceName)
	if stamped, detail := gapLostStamped(t, indexDB); stamped {
		t.Errorf("a fresh start recorded a loss: %s", detail)
	}
}

// TestOne_MariaDB_explicitStartPosStaysPositionThenResetMovesToGTID covers
// three edges in one lifecycle:
//  1. an explicit --start-file/--start-pos keeps position mode on MariaDB, with
//     the #1117 zero-position fix still in force (sane positions on 11.4+);
//  2. a restart over that position checkpoint keeps position mode;
//  3. --reset, with the checkpoint caught up to the source's current binlog
//     position, lands in GTID mode and records nothing as lost: the documented
//     way to move an existing capture to GTID without losing events.
func TestOne_MariaDB_explicitStartPosStaysPositionThenResetMovesToGTID(t *testing.T) {
	indexDB, indexName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	sourceDB, sourceName := mariadbFlavorSource(t)

	caughtUpInPosition := func() bool {
		s, err := loadStreamState(indexDB)
		if err != nil {
			t.Fatalf("poll stream_state: %v", err)
		}
		if s == nil || s.mode != "position" {
			return false
		}
		file, pos, err := config.CurrentBinlogPosition(sourceDB)
		if err != nil {
			t.Fatalf("CurrentBinlogPosition: %v", err)
		}
		return s.binlogFile == file && s.binlogPos == uint64(pos)
	}

	// ── 1. explicit start position ──
	file, pos, err := config.CurrentBinlogPosition(sourceDB)
	if err != nil {
		t.Fatalf("CurrentBinlogPosition: %v", err)
	}
	cfg := flavorTestConfig(indexName, mariadbSourceDSN(sourceName), sourceName, "", 99971)
	cfg.StartFile, cfg.StartPos = file, pos
	if err := runOneUntil(t, cfg, true,
		func() {
			for i := range 2 {
				testutil.MustExec(t, sourceDB, "INSERT INTO orders (amount) VALUES (?)", float64(i+1))
			}
		},
		func() bool { return indexedOrders(t, indexDB, sourceName) >= 2 && caughtUpInPosition() },
	); err != nil {
		t.Fatalf("run 1 (explicit --start-pos): %v", err)
	}
	assertSanePositions(t, indexDB, sourceName)

	// ── 2. restart over the position checkpoint, no flags ──
	testutil.MustExec(t, sourceDB, "INSERT INTO orders (amount) VALUES (3)")
	cfg2 := flavorTestConfig(indexName, mariadbSourceDSN(sourceName), sourceName, "", 99971)
	if err := runOneUntil(t, cfg2, false, nil,
		func() bool { return indexedOrders(t, indexDB, sourceName) >= 3 && caughtUpInPosition() },
	); err != nil {
		t.Fatalf("run 2 (resume position checkpoint): %v", err)
	}
	if s, _ := loadStreamState(indexDB); s == nil || s.mode != "position" {
		t.Fatalf("resume over a position checkpoint changed the mode: %+v", s)
	}
	assertExactlyOnce(t, indexedPKs(t, indexDB, sourceName, "orders"), pkRange(1, 3))

	// ── 3. --reset at the current position ──
	cfg3 := flavorTestConfig(indexName, mariadbSourceDSN(sourceName), sourceName, "", 99971)
	cfg3.Reset = true
	var post *gomysql.MariadbGTIDSet
	if err := runOneUntil(t, cfg3, true,
		func() {
			for i := range 2 {
				testutil.MustExec(t, sourceDB, "INSERT INTO orders (amount) VALUES (?)", float64(i+4))
			}
			_, post = mariadbBinlogPos(t, sourceDB)
		},
		func() bool { return indexedOrders(t, indexDB, sourceName) >= 5 && checkpointCovers(t, indexDB, post) },
	); err != nil {
		t.Fatalf("run 3 (--reset): %v", err)
	}
	s, err := loadStreamState(indexDB)
	if err != nil || s == nil || s.mode != "gtid" {
		t.Fatalf("after --reset the checkpoint is %+v (%v), want GTID mode", s, err)
	}
	if stamped, detail := gapLostStamped(t, indexDB); stamped {
		t.Errorf("--reset from a caught-up position checkpoint recorded a loss: %s", detail)
	}
	assertExactlyOnce(t, indexedPKs(t, indexDB, sourceName, "orders"), pkRange(1, 5))
	assertSanePositions(t, indexDB, sourceName)
	if strings.TrimSpace(s.gtidSet) == "" {
		t.Error("GTID checkpoint after --reset is empty on a source that has written transactions")
	}
}
