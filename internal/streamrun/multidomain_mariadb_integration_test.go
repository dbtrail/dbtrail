//go:build integration

// Live multi-domain GTID validation for #621 (MariaDB beta data-safety
// blocker). The ratified stance is option (1), per-domain resume: the
// checkpoint's MariaDB GTID set carries every domain's position independently
// (gomysql.ParseMariadbGTIDSet → StartSyncGTID hands the server the full
// per-domain map) and gap detection compares sequences per domain
// (mariadbCheckpointCoversFloor, #518). Both were argued structurally; this
// file is the empirical half — a real multi-domain stream, produced the way
// MariaDB actually produces one (per-session `SET SESSION gtid_domain_id`,
// no cluster required), driven through the same One() entry point the CLI
// uses, across a stop and a resume.
//
// What it pins that no unit test can:
//   - a resume from a two-domain checkpoint re-attaches each domain at its own
//     position: no event lost, none double-indexed (assertExactlyOnce over
//     interleaved writes from both domains, spanning the restart);
//   - the accumulated stream_state gtid_set covers BOTH domains and each
//     domain's sequence advances independently across the restart;
//   - the domain-aware gap check does not false-alarm on a healthy multi-domain
//     checkpoint, including against a REAL multi-domain purge floor
//     (FLUSH + PURGE make BINLOG_GTID_POS return a two-domain floor, so
//     mariadbCheckpointCoversFloor genuinely iterates both domains — asserted,
//     not assumed). Run 2 sets NoGapFill, so a false unfillable verdict would
//     refuse to start and fail the test loudly instead of warn-and-advance.
package streamrun

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// domainMaxSeq returns the sequence number the set holds for one domain, 0
// when the domain is absent — so "the domain advanced" is expressible even
// when the earlier set had never seen that domain. (go-mysql v1.15.0+ holds
// exactly one GTID per domain, matching MariaDB's own position semantics.)
func domainMaxSeq(set *gomysql.MariadbGTIDSet, domain uint32) uint64 {
	g, ok := set.Sets[domain]
	if !ok {
		return 0
	}
	return g.SequenceNumber
}

func TestIntegrationMultiDomainGTIDResumeExactlyOnce_mariadb(t *testing.T) {
	indexDB, indexName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	sourceDB, sourceName := testutil.CreateTestMariaDB(t)
	sourceDSN := testutil.MariaDBBaseDSN() + "/" + sourceName + "?parseTime=true"

	testutil.MustExec(t, sourceDB, `CREATE TABLE orders (
		id  INT PRIMARY KEY,
		dom INT NOT NULL
	)`)

	ctx := context.Background()

	// Two dedicated connections, one per GTID domain. gtid_domain_id is a
	// SESSION variable, so each connection's transactions are stamped into its
	// own domain — this is exactly how MariaDB multi-domain streams arise
	// (per-tier writers, multi-source funnels), just on one server.
	domainConn := func(domain int) *sql.Conn {
		conn, err := sourceDB.Conn(ctx)
		if err != nil {
			t.Fatalf("open domain-%d connection: %v", domain, err)
		}
		t.Cleanup(func() { conn.Close() })
		if _, err := conn.ExecContext(ctx,
			fmt.Sprintf("SET SESSION gtid_domain_id = %d", domain)); err != nil {
			testutil.SkipOrFailMariaDB(t, "SET SESSION gtid_domain_id = %d failed (needs SUPER): %v", domain, err)
		}
		return conn
	}
	conns := [2]*sql.Conn{domainConn(0), domainConn(1)}

	// insertInterleaved writes ids [lo, hi] alternating between the two
	// domains (even id → domain 0, odd id → domain 1), so both domains have
	// in-flight history on every phase and the binlog interleaves them.
	insertInterleaved := func(lo, hi int) {
		for i := lo; i <= hi; i++ {
			domain := i % 2
			if _, err := conns[domain].ExecContext(ctx,
				"INSERT INTO orders (id, dom) VALUES (?, ?)", i, domain); err != nil {
				t.Fatalf("insert id %d into domain %d: %v", i, domain, err)
			}
		}
	}

	currentExecuted := func() string {
		var s string
		if err := sourceDB.QueryRow("SELECT @@gtid_binlog_pos").Scan(&s); err != nil {
			t.Fatalf("read @@gtid_binlog_pos: %v", err)
		}
		return strings.TrimSpace(s)
	}

	// checkpointCaughtUp polls stream_state until the durable checkpoint's GTID
	// set equals a target executed set. Waiting on the CHECKPOINT (not on an
	// indexed row) is what makes the post-run assertions race-free: the commit
	// event that advances the set trails the row event that lands the row.
	checkpointCaughtUp := func(want string) func() bool {
		return func() bool {
			s, err := loadStreamState(indexDB)
			if err != nil {
				t.Fatalf("poll stream_state: %v", err)
			}
			return s != nil && mariadbGTIDSetsEqual(s.gtidSet, want)
		}
	}

	seedStr := currentExecuted()
	if seedStr == "" {
		testutil.SkipOrFailMariaDB(t, "MariaDB reported an empty GTID position; GTID binlogging not active")
	}
	seed, err := parseMariadbSet(seedStr)
	if err != nil {
		t.Fatalf("parse seed GTID set %q: %v", seedStr, err)
	}

	baseCfg := func(serverID uint32) Config {
		return Config{
			IndexDSN:   testutil.IntegrationDSN(indexName),
			SourceDSN:  sourceDSN,
			Flavor:     gomysql.MariaDBFlavor,
			ServerID:   serverID,
			BatchSize:  1,
			Schemas:    sourceName,
			Checkpoint: 1,
			GapTimeout: 30,
			Format:     "text",
			SSLMode:    "preferred",
			Deps:       testStreamDeps(),
		}
	}

	// ── run 1: stream interleaved two-domain traffic, checkpoint durably ──
	// StartGTID pins run 1 to the PRE-write executed set (a fresh run with no
	// flags would start from the post-write @@gtid_binlog_pos and skip ids
	// 1-6), so the checkpoint this run leaves behind is the multi-domain GTID
	// checkpoint whose RESUME is under test.
	insertInterleaved(1, 6)
	phase1Executed := currentExecuted()

	cfg1 := baseCfg(99885)
	cfg1.StartGTID = seedStr
	if err := runOneUntil(t, cfg1, false, nil, checkpointCaughtUp(phase1Executed)); err != nil {
		t.Fatalf("run 1 (initial multi-domain stream): %v", err)
	}

	saved, err := loadStreamState(indexDB)
	if err != nil {
		t.Fatalf("loadStreamState after run 1: %v", err)
	}
	if saved == nil {
		t.Fatal("run 1 saved no checkpoint")
	}
	if saved.mode != "gtid" || saved.flavor != gomysql.MariaDBFlavor {
		t.Fatalf("run 1 checkpoint mode/flavor = %q/%q, want gtid/mariadb", saved.mode, saved.flavor)
	}
	cp1, err := parseMariadbSet(saved.gtidSet)
	if err != nil {
		t.Fatalf("run 1 checkpoint gtid_set %q does not parse as a MariaDB set: %v", saved.gtidSet, err)
	}
	// The accumulated set must cover both domains, each advanced past the seed
	// — the per-domain map is actually IN the durable checkpoint.
	for _, domain := range []uint32{0, 1} {
		if _, ok := cp1.Sets[domain]; !ok {
			t.Fatalf("run 1 checkpoint %q is missing domain %d", saved.gtidSet, domain)
		}
		if domainMaxSeq(cp1, domain) <= domainMaxSeq(seed, domain) {
			t.Fatalf("run 1 checkpoint %q did not advance domain %d past the seed %q",
				saved.gtidSet, domain, seedStr)
		}
	}
	assertExactlyOnce(t, indexedPKs(t, indexDB, sourceName, "orders"), pkRange(1, 6))

	// ── a real multi-domain purge floor ───────────────────────────────────
	// Roll the binlog and purge the earlier files so BINLOG_GTID_POS over the
	// oldest SURVIVING binlog returns a floor that itself spans both domains.
	// Without this, the floor is empty on a fresh server and
	// mariadbCheckpointCoversFloor never runs — the healthy-path check below
	// would be vacuously green. Server-wide binlog state: this test relies on
	// the suite's -p 1 serialization, like TestDetectMariaDBGTIDGap_livePurge.
	testutil.MustExec(t, sourceDB, "FLUSH BINARY LOGS")
	currentFile, _, err := config.CurrentBinlogPosition(sourceDB)
	if err != nil {
		t.Fatalf("CurrentBinlogPosition after FLUSH: %v", err)
	}
	// PURGE silently skips binlogs a replica connection still has open, and
	// run 1's server-side dump thread can outlive the client's Close by a
	// beat — so purge in a retry loop until the earlier files are actually
	// gone, instead of asserting on a race.
	var earliest string
	purgeDeadline := time.Now().Add(30 * time.Second)
	for {
		testutil.MustExec(t, sourceDB, "PURGE BINARY LOGS TO '"+currentFile+"'")
		earliest, err = earliestBinlogFile(ctx, sourceDB)
		if err != nil {
			t.Fatalf("earliestBinlogFile after purge: %v", err)
		}
		if earliest == currentFile {
			break
		}
		if time.Now().After(purgeDeadline) {
			t.Fatalf("binlog purge never completed: earliest surviving file is %s, want %s (a lingering dump thread pins earlier files)", earliest, currentFile)
		}
		time.Sleep(200 * time.Millisecond)
	}
	var floorNS sql.NullString
	if err := sourceDB.QueryRow("SELECT BINLOG_GTID_POS(?, 4)", earliest).Scan(&floorNS); err != nil {
		t.Fatalf("query BINLOG_GTID_POS(%q, 4): %v", earliest, err)
	}
	floorStr := strings.TrimSpace(floorNS.String)
	if floorStr == "" {
		t.Fatal("purge floor is empty after PURGE BINARY LOGS; the multi-domain coverage check would be vacuous")
	}
	floor, err := parseMariadbSet(floorStr)
	if err != nil {
		t.Fatalf("parse purge floor %q: %v", floorStr, err)
	}
	for _, domain := range []uint32{0, 1} {
		if _, ok := floor.Sets[domain]; !ok {
			t.Fatalf("purge floor %q is missing domain %d; the coverage check would not be exercised per domain", floorStr, domain)
		}
	}

	// Healthy multi-domain checkpoint vs a real multi-domain floor: the
	// domain-aware gap check must not raise a false alarm.
	gap1, err := detectMariaDBGTIDGap(sourceDB, saved.gtidSet, 30*time.Second)
	if err != nil {
		t.Fatalf("gap detection on the multi-domain checkpoint: %v", err)
	}
	if gap1.HasGap {
		t.Fatalf("healthy multi-domain checkpoint %q tripped the gap detector: %+v", saved.gtidSet, gap1)
	}

	// ── run 2: write more under BOTH domains while stopped, then resume ───
	insertInterleaved(7, 12)
	phase2Executed := currentExecuted()

	// NoGapFill makes the healthy-resume claim structural: if the domain-aware
	// gap check misread this resume as unfillable, One would refuse to start
	// and this run would fail — instead of auto-advancing past "lost" events
	// and leaving only a log line behind.
	cfg2 := baseCfg(99886)
	cfg2.NoGapFill = true
	if err := runOneUntil(t, cfg2, false, nil, checkpointCaughtUp(phase2Executed)); err != nil {
		t.Fatalf("run 2 (multi-domain resume): %v", err)
	}

	// THE #621 acceptance: across the stop/resume, every id from both domains
	// is indexed exactly once — per-domain resume neither lost a domain's
	// backlog nor replayed a domain from too early a position.
	assertExactlyOnce(t, indexedPKs(t, indexDB, sourceName, "orders"), pkRange(1, 12))

	final, err := loadStreamState(indexDB)
	if err != nil {
		t.Fatalf("loadStreamState after run 2: %v", err)
	}
	cp2, err := parseMariadbSet(final.gtidSet)
	if err != nil {
		t.Fatalf("final checkpoint gtid_set %q does not parse: %v", final.gtidSet, err)
	}
	for _, domain := range []uint32{0, 1} {
		if domainMaxSeq(cp2, domain) <= domainMaxSeq(cp1, domain) {
			t.Errorf("domain %d did not advance across the resume (checkpoint %q → %q)",
				domain, saved.gtidSet, final.gtidSet)
		}
	}

	// Every indexed row must be attributed to the domain that wrote it: the
	// GTID's domain prefix has to match the row's own dom column. A resume
	// that conflated domains (or a parser that dropped the domain) shows here.
	rows, err := indexDB.Query(`
		SELECT pk_values, gtid, JSON_EXTRACT(row_after, '$.dom')
		FROM binlog_events
		WHERE schema_name = ? AND table_name = 'orders'`, sourceName)
	if err != nil {
		t.Fatalf("query domain attribution: %v", err)
	}
	defer rows.Close()
	checked := 0
	for rows.Next() {
		var pk, gtid, dom string
		if err := rows.Scan(&pk, &gtid, &dom); err != nil {
			t.Fatalf("scan domain attribution: %v", err)
		}
		wantDomain := strconv.Itoa(mustAtoi(t, pk) % 2)
		gotDomain, _, ok := strings.Cut(gtid, "-")
		if !ok {
			t.Fatalf("pk %s: indexed gtid %q is not MariaDB domain-server-seq form", pk, gtid)
		}
		if gotDomain != wantDomain || dom != wantDomain {
			t.Errorf("pk %s: domain attribution mismatch — gtid %q (domain %s), row dom %s, want domain %s",
				pk, gtid, gotDomain, dom, wantDomain)
		}
		checked++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate domain attribution: %v", err)
	}
	if checked != 12 {
		t.Fatalf("domain attribution covered %d rows, want 12", checked)
	}

	// Post-resume clean bill: the fully-caught-up two-domain checkpoint still
	// reads as gap-free.
	gap2, err := detectMariaDBGTIDGap(sourceDB, final.gtidSet, 30*time.Second)
	if err != nil {
		t.Fatalf("gap detection after the resume: %v", err)
	}
	if gap2.HasGap {
		t.Fatalf("caught-up multi-domain checkpoint %q tripped the gap detector after resume: %+v", final.gtidSet, gap2)
	}
}

func mustAtoi(t *testing.T, s string) int {
	t.Helper()
	n, err := strconv.Atoi(s)
	if err != nil {
		t.Fatalf("pk %q is not numeric: %v", s, err)
	}
	return n
}
