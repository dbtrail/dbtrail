//go:build integration

package pgverifysource

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/consistency"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/pgbaseline"
	"github.com/dbtrail/dbtrail/internal/testutil"
	"github.com/dbtrail/dbtrail/internal/verify"
)

// Names are branch-unique: the PG server is shared with other test runs;
// everything is dropped in t.Cleanup.
const (
	pgckTbl    = "pgvs_ck_it_1024"
	pgckTblRev = "pgvs_ck_it_1024_rev"
	pgckTblGen = "pgvs_ck_it_1024_gen"
	vpgTbl     = "pgvs_verify_it_1024"
	vpgPub     = "pgvs_verify_it_pub_1024"
	vpgSlot    = "bintrail_pgvs_verify_it_1024"
)

// TestConsistentTableChecksumPG_Integration proves the PG live-scan render
// contract against a real PostgreSQL: the digest equals a hand-built digest
// over the EXPECTED pinned-GUC text renderings (bool 't' — the output-function
// form a ::text cast would NOT produce — bytea \x hex, timestamptz in UTC ISO,
// numeric with its declared scale, NULL distinct from empty string), it is
// row-order independent, generated columns are excluded, and the normalize
// hook rewrites values before hashing.
func TestConsistentTableChecksumPG_Integration(t *testing.T) {
	baseDSN := testutil.SkipIfNoPostgres(t)
	ctx := context.Background()

	setup, err := pgx.Connect(ctx, baseDSN)
	if err != nil {
		t.Fatalf("connect setup conn: %v", err)
	}
	t.Cleanup(func() { setup.Close(context.Background()) })

	dropAll := func() {
		bg := context.Background()
		for _, tbl := range []string{pgckTbl, pgckTblRev, pgckTblGen} {
			_, _ = setup.Exec(bg, fmt.Sprintf("DROP TABLE IF EXISTS %s", tbl))
		}
	}
	dropAll()
	t.Cleanup(dropAll)

	mustExec := func(sql string, args ...any) {
		t.Helper()
		if _, err := setup.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}

	ddl := `(id int PRIMARY KEY, ok bool, b bytea, ts timestamptz, num numeric(10,2), v text)`
	mustExec(fmt.Sprintf("CREATE TABLE %s %s", pgckTbl, ddl))
	mustExec(fmt.Sprintf("INSERT INTO %s VALUES (1, true, '\\xdeadbeef', '2024-01-02 03:04:05+00', 12.30, 'café 日本語')", pgckTbl))
	mustExec(fmt.Sprintf("INSERT INTO %s VALUES (2, false, NULL, NULL, NULL, '')", pgckTbl))

	// The scan connection MUST be the pinned one — that is the contract under
	// test (TimeZone=UTC, DateStyle=ISO, bytea_output=hex, ...).
	conn, err := connectPinned(ctx, baseDSN)
	if err != nil {
		t.Fatalf("pinned connect: %v", err)
	}
	t.Cleanup(func() { conn.Close(context.Background()) })

	got, err := consistentTableChecksumPG(ctx, conn, "public", pgckTbl, nil)
	if err != nil {
		t.Fatalf("consistentTableChecksumPG: %v", err)
	}
	if got.RowCount != 2 {
		t.Errorf("RowCount = %d, want 2", got.RowCount)
	}
	if got.LSN == 0 {
		t.Error("LSN = 0, want a real WAL anchor")
	}
	wantCols := []string{"id", "ok", "b", "ts", "num", "v"}
	if len(got.Columns) != len(wantCols) {
		t.Fatalf("Columns = %v, want %v", got.Columns, wantCols)
	}
	for i, c := range wantCols {
		if got.Columns[i] != c {
			t.Fatalf("Columns = %v, want %v (attnum order)", got.Columns, wantCols)
		}
	}

	// Hand-built expectation: the EXACT text the pinned output functions must
	// produce. If any rendering drifts (e.g. an unpinned TimeZone leaking
	// through, or a ::text cast turning 't' into 'true'), this digest differs.
	want := consistency.NewHasher()
	want.AddBytes([][]byte{
		[]byte("1"), []byte("t"), []byte(`\xdeadbeef`),
		[]byte("2024-01-02 03:04:05+00"), []byte("12.30"), []byte("café 日本語"),
	})
	want.AddBytes([][]byte{
		[]byte("2"), []byte("f"), nil, nil, nil, []byte(""),
	})
	if got.Digest != want.Digest() {
		t.Errorf("Digest = %s, want the hand-built pinned-GUC rendering digest %s", got.Digest, want.Digest())
	}

	// Order independence: same rows inserted in reverse order → same digest
	// (the multiset fold, same property the MySQL checksum has).
	mustExec(fmt.Sprintf("CREATE TABLE %s %s", pgckTblRev, ddl))
	mustExec(fmt.Sprintf("INSERT INTO %s VALUES (2, false, NULL, NULL, NULL, '')", pgckTblRev))
	mustExec(fmt.Sprintf("INSERT INTO %s VALUES (1, true, '\\xdeadbeef', '2024-01-02 03:04:05+00', 12.30, 'café 日本語')", pgckTblRev))
	rev, err := consistentTableChecksumPG(ctx, conn, "public", pgckTblRev, nil)
	if err != nil {
		t.Fatalf("consistentTableChecksumPG (reversed): %v", err)
	}
	if rev.Digest != got.Digest {
		t.Errorf("row order changed the digest: %s vs %s", rev.Digest, got.Digest)
	}

	// The normalize hook must rewrite values before hashing (and only non-NULL
	// ones — it would have panicked on nil above if misapplied).
	bang, err := consistentTableChecksumPG(ctx, conn, "public", pgckTbl, func(raw []byte) []byte {
		return append(append([]byte{}, raw...), '!')
	})
	if err != nil {
		t.Fatalf("consistentTableChecksumPG (hook): %v", err)
	}
	if bang.Digest == got.Digest {
		t.Error("normalize hook did not affect the digest")
	}

	// STORED generated columns are excluded — same contract as pgbaseline's
	// loadColumns, so the live column set always matches the baseline's.
	mustExec(fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY, n int, twice int GENERATED ALWAYS AS (n * 2) STORED)", pgckTblGen))
	mustExec(fmt.Sprintf("INSERT INTO %s (id, n) VALUES (1, 21)", pgckTblGen))
	gen, err := consistentTableChecksumPG(ctx, conn, "public", pgckTblGen, nil)
	if err != nil {
		t.Fatalf("consistentTableChecksumPG (generated): %v", err)
	}
	if len(gen.Columns) != 2 || gen.Columns[0] != "id" || gen.Columns[1] != "n" {
		t.Errorf("Columns = %v, want [id n] (generated column excluded)", gen.Columns)
	}
	wantGen := consistency.NewHasher()
	wantGen.AddBytes([][]byte{[]byte("1"), []byte("21")})
	if gen.Digest != wantGen.Digest() {
		t.Errorf("generated-column digest = %s, want %s (hashed over id,n only)", gen.Digest, wantGen.Digest())
	}

	// A missing table errors loudly, never a zero-row "match".
	if _, err := consistentTableChecksumPG(ctx, conn, "public", "pgvs_ck_it_nope", nil); err == nil {
		t.Error("want an error for a nonexistent table")
	}
}

// TestVerifyTablePG_Integration is the end-to-end parity proof for #1024:
// a pgbaseline snapshot of a live PG table, an index seeded with the
// relation's schema snapshot and a PG stream checkpoint, and
// verify.VerifyTablePG — wired through LiveSource, exactly as
// cmd/bintrail-pg and the console daemon wire it — comparing the live source
// against the reconstruction. Four legs: MATCH proves the live checksum and
// the reconstruct digest agree byte-for-byte on identical data across the
// tricky renderings (bool, numeric scale, multibyte, NULL vs empty); an
// UNCAPTURED in-place UPDATE is a conclusive content MISMATCH; indexing that
// same UPDATE as a pgoutput-shaped delta event folds it onto the baseline and
// restores MATCH (the delta half of the engine — the PK text-identity join
// and the pgTextPK merge — running against real data); an uncaptured DELETE
// is a conclusive row-count MISMATCH.
//
// Requires BOTH backends: a MySQL index (testutil.CreateTestDB) and a live
// PostgreSQL (BINTRAIL_TEST_PG_DSN).
func TestVerifyTablePG_Integration(t *testing.T) {
	baseDSN := testutil.SkipIfNoPostgres(t)
	indexDB, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	// The delta window (baseline snapshot → now) must be partition-covered or
	// the planner's gap check reports the current hour as rotated-and-lost —
	// same setup rule as every MySQL live-source verify test.
	curHour := time.Now().UTC().Truncate(time.Hour)
	// The hour after the current one has a partition too: verify reads up to
	// its own clock, taken after this fixture, and an hour with no partition
	// in that window makes the verdict inconclusive. Without it a test started
	// in the last second of an hour failed.
	testutil.SetupPartitionedTable(t, indexDB, dbName, []time.Time{curHour.Add(-time.Hour), curHour, curHour.Add(time.Hour)})
	ctx := context.Background()

	setup, err := pgx.Connect(ctx, baseDSN)
	if err != nil {
		t.Fatalf("connect setup conn: %v", err)
	}
	t.Cleanup(func() { setup.Close(context.Background()) })

	dropAll := func() {
		bg := context.Background()
		_, _ = setup.Exec(bg, fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", vpgPub))
		_, _ = setup.Exec(bg, fmt.Sprintf("DROP TABLE IF EXISTS %s", vpgTbl))
		_, _ = setup.Exec(bg, "SELECT pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", vpgSlot)
	}
	dropAll()
	t.Cleanup(dropAll)

	mustExec := func(sql string, args ...any) {
		t.Helper()
		if _, err := setup.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}

	mustExec(fmt.Sprintf(`CREATE TABLE %s (id int PRIMARY KEY, v text, ok bool, num numeric(10,2))`, vpgTbl))
	mustExec(fmt.Sprintf("INSERT INTO %s VALUES (1, 'café 日本語', true, 12.30)", vpgTbl))
	mustExec(fmt.Sprintf("INSERT INTO %s VALUES (2, '', false, NULL)", vpgTbl))
	mustExec(fmt.Sprintf("INSERT INTO %s VALUES (3, NULL, NULL, 0.01)", vpgTbl))
	mustExec(fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", vpgPub, vpgTbl))

	outDir := t.TempDir()
	if _, err := pgbaseline.Run(ctx, pgbaseline.Config{
		QueryDSN:    baseDSN,
		ReplDSN:     pgReplDSN(baseDSN),
		SlotName:    vpgSlot,
		Publication: vpgPub,
		OutputDir:   outDir,
	}); err != nil {
		t.Fatalf("pgbaseline.Run: %v", err)
	}

	// Index side: the relation's schema snapshot (what the capture daemon
	// writes on the first RelationMessage) ...
	if _, err := metadata.WritePGSnapshot(ctx, indexDB, &metadata.PGRelationSchema{
		Schema: "public", Table: vpgTbl,
		Columns: []metadata.PGRelationColumn{
			{Name: "id", Ordinal: 1, IsPK: true},
			{Name: "v", Ordinal: 2},
			{Name: "ok", Ordinal: 3},
			{Name: "num", Ordinal: 4},
		},
	}); err != nil {
		t.Fatalf("WritePGSnapshot: %v", err)
	}
	// ... and a PG stream checkpoint far past any real WAL position, so the
	// coverage verdict takes its PROVEN branch (checkpoint >= anchor) and the
	// run carries no coverage-unverified note.
	if _, err := indexDB.ExecContext(ctx, `
		INSERT INTO stream_state (id, mode, binlog_file, binlog_position, gtid_set, flavor, last_checkpoint, server_id)
		VALUES (1, 'gtid', 'FFFFFFFF/FFFFFFFF', ?, 'FFFFFFFF/FFFFFFFF', 'postgres', UTC_TIMESTAMP(), 1)`,
		uint64(1)<<62); err != nil {
		t.Fatalf("seed stream_state: %v", err)
	}

	resolver, err := verify.ResolverFor(indexDB)
	if err != nil {
		t.Fatalf("ResolverFor: %v", err)
	}
	sourceChecksum, closeSource, err := LiveSource(ctx, baseDSN)
	if err != nil {
		t.Fatalf("LiveSource: %v", err)
	}
	t.Cleanup(func() { _ = closeSource() })

	cfg := verify.PGLiveConfig{
		SourceChecksum: sourceChecksum,
		IndexDB:        indexDB,
		Resolver:       resolver,
		BaselineSource: outDir,
		IndexDBName:    dbName,
		NoArchive:      true,
	}

	// MATCH: live table == baseline, no deltas.
	res, err := verify.VerifyTablePG(ctx, cfg, "public", vpgTbl)
	if err != nil {
		t.Fatalf("VerifyTablePG: %v", err)
	}
	if res.Status != verify.StatusMatch {
		t.Fatalf("status = %q (detail=%q), want match", res.Status, res.Detail)
	}
	if res.SourceRows != 3 || res.ReconstructRows != 3 {
		t.Errorf("rows src/recon = %d/%d, want 3/3", res.SourceRows, res.ReconstructRows)
	}
	if !strings.HasPrefix(res.Anchor, "LSN:") {
		t.Errorf("Anchor = %q, want an LSN: label", res.Anchor)
	}
	if res.Detail != "" {
		t.Errorf("Detail = %q, want empty on a proven-coverage match", res.Detail)
	}

	// MISMATCH (content): an in-place UPDATE the index never captured must
	// surface as a conclusive content divergence at equal row count.
	mustExec(fmt.Sprintf("UPDATE %s SET v = 'tampered' WHERE id = 1", vpgTbl))
	res, err = verify.VerifyTablePG(ctx, cfg, "public", vpgTbl)
	if err != nil {
		t.Fatalf("VerifyTablePG (tampered): %v", err)
	}
	if res.Status != verify.StatusMismatch || !strings.Contains(res.Detail, "content digest differs") {
		t.Fatalf("status=%q detail=%q, want a content mismatch", res.Status, res.Detail)
	}

	// DELTA MATCH: index the same UPDATE as the capture daemon would store it
	// — pgoutput text values in the row images, the raw text PK in pk_values —
	// and the reconstruction must fold it onto the baseline and agree with the
	// live table again. This is the delta half of the engine (time-bounded
	// FetchMerged window, PK text-identity join, pgTextPK merge) against real
	// data; without it a PK-spelling regression between the event and baseline
	// sides (the #1155/#1158 bug class) would ship green and turn every
	// captured write into a false MISMATCH.
	testutil.InsertEvent(t, indexDB, "0/1000000", 100, 200,
		time.Now().UTC().Format("2006-01-02 15:04:05"), nil,
		"public", vpgTbl, 2 /*UPDATE*/, "1", nil,
		[]byte(`{"id":"1","v":"café 日本語","ok":"t","num":"12.30"}`),
		[]byte(`{"id":"1","v":"tampered","ok":"t","num":"12.30"}`))
	res, err = verify.VerifyTablePG(ctx, cfg, "public", vpgTbl)
	if err != nil {
		t.Fatalf("VerifyTablePG (delta indexed): %v", err)
	}
	if res.Status != verify.StatusMatch {
		t.Fatalf("status = %q (detail=%q), want match once the UPDATE is indexed — the delta fold or the PK join is broken", res.Status, res.Detail)
	}

	// MISMATCH (row count): an uncaptured DELETE is always conclusive.
	mustExec(fmt.Sprintf("DELETE FROM %s WHERE id = 2", vpgTbl))
	res, err = verify.VerifyTablePG(ctx, cfg, "public", vpgTbl)
	if err != nil {
		t.Fatalf("VerifyTablePG (deleted): %v", err)
	}
	if res.Status != verify.StatusMismatch || !strings.Contains(res.Detail, "row count differs") {
		t.Fatalf("status=%q detail=%q, want a row-count mismatch", res.Status, res.Detail)
	}
}

// pgReplDSN appends replication=database — same helper shape as the
// pgbaseline integration test's replDSN (unexported there).
func pgReplDSN(base string) string {
	if strings.Contains(base, "?") {
		return base + "&replication=database"
	}
	return base + "?replication=database"
}
