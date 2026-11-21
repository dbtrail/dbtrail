//go:build integration

package pgbaseline_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"log/slog"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/baselineintegrity"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/pgbaseline"
	"github.com/dbtrail/dbtrail/internal/pgcapture"
	"github.com/dbtrail/dbtrail/internal/snapshotdir"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// This file pins the HEADLINE guarantees (review round): the snapshot/delta
// boundary, the RawText↔pgoutput identity join, cancellation liveness of the
// worker pool, mid-run failure leaving no publishable snapshot, and the
// partitioned-table naming contract. Shared-server hygiene: every object name
// is branch-unique (_slice_c) and dropped in t.Cleanup.

// TestPGBaseline_SnapshotBoundary_Integration is THE core promise: a row
// committed immediately AFTER the anchor is read (via the test seam) must be
// ABSENT from the baseline Parquet and PRESENT in the slot's delta stream at
// an LSN strictly greater than the anchor. A refactor that moves the anchor
// SELECT out of the snapshot transaction's first statement fails here.
func TestPGBaseline_SnapshotBoundary_Integration(t *testing.T) {
	baseDSN := testutil.SkipIfNoPostgres(t)
	ctx := context.Background()

	const (
		tbl    = "pgbaseline_it_boundary_slice_c"
		pub    = "bintrail_pgbaseline_it_boundary_pub_slice_c"
		slot   = "bintrail_pgbaseline_it_boundary_slot_slice_c"
		marker = "boundary_marker_slice_c"
	)

	setup, err := pgx.Connect(ctx, baseDSN)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { setup.Close(context.Background()) })
	drop := func() {
		bg := context.Background()
		_, _ = setup.Exec(bg, "DROP PUBLICATION IF EXISTS "+pub)
		_, _ = setup.Exec(bg, "DROP TABLE IF EXISTS "+tbl)
		_, _ = setup.Exec(bg, "SELECT pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", slot)
	}
	drop()
	t.Cleanup(drop)

	mustExec(t, setup, fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY, v text)", tbl))
	mustExec(t, setup, fmt.Sprintf("INSERT INTO %s VALUES (1, 'pre-snapshot')", tbl))
	mustExec(t, setup, fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", pub, tbl))

	// The seam: commit the marker row from a SECOND connection the moment the
	// anchor is read. The insert is autocommit and synchronous, so by the time
	// the hook returns the row is durably committed — after the snapshot, so
	// it MUST be invisible to the baseline and carried by the deltas.
	restore := pgbaseline.SetTestHookAfterSnapshot(func() {
		conn2, err := pgx.Connect(ctx, baseDSN)
		if err != nil {
			t.Errorf("hook connect: %v", err)
			return
		}
		defer conn2.Close(context.Background())
		if _, err := conn2.Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES (2, $1)", tbl), marker); err != nil {
			t.Errorf("hook insert: %v", err)
		}
	})
	defer restore()

	outDir := t.TempDir()
	stats, err := pgbaseline.Run(ctx, pgbaseline.Config{
		QueryDSN:    baseDSN,
		ReplDSN:     replDSN(baseDSN),
		SlotName:    slot,
		Publication: pub,
		OutputDir:   outDir,
		Parallelism: 1, // serial: the COPY runs on the anchor transaction itself
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// ABSENT from the baseline.
	snapDir := singleSnapshotDir(t, outDir)
	rows := readRawRows(t, filepath.Join(snapDir, "public", tbl+".parquet"))
	if len(rows) != 1 {
		t.Fatalf("baseline has %d rows, want 1 (the post-anchor row leaked into the snapshot)", len(rows))
	}
	if got := rows[0]["v"]; got == nil || *got != "pre-snapshot" {
		t.Errorf("baseline row v = %v, want the pre-snapshot row only", strOrNull(got))
	}

	// PRESENT in the slot's deltas (peek does not consume). Two-level LSN
	// assertion:
	//   - the marker row's own change record sits AT or after the anchor
	//     (pg_current_wal_lsn() is the next-write position, so a back-to-back
	//     insert's record can start exactly AT it);
	//   - the marker transaction's COMMIT record is STRICTLY after the anchor
	//     — the load-bearing invariant: pgstreamrun stamps indexed events with
	//     the transaction's commit LSN, so "deltas strictly after the anchor"
	//     is a commit-LSN contract.
	var rowLSNText string
	err = setup.QueryRow(ctx, `
		SELECT min(lsn)::text
		FROM pg_logical_slot_peek_binary_changes($1, NULL, NULL, 'proto_version', '1', 'publication_names', $2)
		WHERE position($3::bytea in data) > 0`,
		slot, pub, []byte(marker)).Scan(&rowLSNText)
	if err != nil {
		t.Fatalf("peek slot changes for the marker row: %v (the post-anchor commit must be in the delta stream)", err)
	}
	if rowLSN := parseLSNText(t, rowLSNText); uint64(rowLSN) < stats.AnchorLSN {
		t.Errorf("marker row record at LSN %d, want >= anchor %d", uint64(rowLSN), stats.AnchorLSN)
	}
	// pgoutput message type byte: 'C' (67) = Commit. Take the commit record of
	// the marker row's transaction (same xid).
	var commitLSNText string
	err = setup.QueryRow(ctx, `
		SELECT min(lsn)::text
		FROM pg_logical_slot_peek_binary_changes($1, NULL, NULL, 'proto_version', '1', 'publication_names', $2)
		WHERE get_byte(data, 0) = 67
		  AND xid IN (
			SELECT xid
			FROM pg_logical_slot_peek_binary_changes($1, NULL, NULL, 'proto_version', '1', 'publication_names', $2)
			WHERE position($3::bytea in data) > 0)`,
		slot, pub, []byte(marker)).Scan(&commitLSNText)
	if err != nil {
		t.Fatalf("peek commit record of the marker transaction: %v", err)
	}
	if commitLSN := parseLSNText(t, commitLSNText); uint64(commitLSN) <= stats.AnchorLSN {
		t.Errorf("marker COMMIT at LSN %d, want strictly > anchor %d — reconstruct's strictly-after-anchor delta window would lose it", uint64(commitLSN), stats.AnchorLSN)
	}
}

// TestPGBaseline_IdentityJoin_Integration pins the RawText contract by
// construction: for the SAME rows, the baseline's raw COPY text must equal
// byte-for-byte what pgcapture's decoder yields from the pgoutput stream —
// per column, across bytea/timestamptz/numeric/bool/float8.
func TestPGBaseline_IdentityJoin_Integration(t *testing.T) {
	baseDSN := testutil.SkipIfNoPostgres(t)
	ctx := context.Background()

	const (
		tbl  = "pgbaseline_it_ident_slice_c"
		pub  = "bintrail_pgbaseline_it_ident_pub_slice_c"
		slot = "bintrail_pgbaseline_it_ident_slot_slice_c"
	)

	setup, err := pgx.Connect(ctx, baseDSN)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { setup.Close(context.Background()) })
	drop := func() {
		bg := context.Background()
		_, _ = setup.Exec(bg, "DROP PUBLICATION IF EXISTS "+pub)
		_, _ = setup.Exec(bg, "DROP TABLE IF EXISTS "+tbl)
		_, _ = setup.Exec(bg, "SELECT pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", slot)
	}
	drop()
	t.Cleanup(drop)

	mustExec(t, setup, fmt.Sprintf(
		"CREATE TABLE %s (id int PRIMARY KEY, b bytea, ts timestamptz, num numeric, ok bool, f8 float8)", tbl))
	mustExec(t, setup, fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL", tbl))
	mustExec(t, setup, fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", pub, tbl))

	// First run creates the slot (table still empty — the deltas start here).
	if _, err := pgbaseline.Run(ctx, pgbaseline.Config{
		QueryDSN: baseDSN, ReplDSN: replDSN(baseDSN),
		SlotName: slot, Publication: pub, OutputDir: t.TempDir(),
	}); err != nil {
		t.Fatalf("first Run (slot creation): %v", err)
	}

	// Typed rows land AFTER the slot: both the second baseline and the delta
	// stream carry them, so the two renderings can be compared byte-for-byte.
	mustExec(t, setup, fmt.Sprintf(
		`INSERT INTO %s VALUES
		 (1, '\x00ff7f2d'::bytea, '2026-07-03 12:34:56.789012+02'::timestamptz, 12345.678900, true, 1.5),
		 (2, ''::bytea, '2026-01-01 00:00:00+00'::timestamptz, -0.000001, false, 2.5e-15)`, tbl))

	outDir := t.TempDir()
	if _, err := pgbaseline.Run(ctx, pgbaseline.Config{
		QueryDSN: baseDSN, SlotName: slot, Publication: pub, OutputDir: outDir,
	}); err != nil {
		t.Fatalf("second Run: %v", err)
	}
	baselineRows := readRawRows(t, filepath.Join(singleSnapshotDir(t, outDir), "public", tbl+".parquet"))
	if len(baselineRows) != 2 {
		t.Fatalf("baseline rows = %d, want 2", len(baselineRows))
	}

	// Decode the same rows from the slot through the REAL pgoutput decoder.
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	cap := pgcapture.New(pgcapture.Config{
		ReplDSN:         replDSN(baseDSN),
		QueryDSN:        baseDSN,
		SlotName:        slot,
		Publication:     pub,
		Filters:         event.Filters{Tables: map[string]bool{"public." + tbl: true}},
		StandbyInterval: 200 * time.Millisecond,
	})
	events := make(chan event.Event, 64)
	runErr := make(chan error, 1)
	go func() { runErr <- cap.Run(runCtx, events) }()

	inserts := map[string]map[string]any{} // pk → RowAfter
	deadline := time.After(20 * time.Second)
	for len(inserts) < 2 {
		select {
		case ev := <-events:
			if ev.EventType == event.EventInsert && ev.Table == tbl {
				inserts[ev.PKValues] = ev.RowAfter
			}
		case err := <-runErr:
			t.Fatalf("capturer exited early: %v", err)
		case <-deadline:
			t.Fatalf("timed out waiting for 2 INSERT events; got %d", len(inserts))
		}
	}
	cancel()
	<-runErr

	for _, pk := range []string{"1", "2"} {
		brow := rowByCol(t, baselineRows, "id", pk)
		erow := inserts[pk]
		if erow == nil {
			t.Fatalf("no INSERT event for pk %s", pk)
		}
		for _, col := range []string{"id", "b", "ts", "num", "ok", "f8"} {
			ev, isStr := erow[col].(string)
			if !isStr {
				t.Errorf("pk %s col %s: event value is %T, want string (NULL not expected here)", pk, col, erow[col])
				continue
			}
			bv := brow[col]
			if bv == nil {
				t.Errorf("pk %s col %s: baseline NULL, event %q", pk, col, ev)
				continue
			}
			if *bv != ev {
				t.Errorf("IDENTITY JOIN BROKEN: pk %s col %s: baseline %q != pgoutput %q", pk, col, *bv, ev)
			}
		}
	}
}

// TestPGBaseline_CancelMidRun_Integration pins the worker-pool liveness fix
// (review blocker): more tables than workers, cancelled mid-COPY — Run must
// RETURN (no deadlock) with an error, and the snapshot must stay _INCOMPLETE.
func TestPGBaseline_CancelMidRun_Integration(t *testing.T) {
	baseDSN := testutil.SkipIfNoPostgres(t)
	ctx := context.Background()

	const (
		pfx  = "pgbaseline_it_cancel_slice_c"
		pub  = "bintrail_pgbaseline_it_cancel_pub_slice_c"
		slot = "bintrail_pgbaseline_it_cancel_slot_slice_c"
	)
	tbls := []string{pfx + "_1", pfx + "_2", pfx + "_3", pfx + "_4", pfx + "_big"}

	setup, err := pgx.Connect(ctx, baseDSN)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { setup.Close(context.Background()) })
	drop := func() {
		bg := context.Background()
		_, _ = setup.Exec(bg, "DROP PUBLICATION IF EXISTS "+pub)
		for _, tb := range tbls {
			_, _ = setup.Exec(bg, "DROP TABLE IF EXISTS "+tb)
		}
		_, _ = setup.Exec(bg, "SELECT pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", slot)
	}
	drop()
	t.Cleanup(drop)

	for _, tb := range tbls {
		mustExec(t, setup, fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY, v text)", tb))
		mustExec(t, setup, fmt.Sprintf("INSERT INTO %s VALUES (1, 'x')", tb))
	}
	// The big table keeps at least one COPY busy well past the cancel point.
	mustExec(t, setup, fmt.Sprintf(
		"INSERT INTO %s SELECT g, repeat('y', 200) FROM generate_series(2, 1000000) g", tbls[4]))
	mustExec(t, setup, fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", pub, strings.Join(tbls, ", ")))

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		time.Sleep(250 * time.Millisecond)
		cancel()
	}()

	outDir := t.TempDir()
	type result struct {
		stats pgbaseline.Stats
		err   error
	}
	done := make(chan result, 1)
	go func() {
		stats, err := pgbaseline.Run(runCtx, pgbaseline.Config{
			QueryDSN: baseDSN, ReplDSN: replDSN(baseDSN),
			SlotName: slot, Publication: pub, OutputDir: outDir,
			Parallelism: 2, // 5 tables > 2 workers: the old unbuffered feeder deadlocked here
		})
		done <- result{stats, err}
	}()

	select {
	case res := <-done:
		if res.err == nil {
			t.Fatal("Run succeeded despite mid-run cancellation, want an error")
		}
	case <-time.After(60 * time.Second):
		t.Fatal("DEADLOCK: Run did not return within 60s of cancellation (worker-pool feeder blocked)")
	}

	// Whatever partial state exists must be positively marked incomplete.
	entries, err := os.ReadDir(outDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) == 1 {
		snapDir := filepath.Join(outDir, entries[0].Name())
		if baseline.SnapshotComplete(snapDir) {
			t.Error("cancelled snapshot reads as COMPLETE — _INCOMPLETE marker missing")
		}
		if _, err := os.Stat(filepath.Join(snapDir, baseline.SuccessMarker)); err == nil {
			t.Error("_SUCCESS marker present on a cancelled run")
		}
	}
}

// TestPGBaseline_MidRunTableFailure_Integration: one table's COPY fails
// (permission denied for a restricted role) — the run must fail naming the
// table, and the snapshot must not be publishable: no _SUCCESS, no manifest,
// SnapshotComplete false (which is exactly what baseline discovery checks),
// and the failed table's partial Parquet removed.
func TestPGBaseline_MidRunTableFailure_Integration(t *testing.T) {
	baseDSN := testutil.SkipIfNoPostgres(t)
	ctx := context.Background()

	const (
		tblOK     = "pgbaseline_it_fail_ok_slice_c"
		tblDenied = "pgbaseline_it_fail_denied_slice_c"
		pub       = "bintrail_pgbaseline_it_fail_pub_slice_c"
		slot      = "bintrail_pgbaseline_it_fail_slot_slice_c"
		role      = "bintrail_pgb_limited_slice_c"
	)

	setup, err := pgx.Connect(ctx, baseDSN)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { setup.Close(context.Background()) })
	drop := func() {
		bg := context.Background()
		_, _ = setup.Exec(bg, "DROP PUBLICATION IF EXISTS "+pub)
		_, _ = setup.Exec(bg, "DROP TABLE IF EXISTS "+tblOK)
		_, _ = setup.Exec(bg, "DROP TABLE IF EXISTS "+tblDenied)
		_, _ = setup.Exec(bg, "SELECT pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", slot)
		_, _ = setup.Exec(bg, "DROP OWNED BY "+role)
		_, _ = setup.Exec(bg, "DROP ROLE IF EXISTS "+role)
	}
	drop()
	t.Cleanup(drop)

	mustExec(t, setup, fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY, v text)", tblOK))
	mustExec(t, setup, fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY, v text)", tblDenied))
	mustExec(t, setup, fmt.Sprintf("INSERT INTO %s VALUES (1, 'a')", tblOK))
	mustExec(t, setup, fmt.Sprintf("INSERT INTO %s VALUES (1, 'b')", tblDenied))
	mustExec(t, setup, fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s, %s", pub, tblOK, tblDenied))
	// Slot pre-created (the restricted role has no REPLICATION privilege).
	mustExec(t, setup, "SELECT pg_create_logical_replication_slot($1, 'pgoutput')", slot)

	mustExec(t, setup, fmt.Sprintf("CREATE ROLE %s LOGIN PASSWORD 'testpgb'", role))
	mustExec(t, setup, fmt.Sprintf("GRANT SELECT ON %s TO %s", tblOK, role)) // tblDenied: NO grant
	mustExec(t, setup, fmt.Sprintf("GRANT pg_read_all_stats TO %s", role))   // pg_replication_slots visibility

	// A role that cannot read a table is refused before anything is written
	// (#3022), so that check is taken out here: this test is about a table
	// that fails once the copy is under way, as it still can when the
	// permission is taken away between the check and the COPY.
	defer pgbaseline.SetCheckReadAccess(func(context.Context, *pgx.Conn, string, event.Filters) error { return nil })()

	outDir := t.TempDir()
	_, err = pgbaseline.Run(ctx, pgbaseline.Config{
		QueryDSN:    limitedDSN(t, baseDSN, role, "testpgb"),
		SlotName:    slot,
		Publication: pub,
		OutputDir:   outDir,
		Parallelism: 1,
	})
	if err == nil {
		t.Fatal("Run succeeded with a permission-denied table, want an error")
	}
	if !strings.Contains(err.Error(), tblDenied) {
		t.Errorf("error %q does not name the failed table %s", err, tblDenied)
	}

	snapDir := singleSnapshotDir(t, outDir)
	if baseline.SnapshotComplete(snapDir) {
		t.Error("failed snapshot reads as COMPLETE — discovery (reconstruct.FindBaseline) would serve it")
	}
	if _, err := os.Stat(filepath.Join(snapDir, baseline.SuccessMarker)); err == nil {
		t.Error("_SUCCESS marker present on a failed run")
	}
	if _, ok, _ := baselineintegrity.LoadManifest(snapDir); ok {
		t.Error("integrity manifest present on a failed run")
	}
	if _, err := os.Stat(filepath.Join(snapDir, "public", tblDenied+".parquet")); err == nil {
		t.Error("failed table's partial Parquet was not removed")
	}
}

// TestPGBaseline_PartitionedTables_Integration pins the naming contract for
// partitioned tables under both publish_via_partition_root settings, and that
// the loud pubviaroot warning fires.
func TestPGBaseline_PartitionedTables_Integration(t *testing.T) {
	baseDSN := testutil.SkipIfNoPostgres(t)
	ctx := context.Background()

	const (
		parent   = "pgbaseline_it_part_slice_c"
		pubRoot  = "bintrail_pgbaseline_it_part_root_pub_slice_c"
		pubLeaf  = "bintrail_pgbaseline_it_part_leaf_pub_slice_c"
		slotName = "bintrail_pgbaseline_it_part_slot_slice_c"
	)
	leaf1, leaf2 := parent+"_p1", parent+"_p2"

	setup, err := pgx.Connect(ctx, baseDSN)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { setup.Close(context.Background()) })
	drop := func() {
		bg := context.Background()
		_, _ = setup.Exec(bg, "DROP PUBLICATION IF EXISTS "+pubRoot)
		_, _ = setup.Exec(bg, "DROP PUBLICATION IF EXISTS "+pubLeaf)
		_, _ = setup.Exec(bg, "DROP TABLE IF EXISTS "+parent)
		_, _ = setup.Exec(bg, "SELECT pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", slotName)
	}
	drop()
	t.Cleanup(drop)

	mustExec(t, setup, fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY, v text) PARTITION BY RANGE (id)", parent))
	mustExec(t, setup, fmt.Sprintf("CREATE TABLE %s PARTITION OF %s FOR VALUES FROM (0) TO (100)", leaf1, parent))
	mustExec(t, setup, fmt.Sprintf("CREATE TABLE %s PARTITION OF %s FOR VALUES FROM (100) TO (200)", leaf2, parent))
	mustExec(t, setup, fmt.Sprintf("INSERT INTO %s VALUES (1,'a'), (2,'b'), (150,'c'), (151,'d'), (152,'e')", parent))
	mustExec(t, setup, fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s WITH (publish_via_partition_root = true)", pubRoot, parent))
	mustExec(t, setup, fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", pubLeaf, parent))

	runWith := func(publication string) (string, *logCapture) {
		t.Helper()
		lc := newLogCapture()
		outDir := t.TempDir()
		_, err := pgbaseline.Run(ctx, pgbaseline.Config{
			QueryDSN: baseDSN, ReplDSN: replDSN(baseDSN),
			SlotName: slotName, Publication: publication, OutputDir: outDir,
			Logger: lc.logger,
		})
		if err != nil {
			t.Fatalf("Run(%s): %v", publication, err)
		}
		return singleSnapshotDir(t, outDir), lc
	}

	// pubviaroot=true: ONE file under the PARENT name with all 5 rows.
	snapRoot, lcRoot := runWith(pubRoot)
	rowsParent := readRawRows(t, filepath.Join(snapRoot, "public", parent+".parquet"))
	if len(rowsParent) != 5 {
		t.Errorf("parent-named baseline rows = %d, want 5 (all partitions under the parent)", len(rowsParent))
	}
	for _, leaf := range []string{leaf1, leaf2} {
		if _, err := os.Stat(filepath.Join(snapRoot, "public", leaf+".parquet")); err == nil {
			t.Errorf("leaf file %s present under publish_via_partition_root=true", leaf)
		}
	}
	if !lcRoot.contains("partitioned table parent") {
		t.Error("no parent-name warning fired for publish_via_partition_root=true")
	}

	// pubviaroot=false (default): one file PER LEAF, no parent file.
	snapLeaf, lcLeaf := runWith(pubLeaf)
	rows1 := readRawRows(t, filepath.Join(snapLeaf, "public", leaf1+".parquet"))
	rows2 := readRawRows(t, filepath.Join(snapLeaf, "public", leaf2+".parquet"))
	if len(rows1) != 2 || len(rows2) != 3 {
		t.Errorf("leaf-named baselines rows = %d/%d, want 2/3", len(rows1), len(rows2))
	}
	if _, err := os.Stat(filepath.Join(snapLeaf, "public", parent+".parquet")); err == nil {
		t.Error("parent file present under publish_via_partition_root=false")
	}
	if !lcLeaf.contains("leaf partition") {
		t.Error("no leaf-name warning fired for publish_via_partition_root=false")
	}
}

// ── helpers ──

func mustExec(t *testing.T, conn *pgx.Conn, sql string, args ...any) {
	t.Helper()
	if _, err := conn.Exec(context.Background(), sql, args...); err != nil {
		t.Fatalf("exec %q: %v", sql, err)
	}
}

func parseLSNText(t *testing.T, s string) pglogrepl.LSN {
	t.Helper()
	lsn, err := pglogrepl.ParseLSN(s)
	if err != nil {
		t.Fatalf("parse LSN %q: %v", s, err)
	}
	return lsn
}

// singleSnapshotDir returns the one snapshot directory a baseline run wrote
// under outDir.
//
// It SELECTS by name rather than asserting the directory holds exactly one
// entry. A baselines root carries pointer machinery beside its snapshots since
// #1484 -- the `current` symlink and its flock file -- so a count is no longer
// a statement about how many snapshots were written, which is the thing these
// tests actually mean. Selecting on the timestamp name is the same predicate
// every discovery path uses, so this cannot drift from what the product reads.
func singleSnapshotDir(t *testing.T, outDir string) string {
	t.Helper()
	entries, err := os.ReadDir(outDir)
	if err != nil {
		t.Fatalf("read %s: %v", outDir, err)
	}
	var snapshots []string
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		if _, ok := snapshotdir.ParseTime(e.Name()); ok {
			snapshots = append(snapshots, e.Name())
		}
	}
	if len(snapshots) != 1 {
		t.Fatalf("expected exactly one snapshot dir in %s, got %v (all entries: %v)",
			outDir, snapshots, entries)
	}
	return filepath.Join(outDir, snapshots[0])
}

// limitedDSN rewrites the test DSN's credentials to the restricted role.
func limitedDSN(t *testing.T, base, user, pass string) string {
	t.Helper()
	u, err := url.Parse(base)
	if err != nil {
		t.Fatalf("parse DSN: %v", err)
	}
	u.User = url.UserPassword(user, pass)
	return u.String()
}

// logCapture collects slog output for warning assertions.
type logCapture struct {
	buf    *strings.Builder
	logger *slog.Logger
}

func newLogCapture() *logCapture {
	buf := &strings.Builder{}
	return &logCapture{buf: buf, logger: slog.New(slog.NewTextHandler(buf, nil))}
}

func (lc *logCapture) contains(sub string) bool { return strings.Contains(lc.buf.String(), sub) }
