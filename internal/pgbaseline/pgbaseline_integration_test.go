//go:build integration

package pgbaseline_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/parquet-go/parquet-go"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/baselineintegrity"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/pgbaseline"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// Names are branch-unique (_slice_c): the PG server is SHARED with other test
// runs, so nothing here may collide with theirs; everything is dropped in
// t.Cleanup.
const (
	itSlot   = "bintrail_pgbaseline_it_slice_c"
	itPub    = "bintrail_pgbaseline_it_pub_slice_c"
	itTblA   = "pgbaseline_it_a_slice_c" // TOAST-able text + multibyte + escapes
	itTblB   = "pgbaseline_it_b_slice_c" // NULL vs empty string discrimination
	itTblGen = "pgbaseline_it_gen_slice_c"
)

// TestPGBaseline_Integration drives pgbaseline.Run end-to-end against a live
// PostgreSQL: snapshot correctness (NULL vs empty, TOAST, escapes, multibyte),
// the LSN anchor metadata, the slot ordering invariant
// (confirmed_flush_lsn ≤ anchor), markers + manifest, generated-column
// exclusion, and a re-baseline run over the already-existing slot.
func TestPGBaseline_Integration(t *testing.T) {
	baseDSN := testutil.SkipIfNoPostgres(t)
	ctx := context.Background()

	setup, err := pgx.Connect(ctx, baseDSN)
	if err != nil {
		t.Fatalf("connect setup conn: %v", err)
	}
	t.Cleanup(func() { setup.Close(context.Background()) })

	dropAll := func(c *pgx.Conn) {
		bg := context.Background()
		_, _ = c.Exec(bg, fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", itPub))
		for _, tbl := range []string{itTblA, itTblB, itTblGen} {
			_, _ = c.Exec(bg, fmt.Sprintf("DROP TABLE IF EXISTS %s", tbl))
		}
		// Drop the slot last; it is never activated by this test (no
		// StartReplication), so no active_pid wait (#607/#625) is needed —
		// but keep the existence-gated form so a rerun after a partial
		// failure is clean.
		_, _ = c.Exec(bg, "SELECT pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", itSlot)
	}
	dropAll(setup)
	t.Cleanup(func() { dropAll(setup) })

	mustExec := func(sql string, args ...any) {
		t.Helper()
		if _, err := setup.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}

	// Table A: TOAST-able text (COPY reads full values — TOAST is a non-issue
	// for the baseline, prove it), plus values that stress the COPY text
	// escapes and multibyte UTF-8.
	mustExec(fmt.Sprintf(`CREATE TABLE %s (id int PRIMARY KEY, note text, big text)`, itTblA))
	mustExec(fmt.Sprintf("ALTER TABLE %s ALTER COLUMN big SET STORAGE EXTERNAL", itTblA))
	bigVal := strings.Repeat("Z", 8000)
	mustExec(fmt.Sprintf("INSERT INTO %s VALUES (1, $1, $2)", itTblA),
		"tab\there\nnewline\\backslash café 日本語", bigVal)
	mustExec(fmt.Sprintf("INSERT INTO %s VALUES (2, $1, NULL)", itTblA), `\N literal`)

	// Prove big is genuinely out-of-line so the TOAST claim is actually tested.
	var toastSize int64
	if err := setup.QueryRow(ctx, "SELECT pg_relation_size(reltoastrelid) FROM pg_class WHERE oid = $1::regclass", "public."+itTblA).Scan(&toastSize); err != nil {
		t.Fatalf("toast relation size: %v", err)
	}
	if toastSize == 0 {
		t.Fatal("TOAST relation is empty — big stored inline, TOAST path not exercised")
	}

	// Table B: NULL vs empty string must stay distinguishable end-to-end.
	mustExec(fmt.Sprintf(`CREATE TABLE %s (id int PRIMARY KEY, v text)`, itTblB))
	mustExec(fmt.Sprintf("INSERT INTO %s VALUES (1, '')", itTblB))
	mustExec(fmt.Sprintf("INSERT INTO %s VALUES (2, NULL)", itTblB))
	mustExec(fmt.Sprintf("INSERT INTO %s VALUES (3, 'x')", itTblB))

	// Table Gen: a STORED generated column must be EXCLUDED (pgoutput never
	// streams it on PG 14–17, so the delta path never carries it).
	mustExec(fmt.Sprintf(`CREATE TABLE %s (id int PRIMARY KEY, n int, twice int GENERATED ALWAYS AS (n * 2) STORED)`, itTblGen))
	mustExec(fmt.Sprintf("INSERT INTO %s (id, n) VALUES (1, 21)", itTblGen))

	mustExec(fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s, %s, %s", itPub, itTblA, itTblB, itTblGen))

	outDir := t.TempDir()
	stats, err := pgbaseline.Run(ctx, pgbaseline.Config{
		QueryDSN:    baseDSN,
		ReplDSN:     replDSN(baseDSN),
		SlotName:    itSlot,
		Publication: itPub,
		OutputDir:   outDir,
		Compression: "zstd",
		Parallelism: 2, // exercise the exported-snapshot worker path
	})
	if err != nil {
		t.Fatalf("pgbaseline.Run: %v", err)
	}
	if stats.TablesProcessed != 3 || stats.FilesWritten != 3 {
		t.Errorf("stats tables/files = %d/%d, want 3/3", stats.TablesProcessed, stats.FilesWritten)
	}
	if stats.RowsWritten != 6 {
		t.Errorf("stats.RowsWritten = %d, want 6", stats.RowsWritten)
	}
	if !stats.SlotCreated {
		t.Error("stats.SlotCreated = false, want true on first run")
	}
	if stats.AnchorLSN == 0 {
		t.Error("stats.AnchorLSN = 0, want a real LSN")
	}
	// #771: DeltaStartLSN is the corrected, safe delta-replay floor (the
	// slot's own confirmed_flush_lsn/restart_lsn, read before the snapshot
	// began) and must never exceed the live snapshot anchor.
	if stats.DeltaStartLSN == 0 {
		t.Error("stats.DeltaStartLSN = 0, want a real LSN")
	}
	if stats.DeltaStartLSN > stats.AnchorLSN {
		t.Errorf("stats.DeltaStartLSN %d > stats.AnchorLSN %d — safety invariant violated", stats.DeltaStartLSN, stats.AnchorLSN)
	}

	// ── Snapshot directory, markers, manifest ──
	snapDir := singleSnapshotDir(t, outDir)
	if _, err := os.Stat(filepath.Join(snapDir, baseline.SuccessMarker)); err != nil {
		t.Errorf("_SUCCESS marker missing: %v", err)
	}
	if !baseline.SnapshotComplete(snapDir) {
		t.Error("SnapshotComplete = false, want true")
	}
	if _, ok, err := baselineintegrity.LoadManifest(snapDir); err != nil || !ok {
		t.Errorf("LoadManifest: ok=%v err=%v, want a valid manifest", ok, err)
	}
	for _, tbl := range []string{itTblA, itTblB, itTblGen} {
		if err := baselineintegrity.ValidateLocalFile(filepath.Join(snapDir, "public", tbl+".parquet")); err != nil {
			t.Errorf("manifest validation of %s: %v", tbl, err)
		}
	}

	// ── MetaKeyLSN: present, equals Stats.DeltaStartLSN (#771 — NOT
	// Stats.AnchorLSN; they only coincide here because nothing else wrote WAL
	// between slot creation and the snapshot anchor in this first run), ≤
	// current WAL ──
	pathA := filepath.Join(snapDir, "public", itTblA+".parquet")
	md := readParquetMetadata(t, pathA)
	lsnStr, ok := md[baseline.MetaKeyLSN]
	if !ok {
		t.Fatalf("MetaKeyLSN absent from %s metadata (%v)", pathA, md)
	}
	// #1570: a PostgreSQL baseline is its own read of the source, stamped
	// with its own snapshot instant and zero updates since.
	if md[baseline.MetaKeyLastDumpAt] == "" || md[baseline.MetaKeyLastDumpAt] != md[baseline.MetaKeySnapshotTimestamp] ||
		md[baseline.MetaKeyFoldGeneration] != "0" {
		t.Errorf("source read = %q / %q, want the snapshot's own instant %q and 0 updates",
			md[baseline.MetaKeyLastDumpAt], md[baseline.MetaKeyFoldGeneration], md[baseline.MetaKeySnapshotTimestamp])
	}
	embeddedLSN, err := strconv.ParseUint(lsnStr, 10, 64)
	if err != nil {
		t.Fatalf("MetaKeyLSN %q is not a decimal uint64: %v", lsnStr, err)
	}
	if embeddedLSN != stats.DeltaStartLSN {
		t.Errorf("MetaKeyLSN %d != Stats.DeltaStartLSN %d", embeddedLSN, stats.DeltaStartLSN)
	}
	var curLSNText string
	if err := setup.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&curLSNText); err != nil {
		t.Fatalf("pg_current_wal_lsn: %v", err)
	}
	curLSN, err := pglogrepl.ParseLSN(curLSNText)
	if err != nil {
		t.Fatalf("parse current LSN: %v", err)
	}
	if embeddedLSN > uint64(curLSN) {
		t.Errorf("embedded delta-floor LSN %d > current WAL LSN %d", embeddedLSN, uint64(curLSN))
	}
	if md["bintrail.source_database"] != "public" || md["bintrail.source_table"] != itTblA {
		t.Errorf("source metadata wrong: %v", md)
	}
	if _, hasCreate := md[baseline.MetaKeyCreateTableSQL]; hasCreate {
		t.Error("MetaKeyCreateTableSQL present — must be omitted for PG baselines")
	}

	// ── Ordering invariant: slot exists, confirmed_flush_lsn ≤ embedded floor
	// ≤ anchor ── The slot was created BEFORE the snapshot transaction, so its
	// consistent point (= confirmed_flush_lsn on a fresh, never-acked slot)
	// cannot be past the anchor; the overlap is redelivered and merged
	// idempotently. Re-reading confirmed_flush_lsn here (nothing has consumed
	// the slot since Run) should match what got embedded.
	var flushText string
	if err := setup.QueryRow(ctx, "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=$1", itSlot).Scan(&flushText); err != nil {
		t.Fatalf("slot %s missing or unreadable after Run: %v", itSlot, err)
	}
	flush, err := pglogrepl.ParseLSN(flushText)
	if err != nil {
		t.Fatalf("parse confirmed_flush_lsn %q: %v", flushText, err)
	}
	if uint64(flush) != embeddedLSN {
		t.Errorf("slot confirmed_flush_lsn %d != embedded delta-floor LSN %d (slot untouched since Run)", uint64(flush), embeddedLSN)
	}

	// ── Table A contents: TOAST value complete, escapes + multibyte intact ──
	rowsA := readRawRows(t, pathA) // columns alphabetical: big, id, note
	if len(rowsA) != 2 {
		t.Fatalf("table A: %d rows, want 2", len(rowsA))
	}
	a1 := rowByCol(t, rowsA, "id", "1")
	if got := a1["big"]; got == nil || *got != bigVal {
		t.Errorf("table A row 1 big: got %d bytes, want the full %d-byte TOASTed value", lenOrNull(got), len(bigVal))
	}
	if got := a1["note"]; got == nil || *got != "tab\there\nnewline\\backslash café 日本語" {
		t.Errorf("table A row 1 note = %v, want escapes+multibyte intact", strOrNull(got))
	}
	a2 := rowByCol(t, rowsA, "id", "2")
	if got := a2["note"]; got == nil || *got != `\N literal` {
		t.Errorf("table A row 2 note = %v, want the literal backslash-N string", strOrNull(got))
	}
	if a2["big"] != nil {
		t.Errorf("table A row 2 big = %q, want NULL", *a2["big"])
	}

	// ── Table B: NULL vs empty string distinguished ──
	rowsB := readRawRows(t, filepath.Join(snapDir, "public", itTblB+".parquet"))
	if len(rowsB) != 3 {
		t.Fatalf("table B: %d rows, want 3", len(rowsB))
	}
	b1 := rowByCol(t, rowsB, "id", "1")
	if got := b1["v"]; got == nil || *got != "" {
		t.Errorf("table B row 1 v = %v, want non-NULL empty string", strOrNull(got))
	}
	b2 := rowByCol(t, rowsB, "id", "2")
	if b2["v"] != nil {
		t.Errorf("table B row 2 v = %q, want NULL", *b2["v"])
	}
	b3 := rowByCol(t, rowsB, "id", "3")
	if got := b3["v"]; got == nil || *got != "x" {
		t.Errorf("table B row 3 v = %v, want %q", strOrNull(got), "x")
	}

	// ── Generated column excluded; base columns intact ──
	rowsG := readRawRows(t, filepath.Join(snapDir, "public", itTblGen+".parquet"))
	if len(rowsG) != 1 {
		t.Fatalf("table Gen: %d rows, want 1", len(rowsG))
	}
	if _, has := rowsG[0]["twice"]; has {
		t.Error("generated column 'twice' present in baseline — must be excluded to match the delta path's column set")
	}
	g1 := rowByCol(t, rowsG, "id", "1")
	if got := g1["n"]; got == nil || *got != "21" {
		t.Errorf("table Gen n = %v, want %q", strOrNull(got), "21")
	}

	// ── Re-baseline: slot already exists, NO replication DSN needed — the
	// first-run and re-baseline path are the same code path by design. ──
	time.Sleep(1100 * time.Millisecond) // snapshot dirs are timestamp-named (1s granularity); don't collide
	mustExec(fmt.Sprintf("UPDATE %s SET v='updated' WHERE id=3", itTblB))
	outDir2 := t.TempDir()
	stats2, err := pgbaseline.Run(ctx, pgbaseline.Config{
		QueryDSN:    baseDSN,
		ReplDSN:     "", // must not be needed: the slot exists
		SlotName:    itSlot,
		Publication: itPub,
		OutputDir:   outDir2,
	})
	if err != nil {
		t.Fatalf("re-baseline Run: %v", err)
	}
	if stats2.SlotCreated {
		t.Error("re-baseline SlotCreated = true, want false (slot pre-existed)")
	}
	if stats2.AnchorLSN < stats.AnchorLSN {
		t.Errorf("re-baseline anchor %d < first anchor %d — anchors must be monotonic", stats2.AnchorLSN, stats.AnchorLSN)
	}
	snapDir2 := singleSnapshotDir(t, outDir2)
	if !baseline.SnapshotComplete(snapDir2) {
		t.Error("re-baseline snapshot not complete")
	}
	rowsB2 := readRawRows(t, filepath.Join(snapDir2, "public", itTblB+".parquet"))
	b3v2 := rowByCol(t, rowsB2, "id", "3")
	if got := b3v2["v"]; got == nil || *got != "updated" {
		t.Errorf("re-baseline table B row 3 v = %v, want %q", strOrNull(got), "updated")
	}

	// ── #771 pin: the slot was never consumed (no StartReplication) between
	// the two runs, so its floor cannot have moved even though the UPDATE
	// above committed and durably advanced the live WAL anchor in between.
	// This reproduces, deterministically, the exact shape of gap the bug
	// left open: a value the LIVE anchor alone cannot distinguish from "no
	// concurrent activity", but the slot floor correctly reflects. ──
	if stats2.DeltaStartLSN > stats2.AnchorLSN {
		t.Errorf("re-baseline DeltaStartLSN %d > AnchorLSN %d — safety invariant violated", stats2.DeltaStartLSN, stats2.AnchorLSN)
	}
	if stats2.DeltaStartLSN != stats.DeltaStartLSN {
		t.Errorf("re-baseline DeltaStartLSN %d != first-run DeltaStartLSN %d — the slot floor must not move when nothing consumed it", stats2.DeltaStartLSN, stats.DeltaStartLSN)
	}
	md2 := readParquetMetadata(t, filepath.Join(snapDir2, "public", itTblB+".parquet"))
	lsnStr2, ok := md2[baseline.MetaKeyLSN]
	if !ok {
		t.Fatalf("re-baseline MetaKeyLSN absent (%v)", md2)
	}
	embeddedLSN2, err := strconv.ParseUint(lsnStr2, 10, 64)
	if err != nil {
		t.Fatalf("re-baseline MetaKeyLSN %q is not a decimal uint64: %v", lsnStr2, err)
	}
	if embeddedLSN2 != stats2.DeltaStartLSN {
		t.Errorf("re-baseline embedded MetaKeyLSN %d != Stats.DeltaStartLSN %d", embeddedLSN2, stats2.DeltaStartLSN)
	}
	// THE core #771 regression pin: pre-fix, this metadata held AnchorLSN —
	// which, in this exact scenario (a commit lands between slot creation and
	// the second run's live anchor read with the slot never consumed), would
	// equal stats2.AnchorLSN and sit STRICTLY ABOVE the marker UPDATE's commit
	// LSN once accounting for the invisible-commit race #771 describes,
	// silently excluding a concurrently-committing transaction from the delta
	// window. The corrected embedded value must be the smaller, safe slot
	// floor — strictly below the live anchor here, since the UPDATE
	// committed (and advanced the live WAL position) after the slot was
	// created and with the slot never consumed since.
	if embeddedLSN2 >= stats2.AnchorLSN {
		t.Errorf("re-baseline embedded delta floor %d >= live anchor %d — want strictly less (an UPDATE committed between slot creation and this run's anchor read, with the slot never consumed)", embeddedLSN2, stats2.AnchorLSN)
	}
}

// TestPGBaseline_MissingSlotNoReplDSN_Fails: a missing slot with no replication
// DSN must be an actionable error, never a silent skip.
func TestPGBaseline_MissingSlotNoReplDSN_Fails(t *testing.T) {
	baseDSN := testutil.SkipIfNoPostgres(t)
	ctx := context.Background()

	const tbl = "pgbaseline_it_noslot_slice_c"
	const pub = "bintrail_pgbaseline_it_noslot_pub_slice_c"
	setup, err := pgx.Connect(ctx, baseDSN)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { setup.Close(context.Background()) })
	drop := func() {
		bg := context.Background()
		_, _ = setup.Exec(bg, "DROP PUBLICATION IF EXISTS "+pub)
		_, _ = setup.Exec(bg, "DROP TABLE IF EXISTS "+tbl)
	}
	drop()
	t.Cleanup(drop)
	if _, err := setup.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY)", tbl)); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := setup.Exec(ctx, fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", pub, tbl)); err != nil {
		t.Fatalf("create publication: %v", err)
	}

	_, err = pgbaseline.Run(ctx, pgbaseline.Config{
		QueryDSN:    baseDSN,
		SlotName:    "bintrail_pgbaseline_it_absent_slot_slice_c",
		Publication: pub,
		OutputDir:   t.TempDir(),
	})
	if err == nil {
		t.Fatal("Run succeeded with a missing slot and no repl DSN, want a fatal error")
	}
	if !strings.Contains(err.Error(), "does not exist") || !strings.Contains(err.Error(), "repl-dsn") {
		t.Errorf("error %q is not the actionable missing-slot message", err)
	}
}

// TestPGBaseline_EmptyPublication_Fails: a publication matching zero tables
// must never publish an empty baseline (the #461 guard, mirrored).
func TestPGBaseline_EmptyPublication_Fails(t *testing.T) {
	baseDSN := testutil.SkipIfNoPostgres(t)
	ctx := context.Background()

	const tbl = "pgbaseline_it_empty_slice_c"
	const pub = "bintrail_pgbaseline_it_empty_pub_slice_c"
	const slot = "bintrail_pgbaseline_it_empty_slot_slice_c"
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
	if _, err := setup.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY)", tbl)); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := setup.Exec(ctx, fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", pub, tbl)); err != nil {
		t.Fatalf("create publication: %v", err)
	}

	outDir := t.TempDir()
	_, err = pgbaseline.Run(ctx, pgbaseline.Config{
		QueryDSN:    baseDSN,
		ReplDSN:     replDSN(baseDSN),
		SlotName:    slot,
		Publication: pub,
		OutputDir:   outDir,
		// A filter that matches nothing → the empty-baseline guard fires.
		Filters: event.Filters{Tables: map[string]bool{"public.absent_table_slice_c": true}},
	})
	if err == nil {
		t.Fatal("Run succeeded with zero matching tables, want the empty-baseline guard")
	}
	if !strings.Contains(err.Error(), "no tables") {
		t.Errorf("error %q is not the empty-baseline guard", err)
	}
	// The guard fires before the snapshot directory is created — nothing to
	// mistake for a baseline may exist.
	if entries, _ := os.ReadDir(outDir); len(entries) != 0 {
		t.Errorf("output dir not empty after the guard: %v", entries)
	}
}

// ── helpers ──

func replDSN(base string) string {
	if strings.Contains(base, "?") {
		return base + "&replication=database"
	}
	return base + "?replication=database"
}

// readParquetMetadata returns the file's key-value footer metadata.
func readParquetMetadata(t *testing.T, path string) map[string]string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	pf, err := parquet.OpenFile(f, fi.Size())
	if err != nil {
		t.Fatalf("OpenFile %s: %v", path, err)
	}
	md := map[string]string{}
	for _, kv := range pf.Metadata().KeyValueMetadata {
		md[kv.Key] = kv.Value
	}
	return md
}

// readRawRows reads every row of a RawText Parquet file as column→value maps;
// a nil pointer means SQL NULL (distinguished from the empty string).
func readRawRows(t *testing.T, path string) []map[string]*string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	pf, err := parquet.OpenFile(f, fi.Size())
	if err != nil {
		t.Fatalf("OpenFile %s: %v", path, err)
	}
	fields := pf.Schema().Fields()
	reader := parquet.NewReader(pf)
	defer reader.Close()

	var out []map[string]*string
	for {
		rows := make([]parquet.Row, 64)
		n, err := reader.ReadRows(rows)
		for _, r := range rows[:n] {
			m := make(map[string]*string, len(fields))
			for i, field := range fields {
				v := r[i]
				if v.IsNull() {
					m[field.Name()] = nil
					continue
				}
				s := string(v.ByteArray())
				m[field.Name()] = &s
			}
			out = append(out, m)
		}
		if err != nil || n == 0 {
			break
		}
	}
	return out
}

func rowByCol(t *testing.T, rows []map[string]*string, col, want string) map[string]*string {
	t.Helper()
	for _, r := range rows {
		if v := r[col]; v != nil && *v == want {
			return r
		}
	}
	t.Fatalf("no row with %s=%q", col, want)
	return nil
}

func lenOrNull(s *string) int {
	if s == nil {
		return -1
	}
	return len(*s)
}

func strOrNull(s *string) string {
	if s == nil {
		return "<NULL>"
	}
	return *s
}
