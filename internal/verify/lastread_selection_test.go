package verify

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// The default check compares each table's last read of the database with the
// snapshot before it, not the two newest snapshots: a snapshot built from the
// recorded changes never read the database, so it is not an independent
// reference. The footers below are stamped the way the real writers stamp
// them: a dump like baseline.Run (producer, its own instant as the last read,
// zero folds), a fold like the reconstruct writer (producer, the read it
// inherits, one more fold).

var (
	lr0 = time.Date(2026, 9, 1, 3, 0, 0, 0, time.UTC)
	lr1 = time.Date(2026, 9, 2, 3, 0, 0, 0, time.UTC)
	lr2 = time.Date(2026, 9, 3, 3, 0, 0, 0, time.UTC)
	lr3 = time.Date(2026, 9, 4, 3, 0, 0, 0, time.UTC)
)

const lrCreateSQL = "CREATE TABLE `orders` (\n  `id` int NOT NULL,\n  PRIMARY KEY (`id`)\n);\n"

func lrCols(t *testing.T) []baseline.Column {
	t.Helper()
	cols, err := baseline.ParseSchemaText(lrCreateSQL)
	if err != nil {
		t.Fatal(err)
	}
	return cols
}

// lrDump is a dump's footer, as baseline.Run writes it.
func lrDump(at time.Time, pos int64) map[string]string {
	ts := at.UTC().Format(time.RFC3339)
	return map[string]string{
		baseline.MetaKeyCreateTableSQL:    lrCreateSQL,
		baseline.MetaKeySnapshotTimestamp: ts,
		baseline.MetaKeyMydumperFormat:    "sql",
		baseline.MetaKeySnapshotProducer:  baseline.ProducerDump,
		baseline.MetaKeyLastDumpAt:        ts,
		baseline.MetaKeyFoldGeneration:    "0",
		baseline.MetaKeyBinlogFile:        "binlog.000001",
		baseline.MetaKeyBinlogPos:         strconv.FormatInt(pos, 10),
	}
}

// lrFold is a fold's footer: written at `at`, folded from `from`, inheriting
// the read `read` (zero: a fold written before the read was recorded).
func lrFold(at, from, read time.Time, gen int, pos int64) map[string]string {
	md := map[string]string{
		baseline.MetaKeyCreateTableSQL:    lrCreateSQL,
		baseline.MetaKeySnapshotTimestamp: at.UTC().Format(time.RFC3339),
		baseline.MetaKeySnapshotProducer:  baseline.ProducerReconstruct,
		baseline.MetaKeyDerivedFrom:       from.UTC().Format(time.RFC3339),
		baseline.MetaKeyBinlogFile:        "binlog.000001",
		baseline.MetaKeyBinlogPos:         strconv.FormatInt(pos, 10),
	}
	if !read.IsZero() {
		md[baseline.MetaKeyLastDumpAt] = read.UTC().Format(time.RFC3339)
		md[baseline.MetaKeyFoldGeneration] = strconv.Itoa(gen)
	}
	return md
}

func lrPath(root string, at time.Time, table string) string {
	return filepath.Join(root, reconstruct.SnapshotDirName(at), "shop", table+".parquet")
}

func lrWrite(t *testing.T, root string, at time.Time, table string, md map[string]string) string {
	t.Helper()
	path := lrPath(root, at, table)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	w, err := baseline.NewWriter(path, lrCols(t), baseline.WriterConfig{Compression: "none", RowGroupSize: 100, Metadata: md})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRow([]string{"1"}, []bool{false}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := baseline.WriteSuccessMarker(filepath.Dir(filepath.Dir(path))); err != nil {
		t.Fatal(err)
	}
	return path
}

// lrStartChain writes the empty sequence-0 pair a full backup starts a chain
// with, stamped with the keys baseline's writeEmptyTableDelta stamps: the
// base's own instant and anchor, and none of the dump's provenance keys.
func lrStartChain(t *testing.T, base string, at time.Time, pos int64) {
	t.Helper()
	fi, err := os.Stat(base)
	if err != nil {
		t.Fatal(err)
	}
	ts, p := at.UTC().Format(time.RFC3339), strconv.FormatInt(pos, 10)
	if err := baseline.WriteTableDeltaPair(base, 0, lrCols(t), map[string]string{
		baseline.MetaKeySnapshotTimestamp: ts,
		baseline.MetaKeyBinlogFile:        "binlog.000001",
		baseline.MetaKeyBinlogPos:         p,
		baseline.MetaKeyCreateTableSQL:    lrCreateSQL,
		baseline.MetaKeyDeltaChainStart:   ts,
		baseline.MetaKeyDeltaBaseAnchor:   "binlog.000001:" + p,
		baseline.MetaKeyDeltaBaseSize:     strconv.FormatInt(fi.Size(), 10),
	}, nil, nil); err != nil {
		t.Fatal(err)
	}
}

// lrPair writes one table-delta pair beside base, stamped as a chain that
// started at chainStart.
func lrPair(t *testing.T, base string, seq int, chainStart time.Time, md map[string]string) {
	t.Helper()
	m := map[string]string{}
	for k, v := range md {
		m[k] = v
	}
	m[baseline.MetaKeyDeltaChainStart] = chainStart.UTC().Format(time.RFC3339)
	if err := baseline.WriteTableDeltaPair(base, seq, lrCols(t), m, nil, nil); err != nil {
		t.Fatal(err)
	}
}

// lrCarry hard-links a table (and the given pairs of its chain) into a later
// snapshot, the way a refresh carries an unchanged file.
func lrCarry(t *testing.T, root string, from, to time.Time, table string, seqs ...int) string {
	t.Helper()
	src, dst := lrPath(root, from, table), lrPath(root, to, table)
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(src, dst); err != nil {
		t.Fatal(err)
	}
	for _, s := range seqs {
		fp, fu := baseline.TableDeltaPaths(src, s)
		tp, tu := baseline.TableDeltaPaths(dst, s)
		if err := os.Link(fp, tp); err != nil {
			t.Fatal(err)
		}
		if err := os.Link(fu, tu); err != nil {
			t.Fatal(err)
		}
	}
	if err := baseline.WriteSuccessMarker(filepath.Dir(filepath.Dir(dst))); err != nil {
		t.Fatal(err)
	}
	return dst
}

// lrFind runs the selection and checks the invariant every caller relies on:
// one answer per table, never two.
func lrFind(t *testing.T, root string) ([]BaselinePair, []string) {
	t.Helper()
	pairs, prevOnly, err := FindBaselinePair(context.Background(), root)
	if err != nil {
		t.Fatalf("FindBaselinePair: %v", err)
	}
	seen := map[string]bool{}
	var gone []string
	for _, p := range pairs {
		k := p.Schema + "." + p.Table
		if seen[k] {
			t.Fatalf("%s answered twice", k)
		}
		seen[k] = true
	}
	for _, d := range prevOnly {
		k := d.Schema + "." + d.Table
		if seen[k] {
			t.Fatalf("%s answered twice (a pair and absent from the newest)", k)
		}
		seen[k] = true
		gone = append(gone, k)
	}
	return pairs, gone
}

func lrOne(t *testing.T, pairs []BaselinePair, table string) BaselinePair {
	t.Helper()
	for _, p := range pairs {
		if p.Table == table {
			return p
		}
	}
	t.Fatalf("no answer for %s in %+v", table, pairs)
	return BaselinePair{}
}

// wantCompared: the table is compared, the newer side being the read at n
// and the older side the snapshot at prev.
func wantCompared(t *testing.T, p BaselinePair, prev, n time.Time, newPos uint64) {
	t.Helper()
	if p.Settled != nil {
		t.Fatalf("%s: settled as %s (%q), want compared %s -> %s", p.Table, p.Settled.Status, p.Settled.Detail, prev, n)
	}
	if !p.NewSnapshot.Equal(n) || !p.PrevSnapshot.Equal(prev) || p.NewAnchor.Pos != newPos {
		t.Fatalf("%s: compared %s -> %s at pos %d, want %s -> %s at pos %d",
			p.Table, p.PrevSnapshot, p.NewSnapshot, p.NewAnchor.Pos, prev, n, newPos)
	}
	if !p.NewReadFromDatabase {
		t.Fatalf("%s: the newer side is not marked as a read of the database", p.Table)
	}
	if !strings.HasSuffix(p.NewPath, filepath.Join(reconstruct.SnapshotDirName(n), "shop", p.Table+".parquet")) {
		t.Fatalf("%s: newer side %s is not the file the read wrote", p.Table, p.NewPath)
	}
}

func wantSettled(t *testing.T, p BaselinePair, status Status, says ...string) {
	t.Helper()
	if p.Settled == nil {
		t.Fatalf("%s: compared %s -> %s, want settled %s", p.Table, p.PrevSnapshot, p.NewSnapshot, status)
	}
	if p.Settled.Status != status || p.Settled.Schema != p.Schema || p.Settled.Table != p.Table {
		t.Fatalf("%s: settled %+v, want status %s for this table", p.Table, *p.Settled, status)
	}
	for _, s := range says {
		if !strings.Contains(p.Settled.Detail, s) {
			t.Fatalf("%s: detail %q does not say %q", p.Table, p.Settled.Detail, s)
		}
	}
}

// 1. The newest snapshot is a read: the pair is today's, the two newest.
func TestLastRead_newestIsARead(t *testing.T) {
	root := t.TempDir()
	lrWrite(t, root, lr0, "orders", lrDump(lr0, 100))
	lrWrite(t, root, lr1, "orders", lrDump(lr1, 200))
	pairs, _ := lrFind(t, root)
	wantCompared(t, lrOne(t, pairs, "orders"), lr0, lr1, 200)
}

// 2. Newest built from changes (the table rewritten): the read before it and
// the snapshot before that, never the fold.
func TestLastRead_newestIsAFold(t *testing.T) {
	root := t.TempDir()
	lrWrite(t, root, lr0, "orders", lrDump(lr0, 100))
	lrWrite(t, root, lr1, "orders", lrDump(lr1, 200))
	lrWrite(t, root, lr2, "orders", lrFold(lr2, lr1, lr1, 1, 300))
	lrWrite(t, root, lr3, "orders", lrFold(lr3, lr2, lr1, 2, 400))
	pairs, _ := lrFind(t, root)
	wantCompared(t, lrOne(t, pairs, "orders"), lr0, lr1, 200)
}

// 3. Newest built from changes kept beside the table: the base is the read's
// own file by hard link, the chain beside it. The read is where it was written.
func TestLastRead_deltaChainPointsAtItsRead(t *testing.T) {
	root := t.TempDir()
	lrWrite(t, root, lr0, "orders", lrDump(lr0, 100))
	base := lrWrite(t, root, lr1, "orders", lrDump(lr1, 200))
	lrStartChain(t, base, lr1, 200) // the empty pair a full backup starts a chain with
	carried := lrCarry(t, root, lr1, lr2, "orders", 0)
	lrPair(t, carried, 1, lr1, lrFold(lr2, lr1, lr1, 1, 300))
	pairs, _ := lrFind(t, root)
	wantCompared(t, lrOne(t, pairs, "orders"), lr0, lr1, 200)
}

// 4. A table reused unchanged: the later snapshot holds the read's own bytes.
// Taking it as the read would compare it with the snapshot before it, which
// holds the same bytes too: a file against itself.
func TestLastRead_carriedFileIsNotARead(t *testing.T) {
	root := t.TempDir()
	lrWrite(t, root, lr0, "orders", lrDump(lr0, 100))
	lrWrite(t, root, lr1, "orders", lrDump(lr1, 200))
	lrCarry(t, root, lr1, lr2, "orders")
	lrCarry(t, root, lr1, lr3, "orders")
	pairs, _ := lrFind(t, root)
	wantCompared(t, lrOne(t, pairs, "orders"), lr0, lr1, 200)
}

// 5. The only read is the oldest snapshot: nothing before it to compare with.
func TestLastRead_readIsTheOldest(t *testing.T) {
	root := t.TempDir()
	lrWrite(t, root, lr0, "orders", lrDump(lr0, 100))
	lrWrite(t, root, lr1, "orders", lrFold(lr1, lr0, lr0, 1, 200))
	lrWrite(t, root, lr2, "orders", lrFold(lr2, lr1, lr0, 2, 300))
	pairs, _ := lrFind(t, root)
	wantSettled(t, lrOne(t, pairs, "orders"), StatusInconclusive, lr0.Format(time.RFC3339), "no earlier snapshot")
}

// 6. The snapshot that read the database is no longer kept.
func TestLastRead_readNoLongerKept(t *testing.T) {
	root := t.TempDir()
	lrWrite(t, root, lr1, "orders", lrFold(lr1, lr0, lr0, 1, 200))
	lrWrite(t, root, lr2, "orders", lrFold(lr2, lr1, lr0, 2, 300))
	pairs, _ := lrFind(t, root)
	wantSettled(t, lrOne(t, pairs, "orders"), StatusInconclusive, lr0.Format(time.RFC3339), "no longer kept", "full backup")
}

// 7. A fold written before the read was recorded, table rewritten: when the
// database was last read is not on record. No guess, no walk.
func TestLastRead_readNotOnRecord(t *testing.T) {
	root := t.TempDir()
	lrWrite(t, root, lr0, "orders", lrDump(lr0, 100))
	lrWrite(t, root, lr1, "orders", lrFold(lr1, lr0, time.Time{}, 0, 200))
	lrWrite(t, root, lr2, "orders", lrFold(lr2, lr1, time.Time{}, 0, 300))
	pairs, _ := lrFind(t, root)
	wantSettled(t, lrOne(t, pairs, "orders"), StatusInconclusive, "does not record when the database was last read", "full backup")
}

// 8. A read of one table only: each table finds its own read; a table the
// newest snapshot does not hold is reported once, as absent.
func TestLastRead_perTableReads(t *testing.T) {
	root := t.TempDir()
	lrWrite(t, root, lr0, "orders", lrDump(lr0, 100))
	lrWrite(t, root, lr0, "users", lrDump(lr0, 100))
	lrWrite(t, root, lr1, "orders", lrFold(lr1, lr0, lr0, 1, 200))
	lrWrite(t, root, lr1, "users", lrDump(lr1, 200)) // users read again, alone
	lrWrite(t, root, lr1, "gone", lrDump(lr0, 100))
	lrWrite(t, root, lr2, "orders", lrFold(lr2, lr1, lr0, 2, 300))
	lrWrite(t, root, lr2, "users", lrFold(lr2, lr1, lr1, 1, 300))
	pairs, gone := lrFind(t, root)
	wantSettled(t, lrOne(t, pairs, "orders"), StatusInconclusive, "no earlier snapshot")
	wantCompared(t, lrOne(t, pairs, "users"), lr0, lr1, 200)
	if len(pairs) != 2 || len(gone) != 1 || gone[0] != "shop.gone" {
		t.Fatalf("pairs=%d gone=%v; want orders and users answered, gone reported once as absent", len(pairs), gone)
	}
}

// 9. One snapshot: nothing to verify yet (the caller's no-predecessor note).
func TestLastRead_oneSnapshot(t *testing.T) {
	root := t.TempDir()
	lrWrite(t, root, lr0, "orders", lrDump(lr0, 100))
	pairs, gone := lrFind(t, root)
	if len(pairs) != 0 || len(gone) != 0 {
		t.Fatalf("pairs=%+v gone=%v, want nothing", pairs, gone)
	}
}

// 10. A footer this build cannot read on the read's own snapshot: that table
// is an error naming the file, and the others are still checked.
func TestLastRead_unreadableFooterIsThatTablesError(t *testing.T) {
	root := t.TempDir()
	lrWrite(t, root, lr0, "orders", lrDump(lr0, 100))
	lrWrite(t, root, lr0, "users", lrDump(lr0, 100))
	lrWrite(t, root, lr1, "users", lrDump(lr1, 200))
	lrWrite(t, root, lr2, "orders", lrFold(lr2, lr1, lr1, 1, 300))
	lrWrite(t, root, lr2, "users", lrFold(lr2, lr1, lr1, 1, 300))
	broken := lrPath(root, lr1, "orders")
	if err := os.WriteFile(broken, []byte("not parquet"), 0o644); err != nil {
		t.Fatal(err)
	}
	pairs, _ := lrFind(t, root)
	wantSettled(t, lrOne(t, pairs, "orders"), StatusError, broken)
	wantCompared(t, lrOne(t, pairs, "users"), lr0, lr1, 200)
}

// 11. Two reads with the same position (nothing was written between them): a
// real comparison, not "nothing was checked". That branch is for a newer side
// that keeps the older side's file, which a read never does.
func TestLastRead_sameAnchorReadsAreCompared(t *testing.T) {
	root := t.TempDir()
	lrWrite(t, root, lr0, "orders", lrDump(lr0, 100))
	base := lrWrite(t, root, lr1, "orders", lrDump(lr1, 100))
	lrStartChain(t, base, lr1, 100)
	pairs, _ := lrFind(t, root)
	p := lrOne(t, pairs, "orders")
	wantCompared(t, p, lr0, lr1, 100)
	if p.PrevAnchor != p.NewAnchor {
		t.Fatalf("fixture: want equal anchors, got %+v", p)
	}
	p.NewHasDelta = true // the empty pair the read starts its chain with
	if pairComparesNothing(p) {
		t.Fatal("two reads at the same position were treated as one file compared with itself")
	}
	// The case the branch exists for still takes it: a newer side that is
	// not a read, holding the older side's file.
	p.NewReadFromDatabase = false
	if !pairComparesNothing(p) {
		t.Fatal("a newer side holding the older side's file is no longer reported as nothing checked")
	}
}

// 12. PostgreSQL: the same rule over LSN anchors.
func TestLastRead_postgres(t *testing.T) {
	root := t.TempDir()
	pg := func(md map[string]string, lsn string) map[string]string {
		delete(md, baseline.MetaKeyBinlogFile)
		delete(md, baseline.MetaKeyBinlogPos)
		delete(md, baseline.MetaKeyMydumperFormat)
		md[baseline.MetaKeyLSN] = lsn
		return md
	}
	lrWrite(t, root, lr0, "orders", pg(lrDump(lr0, 0), "4096"))
	lrWrite(t, root, lr1, "orders", pg(lrDump(lr1, 0), "8192"))
	lrWrite(t, root, lr2, "orders", pg(lrFold(lr2, lr1, lr1, 1, 0), "12288"))
	pairs, _ := lrFind(t, root)
	p := lrOne(t, pairs, "orders")
	if p.Settled != nil || !p.NewSnapshot.Equal(lr1) || !p.PrevSnapshot.Equal(lr0) || p.NewLSN != 8192 {
		t.Fatalf("pg pair %+v (settled %v), want lr0 -> lr1 at LSN 8192", p, p.Settled)
	}
}

// 13. The folder of the read the newest copy names cannot be read: the read
// looks "no longer kept", and the run refuses (#1639) instead of saying so,
// even though that folder is older than the second newest.
func TestLastRead_unreadableReadFolderRefuses(t *testing.T) {
	root := t.TempDir()
	lrWrite(t, root, lr0, "orders", lrDump(lr0, 100))
	lrWrite(t, root, lr1, "orders", lrDump(lr1, 200))
	lrWrite(t, root, lr2, "orders", lrFold(lr2, lr1, lr1, 1, 300))
	lrWrite(t, root, lr3, "orders", lrFold(lr3, lr2, lr1, 2, 400))
	unreadable1639(t, filepath.Join(root, reconstruct.SnapshotDirName(lr1)))
	if _, _, err := FindBaselinePair(context.Background(), root); !errors.Is(err, reconstruct.ErrUnreadableSnapshot) {
		t.Fatalf("err = %v, want a refusal: the read this check needs sits in the unreadable folder", err)
	}
}

// 13b. An unreadable folder between the older side and the read, with the
// newest snapshot a fold: the snapshot before the read may be in it, so the
// pair the listing would make (skipping it) is refused, not compared.
func TestLastRead_unreadableInsideTheComparedSpanRefuses(t *testing.T) {
	root := t.TempDir()
	mid := lr0.Add(12 * time.Hour)
	lrWrite(t, root, lr0, "orders", lrDump(lr0, 100))
	lrWrite(t, root, mid, "orders", lrDump(mid, 150))
	lrWrite(t, root, lr1, "orders", lrDump(lr1, 200))
	lrWrite(t, root, lr2, "orders", lrFold(lr2, lr1, lr1, 1, 300))
	lrWrite(t, root, lr3, "orders", lrFold(lr3, lr2, lr1, 2, 400))
	unreadable1639(t, filepath.Join(root, reconstruct.SnapshotDirName(mid)))
	if _, _, err := FindBaselinePair(context.Background(), root); !errors.Is(err, reconstruct.ErrUnreadableSnapshot) {
		t.Fatalf("err = %v, want a refusal: the snapshot before the read may be in the unreadable folder", err)
	}
}

// 14. An unreadable folder older than every snapshot a comparison uses
// changes nothing.
func TestLastRead_unreadableOlderThanTheComparedSpan(t *testing.T) {
	root := t.TempDir()
	old := time.Date(2026, 8, 1, 3, 0, 0, 0, time.UTC)
	lrWrite(t, root, old, "orders", lrDump(old, 50))
	lrWrite(t, root, lr0, "orders", lrDump(lr0, 100))
	lrWrite(t, root, lr1, "orders", lrDump(lr1, 200))
	lrWrite(t, root, lr2, "orders", lrFold(lr2, lr1, lr1, 1, 300))
	unreadable1639(t, filepath.Join(root, reconstruct.SnapshotDirName(old)))
	pairs, _ := lrFind(t, root)
	wantCompared(t, lrOne(t, pairs, "orders"), lr0, lr1, 200)
}

// 15. The snapshot before the read keeps its table as a chain that started
// earlier: the comparison starts where the chain did, from the chain's base
// and its anchor (the older read), so every recorded change up to the new
// read is replayed.
func TestLastRead_previousIsAChain(t *testing.T) {
	root := t.TempDir()
	base := lrWrite(t, root, lr0, "orders", lrDump(lr0, 100))
	lrStartChain(t, base, lr0, 100)
	carried := lrCarry(t, root, lr0, lr1, "orders", 0)
	lrPair(t, carried, 1, lr0, lrFold(lr1, lr0, lr0, 1, 200))
	lrWrite(t, root, lr2, "orders", lrDump(lr2, 300))
	pairs, _ := lrFind(t, root)
	p := lrOne(t, pairs, "orders")
	wantCompared(t, p, lr0, lr2, 300)
	if p.PrevAnchor.Pos != 100 || !strings.HasSuffix(p.PrevPath, filepath.Join(reconstruct.SnapshotDirName(lr1), "shop", "orders.parquet")) {
		t.Fatalf("older side %s at pos %d, want the chain snapshot's base at the older read's anchor (100)", p.PrevPath, p.PrevAnchor.Pos)
	}
}

// The report says which read each compared table was checked against.
func TestLastRead_reportNamesTheRead(t *testing.T) {
	rep := NewReport(ModeBaselinePair, []TableResult{
		{Schema: "shop", Table: "orders", Status: StatusMatch, ComparedTo: lr1},
		{Schema: "shop", Table: "users", Status: StatusInconclusive, Detail: "no earlier snapshot"},
	})
	got := map[string]string{}
	for _, tr := range rep.Tables {
		got[tr.Table] = tr.ComparedTo
	}
	if got["orders"] != "2026-09-02T03:00:00Z" || got["users"] != "" {
		t.Fatalf("compared_to = %v, want orders at its read and nothing for a table not compared", got)
	}
}

// A pair the selection already answered is returned as is: nothing is
// resolved or fetched (the zero config here has no resolver to call).
func TestLastRead_settledPairIsReturnedAsIs(t *testing.T) {
	want := TableResult{Schema: "shop", Table: "orders", Status: StatusInconclusive, Detail: "no earlier snapshot"}
	got, err := VerifyBaselinePair(context.Background(), BaselineConfig{}, BaselinePair{Schema: "shop", Table: "orders", Settled: &want})
	if err != nil || got != want {
		t.Fatalf("got %+v err=%v, want the settled answer %+v", got, err, want)
	}
}

// "No earlier snapshot" is a claim about every older folder: an unreadable one
// may hold the earlier snapshot, so the run refuses and names it (#1639)
// instead of sending the operator to take a full backup.
func TestLastRead_readIsTheOldestReadableRefuses(t *testing.T) {
	root := t.TempDir()
	old := time.Date(2026, 8, 1, 3, 0, 0, 0, time.UTC)
	lrWrite(t, root, old, "orders", lrDump(old, 50))
	lrWrite(t, root, lr0, "orders", lrDump(lr0, 100))
	lrWrite(t, root, lr1, "orders", lrFold(lr1, lr0, lr0, 1, 200))
	lrWrite(t, root, lr2, "orders", lrFold(lr2, lr1, lr0, 2, 300))
	unreadable1639(t, filepath.Join(root, reconstruct.SnapshotDirName(old)))
	if _, _, err := FindBaselinePair(context.Background(), root); !errors.Is(err, reconstruct.ErrUnreadableSnapshot) {
		t.Fatalf("err = %v, want a refusal: the snapshot before the read may be in the unreadable folder", err)
	}
}

// A table the snapshot before the newest does not hold (that one was a subset)
// is compared with the newest snapshot before the read that does hold it.
func TestLastRead_previousSkipsASubsetSnapshot(t *testing.T) {
	root := t.TempDir()
	lrWrite(t, root, lr0, "orders", lrDump(lr0, 100))
	lrWrite(t, root, lr0, "users", lrDump(lr0, 100))
	lrWrite(t, root, lr1, "users", lrDump(lr1, 200)) // a subset: orders not in it
	lrWrite(t, root, lr2, "orders", lrDump(lr2, 300))
	lrWrite(t, root, lr2, "users", lrDump(lr2, 300))
	pairs, _ := lrFind(t, root)
	wantCompared(t, lrOne(t, pairs, "orders"), lr0, lr2, 300)
	wantCompared(t, lrOne(t, pairs, "users"), lr1, lr2, 300)
}

// The newest copy names a read whose snapshot holds a copy built from the
// recorded changes: the records contradict each other, an error, not a gap.
func TestLastRead_contradictoryRecordsAreAnError(t *testing.T) {
	root := t.TempDir()
	lrWrite(t, root, lr0, "orders", lrDump(lr0, 100))
	lrWrite(t, root, lr1, "orders", lrFold(lr1, lr0, lr0, 1, 200))
	lrWrite(t, root, lr2, "orders", lrFold(lr2, lr1, lr1, 1, 300)) // claims a read at lr1
	pairs, _ := lrFind(t, root)
	wantSettled(t, lrOne(t, pairs, "orders"), StatusError, lr1.Format(time.RFC3339), lrPath(root, lr1, "orders"))
}

// A table that stops at a gate before any fingerprint (here: no primary key)
// names no read it was compared against.
func TestLastRead_uncomparedTableNamesNoRead(t *testing.T) {
	res := metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{
		"shop.orders": {Schema: "shop", Table: "orders", Columns: []metadata.ColumnMeta{{Name: "id", DataType: "int"}}},
	})
	got, err := VerifyBaselinePair(context.Background(), BaselineConfig{Resolver: res}, BaselinePair{
		Schema: "shop", Table: "orders", NewSnapshot: lr1, PrevSnapshot: lr0, NewReadFromDatabase: true,
	})
	if err != nil || got.Status != StatusInconclusive || !got.ComparedTo.IsZero() {
		t.Fatalf("got %+v err=%v, want inconclusive with no compared_to", got, err)
	}
}

// Two tables whose "schema.table" spellings coincide (a dot inside a name)
// stay two tables: each gets its own answer from its own files.
func TestLastRead_dottedNamesStayApart(t *testing.T) {
	root := t.TempDir()
	write := func(at time.Time, schema, table string, pos int64) {
		path := filepath.Join(root, reconstruct.SnapshotDirName(at), schema, table+".parquet")
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		w, err := baseline.NewWriter(path, lrCols(t), baseline.WriterConfig{Compression: "none", RowGroupSize: 100, Metadata: lrDump(at, pos)})
		if err != nil {
			t.Fatal(err)
		}
		if err := w.WriteRow([]string{"1"}, []bool{false}); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	}
	write(lr0, "a.b", "c", 100)
	write(lr1, "a.b", "c", 200)
	write(lr1, "a", "b.c", 200) // new since lr0: no earlier snapshot
	pairs, _, err := FindBaselinePair(context.Background(), root)
	if err != nil || len(pairs) != 2 {
		t.Fatalf("pairs=%+v err=%v, want one answer for each table", pairs, err)
	}
	for _, p := range pairs {
		switch p.Schema + "|" + p.Table {
		case "a.b|c":
			if p.Settled != nil || !p.PrevSnapshot.Equal(lr0) {
				t.Errorf("a.b/c = %+v, want compared with its own lr0 file", p)
			}
		case "a|b.c":
			if p.Settled == nil || !strings.Contains(p.Settled.Detail, "no earlier snapshot") {
				t.Errorf("a/b.c = %+v, want no earlier snapshot (it has none of its own)", p)
			}
		default:
			t.Errorf("unexpected answer %+v", p)
		}
	}
}

// Snapshots written before this build knew the read's keys: a dump before
// v0.86.0 has no last_dump_at, one before #1569 no producer either (only the
// mydumper format, or for PostgreSQL the WAL floor, dates it as a dump). They
// are reads all the same, so an upgrade must not turn every table on disk into
// "not on record". Each vintage: two dumps, the first fold after the upgrade
// (which stamps the read it derives from the dump), and a later snapshot that
// reuses the old dump's file unchanged.
func TestLastRead_olderDumpsAreStillReads(t *testing.T) {
	strip := func(md map[string]string, keys ...string) map[string]string {
		for _, k := range keys {
			delete(md, k)
		}
		return md
	}
	pg := func(md map[string]string, lsn int64) map[string]string {
		md = strip(md, baseline.MetaKeyMydumperFormat, baseline.MetaKeyBinlogFile, baseline.MetaKeyBinlogPos)
		md[baseline.MetaKeyLSN] = strconv.FormatInt(lsn, 10)
		return md
	}
	noRead := func(at time.Time, pos int64) map[string]string {
		return strip(lrDump(at, pos), baseline.MetaKeyLastDumpAt, baseline.MetaKeyFoldGeneration)
	}
	noProducer := func(at time.Time, pos int64) map[string]string {
		return strip(noRead(at, pos), baseline.MetaKeySnapshotProducer)
	}
	vintages := []struct {
		name string
		dump func(time.Time, int64) map[string]string
		fold func(at, from, read time.Time, gen int, pos int64) map[string]string
	}{
		{"before v0.86.0", noRead, lrFold},
		{"before #1569", noProducer, lrFold},
		{"postgres before #1569",
			func(at time.Time, pos int64) map[string]string { return pg(noProducer(at, pos), pos) },
			func(at, from, read time.Time, gen int, pos int64) map[string]string {
				return pg(lrFold(at, from, read, gen, pos), pos)
			}},
	}
	for _, v := range vintages {
		check := func(t *testing.T, pairs []BaselinePair) {
			t.Helper()
			p := lrOne(t, pairs, "orders")
			if p.Settled != nil || !p.NewSnapshot.Equal(lr1) || !p.PrevSnapshot.Equal(lr0) || !p.NewReadFromDatabase {
				t.Fatalf("got %+v (settled %+v), want the older dump at %s compared with the read at %s", p, p.Settled, lr0, lr1)
			}
		}
		t.Run(v.name+"/two dumps", func(t *testing.T) {
			root := t.TempDir()
			lrWrite(t, root, lr0, "orders", v.dump(lr0, 100))
			lrWrite(t, root, lr1, "orders", v.dump(lr1, 200))
			pairs, _ := lrFind(t, root)
			check(t, pairs)
		})
		t.Run(v.name+"/first fold after the upgrade", func(t *testing.T) {
			root := t.TempDir()
			lrWrite(t, root, lr0, "orders", v.dump(lr0, 100))
			lrWrite(t, root, lr1, "orders", v.dump(lr1, 200))
			lrWrite(t, root, lr2, "orders", v.fold(lr2, lr1, lr1, 1, 300))
			pairs, _ := lrFind(t, root)
			check(t, pairs)
		})
		t.Run(v.name+"/old dump reused unchanged", func(t *testing.T) {
			root := t.TempDir()
			lrWrite(t, root, lr0, "orders", v.dump(lr0, 100))
			lrWrite(t, root, lr1, "orders", v.dump(lr1, 200))
			lrCarry(t, root, lr1, lr2, "orders")
			pairs, _ := lrFind(t, root)
			check(t, pairs)
		})
	}
}

// A newest footer that will not open is that table's error, naming the file:
// not "not on record", which would send the operator to take a backup while a
// corrupt file sits there, and would let the run pass on the other tables.
func TestLastRead_newestFooterUnreadable(t *testing.T) {
	root := t.TempDir()
	lrWrite(t, root, lr0, "orders", lrDump(lr0, 100))
	lrWrite(t, root, lr1, "orders", lrDump(lr1, 200))
	broken := lrWrite(t, root, lr2, "orders", lrFold(lr2, lr1, lr1, 1, 300))
	if err := os.WriteFile(broken, []byte("not parquet"), 0o644); err != nil {
		t.Fatal(err)
	}
	pairs, _ := lrFind(t, root)
	wantSettled(t, lrOne(t, pairs, "orders"), StatusError, broken)
}

// The older side's footer will not open: an error, not a comparison that
// falls back to a bound nobody read.
func TestLastRead_previousFooterUnreadable(t *testing.T) {
	root := t.TempDir()
	broken := lrWrite(t, root, lr0, "orders", lrDump(lr0, 100))
	lrWrite(t, root, lr1, "orders", lrDump(lr1, 200))
	lrWrite(t, root, lr2, "orders", lrFold(lr2, lr1, lr1, 1, 300))
	if err := os.WriteFile(broken, []byte("not parquet"), 0o644); err != nil {
		t.Fatal(err)
	}
	pairs, _ := lrFind(t, root)
	wantSettled(t, lrOne(t, pairs, "orders"), StatusError, broken)
}

// The chain beside the older side will not open: an error, not a comparison
// that starts at the older side's folder instead of where its chain did.
func TestLastRead_previousChainUnreadable(t *testing.T) {
	root := t.TempDir()
	base := lrWrite(t, root, lr0, "orders", lrDump(lr0, 100))
	lrStartChain(t, base, lr0, 100)
	_, upserts := baseline.TableDeltaPaths(base, 0)
	if err := os.WriteFile(upserts, []byte("not parquet"), 0o644); err != nil {
		t.Fatal(err)
	}
	lrWrite(t, root, lr1, "orders", lrDump(lr1, 200))
	pairs, _ := lrFind(t, root)
	wantSettled(t, lrOne(t, pairs, "orders"), StatusError, "the chain beside", base)
}

// A producer this build does not know (a newer version's) is not a read:
// the table is not checked, and says why, rather than trusting an operation
// nobody here knows the name of. Not an error either: nothing contradicts.
func TestLastRead_unknownMakerIsNotARead(t *testing.T) {
	root := t.TempDir()
	lrWrite(t, root, lr0, "orders", lrDump(lr0, 100))
	lrWrite(t, root, lr1, "orders", lrDump(lr1, 200))
	md := lrDump(lr2, 300)
	md[baseline.MetaKeySnapshotProducer] = "made-by-a-newer-version"
	lrWrite(t, root, lr2, "orders", md)
	pairs, _ := lrFind(t, root)
	wantSettled(t, lrOne(t, pairs, "orders"), StatusInconclusive, "does not say how its copy was made")
}

// The newest copy names a read whose snapshot holds a file carried from an
// earlier one: no writer produces this (a carried file keeps its ancestor's
// read), so the records contradict each other and it is an error.
func TestLastRead_carriedWhereTheReadShouldBeIsAnError(t *testing.T) {
	root := t.TempDir()
	lrWrite(t, root, lr0, "orders", lrDump(lr0, 100))
	lrCarry(t, root, lr0, lr1, "orders")
	lrWrite(t, root, lr2, "orders", lrFold(lr2, lr1, lr1, 1, 300)) // claims a read at lr1
	pairs, _ := lrFind(t, root)
	wantSettled(t, lrOne(t, pairs, "orders"), StatusError, lr1.Format(time.RFC3339), lrPath(root, lr1, "orders"))
}
