package reconstruct

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/baselineintegrity"
	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	"github.com/dbtrail/dbtrail/internal/query"
)

// #1735: the MAJOR compaction. A daemon job folds a table's whole chain into
// a new table file and stages it; the next refresh puts it in place of the
// base, starts the chain over at the folded pair, and repositions any pairs
// written after the job read the chain. Every test here compares the state a
// reader sees against the full rewrite, which is the reference every restore
// has been built on.

// stageMajor runs the job for the chain beside base and marks the result
// complete, as the daemon does.
func stageMajor(t *testing.T, compactDir, base string) (*MajorCompaction, string) {
	t.Helper()
	bmeta, err := baseline.ReadParquetMetadata(base)
	if err != nil {
		t.Fatal(err)
	}
	d, err := readTableDelta(context.Background(), base, bmeta)
	if err != nil || d == nil {
		t.Fatalf("no chain beside %s: %v", base, err)
	}
	dir := CompactionDir(compactDir, "mydb", "orders", d.Meta.DeltaChainStart)
	mc, err := CompactTableDeltaMajor(context.Background(), base, dir, nil, duckdbutil.Tuning{})
	if err != nil {
		t.Fatalf("major compaction: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, baseline.SuccessMarker), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	return mc, dir
}

// rawFooter reads every bintrail.* footer key of a Parquet file.
func rawFooter(t *testing.T, path string) map[string]string {
	t.Helper()
	ddb, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer ddb.Close()
	md, err := readBintrailFooter(context.Background(), ddb, path)
	if err != nil {
		t.Fatal(err)
	}
	return md
}

// rewriteFile rewrites a Parquet file through DuckDB with its footer mutated
// and, when where is set, only the rows it keeps: a tampered result.
func rewriteFile(t *testing.T, path, where string, mutate func(map[string]string)) {
	t.Helper()
	md := rawFooter(t, path)
	if mutate != nil {
		mutate(md)
	}
	ddb, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer ddb.Close()
	q := fmt.Sprintf("SELECT * FROM read_parquet('%s')", path)
	if where != "" {
		q += " WHERE " + where
	}
	tmp := path + ".rewrite"
	if _, err := ddb.Exec(fmt.Sprintf("COPY (%s) TO '%s' (FORMAT PARQUET, KV_METADATA {%s})", q, tmp, kvLiteral(md))); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, path); err != nil {
		t.Fatal(err)
	}
}

// followState reads a table the way a view that follows the newest snapshot
// does (#1918): through the widened globs.
func followState(t *testing.T, base string) []map[string]any {
	t.Helper()
	pg, ug := baseline.TableDeltaFollowGlobs(base)
	lit := func(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }
	ddb, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer ddb.Close()
	out := filepath.Join(t.TempDir(), "follow.parquet")
	q := fmt.Sprintf("COPY (%s) TO %s (FORMAT PARQUET)", baseline.TableDeltaFollowStateSQL(lit(base), lit(pg), lit(ug), base, ""), lit(out))
	if _, err := ddb.Exec(q); err != nil {
		t.Fatalf("follow state: %v", err)
	}
	return readSnapshotRows(t, out)
}

// positionsOf are the base rows holding the given ids, ascending.
func positionsOf(t *testing.T, base string, ids ...int) []int64 {
	t.Helper()
	ddb, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer ddb.Close()
	in := make([]string, len(ids))
	for i, id := range ids {
		in[i] = strconv.Itoa(id)
	}
	rows, err := ddb.Query(fmt.Sprintf("SELECT file_row_number FROM parquet_scan('%s', file_row_number=true) WHERE id IN (%s) ORDER BY 1", base, strings.Join(in, ",")))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := []int64{}
	for rows.Next() {
		var p int64
		if err := rows.Scan(&p); err != nil {
			t.Fatal(err)
		}
		out = append(out, p)
	}
	return out
}

func chainNames(base string) []string {
	out, _ := filepath.Glob(strings.TrimSuffix(base, ".parquet") + ".*")
	for i := range out {
		out[i] = filepath.Base(out[i])
	}
	sort.Strings(out)
	return out
}

// majorRun is a chain of three windows beside a base, with a major result
// staged for it, and the helpers to publish more windows and check them.
type majorRun struct {
	t                *testing.T
	root, compactDir string
	base, refOut     string
	at, prevTime     time.Time
	stage            string
	mc               *MajorCompaction
	i                int
	job              bool
	floor            time.Time
	capGap           *CaptureGap
	lastRep          *TableReport
}

func newMajorRun(t *testing.T, windows []map[string]*query.ResultRow) *majorRun {
	t.Helper()
	noCompaction(t)
	r := &majorRun{t: t, root: t.TempDir(), job: true}
	r.compactDir = filepath.Join(r.root, ".compact")
	r.base, r.refOut, r.at = plainChain(t, r.root, windows)
	r.prevTime = r.at
	r.i = 100
	return r
}

func (r *majorRun) stageIt() {
	r.t.Helper()
	r.mc, r.stage = stageMajor(r.t, r.compactDir, r.base)
}

// next publishes one more window, with the job's config when r.job, and
// checks the state against the reference.
func (r *majorRun) next(w map[string]*query.ResultRow) *TableReport {
	t := r.t
	t.Helper()
	r.i++
	r.at = r.at.Add(5 * time.Minute)
	cut := &query.BinlogPos{File: "binlog.000009", Pos: uint64(1000 * r.i)}
	_, r.refOut, _ = emitSnapshot(t, r.refOut, cloneChanges(w), cut, r.at)
	mutate := func(p *tableDeltaPublish) {
		if r.job {
			p.cfg.CompactDir, p.cfg.CompactionJob = r.compactDir, true
		}
		p.cfg.ChainStartFloor = r.floor
		p.capGap = r.capGap
	}
	nb, rep, err := deltaWindow(t, r.root, r.base, r.prevTime, cloneChanges(w), r.at, cut, mutate)
	if err != nil {
		t.Fatalf("window %d: %v", r.i, err)
	}
	r.prevTime, r.base, r.lastRep = r.at, nb, rep
	want := byID(readSnapshotRows(t, r.refOut))
	if got := byID(deltaState(t, nb)); !reflect.DeepEqual(got, want) {
		t.Fatalf("window %d: state differs from the full rewrite:\n got %v\nwant %v", r.i, got, want)
	}
	return rep
}

func TestCompactTableDeltaMajor_foldsTheChainIntoOneTableFile(t *testing.T) {
	noCompaction(t)
	root := t.TempDir()
	base, refOut, at := plainChain(t, root, zooWindows())
	baseInfo, _ := os.Stat(base)
	chain, _ := baseline.ListTableDelta(context.Background(), base)
	lastFooter := rawFooter(t, chain.Last().Upserts)

	out := filepath.Join(t.TempDir(), "stage")
	mc, err := CompactTableDeltaMajor(context.Background(), base, out, nil, duckdbutil.Tuning{})
	if err != nil {
		t.Fatal(err)
	}
	if mc.Seq != 2 || filepath.Base(mc.Base) != "orders.parquet" || !mc.NewChainStart.Equal(at) {
		t.Fatalf("compaction = %+v, want pair 2 folded and a chain that starts at %s", mc, at)
	}
	want := byID(readSnapshotRows(t, refOut))
	if got := byID(readSnapshotRows(t, mc.Base)); !reflect.DeepEqual(got, want) {
		t.Fatalf("the folded table differs from the full rewrite:\n got %v\nwant %v", got, want)
	}
	if mc.Rows != int64(len(want)) {
		t.Fatalf("rows = %d, want %d", mc.Rows, len(want))
	}
	// Its footer is the folded pair's, with the chain keys replaced by the
	// record of what was folded. Nothing else of it lost, byte for byte.
	got := rawFooter(t, mc.Base)
	for k, v := range lastFooter {
		if strings.HasPrefix(k, "bintrail.delta_") {
			if _, ok := got[k]; ok {
				t.Fatalf("the folded table carries the chain key %s", k)
			}
			continue
		}
		if got[k] != v {
			t.Fatalf("footer %s = %q, want the folded pair's %q", k, got[k], v)
		}
	}
	bm, _ := baseline.ReadParquetMetadata(base)
	if got[baseline.MetaKeyFoldedChainStart] != lastFooter[baseline.MetaKeyDeltaChainStart] ||
		got[baseline.MetaKeyFoldedBaseAnchor] != baseAnchorString(bm) ||
		got[baseline.MetaKeyFoldedBaseSize] != strconv.FormatInt(baseInfo.Size(), 10) ||
		got[baseline.MetaKeyFoldedSeq] != "2" {
		t.Fatalf("folded keys = %v", got)
	}
	// The source is untouched.
	if fi, _ := os.Stat(base); fi.Size() != baseInfo.Size() {
		t.Fatal("the base was modified")
	}
	if left, _ := filepath.Glob(filepath.Join(out, "*.tmp")); len(left) != 0 {
		t.Fatalf("temporary files left: %v", left)
	}
}

func TestCompactTableDeltaMajor_refusesWhatItCannotFold(t *testing.T) {
	noCompaction(t)
	ctx := context.Background()
	check := func(name, base, want string) {
		t.Helper()
		out := filepath.Join(t.TempDir(), "stage")
		_, err := CompactTableDeltaMajor(ctx, base, out, nil, duckdbutil.Tuning{})
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("%s: err = %v, want %q", name, err, want)
		}
		if left, _ := os.ReadDir(out); len(left) != 0 {
			t.Fatalf("%s: a refused compaction left %v", name, left)
		}
	}
	// No chain at all.
	root := t.TempDir()
	bare := writeStampedBaseline(t, root, time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC))
	check("no chain", bare, "no chain")
	// Only the empty sequence-0 pair: its anchor is the base's, nothing to fold.
	if err := baseline.WriteEmptyTableDeltas(filepath.Dir(filepath.Dir(bare))); err != nil {
		t.Fatal(err)
	}
	check("empty chain", bare, "nothing to fold")
	// Half a pair: the chain is set aside, and so is the job.
	base, _, _ := plainChain(t, t.TempDir(), zooWindows())
	c, _ := baseline.ListTableDelta(ctx, base)
	if err := os.Remove(c.Files[1].Posdel); err != nil {
		t.Fatal(err)
	}
	check("half a pair", base, "set aside")
	// A disk that cannot take the table: refused before anything is written.
	base2, _, _ := plainChain(t, t.TempDir(), zooWindows())
	out := filepath.Join(t.TempDir(), "stage")
	_, err := CompactTableDeltaMajor(ctx, base2, out, func(string, int64) error { return fmt.Errorf("disk full") }, duckdbutil.Tuning{})
	if err == nil || !strings.Contains(err.Error(), "disk full") {
		t.Fatalf("space check: err = %v", err)
	}
	if left, _ := os.ReadDir(out); len(left) != 0 {
		t.Fatalf("a refused compaction left %v", left)
	}
}

// With nothing written after the job read the chain (the daemon's usual
// case: the job holds the refresh's slot), the chain after adoption is the
// empty start pair plus this window's.
func TestPublishWithTableDelta_adoptsAMajorCompaction(t *testing.T) {
	r := newMajorRun(t, zooWindows())
	oldBase, oldSnap := r.base, filepath.Dir(filepath.Dir(r.base))
	if _, err := baselineintegrity.WriteManifestFrom(oldSnap, nil); err != nil {
		t.Fatal(err)
	}
	foldedAt := r.at
	r.stageIt()
	rep := r.next(changeMap(upd(3, "three-v9"), ins(11, "eleven")))
	if rep.DeltaChainFolded != "0-2" || rep.DeltaCompacted != "" || !rep.DeltaPairWritten || rep.DeltaSeq != 1 {
		t.Fatalf("report = %+v, want pairs 0-2 folded and pair 1 written", rep)
	}
	wantNames := []string{"orders.000000.posdel", "orders.000000.upserts", "orders.000001.posdel", "orders.000001.upserts", "orders.parquet"}
	if got := chainNames(r.base); !reflect.DeepEqual(got, wantNames) {
		t.Fatalf("files = %v\nwant %v", got, wantNames)
	}
	if sameFile(r.base, oldBase) {
		t.Fatal("the base was carried, not replaced by the folded table")
	}
	if _, err := os.Stat(r.stage); !os.IsNotExist(err) {
		t.Fatalf("the adopted result was not removed: %v", err)
	}
	// The new pair says it was derived from the previous snapshot's table,
	// not from the staging folder, which no longer exists.
	pm := rawFooter(t, strings.TrimSuffix(r.base, ".parquet")+".000001.upserts")
	if pm[MetaKeyDerivedFromPath] != oldBase {
		t.Fatalf("derived from %q, want %q", pm[MetaKeyDerivedFromPath], oldBase)
	}
	// The chain now starts at the folded pair: readers fetch from there.
	bm, _ := baseline.ReadParquetMetadata(r.base)
	d, err := readTableDelta(context.Background(), r.base, bm)
	if err != nil || d == nil || !d.Meta.DeltaChainStart.Equal(foldedAt) {
		t.Fatalf("chain = %+v err=%v, want it to start at %s", d, err, foldedAt)
	}
	from, err := SnapshotReadsFrom(context.Background(), r.root, r.at)
	if err != nil || !from.Equal(foldedAt) {
		t.Fatalf("SnapshotReadsFrom = %s err=%v, want %s", from, err, foldedAt)
	}
	path, st0, _, err := FindBaseline(context.Background(), r.root, "mydb", "orders", r.at)
	if err != nil || path != r.base || !st0.Equal(foldedAt) {
		t.Fatalf("FindBaseline = %s %s err=%v, want %s at the chain start %s", path, st0, err, r.base, foldedAt)
	}
	// A view that follows the newest snapshot reads the same state.
	if got, want := byID(followState(t, r.base)), byID(readSnapshotRows(t, r.refOut)); !reflect.DeepEqual(got, want) {
		t.Fatalf("the following view differs:\n got %v\nwant %v", got, want)
	}
	// The new snapshot's manifest hashes every file afresh: none of them is
	// the previous snapshot's file under the same name.
	newSnap := filepath.Dir(filepath.Dir(r.base))
	st, err := baselineintegrity.WriteManifestFrom(newSnap, []string{oldSnap})
	if err != nil || st.Reused != 0 || st.Hashed != 5 {
		t.Fatalf("manifest = %+v err=%v, want 5 files hashed and none reused", st, err)
	}
	for _, f := range wantNames {
		if err := baselineintegrity.ValidateLocalFile(filepath.Join(newSnap, "mydb", f)); err != nil {
			t.Fatalf("%s: %v", f, err)
		}
	}
	// And the chain goes on from there.
	rep = r.next(changeMap(del(11)))
	if rep.DeltaChainFolded != "" || rep.DeltaSeq != 2 {
		t.Fatalf("report after = %+v", rep)
	}
}

// Pairs written after the job read the chain (a job that ran while another
// writer extended the chain, or a result staged before a slow refresh): their
// dead row numbers refer to the OLD base and are recomputed against the new
// one, including a key the folded pairs inserted, which had no row in the old
// base and has one in the new.
func TestPublishWithTableDelta_majorAdoptionRepositionsTheTail(t *testing.T) {
	windows := []map[string]*query.ResultRow{
		changeMap(upd(1, "one-v2"), del(2), ins(9, "nine")),
		changeMap(upd(3, "three-v2"), ins(7, "seven")),
		changeMap(upd(1, "one-v3")),
	}
	r := newMajorRun(t, windows)
	r.stageIt()
	r.job = false // the tail: refreshes that do not look at the result
	r.next(changeMap(upd(9, "nine-v2"), del(3), ins(2, "two-back")))
	r.next(changeMap(upd(7, "seven-v2"), del(1), ins(8, "eight")))
	oldTail := chainNames(r.base)
	if len(oldTail) != 11 { // 0..4 plus the base
		t.Fatalf("tail setup: %v", oldTail)
	}
	r.job = true
	rep := r.next(changeMap(upd(8, "eight-v2"), ins(10, "ten")))
	if rep.DeltaChainFolded != "0-2" || rep.DeltaSeq != 3 {
		t.Fatalf("report = %+v, want 0-2 folded and the chain at pair 3", rep)
	}
	wantNames := []string{"orders.000000.posdel", "orders.000000.upserts", "orders.000001.posdel", "orders.000001.upserts",
		"orders.000002.posdel", "orders.000002.upserts", "orders.000003.posdel", "orders.000003.upserts", "orders.parquet"}
	if got := chainNames(r.base); !reflect.DeepEqual(got, wantNames) {
		t.Fatalf("files = %v\nwant %v", got, wantNames)
	}
	// Pair 1 was the tail's first: it killed 9 (inserted by a folded pair)
	// and 3; pair 2 killed 7 and 1. Against the NEW base.
	p1 := strings.TrimSuffix(r.base, ".parquet") + ".000001.posdel"
	p2 := strings.TrimSuffix(r.base, ".parquet") + ".000002.posdel"
	if got, want := readPositions(t, p1), positionsOf(t, r.base, 9, 3); !reflect.DeepEqual(got, want) || len(want) != 2 {
		t.Fatalf("pair 1 dead rows = %v, want %v", got, want)
	}
	if got, want := readPositions(t, p2), positionsOf(t, r.base, 7, 1); !reflect.DeepEqual(got, want) || len(want) != 2 {
		t.Fatalf("pair 2 dead rows = %v, want %v", got, want)
	}
	// Their footers name the new chain and base, and their own place in it.
	bm, _ := baseline.ReadParquetMetadata(r.base)
	fi, _ := os.Stat(r.base)
	for seq, p := range []string{p1, p2} {
		for _, f := range []string{p, strings.TrimSuffix(p, ".posdel") + ".upserts"} {
			m := rawFooter(t, f)
			if m[baseline.MetaKeyDeltaSeq] != strconv.Itoa(seq+1) || m[baseline.MetaKeyDeltaSeqLo] != "" ||
				m[baseline.MetaKeyDeltaBaseAnchor] != baseAnchorString(bm) || m[baseline.MetaKeyDeltaBaseSize] != strconv.FormatInt(fi.Size(), 10) ||
				m[baseline.MetaKeyDeltaChainStart] != bm.SnapshotTimestamp.UTC().Format(time.RFC3339) {
				t.Fatalf("%s footer = %v", filepath.Base(f), m)
			}
		}
	}
	r.next(changeMap(del(10), upd(2, "two-v3")))
}

// Everything a refresh must refuse to adopt: the result is removed with a
// warning and the chain is extended as if it had never been staged.
func TestPublishWithTableDelta_majorResultNotAdopted(t *testing.T) {
	cases := []struct {
		name   string
		tamper func(t *testing.T, r *majorRun)
		kept   bool // left for the job to finish, not removed
	}{
		{"unfinished", func(t *testing.T, r *majorRun) { os.Remove(filepath.Join(r.stage, baseline.SuccessMarker)) }, true},
		{"another chain", func(t *testing.T, r *majorRun) {
			rewriteFile(t, r.mc.Base, "", func(m map[string]string) {
				m[baseline.MetaKeyFoldedChainStart] = r.at.Add(time.Hour).UTC().Format(time.RFC3339)
			})
		}, false},
		{"another base", func(t *testing.T, r *majorRun) {
			rewriteFile(t, r.mc.Base, "", func(m map[string]string) { m[baseline.MetaKeyFoldedBaseSize] = "1" })
		}, false},
		{"a pair the chain does not end at", func(t *testing.T, r *majorRun) {
			rewriteFile(t, r.mc.Base, "", func(m map[string]string) { m[baseline.MetaKeyFoldedSeq] = "9" })
		}, false},
		{"another anchor than the folded pair's", func(t *testing.T, r *majorRun) {
			rewriteFile(t, r.mc.Base, "", func(m map[string]string) { m[baseline.MetaKeyBinlogPos] = "1" })
		}, false},
		{"another CREATE TABLE", func(t *testing.T, r *majorRun) {
			rewriteFile(t, r.mc.Base, "", func(m map[string]string) { m[baseline.MetaKeyCreateTableSQL] += " " })
		}, false},
		{"a row lost", func(t *testing.T, r *majorRun) { rewriteFile(t, r.mc.Base, "id <> 3", nil) }, false},
		{"a row twice", func(t *testing.T, r *majorRun) {
			rewriteFile(t, r.mc.Base, "true UNION ALL SELECT * FROM read_parquet('"+r.mc.Base+"') WHERE id = 3", nil)
		}, false},
		{"one row in place of another", func(t *testing.T, r *majorRun) {
			// The count stays; only the key sum can tell.
			rewriteFile(t, r.mc.Base, "id <> 3 UNION ALL SELECT * FROM read_parquet('"+r.mc.Base+"') WHERE id = 2", nil)
		}, false},
		{"something else in the folder", func(t *testing.T, r *majorRun) {
			os.WriteFile(filepath.Join(r.stage, "notes.txt"), []byte("x"), 0o644)
		}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := newMajorRun(t, zooWindows())
			r.stageIt()
			tc.tamper(t, r)
			rep := r.next(changeMap(upd(3, "three-v9")))
			if rep.DeltaChainFolded != "" {
				t.Fatalf("adopted: %+v", rep)
			}
			if rep.DeltaSeq != 3 || rep.DeltaChainFiles != 4 {
				t.Fatalf("report = %+v, want the old chain extended to pair 3", rep)
			}
			left, err := os.ReadDir(r.stage)
			if tc.kept {
				if err != nil || rep.DeltaChainFoldRefused != "" {
					t.Fatalf("an unfinished result was removed or reported: %v %+v", err, rep)
				}
				return
			}
			// Removed, and the refusal recorded where the job looks, so the
			// same chain is not folded again every cycle; and reported.
			if err != nil || len(left) != 1 || left[0].Name() != CompactionRefusedMarker || rep.DeltaChainFoldRefused == "" {
				t.Fatalf("after a refusal: %v err=%v report=%+v", left, err, rep)
			}
		})
	}
}

// A failure to write is not a refusal: the result is kept, nothing is
// recorded against the chain, and the next refresh adopts it (with the pair
// the failed one wrote as its tail).
func TestPublishWithTableDelta_majorKeptWhenAdoptionCannotWrite(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores the folder's permissions")
	}
	r := newMajorRun(t, zooWindows())
	r.stageIt()
	if err := os.Chmod(r.stage, 0o555); err != nil {
		t.Fatal(err)
	}
	rep := r.next(changeMap(upd(3, "three-v9")))
	if err := os.Chmod(r.stage, 0o755); err != nil {
		t.Fatal(err)
	}
	if rep.DeltaChainFolded != "" || rep.DeltaChainFoldRefused != "" || rep.DeltaSeq != 3 {
		t.Fatalf("report = %+v, want the old chain extended and nothing refused", rep)
	}
	if _, err := os.Stat(filepath.Join(r.stage, "orders.parquet")); err != nil {
		t.Fatalf("the result was not kept: %v", err)
	}
	rep = r.next(changeMap(ins(12, "twelve")))
	if rep.DeltaChainFolded != "0-2" || rep.DeltaSeq != 2 {
		t.Fatalf("report = %+v, want the result adopted with one tail pair", rep)
	}
}

// The tail may only hold plain pairs: a range after the folded pair was
// made by another job for another view of the chain.
func TestPublishWithTableDelta_majorTailHoldingARangeIsNotAdopted(t *testing.T) {
	r := newMajorRun(t, zooWindows())
	r.stageIt()
	r.job = false
	r.next(changeMap(upd(3, "a")))
	r.next(changeMap(upd(3, "b")))
	c, _ := baseline.ListTableDelta(context.Background(), r.base)
	mc, err := CompactTableDeltaMinor(context.Background(), r.base, c, 3, 4, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	installRange(t, r.base, c, mc)
	r.job = true
	rep := r.next(changeMap(upd(3, "c")))
	if rep.DeltaChainFolded != "" || rep.DeltaChainFoldRefused == "" {
		t.Fatalf("adopted over a range tail: %+v", rep)
	}
	if _, err := os.Stat(r.mc.Base); !os.IsNotExist(err) {
		t.Fatal("the rejected result was left behind")
	}
}

// A refresh that died while adopting left pairs in the staging folder: the
// next one rebuilds them.
func TestPublishWithTableDelta_majorAdoptionCleansADeadAdoption(t *testing.T) {
	r := newMajorRun(t, zooWindows())
	r.stageIt()
	stem := filepath.Join(r.stage, "orders")
	for _, n := range []string{".000000.posdel", ".000000.upserts", ".000001.upserts", ".000001.posdel.tmp"} {
		if err := os.WriteFile(stem+n, []byte("half written"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	rep := r.next(changeMap(upd(3, "three-v9")))
	if rep.DeltaChainFolded != "0-2" {
		t.Fatalf("report = %+v, want the result adopted", rep)
	}
}

// A reason that forces the rewrite in place leaves the result alone: the
// rewrite folds the old chain, and the result is swept once the chain it was
// made for has ended.
func TestPublishWithTableDelta_forcedRewriteSkipsTheMajor(t *testing.T) {
	r := newMajorRun(t, zooWindows())
	r.stageIt()
	r.capGap = &CaptureGap{At: r.at, Detail: "test"}
	rep := r.next(changeMap(upd(3, "three-v9")))
	if rep.DeltaChainFolded != "" || !strings.Contains(rep.DeltaCompacted, "capture gap") {
		t.Fatalf("report = %+v, want the rewrite in place", rep)
	}
	if _, err := os.Stat(r.stage); err != nil {
		t.Fatalf("the result was touched by a rewrite that did not use it: %v", err)
	}
	r.capGap = nil
	r.next(changeMap(upd(3, "three-v10")))
	if _, err := os.Stat(r.stage); !os.IsNotExist(err) {
		t.Fatal("the result of an ended chain was not swept")
	}
}

// #1904 still ends the chain in time: when even the folded pair is at or
// before the line, the refresh writes the table in full (from the adopted
// file, which is the cheaper read) and the result is removed.
func TestPublishWithTableDelta_floorAfterAdoptionStillRewrites(t *testing.T) {
	r := newMajorRun(t, zooWindows())
	r.stageIt()
	r.floor = r.at // the folded pair's time: at the line
	rep := r.next(changeMap(upd(3, "three-v9")))
	if !strings.Contains(rep.DeltaCompacted, "too close to the oldest events") {
		t.Fatalf("report = %+v, want the chain ended at the floor", rep)
	}
	if _, err := os.Stat(r.stage); !os.IsNotExist(err) {
		t.Fatal("the used result was left behind")
	}
	if got := chainNames(r.base); !reflect.DeepEqual(got, []string{"orders.000000.posdel", "orders.000000.upserts", "orders.parquet"}) {
		t.Fatalf("files = %v", got)
	}
}

// With the job on, age and size no longer rewrite inside the refresh until
// the job has fallen well behind; with it off (the command line) they do, as
// before. The floor and every in-place reason are the same in both modes.
func TestTableDeltaCompactReason_1735(t *testing.T) {
	at := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	mk := func(age time.Duration, size int64) *tableDelta {
		return &tableDelta{Meta: baseline.DumpMetadata{DeltaChainStart: at.Add(-age), DeltaSeq: 5}, PairSize: size}
	}
	reason := func(d *tableDelta, job bool, floor time.Time, spilled bool) string {
		return tableDeltaCompactReason(d, "/snap/db/t.parquet", 10<<20, spilled, nil, at, true, "", floor, time.Time{}, job)
	}
	cases := []struct {
		name     string
		d        *tableDelta
		floor    time.Time
		spilled  bool
		cli, job string // substrings; "" means no rewrite
	}{
		{"young and small", mk(time.Hour, 1<<20), time.Time{}, false, "", ""},
		{"a day old", mk(25*time.Hour, 1<<20), time.Time{}, false, "old", ""},
		{"two days old", mk(49*time.Hour, 1<<20), time.Time{}, false, "old", "the compaction job has not folded it"},
		{"past a quarter", mk(time.Hour, 3<<20), time.Time{}, false, "passed 25%", ""},
		{"past the table", mk(time.Hour, 11<<20), time.Time{}, false, "passed 25%", "the compaction job has not folded it"},
		{"at the floor", mk(time.Hour, 1<<20), at.Add(-time.Hour), false, "too close", "too close"},
		{"spilled", mk(time.Hour, 1<<20), time.Time{}, true, "memory", "memory"},
	}
	for _, tc := range cases {
		for _, job := range []bool{false, true} {
			want := tc.cli
			if job {
				want = tc.job
			}
			got := reason(tc.d, job, tc.floor, tc.spilled)
			if (want == "") != (got == "") || !strings.Contains(got, want) {
				t.Fatalf("%s, job=%v: reason = %q, want %q", tc.name, job, got, want)
			}
		}
	}
}

func TestMajorCompactionReason(t *testing.T) {
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	last := func(age time.Duration, moved bool) baseline.DumpMetadata {
		m := baseline.DumpMetadata{DeltaChainStart: now.Add(-age), BinlogFile: "binlog.000009", BinlogPos: 4, DeltaBaseAnchor: "binlog.000009:4"}
		if moved {
			m.BinlogPos = 900
		}
		return m
	}
	lead := 70 * time.Minute
	cases := []struct {
		name        string
		m           baseline.DumpMetadata
		chain, base int64
		floor       time.Time
		want        string
	}{
		{"nothing to fold", last(30*time.Hour, false), 5 << 20, 10 << 20, time.Time{}, ""},
		{"no anchor", baseline.DumpMetadata{DeltaChainStart: now.Add(-30 * time.Hour)}, 5 << 20, 10 << 20, time.Time{}, ""},
		{"young", last(30*time.Minute, true), 5 << 20, 10 << 20, now, ""},
		{"quiet and small", last(2*time.Hour, true), 1 << 20, 10 << 20, time.Time{}, ""},
		{"a day old", last(24*time.Hour, true), 1 << 20, 10 << 20, time.Time{}, "old"},
		{"past a quarter", last(2*time.Hour, true), 3 << 20, 10 << 20, time.Time{}, "25%"},
		{"small table past a quarter", last(2*time.Hour, true), 100 << 10, 10 << 10, time.Time{}, ""},
		{"near the floor", last(3*time.Hour, true), 1 << 20, 10 << 20, now.Add(-3*time.Hour - lead + time.Minute), "oldest events"},
		{"clear of the floor", last(3*time.Hour, true), 1 << 20, 10 << 20, now.Add(-3*time.Hour - lead - time.Minute), ""},
	}
	for _, tc := range cases {
		got := MajorCompactionReason(tc.m, now, tc.chain, tc.base, tc.floor, lead)
		if (tc.want == "") != (got == "") || !strings.Contains(got, tc.want) {
			t.Fatalf("%s: reason = %q, want %q", tc.name, got, tc.want)
		}
	}
}

// The refresh's report says the table file was replaced by the job's, in
// both shapes a run can leave (a pair written or not). Printed so the words
// are read as they will appear.
func TestRefreshOutcomes_sayTheFoldedFileWasPutInPlace(t *testing.T) {
	reps := []*TableReport{
		{Schema: "mydb", Table: "orders", TableDelta: true, DeltaPairWritten: true, DeltaSeq: 1, DeltaChainFiles: 2,
			DeltaDeadRows: 1, DeltaUpsertRows: 2, DeltaChainFolded: "0-17"},
		{Schema: "mydb", Table: "items", TableDelta: true, DeltaSeq: 0, DeltaChainFiles: 1, DeltaChainFolded: "0-4"},
	}
	out := RefreshOutcomes([]string{"mydb.orders", "mydb.items"}, reps, nil)
	for _, o := range out {
		t.Logf("%s: %s", o.Table, o.Detail)
		if o.Verdict != RefreshVerdictRefreshed || !strings.Contains(o.Detail, "rebuilt by the compaction job") || strings.Contains(o.Detail, "daemon") {
			t.Fatalf("%+v", o)
		}
	}
	if !strings.Contains(out[0].Detail, "pairs 0-17 folded in") || !strings.Contains(out[0].Detail, "as pair 1") {
		t.Fatalf("orders: %s", out[0].Detail)
	}
	if !strings.Contains(out[1].Detail, "no events in the window") || strings.Contains(out[1].Detail, "kept as they are") {
		t.Fatalf("items: %s", out[1].Detail)
	}
}

// Once a fold is refused the job leaves that chain alone, so the refresh takes
// the day rule back: a refused chain past a day is written in full here, not
// left to grow to the two-day backstop.
func TestPublishWithTableDelta_aRefusedChainGetsTheDayRuleBack(t *testing.T) {
	r := newMajorRun(t, zooWindows())
	r.stageIt()
	rewriteFile(t, r.mc.Base, "", func(m map[string]string) { m[baseline.MetaKeyFoldedBaseSize] = "1" })
	if rep := r.next(changeMap(upd(3, "a"))); rep.DeltaChainFoldRefused == "" {
		t.Fatalf("not refused: %+v", rep)
	}
	r.at = r.at.Add(25 * time.Hour)
	rep := r.next(changeMap(upd(3, "b")))
	if !strings.Contains(rep.DeltaCompacted, "old") || strings.Contains(rep.DeltaCompacted, "has not folded") {
		t.Fatalf("report = %+v, want the chain written in full on its age", rep)
	}
}

// The readers #1735 names read the adopted snapshot like any other: verify's
// chain-start lookup and the listing the console's backup detail shows.
func TestAdoptedSnapshot_readByVerifyAndTheListing(t *testing.T) {
	r := newMajorRun(t, zooWindows())
	foldedAt := r.at
	r.stageIt()
	r.next(changeMap(upd(3, "three-v9")))
	start, err := DeltaChainStart(context.Background(), r.base)
	if err != nil || !start.Equal(foldedAt) {
		t.Fatalf("DeltaChainStart = %s err=%v, want %s", start, err, foldedAt)
	}
	files, err := ListBaselines(context.Background(), r.root)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, f := range files {
		if f.Path == r.base {
			found = f.SnapshotTime.Equal(r.at) && f.DeltaUpserts == "orders.000001.upserts"
		}
	}
	if !found {
		t.Fatalf("the adopted table is not listed as expected: %+v", files)
	}
}
