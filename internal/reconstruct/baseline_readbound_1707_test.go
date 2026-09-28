package reconstruct

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/status"
)

// copyTable copies a table file and every delta file beside it to another
// table name in dstDir. The files are what a listing and FindBaseline look
// at; nothing in them names the table.
func copyTable(t *testing.T, srcBase, dstDir, table string) string {
	t.Helper()
	if err := os.MkdirAll(dstDir, 0o755); err != nil {
		t.Fatal(err)
	}
	srcDir, stem := filepath.Dir(srcBase), strings.TrimSuffix(filepath.Base(srcBase), ".parquet")
	entries, err := os.ReadDir(srcDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		rest, ok := strings.CutPrefix(e.Name(), stem+".")
		if !ok {
			continue
		}
		in, err := os.Open(filepath.Join(srcDir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		out, err := os.Create(filepath.Join(dstDir, table+"."+rest))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.Copy(out, in); err != nil {
			t.Fatal(err)
		}
		in.Close()
		if err := out.Close(); err != nil {
			t.Fatal(err)
		}
	}
	return filepath.Join(dstDir, table+".parquet")
}

// chainFrom builds, in a root of its own, a chain of `windows` hourly deltas
// over a table file written at start, and returns the newest table file and
// its directory's time.
func chainFrom(t *testing.T, start time.Time, windows int) (base string, dirTime time.Time) {
	t.Helper()
	root := t.TempDir()
	base, dirTime = writeStampedBaseline(t, root, start), start
	for i := 1; i <= windows; i++ {
		at := start.Add(time.Duration(i) * time.Hour)
		nb, _, err := deltaWindow(t, root, base, dirTime, changeMap(upd(1, fmt.Sprintf("v%d", i))), at,
			&query.BinlogPos{File: "binlog.000009", Pos: uint64(1000 * i)}, nil)
		if err != nil {
			t.Fatal(err)
		}
		base, dirTime = nb, at
	}
	return base, dirTime
}

// TestReadBounds_isTheTimeFindBaselineReturns is the seam of #1707: for the
// same table of the same snapshot, the instant the verdict grades on and the
// instant a reader fetches events from are one. Each table below is a shape
// the two could come to disagree on.
func TestReadBounds_isTheTimeFindBaselineReturns(t *testing.T) {
	noCompaction(t)
	ctx := context.Background()
	root := t.TempDir()
	dir := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	snap := filepath.Join(root, SnapshotDirName(dir), "mydb")
	cest := time.FixedZone("CEST", 2*60*60)

	// orders: a chain of 10 windows, started 02:00. The report's shape.
	ordersStart := dir.Add(-10 * time.Hour)
	b, at := chainFrom(t, ordersStart, 10)
	if !at.Equal(dir) {
		t.Fatalf("fixture: chain ends %s, want %s", at, dir)
	}
	copyTable(t, b, snap, "orders")
	// users: a chain of its own, started 11:00. Same snapshot, another start.
	usersStart := dir.Add(-time.Hour)
	b, _ = chainFrom(t, usersStart, 1)
	copyTable(t, b, snap, "users")
	// orders_2: a name that starts like another table's. No chain of its own.
	plainSrc := writeStampedBaseline(t, t.TempDir(), dir)
	copyTable(t, plainSrc, snap, "orders_2")
	// plain: no delta files at all.
	copyTable(t, plainSrc, snap, "plain")
	// empty: the empty first pair a full backup writes.
	emptyDir := filepath.Join(t.TempDir(), SnapshotDirName(dir))
	copyTable(t, plainSrc, filepath.Join(emptyDir, "mydb"), "empty")
	if err := baseline.WriteEmptyTableDeltas(emptyDir); err != nil {
		t.Fatal(err)
	}
	copyTable(t, filepath.Join(emptyDir, "mydb", "empty.parquet"), snap, "empty")
	// later: a footer that claims a start AFTER its directory.
	later := copyTable(t, plainSrc, snap, "later")
	writeLegacyPair(t, later, dir.Add(3*time.Hour), query.BinlogPos{File: "binlog.000009", Pos: 10}, nil, nil)
	// offset: the v0.83.0 pair, its start written with a UTC offset. 09:30
	// at +02:00 is 07:30 UTC.
	offset := copyTable(t, plainSrc, snap, "offset")
	offsetStart := time.Date(2026, 5, 2, 9, 30, 0, 0, cest)
	writeLegacyPair(t, offset, offsetStart, query.BinlogPos{File: "binlog.000009", Pos: 10}, nil, nil)
	// day: a chain that started 30 hours before, past the writer's cap.
	b, _ = chainFrom(t, dir.Add(-30*time.Hour), 1)
	copyTable(t, b, snap, "day")
	// An older snapshot of orders, and one that is incomplete and newer.
	copyTable(t, plainSrc, filepath.Join(root, SnapshotDirName(dir.Add(-48*time.Hour)), "mydb"), "orders")
	partial := filepath.Join(root, SnapshotDirName(dir.Add(time.Hour)))
	copyTable(t, plainSrc, filepath.Join(partial, "mydb"), "orders")
	if err := os.WriteFile(filepath.Join(partial, baseline.IncompleteMarker), nil, 0o644); err != nil {
		t.Fatal(err)
	}

	files, err := ListBaselines(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	var newest []BaselineFile
	for _, i := range NewestPerTable(files) {
		newest = append(newest, files[i])
	}
	want := map[string]time.Time{
		"orders": ordersStart, "users": usersStart, "orders_2": dir, "plain": dir,
		"empty": dir, "later": dir, "offset": offsetStart, "day": dir.Add(-30 * time.Hour),
	}
	if len(newest) != len(want) {
		t.Fatalf("listed %d tables, want %d: %+v", len(newest), len(want), newest)
	}
	bounds := ReadBounds(ctx, newest)
	for i, f := range newest {
		if !f.SnapshotTime.Equal(dir) {
			t.Errorf("%s: newest snapshot %s, want %s (the incomplete one must not be listed)", f.Table, f.SnapshotTime, dir)
		}
		if bounds[i].Unread {
			t.Errorf("%s: unread", f.Table)
			continue
		}
		got := bounds[i].From(f.SnapshotTime)
		path, reader, _, err := FindBaseline(ctx, root, f.Schema, f.Table, dir.Add(30*time.Minute))
		if err != nil {
			t.Fatalf("FindBaseline %s: %v", f.Table, err)
		}
		if path != f.Path {
			t.Fatalf("%s: FindBaseline read %s, the listing graded %s", f.Table, path, f.Path)
		}
		if !got.Equal(reader) {
			t.Errorf("%s: the verdict grades on %s, a reader fetches from %s", f.Table, got, reader)
		}
		if !got.Equal(want[f.Table]) {
			t.Errorf("%s: graded on %s, want %s", f.Table, got, want[f.Table])
		}
		if hasDelta := f.Table != "plain" && f.Table != "orders_2"; f.HasDelta() != hasDelta {
			t.Errorf("%s: HasDelta = %v", f.Table, f.HasDelta())
		}
	}

	// The older snapshot of orders has no chain, and is graded on itself.
	for i, f := range files {
		if f.Table == "orders" && !f.SnapshotTime.Equal(dir) {
			if b := ReadBounds(ctx, files[i:i+1])[0]; f.HasDelta() || b != (status.ReadBound{}) {
				t.Errorf("older snapshot of orders: %+v, bound %+v", f, b)
			}
		}
	}
}

// TestReadBounds_damagedDeltaIsUnread: a reader survives damaged delta files
// by reading from the time the table file was written, which can be days
// back. The verdict does not guess at that: the start is unread, and unread
// is never graded as covered.
func TestReadBounds_damagedDeltaIsUnread(t *testing.T) {
	noCompaction(t)
	ctx := context.Background()
	dir := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	// Three windows: pairs 0, 1 and 2.
	src, _ := chainFrom(t, dir.Add(-3*time.Hour), 3)
	posdel1, upserts1 := baseline.TableDeltaPaths("x.parquet", 1)
	posdel1, upserts1 = strings.TrimPrefix(posdel1, "x"), strings.TrimPrefix(upserts1, "x")

	for _, tc := range []struct {
		name   string
		damage func(t *testing.T, base string)
	}{
		{"a dead-positions file with no upserts", func(t *testing.T, base string) {
			rm(t, strings.TrimSuffix(base, ".parquet")+upserts1)
		}},
		{"an upserts file with no dead positions", func(t *testing.T, base string) {
			rm(t, strings.TrimSuffix(base, ".parquet")+posdel1)
		}},
		{"a pair missing from the middle of the chain", func(t *testing.T, base string) {
			rm(t, strings.TrimSuffix(base, ".parquet")+posdel1)
			rm(t, strings.TrimSuffix(base, ".parquet")+upserts1)
		}},
		{"a newest upserts file that is not a Parquet file", func(t *testing.T, base string) {
			_, up := baseline.TableDeltaPaths(base, 2)
			if err := os.WriteFile(up, []byte("not parquet"), 0o644); err != nil {
				t.Fatal(err)
			}
		}},
		{"a newest upserts file that cannot be opened", func(t *testing.T, base string) {
			if os.Geteuid() == 0 {
				t.Skip("root opens a file of mode 000")
			}
			_, up := baseline.TableDeltaPaths(base, 2)
			if err := os.Chmod(up, 0); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			snap := filepath.Join(root, SnapshotDirName(dir), "mydb")
			base := copyTable(t, src, snap, "orders")
			copyTable(t, src, snap, "users") // the table beside it is whole
			tc.damage(t, base)
			files, err := ListBaselines(ctx, root)
			if err != nil || len(files) != 2 {
				t.Fatalf("files = %+v, err = %v", files, err)
			}
			bounds := ReadBounds(ctx, files)
			if !bounds[0].Unread || !bounds[0].ChainStart.IsZero() {
				t.Errorf("orders: %+v, want unread", bounds[0])
			}
			if !files[0].HasDelta() {
				t.Errorf("orders: the listing saw no delta files: %+v", files[0])
			}
			// The damage is one table's: the other is read.
			if bounds[1].Unread || !bounds[1].ChainStart.Equal(dir.Add(-3*time.Hour)) {
				t.Errorf("users: %+v, want the chain's start", bounds[1])
			}
			now := dir.Add(time.Hour)
			floor := status.DeltaFloor{Hour: dir.Add(-24 * time.Hour)}
			if got := floor.GradeTable(files[0].SnapshotTime, bounds[0], now); got != status.BaselineUnknown {
				t.Errorf("orders grades %q, want unknown", got)
			}
		})
	}
}

func rm(t *testing.T, path string) {
	t.Helper()
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
}

// countingChainStarts replaces the footer read with one that counts and
// answers from a table of upserts path to start.
type countingChainStarts struct {
	mu     sync.Mutex
	asked  []string
	starts map[string]time.Time
	fail   map[string]error
}

func (c *countingChainStarts) install(t *testing.T) {
	t.Helper()
	prev := chainStartAt
	t.Cleanup(func() { chainStartAt = prev })
	chainStartAt = func(_ context.Context, upserts string) (time.Time, error) {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.asked = append(c.asked, upserts)
		if err := c.fail[upserts]; err != nil {
			return time.Time{}, err
		}
		start, ok := c.starts[upserts]
		if !ok {
			return time.Time{}, fmt.Errorf("unexpected footer read of %s", upserts)
		}
		return start, nil
	}
}

func (c *countingChainStarts) reads() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := len(c.asked)
	c.asked = nil
	return n
}

// forgetS3ChainStarts empties the per-process memo, which outlives a test.
func forgetS3ChainStarts(t *testing.T) {
	t.Helper()
	clear := func() {
		s3ChainStarts.Range(func(k, _ any) bool { s3ChainStarts.Delete(k); return true })
	}
	clear()
	t.Cleanup(clear)
}

// chainKeys is a table's keys in a snapshot directory: its file and the
// pairs 0..last of its chain.
func chainKeys(dir, table string, last int) []string {
	keys := []string{dir + "/" + table + ".parquet"}
	for seq := 0; seq <= last; seq++ {
		p, u := baseline.TableDeltaPaths(dir+"/"+table+".parquet", seq)
		keys = append(keys, p, u)
	}
	return keys
}

// TestReadBounds_s3Requests pins what the verdict costs over S3 (#1707, and
// the bounds of #1679 and #1847 it must not undo): the listing makes the
// requests it made before, the chain is found on the names that listing
// already returned, and its start is one footer read per table WITH a chain,
// asked for, once per process.
func TestReadBounds_s3Requests(t *testing.T) {
	ctx := context.Background()
	forgetS3ChainStarts(t)
	f := &fakeS3Snapshots{}
	const tables = 5
	for d := 1; d <= 3; d++ {
		keys := []string{dirAt(d) + "/_SUCCESS", dirAt(d) + "/shop/plain.parquet"}
		for i := range tables {
			keys = append(keys, chainKeys(dirAt(d), fmt.Sprintf("shop/t%d", i), 40)...)
		}
		f.add(keys...)
	}
	stubS3Snapshots(t, f)
	c := &countingChainStarts{starts: map[string]time.Time{}, fail: map[string]error{}}
	start := time.Date(2026, 9, 18, 0, 30, 0, 0, time.UTC)
	for d := 1; d <= 3; d++ {
		for i := range tables {
			_, up := baseline.TableDeltaPaths(fmt.Sprintf("s3://b/base/%s/shop/t%d.parquet", dirAt(d), i), 40)
			c.starts[up] = start.Add(time.Duration(i) * time.Minute)
		}
	}
	c.install(t)

	files, err := ListBaselines(ctx, "s3://b/base")
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 3*(tables+1) {
		t.Fatalf("listed %d files, want %d", len(files), 3*(tables+1))
	}
	if f.dirCalls != 1 || len(f.dirsRead()) != 3 {
		t.Fatalf("the listing made %d directory listings and read %v", f.dirCalls, f.asked())
	}
	if n := c.reads(); n != 0 {
		t.Fatalf("the listing read %d footers, want none", n)
	}

	var newest []BaselineFile
	for _, i := range NewestPerTable(files) {
		newest = append(newest, files[i])
	}
	bounds := ReadBounds(ctx, newest)
	if n := c.reads(); n != tables {
		t.Fatalf("grading each table's newest snapshot read %d footers, want %d (one per table with a chain)", n, tables)
	}
	if got := f.dirsRead(); f.dirCalls != 1 || len(got) != 0 {
		t.Fatalf("reading the bounds listed again: %d directory listings, %v", f.dirCalls, got)
	}
	for i, nf := range newest {
		var want time.Time
		if nf.Table != "plain" {
			var n int
			fmt.Sscanf(nf.Table, "t%d", &n)
			want = start.Add(time.Duration(n) * time.Minute)
		}
		if bounds[i].Unread || !bounds[i].ChainStart.Equal(want) {
			t.Errorf("%s: %+v, want start %s", nf.Table, bounds[i], want)
		}
	}

	// Again, and through a fresh listing: nothing is read twice.
	files, err = ListBaselines(ctx, "s3://b/base")
	if err != nil {
		t.Fatal(err)
	}
	ReadBounds(ctx, newest)
	if n := c.reads(); n != 0 {
		t.Fatalf("the second grading read %d footers, want none", n)
	}
	// The whole inventory, for the record of what NOT bounding costs.
	ReadBounds(ctx, files)
	if n := c.reads(); n != 2*tables {
		t.Fatalf("grading every file read %d more footers, want %d", n, 2*tables)
	}
}

// A footer that could not be read is unread, is not kept, and is asked for
// again next time: a store that did not answer once must not freeze the
// verdict at unknown, and must never read as covered.
func TestReadBounds_s3FailureIsUnreadAndRetried(t *testing.T) {
	ctx := context.Background()
	forgetS3ChainStarts(t)
	f := &fakeS3Snapshots{}
	f.add(append(chainKeys(dirAt(1), "shop/orders", 1), chainKeys(dirAt(1), "shop/users", 0)...)...)
	f.add(dirAt(1) + "/_SUCCESS")
	stubS3Snapshots(t, f)
	_, ordersUp := baseline.TableDeltaPaths("s3://b/base/"+dirAt(1)+"/shop/orders.parquet", 1)
	_, usersUp := baseline.TableDeltaPaths("s3://b/base/"+dirAt(1)+"/shop/users.parquet", 0)
	start := time.Date(2026, 9, 17, 20, 0, 0, 0, time.UTC)
	c := &countingChainStarts{
		starts: map[string]time.Time{ordersUp: start, usersUp: start.Add(time.Hour)},
		fail:   map[string]error{ordersUp: errors.New("SlowDown")},
	}
	c.install(t)
	files, err := ListBaselines(ctx, "s3://b/base")
	if err != nil || len(files) != 2 {
		t.Fatalf("files = %+v, err = %v", files, err)
	}
	bounds := ReadBounds(ctx, files)
	if !bounds[0].Unread || !bounds[0].ChainStart.IsZero() {
		t.Errorf("orders: %+v, want unread", bounds[0])
	}
	if bounds[1].Unread || !bounds[1].ChainStart.Equal(start.Add(time.Hour)) {
		t.Errorf("users: %+v, want ITS start, not the neighbour's", bounds[1])
	}
	c.reads()
	delete(c.fail, ordersUp)
	bounds = ReadBounds(ctx, files)
	if n := c.reads(); n != 1 {
		t.Errorf("the retry read %d footers, want 1: the failed one alone", n)
	}
	if bounds[0].Unread || !bounds[0].ChainStart.Equal(start) {
		t.Errorf("orders after the store answered: %+v", bounds[0])
	}

	// A deadline that has passed: every unanswered chain is unread.
	forgetS3ChainStarts(t)
	done, cancel := context.WithCancel(ctx)
	cancel()
	chainStartAt = func(ctx context.Context, _ string) (time.Time, error) { return time.Time{}, ctx.Err() }
	for i, b := range ReadBounds(done, files) {
		if !b.Unread {
			t.Errorf("%s under a passed deadline: %+v, want unread", files[i].Table, b)
		}
	}
}

// The S3 listing marks a chain by the same rule as the local one, off the
// names of the directory read: a whole chain, a damaged one, none, and names
// that only look like another table's.
func TestListBaselinesS3_marksTheChainBesideEachTable(t *testing.T) {
	forgetS3ChainStarts(t)
	f := &fakeS3Snapshots{}
	d := dirAt(1)
	f.add(d + "/_SUCCESS")
	f.add(chainKeys(d, "shop/orders", 2)...)
	f.add(d+"/shop/orders_2.parquet", d+"/shop/plain.parquet")
	f.add(d+"/shop/half.parquet", d+"/shop/half.000000.posdel")
	f.add(d+"/shop/gap.parquet", d+"/shop/gap.000001.posdel", d+"/shop/gap.000001.upserts")
	f.add(d+"/shop/old.parquet", d+"/shop/old.posdel", d+"/shop/old.upserts")
	f.add(chainKeys(d, "other/orders", 0)...)
	stubS3Snapshots(t, f)
	files, err := ListBaselines(context.Background(), "s3://b/base")
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, bf := range files {
		v := bf.DeltaUpserts
		if bf.DeltaErr != nil {
			if !errors.Is(bf.DeltaErr, baseline.ErrHalfTableDelta) || v != "" {
				t.Errorf("%s.%s: err %v, upserts %q", bf.Schema, bf.Table, bf.DeltaErr, v)
			}
			v = "damaged"
		}
		got[bf.Schema+"."+bf.Table] = v
		if v != "" && v != "damaged" {
			if want := "s3://b/base/" + d + "/" + bf.Schema + "/" + v; bf.deltaUpsertsPath() != want {
				t.Errorf("%s.%s: path %q, want %q", bf.Schema, bf.Table, bf.deltaUpsertsPath(), want)
			}
		}
	}
	want := map[string]string{
		"shop.orders": "orders.000002.upserts", "shop.orders_2": "", "shop.plain": "",
		"shop.half": "damaged", "shop.gap": "damaged", "shop.old": "old.upserts",
		"other.orders": "orders.000000.upserts",
	}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for k, w := range want {
		if got[k] != w {
			t.Errorf("%s: %q, want %q", k, got[k], w)
		}
	}
}
