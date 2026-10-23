package keyidx

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	"github.com/parquet-go/parquet-go"
)

// Review round on #2496 slice 2: the key's canonical order is the base
// file's, the header names the columns, one build at a time, the disk guard
// counts the sort's spill, a build that does not end in an index removes a
// stale one, a cancelled build says so, and row numbers are checked against
// an independent read of the file.

// The order the caller names the key in does not matter: the records are
// encoded in the order the base file holds the columns, and the header
// names them in that order. The fixture is order_line-shaped: the file's
// columns are (o_id, d_id, w_id, number) and the PRIMARY KEY is declared
// (w_id, d_id, o_id, number).
func TestBuild_keyOrderIsTheBaseFilesOrder(t *testing.T) {
	base := writeBase(t, "SELECT (i % 50)::INTEGER AS ol_o_id, (i % 10)::INTEGER AS ol_d_id, (i % 3)::INTEGER AS ol_w_id, (i % 7)::INTEGER AS ol_number FROM range(500) r(i) ORDER BY hash(i)", 1000)
	var files [][]byte
	for _, keys := range [][]string{
		{"ol_o_id", "ol_d_id", "ol_w_id", "ol_number"}, // table order
		{"ol_w_id", "ol_d_id", "ol_o_id", "ol_number"}, // PRIMARY KEY order
		{"ol_number", "ol_w_id", "ol_o_id", "ol_d_id"}, // shuffled
	} {
		got := checkIndex(t, base, keys)
		if len(got) != 500 {
			t.Fatal(len(got))
		}
		b, err := os.ReadFile(PathFor(base))
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, b)
		ix, err := openIndex(PathFor(base))
		if err != nil {
			t.Fatal(err)
		}
		ix.Close()
		if want := []string{"ol_o_id", "ol_d_id", "ol_w_id", "ol_number"}; !slices.Equal(ix.Names, want) {
			t.Fatalf("keys named %q: header names %q, want the file's order %q", keys, ix.Names, want)
		}
	}
	if !bytes.Equal(files[0], files[1]) || !bytes.Equal(files[0], files[2]) {
		t.Fatal("the same key named in three orders gave different index files")
	}
}

func TestBuild_refusesAKeyTheBaseDoesNotHave(t *testing.T) {
	base := writeBase(t, "SELECT i::INTEGER AS id FROM range(10) r(i)", 1000)
	for _, tc := range []struct {
		keys []string
		want string
	}{
		{[]string{"nope"}, `key column "nope" is not a column of the base file`},
		{[]string{"id", "id"}, `key column "id" named twice`},
	} {
		if _, err := Build(context.Background(), base, tc.keys, duckdbutil.DefaultTuning()); err == nil || err.Error() != tc.want {
			t.Errorf("keys %q: err %v, want %q", tc.keys, err, tc.want)
		}
		if _, err := os.Stat(PathFor(base)); err == nil {
			t.Errorf("keys %q: an index was written", tc.keys)
		}
	}
}

// Builds run one at a time per process, whatever the callers do.
func TestBuild_oneBuildAtATime(t *testing.T) {
	var cur, peak atomic.Int32
	BuildHook = func() {
		n := cur.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(30 * time.Millisecond)
		cur.Add(-1)
	}
	t.Cleanup(func() { BuildHook = nil })
	var wg sync.WaitGroup
	for i := range 5 {
		base := writeBase(t, "SELECT i::INTEGER AS id FROM range(100) r(i)", 1000)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := Build(context.Background(), base, []string{"id"}, duckdbutil.DefaultTuning()); err != nil {
				t.Errorf("build %d: %v", i, err)
			}
		}()
	}
	wg.Wait()
	if peak.Load() != 1 {
		t.Fatalf("%d builds ran at once, want 1", peak.Load())
	}
}

// The disk guard: the index beside the base and the sort's spill (the
// records once more) in the spill directory, summed when the two are one
// filesystem, each against its own when they are not.
func TestBuild_diskGuardCountsTheSpill(t *testing.T) {
	base := writeBase(t, "SELECT i::INTEGER AS id FROM range(100) r(i)", 1000)
	prev, prevSame, prevSpill := diskSpace, sameFilesystem, spillDir
	t.Cleanup(func() { diskSpace, sameFilesystem, spillDir = prev, prevSame, prevSpill })
	spill := t.TempDir()
	spillDir = func() string { return spill }
	index := uint64(ExpectedSize(100, []string{"id"}, 4))
	records := uint64(100 * 12)
	free := map[string]uint64{}
	diskSpace = func(dir string) (uint64, uint64, error) { return free[dir], 1 << 40, nil }
	run := func() (Result, error) {
		return Build(context.Background(), base, []string{"id"}, duckdbutil.DefaultTuning())
	}
	// One filesystem: the base's directory must hold both.
	sameFilesystem = func(a, b string) bool { return a == filepath.Dir(base) && b == spill }
	free[filepath.Dir(base)] = index + records + FreeMargin - 1
	if res, err := run(); err != nil || !res.Warn || !strings.Contains(res.Skipped, "the index with its sort's spill") {
		t.Fatalf("one filesystem, one byte short: res %+v err %v", res, err)
	}
	free[filepath.Dir(base)] = index + records + FreeMargin
	if res, err := run(); err != nil || res.Skipped != "" {
		t.Fatalf("one filesystem, exactly enough: res %+v err %v", res, err)
	}
	// Two filesystems: each its own.
	sameFilesystem = func(a, b string) bool { return false }
	free[filepath.Dir(base)] = index + FreeMargin
	free[spill] = records + FreeMargin - 1
	if res, err := run(); err != nil || !res.Warn || !strings.Contains(res.Skipped, "the sort's spill") || !strings.Contains(res.Skipped, spill) {
		t.Fatalf("spill short: res %+v err %v", res, err)
	}
	free[spill] = records + FreeMargin
	free[filepath.Dir(base)] = index + FreeMargin - 1
	if res, err := run(); err != nil || !res.Warn || !strings.Contains(res.Skipped, "the index needs") {
		t.Fatalf("index short: res %+v err %v", res, err)
	}
	free[filepath.Dir(base)] = index + FreeMargin
	if res, err := run(); err != nil || res.Skipped != "" {
		t.Fatalf("both enough: res %+v err %v", res, err)
	}
}

// A build that does not end in an index removes the one already there: a
// failure (NULL key) and a skip (text key) over a base whose earlier index
// is beside it.
func TestBuild_removesAStaleIndexWhenItWritesNone(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
	}{
		{"a NULL key fails the build", "SELECT CASE WHEN i = 5 THEN NULL ELSE i END::INTEGER AS id FROM range(10) r(i)"},
		{"a text key skips the build", "SELECT i::VARCHAR AS id FROM range(10) r(i)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := writeBase(t, tc.sql, 1000)
			stale := PathFor(base)
			if err := os.WriteFile(stale, []byte("an index from an earlier build"), 0o644); err != nil {
				t.Fatal(err)
			}
			res, err := Build(context.Background(), base, []string{"id"}, duckdbutil.DefaultTuning())
			if err == nil && res.Skipped == "" {
				t.Fatal("the build wrote an index")
			}
			if _, serr := os.Stat(stale); serr == nil {
				t.Fatalf("the stale index is still beside the base (err %v, skipped %q)", err, res.Skipped)
			}
		})
	}
}

// Cancelled from inside the scan, after blocks were written: the error is
// the cancellation, and neither the index nor the .tmp is left.
func TestBuild_cancelledPartWayLeavesNoFile(t *testing.T) {
	base := writeBase(t, "SELECT i::INTEGER AS id FROM range(20000) r(i) ORDER BY hash(i)", 1000)
	ctx, cancel := context.WithCancel(context.Background())
	flushed := 0
	blockHook = func() {
		flushed++
		if flushed == 2 {
			cancel()
		}
	}
	t.Cleanup(func() { blockHook = nil; cancel() })
	_, err := Build(ctx, base, []string{"id"}, duckdbutil.DefaultTuning())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if flushed != 2 {
		t.Fatalf("%d blocks were written after the cancellation at the second", flushed)
	}
	for _, p := range []string{PathFor(base), PathFor(base) + ".tmp"} {
		if _, serr := os.Stat(p); serr == nil {
			t.Fatalf("%s left behind", p)
		}
	}
	// And the publish policy says it once, at Debug, not as damage.
	logs := captureDebug(t)
	WriteBeside(ctx, "s", "t", base, []string{"id"}, duckdbutil.Tuning{})
	if n := strings.Count(logs.String(), "level=WARN"); n != 0 {
		t.Fatalf("a cancelled build warned %d time(s):\n%s", n, logs.String())
	}
	if !strings.Contains(logs.String(), `level=DEBUG msg="key index not written: the build was cancelled"`) {
		t.Fatalf("no debug line for the cancellation:\n%s", logs.String())
	}
}

// The row numbers point at the right physical rows: a sample of records
// across several row groups is checked against parquet-go reading row r of
// the file, an engine the writer did not use.
func TestBuild_rowNumbersMatchThePhysicalRows(t *testing.T) {
	base := writeBase(t, "SELECT (i * 7919 % 10007)::INTEGER AS id, i AS n FROM range(10000) r(i) ORDER BY hash(i)", 1000)
	got := checkIndex(t, base, []string{"id"})
	f, err := os.Open(base)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	pf, err := parquet.OpenFile(f, fi.Size())
	if err != nil {
		t.Fatal(err)
	}
	if len(pf.RowGroups()) < 2 {
		t.Fatalf("the fixture has %d row group(s)", len(pf.RowGroups()))
	}
	col := -1
	for i, c := range pf.Schema().Columns() {
		if c[0] == "id" {
			col = i
		}
	}
	if col < 0 {
		t.Fatal("no id column")
	}
	reader := parquet.NewReader(pf)
	defer reader.Close()
	rows := make([]parquet.Row, 1)
	checked := 0
	for i := 0; i < len(got); i += 97 {
		if err := reader.SeekToRow(got[i].Row); err != nil {
			t.Fatal(err)
		}
		if n, err := reader.ReadRows(rows); err != nil || n != 1 {
			t.Fatalf("row %d: (%d, %v)", got[i].Row, n, err)
		}
		want := encodeRef(t, []any{rows[0][col].Int32()})
		if !bytes.Equal(got[i].Key, want) {
			t.Fatalf("record %d says row %d holds key %x; the file holds %x there", i, got[i].Row, got[i].Key, want)
		}
		checked++
	}
	if checked < 100 {
		t.Fatalf("checked %d rows", checked)
	}
}

func TestPrimaryKeyColumns_quotesAndForms(t *testing.T) {
	for _, tc := range []struct {
		name string
		ddl  string
		want []string
	}{
		{"a name holding a quote, then a COMMENT decoy",
			"CREATE TABLE `t` (\n  `o'clock` int NOT NULL,\n  `c` text COMMENT 'PRIMARY KEY (`x`)',\n  PRIMARY KEY (`o'clock`)\n) ENGINE=InnoDB;", []string{"o'clock"}},
		{"a name holding a backtick and a quote",
			"CREATE TABLE `t` (`a``b'c` int, `d` text COMMENT 'PRIMARY KEY (`x`)', PRIMARY KEY (`a``b'c`))", []string{"a`b'c"}},
		{"a double-quoted string holding PRIMARY KEY",
			"CREATE TABLE `t` (`id` int, `c` varchar(9) DEFAULT \"PRIMARY KEY (`x`)\", PRIMARY KEY (`id`))", []string{"id"}},
		{"DESC and ASC, as MySQL 8 prints them",
			"CREATE TABLE `t` (\n  `a` int NOT NULL,\n  `b` int NOT NULL,\n  PRIMARY KEY (`a` DESC,`b` ASC)\n) ENGINE=InnoDB;", []string{"a", "b"}},
		{"under a CONSTRAINT name",
			"CREATE TABLE `t` (\n  `id` int NOT NULL,\n  CONSTRAINT `pk_t` PRIMARY KEY (`id`)\n)", []string{"id"}},
		{"inline on the column",
			"CREATE TABLE `t` (\n  `id` int NOT NULL PRIMARY KEY,\n  `v` text\n)", []string{"id"}},
		{"inline, last column",
			"CREATE TABLE `t` (`v` text, `id` bigint unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY)", []string{"id"}},
		{"a KEY that is not primary, and a column named primary_key",
			"CREATE TABLE `t` (\n  `primary_key` int,\n  `id` int,\n  KEY `k` (`primary_key`),\n  PRIMARY KEY (`id`)\n)", []string{"id"}},
		{"no key at all", "CREATE TABLE `t` (`id` int, KEY `k` (`id`))", nil},
	} {
		if got := PrimaryKeyColumns(tc.ddl); !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%s: got %q, want %q", tc.name, got, tc.want)
		}
	}
}

func TestBlankStrings(t *testing.T) {
	for in, want := range map[string]string{
		"a 'b' c":              "a ' ' c",
		"`o'clock` 'x'":        "`o'clock` ' '",
		"`a``b` 'c'":           "`a``b` ' '",
		`"PRIMARY KEY" x`:      `"           " x`,
		`'it''s' y`:            `'     ' y`,
		`'a\'b' z`:             `'    ' z`,
		"'unterminated":        "'            ",
		"`unterminated 'quote": "`unterminated 'quote",
	} {
		if got := blankStrings(in); got != want {
			t.Errorf("%q: got %q, want %q", in, got, want)
		}
	}
}

// A .tmp that will not go is said with its path.
func TestRemoveTemp_saysWhatWillNotGo(t *testing.T) {
	logs := captureDebug(t)
	dir := filepath.Join(t.TempDir(), "t.keyidx.tmp")
	if err := os.MkdirAll(filepath.Join(dir, "x"), 0o755); err != nil {
		t.Fatal(err)
	}
	removeTemp(dir)
	if !strings.Contains(logs.String(), `level=WARN msg="could not remove a key index's temporary file"`) || !strings.Contains(logs.String(), dir) {
		t.Fatalf("log:\n%s", logs.String())
	}
	logs.Reset()
	removeTemp(filepath.Join(t.TempDir(), "absent.tmp"))
	if logs.Len() != 0 {
		t.Fatalf("an absent .tmp was logged:\n%s", logs.String())
	}
}

// Bound: an index is tied to its base by size, footer CRC and rows; one
// copied beside another base of the same shape is refused; an absent one
// is false without an error; a damaged one is an error.
func TestBound(t *testing.T) {
	a := writeBase(t, "SELECT i::INTEGER AS id FROM range(100) r(i)", 1000)
	b := writeBase(t, "SELECT (99 - i)::INTEGER AS id FROM range(100) r(i)", 1000)
	if ok, err := Bound(a); ok || err != nil {
		t.Fatalf("absent: %v %v", ok, err)
	}
	if _, err := Build(context.Background(), a, []string{"id"}, duckdbutil.DefaultTuning()); err != nil {
		t.Fatal(err)
	}
	if ok, err := Bound(a); !ok || err != nil {
		t.Fatalf("own base: %v %v", ok, err)
	}
	raw, err := os.ReadFile(PathFor(a))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(PathFor(b), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	if ok, err := Bound(b); ok || err != nil {
		t.Fatalf("another base of the same rows: %v %v", ok, err)
	}
	// Two files of the SAME size and rows whose footers differ by one
	// metadata byte: only the footer CRC tells them apart.
	ddb := openDuck(t)
	var same [2]string
	for i, tag := range []string{"x", "y"} {
		same[i] = filepath.Join(t.TempDir(), "s.parquet")
		if _, err := ddb.Exec("COPY (SELECT i::INTEGER AS id FROM range(100) r(i)) TO '" + same[i] + "' (FORMAT PARQUET, KV_METADATA {'tag': '" + tag + "'})"); err != nil {
			t.Fatal(err)
		}
	}
	if fi2(t, same[0]) != fi2(t, same[1]) {
		t.Fatalf("fixture: the two files differ in size (%d, %d)", fi2(t, same[0]), fi2(t, same[1]))
	}
	if _, err := Build(context.Background(), same[0], []string{"id"}, duckdbutil.DefaultTuning()); err != nil {
		t.Fatal(err)
	}
	raw, _ = os.ReadFile(PathFor(same[0]))
	if err := os.WriteFile(PathFor(same[1]), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	if ok, err := Bound(same[0]); !ok || err != nil {
		t.Fatalf("own base, same-size pair: %v %v", ok, err)
	}
	if ok, err := Bound(same[1]); ok || err != nil {
		t.Fatalf("a base of the same size and rows with another footer: bound %v err %v", ok, err)
	}
	raw[len(raw)-1] ^= 1
	if err := os.WriteFile(PathFor(a), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	if ok, err := Bound(a); ok || !errors.Is(err, errCorrupt) {
		t.Fatalf("damaged: %v %v", ok, err)
	}
}

// EnsureBeside, for a base that was not just written: builds when the
// index is absent or not bound, keeps a bound one, sweeps a leftover .tmp.
func TestEnsureBeside(t *testing.T) {
	base := writeBase(t, "SELECT i::INTEGER AS id FROM range(100) r(i)", 1000)
	tmp := PathFor(base) + ".tmp"
	if err := os.WriteFile(tmp, []byte("left by a crash"), 0o644); err != nil {
		t.Fatal(err)
	}
	builtN := 0
	BuildHook = func() { builtN++ }
	t.Cleanup(func() { BuildHook = nil })
	ctx := context.Background()
	if res := EnsureBeside(ctx, "s", "t", base, []string{"id"}, duckdbutil.Tuning{}); res.Records != 100 || builtN != 1 {
		t.Fatalf("absent: res %+v builds %d", res, builtN)
	}
	if _, err := os.Stat(tmp); err == nil {
		t.Fatal("the leftover .tmp survived")
	}
	logs := captureDebug(t)
	if res := EnsureBeside(ctx, "s", "t", base, []string{"id"}, duckdbutil.Tuning{}); builtN != 1 || res.Records != 0 {
		t.Fatalf("bound: res %+v builds %d; want no build", res, builtN)
	}
	if !strings.Contains(logs.String(), `level=DEBUG msg="key index present"`) {
		t.Fatalf("log:\n%s", logs.String())
	}
	// The base is rewritten (another file of the same rows): the index no
	// longer binds, and is built again.
	other := writeBase(t, "SELECT (99 - i)::INTEGER AS id FROM range(100) r(i)", 1000)
	if err := os.Rename(other, base); err != nil {
		t.Fatal(err)
	}
	logs.Reset()
	if res := EnsureBeside(ctx, "s", "t", base, []string{"id"}, duckdbutil.Tuning{}); builtN != 2 || res.Records != 100 {
		t.Fatalf("unbound: res %+v builds %d", res, builtN)
	}
	if ok, err := Bound(base); !ok || err != nil {
		t.Fatalf("after the rebuild: %v %v", ok, err)
	}
	// A damaged index is said, and built again.
	raw, _ := os.ReadFile(PathFor(base))
	raw[len(raw)-1] ^= 1
	os.WriteFile(PathFor(base), raw, 0o644)
	logs.Reset()
	if res := EnsureBeside(ctx, "s", "t", base, []string{"id"}, duckdbutil.Tuning{}); builtN != 3 || res.Records != 100 {
		t.Fatalf("damaged: res %+v builds %d", res, builtN)
	}
	if !strings.Contains(logs.String(), `level=WARN msg="key index beside an existing file is not usable; building it again"`) {
		t.Fatalf("log:\n%s", logs.String())
	}
}

// A routing-only tuning (S3Direct, no caps) is not a budget: the build
// takes the session's default, so the sort has a memory cap to spill under.
func TestBuild_routingOnlyTuningTakesTheDefaultBudget(t *testing.T) {
	ctx := duckdbutil.WithSessionTuning(context.Background(), duckdbutil.Tuning{Threads: 1, MemoryLimit: "1GB"})
	if got := buildTuning(ctx, duckdbutil.Tuning{S3Direct: true}); got != (duckdbutil.Tuning{Threads: 1, MemoryLimit: "1GB"}) {
		t.Fatalf("S3Direct alone gave %+v", got)
	}
	if got := buildTuning(ctx, duckdbutil.Tuning{}); got != (duckdbutil.Tuning{Threads: 1, MemoryLimit: "1GB"}) {
		t.Fatalf("zero gave %+v", got)
	}
	if got := buildTuning(ctx, duckdbutil.Tuning{Threads: 3}); got != (duckdbutil.Tuning{Threads: 3}) {
		t.Fatalf("a real tuning gave %+v", got)
	}
}
