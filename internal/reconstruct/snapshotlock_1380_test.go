package reconstruct

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/baselineintegrity"
	"github.com/dbtrail/dbtrail/internal/query"
)

// #1380: a snapshot updated from the recorded changes reads no source, so how
// its rows were locked is what the snapshot it was built from says. A torn
// one stays torn, one with no record stays with no record, and nothing on
// this path can turn either into a consistent one.

func TestSnapshotFileMetadata_inheritsTheLock(t *testing.T) {
	at := time.Date(2026, 5, 1, 3, 0, 0, 0, time.UTC)
	for _, c := range []struct {
		name, from string
		want       baseline.ReadConsistency
	}{
		{"built on a snapshot read with no locks", "no-lock", baseline.ReadTorn},
		{"built on a locked one", "ftwrl", baseline.ReadConsistent},
		{"built on a postgres one", baseline.LockStampPGRepeatableRead, baseline.ReadConsistent},
		{"built on one written before the record", "", baseline.ReadUnknown},
		// A value from a later version travels as it is, and reads unknown
		// here: this fold must not decide what it does not understand.
		{"built on a record this program cannot read", "locked-v2", baseline.ReadUnknown},
	} {
		in := mergeInput{Schema: "s", Table: "t", SnapshotAt: at}
		in.SourceBaseline.Metadata = baseline.DumpMetadata{LockMode: c.from}
		md := snapshotFileMetadata(in)
		got, has := md[baseline.MetaKeyLockMode]
		if got != c.from || has != (c.from != "") {
			t.Errorf("%s: the footer records %q (present %v), want %q", c.name, got, has, c.from)
		}
		if lock := baseline.ReadConsistencyOfStamp(got); lock != c.want {
			t.Errorf("%s: reads %s, want %s", c.name, lock, c.want)
		}
	}
}

// writeLockedBaseline is writeZooBaseline at a snapshot path, with a lock
// record (none when stamp is empty) and the manifest a carry forward checks.
func writeLockedBaseline(t *testing.T, root, stamp, table, lock string) string {
	t.Helper()
	snapDir := filepath.Join(root, stamp)
	path := filepath.Join(snapDir, "mydb", table+".parquet")
	md := map[string]string{
		baseline.MetaKeyCreateTableSQL: zooCreateTableSQL,
		baseline.MetaKeyBinlogFile:     "binlog.000007",
		baseline.MetaKeyBinlogPos:      "4",
	}
	if lock != "" {
		md[baseline.MetaKeyLockMode] = lock
	}
	w, err := baseline.NewWriter(path, zooColumns(t), baseline.WriterConfig{Compression: "none", RowGroupSize: 10, Metadata: md})
	if err != nil {
		t.Fatalf("baseline.NewWriter: %v", err)
	}
	rows, nulls := zooRows()
	for i, r := range rows {
		if err := w.WriteRow(r, nulls[i]); err != nil {
			t.Fatalf("WriteRow %d: %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := baselineintegrity.WriteManifest(snapDir); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	return path
}

// foldOnto rewrites one table into snapDir from src, through the real writer.
func foldOnto(t *testing.T, src, snapDir, table string, at time.Time) string {
	t.Helper()
	srcMeta, err := baseline.ReadParquetMetadata(src)
	if err != nil {
		t.Fatalf("read source footer: %v", err)
	}
	in := mergeInput{
		LocalBaselinePath: src,
		CreateTableSQL:    zooCreateTableSQL,
		Schema:            "mydb",
		Table:             table,
		PKCols:            pkColsIntID(),
		Changes:           map[string]*query.ResultRow{},
		SnapshotDir:       snapDir,
		SnapshotAt:        at,
		SourceBaseline:    baselineMeta{Path: src, Time: at.Add(-time.Hour), Metadata: srcMeta},
	}
	if err := mergeBaselineIntoParquet(t.Context(), in, &TableReport{Schema: "mydb", Table: table}); err != nil {
		t.Fatalf("mergeBaselineIntoParquet: %v", err)
	}
	return filepath.Join(snapDir, "mydb", table+".parquet")
}

func lockOfFile(t *testing.T, path string) baseline.ReadConsistency {
	t.Helper()
	md, err := baseline.ReadParquetMetadata(path)
	if err != nil {
		t.Fatalf("read the footer of %s: %v", path, err)
	}
	return baseline.ReadConsistencyOf(md)
}

// fileState is what must not change in a published file.
type fileState struct {
	bytes []byte
	mod   time.Time
	size  int64
}

func stateOf(t *testing.T, path string) fileState {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return fileState{bytes: b, mod: fi.ModTime(), size: fi.Size()}
}

func (s fileState) same(o fileState) bool {
	return s.size == o.size && s.mod.Equal(o.mod) && bytes.Equal(s.bytes, o.bytes)
}

// Through the real writer and the real footer reader, two folds deep: the
// record is inherited at every step, and the snapshot folded from is left as
// it was published.
func TestFold_inheritsTheLockAndLeavesItsSourceAlone(t *testing.T) {
	at := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
	for _, c := range []struct {
		name, lock string
		want       baseline.ReadConsistency
	}{
		{"read with no locks", "no-lock", baseline.ReadTorn},
		{"read with locks", "lock-all", baseline.ReadConsistent},
		{"no record", "", baseline.ReadUnknown},
	} {
		root := t.TempDir()
		src := writeLockedBaseline(t, root, "2026-06-03T12-00-00Z", "orders", c.lock)
		before := stateOf(t, src)

		first := foldOnto(t, src, filepath.Join(root, "2026-06-10T12-00-00Z"), "orders", at)
		if got := lockOfFile(t, first); got != c.want {
			t.Errorf("%s: the snapshot built from it reads %s, want %s", c.name, got, c.want)
		}
		second := foldOnto(t, first, filepath.Join(root, "2026-06-11T12-00-00Z"), "orders", at.Add(24*time.Hour))
		if got := lockOfFile(t, second); got != c.want {
			t.Errorf("%s: the second snapshot built from it reads %s, want %s", c.name, got, c.want)
		}
		if !before.same(stateOf(t, src)) {
			t.Errorf("%s: the snapshot that was folded from changed on disk", c.name)
		}
		if got := lockOfFile(t, src); got != c.want {
			t.Errorf("%s: the snapshot that was folded from now reads %s", c.name, got)
		}
	}
}

// A table that did not change is carried forward as the older snapshot's own
// file. Its footer is the older snapshot's, so it says what that snapshot
// said, and the older snapshot is not touched to make it say so.
func TestCarryForward_keepsTheLockOfTheFileItCarries(t *testing.T) {
	for _, c := range []struct {
		name, lock string
		want       baseline.ReadConsistency
	}{
		{"read with no locks", "no-lock", baseline.ReadTorn},
		{"read with locks", "ftwrl", baseline.ReadConsistent},
		{"no record", "", baseline.ReadUnknown},
	} {
		for _, how := range []string{"link", "copy"} {
			root := t.TempDir()
			src := writeLockedBaseline(t, root, "2026-06-03T12-00-00Z", "orders", c.lock)
			before := stateOf(t, src)
			if how == "copy" {
				prev := linkFile
				linkFile = func(string, string) error { return os.ErrPermission }
				t.Cleanup(func() { linkFile = prev })
			}
			newDir := filepath.Join(root, "2026-06-10T12-00-00Z")
			linked, err := carryForward(context.Background(), src, newDir, "mydb", "orders")
			if how == "copy" {
				linkFile = os.Link
			}
			if err != nil {
				t.Fatalf("carryForward: %v", err)
			}
			if linked != (how == "link") {
				t.Fatalf("%s by %s: linked = %v", c.name, how, linked)
			}
			if got := lockOfFile(t, filepath.Join(newDir, "mydb", "orders.parquet")); got != c.want {
				t.Errorf("%s, carried by %s: reads %s, want %s", c.name, how, got, c.want)
			}
			if !before.same(stateOf(t, src)) {
				t.Errorf("%s, carried by %s: the older snapshot changed on disk", c.name, how)
			}
		}
	}
}

// One snapshot, tables from two older ones: orders carried from a snapshot
// read with no locks, users folded from a locked one. Each table says what
// its own rows descend from, and the snapshot as a whole is the worst.
func TestSnapshot_ofTwoSourcesIsTheWorstOfThem(t *testing.T) {
	root := t.TempDir()
	torn := writeLockedBaseline(t, root, "2026-06-01T12-00-00Z", "orders", "no-lock")
	locked := writeLockedBaseline(t, root, "2026-06-03T12-00-00Z", "users", "ftwrl")
	newDir := filepath.Join(root, "2026-06-10T12-00-00Z")
	if _, err := carryForward(context.Background(), torn, newDir, "mydb", "orders"); err != nil {
		t.Fatalf("carryForward: %v", err)
	}
	foldOnto(t, locked, newDir, "users", time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC))

	orders := lockOfFile(t, filepath.Join(newDir, "mydb", "orders.parquet"))
	users := lockOfFile(t, filepath.Join(newDir, "mydb", "users.parquet"))
	if orders != baseline.ReadTorn || users != baseline.ReadConsistent {
		t.Fatalf("orders reads %s and users %s; want torn and consistent", orders, users)
	}
	if got := baseline.WorstReadConsistency(orders, users); got != baseline.ReadTorn {
		t.Fatalf("the snapshot reads %s, want torn", got)
	}
	if got := baseline.WorstReadConsistency(users, orders); got != baseline.ReadTorn {
		t.Fatalf("the snapshot reads %s in the other order, want torn", got)
	}
}
