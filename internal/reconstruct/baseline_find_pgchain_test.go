package reconstruct

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/pgdelta"
	"github.com/dbtrail/dbtrail/internal/query"
)

// writePGFindBase writes a PostgreSQL table file in the snapshot folder of
// dir, whose footer says its rows were read at read (the zero time: no time
// of its own), as a snapshot update leaves a table file it carried forward.
func writePGFindBase(t *testing.T, root string, dir, read time.Time) string {
	t.Helper()
	path := filepath.Join(root, SnapshotDirName(dir), "public", "orders.parquet")
	md := map[string]string{baseline.MetaKeyLSN: "4096", baseline.MetaKeyPGDeltaSince: read.Add(-time.Minute).Format(time.RFC3339)}
	if !read.IsZero() {
		md[baseline.MetaKeySnapshotTimestamp] = read.Format(time.RFC3339)
	}
	w, err := baseline.NewWriter(path, []baseline.Column{{Name: "id", RawText: true}}, baseline.WriterConfig{Compression: "none", RowGroupSize: 10, Metadata: md})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRow([]string{"1"}, []bool{false}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, SnapshotDirName(dir), baseline.SuccessMarker), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// writePGPair0 puts the names of pair 0 beside the table file at base: what
// FindBaseline looks for is that they are there.
func writePGPair0(t *testing.T, base string, files ...bool) {
	t.Helper()
	dead, rows, err := pgdelta.Paths(base, 0)
	if err != nil {
		t.Fatal(err)
	}
	for i, f := range []string{dead, rows} {
		if len(files) > i && !files[i] {
			continue
		}
		if err := os.WriteFile(f, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// #3082 (the snapshot update writes a chain of pairs beside a PostgreSQL
// table file it carries forward): the time FindBaseline returns for such a
// file is the time its rows were read, from its own footer, not the folder
// it sits in, as it is the start of the chain for a MySQL delta. Every reader
// bounds its TRUNCATE and capture-gap checks from it, and a single-row read
// that applies no change labels its row with it.
func TestFindBaseline_PGChainReturnsTheFileOwnTime(t *testing.T) {
	ctx := context.Background()
	read := time.Date(2026, 11, 22, 9, 0, 0, 0, time.UTC)
	folder := read.Add(3 * time.Hour)
	at := folder.Add(time.Minute)

	t.Run("a chain beside the file: the file's time", func(t *testing.T) {
		root := t.TempDir()
		base := writePGFindBase(t, root, folder, read)
		writePGPair0(t, base)
		_, got, _, err := FindBaseline(ctx, root, "public", "orders", at)
		if err != nil || !got.Equal(read) {
			t.Fatalf("FindBaseline = %s, %v; want the file's own time %s (the folder is %s)", got, err, read, folder)
		}
	})
	t.Run("half of pair 0 is enough to say a chain is there", func(t *testing.T) {
		root := t.TempDir()
		base := writePGFindBase(t, root, folder, read)
		writePGPair0(t, base, false, true)
		if _, got, _, err := FindBaseline(ctx, root, "public", "orders", at); err != nil || !got.Equal(read) {
			t.Fatalf("FindBaseline = %s, %v; want %s", got, err, read)
		}
	})
	t.Run("no chain: the folder's time, as before", func(t *testing.T) {
		root := t.TempDir()
		writePGFindBase(t, root, folder, read)
		if _, got, _, err := FindBaseline(ctx, root, "public", "orders", at); err != nil || !got.Equal(folder) {
			t.Fatalf("FindBaseline = %s, %v; want the folder's time %s", got, err, folder)
		}
	})
	t.Run("a chain beside a file that records no time of its own: refused", func(t *testing.T) {
		root := t.TempDir()
		base := writePGFindBase(t, root, folder, time.Time{})
		writePGPair0(t, base)
		_, _, _, err := FindBaseline(ctx, root, "public", "orders", at)
		if err == nil || !strings.Contains(err.Error(), "full") {
			t.Fatalf("FindBaseline over a chain beside a file with no time: %v; want a refusal that says what to do", err)
		}
	})
	t.Run("a file's time after its folder's is not trusted", func(t *testing.T) {
		root := t.TempDir()
		base := writePGFindBase(t, root, folder, folder.Add(time.Hour))
		writePGPair0(t, base)
		if _, got, _, err := FindBaseline(ctx, root, "public", "orders", at.Add(2*time.Hour)); err != nil || !got.Equal(folder) {
			t.Fatalf("FindBaseline = %s, %v; want the folder's time %s", got, err, folder)
		}
	})
}

// The S3 side asks the store for the two names of pair 0 once per file and
// process (a published snapshot does not change), and a store that cannot be
// asked is an error for a PostgreSQL file, never the folder's time: it would
// put the TRUNCATE and capture-gap checks after changes the file misses.
func TestPGChainStart_S3(t *testing.T) {
	ctx := context.Background()
	read := time.Date(2026, 11, 22, 9, 0, 0, 0, time.UTC)
	prevProbe, prevFooter := pgPair0S3, pgFindFooter
	t.Cleanup(func() { pgPair0S3, pgFindFooter = prevProbe, prevFooter; resetS3PGPair0() })
	asked := 0
	answer, probeErr := true, error(nil)
	pgPair0S3 = func(context.Context, string, string) (bool, error) { asked++; return answer, probeErr }
	pgFindFooter = func(context.Context, string) (baseline.DumpMetadata, error) {
		return baseline.DumpMetadata{LSN: 4096, SnapshotTimestamp: read}, nil
	}
	path := "s3://bucket/snaps/" + SnapshotDirName(read.Add(time.Hour)) + "/public/orders.parquet"
	for range 2 {
		got, err := pgChainStart(ctx, path)
		if err != nil || !got.Equal(read) {
			t.Fatalf("pgChainStart = %s, %v; want %s", got, err, read)
		}
	}
	if asked != 1 {
		t.Fatalf("the store was asked %d times for one file; want once", asked)
	}
	resetS3PGPair0()
	answer = false
	if got, err := pgChainStart(ctx, path); err != nil || !got.IsZero() {
		t.Fatalf("no pair 0 in the store: %s, %v; want no time", got, err)
	}
	// A store that cannot be asked.
	resetS3PGPair0()
	probeErr = errors.New("listing refused")
	if _, err := pgChainStart(ctx, path); err == nil || !strings.Contains(err.Error(), "listing refused") {
		t.Fatalf("a store that cannot be asked, for a PostgreSQL file: %v; want an error", err)
	}
	// ... and for a file that is not a PostgreSQL one, nothing changes.
	pgFindFooter = func(context.Context, string) (baseline.DumpMetadata, error) {
		return baseline.DumpMetadata{BinlogFile: "binlog.000001", BinlogPos: 4}, nil
	}
	if got, err := pgChainStart(ctx, path); err != nil || !got.IsZero() {
		t.Fatalf("a store that cannot be asked, for a MySQL file: %s, %v; want no time and no error", got, err)
	}
	if asked != 4 {
		t.Fatalf("the store was asked %d times; an error is not remembered", asked)
	}
}

// For a MySQL table file nothing FindBaseline returns changes: with no delta
// the folder's time, with one the chain's start (TestFindBaseline_returnsTheChainStart),
// and the PostgreSQL probe is not reached over a MySQL chain at all.
func TestFindBaseline_MySQLTimeUnchanged(t *testing.T) {
	ctx := context.Background()
	noCompaction(t)
	calls := 0
	prev := pgChainStartOf
	pgChainStartOf = func(ctx context.Context, path string) (time.Time, error) { calls++; return prev(ctx, path) }
	t.Cleanup(func() { pgChainStartOf = prev })

	root := t.TempDir()
	t0 := time.Date(2026, 5, 1, 9, 0, 0, 0, time.UTC)
	base := writeStampedBaseline(t, root, t0)
	if _, got, _, err := FindBaseline(ctx, root, "mydb", "orders", t0.Add(time.Hour)); err != nil || !got.Equal(t0) {
		t.Fatalf("a MySQL file with no delta: %s, %v; want its folder's time %s", got, err, t0)
	}
	if calls != 1 {
		t.Fatalf("the PostgreSQL probe ran %d times over a MySQL file with no delta; want once (two names looked for)", calls)
	}
	at := t0.Add(3 * time.Hour)
	nb, _, err := deltaWindow(t, root, base, t0, changeMap(upd(1, "v1")), at, &query.BinlogPos{File: "binlog.000009", Pos: 1000}, nil)
	if err != nil {
		t.Fatal(err)
	}
	calls = 0
	path, got, _, err := FindBaseline(ctx, root, "mydb", "orders", at.Add(time.Hour))
	if err != nil || path != nb || !got.Equal(t0) {
		t.Fatalf("a MySQL file with a delta: %s %s, %v; want %s and the chain's start %s", path, got, err, nb, t0)
	}
	if calls != 0 {
		t.Fatalf("the PostgreSQL probe ran %d times over a MySQL chain; want none", calls)
	}
}
