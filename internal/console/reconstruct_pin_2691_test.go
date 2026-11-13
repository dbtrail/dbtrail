package console

import (
	"bytes"
	"context"
	"log/slog"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// pinFixtureSnapshot writes <root>/<name>/shop/orders.parquet and _SUCCESS:
// a complete snapshot as far as the cleanup and the lookup are concerned.
func pinFixtureSnapshot(t *testing.T, root, name string) string {
	t.Helper()
	p := filepath.Join(root, name, "shop", "orders.parquet")
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte("not parquet"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := baseline.WriteSuccessMarker(filepath.Join(root, name)); err != nil {
		t.Fatal(err)
	}
	return p
}

// withPruneRule makes every server's folder pruned to keep, for the test.
func withPruneRule(t *testing.T, keep int, seen *[]string) {
	t.Helper()
	prev := pruneRule
	t.Cleanup(func() { pruneRule = prev })
	pruneRule = func(_ *Server, id string) (int, time.Duration) {
		if seen != nil {
			*seen = append(*seen, id)
		}
		return keep, 0
	}
}

// cleanupInsidePin wraps pinBaselineRead so that a real cleanup to a count of
// 1 runs right after the console's own pin is taken: what happens when a
// snapshot is published while the read is on its files. It records the
// cleanup's result and the readers that pinned.
func cleanupInsidePin(t *testing.T, root string) (results *[]baseline.PruneResult, readers *[]string) {
	t.Helper()
	prev := pinBaselineRead
	t.Cleanup(func() { pinBaselineRead = prev })
	results, readers = &[]baseline.PruneResult{}, &[]string{}
	pinBaselineRead = func(s *Server, id string) reconstruct.PinFunc {
		real := prev(s, id)
		return func(ctx context.Context, path, what string) func() {
			*readers = append(*readers, what)
			release := real(ctx, path, what)
			res, err := baseline.PruneLocal(context.Background(), baseline.PruneOptions{LocalDir: root, KeepNewest: 1})
			if err != nil {
				t.Errorf("cleanup during the read: %v", err)
			}
			*results = append(*results, res)
			return release
		}
	}
	return results, readers
}

// /api/reconstruct keeps the snapshot it reads from a cleanup that runs while
// it reads (#2691), and lets it go when it ends.
func TestReconstruct_aCleanupDuringTheReadKeepsItsSnapshot(t *testing.T) {
	dir := t.TempDir()
	old, newer := "2026-06-01T00-00-00Z", "2026-06-02T00-00-00Z"
	oldFile := pinFixtureSnapshot(t, dir, old)
	pinFixtureSnapshot(t, dir, newer)
	srv := newBaselineServer(t, dir, true)
	srv.cm.boot.resolver = metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{
		"shop.orders": {Schema: "shop", Table: "orders", PKColumns: []string{"id"},
			Columns: []metadata.ColumnMeta{{Name: "id", OrdinalPosition: 1, IsPK: true, DataType: "int"}}},
	})
	withPruneRule(t, 1, nil)
	results, readers := cleanupInsidePin(t, dir)

	rec := httptest.NewRecorder()
	srv.handleReconstruct(rec, httptest.NewRequest("GET", "/api/reconstruct?schema=shop&table=orders&pk=1&at=2026-06-01T12:00:00Z", nil))
	// The file is not Parquet: the read after the pin answers 500. What is
	// tested is the snapshot under it.
	t.Logf("the read answered %d: %s", rec.Code, rec.Body.String())
	if !slices.Equal(*readers, []string{"a time-travel read"}) {
		t.Fatalf("readers that pinned: %v, want one time-travel read: /api/reconstruct reads without a pin", *readers)
	}
	if len(*results) != 1 || (*results)[0].KeptPinned != 1 || len((*results)[0].Pruned) != 0 {
		t.Fatalf("the cleanup during the read: %+v, want the read's snapshot kept as pinned and nothing removed", *results)
	}
	if _, err := os.Stat(oldFile); err != nil {
		t.Fatalf("the snapshot the read was on is gone: %v", err)
	}
	if got := baseline.Pinned(dir); len(got) != 0 {
		t.Fatalf("the read is over and %v is still pinned", got)
	}
	// Released: the next cleanup removes it.
	res, err := baseline.PruneLocal(context.Background(), baseline.PruneOptions{LocalDir: dir, KeepNewest: 1})
	if err != nil || !slices.Equal(res.Pruned, []string{old}) {
		t.Fatalf("the cleanup after the read: pruned %v (err %v), want [%s]", res.Pruned, err, old)
	}
}

// The reads of a past moment count with the downloads (#2691): with the
// downloads at their limit, a read of another old snapshot pins nothing, and
// goes on unpinned as before rather than being refused.
func TestPinBaselineRead_countsWithTheDownloads(t *testing.T) {
	dir := t.TempDir()
	a, b, newest := "2026-06-01T00-00-00Z", "2026-06-02T00-00-00Z", "2026-06-03T00-00-00Z"
	pinFixtureSnapshot(t, dir, a)
	bFile := pinFixtureSnapshot(t, dir, b)
	pinFixtureSnapshot(t, dir, newest)
	srv := newBaselineServer(t, dir, true)
	withPruneRule(t, 1, nil)
	// Count 1: one snapshot past the count may be held. A download takes it.
	release, ok := pinForDownload(dir, a, 1, 0, time.Hour)
	defer release()
	if !ok {
		t.Fatal("the first download past the count was refused")
	}
	r := pinBaselineRead(srv, "")(context.Background(), bFile, "a time-travel read")
	defer r()
	if got := baseline.Pinned(dir); !slices.Equal(got, []string{a}) {
		t.Fatalf("pinned %v, want only the download's %s: the read went past the cap", got, a)
	}
	// A read of the snapshot the download holds is let in: it holds nothing
	// more. Released, the download's pin is still there; the download's
	// release then lets it go, so the read's pin was taken.
	r2 := pinBaselineRead(srv, "")(context.Background(), filepath.Join(dir, a, "shop", "orders.parquet"), "a _snapshot query")
	release()
	if got := baseline.Pinned(dir); !slices.Equal(got, []string{a}) {
		t.Fatalf("pinned %v once the download ended, want %s still held by the read: the read of a snapshot a download holds was refused", got, a)
	}
	r2()
	if got := baseline.Pinned(dir); len(got) != 0 {
		t.Fatalf("pinned %v after both ended", got)
	}
}

// A read's pin lasts as long as the reader's own deadline when that is
// later than a download's of its bytes (#2691): a `_snapshot` query reads
// the change log first, which can take minutes on a small table.
func TestReadPinLimit(t *testing.T) {
	if got := readPinLimit(context.Background(), 10<<20); got != downloadPinLimit(10<<20) {
		t.Fatalf("no deadline: %v, want a download's %v", got, downloadPinLimit(10<<20))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if got := readPinLimit(ctx, 0); got < 4*time.Minute || got > 5*time.Minute {
		t.Fatalf("a 5-minute query on a small table: %v, want the query's deadline", got)
	}
	short, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	if got := readPinLimit(short, 100<<20); got != downloadPinLimit(100<<20) {
		t.Fatalf("a short deadline on a big table: %v, want a download's %v", got, downloadPinLimit(100<<20))
	}
}

// The folder comes from the path the lookup answered with, and the rule from
// the server the port target is for (#2691): a lookup that answered from
// another folder pins there, and the count read is that server's.
func TestResolveFlashback_pinsWithItsServersRule(t *testing.T) {
	clearStores(t)
	reg, err := LoadRegistry(filepath.Join(t.TempDir(), "console-servers.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	ent, err := reg.Add(ServerEntry{Name: "srv", DSN: "u:p@tcp(127.0.0.1:1)/idx?timeout=2s", BaselineDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "t", Registry: reg, LazyIndex: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.cm.CloseAll)
	var ids []string
	withPruneRule(t, 3, &ids)
	tgt, err := srv.ResolveFlashback(context.Background(), "srv")
	if err != nil {
		t.Fatal(err)
	}
	if tgt.PinBaseline == nil {
		t.Fatal("the port target carries no pin: `_snapshot` reads unpinned")
	}
	other := t.TempDir()
	p := pinFixtureSnapshot(t, other, "2026-06-01T00-00-00Z")
	release := tgt.PinBaseline(context.Background(), p, "a _snapshot query")
	if got := baseline.Pinned(other); !slices.Equal(got, []string{"2026-06-01T00-00-00Z"}) {
		t.Fatalf("pinned in the lookup's folder: %v, want the snapshot of the path", got)
	}
	release()
	if !slices.Equal(ids, []string{ent.ID}) {
		t.Fatalf("the rule was read for %v, want the target's server %q", ids, ent.ID)
	}
}

// A read past the cap is said in a warning, once every 10 minutes per
// server (#2691): a read that then fails on a removed snapshot has a line
// that explains it, and a client repeating reads does not fill the log.
func TestWarnUnpinnedRead_oncePerServerEveryTenMinutes(t *testing.T) {
	var buf bytes.Buffer
	prevLog, prevNow := slog.Default(), unpinnedReadNow
	t.Cleanup(func() { slog.SetDefault(prevLog); unpinnedReadNow = prevNow })
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	clock := time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC)
	unpinnedReadNow = func() time.Time { return clock }
	a, b := "warn-a-"+t.Name(), "warn-b-"+t.Name()
	warnUnpinnedRead(a, "2026-06-01T00-00-00Z", "a time-travel read")
	clock = clock.Add(9 * time.Minute)
	warnUnpinnedRead(a, "2026-06-01T00-00-00Z", "a _snapshot query")
	warnUnpinnedRead(b, "2026-06-01T00-00-00Z", "the MCP reconstruct tool")
	clock = clock.Add(2 * time.Minute)
	warnUnpinnedRead(a, "2026-06-01T00-00-00Z", "a time-travel read")
	out := buf.String()
	t.Logf("the log:\n%s", out)
	warns := strings.Count(out, "level=WARN")
	if warns != 3 || strings.Count(out, "level=DEBUG") != 1 {
		t.Fatalf("%d warnings and %d debug lines, want 3 (a, b, a after 10 minutes) and 1 (a within them)", warns, strings.Count(out, "level=DEBUG"))
	}
	if !strings.Contains(out, "console: a read of a past moment goes without a pin: downloads and reads of this server's older snapshots hold their limit, so the cleanup may remove its snapshot while it runs and the read then fails; it can be run again (said once every 10 minutes per server)") {
		t.Fatal("the warning's words changed")
	}
}
