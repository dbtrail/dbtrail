package consoleapp

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
)

// #1681: the daemon's prune loop applies each server's keep-newest count to
// its local folder when it has no external destination, with or without an
// age retention, and never where the folder is also another rule's.

// makeLocalSnapshots writes n complete snapshots, days apart and older than a
// day, into dir, returning their names oldest first.
func makeLocalSnapshots(t *testing.T, dir string, n int) []string {
	t.Helper()
	var names []string
	for i := n; i >= 1; i-- {
		ts := time.Now().UTC().Add(-time.Duration(i) * 48 * time.Hour).Truncate(time.Second)
		name := strings.ReplaceAll(ts.Format(time.RFC3339), ":", "-")
		snap := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Join(snap, "shop"), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(snap, "shop", "orders.parquet"), []byte("rows"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := baseline.WriteSuccessMarker(snap); err != nil {
			t.Fatal(err)
		}
		names = append(names, name)
	}
	return names
}

func snapshotsIn(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range entries {
		if e.IsDir() && !strings.HasPrefix(e.Name(), ".") {
			out = append(out, e.Name())
		}
	}
	sort.Strings(out)
	return out
}

// With no age retention set, a local-only server with a count is pruned to
// that count, for real, and one with no count keeps everything.
func TestBaselinePruneSweep_keepNewestRunsWithoutARetention(t *testing.T) {
	reg := liveRegistry(t)
	counted, uncounted := t.TempDir(), t.TempDir()
	if _, err := reg.Add(console.ServerEntry{Name: "new", DSN: "u:p@tcp(h:3306)/a", BaselineDir: counted, LocalKeepNewest: 2}); err != nil {
		t.Fatal(err)
	}
	if _, err := reg.Add(console.ServerEntry{Name: "old", DSN: "u:p@tcp(h:3306)/b", BaselineDir: uncounted}); err != nil {
		t.Fatal(err)
	}
	c := makeLocalSnapshots(t, counted, 5)
	u := makeLocalSnapshots(t, uncounted, 5)

	baselinePruneSweep(context.Background(), reg, "", "", "", baseline.PruneLocal)

	if got := snapshotsIn(t, counted); strings.Join(got, ",") != strings.Join(c[3:], ",") {
		t.Errorf("counted folder = %v, want the newest two %v", got, c[3:])
	}
	if got := snapshotsIn(t, uncounted); len(got) != len(u) {
		t.Errorf("a server with no count lost snapshots: %v", got)
	}
	rec, ok, err := baseline.ReadLastPrune(counted)
	if err != nil || !ok || rec.Removed != 3 {
		t.Errorf("last prune = %+v ok=%v err=%v, want 3 removed", rec, ok, err)
	}
}

// A server that gains an external destination stops being pruned to its count
// (that mode removes only what the destination confirmed); losing it again
// resumes the count. Real files, real prune, across three sweeps.
func TestBaselinePruneSweep_switchingTheDestinationSwitchesTheRule(t *testing.T) {
	reg := liveRegistry(t)
	dir := t.TempDir()
	e, err := reg.Add(console.ServerEntry{Name: "s", DSN: "u:p@tcp(h:3306)/a", BaselineDir: dir, LocalKeepNewest: 1})
	if err != nil {
		t.Fatal(err)
	}
	names := makeLocalSnapshots(t, dir, 4)

	// Destination on, no age retention: nothing prunes this folder at all.
	e.BaselineS3 = "s3://bucket/prefix/"
	if err := reg.Update(e); err != nil {
		t.Fatal(err)
	}
	var calls []baseline.PruneOptions
	record := func(ctx context.Context, o baseline.PruneOptions) (baseline.PruneResult, error) {
		calls = append(calls, o)
		return baseline.PruneLocal(ctx, o)
	}
	baselinePruneSweep(context.Background(), reg, "", "", "", record)
	if len(calls) != 0 {
		t.Fatalf("with a destination and no retention, prune ran: %+v", calls)
	}
	// Destination on, age retention on: the S3 rule, never the count.
	calls = nil
	fake := func(_ context.Context, o baseline.PruneOptions) (baseline.PruneResult, error) {
		calls = append(calls, o)
		return baseline.PruneResult{}, nil
	}
	baselinePruneSweep(context.Background(), reg, "", "", "7d", fake)
	if len(calls) != 1 || calls[0].S3URL == "" || calls[0].KeepNewest != 0 {
		t.Fatalf("with a destination the S3 rule applies, got %+v", calls)
	}
	if got := snapshotsIn(t, dir); len(got) != 4 {
		t.Fatalf("snapshots removed while a destination was set: %v", got)
	}
	// Destination off again: the count applies on the next sweep.
	e.BaselineS3 = ""
	if err := reg.Update(e); err != nil {
		t.Fatal(err)
	}
	baselinePruneSweep(context.Background(), reg, "", "", "", baseline.PruneLocal)
	if got := snapshotsIn(t, dir); len(got) != 1 || got[0] != names[3] {
		t.Fatalf("after the destination was removed: %v, want [%s]", got, names[3])
	}
}

// A folder two servers share, or the daemon's own --baseline-dir (also under
// another spelling, through a symlink), is never pruned to a count: a
// snapshot does not say which server wrote it, so one server's newer copy of
// a table would count as the other's newest.
func TestLocalKeepPruneTargets_sharedFoldersAreNeverCounted(t *testing.T) {
	root := t.TempDir()
	global := filepath.Join(root, "global")
	if err := os.MkdirAll(global, 0o700); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(root, "alias")
	if err := os.Symlink(global, alias); err != nil {
		t.Fatal(err)
	}
	entries := []console.ServerEntry{
		{Name: "a", BaselineDir: "/shared-with-s3", LocalKeepNewest: 2},
		{Name: "b", BaselineDir: "/shared-with-s3", BaselineS3: "s3://b/p/"},
		{Name: "c", BaselineDir: alias, LocalKeepNewest: 2},
		{Name: "e", BaselineDir: "/two/", LocalKeepNewest: 5},
		{Name: "d", BaselineDir: "/two", LocalKeepNewest: 2},
		{Name: "f", BaselineDir: "/with-a-keeper", LocalKeepNewest: 2},
		{Name: "g", BaselineDir: "/with-a-keeper"},
		{Name: "h", BaselineDir: "/solo", LocalKeepNewest: 3},
		{Name: "i", BaselineDir: "/counted-with-s3", BaselineS3: "s3://b/q/", LocalKeepNewest: 3},
		{Name: "j", BaselineDir: "/zero", LocalKeepNewest: 0},
	}
	got := map[string]int{}
	for _, tgt := range localKeepPruneTargets(entries, global) {
		if tgt.s3 != "" {
			t.Errorf("a local-only target carries a destination: %+v", tgt)
		}
		got[tgt.dir] = tgt.keepNewest
	}
	if len(got) != 1 || got["/solo"] != 3 {
		t.Fatalf("targets = %v, want only /solo keeping 3", got)
	}
	for _, e := range entries[:7] {
		if !console.LocalKeepBlocked(entries, e, global) {
			t.Errorf("%s: its folder is shared or the daemon's, but it is not reported blocked", e.Name)
		}
	}
	if console.LocalKeepBlocked(entries, entries[7], global) {
		t.Error("a folder of its own is reported blocked")
	}
}

// The loop starts whenever there is a registry: counts are per server and can
// be saved at any time. Without a registry it still needs a retention.
func TestStartBaselinePruneLoop_startsForARegistryWithoutARetention(t *testing.T) {
	if !pruneLoopStarts(liveRegistry(t), 0) {
		t.Error("a registry with no retention must start the loop")
	}
	if pruneLoopStarts(nil, 0) {
		t.Error("no registry and no retention must not start the loop")
	}
	if !pruneLoopStarts(nil, time.Hour) {
		t.Error("a retention alone must start the loop")
	}
}

// The console is told the prune loop runs exactly when it does, so the
// listing's retention line follows the loop (#1681).
func TestUpConsoleConfig_localPruneLoopFollowsTheLoopGate(t *testing.T) {
	prev := upConsoleBaselineRetain
	t.Cleanup(func() { upConsoleBaselineRetain = prev })
	upConsoleBaselineRetain = ""
	opts := consoleOpts{Listen: "127.0.0.1:8090", Token: "tok"}
	cfg, err := upConsoleConfig(nil, "user:pass@tcp(127.0.0.1:3306)/binlog_index", opts, liveRegistry(t))
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.LocalPruneLoop {
		t.Error("a registry and no retention: the loop runs, the console must be told")
	}
	if !cfg.MayCreateFolders {
		t.Error("watch takes the snapshots, so it must be allowed to create their folders")
	}
	cfg, err = upConsoleConfig(nil, "user:pass@tcp(127.0.0.1:3306)/binlog_index", opts, nil)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.LocalPruneLoop {
		t.Error("no registry and no retention: no loop, the console must not claim one")
	}
}

// A folder that stopped being shared while it still holds the other server's
// snapshots is not a prune target: the daemon's sweep never counts them as
// the remaining server's copies. Through the real registry, so the flag the
// sweep reads is the one a delete sets.
func TestLocalKeepPruneTargets_aFolderThatWasSharedStaysUncounted(t *testing.T) {
	state := t.TempDir()
	reg, err := console.LoadRegistry(filepath.Join(state, "console-servers.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	shared := filepath.Join(state, "shared")
	if _, err := reg.Add(console.ServerEntry{Name: "a", DSN: "u:p@tcp(h:3306)/a", BaselineDir: shared, LocalKeepNewest: 1}); err != nil {
		t.Fatal(err)
	}
	b, err := reg.Add(console.ServerEntry{Name: "b", DSN: "u:p@tcp(h:3306)/b", BaselineDir: shared})
	if err != nil {
		t.Fatal(err)
	}
	names := makeLocalSnapshots(t, shared, 3)
	if err := reg.Delete(b.ID); err != nil {
		t.Fatal(err)
	}
	if got := localKeepPruneTargets(reg.List(), ""); len(got) != 0 {
		t.Fatalf("targets = %+v, want none: the folder still holds b's snapshots", got)
	}
	baselinePruneSweep(context.Background(), reg, "", "", "", baseline.PruneLocal)
	if got := snapshotsIn(t, shared); len(got) != len(names) {
		t.Fatalf("the sweep removed snapshots from a folder that held another server's: %v", got)
	}
}
