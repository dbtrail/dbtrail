package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/views"
)

// baselineDirWithTwoSnapshots builds a root holding an older and a newer
// snapshot of the same table, and returns the root plus both directory names
// (older first). No pointer: each test publishes the one it wants.
func baselineDirWithTwoSnapshots(t *testing.T) (root, older, newer string) {
	t.Helper()
	root = t.TempDir()
	older, newer = "2026-06-09T12-00-00Z", "2026-06-10T12-00-00Z"
	for _, s := range []string{older, newer} {
		p := filepath.Join(root, s, "shop", "orders.parquet")
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root, older, newer
}

// runViewsOverBaselines runs the command with only a baseline root configured
// and returns the generated SQL.
//
// "Only" is literal now (#1552). This used to pass a throwaway --archive-dir,
// because runViews refused a file with no archive source even when the file
// defined no view that reads one — so the helper's own name was wrong, and the
// dummy quietly gave these tests an archive source the scenario never had.
func runViewsOverBaselines(t *testing.T, root string, pin bool) string {
	t.Helper()
	saveViewsFlags(t)
	vIndexDSN, vArchiveDir, vArchiveS3, vBaselineS3 = "", "", "", ""
	vBintrailID, vBaselineDir, vOut = "", root, "-"
	vNoBaselines, vIncludeLive, vIncludeEvents, vPinSnapshot = false, false, false, pin
	out, err := runViewsToString(t)
	if err != nil {
		t.Fatalf("runViews: %v", err)
	}
	return out
}

func statePath(t *testing.T, sql string) string {
	t.Helper()
	for _, line := range strings.Split(sql, "\n") {
		if i := strings.Index(line, "read_parquet('"); i >= 0 {
			rest := line[i+len("read_parquet('"):]
			return rest[:strings.Index(rest, "'")]
		}
	}
	t.Fatalf("no state view in:\n%s", sql)
	return ""
}

// TestRunViews_stateViewsFollowTheCurrentPointer is THE assertion for #1484:
// by default a generated file names the pointer, so a snapshot published after
// it was written reaches it. Driven through runViews rather than the
// generator, because what can silently regress is the command layer never
// rewriting the paths at all.
func TestRunViews_stateViewsFollowTheCurrentPointer(t *testing.T) {
	root, _, newer := baselineDirWithTwoSnapshots(t)
	if err := baseline.PublishCurrentPointer(filepath.Join(root, newer)); err != nil {
		t.Fatal(err)
	}

	sql := runViewsOverBaselines(t, root, false)
	// A following view reads the table's file and, through globs beside it,
	// any chain of deltas (#1733), so the file is not the first read_parquet.
	want := filepath.Join(root, baseline.CurrentLinkName, "shop", "orders.parquet")
	if !strings.Contains(sql, "read_parquet('"+want+"'") {
		t.Fatalf("the state view does not read the table through the pointer (%q):\n%s", want, sql)
	}
	if strings.Contains(sql, filepath.Join(root, newer)) {
		t.Fatalf("the state view names the snapshot directory instead of the pointer:\n%s", sql)
	}
	if !strings.Contains(sql, "views follow the `"+baseline.CurrentLinkName+"` pointer") {
		t.Fatal("the file does not say it follows the pointer")
	}
	// And it must not keep the old promise, which is now false.
	if strings.Contains(sql, "stays bound to the snapshot it was generated against") {
		t.Fatal("the file still warns that its state views stop changing")
	}
}

// TestRunViews_pinSnapshotBindsToTheSnapshot covers the opt-out: an operator
// who wants a fixed point in time gets a path that cannot move under them.
func TestRunViews_pinSnapshotBindsToTheSnapshot(t *testing.T) {
	root, _, newer := baselineDirWithTwoSnapshots(t)
	if err := baseline.PublishCurrentPointer(filepath.Join(root, newer)); err != nil {
		t.Fatal(err)
	}

	sql := runViewsOverBaselines(t, root, true)
	got := statePath(t, sql)
	if !strings.Contains(got, newer) {
		t.Fatalf("--pin-snapshot produced %q, want the snapshot directory %q", got, newer)
	}
	if strings.Contains(got, baseline.CurrentLinkName) {
		t.Fatalf("--pin-snapshot still went through the pointer: %q", got)
	}
	if strings.Contains(sql, "follow the `"+baseline.CurrentLinkName+"` pointer") {
		t.Fatal("a pinned file claims to follow the pointer")
	}
}

// TestRunViews_doesNotFollowAPointerNamingAnotherSnapshot is the honesty
// guard. A pointer left behind at an older snapshot (a root written by a build
// that did not maintain it, say) must not be followed: the header describes the
// newest snapshot's tables and column types, so reading rows from a different
// one would make the file lie about its own contents.
func TestRunViews_doesNotFollowAPointerNamingAnotherSnapshot(t *testing.T) {
	root, older, newer := baselineDirWithTwoSnapshots(t)
	if err := os.Symlink(older, filepath.Join(root, baseline.CurrentLinkName)); err != nil {
		t.Fatal(err)
	}

	sql := runViewsOverBaselines(t, root, false)
	got := statePath(t, sql)
	if !strings.Contains(got, newer) {
		t.Fatalf("state view reads %q, want the newest snapshot %q", got, newer)
	}
	if strings.Contains(sql, "follow the `"+baseline.CurrentLinkName+"` pointer") {
		t.Fatal("followed a pointer that names a different snapshot")
	}
}

// TestRunViews_doesNotFollowWhenTheRootHasNoPointer covers every root that
// exists today: none carries a pointer until its next snapshot completes. The
// file must be exactly what it was before this feature, with no dangling path
// and no claim it cannot keep.
func TestRunViews_doesNotFollowWhenTheRootHasNoPointer(t *testing.T) {
	root, _, newer := baselineDirWithTwoSnapshots(t)

	sql := runViewsOverBaselines(t, root, false)
	got := statePath(t, sql)
	if !strings.Contains(got, newer) {
		t.Fatalf("state view reads %q, want the newest snapshot %q", got, newer)
	}
	if strings.Contains(sql, baseline.CurrentLinkName+"/shop") {
		t.Fatalf("a path through a nonexistent pointer was emitted: %q", got)
	}
	if !strings.Contains(sql, "stays bound to the snapshot it was generated against") {
		t.Fatal("a pinned file dropped the warning that its rows stop changing")
	}
}

// TestApplyFollow_s3FollowsByMarkerNotByPointer pins WHICH mechanism an S3
// baseline root gets (#1550).
//
// Never the pointer, and the original reason still holds: there is no pointer
// object in S3, and publishing one would mean copying every table to a second
// prefix, which is not atomic across tables, so a query could read half of one
// snapshot and half of another. What changed is that refusing the pointer is no
// longer the end of it — the views select the newest marked snapshot when they
// are READ, which needs no second prefix and no rewritten path.
//
// The path staying put is half the assertion: under FollowNewest the following
// lives in the emitted SQL, so a rewritten path here would mean two mechanisms
// were applied to one table.
func TestApplyFollow_s3FollowsByMarkerNotByPointer(t *testing.T) {
	in := &views.Input{
		BaselineSource: "s3://bucket/baselines/",
		Baselines: []views.BaselineTable{
			{Schema: "shop", Table: "orders", Path: "s3://bucket/baselines/2026-06-10T12-00-00Z/shop/orders.parquet"},
		},
	}
	views.ApplyFollow(in, in.BaselineSource, false)
	if in.Follow != views.FollowNewest {
		t.Fatalf("an S3 baseline root got Follow=%v, want FollowNewest", in.Follow)
	}
	if !strings.Contains(in.Baselines[0].Path, "2026-06-10T12-00-00Z") {
		t.Fatalf("S3 path was rewritten: %q", in.Baselines[0].Path)
	}
	if strings.Contains(in.Baselines[0].Path, baseline.CurrentLinkName) {
		t.Fatalf("S3 path was pointed at a pointer that cannot exist: %q", in.Baselines[0].Path)
	}
	if got := in.Baselines[0].Rel; got != "shop/orders.parquet" {
		t.Errorf("Rel is %q, want the path below the snapshot directory", got)
	}
}

// TestApplyFollow_pinRefusesEveryMechanism keeps --pin-snapshot ahead of the
// mechanism choice. It is the operator asking for today's rows to stay today's,
// and an answer of "not the pointer, so the marker instead" would honour the
// letter of the refusal while following anyway.
func TestApplyFollow_pinRefusesEveryMechanism(t *testing.T) {
	in := &views.Input{
		BaselineSource: "s3://bucket/baselines/",
		Baselines: []views.BaselineTable{
			{Schema: "shop", Table: "orders", Path: "s3://bucket/baselines/2026-06-10T12-00-00Z/shop/orders.parquet"},
		},
	}
	views.ApplyFollow(in, in.BaselineSource, true)
	if in.Follow != views.FollowNone {
		t.Fatalf("--pin-snapshot still followed, with Follow=%v", in.Follow)
	}
	if in.Baselines[0].Rel != "" {
		t.Errorf("a pinned table carries Rel=%q; nothing should have been prepared to follow with",
			in.Baselines[0].Rel)
	}
}
