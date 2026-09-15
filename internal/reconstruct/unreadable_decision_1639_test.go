package reconstruct

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// #1639: the places that DECIDE on a backup (which tables the next fold
// covers, which snapshot a restore anchors on, which file a row is read from)
// used to answer from whatever the folder walk could open. A backup folder the
// walk could not read is only unsafe when it sorts at or after the snapshot
// the decision would pick; one older than the pick changes nothing, and must
// not turn every decision into a refusal.

var (
	t1639a = time.Date(2026, 9, 1, 6, 0, 0, 0, time.UTC)
	t1639b = time.Date(2026, 9, 2, 6, 0, 0, 0, time.UTC)
	t1639c = time.Date(2026, 9, 3, 6, 0, 0, 0, time.UTC)
)

// snapshot1639 lays out <root>/<ts>/<schema>/<table>.parquet for each
// "schema.table" entry and returns the snapshot directory. Listing and lookup
// read names only, so the files can be empty.
func snapshot1639(t *testing.T, root string, ts time.Time, tables ...string) string {
	t.Helper()
	dir := filepath.Join(root, SnapshotDirName(ts))
	for _, e := range tables {
		schema, table, _ := strings.Cut(e, ".")
		if err := os.MkdirAll(filepath.Join(dir, schema), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, schema, table+".parquet"), nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func mustRefuseUnreadable(t *testing.T, err error, dir string) {
	t.Helper()
	if !errors.Is(err, ErrUnreadableSnapshot) {
		t.Fatalf("err = %v, want ErrUnreadableSnapshot", err)
	}
	if errors.Is(err, ErrNoBaseline) {
		t.Fatalf("err = %v also reads as ErrNoBaseline, which callers answer with a binlog-only or other-location fallback", err)
	}
	if !strings.Contains(err.Error(), filepath.Base(dir)) {
		t.Fatalf("err = %v does not name the folder %s", err, filepath.Base(dir))
	}
}

func TestNewestSnapshotTables_newerUnreadableRefuses(t *testing.T) {
	root := t.TempDir()
	snapshot1639(t, root, t1639a, "shop.a")
	newer := snapshot1639(t, root, t1639b, "shop.a", "shop.b")
	unreadable(t, newer)
	tables, err := NewestSnapshotTables(context.Background(), root)
	mustRefuseUnreadable(t, err, newer)
	if tables != nil {
		t.Fatalf("tables = %v alongside a refusal", tables)
	}
}

func TestNewestSnapshotTables_olderUnreadableChangesNothing(t *testing.T) {
	root := t.TempDir()
	unreadable(t, snapshot1639(t, root, t1639a, "shop.old"))
	snapshot1639(t, root, t1639b, "shop.a", "shop.b")
	tables, err := NewestSnapshotTables(context.Background(), root)
	if err != nil || !slices.Equal(tables, []string{"shop.a", "shop.b"}) {
		t.Fatalf("tables, err = %v, %v; want [shop.a shop.b], nil", tables, err)
	}
}

// The only snapshot unreadable: before, an empty readable subset read as "no
// previous backup", and the schedule took a full read of production for a
// cause that did not happen.
func TestNewestSnapshotTables_onlySnapshotUnreadableRefuses(t *testing.T) {
	root := t.TempDir()
	only := snapshot1639(t, root, t1639a, "shop.a")
	unreadable(t, only)
	_, err := NewestSnapshotTables(context.Background(), root)
	mustRefuseUnreadable(t, err, only)
}

// A schema folder inside the newest snapshot: the snapshot is found, but its
// table list is short, and a fold over it would publish a snapshot missing
// that schema.
func TestNewestSnapshotTables_unreadableSchemaInNewestRefuses(t *testing.T) {
	root := t.TempDir()
	dir := snapshot1639(t, root, t1639a, "shop.a", "crm.b")
	unreadable(t, filepath.Join(dir, "crm"))
	_, err := NewestSnapshotTables(context.Background(), root)
	mustRefuseUnreadable(t, err, dir)
}

func TestSnapshotAt_unreadableBetweenAnchorAndTargetRefuses(t *testing.T) {
	root := t.TempDir()
	snapshot1639(t, root, t1639a, "shop.a")
	mid := snapshot1639(t, root, t1639b, "shop.a", "shop.b")
	snapshot1639(t, root, t1639c, "shop.a", "shop.b", "shop.c")
	unreadable(t, mid)

	_, _, err := SnapshotAt(context.Background(), root, t1639b.Add(time.Hour))
	mustRefuseUnreadable(t, err, mid)

	// Older than the pick: the pick is the newest snapshot, and the unreadable
	// one is not a candidate for it.
	tables, anchor, err := SnapshotAt(context.Background(), root, t1639c.Add(time.Hour))
	if err != nil || !anchor.Equal(t1639c) || len(tables) != 3 {
		t.Fatalf("after the newest: tables, anchor, err = %v, %v, %v", tables, anchor, err)
	}
	// Newer than the target: not a candidate either.
	tables, anchor, err = SnapshotAt(context.Background(), root, t1639a.Add(time.Hour))
	if err != nil || !anchor.Equal(t1639a) || !slices.Equal(tables, []string{"shop.a"}) {
		t.Fatalf("before the unreadable one: tables, anchor, err = %v, %v, %v", tables, anchor, err)
	}
	// Exactly at the unreadable snapshot's instant: it is the pick.
	_, _, err = SnapshotAt(context.Background(), root, t1639b)
	mustRefuseUnreadable(t, err, mid)
}

func TestSnapshotAt_everyCandidateUnreadableRefuses(t *testing.T) {
	root := t.TempDir()
	only := snapshot1639(t, root, t1639a, "shop.a")
	snapshot1639(t, root, t1639c, "shop.a")
	unreadable(t, only)
	_, _, err := SnapshotAt(context.Background(), root, t1639b)
	mustRefuseUnreadable(t, err, only)
}

// Time-travel, MCP and _snapshot read one row and have an older answer to
// give: they give it, with a warning naming the folder, instead of the stale
// warning's "the table is absent from the newest snapshot", which would send
// the operator to re-dump a table that is there.
func TestFindBaseline_newerUnreadableWarns(t *testing.T) {
	root := t.TempDir()
	snapshot1639(t, root, t1639a, "shop.a")
	newer := snapshot1639(t, root, t1639b, "shop.a")
	unreadable(t, newer)
	path, at, stale, err := FindBaseline(context.Background(), root, "shop", "a", t1639c)
	if err != nil || !at.Equal(t1639a) || !strings.HasPrefix(path, filepath.Join(root, SnapshotDirName(t1639a))) {
		t.Fatalf("path, at, err = %s, %v, %v; want the older snapshot", path, at, err)
	}
	if !stale.Stale() || !strings.Contains(stale.Message, "could not be read") || !strings.Contains(stale.Message, filepath.Base(newer)) ||
		strings.Contains(stale.Message, "absent") {
		t.Fatalf("stale = %+v", stale)
	}
	if !stale.UsingSnapshot.Equal(t1639a) || !stale.NewestSnapshot.Equal(t1639b) {
		t.Fatalf("stale times = %+v", stale)
	}
}

func TestFindBaseline_olderOrLaterUnreadableDoesNotWarn(t *testing.T) {
	root := t.TempDir()
	unreadable(t, snapshot1639(t, root, t1639a, "shop.a"))
	snapshot1639(t, root, t1639b, "shop.a")
	unreadable(t, snapshot1639(t, root, t1639c, "shop.a"))
	_, at, stale, err := FindBaseline(context.Background(), root, "shop", "a", t1639b.Add(time.Hour))
	if err != nil || !at.Equal(t1639b) || stale.Stale() {
		t.Fatalf("at, stale, err = %v, %+v, %v; want the readable snapshot, no warning", at, stale, err)
	}
}

// Nothing readable to answer with. ErrNoBaseline would be the wrong verdict:
// the full-table reconstruct answers it with a binlog-only table and the
// console with its other location, both silently.
func TestFindBaseline_onlyCandidateUnreadableRefuses(t *testing.T) {
	root := t.TempDir()
	only := snapshot1639(t, root, t1639a, "shop.a")
	unreadable(t, only)
	_, _, _, err := FindBaseline(context.Background(), root, "shop", "a", t1639b)
	mustRefuseUnreadable(t, err, only)
}

// The warning that existed keeps its words when the newer snapshot is
// readable and simply lacks the table.
func TestFindBaseline_absentFromReadableNewerKeepsStaleWarning(t *testing.T) {
	root := t.TempDir()
	snapshot1639(t, root, t1639a, "shop.a")
	snapshot1639(t, root, t1639b, "shop.other")
	_, _, stale, err := FindBaseline(context.Background(), root, "shop", "a", t1639c)
	if err != nil || !strings.Contains(stale.Message, "absent from the newest snapshot") {
		t.Fatalf("stale, err = %+v, %v", stale, err)
	}
}

// A genuinely missing table in an unreadable-free tree is still ErrNoBaseline.
func TestFindBaseline_missingTableIsStillNoBaseline(t *testing.T) {
	root := t.TempDir()
	snapshot1639(t, root, t1639a, "shop.other")
	_, _, _, err := FindBaseline(context.Background(), root, "shop", "a", t1639b)
	if !errors.Is(err, ErrNoBaseline) || errors.Is(err, ErrUnreadableSnapshot) {
		t.Fatalf("err = %v", err)
	}
}

// A file where a schema folder would be is absence, not an unreadable folder:
// the stat fails with ENOTDIR, and a refusal would disable the binlog-only
// and other-location fallbacks for a table that simply is not there.
func TestFindBaseline_fileInPlaceOfSchemaFolderIsAbsence(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, SnapshotDirName(t1639a))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "shop"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, _, _, err := FindBaseline(context.Background(), root, "shop", "a", t1639b)
	if !errors.Is(err, ErrNoBaseline) || errors.Is(err, ErrUnreadableSnapshot) {
		t.Fatalf("err = %v, want ErrNoBaseline", err)
	}
}

// The warning for an older pick behind an unreadable folder is marked, so a
// caller with a second location knows to ask it.
func TestFindBaseline_unreadableWarningIsMarked(t *testing.T) {
	root := t.TempDir()
	snapshot1639(t, root, t1639a, "shop.a")
	unreadable(t, snapshot1639(t, root, t1639b, "shop.a"))
	_, _, stale, err := FindBaseline(context.Background(), root, "shop", "a", t1639c)
	if err != nil || !stale.Unreadable {
		t.Fatalf("stale=%+v err=%v", stale, err)
	}
	other := t.TempDir()
	snapshot1639(t, other, t1639a, "shop.a")
	snapshot1639(t, other, t1639b, "shop.other")
	if _, _, stale, _ := FindBaseline(context.Background(), other, "shop", "a", t1639c); stale.Unreadable {
		t.Fatalf("a table absent from a readable newer snapshot is marked unreadable: %+v", stale)
	}
}
