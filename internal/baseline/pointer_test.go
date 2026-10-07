package baseline

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/snapshotdir"
)

// mkSnapshot creates <root>/<name>/<db>/<table>.parquet and returns the
// snapshot directory. It does NOT write a completeness marker: tests that care
// about markers write them through WriteSuccessMarker so the wiring is what is
// under test, not a hand-placed file.
// Every fixture timestamp here is deliberately in the PAST and stays there. A
// snapshot dated in the future never takes the pointer (see
// PublishCurrentPointer), so a fixture named for "tomorrow" silently stops
// exercising what its test claims -- which is exactly what happened when these
// were written a day before the date they used.
func mkSnapshot(t *testing.T, root, name string) string {
	t.Helper()
	snap := filepath.Join(root, name)
	if err := os.MkdirAll(filepath.Join(snap, "shop"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(snap, "shop", "orders.parquet"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	return snap
}

func readPointer(t *testing.T, root string) string {
	t.Helper()
	target, err := os.Readlink(filepath.Join(root, CurrentLinkName))
	if err != nil {
		t.Fatalf("readlink %s: %v", CurrentLinkName, err)
	}
	return target
}

// TestPublishCurrentPointer_targetIsTheBareSnapshotName pins the link target as
// a RELATIVE name. An absolute path would break the moment the baselines root
// is bind-mounted at another path in a container, copied to another host, or
// simply moved — which is the whole situation the pointer exists to survive.
func TestPublishCurrentPointer_targetIsTheBareSnapshotName(t *testing.T) {
	root := t.TempDir()
	snap := mkSnapshot(t, root, "2025-08-31T03-00-00Z")
	if err := PublishCurrentPointer(snap); err != nil {
		t.Fatalf("PublishCurrentPointer: %v", err)
	}
	if got := readPointer(t, root); got != "2025-08-31T03-00-00Z" {
		t.Fatalf("pointer target = %q, want the bare snapshot name", got)
	}
	// And it must actually resolve to the snapshot's data.
	if _, err := os.Stat(filepath.Join(root, CurrentLinkName, "shop", "orders.parquet")); err != nil {
		t.Fatalf("reading through the pointer: %v", err)
	}
}

// TestPublishCurrentPointer_movesForwardOnly guards the rule that keeps
// `reconstruct --output-format parquet --at <a past instant>` from dragging
// every generated views file back to yesterday's data. Completing an OLDER
// snapshot is a legitimate operation; it just must not win the pointer.
func TestPublishCurrentPointer_movesForwardOnly(t *testing.T) {
	root := t.TempDir()
	newer := mkSnapshot(t, root, "2025-08-31T03-00-00Z")
	older := mkSnapshot(t, root, "2025-08-30T03-00-00Z")

	if err := PublishCurrentPointer(newer); err != nil {
		t.Fatalf("publish newer: %v", err)
	}
	if err := PublishCurrentPointer(older); err != nil {
		t.Fatalf("publish older: %v", err)
	}
	if got := readPointer(t, root); got != "2025-08-31T03-00-00Z" {
		t.Fatalf("pointer moved backwards to %q", got)
	}

	// Forward still works after the declined move.
	newest := mkSnapshot(t, root, "2025-09-01T03-00-00Z")
	if err := PublishCurrentPointer(newest); err != nil {
		t.Fatalf("publish newest: %v", err)
	}
	if got := readPointer(t, root); got != "2025-09-01T03-00-00Z" {
		t.Fatalf("pointer = %q, want the newest snapshot", got)
	}
}

// TestPublishCurrentPointer_ignoresANonSnapshotDirectory covers the
// `reconstruct --output-format mydumper` shape: _SUCCESS is written into an
// operator-chosen dump directory that is not a timestamped snapshot under a
// baselines root. Publishing a pointer next to it would litter an unrelated
// directory and name a target no baseline reader understands.
func TestPublishCurrentPointer_ignoresANonSnapshotDirectory(t *testing.T) {
	root := t.TempDir()
	dump := filepath.Join(root, "nightly-dump")
	if err := os.MkdirAll(dump, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := PublishCurrentPointer(dump); err != nil {
		t.Fatalf("PublishCurrentPointer: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(root, CurrentLinkName)); !os.IsNotExist(err) {
		t.Fatalf("a pointer was published beside a non-snapshot directory (Lstat err = %v)", err)
	}
}

// TestPublishCurrentPointer_refusesToReplaceRealData pins that an existing
// non-symlink `current` is never removed. An operator may have a real directory
// by that name; silently deleting it to make room for a convenience pointer
// would be data loss in a recovery product.
func TestPublishCurrentPointer_refusesToReplaceRealData(t *testing.T) {
	root := t.TempDir()
	real := filepath.Join(root, CurrentLinkName)
	if err := os.MkdirAll(real, 0o755); err != nil {
		t.Fatal(err)
	}
	canary := filepath.Join(real, "keep-me")
	if err := os.WriteFile(canary, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	snap := mkSnapshot(t, root, "2025-08-31T03-00-00Z")
	err := PublishCurrentPointer(snap)
	if err == nil {
		t.Fatal("PublishCurrentPointer replaced a real directory without complaining")
	}
	if !strings.Contains(err.Error(), "not a symlink") {
		t.Fatalf("error does not say what is wrong: %v", err)
	}
	if _, serr := os.Stat(canary); serr != nil {
		t.Fatalf("the existing directory's contents were destroyed: %v", serr)
	}
}

// TestWriteSuccessMarker_publishesThePointer tests the WIRING, not the
// function: the pointer's whole correctness argument is that it moves at the
// one place a snapshot becomes complete. A PublishCurrentPointer that works
// perfectly but is never called is the failure this guards.
func TestWriteSuccessMarker_publishesThePointer(t *testing.T) {
	root := t.TempDir()
	snap := mkSnapshot(t, root, "2025-08-31T03-00-00Z")
	if err := WriteSuccessMarker(snap); err != nil {
		t.Fatalf("WriteSuccessMarker: %v", err)
	}
	if got := readPointer(t, root); got != "2025-08-31T03-00-00Z" {
		t.Fatalf("pointer = %q after WriteSuccessMarker", got)
	}
}

// TestWriteSuccessMarker_onADumpDirectoryPublishesNoPointer is the same wiring
// from the other side: `reconstruct --output-format mydumper` marks a plain
// dump directory complete, and that must not create a pointer in whatever
// directory happens to be its parent.
func TestWriteSuccessMarker_onADumpDirectoryPublishesNoPointer(t *testing.T) {
	root := t.TempDir()
	dump := filepath.Join(root, "nightly-dump")
	if err := os.MkdirAll(dump, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := WriteSuccessMarker(dump); err != nil {
		t.Fatalf("WriteSuccessMarker: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(root, CurrentLinkName)); !os.IsNotExist(err) {
		t.Fatalf("a pointer was published for a dump directory (Lstat err = %v)", err)
	}
}

// TestUploadWithOps_skipsTheCurrentPointer is a regression guard with a real
// failure behind it. filepath.WalkDir does not follow symlinks, so `current`
// arrives at the walk callback with IsDir() false; before the regular-file
// check it was handed to the file uploader, which opens the path — following
// the link to a DIRECTORY — and fails with "is a directory", taking every
// baseline S3 upload down with it.
//
// The mock therefore READS the file the way storage.UploadFile does. An
// uploadFile that only records its key would pass this test while production
// failed, which is the trap that made the bug invisible in the first place.
func TestUploadWithOps_skipsTheCurrentPointer(t *testing.T) {
	root := t.TempDir()
	snap := mkSnapshot(t, root, "2025-08-31T03-00-00Z")
	if err := WriteSuccessMarker(snap); err != nil { // also publishes the pointer
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(root, CurrentLinkName)); err != nil {
		t.Fatalf("test premise: the pointer was not published: %v", err)
	}

	var keys []string
	ops := s3UploadOps{
		putEmpty: func(_ context.Context, _ string) error { return nil },
		uploadFile: func(_ context.Context, path, k string) error {
			if _, err := os.ReadFile(path); err != nil {
				return err // exactly what the real uploader does with this path
			}
			keys = append(keys, k)
			return nil
		},
		objectExists: func(_ context.Context, _ string) (bool, error) { return false, nil },
		deleteObject: func(_ context.Context, _ string) error { return nil },
	}

	n, err := uploadWithOps(context.Background(), root, "p", false, lockedOps(ops))
	if err != nil {
		t.Fatalf("uploadWithOps: %v", err)
	}
	if n != 2 { // orders.parquet + _SUCCESS
		t.Fatalf("uploaded %d objects, want 2 (the pointer must not be one)", n)
	}
	for _, k := range keys {
		if strings.Contains(k, CurrentLinkName) {
			t.Fatalf("the pointer was uploaded as %q; it is a local convenience and means nothing in S3", k)
		}
	}
}

// TestCurrentPointerIsInvisibleToPruneAndDiscovery pins the claim the pointer's
// safety rests on: it cannot be selected as a baseline and it cannot be pruned.
// Both fall out of enumerating with ReadDir+IsDir (false for a symlink) AND
// requiring the name to parse as a timestamp, so this asserts the behaviour
// rather than the two mechanisms.
func TestCurrentPointerIsInvisibleToPruneAndDiscovery(t *testing.T) {
	root := t.TempDir()
	snap := mkSnapshot(t, root, "2025-08-31T03-00-00Z")
	if err := WriteSuccessMarker(snap); err != nil {
		t.Fatal(err)
	}

	snaps, err := enumerateLocalSnapshots(root)
	if err != nil {
		t.Fatalf("enumerateLocalSnapshots: %v", err)
	}
	if len(snaps) != 1 {
		t.Fatalf("enumerated %d snapshots, want 1 (the pointer must not be one)", len(snaps))
	}
	if snaps[0].name != "2025-08-31T03-00-00Z" {
		t.Fatalf("enumerated %q", snaps[0].name)
	}

	// And the snapshot the pointer names is a keeper, so prune can never leave
	// the pointer dangling: it holds the newest copy of every table it carries.
	keepers := computeKeepers(snaps, snaps[0].ts.Add(1))
	if !keepers["2025-08-31T03-00-00Z"] {
		t.Fatal("the pointed-at snapshot is not a keeper; prune could dangle the pointer")
	}
}

// TestResolveCurrentPointer_reportsUnusableShapes covers what the views
// generator asks before deciding to follow: a root with no pointer, and a
// pointer whose target is not a snapshot name, must both report unusable so the
// caller names a snapshot directly instead of emitting a path that resolves to
// nothing.
func TestResolveCurrentPointer_reportsUnusableShapes(t *testing.T) {
	t.Run("no pointer", func(t *testing.T) {
		if name, ok := ResolveCurrentPointer(t.TempDir()); ok {
			t.Fatalf("reported a usable pointer %q in a root that has none", name)
		}
	})

	t.Run("not a symlink", func(t *testing.T) {
		root := t.TempDir()
		if err := os.MkdirAll(filepath.Join(root, CurrentLinkName), 0o755); err != nil {
			t.Fatal(err)
		}
		if name, ok := ResolveCurrentPointer(root); ok {
			t.Fatalf("reported a real directory as a usable pointer (%q)", name)
		}
	})

	t.Run("target is not a snapshot name", func(t *testing.T) {
		root := t.TempDir()
		if err := os.Symlink("somewhere-else", filepath.Join(root, CurrentLinkName)); err != nil {
			t.Fatal(err)
		}
		if name, ok := ResolveCurrentPointer(root); ok {
			t.Fatalf("reported %q as a usable pointer", name)
		}
	})

	t.Run("usable", func(t *testing.T) {
		root := t.TempDir()
		snap := mkSnapshot(t, root, "2025-08-31T03-00-00Z")
		if err := PublishCurrentPointer(snap); err != nil {
			t.Fatal(err)
		}
		name, ok := ResolveCurrentPointer(root)
		if !ok || name != "2025-08-31T03-00-00Z" {
			t.Fatalf("ResolveCurrentPointer = %q, %v", name, ok)
		}
	})
}

// TestRewriteToPointer_refusesAURLRoot pins the behaviour the console relies on
// for an S3 baseline destination. The console hands its configured root through
// verbatim, so this function is the only thing standing between an s3:// URL
// and a rewritten path that resolves nowhere. Asserted here rather than with a
// prefix check at the call site: a check that cannot change the outcome reads
// as a guard without being one.
func TestRewriteToPointer_refusesAURLRoot(t *testing.T) {
	paths := []string{"s3://bucket/baselines/2025-08-31T03-00-00Z/shop/orders.parquet"}
	if got, _, ok := RewriteToPointer("s3://bucket/baselines/", paths); ok {
		t.Fatalf("rewrote an S3 root to %v", got)
	}
}

// TestRewriteToPointer_refusesAPathOutsideTheRoot covers the other way a caller
// can hand in something that is not a table of this snapshot. Rewriting it
// would silently move a view to a file nobody asked for.
func TestRewriteToPointer_refusesAPathOutsideTheRoot(t *testing.T) {
	root := t.TempDir()
	snap := mkSnapshot(t, root, "2025-08-31T03-00-00Z")
	if err := PublishCurrentPointer(snap); err != nil {
		t.Fatal(err)
	}
	paths := []string{
		filepath.Join(snap, "shop", "orders.parquet"),
		filepath.Join(t.TempDir(), "2025-08-31T03-00-00Z", "shop", "elsewhere.parquet"),
	}
	if got, _, ok := RewriteToPointer(root, paths); ok {
		t.Fatalf("rewrote paths spanning two roots to %v", got)
	}
}

// TestSplitSnapshotPath_contract pins the helper's refusals directly. They are
// not reachable through RewriteToPointer's callers today, because a path that
// escapes the root also fails the pointer-name comparison a line later. That
// makes them exactly the kind of check that rots: it reads as a guard, nothing
// exercises it, and the day a caller passes a different shape it turns out to
// have been decorative. Specifying it here is what keeps it real.
func TestSplitSnapshotPath_contract(t *testing.T) {
	root := "/data/baselines"
	cases := []struct {
		name           string
		path           string
		snapshot, rest string
	}{
		{"a table of a snapshot", root + "/2025-08-31T03-00-00Z/shop/orders.parquet",
			"2025-08-31T03-00-00Z", "shop/orders.parquet"},
		{"outside the root", "/elsewhere/2025-08-31T03-00-00Z/shop/orders.parquet", "", ""},
		{"the snapshot directory itself", root + "/2025-08-31T03-00-00Z", "", ""},
		{"the snapshot directory with a trailing separator", root + "/2025-08-31T03-00-00Z/", "", ""},
		{"the root itself", root, "", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			snapshot, rest := splitSnapshotPath(root, c.path)
			if snapshot != c.snapshot || rest != c.rest {
				t.Fatalf("splitSnapshotPath(%q, %q) = %q, %q; want %q, %q",
					root, c.path, snapshot, rest, c.snapshot, c.rest)
			}
		})
	}
}

// TestPublishCurrentPointer_advancesToAFewerTableSnapshot pins a DECISION, not
// a convenience. Publishing a snapshot that carries fewer tables moves the
// pointer, so a views file following it loses the view for a table the new
// snapshot does not hold -- loudly, by an unresolvable path.
//
// The alternative, refusing to advance unless the new snapshot is a superset,
// is the change someone will reach for on reading that. It must not be made
// without also reading why: a dropped table would freeze the pointer forever,
// since no later snapshot could satisfy the rule again, and every generated
// file would quietly serve older and older rows. This test is the tripwire.
func TestPublishCurrentPointer_advancesToAFewerTableSnapshot(t *testing.T) {
	root := t.TempDir()
	wide := mkSnapshot(t, root, "2025-08-30T03-00-00Z")
	if err := os.WriteFile(filepath.Join(wide, "shop", "customers.parquet"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := WriteSuccessMarker(wide); err != nil {
		t.Fatal(err)
	}

	// The newer snapshot holds only one of the two tables.
	narrow := mkSnapshot(t, root, "2025-08-31T03-00-00Z")
	if err := WriteSuccessMarker(narrow); err != nil {
		t.Fatal(err)
	}

	if got := readPointer(t, root); got != "2025-08-31T03-00-00Z" {
		t.Fatalf("pointer = %q; it must advance to the newest complete snapshot "+
			"even when that snapshot carries fewer tables (read this test's comment before changing it)", got)
	}
	// The consequence, stated so it cannot be introduced by accident later.
	if _, err := os.Stat(filepath.Join(root, CurrentLinkName, "shop", "customers.parquet")); err == nil {
		t.Fatal("test premise: the narrower snapshot was expected not to hold customers")
	}
}

// TestPublishCurrentPointer_stagingNamesAreUnique guards the fix for a race
// with a quiet outcome. Two producers writing into one baselines root staged
// under a SHARED name: A created its link, B removed it and created its own,
// and A's rename then published B's target while A returned success. With a
// snapshot legitimately produced for a past instant (`reconstruct
// --output-format parquet --at`), that lands the pointer on the OLDER snapshot
// and forward-only keeps it there until something newer than A completes.
//
// The race itself is not deterministically reproducible; the property that
// removes it is, so that is what is pinned.
func TestPublishCurrentPointer_stagingNamesAreUnique(t *testing.T) {
	seen := make(map[string]bool, 64)
	for range 64 {
		n := stagingName()
		if seen[n] {
			t.Fatalf("stagingName returned %q twice; two producers in one root would collide", n)
		}
		seen[n] = true
		if !strings.HasPrefix(n, ".") {
			t.Fatalf("staging name %q is not hidden from discovery", n)
		}
		if _, ok := snapshotdir.ParseTime(n); ok {
			t.Fatalf("staging name %q parses as a snapshot timestamp", n)
		}
	}
}

// TestPublishCurrentPointer_leavesNoStagingLeftover pairs with it: a successful
// publish must not accumulate one dangling link per run now that the name
// changes every time.
func TestPublishCurrentPointer_leavesNoStagingLeftover(t *testing.T) {
	root := t.TempDir()
	for _, name := range []string{"2025-08-30T03-00-00Z", "2025-08-31T03-00-00Z"} {
		if err := PublishCurrentPointer(mkSnapshot(t, root, name)); err != nil {
			t.Fatal(err)
		}
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), currentLinkTmp) {
			t.Fatalf("staging leftover %q survived a successful publish", e.Name())
		}
	}

	// The half that matters. A successful publish consumes its own staging link
	// by renaming it, so the loop above passes with no sweep at all. What the
	// sweep is FOR is the link a publish that died between the symlink and the
	// rename left behind: those are uniquely named, so nothing overwrites them
	// and they accumulate one per crash.
	crashed := filepath.Join(root, stagingName())
	if err := os.Symlink("2025-08-30T03-00-00Z", crashed); err != nil {
		t.Fatal(err)
	}
	if err := PublishCurrentPointer(mkSnapshot(t, root, "2025-09-01T03-00-00Z")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(crashed); !os.IsNotExist(err) {
		t.Fatalf("a leftover from an interrupted publish was not swept (Lstat err = %v)", err)
	}
}

// TestPublishCurrentPointer_anIncompleteNewerSnapshotDoesNotBlockPublishing
// covers the completeness half of the forward-only rule. A newer snapshot that
// is still being WRITTEN carries _INCOMPLETE, and it must not stop an older
// snapshot that just finished from taking the pointer: it does not hold data
// anyone can read yet, and treating it as the newest would leave the pointer
// stuck on whatever preceded both for as long as the write takes.
func TestPublishCurrentPointer_anIncompleteNewerSnapshotDoesNotBlockPublishing(t *testing.T) {
	root := t.TempDir()
	inProgress := mkSnapshot(t, root, "2025-09-01T03-00-00Z")
	if err := WriteIncompleteMarker(inProgress); err != nil {
		t.Fatal(err)
	}
	finished := mkSnapshot(t, root, "2025-08-31T03-00-00Z")
	if err := WriteSuccessMarker(finished); err != nil {
		t.Fatal(err)
	}

	if got := readPointer(t, root); got != "2025-08-31T03-00-00Z" {
		t.Fatalf("pointer = %q; an unfinished newer snapshot blocked a completed one", got)
	}

	// And once the newer one finishes, it takes the pointer.
	if err := WriteSuccessMarker(inProgress); err != nil {
		t.Fatal(err)
	}
	if got := readPointer(t, root); got != "2025-09-01T03-00-00Z" {
		t.Fatalf("pointer = %q after the newer snapshot completed", got)
	}
}

// TestRewriteToPointer_refusesAURLRootWithoutTouchingTheCwd is the discriminating
// version of the S3 refusal. filepath.Rel succeeds on an s3:// pair and
// filepath.Join produces "s3:/bucket/.../current", a RELATIVE path -- so without
// a structural guard the only thing refusing the rewrite is os.Lstat failing,
// which depends on what sits next to the process working directory. This test
// builds exactly that: a working directory where the probe SUCCEEDS.
func TestRewriteToPointer_refusesAURLRootWithoutTouchingTheCwd(t *testing.T) {
	cwd := t.TempDir()
	// The path filepath.Join(root, CurrentLinkName) yields for this root, made
	// resolvable relative to the working directory.
	probe := filepath.Join(cwd, "s3:", "bucket", "baselines")
	if err := os.MkdirAll(probe, 0o755); err != nil {
		t.Fatal(err)
	}
	mkSnapshot(t, probe, "2025-08-30T03-00-00Z")
	if err := os.Symlink("2025-08-30T03-00-00Z", filepath.Join(probe, CurrentLinkName)); err != nil {
		t.Fatal(err)
	}
	t.Chdir(cwd)

	root := "s3://bucket/baselines"
	paths := []string{root + "/2025-08-30T03-00-00Z/shop/orders.parquet"}
	if got, _, ok := RewriteToPointer(root, paths); ok {
		t.Fatalf("rewrote an S3 root to %v by resolving a pointer against the working directory", got)
	}
}

// TestUploadWithOps_uploadsASymlinkedTableFile guards against the fix for the
// pointer becoming a data-loss bug of its own. Skipping every non-regular entry
// is the obvious way to keep `current` out of the walk, and it silently drops a
// table an operator symlinked onto another volume -- while _SUCCESS still
// publishes, so the remote snapshot is discoverable, marked complete, and
// missing a table. That loss would surface mid-recovery.
//
// The mock READS the file, as storage.UploadFile does, so a skip shows up as a
// missing key rather than as a passing test over a path nobody opened.
func TestUploadWithOps_uploadsASymlinkedTableFile(t *testing.T) {
	root := t.TempDir()
	snap := mkSnapshot(t, root, "2025-08-31T03-00-00Z")
	elsewhere := t.TempDir()
	big := filepath.Join(elsewhere, "big.parquet")
	if err := os.WriteFile(big, []byte("payload"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(big, filepath.Join(snap, "shop", "big.parquet")); err != nil {
		t.Fatal(err)
	}
	if err := WriteSuccessMarker(snap); err != nil {
		t.Fatal(err)
	}

	var keys []string
	ops := s3UploadOps{
		putEmpty: func(_ context.Context, _ string) error { return nil },
		uploadFile: func(_ context.Context, path, k string) error {
			if _, err := os.ReadFile(path); err != nil {
				return err
			}
			keys = append(keys, k)
			return nil
		},
		objectExists: func(_ context.Context, _ string) (bool, error) { return false, nil },
		deleteObject: func(_ context.Context, _ string) error { return nil },
	}
	if _, err := uploadWithOps(context.Background(), root, "p", false, lockedOps(ops)); err != nil {
		t.Fatalf("uploadWithOps: %v", err)
	}
	var found bool
	for _, k := range keys {
		if strings.HasSuffix(k, "shop/big.parquet") {
			found = true
		}
		if strings.Contains(k, CurrentLinkName) {
			t.Fatalf("the pointer was uploaded as %q", k)
		}
	}
	if !found {
		t.Fatalf("the symlinked table file was not uploaded; keys = %v", keys)
	}
}

// TestUploadWithOps_survivesAStagingLeftover pairs with it from the other side.
// A crash between the staged symlink and the rename leaves a dangling
// `.current.tmp.*`; refusing the upload over it would make an operator hunt a
// hidden link they never created, on a snapshot that is genuinely complete.
func TestUploadWithOps_survivesAStagingLeftover(t *testing.T) {
	root := t.TempDir()
	snap := mkSnapshot(t, root, "2025-08-31T03-00-00Z")
	if err := WriteSuccessMarker(snap); err != nil {
		t.Fatal(err)
	}
	// Exactly what an interrupted publish leaves: a dangling staging link.
	if err := os.Symlink("2025-08-31T03-00-00Z", filepath.Join(root, stagingName())); err != nil {
		t.Fatal(err)
	}

	ops := s3UploadOps{
		putEmpty: func(_ context.Context, _ string) error { return nil },
		uploadFile: func(_ context.Context, path, _ string) error {
			_, err := os.ReadFile(path)
			return err
		},
		objectExists: func(_ context.Context, _ string) (bool, error) { return false, nil },
		deleteObject: func(_ context.Context, _ string) error { return nil },
	}
	if _, err := uploadWithOps(context.Background(), root, "p", false, lockedOps(ops)); err != nil {
		t.Fatalf("a staging leftover failed the upload: %v", err)
	}
}

// TestUploadWithOps_refusesAnUnreadableSnapshotFile is the third arm: something
// that is neither the pointer nor a regular file must be LOUD, not skipped.
// Publishing _SUCCESS over a snapshot that could not be uploaded whole is the
// failure the other two tests exist to prevent.
func TestUploadWithOps_refusesAnUnreadableSnapshotFile(t *testing.T) {
	root := t.TempDir()
	snap := mkSnapshot(t, root, "2025-08-31T03-00-00Z")
	// A symlinked SCHEMA directory: resolves, but not to a file.
	if err := os.Symlink(t.TempDir(), filepath.Join(snap, "warehouse")); err != nil {
		t.Fatal(err)
	}
	if err := WriteSuccessMarker(snap); err != nil {
		t.Fatal(err)
	}

	var published []string
	ops := s3UploadOps{
		putEmpty:     func(_ context.Context, _ string) error { return nil },
		uploadFile:   func(_ context.Context, _, k string) error { published = append(published, k); return nil },
		objectExists: func(_ context.Context, _ string) (bool, error) { return false, nil },
		deleteObject: func(_ context.Context, _ string) error { return nil },
	}
	_, err := uploadWithOps(context.Background(), root, "p", false, lockedOps(ops))
	if err == nil {
		t.Fatal("uploaded a snapshot holding something that is not a file, without complaining")
	}
	for _, k := range published {
		if strings.HasSuffix(k, SuccessMarker) {
			t.Fatal("_SUCCESS was published over a snapshot that could not be uploaded whole")
		}
	}
}

// TestPruneLocal_neverPrunesThePointerTarget guards the invariant the pointer
// rests on. It is redundant while publication succeeds, because the pointer
// follows the newest snapshot, which is already a keeper. It stops being
// redundant the moment a publish FAILS: the pointer then lags, and a lagging
// snapshot whose tables all appear in a newer one is otherwise prunable -- so
// retention would delete it and break every followed views file at once.
func TestPruneLocal_neverPrunesThePointerTarget(t *testing.T) {
	root := t.TempDir()
	old := mkSnapshot(t, root, "2025-06-01T00-00-00Z")
	if err := WriteSuccessMarker(old); err != nil {
		t.Fatal(err)
	}
	// The pointer is now at the old snapshot. A newer one completes but its
	// publish fails, which is what leaves the pointer behind; simulate that end
	// state directly by marking the newer one complete without republishing.
	newer := mkSnapshot(t, root, "2025-08-31T03-00-00Z")
	if err := os.WriteFile(filepath.Join(newer, SuccessMarker), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if got := readPointer(t, root); got != "2025-06-01T00-00-00Z" {
		t.Fatalf("test premise: pointer = %q, want it lagging at the old snapshot", got)
	}

	// Everything is durable and past retention, so nothing but the keeper rules
	// stands between the old snapshot and deletion.
	res, err := pruneWithProbe(context.Background(), PruneOptions{
		LocalDir: root, Retain: time.Hour, S3URL: "s3://b/p",
		Now: time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC),
	}, func(context.Context, string) (bool, error) { return true, nil })
	if err != nil {
		t.Fatalf("pruneWithProbe: %v", err)
	}
	for _, name := range res.Pruned {
		if name == "2025-06-01T00-00-00Z" {
			t.Fatal("retention pruned the snapshot the `current` pointer names, leaving it dangling")
		}
	}
	if _, err := os.Stat(filepath.Join(root, CurrentLinkName, "shop", "orders.parquet")); err != nil {
		t.Fatalf("the pointer dangles after prune: %v", err)
	}
}

// TestPublishCurrentPointer_concurrentPublishersConvergeOnTheNewest is the
// guard for the race a review MEASURED: with a unique staging name but no
// re-read, 3 in 400 concurrent pairs left the pointer on the OLDER snapshot.
// Both publishers read the same old pointer, both decide they may advance, and
// the last rename wins regardless of which snapshot is newer. Publishing a past
// instant is legitimate (`reconstruct --output-format parquet --at`), so this
// is reachable, and forward-only then pins the mistake in place.
//
// Many rounds on purpose: one round passes by luck most of the time, which is
// exactly why the bug was invisible until someone counted.
func TestPublishCurrentPointer_concurrentPublishersConvergeOnTheNewest(t *testing.T) {
	const (
		rounds = 120
		older  = "2025-06-01T00-00-00Z"
		newer  = "2025-08-31T03-00-00Z"
	)
	var landedOld int
	for range rounds {
		root := t.TempDir()
		oldSnap := mkSnapshot(t, root, older)
		newSnap := mkSnapshot(t, root, newer)
		// PRODUCTION SHAPE, and this is the whole point of the fixture. An
		// earlier version created both directories up front, and since a
		// marker-less directory is complete-by-default, the older publisher saw
		// the newer one and declined BEFORE staging every single time: 120
		// rounds of a single-writer non-race that still caught a revert but
		// could never see the interleaving it is named for. Stamping
		// _INCOMPLETE first and completing inside the goroutine reproduces the
		// window a peer actually races through.
		for _, snap := range []string{oldSnap, newSnap} {
			if err := WriteIncompleteMarker(snap); err != nil {
				t.Fatal(err)
			}
		}

		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		for _, snap := range []string{newSnap, oldSnap} {
			go func() {
				defer wg.Done()
				<-start
				// An error is allowed (a peer may hold the lock too long);
				// landing on the older snapshot is not.
				_ = WriteSuccessMarker(snap)
			}()
		}
		close(start)
		wg.Wait()

		got, ok := ResolveCurrentPointer(root)
		if !ok {
			t.Fatalf("no pointer published at all")
		}
		if got == older {
			landedOld++
		}
	}
	if landedOld > 0 {
		t.Fatalf("%d of %d concurrent publishes left the pointer on the OLDER snapshot", landedOld, rounds)
	}
}

// TestPublishCurrentPointer_aFutureDatedSnapshotNeverFreezesThePointer guards a
// shape that turns the whole feature inside out. `bintrail baseline
// --timestamp` accepts any ISO 8601 with no upper bound, and a host clock that
// jumps forward produces the same thing. Before this rule, one future-dated
// snapshot took the pointer and then outranked every real snapshot that
// followed, so the pointer froze there PERMANENTLY: every followed views file
// served that snapshot's rows forever, every path resolving, nothing logged.
func TestPublishCurrentPointer_aFutureDatedSnapshotNeverFreezesThePointer(t *testing.T) {
	root := t.TempDir()
	future := time.Now().UTC().AddDate(1, 0, 0).Format("2006-01-02T15-04-05Z")
	if err := WriteSuccessMarker(mkSnapshot(t, root, future)); err != nil {
		t.Fatal(err)
	}
	if _, ok := ResolveCurrentPointer(root); ok {
		t.Fatal("a snapshot dated in the future took the pointer")
	}

	// And a real snapshot completing afterwards still takes it.
	if err := WriteSuccessMarker(mkSnapshot(t, root, "2025-08-31T03-00-00Z")); err != nil {
		t.Fatal(err)
	}
	if got := readPointer(t, root); got != "2025-08-31T03-00-00Z" {
		t.Fatalf("pointer = %q; a future-dated snapshot blocked a real one", got)
	}
}

// TestSweepStagingLeftovers_leavesAPeersInFlightLink is the other half of the
// sweep. A staged link belonging to ANOTHER process may be microseconds from
// its own rename; removing it turns a benign race into a publish failure that
// logs at Error and tells the operator their views are frozen, on a run where
// the pointer is perfectly correct.
func TestSweepStagingLeftovers_leavesAPeersInFlightLink(t *testing.T) {
	root := t.TempDir()
	peer := filepath.Join(root, currentLinkTmp+".999999.1.1") // another pid
	if err := os.Symlink("2025-08-31T03-00-00Z", peer); err != nil {
		t.Fatal(err)
	}
	if err := PublishCurrentPointer(mkSnapshot(t, root, "2025-08-31T03-00-00Z")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(peer); err != nil {
		t.Fatalf("the sweep removed another process's staged link: %v", err)
	}
}

// TestResolveCurrentPointer_refusesATargetOutsideTheRoot closes a redirection
// this code would otherwise accept. PublishCurrentPointer only ever writes a
// bare directory name, so a target with a separator was made by hand -- and
// taking filepath.Base of it let `current -> ../../other-root/<same timestamp>`
// satisfy RewriteToPointer's equality check and point every state view at
// another root's data, silently.
func TestResolveCurrentPointer_refusesATargetOutsideTheRoot(t *testing.T) {
	root := t.TempDir()
	other := t.TempDir()
	mkSnapshot(t, other, "2025-08-31T03-00-00Z")
	rel, err := filepath.Rel(root, filepath.Join(other, "2025-08-31T03-00-00Z"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(rel, filepath.Join(root, CurrentLinkName)); err != nil {
		t.Fatal(err)
	}
	if name, ok := ResolveCurrentPointer(root); ok {
		t.Fatalf("accepted a pointer into another root as %q", name)
	}
}

// TestUploadWithOps_skipsThePointerLock keeps the flock file out of S3. It is a
// REGULAR file directly under the baselines root, so the symlink test that
// catches the pointer and its staging links cannot see it, and it would be
// published as though it were snapshot data.
func TestUploadWithOps_skipsThePointerLock(t *testing.T) {
	root := t.TempDir()
	snap := mkSnapshot(t, root, "2025-08-31T03-00-00Z")
	if err := WriteSuccessMarker(snap); err != nil { // creates the lock file
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, pointerLockName)); err != nil {
		t.Fatalf("test premise: no lock file was created: %v", err)
	}

	var keys []string
	ops := s3UploadOps{
		putEmpty: func(_ context.Context, _ string) error { return nil },
		uploadFile: func(_ context.Context, path, k string) error {
			if _, err := os.ReadFile(path); err != nil {
				return err
			}
			keys = append(keys, k)
			return nil
		},
		objectExists: func(_ context.Context, _ string) (bool, error) { return false, nil },
		deleteObject: func(_ context.Context, _ string) error { return nil },
	}
	if _, err := uploadWithOps(context.Background(), root, "p", false, lockedOps(ops)); err != nil {
		t.Fatalf("uploadWithOps: %v", err)
	}
	for _, k := range keys {
		if strings.Contains(k, pointerLockName) {
			t.Fatalf("the pointer lock was uploaded as %q", k)
		}
	}
}

// TestPublishCurrentPointer_refusesWhenTheRootCannotBeListed covers the guard's
// own error path, which is where a guard most often fails open. The
// forward-only rule is decided from a directory listing; when that listing
// fails, skipping the rule does not mean "no opinion", it means the rule is
// GONE -- and a snapshot produced for a past instant then takes the pointer,
// every followed file starts serving that day's rows, and forward-only pins it
// there.
//
// The permission split is exact, and getting it wrong tests another branch
// entirely: listing needs r, creating the lock file needs wx, resolving a
// symlink needs x. 0o311 is the one shape that lets every earlier step succeed
// and fails only os.ReadDir.
func TestPublishCurrentPointer_refusesWhenTheRootCannotBeListed(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root bypasses directory permissions")
	}
	root := t.TempDir()
	snap := mkSnapshot(t, root, "2025-06-01T00-00-00Z")
	t.Cleanup(func() { _ = os.Chmod(root, 0o755) })
	if err := os.Chmod(root, 0o311); err != nil {
		t.Fatal(err)
	}

	err := PublishCurrentPointer(snap)
	if err == nil {
		t.Fatal("published without being able to decide whether this snapshot outranks the newest")
	}
	// Not the staging write failing later: the refusal must be the DECISION.
	if !strings.Contains(err.Error(), "may take the pointer") {
		t.Fatalf("refusal does not name the decision it could not make: %v", err)
	}
}
