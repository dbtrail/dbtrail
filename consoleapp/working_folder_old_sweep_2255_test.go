package consoleapp

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
)

// stagedBuild writes a .sql build as a daemon leaves it and dates every part
// of it.
func stagedBuild(t *testing.T, root, server, run string, at time.Time) string {
	t.Helper()
	dir := filepath.Join(root, "sql-export", server, run)
	if err := os.MkdirAll(filepath.Join(dir, "shop"), 0o700); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{"metadata", filepath.Join("shop", "orders.sql")} {
		if err := os.WriteFile(filepath.Join(dir, f), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	dateTree(t, dir, at)
	return dir
}

func dateTree(t *testing.T, dir string, at time.Time) {
	t.Helper()
	var paths []string
	if err := filepath.WalkDir(dir, func(p string, _ os.DirEntry, err error) error {
		paths = append(paths, p)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	// Deepest first: dating a file does not touch its folder, creating one
	// did.
	for i := len(paths) - 1; i >= 0; i-- {
		if err := os.Chtimes(paths[i], at, at); err != nil {
			t.Fatal(err)
		}
	}
}

func leftOnDisk2255(p string) bool { _, err := os.Lstat(p); return err == nil }

// An install that never set the folder moves to the new default on upgrade.
// The .sql builds its previous process left under the old one have nothing
// looking at them any more: the startup sweep only reads the folder in use.
func TestSweepUnusedDefaultFolders_2255(t *testing.T) {
	now := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	long := now.Add(-unusedDefaultBuildAge - time.Minute)
	recent := now.Add(-unusedDefaultBuildAge + time.Minute)

	setup := func(t *testing.T) (legacy, state string, reg *console.Registry) {
		t.Helper()
		legacy = tempAs(t)
		stubFSKind(t, localFS)
		old := upBaselineStageDir
		upBaselineStageDir = ""
		t.Cleanup(func() { upBaselineStageDir = old })
		state = t.TempDir()
		reg, err := console.LoadRegistry(filepath.Join(state, "console-servers.yaml"))
		if err != nil {
			t.Fatal(err)
		}
		return legacy, state, reg
	}

	t.Run("old builds in the old default go, recent ones and everything else stay", func(t *testing.T) {
		legacy, state, reg := setup(t)
		dead := stagedBuild(t, legacy, "srv1", "100", long)
		alsoDead := stagedBuild(t, legacy, "srv2", "7", long)
		live := stagedBuild(t, legacy, "srv1", "200", recent)
		// A build whose folder is old and one file is not: being written.
		writing := stagedBuild(t, legacy, "srv3", "1", long)
		if err := os.Chtimes(filepath.Join(writing, "shop", "orders.sql"), recent, recent); err != nil {
			t.Fatal(err)
		}
		// What another daemon on this host may be using: never by a glob.
		var others []string
		for _, n := range []string{"dump-123", "baseline-9", "pgbaseline-4", "sql-export-notes.txt"} {
			p := filepath.Join(legacy, n)
			if err := os.MkdirAll(filepath.Join(p, "inner"), 0o700); err != nil {
				t.Fatal(err)
			}
			dateTree(t, p, long)
			others = append(others, p)
		}
		stray := filepath.Join(legacy, "sql-export", "a-file")
		if err := os.WriteFile(stray, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(stray, long, long); err != nil {
			t.Fatal(err)
		}

		current := baselineStagingDirFor(reg)
		if current != filepath.Join(state, "baseline-staging") {
			t.Fatalf("the folder in use is %s", current)
		}
		sweepUnusedDefaultFolders(reg, current, now)

		for _, p := range []string{dead, alsoDead} {
			if leftOnDisk2255(p) {
				t.Errorf("%s is still there", p)
			}
		}
		// srv2 held nothing else: its folder goes with its last build.
		if leftOnDisk2255(filepath.Dir(alsoDead)) {
			t.Errorf("the emptied server folder %s is still there", filepath.Dir(alsoDead))
		}
		for _, p := range append([]string{live, writing, stray, legacy, filepath.Join(legacy, "sql-export")}, others...) {
			if !leftOnDisk2255(p) {
				t.Errorf("%s was removed", p)
			}
		}
		for _, p := range others {
			if !leftOnDisk2255(filepath.Join(p, "inner")) {
				t.Errorf("the inside of %s was touched", p)
			}
		}
	})

	t.Run("emptied, the sql-export folder goes and the old folder itself stays", func(t *testing.T) {
		legacy, _, reg := setup(t)
		stagedBuild(t, legacy, "srv1", "100", long)
		sweepUnusedDefaultFolders(reg, baselineStagingDirFor(reg), now)
		if leftOnDisk2255(filepath.Join(legacy, "sql-export")) {
			t.Error("the emptied sql-export folder is still there")
		}
		if !leftOnDisk2255(legacy) {
			t.Error("the old folder itself was removed: another daemon on this host may be about to write into it")
		}
	})

	t.Run("the old default is the folder in use: nothing is touched", func(t *testing.T) {
		legacy, _, reg := setup(t)
		stubFSKind(t, func(string) (fsClass, string, error) { return fsNetwork, "nfs", nil })
		current := baselineStagingDirFor(reg)
		if current != legacy {
			t.Fatalf("the folder in use is %s, want the temp one", current)
		}
		build := stagedBuild(t, legacy, "srv1", "100", long)
		sweepUnusedDefaultFolders(reg, current, now)
		if !leftOnDisk2255(build) {
			t.Fatal("a build in the folder in use was removed by the sweep of unused folders")
		}
	})

	t.Run("the folder in use reached by another spelling is still the folder in use", func(t *testing.T) {
		legacy, _, reg := setup(t)
		build := stagedBuild(t, legacy, "srv1", "100", long)
		link := filepath.Join(t.TempDir(), "link")
		if err := os.Symlink(legacy, link); err != nil {
			t.Skip(err)
		}
		for _, current := range []string{legacy + string(filepath.Separator), filepath.Join(legacy, "sql-export", ".."), link} {
			sweepUnusedDefaultFolders(reg, current, now)
			if !leftOnDisk2255(build) {
				t.Fatalf("in use as %q, and its build was removed", current)
			}
		}
	})

	t.Run("a folder is set: the defaults are not this daemon's to clear", func(t *testing.T) {
		legacy, state, reg := setup(t)
		inTempDir := stagedBuild(t, legacy, "srv1", "100", long)
		beside := stagedBuild(t, filepath.Join(state, "baseline-staging"), "srv1", "100", long)
		upBaselineStageDir = t.TempDir()
		sweepUnusedDefaultFolders(reg, baselineStagingDirFor(reg), now)
		saved := t.TempDir()
		upBaselineStageDir = ""
		if err := reg.SetBackupSetting(console.BackupSettingStagingDir, &saved); err != nil {
			t.Fatal(err)
		}
		sweepUnusedDefaultFolders(reg, baselineStagingDirFor(reg), now)
		for _, p := range []string{inTempDir, beside} {
			if !leftOnDisk2255(p) {
				t.Errorf("%s was removed although a folder is set", p)
			}
		}
	})

	t.Run("back in the temp folder: old builds beside the data go too", func(t *testing.T) {
		legacy, state, reg := setup(t)
		// The data folder is network storage now, so the daemon is back in
		// the temp folder.
		beside := filepath.Join(state, "baseline-staging")
		dead := stagedBuild(t, beside, "srv1", "100", long)
		live := stagedBuild(t, beside, "srv1", "200", recent)
		stubFSKind(t, func(p string) (fsClass, string, error) {
			if p == state {
				return fsNetwork, "nfs", nil
			}
			return fsLocal, "", nil
		})
		current := baselineStagingDirFor(reg)
		if current != legacy {
			t.Fatalf("the folder in use is %s", current)
		}
		sweepUnusedDefaultFolders(reg, current, now)
		if leftOnDisk2255(dead) || !leftOnDisk2255(live) {
			t.Fatalf("beside the data: old build there=%v, recent build there=%v", leftOnDisk2255(dead), leftOnDisk2255(live))
		}
	})

	t.Run("links are not followed", func(t *testing.T) {
		// Each target holds a dead build laid out exactly where the sweep
		// would find it if it followed the link.
		for _, link := range []string{"build", "server", "sql-export", "working folder"} {
			legacy, _, reg := setup(t)
			outside := t.TempDir()
			var victim, at, to string
			switch link {
			case "build":
				victim = stagedBuild(t, outside, "srv1", "100", long)
				at, to = filepath.Join(legacy, "sql-export", "srv1", "100"), victim
			case "server":
				victim = stagedBuild(t, outside, "srv1", "100", long)
				at, to = filepath.Join(legacy, "sql-export", "srv1"), filepath.Dir(victim)
			case "sql-export":
				victim = stagedBuild(t, outside, "srv1", "100", long)
				at, to = filepath.Join(legacy, "sql-export"), filepath.Join(outside, "sql-export")
			case "working folder":
				victim = stagedBuild(t, outside, "srv1", "100", long)
				at, to = legacy, outside
			}
			if err := os.MkdirAll(filepath.Dir(at), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(to, at); err != nil {
				t.Skip(err)
			}
			sweepUnusedDefaultFolders(reg, baselineStagingDirFor(reg), now)
			if !leftOnDisk2255(victim) {
				t.Errorf("a build outside the old folder was removed through a linked %s", link)
			}
		}
	})

	t.Run("only inside folders that are this user's and closed to others", func(t *testing.T) {
		for _, c := range []struct{ name, open string }{
			{"working folder", ""},
			{"sql-export", "sql-export"},
			{"server", filepath.Join("sql-export", "srv1")},
			{"build", filepath.Join("sql-export", "srv1", "100")},
		} {
			for _, mode := range []os.FileMode{0o777, 0o775, 0o707} {
				legacy, _, reg := setup(t)
				build := stagedBuild(t, legacy, "srv1", "100", long)
				opened := filepath.Join(legacy, c.open)
				if err := os.Chmod(opened, mode); err != nil {
					t.Fatal(err)
				}
				dateTree(t, legacy, long)
				sweepUnusedDefaultFolders(reg, baselineStagingDirFor(reg), now)
				if !leftOnDisk2255(build) {
					t.Errorf("the %s is mode %o, which another user can write into, and the build was removed", c.name, mode)
				}
			}
		}
		// Readable by others is not writable by others.
		legacy, _, reg := setup(t)
		build := stagedBuild(t, legacy, "srv1", "100", long)
		for _, d := range []string{legacy, filepath.Join(legacy, "sql-export"), filepath.Dir(build), build} {
			if err := os.Chmod(d, 0o755); err != nil {
				t.Fatal(err)
			}
		}
		dateTree(t, legacy, long)
		sweepUnusedDefaultFolders(reg, baselineStagingDirFor(reg), now)
		if leftOnDisk2255(build) {
			t.Error("folders others can only read (755) kept a dead build")
		}
	})

	t.Run("a working folder another user could rename is left alone, unless its parent is sticky", func(t *testing.T) {
		legacy, _, reg := setup(t)
		build := stagedBuild(t, legacy, "srv1", "100", long)
		parent := filepath.Dir(legacy)
		t.Cleanup(func() { _ = os.Chmod(parent, 0o700) })
		if err := os.Chmod(parent, 0o777); err != nil {
			t.Fatal(err)
		}
		sweepUnusedDefaultFolders(reg, baselineStagingDirFor(reg), now)
		if !leftOnDisk2255(build) {
			t.Fatal("the folder that holds the working folder is open to every user with no sticky bit, and the build was removed")
		}
		// The shape of /tmp.
		if err := os.Chmod(parent, 0o777|os.ModeSticky); err != nil {
			t.Fatal(err)
		}
		if fi, _ := os.Stat(parent); fi.Mode()&os.ModeSticky == 0 {
			t.Skip("this filesystem does not keep the sticky bit")
		}
		sweepUnusedDefaultFolders(reg, baselineStagingDirFor(reg), now)
		if leftOnDisk2255(build) {
			t.Fatal("under a sticky parent, like /tmp, a dead build was kept")
		}
	})

	t.Run("folders of another user are left alone", func(t *testing.T) {
		// Seen from another user: every folder here is then somebody
		// else's, at each level in turn the first one to say so.
		legacy, _, reg := setup(t)
		build := stagedBuild(t, legacy, "srv1", "100", long)
		old := currentUID
		t.Cleanup(func() { currentUID = old })
		currentUID = func() int { return old() + 1 }
		sweepUnusedDefaultFolders(reg, baselineStagingDirFor(reg), now)
		if !leftOnDisk2255(build) {
			t.Fatal("a build in folders of another user was removed")
		}
		currentUID = old
		sweepUnusedDefaultFolders(reg, baselineStagingDirFor(reg), now)
		if leftOnDisk2255(build) {
			t.Fatal("back as the owner, the dead build was kept")
		}
	})

	t.Run("a folder is this user's only by its owner, whatever its mode", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.Chmod(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		if why := ownFolder(dir); why != "" {
			t.Fatalf("this user's own closed folder: %s", why)
		}
		old := currentUID
		t.Cleanup(func() { currentUID = old })
		currentUID = func() int { return old() + 1 }
		if why := ownFolder(dir); why == "" {
			t.Fatal("a closed folder of another user counts as this user's")
		}
	})

	t.Run("a folder above that belongs to another user holds nothing in place", func(t *testing.T) {
		if currentUID() == 0 {
			t.Skip("root is trusted above any folder")
		}
		dir := t.TempDir()
		if err := os.Mkdir(filepath.Join(dir, "x"), 0o700); err != nil {
			t.Fatal(err)
		}
		// Free of links first, as the sweep hands it over.
		dir, err := filepath.EvalSymlinks(dir)
		if err != nil {
			t.Fatal(err)
		}
		if why := heldInPlace(filepath.Join(dir, "x")); why != "" {
			t.Fatalf("under this user's own folders: %s", why)
		}
		old := currentUID
		t.Cleanup(func() { currentUID = old })
		currentUID = func() int { return old() + 1 }
		if why := heldInPlace(filepath.Join(dir, "x")); why == "" {
			t.Fatal("a path under another user's 0700 folder counts as held in place: its owner can rename it")
		}
	})

	t.Run("a link above the working folder: checked and removed through one path", func(t *testing.T) {
		// heldInPlace takes a path with no link in it and refuses one that
		// has: where a link leads says nothing about who can repoint it.
		realDir, err := filepath.EvalSymlinks(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(filepath.Join(realDir, "inside", "x"), 0o700); err != nil {
			t.Fatal(err)
		}
		link := filepath.Join(realDir, "link")
		if err := os.Symlink(filepath.Join(realDir, "inside"), link); err != nil {
			t.Skip(err)
		}
		if why := heldInPlace(filepath.Join(realDir, "inside", "x")); why != "" {
			t.Fatalf("by its real path: %s", why)
		}
		if why := heldInPlace(filepath.Join(link, "x")); why == "" {
			t.Fatal("a path through a link counts as held in place")
		}

		// The sweep, with the temp folder reached through a link: it works
		// on the real folder, and a dead build there still goes.
		realTmp, err := filepath.EvalSymlinks(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		viaLink := filepath.Join(realDir, "tmp-link")
		if err := os.Symlink(realTmp, viaLink); err != nil {
			t.Fatal(err)
		}
		t.Setenv("TMPDIR", viaLink)
		stubFSKind(t, localFS)
		oldDir := upBaselineStageDir
		upBaselineStageDir = ""
		t.Cleanup(func() { upBaselineStageDir = oldDir })
		reg, err := console.LoadRegistry(filepath.Join(t.TempDir(), "console-servers.yaml"))
		if err != nil {
			t.Fatal(err)
		}
		build := stagedBuild(t, filepath.Join(realTmp, "bintrail-baseline-staging"), "srv1", "100", long)
		sweepUnusedDefaultFolders(reg, baselineStagingDirFor(reg), now)
		if leftOnDisk2255(build) {
			t.Fatal("with the temp folder reached through a link, a dead build in the real folder was kept")
		}
		// The same, with the folder that holds the real one open to all:
		// the real chain is what is judged.
		build = stagedBuild(t, filepath.Join(realTmp, "bintrail-baseline-staging"), "srv1", "200", long)
		t.Cleanup(func() { _ = os.Chmod(realTmp, 0o700) })
		if err := os.Chmod(realTmp, 0o777); err != nil {
			t.Fatal(err)
		}
		sweepUnusedDefaultFolders(reg, baselineStagingDirFor(reg), now)
		if !leftOnDisk2255(build) {
			t.Fatal("the real folder above is open to every user, and the build was removed through the link")
		}
	})

	t.Run("a folder two levels up that others can write into is seen", func(t *testing.T) {
		top, err := filepath.EvalSymlinks(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(top, "a", "b", "c")
		if err := os.MkdirAll(path, 0o700); err != nil {
			t.Fatal(err)
		}
		if why := heldInPlace(path); why != "" {
			t.Fatalf("closed chain: %s", why)
		}
		t.Cleanup(func() { _ = os.Chmod(top, 0o700) })
		if err := os.Chmod(top, 0o777); err != nil {
			t.Fatal(err)
		}
		if why := heldInPlace(path); why == "" {
			t.Fatal("a folder two levels above is open to every user, and the path counts as held in place")
		}
	})

	t.Run("the age line: written exactly a day ago goes, a second later stays", func(t *testing.T) {
		legacy, _, reg := setup(t)
		atLine := stagedBuild(t, legacy, "srv1", "1", now.Add(-unusedDefaultBuildAge))
		after := stagedBuild(t, legacy, "srv1", "2", now.Add(-unusedDefaultBuildAge+time.Second))
		dateTree(t, filepath.Join(legacy, "sql-export"), long)
		dateTree(t, atLine, now.Add(-unusedDefaultBuildAge))
		dateTree(t, after, now.Add(-unusedDefaultBuildAge+time.Second))
		sweepUnusedDefaultFolders(reg, baselineStagingDirFor(reg), now)
		if leftOnDisk2255(atLine) || !leftOnDisk2255(after) {
			t.Fatalf("at the line: there=%v; a second after it: there=%v", leftOnDisk2255(atLine), leftOnDisk2255(after))
		}
	})

	t.Run("a build with a corner that cannot be read is not proven dead", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("root reads everything")
		}
		legacy, _, reg := setup(t)
		build := stagedBuild(t, legacy, "srv1", "100", long)
		corner := filepath.Join(build, "shop")
		if err := os.Chmod(corner, 0o000); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(corner, 0o700) })
		sweepUnusedDefaultFolders(reg, baselineStagingDirFor(reg), now)
		// The folder alone proves nothing: a removal that starts and then
		// meets the corner leaves the folder and takes the rest.
		if !leftOnDisk2255(build) || !leftOnDisk2255(filepath.Join(build, "metadata")) {
			t.Fatal("a build that could not be read through was removed, or emptied")
		}
	})

	t.Run("no old folder, no registry: nothing happens", func(t *testing.T) {
		_, _, reg := setup(t)
		sweepUnusedDefaultFolders(reg, baselineStagingDirFor(reg), now)
		sweepUnusedDefaultFolders(nil, baselineStagingDirFor(nil), now)
	})
}

// What startup runs: the folder in use is cleared of every build a previous
// process left, whatever its age, and the other default only of dead ones.
func TestSweepWorkingFoldersAtBoot_2255(t *testing.T) {
	legacy := tempAs(t)
	stubFSKind(t, localFS)
	old := upBaselineStageDir
	upBaselineStageDir = ""
	t.Cleanup(func() { upBaselineStageDir = old })
	state := t.TempDir()
	reg, err := console.LoadRegistry(filepath.Join(state, "console-servers.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	inUse := filepath.Join(state, "baseline-staging")
	mine := stagedBuild(t, inUse, "srv1", "1", now)
	dead := stagedBuild(t, legacy, "srv1", "1", now.Add(-unusedDefaultBuildAge-time.Hour))
	theirs := stagedBuild(t, legacy, "srv1", "2", now)

	sweepWorkingFoldersAtBoot(reg, now)

	if leftOnDisk2255(mine) {
		t.Error("a build a previous process left in the folder in use is still there")
	}
	if leftOnDisk2255(dead) {
		t.Error("a dead build in the old default is still there")
	}
	if !leftOnDisk2255(theirs) {
		t.Error("a recent build in the old default, which may be another daemon's, was removed")
	}
}

// A running daemon looks again: what an upgrade leaves in the old default is
// younger than a day at the start that follows it, and that daemon may not
// start again for months.
func TestRunUnusedDefaultSweep_2255(t *testing.T) {
	legacy := tempAs(t)
	stubFSKind(t, localFS)
	old := upBaselineStageDir
	upBaselineStageDir = ""
	t.Cleanup(func() { upBaselineStageDir = old })
	reg, err := console.LoadRegistry(filepath.Join(t.TempDir(), "console-servers.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	dead := stagedBuild(t, legacy, "srv1", "1", time.Now().Add(-unusedDefaultBuildAge-time.Hour))
	live := stagedBuild(t, legacy, "srv1", "2", time.Now())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { runUnusedDefaultSweep(ctx, reg, 5*time.Millisecond); close(done) }()
	deadline := time.Now().Add(10 * time.Second)
	for leftOnDisk2255(dead) && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the sweep did not stop with the daemon")
	}
	if leftOnDisk2255(dead) {
		t.Fatal("a running daemon never removed the dead build")
	}
	if !leftOnDisk2255(live) {
		t.Fatal("the recent build was removed")
	}
}
