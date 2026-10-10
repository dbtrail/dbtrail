package consoleapp

import (
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/console"
)

// stubFSKind replaces the filesystem probe for one test.
func stubFSKind(t *testing.T, fn func(string) (fsClass, string, error)) {
	t.Helper()
	old := fsKindFn
	fsKindFn = fn
	forgetWorkingFolderBoot()
	t.Cleanup(func() { fsKindFn = old; forgetWorkingFolderBoot() })
}

// forgetWorkingFolderBoot drops what this process resolved, for a test that
// changes what the answer depends on.
func forgetWorkingFolderBoot() {
	workingFolderBoot.Lock()
	workingFolderBoot.key, workingFolderBoot.def = "", workingFolderDefault{}
	workingFolderBoot.Unlock()
}

func localFS(string) (fsClass, string, error) { return fsLocal, "ext4", nil }

// tempAs points the system temp folder at a directory of the test's, so the
// old default is a path the test owns.
func tempAs(t *testing.T) (legacy string) {
	t.Helper()
	tmp := t.TempDir()
	t.Setenv("TMPDIR", tmp)
	return filepath.Join(tmp, "bintrail-baseline-staging")
}

func TestDefaultWorkingFolder_2255(t *testing.T) {
	t.Run("beside the servers file, in a folder it creates", func(t *testing.T) {
		tempAs(t)
		stubFSKind(t, localFS)
		state := filepath.Join(t.TempDir(), "not", "there", "yet")
		got := defaultWorkingFolder(filepath.Join(state, "console-servers.yaml"))
		want := filepath.Join(state, "baseline-staging")
		if got.Dir != want || got.Why != "" {
			t.Fatalf("got %+v, want %s with no reason", got, want)
		}
		if fi, err := os.Stat(want); err != nil || !fi.IsDir() {
			t.Fatalf("the folder was not created: %v", err)
		}
		if left, _ := os.ReadDir(want); len(left) != 0 {
			t.Fatalf("the write check left %d entries behind", len(left))
		}
		// On a fresh install this is the first thing to create DBTrail's
		// data folder, which holds the credential files: private, like every
		// other writer makes it, and the working folder with it.
		for _, d := range []string{state, want} {
			fi, err := os.Stat(d)
			if err != nil {
				t.Fatal(err)
			}
			if fi.Mode().Perm() != 0o700 {
				t.Errorf("%s was created %o, want 700", d, fi.Mode().Perm())
			}
		}
	})

	t.Run("a data folder that exists keeps the mode it has", func(t *testing.T) {
		tempAs(t)
		stubFSKind(t, localFS)
		state := t.TempDir()
		if err := os.Chmod(state, 0o750); err != nil {
			t.Fatal(err)
		}
		if got := defaultWorkingFolder(filepath.Join(state, "console-servers.yaml")); got.Why != "" {
			t.Fatalf("got %+v", got)
		}
		if fi, _ := os.Stat(state); fi.Mode().Perm() != 0o750 {
			t.Errorf("the data folder's mode changed to %o", fi.Mode().Perm())
		}
	})

	t.Run("data held in memory: the temp folder, and nothing is created there", func(t *testing.T) {
		legacy := tempAs(t)
		state := t.TempDir()
		stubFSKind(t, func(p string) (fsClass, string, error) {
			if p == state {
				return fsMemory, "tmpfs", nil
			}
			return fsLocal, "", nil
		})
		got := defaultWorkingFolder(filepath.Join(state, "console-servers.yaml"))
		if got.Dir != legacy || got.InMemory || got.Unusable || !strings.Contains(got.Why, "held in memory (tmpfs)") || !strings.Contains(got.Why, state) {
			t.Fatalf("got %+v", got)
		}
		if _, err := os.Stat(filepath.Join(state, "baseline-staging")); !os.IsNotExist(err) {
			t.Fatalf("a folder was created in memory (stat: %v)", err)
		}
	})

	t.Run("the path is cleaned and absolute whatever the servers file was written as", func(t *testing.T) {
		tempAs(t)
		stubFSKind(t, localFS)
		// The real spelling of the folder, so the expectation is written
		// down here and not worked out the way the code works it out (a temp
		// folder on macOS is reached through a symlink).
		base, err := filepath.EvalSymlinks(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		t.Chdir(base)
		want := base + "/state/baseline-staging"
		for _, in := range []string{
			"state/console-servers.yaml",
			"./state//console-servers.yaml",
			"state/sub/../console-servers.yaml",
			base + "/state//console-servers.yaml",
			base + "/other/../state/console-servers.yaml",
		} {
			got := defaultWorkingFolder(in)
			real, err := filepath.EvalSymlinks(got.Dir)
			if err != nil {
				t.Fatalf("%q: %v", in, err)
			}
			if real != want || !filepath.IsAbs(got.Dir) || got.Dir != filepath.Clean(got.Dir) {
				t.Errorf("%q: got %s (really %s), want %s", in, got.Dir, real, want)
			}
		}
	})

	t.Run("no servers file on disk: the temp folder, and it says why", func(t *testing.T) {
		legacy := tempAs(t)
		stubFSKind(t, localFS)
		for _, in := range []string{"", "   "} {
			got := defaultWorkingFolder(in)
			if got.Dir != legacy || got.Why == "" {
				t.Fatalf("%q: got %+v, want %s with a reason", in, got, legacy)
			}
		}
	})

	t.Run("network storage: the temp folder, and nothing is created beside the servers file", func(t *testing.T) {
		legacy := tempAs(t)
		state := t.TempDir()
		var asked string
		stubFSKind(t, func(p string) (fsClass, string, error) {
			if asked == "" {
				asked = p
			}
			return fsNetwork, "nfs", nil
		})
		got := defaultWorkingFolder(filepath.Join(state, "console-servers.yaml"))
		if got.Dir != legacy {
			t.Fatalf("got %s, want %s", got.Dir, legacy)
		}
		if !strings.Contains(got.Why, "network storage") || !strings.Contains(got.Why, "nfs") || !strings.Contains(got.Why, state) {
			t.Fatalf("the reason does not name the storage and the folder: %q", got.Why)
		}
		if asked != state {
			t.Fatalf("asked about %q, want the folder of the servers file %q", asked, state)
		}
		if got.Unusable {
			t.Fatal("network storage is a choice, not a folder that failed")
		}
		if _, err := os.Stat(filepath.Join(state, "baseline-staging")); !os.IsNotExist(err) {
			t.Fatalf("a folder was created on network storage (stat: %v)", err)
		}
	})

	t.Run("network storage is asked of the nearest folder that exists", func(t *testing.T) {
		tempAs(t)
		root := t.TempDir()
		var asked string
		stubFSKind(t, func(p string) (fsClass, string, error) {
			if asked == "" {
				asked = p
			}
			return fsNetwork, "nfs", nil
		})
		defaultWorkingFolder(filepath.Join(root, "a", "b", "console-servers.yaml"))
		if asked != root {
			t.Fatalf("asked about %q, want %q", asked, root)
		}
	})

	t.Run("in the temp folder, it says whether that folder is memory", func(t *testing.T) {
		legacy := tempAs(t)
		for _, c := range []struct {
			name   string
			class  fsClass
			err    error
			memory bool
		}{
			{"memory", fsMemory, nil, true},
			{"a disk", fsLocal, nil, false},
			{"network storage", fsNetwork, nil, false},
			{"cannot be read", fsMemory, errors.New("statfs: boom"), false},
		} {
			var asked []string
			stubFSKind(t, func(p string) (fsClass, string, error) { asked = append(asked, p); return c.class, "x", c.err })
			got := defaultWorkingFolder("")
			if got.Dir != legacy || got.InMemory != c.memory {
				t.Errorf("%s: got %+v, want memory=%v", c.name, got, c.memory)
			}
			// Asked of the temp folder itself (it does not exist yet, so of
			// its parent), not of anything else.
			if len(asked) != 1 || asked[0] != filepath.Dir(legacy) {
				t.Errorf("%s: asked about %q, want %q", c.name, asked, filepath.Dir(legacy))
			}
		}
		// Beside the data, the question is not about the temp folder at all.
		stubFSKind(t, localFS)
		if got := defaultWorkingFolder(filepath.Join(t.TempDir(), "console-servers.yaml")); got.InMemory {
			t.Errorf("beside the data: %+v", got)
		}
	})

	t.Run("the kind of storage cannot be read: treated as a local disk", func(t *testing.T) {
		tempAs(t)
		state := t.TempDir()
		stubFSKind(t, func(string) (fsClass, string, error) { return fsLocal, "", errors.New("statfs: boom") })
		got := defaultWorkingFolder(filepath.Join(state, "console-servers.yaml"))
		if got.Dir != filepath.Join(state, "baseline-staging") || got.Why != "" {
			t.Fatalf("got %+v", got)
		}
	})

	t.Run("the folder cannot be created: the temp folder, and it says why", func(t *testing.T) {
		legacy := tempAs(t)
		stubFSKind(t, localFS)
		state := t.TempDir()
		// A FILE where the folder would go: fails for root too.
		if err := os.WriteFile(filepath.Join(state, "baseline-staging"), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		got := defaultWorkingFolder(filepath.Join(state, "console-servers.yaml"))
		if got.Dir != legacy {
			t.Fatalf("got %s, want %s", got.Dir, legacy)
		}
		if !strings.Contains(got.Why, filepath.Join(state, "baseline-staging")) || !strings.Contains(got.Why, "cannot be used") {
			t.Fatalf("reason: %q", got.Why)
		}
		if !got.Unusable {
			t.Fatal("a folder that cannot be written is not marked as such, so the startup log would not warn")
		}
		if strings.Contains(got.Why, "\n") {
			t.Fatalf("the reason spans lines: %q", got.Why)
		}
	})

	t.Run("the folder exists and cannot be written: the temp folder", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("root writes into a read-only folder")
		}
		legacy := tempAs(t)
		stubFSKind(t, localFS)
		state := t.TempDir()
		ro := filepath.Join(state, "baseline-staging")
		if err := os.Mkdir(ro, 0o500); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(ro, 0o700) })
		got := defaultWorkingFolder(filepath.Join(state, "console-servers.yaml"))
		if got.Dir != legacy || !strings.Contains(got.Why, "cannot be used") {
			t.Fatalf("got %+v", got)
		}
	})

	t.Run("a read-only data folder (a mounted config): the temp folder", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("root writes into a read-only folder")
		}
		legacy := tempAs(t)
		stubFSKind(t, localFS)
		state := t.TempDir()
		if err := os.Chmod(state, 0o500); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(state, 0o700) })
		got := defaultWorkingFolder(filepath.Join(state, "console-servers.yaml"))
		if got.Dir != legacy || got.Why == "" {
			t.Fatalf("got %+v", got)
		}
	})
}

// What is set wins, exactly as before, and choosing it touches no disk.
func TestBaselineStagingDirFor_whatIsSetWins_2255(t *testing.T) {
	tempAs(t)
	stubFSKind(t, func(string) (fsClass, string, error) {
		t.Error("the default was worked out although a folder was set")
		return fsLocal, "", nil
	})
	state := t.TempDir()
	reg, err := console.LoadRegistry(filepath.Join(state, "console-servers.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	old := upBaselineStageDir
	t.Cleanup(func() { upBaselineStageDir = old })

	upBaselineStageDir = "/from/the/environment"
	if got := baselineStagingDirFor(reg); got != "/from/the/environment" {
		t.Fatalf("environment: got %s", got)
	}
	saved := "/saved/in/the/page"
	if err := reg.SetBackupSetting(console.BackupSettingStagingDir, &saved); err != nil {
		t.Fatal(err)
	}
	if got := baselineStagingDirFor(reg); got != saved {
		t.Fatalf("saved: got %s", got)
	}
	upBaselineStageDir = ""
	if got := baselineStagingDirFor(reg); got != saved {
		t.Fatalf("saved, nothing in the environment: got %s", got)
	}
	if _, err := os.Stat(filepath.Join(state, "baseline-staging")); !os.IsNotExist(err) {
		t.Fatalf("the default folder was created although it is not in use (stat: %v)", err)
	}
}

func TestBaselineStagingDirFor_nothingSet_2255(t *testing.T) {
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
	if got, want := baselineStagingDirFor(reg), filepath.Join(state, "baseline-staging"); got != want {
		t.Fatalf("got %s, want %s", got, want)
	}
	// No registry, or one that is only in memory: nothing to sit beside.
	mem, _ := console.LoadRegistry("")
	for name, r := range map[string]*console.Registry{"nil": nil, "in memory": mem} {
		if got := baselineStagingDirFor(r); got != legacy {
			t.Fatalf("%s registry: got %s, want %s", name, got, legacy)
		}
	}
}

// The compose stack sets the folder itself. The default has to come to the
// very same path there, or two installs of one product would disagree about
// where a full read writes.
func TestDefaultWorkingFolder_isWhatComposeSets_2255(t *testing.T) {
	compose, err := os.ReadFile("../docker-compose.yml")
	if err != nil {
		t.Fatal(err)
	}
	find := func(key string) string {
		m := regexp.MustCompile(`(?m)^\s+` + key + `:\s*(\S+)\s*$`).FindSubmatch(compose)
		if m == nil {
			t.Fatalf("docker-compose.yml does not set %s", key)
		}
		return string(m[1])
	}
	servers, staging := find("BINTRAIL_CONSOLE_SERVERS"), find("BINTRAIL_CONSOLE_BASELINE_STAGING")
	if got := filepath.Join(filepath.Dir(servers), workingFolderName); got != staging {
		t.Fatalf("beside %s the default is %s, and compose sets %s", servers, got, staging)
	}
}

// What the page is told at startup. With a folder set the default is not in
// use: nothing is worked out and no folder is created for it.
func TestBootWorkingFolderDefault_2255(t *testing.T) {
	legacy := tempAs(t)
	old := upBaselineStageDir
	t.Cleanup(func() { upBaselineStageDir = old })
	state := t.TempDir()
	reg, err := console.LoadRegistry(filepath.Join(state, "console-servers.yaml"))
	if err != nil {
		t.Fatal(err)
	}

	upBaselineStageDir = ""
	stubFSKind(t, localFS)
	if got := bootWorkingFolderDefault(reg); got.Dir != filepath.Join(state, "baseline-staging") || got.Why != "" || got.InMemory {
		t.Fatalf("nothing set: %+v", got)
	}
	// It is the folder the supervisor is given: the page and the read agree.
	if got := baselineStagingDirFor(reg); got != filepath.Join(state, "baseline-staging") {
		t.Fatalf("the read uses %s", got)
	}
	stubFSKind(t, func(p string) (fsClass, string, error) {
		if strings.HasPrefix(p, filepath.Dir(legacy)) {
			return fsMemory, "tmpfs", nil
		}
		return fsNetwork, "nfs", nil
	})
	got := bootWorkingFolderDefault(reg)
	if got.Dir != legacy || !got.InMemory || !strings.Contains(got.Why, "network storage") {
		t.Fatalf("data on network storage, temp in memory: %+v", got)
	}
	if got.Dir != baselineStagingDirFor(reg) {
		t.Fatalf("the page says %s and the read uses %s", got.Dir, baselineStagingDirFor(reg))
	}

	if err := os.RemoveAll(filepath.Join(state, "baseline-staging")); err != nil {
		t.Fatal(err)
	}
	stubFSKind(t, func(string) (fsClass, string, error) {
		t.Error("the default was worked out although a folder is set")
		return fsLocal, "", nil
	})
	upBaselineStageDir = "/from/the/environment"
	if got := bootWorkingFolderDefault(reg); got != (workingFolderDefault{}) {
		t.Fatalf("a folder is set, and the page is told a default: %+v", got)
	}
	upBaselineStageDir = ""
	saved := "/saved/in/the/page"
	if err := reg.SetBackupSetting(console.BackupSettingStagingDir, &saved); err != nil {
		t.Fatal(err)
	}
	if got := bootWorkingFolderDefault(reg); got != (workingFolderDefault{}) {
		t.Fatalf("a folder is saved, and the page is told a default: %+v", got)
	}
	if _, err := os.Stat(filepath.Join(state, "baseline-staging")); !os.IsNotExist(err) {
		t.Fatalf("the unused default folder was created (stat: %v)", err)
	}
}

// Every reader at startup gets ONE answer. Worked out per call, a disk error
// between two calls would have the page name one folder and reads use
// another.
func TestWorkingFolder_resolvedOncePerProcess_2255(t *testing.T) {
	legacy := tempAs(t)
	old := upBaselineStageDir
	upBaselineStageDir = ""
	t.Cleanup(func() { upBaselineStageDir = old })
	state := t.TempDir()
	reg, err := console.LoadRegistry(filepath.Join(state, "console-servers.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	stubFSKind(t, func(string) (fsClass, string, error) {
		calls++
		if calls > 1 {
			// From the second question on, the data folder "is" network
			// storage: a second resolution would move to the temp folder.
			return fsNetwork, "nfs", nil
		}
		return fsLocal, "", nil
	})
	want := filepath.Join(state, "baseline-staging")
	first := bootWorkingFolderDefault(reg)
	for i := range 3 {
		if got := baselineStagingDirFor(reg); got != want {
			t.Fatalf("call %d: the read uses %s, the page was told %s", i, got, first.Dir)
		}
	}
	if first.Dir != want || bootWorkingFolderDefault(reg).Dir != want {
		t.Fatalf("the page was told %s", first.Dir)
	}
	if calls != 1 {
		t.Fatalf("the disk was asked %d times, want once", calls)
	}
	// Another servers file is another answer, not the remembered one.
	other, err := console.LoadRegistry(filepath.Join(t.TempDir(), "console-servers.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if got := baselineStagingDirFor(other); got != legacy {
		t.Fatalf("another servers file got %s, want %s", got, legacy)
	}
}

// The value reaches the page: what upConsoleConfig hands the console is the
// folder the reads are given.
func TestUpConsoleConfig_tellsThePageTheWorkingFolder_2255(t *testing.T) {
	tempAs(t)
	stubFSKind(t, localFS)
	old := upBaselineStageDir
	upBaselineStageDir = ""
	t.Cleanup(func() { upBaselineStageDir = old })
	state := t.TempDir()
	reg, err := console.LoadRegistry(filepath.Join(state, "console-servers.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	opts := consoleOpts{Listen: "127.0.0.1:8090", Token: "tok"}
	cfg, err := upConsoleConfig(nil, "user:pass@tcp(127.0.0.1:3306)/idx", opts, reg)
	if err != nil {
		t.Fatal(err)
	}
	d := cfg.BackupSettingsDefaults
	if d.StagingDir != "" || d.StagingDirDefault != filepath.Join(state, "baseline-staging") || d.StagingDirDefaultWhy != "" || d.StagingDirDefaultInMemory {
		t.Fatalf("nothing set: %+v", d)
	}
	if d.StagingDirDefault != baselineStagingDirFor(reg) {
		t.Fatalf("the page is told %s and the read uses %s", d.StagingDirDefault, baselineStagingDirFor(reg))
	}

	stubFSKind(t, func(p string) (fsClass, string, error) {
		if p == state {
			return fsNetwork, "nfs", nil
		}
		return fsMemory, "tmpfs", nil
	})
	cfg, err = upConsoleConfig(nil, "user:pass@tcp(127.0.0.1:3306)/idx", opts, reg)
	if err != nil {
		t.Fatal(err)
	}
	d = cfg.BackupSettingsDefaults
	if d.StagingDirDefault != baselineStagingDirFor(reg) || !strings.Contains(d.StagingDirDefaultWhy, "network storage") || !d.StagingDirDefaultInMemory {
		t.Fatalf("data on network storage, temp in memory: %+v", d)
	}

	upBaselineStageDir = "/from/the/environment"
	cfg, err = upConsoleConfig(nil, "user:pass@tcp(127.0.0.1:3306)/idx", opts, reg)
	if err != nil {
		t.Fatal(err)
	}
	d = cfg.BackupSettingsDefaults
	if d.StagingDir != "/from/the/environment" || d.StagingDirDefault != "" || d.StagingDirDefaultWhy != "" {
		t.Fatalf("a folder is set: %+v", d)
	}
}
