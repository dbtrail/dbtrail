package packaging

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

const pkgDir = "../../build/packaging/bintrail-console"

func read(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// A path the release config names and the tree does not have fails the
// release at tag push, after the tag exists. Nothing on a pull request builds
// the packages, so this is the check that runs before.
func TestPackaging_everyFileTheReleaseConfigNamesExists(t *testing.T) {
	cfg := read(t, "../../build/.goreleaser.yaml")
	re := regexp.MustCompile(`(?m)^\s+(?:- src|preinstall|postinstall|preremove|postremove):\s+(\S+)\s*$`)
	found := re.FindAllStringSubmatch(cfg, -1)
	var scripts int
	for _, m := range found {
		if _, err := os.Stat(filepath.Join("../..", m[1])); err != nil {
			t.Errorf("build/.goreleaser.yaml names %s: %v", m[1], err)
		}
		if strings.HasSuffix(m[1], ".sh") {
			scripts++
		}
	}
	if scripts != 4 {
		t.Fatalf("found %d maintainer scripts in build/.goreleaser.yaml, want 4: the pattern above no longer reads the file", scripts)
	}
}

// The unit and the release config are two files that must agree on three
// paths. Each is asserted from the other side.
func TestPackaging_theUnitMatchesWhatThePackageInstalls(t *testing.T) {
	cfg := read(t, "../../build/.goreleaser.yaml")
	unit := read(t, filepath.Join(pkgDir, "bintrail-console.service"))
	env := read(t, filepath.Join(pkgDir, "bintrail-console.env"))

	line := func(key string) string {
		t.Helper()
		m := regexp.MustCompile(`(?m)^` + key + `=(.*)$`).FindStringSubmatch(unit)
		if m == nil {
			t.Fatalf("the unit has no %s= line", key)
		}
		return m[1]
	}

	if !strings.Contains(cfg, "bindir: /usr/bin\n") {
		t.Fatal("build/.goreleaser.yaml no longer installs binaries to /usr/bin")
	}
	if got := line("ExecStart"); got != "/usr/bin/bintrail-console watch" {
		t.Errorf("ExecStart=%s, want the packaged binary running watch", got)
	}
	envFile := line("EnvironmentFile")
	if !strings.Contains(cfg, "dst: "+envFile+"\n") {
		t.Errorf("the unit reads %s, which the package does not install", envFile)
	}
	state := "/var/lib/" + line("StateDirectory")
	if !strings.Contains(cfg, "dst: "+state+"\n") {
		t.Errorf("the unit's state folder %s is not a folder the package creates", state)
	}
	if got := line("WorkingDirectory"); got != state {
		t.Errorf("WorkingDirectory=%s, want %s", got, state)
	}
	if got := line("User"); got != "bintrail" {
		t.Errorf("User=%s, want the account preinstall.sh creates", got)
	}

	// Every path the settings file sets must be inside the folder the service
	// may write to, or the first save fails with a permission error.
	var paths int
	for _, l := range strings.Split(env, "\n") {
		name, value, ok := strings.Cut(l, "=")
		if !ok || strings.HasPrefix(l, "#") || !strings.HasPrefix(value, "/") {
			continue
		}
		paths++
		if !strings.HasPrefix(value, state+"/") {
			t.Errorf("%s=%s is outside %s", name, value, state)
		}
	}
	if paths != 5 {
		t.Errorf("the settings file sets %d paths, want 5", paths)
	}
	if !regexp.MustCompile(`(?m)^BINTRAIL_INDEX_DSN=$`).MatchString(env) {
		t.Error("the settings file must ship BINTRAIL_INDEX_DSN empty: a placeholder DSN would start a service pointed at nothing")
	}
}

// stub writes stand-ins for the tools the scripts call. Each one records its
// arguments; getent answers from EXISTING (space separated names).
func stub(t *testing.T) (path, log string) {
	t.Helper()
	dir := t.TempDir()
	log = filepath.Join(dir, "calls.log")
	rec := "#!/bin/sh\necho \"$(basename \"$0\") $*\" >> \"$STUB_LOG\"\n"
	for name, body := range map[string]string{
		"systemctl": rec,
		"groupadd":  rec,
		"useradd":   rec,
		"getent":    "#!/bin/sh\ncase \" $EXISTING \" in *\" $1:$2 \"*) exit 0 ;; esac\nexit 2\n",
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	return dir, log
}

func runScript(t *testing.T, script string, existing string, args ...string) (calls, out string) {
	t.Helper()
	bin, log := stub(t)
	cmd := exec.Command("/bin/sh", append([]string{filepath.Join(pkgDir, script)}, args...)...)
	cmd.Env = []string{"PATH=" + bin + ":/usr/bin:/bin", "STUB_LOG=" + log, "EXISTING=" + existing}
	b, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s %v: %v\n%s", script, args, err, b)
	}
	c, _ := os.ReadFile(log)
	return string(c), string(b)
}

func TestPackaging_preinstallCreatesTheAccountOnceAndLeavesAnExistingOneAlone(t *testing.T) {
	calls, _ := runScript(t, "preinstall.sh", "", "install")
	if !strings.Contains(calls, "groupadd --system bintrail\n") ||
		!strings.Contains(calls, "useradd --system --gid bintrail --home-dir /var/lib/bintrail ") {
		t.Errorf("a first install must create the group and the account, got:\n%s", calls)
	}
	calls, _ = runScript(t, "preinstall.sh", "group:bintrail passwd:bintrail", "upgrade", "0.102.0")
	if calls != "" {
		t.Errorf("an existing account must not be touched, got:\n%s", calls)
	}
	calls, _ = runScript(t, "preinstall.sh", "group:bintrail", "1")
	if strings.Contains(calls, "groupadd") || !strings.Contains(calls, "useradd") {
		t.Errorf("an existing group with no account must get only the account, got:\n%s", calls)
	}
}

// The three scripts below do nothing where systemd is not running, which is
// how they tell: the folder is an absolute path the scripts cannot be pointed
// away from, so these run on Linux hosts with systemd (CI) only.
func needSystemd(t *testing.T) {
	t.Helper()
	if _, err := os.Stat("/run/systemd/system"); err != nil {
		t.Skip("no systemd on this host: the scripts exit before calling systemctl")
	}
}

func TestPackaging_postinstallStartsNothingOnAFirstInstallAndRestartsOnAnUpgrade(t *testing.T) {
	needSystemd(t)
	for _, tc := range []struct {
		name    string
		args    []string
		restart bool
	}{
		{"deb first install", []string{"configure"}, false},
		{"deb first install, empty version", []string{"configure", ""}, false},
		{"rpm first install", []string{"1"}, false},
		{"deb upgrade", []string{"configure", "0.102.0"}, true},
		{"rpm upgrade", []string{"2"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls, out := runScript(t, "postinstall.sh", "", tc.args...)
			if !strings.Contains(calls, "systemctl daemon-reload\n") {
				t.Errorf("want a daemon-reload, got:\n%s", calls)
			}
			if got := strings.Contains(calls, "try-restart bintrail-console.service"); got != tc.restart {
				t.Errorf("restart = %v, want %v; calls:\n%s", got, tc.restart, calls)
			}
			if strings.Contains(calls, "enable") || strings.Contains(calls, " start ") {
				t.Errorf("an install must never enable or start the service, got:\n%s", calls)
			}
			if hint := strings.Contains(out, "systemctl enable --now bintrail-console"); hint == tc.restart {
				t.Errorf("the how-to-start hint is for a first install only; printed = %v", hint)
			}
		})
	}
}

func TestPackaging_preremoveStopsTheServiceOnARemovalAndNeverOnAnUpgrade(t *testing.T) {
	needSystemd(t)
	for _, tc := range []struct {
		arg  string
		stop bool
	}{
		{"remove", true},          // deb
		{"0", true},               // rpm, last version removed
		{"upgrade", false},        // deb
		{"1", false},              // rpm
		{"deconfigure", false},    // deb
		{"failed-upgrade", false}, // deb
	} {
		t.Run(tc.arg, func(t *testing.T) {
			calls, _ := runScript(t, "preremove.sh", "", tc.arg)
			if got := strings.Contains(calls, "systemctl disable --now bintrail-console.service"); got != tc.stop {
				t.Errorf("stopped = %v, want %v; calls:\n%s", got, tc.stop, calls)
			}
			if !tc.stop && calls != "" {
				t.Errorf("an upgrade must not touch the running service, got:\n%s", calls)
			}
		})
	}
}

func TestPackaging_postremoveReloadsAndRemovesNothing(t *testing.T) {
	needSystemd(t)
	for _, arg := range []string{"remove", "purge", "upgrade", "0", "1"} {
		calls, _ := runScript(t, "postremove.sh", "", arg)
		if calls != "systemctl daemon-reload\n" {
			t.Errorf("postremove %s: want only a daemon-reload, got:\n%s", arg, calls)
		}
	}
}
