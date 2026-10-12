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
	// The live settings file is created by postinstall from the shipped copy,
	// never owned by the package: an owned file the operator edited makes
	// dpkg stop and ask on the first upgrade that changes the shipped text.
	envFile := line("EnvironmentFile")
	post := read(t, filepath.Join(pkgDir, "postinstall.sh"))
	if !strings.Contains(post, "settings="+envFile+"\n") {
		t.Errorf("the unit reads %s, which postinstall.sh does not create", envFile)
	}
	if strings.Contains(cfg, "dst: "+envFile) {
		t.Errorf("the package owns %s; ship a copy and let postinstall.sh place it", envFile)
	}
	const shipped = "/usr/share/bintrail-console/bintrail-console.env"
	if !strings.Contains(cfg, "dst: "+shipped+"\n") || !strings.Contains(post, shipped) {
		t.Errorf("the package and postinstall.sh must agree on the shipped copy %s", shipped)
	}
	state := "/var/lib/" + line("StateDirectory")
	if !strings.Contains(cfg, "dst: "+state+"\n") {
		t.Errorf("the unit's state folder %s is not a folder the package creates", state)
	}
	if got := line("WorkingDirectory"); got != state {
		t.Errorf("WorkingDirectory=%s, want %s", got, state)
	}
	if got := line("Restart"); got != "always" {
		t.Errorf("Restart=%s: an exit that reports no error would end capture until someone noticed", got)
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
// arguments; getent answers from EXISTING (space separated names). systemctl
// answers is-enabled from ENABLED, and is-active from ACTIVE: "yes", "no", or
// "once" for a service that is running when asked and gone after a restart.
// bintrail-router.service answers is-active from ROUTER_ACTIVE instead, so a
// case can have one service running and the other not; its "starts" is a
// service that is not running until it is enabled and started.
func stub(t *testing.T) (path, log string) {
	t.Helper()
	dir := t.TempDir()
	log = filepath.Join(dir, "calls.log")
	rec := "#!/bin/sh\necho \"$(basename \"$0\") $*\" >> \"$STUB_LOG\"\n"
	for name, body := range map[string]string{
		"systemctl": rec + `case "$*" in "enable --now bintrail-router.service")
  [ "$ENABLE_FAILS" = yes ] && exit 1
  : > "$STUB_LOG.router-started" ;; esac
case "$1" in
  is-enabled) [ "$ENABLED" = yes ] ;;
  is-active) state=$ACTIVE; mark=$STUB_LOG.asked
    case "$*" in *bintrail-router.service*) state=$ROUTER_ACTIVE; mark=$STUB_LOG.router-asked ;; esac
    case "$state" in
      yes) exit 0 ;;
      once) [ ! -e "$mark" ] && : > "$mark" ;;
      starts) [ -e "$STUB_LOG.router-started" ] ;;
      *) exit 1 ;;
    esac ;;
esac
`,
		"groupadd": rec,
		"useradd":  rec,
		"install":  rec,
		"rm":       rec,
		"rmdir":    rec,
		"sleep":    "#!/bin/sh\n",
		"getent":   "#!/bin/sh\ncase \" $EXISTING \" in *\" $1:$2 \"*) exit 0 ;; esac\nexit 2\n",
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	return dir, log
}

func runScript(t *testing.T, script string, existing string, args ...string) (calls, out string) {
	t.Helper()
	return runScriptEnv(t, script, []string{"EXISTING=" + existing}, args...)
}

func runScriptEnv(t *testing.T, script string, env []string, args ...string) (calls, out string) {
	t.Helper()
	bin, log := stub(t)
	cmd := exec.Command("/bin/sh", append([]string{filepath.Join(pkgDir, script)}, args...)...)
	// The marker that says the router was started once lives in /etc, which a
	// test cannot write: every run gets one of its own, absent unless the case
	// names another.
	cmd.Env = append([]string{"PATH=" + bin + ":/usr/bin:/bin", "STUB_LOG=" + log,
		"BINTRAIL_ROUTER_MARKER=" + filepath.Join(filepath.Dir(log), "router-started-once")}, env...)
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
		if os.Getenv("CI") != "" {
			t.Fatal("CI runs on a host with no systemd, so the scripts' systemctl branches are not tested anywhere")
		}
		t.Skip("no systemd on this host: the scripts exit before calling systemctl")
	}
}

// The settings file is placed before the systemd check, so this runs on any
// host. The stand-in for install records the call; the real file is never
// written (the test host has no /etc/bintrail/bintrail-console.env).
func TestPackaging_postinstallPlacesTheSettingsFileOnlyWhenThereIsNone(t *testing.T) {
	if _, err := os.Stat("/etc/bintrail/bintrail-console.env"); err == nil {
		t.Skip("this host has a real settings file, which the script must leave alone")
	}
	calls, _ := runScript(t, "postinstall.sh", "", "configure")
	want := "install -m 0600 /usr/share/bintrail-console/bintrail-console.env /etc/bintrail/bintrail-console.env\n"
	if !strings.Contains(calls, want) {
		t.Errorf("want the shipped copy placed with mode 0600, got:\n%s", calls)
	}
	post := read(t, filepath.Join(pkgDir, "postinstall.sh"))
	if !strings.Contains(post, `if [ ! -e "$settings" ]; then`) {
		t.Error("postinstall.sh must place the settings file only when none exists: an upgrade would overwrite the index DSN")
	}
}

func TestPackaging_postinstallStartsNothingOnAFirstInstallAndRestartsOnAnUpgrade(t *testing.T) {
	needSystemd(t)
	for _, tc := range []struct {
		name            string
		args            []string
		active, enabled string
		restart, hint   bool
		warn            bool
	}{
		{"deb first install", []string{"configure"}, "no", "no", false, true, false},
		{"deb first install, empty version", []string{"configure", ""}, "no", "no", false, true, false},
		{"rpm first install", []string{"1"}, "no", "no", false, true, false},
		{"deb upgrade, running", []string{"configure", "0.102.0"}, "yes", "yes", true, false, false},
		{"rpm upgrade, running", []string{"2"}, "yes", "yes", true, false, false},
		{"upgrade, stopped by the operator", []string{"2"}, "no", "yes", false, false, false},
		{"upgrade from a version with no service", []string{"configure", "0.102.0"}, "no", "no", false, true, false},
		{"deb reinstall after a removal", []string{"configure", "0.103.0"}, "no", "no", false, true, false},
		{"upgrade, and it does not come back", []string{"2"}, "once", "yes", true, false, true},
		// A first install over a service that already runs, from a unit the
		// operator wrote: it is theirs to restart.
		{"first install, their own unit running", []string{"1"}, "yes", "yes", false, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls, out := runScriptEnv(t, "postinstall.sh", []string{"ACTIVE=" + tc.active, "ENABLED=" + tc.enabled, "ROUTER_ACTIVE=starts"}, tc.args...)
			if !strings.Contains(calls, "systemctl daemon-reload\n") {
				t.Errorf("want a daemon-reload, got:\n%s", calls)
			}
			if got := strings.Contains(calls, "try-restart bintrail-console.service"); got != tc.restart {
				t.Errorf("restart = %v, want %v; calls:\n%s", got, tc.restart, calls)
			}
			if regexp.MustCompile(`(?m)^systemctl (enable|start).*bintrail-console\.service`).MatchString(calls) {
				t.Errorf("an install must never enable or start the capture service, got:\n%s", calls)
			}
			if got := strings.Contains(out, "systemctl enable --now bintrail-console"); got != tc.hint {
				t.Errorf("how-to-start hint printed = %v, want %v; output:\n%s", got, tc.hint, out)
			}
			if got := strings.Contains(out, "did not come back"); got != tc.warn {
				t.Errorf("did-not-come-back warning printed = %v, want %v; output:\n%s", got, tc.warn, out)
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

func TestPackaging_postremoveDeletesOnlyTheSettingsFileAndOnlyOnAPurge(t *testing.T) {
	// purge runs before the systemd check, so it is tested on any host.
	calls, _ := runScript(t, "postremove.sh", "", "purge")
	if !strings.Contains(calls, "rm -f /etc/bintrail/bintrail-console.env /etc/bintrail/.router-started\n") {
		t.Errorf("a purge must delete the settings file, got:\n%s", calls)
	}
	for _, arg := range []string{"remove", "upgrade", "0", "1", "purge"} {
		calls, _ := runScript(t, "postremove.sh", "", arg)
		if strings.Contains(calls, "/var/lib/bintrail") || strings.Contains(calls, "userdel") {
			t.Errorf("postremove %s must leave the saved state and the account, got:\n%s", arg, calls)
		}
		if arg != "purge" && strings.Contains(calls, "rm") {
			t.Errorf("postremove %s must delete nothing, got:\n%s", arg, calls)
		}
	}
	needSystemd(t)
	for _, arg := range []string{"remove", "upgrade", "0", "1"} {
		if calls, _ := runScript(t, "postremove.sh", "", arg); calls != "systemctl daemon-reload\n" {
			t.Errorf("postremove %s: want only a daemon-reload, got:\n%s", arg, calls)
		}
	}
}

// The router's unit is a second service of the same package. It runs the same
// binary as the same account from the same settings file, so the two read the
// same servers file, and it must not hang off the capture service: it is a
// service of its own so that restarting capture does not drop its connections.
func TestPackaging_theRouterUnitRunsBesideTheCaptureUnitAndNotUnderIt(t *testing.T) {
	cfg := read(t, "../../build/.goreleaser.yaml")
	capture := read(t, filepath.Join(pkgDir, "bintrail-console.service"))
	router := read(t, filepath.Join(pkgDir, "bintrail-router.service"))
	line := func(unit, key string) string {
		t.Helper()
		m := regexp.MustCompile(`(?m)^`+key+`=(.*)$`).FindAllStringSubmatch(unit, -1)
		if len(m) != 1 {
			t.Fatalf("want one %s= line in the unit, found %d", key, len(m))
		}
		return m[0][1]
	}
	if !strings.Contains(cfg, "dst: /usr/lib/systemd/system/bintrail-router.service\n") {
		t.Error("the package does not install bintrail-router.service where systemd reads units")
	}
	if got := line(router, "ExecStart"); got != "/usr/bin/bintrail-console router" {
		t.Errorf("ExecStart=%s, want the packaged binary running router", got)
	}
	if cmd := read(t, "../../consoleapp/router.go"); !strings.Contains(cmd, "Use:   \"router\",") {
		t.Error("consoleapp/router.go no longer declares the router command the unit starts")
	}
	for _, key := range []string{"EnvironmentFile", "User", "Group", "WorkingDirectory"} {
		if got, want := line(router, key), line(capture, key); got != want {
			t.Errorf("%s=%s in the router unit, %s in the capture unit: the two would read different files", key, got, want)
		}
	}
	// The folder belongs to the capture unit, which creates it. A second
	// StateDirectory= here would have systemd reset its mode from this unit.
	if strings.Contains(router, "\nStateDirectory") {
		t.Error("the router unit must not declare the state folder: the capture unit owns it")
	}
	if got := line(router, "Restart"); got != "always" {
		t.Errorf("Restart=%s: applications connect through this port", got)
	}
	if got := line(router, "Delegate"); got != "yes" {
		t.Errorf("Delegate=%s: without it a statement on the copy has no memory ceiling of its own", got)
	}
	// Read-only unless the settings file says otherwise. Environment= is the
	// default and EnvironmentFile= overrides it, whatever their order.
	if got := line(router, "Environment"); !strings.HasPrefix(got, "BINTRAIL_CONSOLE_ROUTE_READ_ONLY=1 ") {
		t.Errorf("Environment=%s, want the router read-only by default", got)
	}
	// The router says its numbers on an address, and the web interface reads
	// them from one: the two units must name the same, or the panel is empty
	// on a stock install.
	if !strings.HasSuffix(line(router, "Environment"), " BINTRAIL_ROUTER_METRICS_ADDR=127.0.0.1:9091") {
		t.Error("the router unit does not turn its numbers on at 127.0.0.1:9091")
	}
	if !strings.Contains(capture, "\nEnvironment=BINTRAIL_CONSOLE_ROUTER_URL=http://127.0.0.1:9091\n") {
		t.Error("the capture unit does not read the router's numbers from 127.0.0.1:9091")
	}
	for _, l := range strings.Split(router, "\n") {
		if strings.HasPrefix(l, "#") {
			continue
		}
		if strings.Contains(l, "bintrail-console.service") {
			t.Errorf("the router unit names the capture unit in %q: restarting capture must not touch the router", l)
		}
	}
}

// The MySQL port is answered by the router and by nothing else, so the package
// starts it: on a first install, and on the first upgrade that brings it (the
// capture service of the version before answered the port itself). The marker
// says that happened, so a router its operator stopped stays stopped. One that
// runs is restarted, as the capture service is. And a router that is not
// running after either is said, with where to look.
func TestPackaging_postinstallStartsTheRouterOnceAndRestartsARunningOne(t *testing.T) {
	needSystemd(t)
	for _, tc := range []struct {
		name            string
		args            []string
		capture, router string
		marked          bool
		restart, start  bool
		warn            bool
		enableFails     bool
	}{
		{"deb first install", []string{"configure"}, "no", "starts", false, false, true, false, false},
		{"rpm first install", []string{"1"}, "no", "starts", false, false, true, false, false},
		{"first upgrade that brings the router", []string{"configure", "0.103.1"}, "yes", "starts", false, false, true, false, false},
		{"rpm first upgrade that brings the router", []string{"2"}, "yes", "starts", false, false, true, false, false},
		{"first install, and the router does not start", []string{"1"}, "no", "no", false, false, true, true, false},
		{"deb upgrade, router running", []string{"configure", "0.104.0"}, "yes", "yes", true, true, false, false, false},
		{"rpm upgrade, router running", []string{"2"}, "yes", "yes", true, true, false, false, false},
		{"upgrade, router running and capture stopped", []string{"2"}, "no", "yes", true, true, false, false, false},
		{"upgrade, router running, marker lost", []string{"2"}, "yes", "yes", false, true, false, false, false},
		{"upgrade, router stopped by the operator", []string{"2"}, "yes", "no", true, false, false, false, false},
		{"reinstall, router stopped by the operator", []string{"configure"}, "no", "no", true, false, false, false, false},
		{"upgrade, and the router does not come back", []string{"2"}, "yes", "once", true, true, false, true, false},
		{"first install, their own router unit running", []string{"1"}, "no", "yes", false, false, true, false, false},
		// Enabling did not work (a masked unit, no systemd to talk to): it is
		// said, nothing claims it started, and no marker stops the next try.
		{"first install, and the router cannot be enabled", []string{"1"}, "no", "no", false, false, true, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			marker := filepath.Join(t.TempDir(), "router-started-once")
			if tc.marked {
				if err := os.WriteFile(marker, nil, 0o644); err != nil {
					t.Fatal(err)
				}
			}
			calls, out := runScriptEnv(t, "postinstall.sh", []string{"ACTIVE=" + tc.capture, "ROUTER_ACTIVE=" + tc.router, "ENABLED=yes", "BINTRAIL_ROUTER_MARKER=" + marker, "ENABLE_FAILS=" + map[bool]string{true: "yes"}[tc.enableFails]}, tc.args...)
			if got := strings.Contains(calls, "try-restart bintrail-router.service"); got != tc.restart {
				t.Errorf("router restart = %v, want %v; calls:\n%s", got, tc.restart, calls)
			}
			if got := strings.Contains(calls, "systemctl enable --now bintrail-router.service\n"); got != tc.start {
				t.Errorf("router enabled and started = %v, want %v; calls:\n%s", got, tc.start, calls)
			}
			// The folder the unit works in must be there before the router
			// starts: the capture unit, which creates it, may never have run.
			if tc.start {
				mk, en := strings.Index(calls, "install -d -o bintrail -g bintrail -m 0750 /var/lib/bintrail"), strings.Index(calls, "systemctl enable --now bintrail-router.service")
				if _, err := os.Stat("/var/lib/bintrail"); err != nil && (mk < 0 || mk > en) {
					t.Errorf("the working folder is not created before the router starts; calls:\n%s", calls)
				}
			}
			if got := strings.Contains(out, "could not be enabled"); got != tc.enableFails {
				t.Errorf("said enabling failed = %v, want %v; output:\n%s", got, tc.enableFails, out)
			}
			if got := strings.Contains(out, "started now"); got != (tc.start && !tc.enableFails) {
				t.Errorf("said the router was started = %v, want %v; output:\n%s", got, tc.start, out)
			}
			if got := strings.Contains(out, "bintrail-router is not running"); got != tc.warn {
				t.Errorf("router warning printed = %v, want %v; output:\n%s", got, tc.warn, out)
			}
			if strings.Contains(out, "bintrail-console did not come back") {
				t.Errorf("the capture service was fine in every case here, got:\n%s", out)
			}
			if _, err := os.Stat(marker); (err == nil) == tc.enableFails {
				t.Errorf("marker there = %v after the script: it must be there unless enabling failed, or the next upgrade starts a stopped router again (or never retries one that did not start)", err == nil)
			}
		})
	}
}

func TestPackaging_preremoveStopsTheRouterOnARemoval(t *testing.T) {
	needSystemd(t)
	for _, arg := range []string{"remove", "0"} {
		calls, _ := runScript(t, "preremove.sh", "", arg)
		if !regexp.MustCompile(`(?m)^systemctl disable --now .*bintrail-router\.service`).MatchString(calls) {
			t.Errorf("preremove %s must stop and disable the router, got:\n%s", arg, calls)
		}
		// And forget that it was started once: the package stopped it, so an
		// install after this removal must start it again.
		if !regexp.MustCompile(`(?m)^rm -f \S*router-started`).MatchString(calls) {
			t.Errorf("preremove %s must remove the started-once marker, got:\n%s", arg, calls)
		}
	}
}
