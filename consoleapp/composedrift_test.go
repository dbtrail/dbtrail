package consoleapp

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"go.yaml.in/yaml/v2"

	"github.com/dbtrail/dbtrail/internal/telemetry"
)

// The stack-drift check (#1529), tested from both ends.
//
// The fixture for "everything is wired" is BUILT FROM ../docker-compose.yml,
// not hand-written: the mounts come off the console service's volume list, the
// state paths off its environment, and the index DSN's host off the entrypoint
// script that builds it. A hand-written fixture would keep passing after the
// compose file changed, which is the one failure this check must never have —
// a false alarm on a correctly wired stack teaches operators to ignore the
// line, and then the real one goes unread too.

// composeDriftFile decodes the fields these fixtures read.
type composeDriftFile struct {
	Version  int `yaml:"x-bintrail-compose-version"`
	Services map[string]struct {
		Environment map[string]any `yaml:"environment"`
		Volumes     []string       `yaml:"volumes"`
		Command     []string       `yaml:"command"`
	} `yaml:"services"`
}

func readComposeDriftFile(t *testing.T) composeDriftFile {
	t.Helper()
	data, err := os.ReadFile(composePath)
	if err != nil {
		t.Fatalf("read %s: %v", composePath, err)
	}
	var doc composeDriftFile
	if err := yaml.Unmarshal(data, &doc); err != nil {
		t.Fatalf("parse %s: %v", composePath, err)
	}
	if _, ok := doc.Services[composeService]; !ok {
		t.Fatalf("no %q service in %s", composeService, composePath)
	}
	return doc
}

// composeStackMounts is the mount table a container running the console
// service has: an overlay root (the image layer plus the writable layer that a
// recreate throws away) and one mount per volume entry, plus the three files
// Docker binds into every container. The volume TARGETS are read off the
// compose file, so moving a volume moves the fixture with it.
func composeStackMounts(t *testing.T) []mountEntry {
	t.Helper()
	doc := readComposeDriftFile(t)
	svc := doc.Services[composeService]
	if len(svc.Volumes) == 0 {
		t.Fatalf("the %q service in %s mounts no volumes; this fixture reads that list and would otherwise assert nothing", composeService, composePath)
	}
	mounts := []mountEntry{{point: "/", fstype: "overlay"}}
	for _, v := range svc.Volumes {
		_, target, found := strings.Cut(v, ":")
		if !found {
			t.Fatalf("volume entry %q on the %q service names no container path", v, composeService)
		}
		target, _, _ = strings.Cut(target, ":") // drop the :ro option
		mounts = append(mounts, mountEntry{point: target, fstype: "ext4"})
	}
	// Docker binds these into every container; they are here so the fixture
	// exercises the same longest-prefix walk a real mount table forces.
	for _, f := range []string{"/etc/resolv.conf", "/etc/hostname", "/etc/hosts"} {
		mounts = append(mounts, mountEntry{point: f, fstype: "ext4"})
	}
	return mounts
}

// composeEntrypointIndexHost pulls the bundled index's host out of the DSN the
// entrypoint script builds. It is what makes composeIndexHost more than a
// hopeful constant: rename the service in the compose file and this fails,
// instead of the detection going quietly blind.
func composeEntrypointIndexHost(t *testing.T) string {
	t.Helper()
	doc := readComposeDriftFile(t)
	svc := doc.Services[composeService]
	script := strings.Join(svc.Command, "\n")
	m := regexp.MustCompile(`@tcp\(([^:)]+)[:)]`).FindStringSubmatch(script)
	if m == nil {
		t.Fatalf("the %q service's command in %s builds no tcp(...) index DSN; this guard reads it and would otherwise assert nothing", composeService, composePath)
	}
	return m[1]
}

// composeEntrypointDatadirRO pulls the read-only index mount the entrypoint
// exports. It is set by the script rather than the environment map, so it has
// to be read from there or the "wired" fixture would be missing it and this
// whole file would pass for the wrong reason.
func composeEntrypointDatadirRO(t *testing.T) string {
	t.Helper()
	doc := readComposeDriftFile(t)
	svc := doc.Services[composeService]
	script := strings.Join(svc.Command, "\n")
	m := regexp.MustCompile(`export BINTRAIL_INDEX_DATADIR_RO=(\S+)`).FindStringSubmatch(script)
	if m == nil {
		t.Fatalf("the %q service's command in %s exports no BINTRAIL_INDEX_DATADIR_RO; the index's free disk space cannot be measured in this stack", composeService, composePath)
	}
	return m[1]
}

// composeStateEnv reads a console state path off the compose environment.
func composeStateEnv(t *testing.T, name string) string {
	t.Helper()
	v, ok := composeConsoleEnvValue(t, name)
	if !ok {
		t.Fatalf("%s does not set %s on the %s service", composePath, name, composeService)
	}
	return v
}

// wiredStack is the current, fully wired compose stack as this check sees it.
// Every finding must be silent here.
func wiredStack(t *testing.T) driftInputs {
	t.Helper()
	datadirRO := composeEntrypointDatadirRO(t)
	env := map[string]string{
		"BINTRAIL_INDEX_DATADIR_RO": datadirRO,
		"BINTRAIL_COMPOSE_VERSION":  strconv.Itoa(bundledComposeVersion),
	}
	return driftInputs{
		getenv:   func(k string) string { return env[k] },
		indexDSN: fmt.Sprintf("root:pw@tcp(%s:3306)/bintrail_index", composeEntrypointIndexHost(t)),
		isDir:    func(p string) bool { return p == datadirRO },
		// The config directory as a RELEASED image leaves it, and the REAL
		// predicate over it. Hardcoding "nothing writes there" is what let the
		// false alarm in: telemetry is compiled into every release build, its
		// consent defaults on, neither this compose file nor .env.example
		// turns it off, and its spool lives in this very directory. So the
		// fixture writes what the image writes and lets the code decide.
		stateInDir: consoleStateInDir,
		mounts:     composeStackMounts(t),
		state: []statePath{
			{what: "your username and password", path: composeStateEnv(t, "BINTRAIL_CONSOLE_AUTH")},
			{what: "the servers you added", path: composeStateEnv(t, "BINTRAIL_CONSOLE_SERVERS")},
			{what: "the AI connection token", path: composeStateEnv(t, "BINTRAIL_CONSOLE_MCP_TOKEN_FILE")},
		},
		configDir:      imageConfigDir(t),
		shippedVersion: bundledComposeVersion,
		versionAdded:   composeVersionAdded,
	}
}

// imageConfigDir builds the console's config directory as a RELEASED image
// leaves it on a fully wired stack: the telemetry spool and consent file, and
// no console state, because the compose file redirects all three console state
// files onto the volume.
//
// Those two entries are not hypothetical and not this test's invention. The
// release build compiles telemetry in, its consent defaults on, and it writes
// under telemetry.ConfigDir(), which is the same ~/.config/bintrail this
// package's state falls back to. The paths come from the telemetry package
// rather than from string literals here, so renaming them there moves this
// fixture with them instead of quietly making it fiction.
func imageConfigDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.MkdirAll(telemetry.SpoolDir(dir), 0o755); err != nil {
		t.Fatalf("seed spool dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(telemetry.SpoolDir(dir), "events.ndjson"), []byte("{}\n"), 0o600); err != nil {
		t.Fatalf("seed spool file: %v", err)
	}
	if err := os.WriteFile(telemetry.StatePath(dir), []byte(`{"notice_shown":true}`), 0o600); err != nil {
		t.Fatalf("seed consent file: %v", err)
	}
	return dir
}

func TestComposeDriftIsSilentOnTheWiredStack(t *testing.T) {
	if got := composeDriftFindings(wiredStack(t)); len(got) != 0 {
		for _, f := range got {
			t.Errorf("the shipped stack reports drift against itself: %s %v", f.msg, f.attrs)
		}
	}
}

// TestComposeDriftFindsAStaleStack takes the wired stack apart the way real
// history did: the read-only index mount landed in 2026-07 and the token path
// override this week, so a file downloaded before either has the volume but
// not these.
func TestComposeDriftFindsAStaleStack(t *testing.T) {
	in := wiredStack(t)
	in.getenv = func(string) string { return "" } // no version, no datadir mount
	in.isDir = func(string) bool { return false }
	in.mounts = removeMount(in.mounts, composeEntrypointDatadirRO(t))
	// The token override does not exist in this file, so the token falls back
	// into the console's config directory, which nothing keeps. Written for
	// real, so the config-directory shape reads it the way it will in
	// production instead of being told the answer.
	in.state[2].path = filepath.Join(in.configDir, "console-mcp-token.yaml")
	if err := os.WriteFile(in.state[2].path, []byte("token: x\n"), 0o600); err != nil {
		t.Fatalf("seed the token file: %v", err)
	}

	got := composeDriftFindings(in)
	if len(got) != 2 {
		t.Fatalf("want 2 findings (disk space, saved settings), got %d: %v", len(got), got)
	}
	joined := fmt.Sprint(got)
	for _, want := range []string{
		"free disk space for the index cannot be measured",
		"the settings saved in the web interface are stored inside the container",
		"console-mcp-token.yaml",
		"docker-compose.yml",
	} {
		if !strings.Contains(joined, want) {
			t.Errorf("the report never mentions %q: %s", want, joined)
		}
	}
	// Named ONCE, and by the state-path shape. The token is both a path this
	// package resolves and a file sitting in the config directory, so the two
	// shapes see the same file; a line that lists it twice reads like two
	// problems. Counting alone cannot tell WHICH shape named it (with the
	// state-path shape gone the directory shape still yields one), so the
	// wording that only that shape produces is asserted too.
	if n := strings.Count(joined, "console-mcp-token.yaml"); n != 1 {
		t.Errorf("the token file is named %d times in one line: %s", n, joined)
	}
	if !strings.Contains(joined, "the AI connection token (") {
		t.Errorf("the token is not named as a resolved state path, so only the directory shape saw it: %s", joined)
	}
	// The two files that ARE on the volume must not be reported as lost.
	if strings.Contains(joined, "console-auth.yaml") || strings.Contains(joined, "console-servers.yaml") {
		t.Errorf("the report names state that is on the volume and is not at risk: %s", joined)
	}
}

func TestComposeDriftStaysSilentWhereItCannotTell(t *testing.T) {
	stale := func(t *testing.T) driftInputs {
		in := wiredStack(t)
		in.getenv = func(string) string { return "" }
		in.isDir = func(string) bool { return false }
		in.state[2].path = filepath.Join(in.configDir, "console-mcp-token.yaml")
		if err := os.WriteFile(in.state[2].path, []byte("token: x\n"), 0o600); err != nil {
			t.Fatalf("seed the token file: %v", err)
		}
		return in
	}
	t.Run("no mount table to read", func(t *testing.T) {
		in := stale(t)
		in.mounts = nil
		if got := composeDriftFindings(in); len(got) != 0 {
			t.Errorf("reported drift with no mount table to prove anything: %v", got)
		}
	})
	t.Run("not a container", func(t *testing.T) {
		in := stale(t)
		in.mounts = []mountEntry{{point: "/", fstype: "ext4"}, {point: "/home", fstype: "ext4"}}
		if got := composeDriftFindings(in); len(got) != 0 {
			t.Errorf("a bare-metal daemon has no compose file to be stale, but got: %v", got)
		}
	})
	t.Run("bring your own index", func(t *testing.T) {
		in := stale(t)
		in.indexDSN = "root:pw@tcp(my-own-mysql.internal:3306)/bintrail_index"
		for _, f := range composeDriftFindings(in) {
			if strings.Contains(f.msg, "free disk space") {
				t.Errorf("a bring-your-own index has no bundled volume to mount, but got: %s", f.msg)
			}
		}
	})
	t.Run("nothing durable to compare against", func(t *testing.T) {
		in := wiredStack(t)
		// A bare `docker run` with no volumes at all: every path is on the
		// writable layer, and that is as likely a throwaway evaluation
		// container as a stale stack, so the mixed-mount shape reports
		// nothing. The config directory is emptied here on purpose, to
		// isolate that shape: the config-directory shape is the OTHER half
		// and is driven by TestConsoleStateFindingReadsTheConfigDirectory
		// below. With state left in it, this subtest would report for that
		// other reason and could never see its own bug.
		in.mounts = []mountEntry{{point: "/", fstype: "overlay"}}
		in.configDir = t.TempDir()
		for _, f := range composeDriftFindings(in) {
			if strings.Contains(f.msg, "settings saved in the web interface are stored") {
				t.Errorf("reported console state loss with no durable mount anywhere to compare against: %s", f.msg)
			}
		}
	})
}

// TestConsoleStateFindingReadsTheConfigDirectory pins the second shape over
// real directory contents: state that no environment variable relocates is
// only reachable through the directory it lands in, so a console-*.yaml there
// is the evidence.
//
// The telemetry row is the one that matters. Every release build writes that
// spool and consent file into this same directory, so "the directory holds
// anything" reported a fully wired stack as broken, and told the operator to
// mount a volume they already have or download a compose file already current.
func TestConsoleStateFindingReadsTheConfigDirectory(t *testing.T) {
	for _, tc := range []struct {
		name  string
		files []string
		dirs  []string
		want  bool
		names string
	}{
		{name: "empty"},
		{
			name:  "what a released image writes there on a wired stack",
			files: []string{"telemetry.json"},
			dirs:  []string{"telemetry-spool"},
		},
		{
			name:  "other tools' files",
			files: []string{"config.env", "dump.key"},
		},
		{
			name:  "console state this package names",
			files: []string{"console-auth.yaml", "telemetry.json"},
			want:  true, names: "console-auth.yaml",
		},
		{
			name:  "console state a newer build keeps there",
			files: []string{"console-users.yaml", "telemetry.json"},
			want:  true, names: "console-users.yaml",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := wiredStack(t)
			dir := t.TempDir()
			for _, f := range tc.files {
				if err := os.WriteFile(filepath.Join(dir, f), []byte("x\n"), 0o600); err != nil {
					t.Fatalf("seed %s: %v", f, err)
				}
			}
			for _, d := range tc.dirs {
				if err := os.MkdirAll(filepath.Join(dir, d), 0o755); err != nil {
					t.Fatalf("seed %s: %v", d, err)
				}
			}
			in.configDir = dir
			f, got := consoleStateFinding(in)
			if got != tc.want {
				t.Fatalf("a config directory holding %v reported=%v, want %v (%v)", append(tc.files, tc.dirs...), got, tc.want, f)
			}
			if tc.names != "" && !strings.Contains(fmt.Sprint(f.attrs), tc.names) {
				t.Errorf("the line does not name the file at risk (%s): %v", tc.names, f.attrs)
			}
		})
	}
}

func TestComposeVersionFinding(t *testing.T) {
	added := map[int]string{3: "a read-only mount of the index volume"}
	for _, tc := range []struct {
		name    string
		value   string
		shipped int
		want    bool
		mention string
	}{
		{name: "no version passed", value: "", shipped: 3},
		{name: "same version", value: "3", shipped: 3},
		{name: "file is newer than the binary", value: "4", shipped: 3},
		{name: "not a number", value: "v3", shipped: 3},
		{name: "file is behind", value: "2", shipped: 3, want: true, mention: "a read-only mount of the index volume"},
		{name: "file is behind, nothing recorded", value: "1", shipped: 2, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := driftInputs{
				getenv:         func(string) string { return tc.value },
				shippedVersion: tc.shipped,
				versionAdded:   added,
			}
			f, ok := composeVersionFinding(in)
			if ok != tc.want {
				t.Fatalf("BINTRAIL_COMPOSE_VERSION=%q against %d reported=%v, want %v", tc.value, tc.shipped, ok, tc.want)
			}
			if tc.mention != "" && !strings.Contains(fmt.Sprint(f.attrs), tc.mention) {
				t.Errorf("the line does not name what is missing: %v", f.attrs)
			}
		})
	}
}

// TestComposeVersionMatchesTheBinary: three places carry the number and all
// three have to agree, or the daemon compares against a file it is not
// shipping.
func TestComposeVersionMatchesTheBinary(t *testing.T) {
	doc := readComposeDriftFile(t)
	if doc.Version != bundledComposeVersion {
		t.Errorf("x-bintrail-compose-version is %d in %s and bundledComposeVersion is %d; bump them together",
			doc.Version, composePath, bundledComposeVersion)
	}
	raw, ok := composeConsoleEnvValue(t, "BINTRAIL_COMPOSE_VERSION")
	if !ok {
		t.Fatalf("%s does not pass BINTRAIL_COMPOSE_VERSION to the %s service, so the daemon can never tell the file's age", composePath, composeService)
	}
	if raw != strconv.Itoa(bundledComposeVersion) {
		t.Errorf("the %s service is passed BINTRAIL_COMPOSE_VERSION=%s, want %d", composeService, raw, bundledComposeVersion)
	}
	for v := range composeVersionAdded {
		if v > bundledComposeVersion {
			t.Errorf("composeVersionAdded describes version %d, which this build does not ship", v)
		}
	}
	// And the complement: every version ABOVE the first has to say what it
	// added, or the finding degrades to "your file is behind" with nothing
	// named, which is the diff-two-files problem the version key exists to
	// remove. Version 1 is the first numbered file and has nothing before it,
	// so the loop starts at 2, the first version with an entry (the
	// host.docker.internal mapping). Every later bump has to add its own, and
	// a bump is exactly when nobody is looking at this file.
	for v := 2; v <= bundledComposeVersion; v++ {
		if _, ok := composeVersionAdded[v]; !ok {
			t.Errorf("compose version %d has no composeVersionAdded entry, so a stack on %d is told its file is behind and not what that costs",
				v, v-1)
		}
	}
}

func TestParseMountinfo(t *testing.T) {
	// Line 1 carries an optional field ("shared:1") before the separator, which
	// is what makes a fixed-offset read of the filesystem type wrong. Line 2
	// has none. Line 3 has an escaped space in the mount point.
	const sample = `25 30 0:23 / / rw,relatime shared:1 - overlay overlay rw,lowerdir=/a
30 25 0:24 / /var/lib/bintrail rw,relatime - ext4 /dev/sda1 rw
31 25 0:25 / /mnt/two\040words rw - tmpfs tmpfs rw
short line
`
	got := parseMountinfo(strings.NewReader(sample))
	want := []mountEntry{
		{point: "/", fstype: "overlay"},
		{point: "/var/lib/bintrail", fstype: "ext4"},
		{point: "/mnt/two words", fstype: "tmpfs"},
	}
	if len(got) != len(want) {
		t.Fatalf("parsed %d mounts, want %d: %v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("mount %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

// TestCoveringMountComparesWholeComponents is the bug this compose file would
// have hit on day one: it mounts /var/lib/bintrail AND
// /var/lib/bintrail-index-ro, and a raw string prefix match puts the second
// inside the first.
func TestCoveringMountComparesWholeComponents(t *testing.T) {
	mounts := []mountEntry{
		{point: "/", fstype: "overlay"},
		{point: "/var/lib/bintrail", fstype: "ext4"},
	}
	m, ok := coveringMount("/var/lib/bintrail-index-ro/mysql.ibd", mounts)
	if !ok || m.point != "/" {
		t.Errorf("/var/lib/bintrail-index-ro resolved onto %+v; it is not inside /var/lib/bintrail", m)
	}
	if m, _ := coveringMount("/var/lib/bintrail/console-auth.yaml", mounts); m.point != "/var/lib/bintrail" {
		t.Errorf("a file on the state volume resolved onto %+v", m)
	}
}

func TestEphemeralPaths(t *testing.T) {
	mounts := []mountEntry{
		{point: "/", fstype: "overlay"},
		{point: "/var/lib/bintrail", fstype: "ext4"},
		{point: "/tmp", fstype: "tmpfs"},
	}
	for path, want := range map[string]bool{
		"/var/lib/bintrail/console-auth.yaml":     false,
		"/home/bintrail/.config/bintrail/x.yaml":  true,
		"/tmp/console-auth.yaml":                  true,
		"/var/lib/bintrail-index-ro/console.yaml": true,
	} {
		if got := ephemeral(path, mounts); got != want {
			t.Errorf("ephemeral(%q) = %v, want %v", path, got, want)
		}
	}
}

func removeMount(mounts []mountEntry, point string) []mountEntry {
	out := make([]mountEntry, 0, len(mounts))
	for _, m := range mounts {
		if m.point != point {
			out = append(out, m)
		}
	}
	return out
}

// The upgrade recipe must FETCH beside the operator's compose file.
//
// Every copy of it (two docs, the compose header, and the runtime `fix` line
// this package logs) says to re-download the file and merge your own edits
// back into it. `curl -fsSLO` writes docker-compose.yml in place, so by the
// time the reader gets to the merge step their edits are gone: a published
// port, the build toggle, a service they added. That is the same silent
// configuration loss #1529 is about, on the path #1529 wrote, so it is pinned
// where all four copies can be read at once.
//
// The scan is bounded to the UPGRADE spans. A first install downloads straight
// to docker-compose.yml and is right to: there is nothing there to lose.
// The recipe does end by moving the reviewed file into place, which is an
// overwrite and the intended one; what must never happen is the DOWNLOAD
// landing there, before the reader has seen what they were about to keep. So
// this is named for the fetch, like the helper it calls.
func TestUpgradeRecipeFetchesBesideTheOperatorsFile(t *testing.T) {
	for _, tc := range []struct {
		file, from, to string
	}{
		{file: "../docs/docker.md", from: "### Upgrading the stack", to: "\n### "},
		{file: "../docs/install.md", from: "**Upgrading later takes three commands", to: "\n## "},
		{file: "../docs/upgrade.md", from: "## 3. Docker Compose", to: "\n## "},
		{file: composePath, from: "# === UPGRADING", to: "x-bintrail-compose-version"},
	} {
		t.Run(tc.file, func(t *testing.T) {
			data, err := os.ReadFile(tc.file)
			if err != nil {
				t.Fatalf("read %s: %v", tc.file, err)
			}
			span, ok := textSpan(string(data), tc.from, tc.to)
			if !ok {
				t.Fatalf("%s has no upgrade section starting %q; this guard reads that span and would otherwise pass vacuously", tc.file, tc.from)
			}
			assertNonDestructiveFetch(t, tc.file, span)
		})
	}
	// The runtime line the daemon logs carries the same recipe.
	t.Run("composeDownload", func(t *testing.T) {
		assertNonDestructiveFetch(t, "composeDownload", composeDownload)
	})
}

// textSpan returns the text from the first occurrence of from up to the next
// occurrence of to after it.
func textSpan(text, from, to string) (string, bool) {
	i := strings.Index(text, from)
	if i < 0 {
		return "", false
	}
	rest := text[i+len(from):]
	if j := strings.Index(rest, to); j >= 0 {
		return text[i : i+len(from)+j], true
	}
	return text[i:], true
}

// composeFileURL is what a real fetch of the compose file names. Matching on
// the URL rather than on the word "curl" is what keeps this from passing on
// PROSE: "re-download docker-compose.yml (the curl in Quick start)" contains
// both words, names no command, and once satisfied the found counter it let a
// whole page with no recipe in it read as covered.
const composeFileURL = "https://raw.githubusercontent.com/dbtrail/dbtrail/main/docker-compose.yml"

// assertNonDestructiveFetch fails when a curl of the compose file in this text
// would land on docker-compose.yml itself.
func assertNonDestructiveFetch(t *testing.T, where, text string) {
	t.Helper()
	found := 0
	for _, line := range strings.Split(text, "\n") {
		if !strings.Contains(line, "curl") || !strings.Contains(line, composeFileURL) {
			continue
		}
		found++
		if out := curlOutputPath(line); out == "docker-compose.yml" {
			t.Errorf("%s: %q writes docker-compose.yml in place, so the edits the reader is then told to merge are already gone",
				where, strings.TrimSpace(line))
		}
	}
	if found == 0 {
		t.Errorf("%s carries no command that downloads the compose file, so an operator reading it either "+
			"never re-downloads or invents their own recipe, and this guard read nothing", where)
	}
}

// curlOutputPath returns the file a curl command line writes, or "" when it
// writes to stdout. The forms it reads:
//
//   - `-o NAME` / `--output NAME` / `--output=NAME`
//   - a flag cluster carrying O (`-fsSLO`), which writes the URL's basename
//   - a cluster carrying lowercase o (`-fsSLo NAME`), which takes the next
//     argument exactly like a bare -o
//   - a SHELL REDIRECT (`> NAME`, `>NAME`, `>> NAME`), which curl knows
//     nothing about and which overwrites just as completely
//
// NOT read, and worth saying plainly rather than implying this is complete:
// `| tee NAME`, a numbered redirect (`1> NAME`), and anything hidden behind a
// wrapper (`sh -c "..."`, a variable holding the filename). None of those
// appear in the tree today; a recipe written in one of them would pass this
// guard, so the guard is a floor, not a proof.
func curlOutputPath(line string) string {
	fields := strings.Fields(line)
	for i, f := range fields {
		if f == ">" || f == ">>" {
			if i+1 < len(fields) {
				return fields[i+1]
			}
			return ""
		}
		if strings.HasPrefix(f, ">") {
			return strings.TrimPrefix(strings.TrimPrefix(f, ">"), ">")
		}
	}
	for i, f := range fields {
		if f == "-o" || f == "--output" {
			if i+1 < len(fields) {
				return fields[i+1]
			}
			return ""
		}
		if v, ok := strings.CutPrefix(f, "--output="); ok {
			return v
		}
	}
	for i, f := range fields {
		if !strings.HasPrefix(f, "-") || strings.HasPrefix(f, "--") {
			continue
		}
		if strings.ContainsRune(f, 'O') {
			for _, g := range fields {
				if strings.HasPrefix(g, "http") {
					return g[strings.LastIndex(g, "/")+1:]
				}
			}
		}
		if strings.ContainsRune(f, 'o') && i+1 < len(fields) {
			return fields[i+1]
		}
	}
	return ""
}

// TestIndexDatadirRemedyFollowsTheEntrypointsOwnCondition.
//
// The entrypoint exports BINTRAIL_INDEX_DATADIR_RO inside `[ -z "$INDEX_DSN" ]`
// only, the same branch that builds the bundled DSN. So an operator who sets
// INDEX_DSN in .env and points it at the bundled index skips the export even on
// a CURRENT file: the finding's fact is still true (free space is not
// measurable) but "download the current file" is the wrong remedy, and sending
// someone to re-download a file they already have is how a warning stops being
// read. The remedy has to split on the same condition the script does.
func TestIndexDatadirRemedyFollowsTheEntrypointsOwnCondition(t *testing.T) {
	base := wiredStack(t)
	dsn := base.indexDSN
	for _, tc := range []struct {
		name     string
		indexDSN string
		wants    string
		rejects  string
	}{
		{
			name:    "the stack built the DSN, so the file is what is behind",
			wants:   "docker-compose.yml.new",
			rejects: "INDEX_DSN",
		},
		{
			name:     "the operator's own INDEX_DSN names the bundled index",
			indexDSN: dsn,
			wants:    "INDEX_DSN",
			rejects:  "docker-compose.yml.new",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := base
			in.getenv = func(k string) string {
				if k == "INDEX_DSN" {
					return tc.indexDSN
				}
				return "" // the export never ran
			}
			in.isDir = func(string) bool { return false }
			f, ok := indexDatadirFinding(in)
			if !ok {
				t.Fatal("free disk space is not measurable here and nothing was reported")
			}
			fix := fmt.Sprint(f.attrs)
			if !strings.Contains(fix, tc.wants) {
				t.Errorf("the remedy never mentions %q: %s", tc.wants, fix)
			}
			if strings.Contains(fix, tc.rejects) {
				t.Errorf("the remedy mentions %q, which is not what this shape needs: %s", tc.rejects, fix)
			}
		})
	}
}

// TestCurlOutputPath pins the parser the guard above rests on, including the
// form it used to be blind to: a shell redirect. `curl -fsSL <url> >
// docker-compose.yml` destroys the operator's file exactly like `-fsSLO` does,
// and reading it as "writes to stdout" let the whole suite stay green over a
// recipe that eats the edits it then tells the reader to merge.
func TestCurlOutputPath(t *testing.T) {
	const url = "https://raw.githubusercontent.com/dbtrail/dbtrail/main/docker-compose.yml"
	for _, tc := range []struct{ line, want string }{
		{line: "curl -fsSLO " + url, want: "docker-compose.yml"},
		{line: "curl -O " + url, want: "docker-compose.yml"},
		{line: "curl -fsSL -o docker-compose.yml.new " + url, want: "docker-compose.yml.new"},
		{line: "curl -fsSL --output docker-compose.yml.new " + url, want: "docker-compose.yml.new"},
		{line: "curl -fsSL " + url + " > docker-compose.yml", want: "docker-compose.yml"},
		{line: "curl -fsSL " + url + " >docker-compose.yml", want: "docker-compose.yml"},
		{line: "curl -fsSL " + url + " >> docker-compose.yml", want: "docker-compose.yml"},
		{line: "curl -fsSL " + url + " > docker-compose.yml.new", want: "docker-compose.yml.new"},
		// Lowercase o inside a cluster takes the NEXT argument, the way -o
		// does; the cluster loop used to test for capital O only.
		{line: "curl -fsSLo docker-compose.yml " + url, want: "docker-compose.yml"},
		{line: "curl -fsSLo docker-compose.yml.new " + url, want: "docker-compose.yml.new"},
		{line: "curl -fsSL --output=docker-compose.yml " + url, want: "docker-compose.yml"},
		{line: "curl -fsSL " + url, want: ""},
	} {
		t.Run(tc.line, func(t *testing.T) {
			if got := curlOutputPath(tc.line); got != tc.want {
				t.Errorf("curlOutputPath(%q) = %q, want %q", tc.line, got, tc.want)
			}
		})
	}
}

// TestIndexDatadirFindingConsultsTheMountTable.
//
// The line used to end "and this one has no read-only mount of its data
// volume" while the code had only looked at an environment variable. The mount
// table was one field away and never read, so the sentence asserted something
// the process had not established, in a package whose whole argument is that
// it never guesses. The two states are also different problems with different
// fixes, so they are worth telling apart.
func TestIndexDatadirFindingConsultsTheMountTable(t *testing.T) {
	if got := composeEntrypointDatadirRO(t); got != composeIndexDatadirRO {
		t.Fatalf("the compose entrypoint exports BINTRAIL_INDEX_DATADIR_RO=%s and this package names %s; "+
			"the finding would report a path the stack does not mount", got, composeIndexDatadirRO)
	}
	base := wiredStack(t)
	for _, tc := range []struct{ name, wants string }{
		{name: "mounted", wants: "is mounted"},
		{name: "not mounted", wants: "not mounted"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := base
			in.getenv = func(string) string { return "" }
			in.isDir = func(string) bool { return false }
			if tc.name == "not mounted" {
				in.mounts = removeMount(in.mounts, composeIndexDatadirRO)
			}
			f, ok := indexDatadirFinding(in)
			if !ok {
				t.Fatal("free disk space is not measurable here and nothing was reported")
			}
			if !strings.Contains(fmt.Sprint(f.attrs), tc.wants) {
				t.Errorf("the report does not say whether the mount is there (%q): %v", tc.wants, f.attrs)
			}
			if strings.Contains(f.msg, "has no read-only mount") {
				t.Errorf("the message asserts the mount is absent; only the attributes establish that: %s", f.msg)
			}
		})
	}
	t.Run("the operator's own INDEX_DSN still needs the mount", func(t *testing.T) {
		in := base
		in.getenv = func(k string) string {
			if k == "INDEX_DSN" {
				return base.indexDSN
			}
			return ""
		}
		in.isDir = func(string) bool { return false }
		in.mounts = removeMount(in.mounts, composeIndexDatadirRO)
		f, ok := indexDatadirFinding(in)
		if !ok {
			t.Fatal("nothing reported")
		}
		// Pointing a variable at a path the operator's file does not mount
		// fixes nothing, so the remedy has to name the mount as well.
		if !strings.Contains(fmt.Sprint(f.attrs), "docker-compose.yml.new") {
			t.Errorf("the remedy sends the operator to a path their file does not mount, with no way to get it: %v", f.attrs)
		}
	})
}

// TestConsoleStateFindingReportsAResolvedPathWithNoFileYet is shape 1 on its
// own, and it needed its own test: every other case here writes the file into
// the config directory, so shape 2 names it too and deleting shape 1 outright
// left the suite green.
//
// This is the docs table's own top row, not a corner: a compose file that
// predates the token override, on a stack where nobody has generated a token
// yet. There is no file for shape 2 to see, and the finding still has to name
// the token, because the loss it is about has not happened yet. Reporting only
// what already exists would mean warning after the state is gone.
func TestConsoleStateFindingReportsAResolvedPathWithNoFileYet(t *testing.T) {
	in := wiredStack(t)
	in.state[2].path = filepath.Join(in.configDir, "console-mcp-token.yaml")
	// Deliberately NOT written. The config directory holds what a released
	// image puts there and no console state at all.
	if held := consoleStateInDir(in.configDir); len(held) != 0 {
		t.Fatalf("the config directory already holds console state %v; this test is about the case where it holds none", held)
	}

	got := composeDriftFindings(in)
	if len(got) != 1 {
		t.Fatalf("want exactly the console-state finding, got %d: %v", len(got), got)
	}
	attrs := fmt.Sprint(got[0].attrs)
	if !strings.Contains(attrs, "the AI connection token") || !strings.Contains(attrs, "console-mcp-token.yaml") {
		t.Errorf("the finding does not name the state path that will not survive: %s", attrs)
	}
}
