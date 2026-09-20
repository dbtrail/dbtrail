package consoleapp

// Stack drift: upgrading the images does not upgrade the compose file (#1529).
//
// `docker compose pull && docker compose up -d` replaces the containers and
// leaves docker-compose.yml alone, because that file belongs to the operator
// and was downloaded once. Everything only a compose file can carry (volumes,
// mounts, ports, profiles) therefore stays frozen at whatever the file said
// the day it was downloaded, while the binaries move on. The worst of it is
// silent: without a durable home, console state lands in the container's
// writable layer and every recreate deletes it, and a console that starts with
// no state is in a perfectly legitimate state and warns about nothing.
//
// So the daemon says it once, at startup, on stderr, and never again: no card
// in the UI, no timer, no repeat. Quiet when everything is wired.
//
// `watch` only. The report is wired at upConsoleConfig, which `serve` does not
// call, so a read-only console reports nothing about its stack. That is the
// right scope for what this checks and not an oversight: the bundled compose
// stack runs `watch`, the index findings are about a capture daemon's own
// index, and `serve` is commonly run by hand against someone else's index,
// where a line about a compose file names nothing the reader has.
//
// Every finding here is evidence-based. Nothing is inferred from "this looks
// like Docker": each one names a fact the process can observe about itself,
// and each one stays silent when the fact cannot be established. The cost of a
// false alarm is higher than the cost of silence, because an operator who
// learns to ignore one line ignores the next one too. That is not an
// aspiration: the first version of the console-state check false-alarmed on
// the reference stack in this repo, because it read "the config directory
// holds anything" as evidence and every release build writes its telemetry
// spool there. What caught it was making the fixture derive from what the
// image actually writes; what fixed it was making the evidence specific to
// the claim. Both are worth keeping.

import (
	"bufio"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/console"
)

// bundledComposeVersion is the version of the docker-compose.yml this build
// ships: the `x-bintrail-compose-version` key at the top of that file, which
// the file also passes to this service as BINTRAIL_COMPOSE_VERSION. Bump the
// two together (TestComposeVersionMatchesTheBinary asserts they agree) and add
// a composeVersionAdded entry saying what the new number buys, or the daemon
// can only tell an operator that their file is old and not what it is missing.
const bundledComposeVersion = 1

// composeVersionAdded names, per version, the wiring that version added. Only
// versions ABOVE the operator's are read, so version 1 (the first numbered
// file) has no entry: there is nothing before it to be missing.
var composeVersionAdded = map[int]string{}

// composeIndexHost is the bundled index MySQL's service name in
// docker-compose.yml. A DSN pointing at it is this process saying it is using
// the bundled index, in the operator's own words.
const composeIndexHost = "index-mysql"

// composeIndexDatadirRO is where the bundled stack mounts the index data
// volume read-only, and what its entrypoint points BINTRAIL_INDEX_DATADIR_RO
// at. Named here so the report can say whether that mount is actually present
// rather than assuming it from the variable being unset; the compose file is
// the source of truth and a test asserts the two agree.
const composeIndexDatadirRO = "/var/lib/bintrail-index-ro"

// composeDownload is the one-line fix every finding ends with. It fetches
// BESIDE the operator's file, never over it: the same line tells them to merge
// their own edits into the new file, and an in-place download has already
// deleted those edits by the time they read that.
const composeDownload = "download the current docker-compose.yml next to yours, merge your own edits into it, " +
	"then move it into place: curl -fsSL -o docker-compose.yml.new " +
	"https://raw.githubusercontent.com/dbtrail/dbtrail/main/docker-compose.yml"

// mountEntry is one line of /proc/self/mountinfo, reduced to the two fields
// this file reads.
type mountEntry struct {
	point  string
	fstype string
}

// parseMountinfo reads /proc/self/mountinfo.
//
// The format is positional up to a point and then not: fields 0-5 are fixed
// (mount id, parent id, device, root, MOUNT POINT, options), followed by zero
// or more optional fields, terminated by a single "-", and only THEN the
// filesystem type. Indexing a fixed offset for the type reads an optional
// field ("shared:1") on any host that has one, so the separator is found
// first. Mount points with spaces, tabs, newlines or backslashes are
// octal-escaped by the kernel and are unescaped here.
func parseMountinfo(r io.Reader) []mountEntry {
	var out []mountEntry
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) < 7 {
			continue
		}
		sep := slices.Index(fields, "-")
		if sep < 6 || sep+1 >= len(fields) {
			continue
		}
		out = append(out, mountEntry{
			point:  unescapeMountPath(fields[4]),
			fstype: fields[sep+1],
		})
	}
	return out
}

var mountPathUnescaper = strings.NewReplacer(`\040`, " ", `\011`, "\t", `\012`, "\n", `\134`, `\`)

func unescapeMountPath(s string) string { return mountPathUnescaper.Replace(s) }

// readMountinfo returns the process's mount table, or nothing at all where
// there is no procfs to read it from (macOS, BSD, a scrubbed container).
// Nothing means "cannot tell", and every caller stays silent on that.
func readMountinfo() []mountEntry {
	f, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return nil
	}
	defer f.Close()
	return parseMountinfo(f)
}

// pathUnder reports whether path is dir or lives inside it, comparing whole
// path components. strings.HasPrefix on the raw strings is wrong here and this
// very compose file proves it: it mounts both /var/lib/bintrail and
// /var/lib/bintrail-index-ro, and a raw prefix match attributes the second to
// the first.
func pathUnder(dir, path string) bool {
	dir = filepath.Clean(dir)
	path = filepath.Clean(path)
	if dir == "/" {
		return strings.HasPrefix(path, "/")
	}
	return path == dir || strings.HasPrefix(path, dir+string(filepath.Separator))
}

// coveringMount returns the mount a path resolves onto: the longest mount
// point that contains it. Later entries win ties, since a mount stacked on an
// existing mount point is the one in effect.
func coveringMount(path string, mounts []mountEntry) (mountEntry, bool) {
	var best mountEntry
	found := false
	for _, m := range mounts {
		if !pathUnder(m.point, path) {
			continue
		}
		if !found || len(filepath.Clean(m.point)) >= len(filepath.Clean(best.point)) {
			best, found = m, true
		}
	}
	return best, found
}

// onImageLayer reports whether this process's root filesystem is a container
// image layer, and whether that could be established at all.
//
// This is the container evidence every finding below is gated on, and it is a
// FACT rather than a guess: an overlay root is the image layer plus the
// writable layer that `docker compose up -d` throws away when it recreates the
// container. On bare metal the root is ext4/xfs/btrfs and nothing here fires,
// which is correct: there is no compose file to be stale and no recreate to
// lose anything.
//
// Honest limit: a container on a btrfs or zfs storage driver has a root of
// that type and reads as "cannot tell", so the daemon stays quiet there.
func onImageLayer(mounts []mountEntry) (bool, bool) {
	root, ok := coveringMount("/", mounts)
	if !ok {
		return false, false
	}
	return root.fstype == "overlay" || root.fstype == "overlayfs", true
}

// ephemeral reports whether a path is destroyed when the container is
// recreated: it is on the writable layer (no mount of its own) or on memory
// (tmpfs). Only meaningful once onImageLayer says yes.
//
// The match is LEXICAL, so a config directory that is a symlink into a mounted
// volume reads as ephemeral and would be reported although it survives. That
// cannot happen on the shipped stack, whose mount targets are absolute paths,
// and resolving symlinks here would mean stat'ing a directory that may not
// exist yet, which answers a different question.
func ephemeral(path string, mounts []mountEntry) bool {
	m, ok := coveringMount(path, mounts)
	if !ok {
		return true // no mount table entry covers it, so nothing keeps it
	}
	if filepath.Clean(m.point) == "/" {
		return true
	}
	return m.fstype == "tmpfs" || m.fstype == "ramfs"
}

// statePath is one file the console keeps on disk, with the plain-words name
// used when reporting it.
type statePath struct {
	what string
	path string
}

// driftInputs is everything the check reads. Injected rather than read
// directly so the findings are a pure function of an observed environment: the
// tests build both the wired stack and the stale one and assert the two
// answers, on a machine that is neither.
type driftInputs struct {
	getenv   func(string) string
	indexDSN string
	isDir    func(string) bool
	// stateInDir lists the console state files a directory holds.
	stateInDir func(string) []string
	mounts     []mountEntry
	// state are the console state files this daemon will actually use, with
	// every override already applied.
	state []statePath
	// configDir is where console state lands when nothing overrides it.
	configDir string
	// shippedVersion and versionAdded describe the compose file this build
	// ships; parameters so the comparison is testable without a release.
	shippedVersion int
	versionAdded   map[int]string
}

// driftFinding is one consolidated line.
type driftFinding struct {
	msg   string
	attrs []any
}

// composeDriftFindings returns what this stack is missing, in the order the
// operator should read it. Empty means either everything is wired or nothing
// could be established, and those two are deliberately indistinguishable from
// the outside: both are silence.
func composeDriftFindings(in driftInputs) []driftFinding {
	inContainer, known := onImageLayer(in.mounts)
	if !known || !inContainer {
		return nil
	}
	var out []driftFinding
	if f, ok := composeVersionFinding(in); ok {
		out = append(out, f)
	}
	if f, ok := indexDatadirFinding(in); ok {
		out = append(out, f)
	}
	if f, ok := consoleStateFinding(in); ok {
		out = append(out, f)
	}
	return out
}

// composeVersionFinding compares the running compose file's version with the
// one this build ships.
//
// Optional on purpose: a file that passes no version says nothing about its
// age, and the daemon answers silence rather than a guess. That keeps every
// stack older than the numbering scheme on exactly today's behaviour, with the
// two evidence-based findings below carrying it instead. A file NEWER than
// this build is also silence: that is an image downgrade, not a stale file.
func composeVersionFinding(in driftInputs) (driftFinding, bool) {
	raw := strings.TrimSpace(in.getenv("BINTRAIL_COMPOSE_VERSION"))
	if raw == "" {
		return driftFinding{}, false
	}
	have, err := strconv.Atoi(raw)
	if err != nil || have >= in.shippedVersion {
		return driftFinding{}, false
	}
	attrs := []any{
		"compose_file_version", have,
		"current_version", in.shippedVersion,
		"fix", composeDownload,
	}
	var missing []string
	for v := have + 1; v <= in.shippedVersion; v++ {
		if what, ok := in.versionAdded[v]; ok {
			missing = append(missing, fmt.Sprintf("%d: %s", v, what))
		}
	}
	if len(missing) > 0 {
		attrs = append(attrs, "not_in_effect", strings.Join(missing, "; "))
	}
	return driftFinding{
		msg: "the docker-compose.yml running this stack is older than the one this version ships, " +
			"so wiring added since then is not in effect",
		attrs: attrs,
	}, true
}

// indexDatadirFinding fires when the bundled index is in effect and the
// read-only mount that measures its free disk space is not.
//
// The condition tracks the one internal/doctor/capacity.go applies
// (indexDatadirFreeFromEnv plus statfsDir): the variable must be set AND name
// an existing directory. Re-deriving it loosely would let this line claim disk
// space is unmeasurable while the capacity check measures it fine, and a
// warning contradicted by the page next to it is worse than no warning. One
// difference, in the silent direction: statfsDir also requires the statfs
// syscall itself to succeed, which this does not attempt. A directory that
// exists but cannot be statfs'd is measured by neither and reported by neither.
//
// The bundled index is evidence, not a guess: the DSN in effect names the
// compose file's own service, and the fix (a mount of that service's volume)
// only exists in that topology. An INDEX_DSN pointing somewhere ELSE never
// reaches here, which matches capacity.go, where the variable is deliberately
// not set for a bring-your-own index so no unrelated volume's free space is
// reported as the index's.
//
// The REMEDY then splits on the entrypoint's own condition, because the fact
// and the fix are not the same question. That script exports the variable
// inside `[ -z "$INDEX_DSN" ]` only, so an operator who sets INDEX_DSN in .env
// and points it at the bundled index skips the export on a CURRENT file too:
// free space really is unmeasurable, and telling them to download a file they
// already have is how a warning stops being read.
func indexDatadirFinding(in driftInputs) (driftFinding, bool) {
	cfg, err := mysql.ParseDSN(in.indexDSN)
	if err != nil || cfg.Net != "tcp" {
		return driftFinding{}, false
	}
	host := cfg.Addr
	if h, _, err := net.SplitHostPort(cfg.Addr); err == nil {
		host = h
	}
	if host != composeIndexHost {
		return driftFinding{}, false
	}
	dir := strings.TrimSpace(in.getenv("BINTRAIL_INDEX_DATADIR_RO"))
	if dir != "" && in.isDir(dir) {
		return driftFinding{}, false
	}
	// What was actually observed about the mount, rather than inferred from the
	// variable. The two states are different problems: a mount that is there
	// needs the variable, a mount that is not needs the compose file first.
	mounted := mountExists(composeIndexDatadirRO, in.mounts)
	mountState := composeIndexDatadirRO + " is not mounted in this container"
	if mounted {
		mountState = composeIndexDatadirRO + " is mounted, but BINTRAIL_INDEX_DATADIR_RO does not name a directory this process can read"
	}

	fix := "the current docker-compose.yml mounts the index volume read-only at " +
		composeIndexDatadirRO + " and points BINTRAIL_INDEX_DATADIR_RO at it, so " + composeDownload
	if strings.TrimSpace(in.getenv("INDEX_DSN")) != "" {
		// The entrypoint exports the variable only when it builds the address
		// itself, so this operator has to. Naming the path is not enough: on a
		// file older than that mount it points at nothing, and a remedy that
		// cannot work is worse than none.
		fix = "your own INDEX_DSN points at the bundled index, and the stack sets BINTRAIL_INDEX_DATADIR_RO " +
			"only when it builds that address itself. Set it too, to a read-only mount of the index data " +
			"volume (" + composeIndexDatadirRO + " in the current stack), or drop INDEX_DSN and let the " +
			"stack build the address."
		if !mounted {
			fix += " Your compose file does not mount that path, so it needs the current file first: " + composeDownload
		}
	}
	attrs := []any{"read_only_index_mount", mountState, "fix", fix}
	if dir != "" {
		attrs = append([]any{"BINTRAIL_INDEX_DATADIR_RO", dir, "problem", "not a directory in this container"}, attrs...)
	}
	return driftFinding{
		msg: "free disk space for the index cannot be measured, so the preflight and the Storage page " +
			"both report it as not measurable. The bundled index runs in its own container, so measuring " +
			"it needs a read-only mount of that container's data volume and BINTRAIL_INDEX_DATADIR_RO " +
			"pointing at it",
		attrs: attrs,
	}, true
}

// mountExists reports whether a path is itself a mount point.
func mountExists(path string, mounts []mountEntry) bool {
	for _, m := range mounts {
		if filepath.Clean(m.point) == filepath.Clean(path) {
			return true
		}
	}
	return false
}

// consoleStateFinding fires when console state is stored where a container
// recreate destroys it. Two shapes, one line, because the fix is the same and
// the console is already noisy enough.
//
//  1. Some state files this daemon uses are on a durable mount and others are
//     not. Mixed is the shape of a compose file that predates one of the
//     overrides: the deployment demonstrably has somewhere durable to put
//     console state, and one file did not get there. (All of them ephemeral is
//     NOT reported: with no durable mount anywhere this is as likely a
//     throwaway `docker run` as a stale stack, and guessing is what this file
//     refuses to do.)
//
//  2. The default config directory is ephemeral AND holds CONSOLE STATE. This
//     is the shape that reaches state no environment variable relocates: a
//     build that keeps its own console-*.yaml there is one recreate away from
//     losing it, and this package cannot name a file it does not write.
//
//     It is an ALLOW-list (console-*), and that is load-bearing rather than
//     tidy. "The directory holds anything" was the first spelling and it
//     false-alarmed on the shipped stack: telemetry is compiled into every
//     release build, its consent defaults on, and it writes its spool and
//     consent file into this very directory, so a wired stack that is merely
//     restarted (not recreated) grew an entry and got told to mount a volume
//     it already has. A deny-list of that one name would have closed the case
//     and reopened it on the next writer that lands there, of which there are
//     already others (`config init --global`, `generate-key`). The claim being
//     made is "saved state is at risk", so the evidence has to be console
//     state.
//
//     Ordering limit, deliberately not fixed: right after the recreate that
//     destroyed such a store the directory is EMPTY, so this is silent on the
//     event itself and can only fire on a later start within that container's
//     life (a restart, a reboot, a crash under `restart: unless-stopped`).
//     Reporting an empty directory instead would warn every correctly wired
//     stack about a file that does not exist, which is the trade this refuses.
func consoleStateFinding(in driftInputs) (driftFinding, bool) {
	var lost, named []string
	durable := false
	for _, s := range in.state {
		if ephemeral(s.path, in.mounts) {
			lost = append(lost, fmt.Sprintf("%s (%s)", s.what, s.path))
			named = append(named, filepath.Base(s.path))
			continue
		}
		durable = true
	}
	if !durable {
		lost, named = nil, nil
	}
	if in.configDir != "" && ephemeral(in.configDir, in.mounts) {
		// Skip what the loop above already named. A state file whose override
		// is missing lands in this very directory, so both shapes see the same
		// file, and naming it twice in one line reads as two problems.
		var held []string
		for _, name := range in.stateInDir(in.configDir) {
			if !slices.Contains(named, name) {
				held = append(held, name)
			}
		}
		if len(held) > 0 {
			lost = append(lost, "settings saved in the web interface in "+in.configDir+" ("+strings.Join(held, ", ")+")")
		}
	}
	if len(lost) == 0 {
		return driftFinding{}, false
	}
	return driftFinding{
		msg: "the settings saved in the web interface are stored inside the container, so the next upgrade that recreates it " +
			"deletes them. Nothing will report the loss: a daemon that starts with no saved settings " +
			"looks exactly like a fresh install",
		attrs: []any{
			"stored_in_the_container", strings.Join(lost, ", "),
			"fix", "mount a volume that covers these paths, or " + composeDownload,
		},
	}, true
}

// consoleStateFileName reports whether a file in the config directory is
// console state.
//
// The console names every file it keeps there `console-<what>`:
// console-servers.yaml, console-auth.yaml, console-mcp-token.yaml, plus the
// console-verify-history.json / console-baseline-history.json pair it writes
// beside the registry. The PREFIX is what this reads, not that list, so the
// histories are covered without being named and so is a console-* file a build
// this package cannot name keeps there, which is the case shape 2 exists for.
// Everything else in that directory belongs to something other than the
// console (the telemetry spool and consent file, a global config.env, a
// generated dump key) and is either rebuildable or not the console's to warn
// about.
func consoleStateFileName(name string) bool {
	return strings.HasPrefix(name, "console-")
}

// consoleStateInDir lists the console state files a directory holds, sorted.
// An unreadable or missing directory holds none, so a permission problem
// cannot manufacture a finding.
func consoleStateInDir(dir string) []string {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	var out []string
	for _, e := range entries {
		if !e.IsDir() && consoleStateFileName(e.Name()) {
			out = append(out, e.Name())
		}
	}
	slices.Sort(out)
	return out
}

func isExistingDir(path string) bool {
	fi, err := os.Stat(path)
	return err == nil && fi.IsDir()
}

// composeDriftWarnOnce keeps this to one report per process. `watch` has two
// entry points and both reach upConsoleConfig; a restart is the only thing
// that should print it again.
var composeDriftWarnOnce sync.Once

// composeDriftReporter is the seam upConsoleConfig calls. A var so a test can
// prove the call is wired at all: what this reports is invisible on a healthy
// stack and on any developer machine, so deleting the call site would be
// invisible too.
var composeDriftReporter = warnComposeDrift

// warnComposeDrift reports stack drift once, at startup, on the daemon's log.
func warnComposeDrift(indexDSN string, opts consoleOpts) {
	composeDriftWarnOnce.Do(func() {
		for _, f := range composeDriftFindings(liveDriftInputs(indexDSN, opts)) {
			slog.Warn(f.msg, f.attrs...)
		}
	})
}

// liveDriftInputs observes this process. The state paths resolve exactly the
// way console.New resolves them (an empty override means the default), so what
// is reported is what the daemon will actually open.
func liveDriftInputs(indexDSN string, opts consoleOpts) driftInputs {
	pick := func(override string, def func() string) string {
		if override != "" {
			return override
		}
		return def()
	}
	return driftInputs{
		getenv:     os.Getenv,
		indexDSN:   indexDSN,
		isDir:      isExistingDir,
		stateInDir: consoleStateInDir,
		mounts:     readMountinfo(),
		state: []statePath{
			{what: "your username and password", path: pick(opts.AuthFile, console.DefaultAuthPath)},
			{what: "the servers you added", path: pick(opts.ServersFile, console.DefaultRegistryPath)},
			{what: "the AI connection token", path: pick(opts.MCPTokenFile, console.DefaultMCPTokenPath)},
		},
		configDir:      filepath.Dir(console.DefaultRegistryPath()),
		shippedVersion: bundledComposeVersion,
		versionAdded:   composeVersionAdded,
	}
}
