package cliapp

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	drivermysql "github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/mydumperlock"
)

// checkMydumperPrivileges is mydumperlock.CheckPrivileges behind a seam, so a
// test can observe the LOCK MODE this call site actually forwards. Without it
// the argument is unverifiable: every mode fails identically against an
// unreachable source, so hardcoding one here passes the whole suite while
// re-introducing #1381 — an operator who selected lock-all gets judged against
// ftwrl's requirements and told to grant BACKUP_ADMIN, which RDS refuses.
var checkMydumperPrivileges = mydumperlock.CheckPrivileges

var dumpCmd = &cobra.Command{
	Use:   "dump",
	Short: "Invoke mydumper to create a logical dump of the source MySQL instance",
	Long: `Invokes mydumper to create a logical dump of the source MySQL instance.
Only one dump may run at a time (enforced by a lockfile). Source connectivity is
verified before the output directory is touched. An existing output directory is
only cleared when it is empty or a recognizable prior dump; a recognizable prior
dump is moved aside and restored if this dump fails; a non-empty directory that
is not a dump is refused rather than deleted.`,
	RunE: runDump,
}

var (
	dmpSourceDSN     string
	dmpOutputDir     string
	dmpSchemas       string
	dmpTables        string
	dmpMydumperPath  string
	dmpMydumperImage string
	dmpThreads       int
	dmpLockMode      string
	dmpFormat        string
	dmpEncrypt       bool
	dmpEncryptKey    string
)

// dumpLockDir is a function returning the directory for the dump lockfile.
// It is a variable so tests can override it with a temp directory.
var dumpLockDir = os.TempDir

func init() {
	dumpCmd.Flags().StringVar(&dmpSourceDSN, "source-dsn", "", "DSN for the source MySQL server (required)")
	dumpCmd.Flags().StringVar(&dmpOutputDir, "output-dir", "", "Directory for mydumper output (required)")
	dumpCmd.Flags().StringVar(&dmpSchemas, "schemas", "", "Comma-separated schema filter (e.g. mydb,otherdb)")
	dumpCmd.Flags().StringVar(&dmpTables, "tables", "", "Comma-separated table filter (e.g. mydb.orders,mydb.items)")
	dumpCmd.Flags().StringVar(&dmpMydumperPath, "mydumper-path", "mydumper", "Path to the mydumper binary")
	dumpCmd.Flags().StringVar(&dmpMydumperImage, "mydumper-image", "mydumper/mydumper:latest", "Docker image for mydumper (used when no local binary is found)")
	dumpCmd.Flags().IntVar(&dmpThreads, "threads", 4, "Number of mydumper dump threads")
	dumpCmd.Flags().StringVar(&dmpLockMode, "lock-mode", string(baseline.DefaultLockMode),
		"How mydumper syncs its threads onto one instant: ftwrl (default, point-consistent; needs RELOAD/FLUSH_TABLES plus BACKUP_ADMIN on MySQL 8.0+), lock-all (point-consistent, needs only LOCK TABLES; the mode that works on managed MySQL such as RDS, where BACKUP_ADMIN cannot be granted), safe-no-lock (no extra privilege, ABORTS rather than emit a torn snapshot), no-lock (accepts a torn snapshot)")
	dumpCmd.Flags().StringVar(&dmpFormat, "format", "text", "Output format: text or json")
	dumpCmd.Flags().BoolVar(&dmpEncrypt, "encrypt", false, "Encrypt dump files at rest using AES-256-CBC and write an HMAC-SHA256 integrity sidecar (<file>.enc.hmac) per file (requires openssl on $PATH)")
	dumpCmd.Flags().StringVar(&dmpEncryptKey, "encrypt-key", "", "Path to encryption key file (default: ~/.config/bintrail/dump.key; generate with 'bintrail generate-key')")
	_ = dumpCmd.MarkFlagRequired("source-dsn")
	_ = dumpCmd.MarkFlagRequired("output-dir")
	bindCommandEnv(dumpCmd)

	rootCmd.AddCommand(dumpCmd)
}

// dumpMode indicates how mydumper will be invoked.
type dumpMode int

const (
	dumpModeLocal  dumpMode = iota // local binary
	dumpModeDocker                 // docker run
)

// dumpResolution holds the result of resolving how to invoke mydumper.
type dumpResolution struct {
	mode  dumpMode
	path  string // binary path (local) or docker path (docker)
	image string // docker image (only for dumpModeDocker)
}

// resolveMydumper determines how to invoke mydumper based on flag state.
// Priority: explicit --mydumper-path → $PATH lookup → Docker → error.
func resolveMydumper(cmd *cobra.Command) (dumpResolution, error) {
	if cmd.Flags().Changed("mydumper-path") {
		path, err := exec.LookPath(dmpMydumperPath)
		if err != nil {
			return dumpResolution{}, fmt.Errorf("mydumper not found at %q: %w", dmpMydumperPath, err)
		}
		return dumpResolution{mode: dumpModeLocal, path: path}, nil
	}

	if path, err := exec.LookPath("mydumper"); err == nil {
		if isShellScript(path) {
			slog.Warn("found mydumper on $PATH but it appears to be a shell script wrapper; skipping in favor of Docker",
				"path", path)
		} else {
			return dumpResolution{mode: dumpModeLocal, path: path}, nil
		}
	}

	dockerPath, err := exec.LookPath("docker")
	if err == nil {
		return dumpResolution{mode: dumpModeDocker, path: dockerPath, image: dmpMydumperImage}, nil
	}

	return dumpResolution{}, fmt.Errorf("mydumper not found on $PATH and Docker is not available; " +
		"install mydumper (https://github.com/mydumper/mydumper), install Docker, or use --mydumper-path")
}

// buildDockerArgs constructs the full argument slice for invoking mydumper via
// docker run. The output directory is bind-mounted at the same absolute path so
// downstream tools need no path translation. When encryptKeyPath is non-empty,
// the key file is also bind-mounted into the container. When defaultsFile is
// non-empty it is likewise bind-mounted read-only at the same path so the
// container's mydumper can read the source password from it via --defaults-file
// (keeping the secret off argv and out of `docker inspect`, #811).
func buildDockerArgs(image, outputDir, host string, mydumperArgs []string, encryptKeyPath, defaultsFile string) []string {
	absOutput, err := filepath.Abs(outputDir)
	if err != nil {
		absOutput = outputDir
	}

	args := []string{
		"run", "--rm",
		"--user", fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid()),
		"-v", absOutput + ":" + absOutput,
	}

	if encryptKeyPath != "" {
		absKey, err := filepath.Abs(encryptKeyPath)
		if err != nil {
			absKey = encryptKeyPath
		}
		args = append(args, "-v", absKey+":"+absKey+":ro")
	}

	if defaultsFile != "" {
		absDefaults, err := filepath.Abs(defaultsFile)
		if err != nil {
			absDefaults = defaultsFile
		}
		args = append(args, "-v", absDefaults+":"+absDefaults+":ro")
	}

	if isLocalhost(host) {
		if runtime.GOOS == "linux" {
			args = append(args, "--network", "host")
		} else {
			slog.Warn("source host is localhost but --network host only works on Linux; "+
				"use the Docker host IP (e.g. host.docker.internal) or set --source-dsn accordingly",
				"host", host, "os", runtime.GOOS)
		}
	}

	args = append(args, image, "mydumper")
	args = append(args, mydumperArgs...)
	return args
}

// isShellScript reports whether the file at path starts with a shebang (#!),
// indicating it is a script rather than a compiled binary.
func isShellScript(path string) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()
	var buf [2]byte
	n, err := f.Read(buf[:])
	return n == 2 && err == nil && buf[0] == '#' && buf[1] == '!'
}

// isLocalhost reports whether the host refers to the local machine.
func isLocalhost(host string) bool {
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}

func runDump(cmd *cobra.Command, args []string) error {
	if !cliutil.IsValidOutputFormat(dmpFormat) {
		return fmt.Errorf("invalid --format %q; must be text or json", dmpFormat)
	}

	// 0. Resolve encryption key path.
	var encryptKeyPath string
	if dmpEncrypt {
		var err error
		encryptKeyPath, err = resolveEncryptKey(dmpEncryptKey)
		if err != nil {
			return err
		}
	}

	// 1. Resolve how to invoke mydumper.
	res, err := resolveMydumper(cmd)
	if err != nil {
		return err
	}

	// 2. Parse source DSN.
	host, port, user, password, err := config.ParseSourceDSN(dmpSourceDSN)
	if err != nil {
		return err
	}

	// 2b. Validate source connectivity BEFORE the output directory is touched.
	// A wrong host/port/credential (or an unreachable server) must fail here —
	// never after the previous, only-good dump has already been destroyed
	// (#809). Injectable via pingSource so unit tests can stub it.
	if err := pingSource(dmpSourceDSN); err != nil {
		return fmt.Errorf("cannot connect to source; refusing to touch --output-dir %q: %w", dmpOutputDir, err)
	}

	// 3. Parse schema and table filters.
	schemas := cliutil.ParseSchemaList(dmpSchemas)
	tables := cliutil.ParseSchemaList(dmpTables)

	// Lock mode is resolved and its privileges probed BEFORE the dump lock and
	// before prepareDumpOutputDir moves a previous dump aside. Step 2b states
	// the rule (#809): a wrong credential must fail before the previous,
	// only-good dump is disturbed — and a missing RELOAD grant is that same
	// failure class. CheckPrivileges opens its own connection, so leaving it
	// below the rename would put a multi-second network round trip inside the
	// window where the good dump exists only under a .old-<pid>-<nanos> name
	// the operator has never seen.
	lockMode, lmErr := baseline.ParseLockMode(dmpLockMode)
	if lmErr != nil {
		return lmErr
	}
	supportsLockMode := true
	// knownOldMydumper is true ONLY when the --version probe ANSWERED and named
	// a build below the floor. It is deliberately not the negation of
	// supportsLockMode: that flag also drops when the version could not be read
	// at all, and the two states carry opposite evidence — one is "this build is
	// old", the other is "we know nothing about this build". The privilege
	// preflight below is the caller that must tell them apart (#1686).
	knownOldMydumper := false
	// Kept so a refusal can quote the REASON the version is unknown instead of
	// blaming an age nothing measured.
	var versionErr error
	var probed mydumperlock.Version // what --version printed, when knownOldMydumper
	if res.mode == dumpModeLocal {
		v, verErr := mydumperlock.ProbeVersion(res.path)
		switch {
		case errors.Is(verErr, mydumperlock.ErrNotRunnable):
			// A binary that cannot run --version will not complete a dump
			// either, so failing here loses nothing. Routing it into the
			// "unknown version" branch instead would make the first hard error
			// a privilege refusal naming BACKUP_ADMIN, for a binary that does
			// not execute at all (#1699).
			return fmt.Errorf("%w; fix or replace that binary (--mydumper-path), a dump runs the same executable", verErr)
		case verErr != nil:
			slog.Warn("could not determine mydumper version; omitting --sync-thread-lock-mode and --trx-tables for safety",
				"error", verErr)
			supportsLockMode = false
			versionErr = verErr
		case !v.SupportsLockMode():
			slog.Warn("mydumper version is older than 0.18; omitting --sync-thread-lock-mode and --trx-tables — the dump may hold heavier locks",
				"version", v.String())
			supportsLockMode = false
			knownOldMydumper = true
			probed = v
		}
	}
	// Refuse rather than silently ignore an explicit choice. Dropping the flag
	// is safe for the DEFAULT (pre-0.18 mydumper locks by default anyway, which
	// is why the omission is only a warning above), but an operator who typed
	// --lock-mode has a reason: safe-no-lock because they lack the privileges
	// FTWRL needs, or no-lock because they knowingly accept skew. Honouring
	// neither while reporting success is the silent-wrong-answer this whole
	// change exists to remove.
	//
	// The refusal splits by the SAME evidence the preflight below uses (#1686).
	// One message for both states asserted "this build does not accept
	// --sync-thread-lock-mode" about a binary whose version was never read —
	// which is how a modern mydumper came to be told it needed an upgrade. It
	// also matters that the preflight's own refusals offer `--lock-mode <mode>`
	// as the way out: an operator who takes that advice lands HERE, so this
	// text is the second half of that conversation and has to say something
	// true about why the flag cannot be sent.
	if !supportsLockMode && cmd.Flags().Changed("lock-mode") {
		if knownOldMydumper {
			return fmt.Errorf("--lock-mode %s needs mydumper %s or newer (this build is %s and does not accept --sync-thread-lock-mode); "+
				"upgrade mydumper, or point --mydumper-image at a newer pinned image", lockMode, mydumperlock.LockModeFloor, probed)
		}
		// Deliberately NOT suggesting --mydumper-image: reaching Docker mode
		// requires NO mydumper on $PATH (see resolveMydumper), so an operator
		// who got here through a local binary cannot act on it.
		return fmt.Errorf("--lock-mode %s cannot be honored: this mydumper's version could not be read (%w), so "+
			"--sync-thread-lock-mode is not being sent and mydumper would choose its own sync mode rather than %s. "+
			"Point --mydumper-path at a build whose --version this release can read", lockMode, versionErr, lockMode)
	}
	// Probe privileges BEFORE launching mydumper. Granting BACKUP_ADMIN without
	// RELOAD/FLUSH_TABLES does not make mydumper fail cleanly — the pinned build
	// SEGFAULTS (#800) — so the crash has to be made unreachable rather than
	// reported. This check used to live only on the console because the CLI
	// always passed NO_LOCK and could not reach the crash; #1377 made the
	// point-consistent mode the default here too, so the guard has to come
	// with it.
	//
	// Gated on knownOldMydumper, NOT on supportsLockMode (#1686). An UNREADABLE
	// version used to skip this, which inverted the guard: the case where we
	// know least about the binary switched off the check that exists precisely
	// because the failure is a crash and not a message. Anything reaching here
	// on that path is the DEFAULT mode, since an explicit --lock-mode was
	// already refused above.
	//
	// Be honest about what that costs: this refusal names `--lock-mode <mode>`
	// as the way out, and on the unreadable path that flag is exactly what the
	// refusal above rejects — so the operator needs TWO messages to learn the
	// real remedy is a readable mydumper. That is the deliberate trade: a
	// two-step but true chain, against a segfault that says nothing at all. It
	// is also why the refusal above had to stop claiming the build is old.
	//
	// A build we positively READ as taking no backup lock keeps skipping, on
	// purpose: requiresBackupAdmin decides from the SERVER's version, not
	// mydumper's, so demanding BACKUP_ADMIN from a build that never issues LOCK
	// INSTANCE FOR BACKUP would refuse a dump that works today. That is a live
	// configuration — Ubuntu 24.04 and Debian bookworm both package 0.10.1.
	// The skip used to cover every pre-0.18 build, which was wider than its own
	// reason: 0.16.3 and 1.0.3 both issue the lock (measured), so only builds
	// older than mydumperlock.LockInstanceExemptBelow are exempt now.
	if (!knownOldMydumper || probed.TakesBackupLock()) && lockMode.NeedsElevatedPrivileges() {
		if err := checkMydumperPrivileges(cmd.Context(), dmpSourceDSN, lockMode, mydumperlock.RemedyCLI, schemas); err != nil {
			return err
		}
	}

	// 4. Acquire dump lock — only one dump at a time.
	lockFile, err := acquireDumpLock()
	if err != nil {
		return fmt.Errorf("another dump is already running: %w", err)
	}
	defer releaseDumpLock(lockFile)

	// 5. Safely prepare the output directory (#809). Refuse to delete a
	// non-empty directory that is not a recognizable prior mydumper/bintrail
	// dump — a typo'd --output-dir (or a stray BINTRAIL_OUTPUT_DIR in a sibling
	// .bintrail.env) must never wipe an arbitrary tree, including baselines
	// that reconstruct/verify depend on. A recognizable prior dump is moved
	// aside (dir → dir.old) and only deleted once THIS dump succeeds, so a
	// failed dump restores the previous (only-good) output.
	prep, err := prepareDumpOutputDir(dmpOutputDir)
	if err != nil {
		return err
	}
	dumpSucceeded := false
	defer func() {
		if dumpSucceeded {
			prep.commit()
		} else {
			prep.rollback()
		}
	}()

	// 6. Probe mydumper version and build args.
	// --sync-thread-lock-mode and --trx-tables require mydumper >= 0.18 (see
	// mydumperlock.Version.SupportsLockMode). Distro apt packages ship older builds —
	// Ubuntu 24.04 and Debian bookworm both package upstream 0.10.1, whose
	// binary self-reports 0.10.0 — so we must not pass the flags
	// unconditionally or the dump fails (#219, #460). Docker mode is NOT
	// version-probed: a pinned --mydumper-image older than 0.18 fails with
	// "unknown option", which is why the docs' pin examples stay >= 0.18.
	// The source password must never reach mydumper's argv (visible in
	// `ps aux` / /proc/<pid>/cmdline and, under Docker, in `docker inspect`).
	// Docker mode delivers it via a 0600 defaults-file bind-mounted read-only;
	// local mode delivers it via MYSQL_PWD in the child env below (#811).
	var defaultsFile string
	if password != "" && res.mode == dumpModeDocker {
		var cleanup func()
		var derr error
		defaultsFile, cleanup, derr = writeMydumperDefaultsFile(password)
		if derr != nil {
			return derr
		}
		defer cleanup()
	}
	mydumperArgs := buildMydumperArgs(host, port, user, defaultsFile, dmpOutputDir, dmpThreads, schemas, tables, encryptKeyPath, supportsLockMode, lockMode)

	// 7. Build the final command depending on resolution mode.
	var c *exec.Cmd
	switch res.mode {
	case dumpModeDocker:
		dockerArgs := buildDockerArgs(res.image, dmpOutputDir, host, mydumperArgs, encryptKeyPath, defaultsFile)
		c = exec.CommandContext(cmd.Context(), res.path, dockerArgs...)
		slog.Info("starting dump via Docker", "image", res.image, "output_dir", dmpOutputDir)
	default:
		c = exec.CommandContext(cmd.Context(), res.path, mydumperArgs...)
		// Pass the password out of band so it stays off argv. MYSQL_PWD is
		// honored by the MySQL client library mydumper links against and sits
		// in the child's mode-0400 /proc/<pid>/environ, not world-readable
		// cmdline (#811).
		if password != "" {
			c.Env = append(os.Environ(), "MYSQL_PWD="+password)
		}
		slog.Info("starting dump", "path", res.path, "output_dir", dmpOutputDir)
	}

	// Always capture stderr into a buffer so the error message can include
	// the actual diagnostic — "exit status 1" by itself hides why mydumper
	// failed (auth plugin mismatch, missing privilege, full disk, etc.).
	// In text mode we ALSO passthrough to the user's terminal so they see
	// progress live; in JSON mode the caller is parsing stdout so we keep
	// stderr buffered only.
	var stderrBuf bytes.Buffer
	if dmpFormat != "json" {
		c.Stdout = os.Stdout
		c.Stderr = io.MultiWriter(os.Stderr, &stderrBuf)
	} else {
		c.Stderr = &stderrBuf
	}
	// Captured immediately before invoking mydumper so it approximates
	// mydumper's own dump-start instant, but as this process's UTC wall
	// clock rather than mydumper's (possibly non-UTC, possibly
	// containerized) local time. Recorded as a sidecar below so
	// `bintrail baseline` can anchor on it instead of re-parsing mydumper's
	// ambiguous "Started dump at" line (#768).
	dumpStartedAt := time.Now().UTC()
	if runErr := c.Run(); runErr != nil {
		if stderr := strings.TrimSpace(stderrBuf.String()); stderr != "" {
			return fmt.Errorf("mydumper failed: %w; stderr: %s", runErr, stderr)
		}
		return fmt.Errorf("mydumper failed: %w", runErr)
	}

	// A dump with no binlog position cannot seed a baseline anything is ever
	// folded onto (#1688): mydumper exits 0 with no position when binary logging
	// is off, when the dump user cannot read it, and (measured) for a build older
	// than 0.16.3 against MySQL 8.4. Returning here, before dumpSucceeded,
	// restores the previous dump if there was one. When there was none the
	// refused dump stays in place, so it is marked, and `bintrail baseline`
	// refuses to convert it (#1744). Metadata missing altogether, or without a
	// "Started dump at" line, is only warned about: that is not a shape any
	// measured build writes, and `bintrail baseline` without --timestamp
	// refuses it on its own.
	switch err := baseline.RequireDumpPosition(dmpOutputDir); {
	case errors.Is(err, baseline.ErrDumpNotAnchored):
		if mErr := baseline.WriteRefusedDumpMarker(dmpOutputDir, err.Error()); mErr != nil {
			slog.Warn("could not mark the refused dump; do not convert it with bintrail baseline",
				"output_dir", dmpOutputDir, "error", mErr)
		}
		return err
	case err != nil:
		slog.Warn("could not read the dump's metadata to confirm it records a binlog position",
			"output_dir", dmpOutputDir, "error", err)
	}

	slog.Info("dump complete", "output_dir", dmpOutputDir)
	// The dump succeeded: the deferred cleanup may now delete the moved-aside
	// previous dump (dir.old) instead of restoring it (#809).
	dumpSucceeded = true

	// Best-effort: a write failure here just means baseline conversion falls
	// back to mydumper's own (TZ-sensitive) "Started dump at" line rather
	// than failing the dump itself.
	if err := baseline.WriteStartedAtMarker(dmpOutputDir, dumpStartedAt); err != nil {
		slog.Warn("could not write dump-start marker; baseline conversion will fall back to mydumper's local-time 'Started dump at' line",
			"output_dir", dmpOutputDir, "error", err)
	}

	// CBC gives no authentication (#960): authenticate every encrypted file
	// with an HMAC-SHA256 sidecar so `bintrail baseline --encrypt` can refuse
	// a tampered/bit-rotted .enc instead of decrypting it into garbled SQL.
	// A failure here is a hard error — an unauthenticated encrypted dump is
	// exactly what this step exists to prevent — but the dump directory is
	// kept (dumpSucceeded is already true) so the operator can retry.
	if encryptKeyPath != "" {
		key, err := readHMACKey(encryptKeyPath)
		if err != nil {
			return err
		}
		n, err := writeDumpHMACSidecars(dmpOutputDir, key)
		if err != nil {
			return fmt.Errorf("write HMAC integrity sidecars: %w", err)
		}
		slog.Info("wrote HMAC-SHA256 integrity sidecars for encrypted dump files",
			"files", n, "output_dir", dmpOutputDir)
	}

	if dmpFormat == "json" {
		return cliutil.OutputJSON(struct {
			OutputDir string `json:"output_dir"`
		}{OutputDir: dmpOutputDir})
	}
	return nil
}

// pingSource validates connectivity to the source before the dump does anything
// destructive to --output-dir. It is a variable so tests can stub it without a
// live server (mirroring dumpLockDir). #809.
var pingSource = defaultPingSource

// defaultPingSource opens and pings the source, then closes. The DSN's default
// database is stripped: mydumper connects at the server level (it selects no
// default schema and derives the dump set from --database/--regex/--tables-list),
// so a database component that go-sql-driver would try to USE must not cause a
// false "cannot connect" that blocks an otherwise-valid dump.
func defaultPingSource(dsn string) error {
	cfg, err := drivermysql.ParseDSN(dsn)
	if err != nil {
		return fmt.Errorf("invalid --source-dsn: %w", err)
	}
	cfg.DBName = ""
	db, err := config.Connect(cfg.FormatDSN())
	if err != nil {
		return err
	}
	return db.Close()
}

// dumpDirMarkers are filenames whose presence in a non-empty --output-dir marks
// it as a recognizable prior mydumper/bintrail dump — safe to clear. mydumper
// writes "metadata" on success and "metadata.partial" while running or after a
// crash; `bintrail dump` adds its own started-at sidecar.
var dumpDirMarkers = []string{"metadata", "metadata.partial", baseline.StartedAtMarkerFile}

// looksLikeDumpDir reports whether the directory listing carries any marker that
// identifies it as a prior mydumper/bintrail dump (#809).
func looksLikeDumpDir(entries []os.DirEntry) bool {
	for _, e := range entries {
		name := e.Name()
		for _, m := range dumpDirMarkers {
			if name == m {
				return true
			}
		}
	}
	return false
}

// dumpDirPrep records how the output directory was prepared so the caller can
// commit (delete the moved-aside previous dump) on success or roll back
// (restore it) on failure. When backup is empty, nothing was moved aside — the
// directory was absent or already empty — and commit/rollback are no-ops.
type dumpDirPrep struct {
	dir    string // the requested --output-dir
	backup string // where a recognizable prior dump was moved (dir.old), or ""
}

// prepareDumpOutputDir readies --output-dir for a fresh dump without ever
// unconditionally deleting it (#809). An absent or empty directory needs no
// preparation. A non-empty directory that is NOT a recognizable prior dump is
// REFUSED (no deletion) with an actionable error. A recognizable prior dump is
// moved aside to dir.old so a failed dump can restore it; commit() deletes the
// backup only once the new dump succeeds.
func prepareDumpOutputDir(dir string) (*dumpDirPrep, error) {
	info, err := os.Stat(dir)
	if os.IsNotExist(err) {
		return &dumpDirPrep{dir: dir}, nil // mydumper creates it
	}
	if err != nil {
		return nil, fmt.Errorf("stat --output-dir %q: %w", dir, err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("--output-dir %q exists and is not a directory; "+
			"remove it yourself or point --output-dir elsewhere", dir)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read --output-dir %q: %w", dir, err)
	}
	if len(entries) == 0 {
		return &dumpDirPrep{dir: dir}, nil // empty: mydumper writes into it
	}
	if !looksLikeDumpDir(entries) {
		return nil, fmt.Errorf("--output-dir %q is not empty and does not look like a prior "+
			"bintrail/mydumper dump (no %q marker); refusing to delete it. "+
			"Remove it yourself or point --output-dir elsewhere", dir, dumpDirMarkers[0])
	}
	// Recognizable prior dump: move it aside so a failed dump can restore it.
	// Use a UNIQUE sibling path rather than a fixed dir.old — the earlier fixed
	// name did an unconditional os.RemoveAll(dir.old), reintroducing exactly the
	// unconditional-tree-deletion this guard exists to eliminate (#809 review):
	//   (a) an operator's own precious backup at <dir>.old would be silently
	//       destroyed the moment <dir> is recognized as a dump; and
	//   (b) if a prior rollback() failed to remove partial output, the only good
	//       copy of the previous dump is stranded at dir.old — and it IS a
	//       recognizable dump — so the next run's RemoveAll(dir.old) would wipe
	//       the last good copy before renaming the partial over it.
	// A per-run suffix never collides with a pre-existing path, so we NEVER
	// delete anything here — a leftover backup from a failed rollback stays put.
	backup := fmt.Sprintf("%s.old-%d-%d", filepath.Clean(dir), os.Getpid(), time.Now().UnixNano())
	if _, err := os.Lstat(backup); err == nil {
		return nil, fmt.Errorf("dump backup path %q already exists; refusing to overwrite it", backup)
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("stat dump backup path %q: %w", backup, err)
	}
	if err := os.Rename(dir, backup); err != nil {
		return nil, fmt.Errorf("move existing dump %q aside to %q: %w", dir, backup, err)
	}
	return &dumpDirPrep{dir: dir, backup: backup}, nil
}

// commit finalizes a successful dump by deleting the moved-aside previous dump.
// Best-effort: a leftover backup is a warning, not a failure of the dump.
func (p *dumpDirPrep) commit() {
	if p == nil || p.backup == "" {
		return
	}
	if err := os.RemoveAll(p.backup); err != nil {
		slog.Warn("dump succeeded but could not remove the previous dump's backup",
			"backup", p.backup, "error", err)
	}
}

// rollback restores the previous dump after a failed dump: it removes whatever
// partial output the failed dump left, then renames the backup back into place.
func (p *dumpDirPrep) rollback() {
	if p == nil || p.backup == "" {
		return
	}
	if err := os.RemoveAll(p.dir); err != nil {
		slog.Warn("dump failed; could not remove partial output before restoring the previous dump — "+
			"it remains at the backup path", "dir", p.dir, "backup", p.backup, "error", err)
		return
	}
	if err := os.Rename(p.backup, p.dir); err != nil {
		slog.Warn("dump failed; could not restore the previous dump from its backup — "+
			"it remains at the backup path", "backup", p.backup, "dir", p.dir, "error", err)
	}
}

// buildMydumperArgs constructs the argument slice for a mydumper invocation.
// --compress-protocol and --complete-insert are always included.
// When supportsLockMode is true (mydumper >= 0.18, see
// mydumperlock.Version.SupportsLockMode), --sync-thread-lock-mode and --trx-tables are
// included for lighter locking. When false (older builds, e.g. distro apt
// packages), they are omitted so the dump works without error (#219, #460).
// Schema filtering: single schema → --database; multiple → --regex.
// Table filtering: --tables-list with a comma-joined list.
// When encryptKeyPath is non-empty, --exec-per-thread and
// --exec-per-thread-extension are added for AES-256-CBC encryption.
//
// The source password is NEVER placed on argv — it would be world-readable via
// `ps aux` / /proc/<pid>/cmdline and, under Docker, in `docker inspect`. It is
// delivered out of band: locally via MYSQL_PWD in the child env, and under
// Docker via a 0600 defaults-file bind-mounted read-only. When defaultsFile is
// non-empty its path is referenced with --defaults-file so mydumper reads the
// password from it (#811).
func buildMydumperArgs(host string, port uint16, user, defaultsFile, outputDir string,
	threads int, schemas, tables []string, encryptKeyPath string, supportsLockMode bool,
	lockMode baseline.LockMode) []string {

	args := []string{
		"--host", host,
		"--port", strconv.Itoa(int(port)),
		"--user", user,
		"--threads", strconv.Itoa(threads),
		"--compress-protocol",
		"--complete-insert",
	}

	if supportsLockMode {
		args = append(args, "--sync-thread-lock-mode", lockMode.MydumperValue(), "--trx-tables")
	}

	if defaultsFile != "" {
		args = append(args, "--defaults-file", defaultsFile)
	}

	switch len(schemas) {
	case 1:
		args = append(args, "--database", schemas[0])
	default:
		if len(schemas) > 1 {
			regex := "^(" + strings.Join(schemas, "|") + ")\\."
			args = append(args, "--regex", regex)
		}
	}

	if len(tables) > 0 {
		args = append(args, "--tables-list", strings.Join(tables, ","))
	}

	if encryptKeyPath != "" {
		absKey, err := filepath.Abs(encryptKeyPath)
		if err != nil {
			absKey = encryptKeyPath
		}
		args = append(args,
			"--exec-per-thread", fmt.Sprintf("openssl enc -aes-256-cbc -pbkdf2 -pass file:%s", absKey),
			"--exec-per-thread-extension", ".enc")
	}

	// --outputdir must be last: Docker wrapper scripts commonly use ${@: -1}
	// (the last argument) for the volume mount path.
	args = append(args, "--outputdir", outputDir)

	return args
}

// writeMydumperDefaultsFile writes the source password to a fresh temp file in
// MySQL option-file format (0600) so mydumper can read it via --defaults-file
// instead of taking it on argv. Used by Docker mode, where a passed-through
// MYSQL_PWD would still surface in `docker inspect` Config.Env. The returned
// cleanup removes the file and must be deferred by the caller even on a later
// error. os.CreateTemp opens O_EXCL with mode 0600, so creation is atomic and
// the secret is never briefly world-readable (#811).
func writeMydumperDefaultsFile(password string) (path string, cleanup func(), err error) {
	f, err := os.CreateTemp("", "bintrail-mydumper-*.cnf")
	if err != nil {
		return "", nil, fmt.Errorf("create mydumper defaults file: %w", err)
	}
	name := f.Name()
	cleanup = func() { _ = os.Remove(name) }
	// Defensive: CreateTemp already uses 0600, but pin it in case of an
	// unusual umask/implementation.
	if chmodErr := f.Chmod(0o600); chmodErr != nil {
		_ = f.Close()
		cleanup()
		return "", nil, fmt.Errorf("chmod mydumper defaults file: %w", chmodErr)
	}
	// Password under both [client] and [mydumper] so any mydumper build picks
	// it up regardless of which group it reads.
	v := escapeMyCnfValue(password)
	content := "[client]\npassword=" + v + "\n[mydumper]\npassword=" + v + "\n"
	if _, werr := f.WriteString(content); werr != nil {
		_ = f.Close()
		cleanup()
		return "", nil, fmt.Errorf("write mydumper defaults file: %w", werr)
	}
	if cerr := f.Close(); cerr != nil {
		cleanup()
		return "", nil, fmt.Errorf("close mydumper defaults file: %w", cerr)
	}
	return name, cleanup, nil
}

// escapeMyCnfValue encodes a value for a MySQL option file. The value is wrapped
// in double quotes (so a '#', ';', or whitespace is not misread as a comment or
// truncated) with backslash and double-quote escaped, matching the escape
// sequences the option-file parser recognizes inside a quoted value.
func escapeMyCnfValue(v string) string {
	v = strings.ReplaceAll(v, `\`, `\\`)
	v = strings.ReplaceAll(v, `"`, `\"`)
	return `"` + v + `"`
}

// extractSchemasFromTables derives unique schema names from a list of
// "db.table" entries. Entries without a dot are silently skipped.
// Returns nil for an empty input or when all entries lack a dot.
func extractSchemasFromTables(tables []string) []string {
	if len(tables) == 0 {
		return nil
	}
	seen := make(map[string]struct{})
	var result []string
	for _, t := range tables {
		dot := strings.IndexByte(t, '.')
		if dot < 0 {
			continue
		}
		schema := t[:dot]
		if _, ok := seen[schema]; !ok {
			seen[schema] = struct{}{}
			result = append(result, schema)
		}
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

// ─── Lock mechanism ───────────────────────────────────────────────────────────

const dumpLockFilename = "bintrail-dump.lock"

func dumpLockPath() string {
	return filepath.Join(dumpLockDir(), dumpLockFilename)
}

// acquireDumpLock atomically creates the lockfile and writes the current PID.
// If the file already exists and contains a live PID, it returns an error.
// A stale lockfile (dead PID) is removed and the acquisition is retried once.
func acquireDumpLock() (*os.File, error) {
	lockPath := dumpLockPath()
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err == nil {
		return writePID(f, lockPath)
	}
	if !os.IsExist(err) {
		return nil, fmt.Errorf("failed to create lock file: %w", err)
	}

	// Lock exists — check whether the owning process is still alive.
	data, readErr := os.ReadFile(lockPath)
	if readErr != nil {
		return nil, fmt.Errorf("lock file exists and could not be read: %w", readErr)
	}
	pid, parseErr := strconv.Atoi(strings.TrimSpace(string(data)))
	if parseErr == nil {
		proc, findErr := os.FindProcess(pid)
		if findErr == nil {
			if sigErr := proc.Signal(syscall.Signal(0)); sigErr == nil {
				// Process is alive — a real concurrent dump is running.
				return nil, fmt.Errorf("dump already running (PID %d)", pid)
			}
		}
	}

	// Stale lock — remove and retry once.
	if removeErr := os.Remove(lockPath); removeErr != nil && !os.IsNotExist(removeErr) {
		return nil, fmt.Errorf("failed to remove stale lock file: %w", removeErr)
	}
	f, err = os.OpenFile(lockPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire lock after removing stale file: %w", err)
	}
	return writePID(f, lockPath)
}

func writePID(f *os.File, lockPath string) (*os.File, error) {
	if _, werr := fmt.Fprintf(f, "%d", os.Getpid()); werr != nil {
		f.Close()
		os.Remove(lockPath)
		return nil, fmt.Errorf("failed to write lock PID: %w", werr)
	}
	return f, nil
}

// resolveEncryptKey returns the absolute path to the encryption key file.
// If keyPath is empty, defaultKeyPath() is used. The file must exist and
// be readable. Additionally, openssl must be available on $PATH since
// mydumper shells out to it for encryption.
func resolveEncryptKey(keyPath string) (string, error) {
	if keyPath == "" {
		keyPath = defaultKeyPath()
	}
	absPath, err := filepath.Abs(keyPath)
	if err != nil {
		return "", fmt.Errorf("resolve key path: %w", err)
	}
	if _, err := os.Stat(absPath); err != nil {
		return "", fmt.Errorf("encryption key file not found at %s; generate one with 'bintrail generate-key'", absPath)
	}
	if _, err := exec.LookPath("openssl"); err != nil {
		return "", fmt.Errorf("openssl not found on $PATH; it is required for dump encryption")
	}
	return absPath, nil
}

// releaseDumpLock closes the lockfile handle and removes the file.
func releaseDumpLock(f *os.File) {
	lockPath := f.Name()
	f.Close()
	os.Remove(lockPath)
}
