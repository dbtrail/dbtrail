package consoleapp

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/mydumperlock"
)

// #1986: with no lock mode chosen by an operator, each snapshot picks one
// from the source host: lock-all for an Amazon RDS or Aurora endpoint, where
// FLUSH TABLES WITH READ LOCK is refused to every user, and ftwrl elsewhere.
// Nothing new is stored: the host is in the request every snapshot carries.

func dsnFor(host string) string { return "u:p@tcp(" + host + ":3306)/" }

func TestSourceIsManaged_readsTheHostName(t *testing.T) {
	for _, c := range []struct {
		dsn  string
		want bool
	}{
		{dsnFor("db1.abc123xyz.us-east-1.rds.amazonaws.com"), true},
		{dsnFor("DB1.ABC123XYZ.US-EAST-1.RDS.AMAZONAWS.COM"), true},  // DNS is case-blind
		{dsnFor("db1.abc123xyz.us-east-1.rds.amazonaws.com."), true}, // fully qualified
		{dsnFor("db1.abc123xyz.cn-north-1.rds.amazonaws.com.cn"), true},
		{dsnFor("shop.cluster-abc123xyz.us-east-1.rds.amazonaws.com"), true},     // Aurora writer
		{dsnFor("shop.cluster-ro-abc123xyz.us-east-1.rds.amazonaws.com"), true},  // Aurora reader
		{dsnFor("shop-proxy.proxy-abc123xyz.us-east-1.rds.amazonaws.com"), true}, // RDS Proxy
		// Names that do not say so: the retry covers them (see below).
		{dsnFor("10.0.0.5"), false},
		{dsnFor("db.example.com"), false}, // a CNAME onto an RDS endpoint
		{dsnFor("127.0.0.1"), false},
		{dsnFor("rds.amazonaws.com.example.com"), false},
		{dsnFor("myrds.amazonaws.com"), false},
		{"not a dsn at all", false},
		{"", false},
		{"u:p@unix(/tmp/mysql.sock)/", false},
	} {
		if got := sourceIsManaged(c.dsn); got != c.want {
			t.Errorf("sourceIsManaged(%q) = %v, want %v", c.dsn, got, c.want)
		}
	}
}

// What an operator chose wins; nothing chosen is "automatic". A saved empty
// value used to mean "the built-in default (ftwrl)"; it now means automatic,
// which on a host that is not managed is still ftwrl.
func TestEffectiveLockMode_operatorChoiceVersusAutomatic(t *testing.T) {
	bootErr := errors.New(`BINTRAIL_CONSOLE_BASELINE_LOCK_MODE: unknown lock mode "lock-everything"`)
	for _, c := range []struct {
		name       string
		saved      *string
		bootMode   baseline.LockMode
		bootChosen bool
		bootErr    error
		wantMode   baseline.LockMode
		wantChosen bool
		wantErr    bool
	}{
		{"env unset, nothing saved", nil, baseline.DefaultLockMode, false, nil, baseline.DefaultLockMode, false, false},
		{"env ftwrl, nothing saved", nil, baseline.LockModeFTWRL, true, nil, baseline.LockModeFTWRL, true, false},
		{"env lock-all, nothing saved", nil, baseline.LockModeLockAll, true, nil, baseline.LockModeLockAll, true, false},
		{"saved empty over env ftwrl", strp(""), baseline.LockModeFTWRL, true, nil, baseline.DefaultLockMode, false, false},
		{"saved ftwrl, env unset", strp("ftwrl"), baseline.DefaultLockMode, false, nil, baseline.LockModeFTWRL, true, false},
		{"saved lock-all, env unset", strp("lock-all"), baseline.DefaultLockMode, false, nil, baseline.LockModeLockAll, true, false},
		{"invalid env, nothing saved: still refuses", nil, baseline.DefaultLockMode, false, bootErr, baseline.DefaultLockMode, false, true},
		{"invalid env, saved lock-all clears it", strp("lock-all"), baseline.DefaultLockMode, false, bootErr, baseline.LockModeLockAll, true, false},
		{"invalid env, saved empty clears it (automatic)", strp(""), baseline.DefaultLockMode, false, bootErr, baseline.DefaultLockMode, false, false},
		{"unreadable saved falls back to env choice", strp("lock-everything"), baseline.LockModeNoLock, true, nil, baseline.LockModeNoLock, true, false},
		{"unreadable saved, env unset: automatic", strp("lock-everything"), baseline.DefaultLockMode, false, nil, baseline.DefaultLockMode, false, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			reg := liveRegistry(t)
			if c.saved != nil {
				save(t, reg, console.BackupSettingLockMode, *c.saved)
			}
			mode, chosen, err := effectiveLockMode(reg, c.bootMode, c.bootChosen, c.bootErr)
			if (err != nil) != c.wantErr {
				t.Fatalf("err = %v, want error %v", err, c.wantErr)
			}
			if mode != c.wantMode || chosen != c.wantChosen {
				t.Errorf("= %s chosen=%v, want %s chosen=%v", mode, chosen, c.wantMode, c.wantChosen)
			}
		})
	}
}

func TestLockModeFor_hostDecidesOnlyWhenNothingWasChosen(t *testing.T) {
	rds := dsnFor("db1.abc.us-east-1.rds.amazonaws.com")
	own := dsnFor("10.0.0.5")
	for _, c := range []struct {
		name       string
		dsn        string
		saved      *string
		bootMode   baseline.LockMode
		bootChosen bool
		wantMode   baseline.LockMode
		wantChosen bool
	}{
		{"automatic, RDS host", rds, nil, baseline.DefaultLockMode, false, baseline.LockModeLockAll, false},
		{"automatic, own host keeps ftwrl (H1)", own, nil, baseline.DefaultLockMode, false, baseline.LockModeFTWRL, false},
		{"env ftwrl on an RDS host is respected", rds, nil, baseline.LockModeFTWRL, true, baseline.LockModeFTWRL, true},
		{"saved ftwrl on an RDS host is respected", rds, strp("ftwrl"), baseline.DefaultLockMode, false, baseline.LockModeFTWRL, true},
		{"saved no-lock on an RDS host is respected", rds, strp("no-lock"), baseline.DefaultLockMode, false, baseline.LockModeNoLock, true},
		{"saved empty on an RDS host is automatic", rds, strp(""), baseline.LockModeFTWRL, true, baseline.LockModeLockAll, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			reg := liveRegistry(t)
			if c.saved != nil {
				save(t, reg, console.BackupSettingLockMode, *c.saved)
			}
			s := newBaselineSupervisor(context.Background(), t.TempDir(), c.bootMode)
			s.lockModeChosen = c.bootChosen
			s.reg = reg
			mode, chosen, err := s.lockModeFor(console.BaselineRequest{SourceDSN: c.dsn})
			if err != nil {
				t.Fatal(err)
			}
			if mode != c.wantMode || chosen != c.wantChosen {
				t.Errorf("= %s chosen=%v, want %s chosen=%v", mode, chosen, c.wantMode, c.wantChosen)
			}
		})
	}
}

// The boot wiring: an operator who set the variable chose; one who did not
// gets the automatic mode. Dropping the copy into the supervisor is silent
// (every daemon would become automatic), so it is asserted here.
func TestResolveEnv_lockModeSetIsCarriedToTheSupervisor(t *testing.T) {
	prevMode, prevSet, prevErr := upConsoleBaselineLockMode, upConsoleBaselineLockModeSet, upConsoleBaselineLockModeErr
	t.Cleanup(func() {
		upConsoleBaselineLockMode, upConsoleBaselineLockModeSet, upConsoleBaselineLockModeErr = prevMode, prevSet, prevErr
	})
	for _, c := range []struct {
		env     string
		wantSet bool
	}{{"nonsense", false}, {"", false}, {"ftwrl", true}, {"lock-all", true}, {"", false}} {
		// No reset by hand: each read must start clean on its own, or the
		// "nonsense" refusal would ride into the next case.
		t.Setenv("BINTRAIL_CONSOLE_BASELINE_LOCK_MODE", c.env)
		resolveBaselineLockModeEnv()
		sup := newBaselineSupervisorFromConfig(context.Background(), t.TempDir(), nil)
		if sup.lockModeChosen != c.wantSet {
			t.Errorf("env %q: supervisor chosen = %v, want %v", c.env, sup.lockModeChosen, c.wantSet)
		}
		if (sup.configErr != nil) != (c.env == "nonsense") {
			t.Errorf("env %q: refusal %v", c.env, sup.configErr)
		}
	}
	// A previous value must not leak into an unset environment.
	upConsoleBaselineLockModeSet = true
	t.Setenv("BINTRAIL_CONSOLE_BASELINE_LOCK_MODE", "")
	resolveBaselineLockModeEnv()
	if upConsoleBaselineLockModeSet {
		t.Error("an unset variable kept the previous run's choice")
	}
}

// ── The retry ──────────────────────────────────────────────────────────────

type dumpCall struct {
	mode baseline.LockMode
	dir  string
}

// stubRetryDump replaces mydumper with fn and records every call, and the DDL
// mark with one that records when it was read.
func stubRetryDump(t *testing.T, fn func(n int, mode baseline.LockMode, dir string) error) (calls *[]dumpCall, marks *[]time.Time) {
	t.Helper()
	prevDump, prevMark := runMydumperFunc, dumpDDLMarkFunc
	t.Cleanup(func() { runMydumperFunc, dumpDDLMarkFunc = prevDump, prevMark })
	var mu sync.Mutex
	calls, marks = &[]dumpCall{}, &[]time.Time{}
	runMydumperFunc = func(_ context.Context, _ string, _ []string, dir string, mode baseline.LockMode) error {
		mu.Lock()
		*calls = append(*calls, dumpCall{mode, dir})
		n := len(*calls)
		mu.Unlock()
		return fn(n, mode, dir)
	}
	dumpDDLMarkFunc = func(console.BaselineRequest) string {
		mu.Lock()
		*marks = append(*marks, time.Now())
		mu.Unlock()
		time.Sleep(2 * time.Millisecond) // so a reused start time cannot pass for a fresh one
		return ""
	}
	return calls, marks
}

var errDeniedFTWRL = &ftwrlDeniedError{err: errors.New("mydumper failed: exit status 1: mydumper could not take the global read lock")}

func TestExecute_retriesWithLockAllOnlyForTheRefusedDefault(t *testing.T) {
	missing := fmt.Errorf("refused: %w", mydumperlock.ErrFTWRLPrivilegesMissing)
	other := errors.New("mydumper failed: exit status 1; output: Access denied for user 'u'@'h' (using password: YES)")
	stop := errors.New("lock-all attempt stopped")
	for _, c := range []struct {
		name      string
		chosen    bool
		saved     *string
		first     error
		wantModes []baseline.LockMode
	}{
		{"(a) global lock denied, automatic", false, nil, errDeniedFTWRL, []baseline.LockMode{"ftwrl", "lock-all"}},
		{"(b) privileges missing, automatic", false, nil, missing, []baseline.LockMode{"ftwrl", "lock-all"}},
		{"(a) but the operator chose ftwrl (env)", true, nil, errDeniedFTWRL, []baseline.LockMode{"ftwrl"}},
		{"(b) but the operator chose ftwrl (env)", true, nil, missing, []baseline.LockMode{"ftwrl"}},
		{"(a) but the operator saved ftwrl", false, strp("ftwrl"), errDeniedFTWRL, []baseline.LockMode{"ftwrl"}},
		{"another mydumper failure", false, nil, other, []baseline.LockMode{"ftwrl"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			calls, _ := stubRetryDump(t, func(n int, _ baseline.LockMode, _ string) error {
				if n == 1 {
					return c.first
				}
				return stop
			})
			s := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
			s.lockModeChosen = c.chosen
			if c.saved != nil {
				s.reg = liveRegistry(t)
				save(t, s.reg, console.BackupSettingLockMode, *c.saved)
			}
			_, err := s.execute(console.BaselineRequest{ServerID: "s1", SourceDSN: dsnFor("10.0.0.5")})
			if err == nil {
				t.Fatal("execute succeeded over a failed dump")
			}
			var got []baseline.LockMode
			for _, cl := range *calls {
				got = append(got, cl.mode)
			}
			if fmt.Sprint(got) != fmt.Sprint(c.wantModes) {
				t.Fatalf("modes = %v, want %v", got, c.wantModes)
			}
			if len(c.wantModes) == 2 {
				// The second failure is the one surfaced: the lock-all one,
				// saying that ftwrl came first.
				if !errors.Is(err, stop) || !strings.Contains(err.Error(), "ftwrl was refused") {
					t.Errorf("err = %v, want the lock-all attempt's error, naming the refused ftwrl", err)
				}
			} else if !errors.Is(err, c.first) && !strings.Contains(err.Error(), c.first.Error()) {
				t.Errorf("err = %v, want the first attempt's error", err)
			}
		})
	}
}

// The automatic RDS choice is lock-all from the start: there is nothing to
// retry, and lock-all never falls further to safe-no-lock or no-lock.
func TestExecute_lockAllIsNeverRetriedOrWeakened(t *testing.T) {
	calls, _ := stubRetryDump(t, func(int, baseline.LockMode, string) error { return errDeniedFTWRL })
	s := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	_, err := s.execute(console.BaselineRequest{ServerID: "s1", SourceDSN: dsnFor("db1.abc.us-east-1.rds.amazonaws.com")})
	if err == nil {
		t.Fatal("execute succeeded over a failed dump")
	}
	if len(*calls) != 1 || (*calls)[0].mode != baseline.LockModeLockAll {
		t.Fatalf("calls = %+v, want exactly one lock-all attempt", *calls)
	}
}

// A cancelled daemon does not start a second dump.
func TestExecute_noRetryAfterShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls, _ := stubRetryDump(t, func(int, baseline.LockMode, string) error { cancel(); return errDeniedFTWRL })
	s := newBaselineSupervisor(ctx, t.TempDir(), baseline.DefaultLockMode)
	if _, err := s.execute(console.BaselineRequest{ServerID: "s1", SourceDSN: dsnFor("10.0.0.5")}); err == nil {
		t.Fatal("execute succeeded")
	}
	if len(*calls) != 1 {
		t.Fatalf("mydumper ran %d times after the daemon stopped, want 1", len(*calls))
	}
}

// The retry starts clean: a new dump folder (the refused one is gone), a new
// DDL mark, and a start time taken after the refused attempt; the snapshot
// records lock-all, the mode actually used (#1380). Driven through the real
// runMydumper with a fake mydumper that refuses the global lock exactly like
// RDS for MariaDB does.
func TestExecute_retryStartsCleanAndRecordsLockAll(t *testing.T) {
	stage := t.TempDir()
	logf := filepath.Join(t.TempDir(), "calls.log")
	meta := "Started dump at: 2026-10-01 10:00:00\\nSHOW MASTER STATUS:\\n\\tLog: binlog.000002\\n\\tPos: 1374\\n\\tGTID:\\n\\nFinished dump at: 2026-10-01 10:00:01\\n"
	script := "#!/bin/bash\n" +
		"if [ \"$1\" = \"--version\" ]; then " + printsVersion(versionModern) + "; fi\n" +
		"out=\"${@: -1}\"\n" +
		"echo \"$*\" >> '" + logf + "'\n" +
		"case \" $* \" in *' FTWRL '*)\n" +
		"  printf '%s\\n' \"** (mydumper:4242): CRITICAL **: 23:14:02.118: Couldn't acquire global lock, snapshots will not be consistent: Access denied for user 'admin'@'%' (using password: YES)\" >&2\n" +
		"  touch \"$out/half-written\"; exit 1;;\nesac\n" +
		"printf '" + meta + "' > \"$out/metadata\"\n" +
		"printf 'CREATE TABLE `t` (\\n  `id` int NOT NULL,\\n  PRIMARY KEY (`id`)\\n) ENGINE=InnoDB;\\n' > \"$out/appdb.t-schema.sql\"\n" +
		"printf 'INSERT INTO `t` VALUES(1),(2);\\n' > \"$out/appdb.t.00000.sql\"\n" +
		"exit 0\n"
	installFake(t, script)
	stubPreflight(t, nil)

	var dirs []string
	// For the marks; mydumper itself is replaced right after (its cleanup
	// restores the real one).
	_, marks := stubRetryDump(t, nil)
	runMydumperFunc = func(ctx context.Context, dsn string, schemas []string, dir string, mode baseline.LockMode) error {
		dirs = append(dirs, dir)
		return runMydumper(ctx, dsn, schemas, dir, mode)
	}

	s := newBaselineSupervisor(context.Background(), stage, baseline.DefaultLockMode)
	local := t.TempDir()
	out, err := s.execute(console.BaselineRequest{ServerID: "s1", SourceDSN: "admin:p@tcp(db.example.com:3306)/", LocalDir: local})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	defer out.cleanup()

	raw, _ := os.ReadFile(logf)
	runs := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(runs) != 2 || !strings.Contains(runs[0], "FTWRL") || !strings.Contains(runs[1], "LOCK_ALL") {
		t.Fatalf("mydumper runs:\n%s\nwant FTWRL, then LOCK_ALL", raw)
	}
	if len(dirs) != 2 || dirs[0] == dirs[1] {
		t.Fatalf("dump folders = %v, want two different ones", dirs)
	}
	if _, err := os.Stat(dirs[0]); !os.IsNotExist(err) {
		t.Errorf("the refused attempt's folder %s is still there (err %v)", dirs[0], err)
	}
	if len(*marks) != 2 {
		t.Fatalf("DDL mark read %d times, want once per attempt", len(*marks))
	}
	if !out.at.After((*marks)[0]) {
		t.Errorf("snapshot start %v is not after the refused attempt began (%v): the retry reused its start time", out.at, (*marks)[0])
	}
	md, err := baseline.ReadParquetMetadata(filepath.Join(out.snapDir, "appdb", "t.parquet"))
	if err != nil {
		t.Fatal(err)
	}
	if md.LockMode != string(baseline.LockModeLockAll) {
		t.Errorf("snapshot records lock mode %q, want %q, the mode actually used", md.LockMode, baseline.LockModeLockAll)
	}
	if entries, _ := os.ReadDir(stage); len(entries) != 0 {
		var names []string
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Errorf("staging folder keeps %v after the run", names)
	}
}

// The typed refusal is made where the hint matches, so execute never reads
// the words; other failures stay untyped.
func TestRunMydumper_globalLockDeniedIsTyped(t *testing.T) {
	for _, c := range []struct {
		name  string
		out   string
		mode  baseline.LockMode
		typed bool
	}{
		{"denied", "Couldn't acquire global lock, snapshots will not be consistent: Access denied for user 'admin'@'%'", baseline.LockModeFTWRL, true},
		{"wrong password", "Error connecting to database: Access denied for user 'u'@'h' (using password: YES)", baseline.LockModeFTWRL, false},
		{"lock-all run", "Couldn't acquire global lock, snapshots will not be consistent: Access denied for user 'admin'@'%'", baseline.LockModeLockAll, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			installFake(t, "#!/bin/bash\nif [ \"$1\" = \"--version\" ]; then "+printsVersion(versionModern)+"; fi\n"+
				"printf '%s\\n' \""+c.out+"\" >&2\nexit 1\n")
			stubPreflight(t, nil)
			err := runMydumper(context.Background(), "admin:p@tcp(127.0.0.1:1)/", []string{"appdb"}, t.TempDir(), c.mode)
			if err == nil {
				t.Fatal("runMydumper succeeded")
			}
			var d *ftwrlDeniedError
			if got := errors.As(err, &d); got != c.typed {
				t.Errorf("typed = %v, want %v: %v", got, c.typed, err)
			}
		})
	}
}

// The Compose install reaches the automatic mode only if the console service
// passes the variable EMPTY when .env does not set it. A default of ftwrl
// there would read as an operator's choice and turn automatic off on every
// Compose install; the first-run walk mirrors the Compose environment.
func TestCompose_lockModeIsEmptyUnlessSet(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "docker-compose.yml"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), "\n      BINTRAIL_CONSOLE_BASELINE_LOCK_MODE: ${BASELINE_LOCK_MODE:-}\n") {
		t.Error("docker-compose.yml does not pass BINTRAIL_CONSOLE_BASELINE_LOCK_MODE empty by default")
	}
	walk, err := os.ReadFile(filepath.Join("..", "test", "console-e2e", "first-run-walk.sh"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(walk), "\n  BINTRAIL_CONSOLE_BASELINE_LOCK_MODE= \\\n") {
		t.Error("first-run-walk.sh does not mirror the Compose default (empty lock mode)")
	}
}

// Trigger refuses an invalid environment value, but the saved value that let
// it through can be cleared before the dump starts: execute refuses then too,
// rather than run in the automatic mode.
func TestExecute_refusesAnInvalidEnvironmentValue(t *testing.T) {
	calls, _ := stubRetryDump(t, func(int, baseline.LockMode, string) error { return nil })
	s := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	s.configErr = errors.New(`BINTRAIL_CONSOLE_BASELINE_LOCK_MODE: unknown lock mode "lock-everything"`)
	if _, err := s.execute(console.BaselineRequest{ServerID: "s1", SourceDSN: dsnFor("db1.abc.us-east-1.rds.amazonaws.com")}); err == nil ||
		!strings.Contains(err.Error(), "lock-everything") {
		t.Fatalf("err = %v, want the configuration refusal", err)
	}
	if len(*calls) != 0 {
		t.Fatalf("mydumper ran %d times over an invalid lock mode", len(*calls))
	}
}
