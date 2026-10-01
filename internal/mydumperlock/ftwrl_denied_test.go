package mydumperlock

import (
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// What mydumper printed on RDS for MariaDB 11.4, run by the master user, which
// holds RELOAD: the privilege check passes, and the server still refuses
// FLUSH TABLES WITH READ LOCK. The "Access denied" alone reads like a password
// problem.
const rdsGlobalLockDenied = "** (mydumper:4242): CRITICAL **: 23:14:02.118: Couldn't acquire global lock, " +
	"snapshots will not be consistent: Access denied for user 'admin'@'%' (using password: YES)"

func TestFTWRLDeniedHint(t *testing.T) {
	for _, c := range []struct {
		remedy Remedy
		want   []string
	}{
		{RemedyCLI, []string{"RDS", "Aurora", "--lock-mode lock-all", "GRANT RELOAD"}},
		{RemedyConsole, []string{"RDS", "Aurora", "lock-all", "BINTRAIL_CONSOLE_BASELINE_LOCK_MODE=lock-all"}},
	} {
		got := FTWRLDeniedHint(baseline.LockModeFTWRL, rdsGlobalLockDenied, c.remedy)
		t.Logf("%s: %s", c.remedy, got)
		for _, w := range c.want {
			if !strings.Contains(got, w) {
				t.Errorf("%s hint lacks %q: %q", c.remedy, w, got)
			}
		}
		if strings.Contains(got, "—") {
			t.Errorf("em dash in %q", got)
		}
		// #1986: the snapshot settings have no lock control since #1846, so
		// naming one sends the operator to look for a control that is not there.
		for _, gone := range []string{"Lock while dumping", "snapshot settings", "Snapshots, Settings"} {
			if strings.Contains(got, gone) {
				t.Errorf("%s hint names %q, a control the console does not have: %q", c.remedy, gone, got)
			}
		}
	}
	// Upper case, as some builds log the SQL error text.
	if FTWRLDeniedHint(baseline.LockModeFTWRL, strings.ToUpper(rdsGlobalLockDenied), RemedyCLI) == "" {
		t.Error("the match must not depend on case")
	}
}

// The hint names one cause, so it must stay quiet for every other failure: a
// wrong password (Access denied without the global lock), a lock that failed
// for another reason, and every mode that never takes the global lock.
func TestFTWRLDeniedHint_quietOtherwise(t *testing.T) {
	for _, c := range []struct {
		mode baseline.LockMode
		out  string
	}{
		{baseline.LockModeFTWRL, "Error connecting to database: Access denied for user 'u'@'10.0.0.9' (using password: YES)"},
		{baseline.LockModeFTWRL, "Couldn't acquire global lock, snapshots will not be consistent: Lock wait timeout exceeded"},
		{baseline.LockModeFTWRL, ""},
		// The two phrases on different lines: another failure beside a lock line.
		{baseline.LockModeFTWRL, "Couldn't acquire global lock, retrying\nError: Access denied; you need the SHOW VIEW privilege"},
		{baseline.LockModeLockAll, rdsGlobalLockDenied},
		{baseline.LockModeSafeNoLock, rdsGlobalLockDenied},
		{baseline.LockModeNoLock, rdsGlobalLockDenied},
	} {
		if got := FTWRLDeniedHint(c.mode, c.out, RemedyCLI); got != "" {
			t.Errorf("mode %s, output %q: hint %q, want none", c.mode, c.out, got)
		}
	}
}

// #1989 review: a lock mode SAVED in the console's settings wins over the
// environment variable, so its remedy is the saved setting, never the
// variable. The hint no longer calls ftwrl "the default": on the console,
// unset is the automatic choice.
func TestFTWRLDeniedHint_savedSetting(t *testing.T) {
	got := FTWRLDeniedHint(baseline.LockModeFTWRL, rdsGlobalLockDenied, RemedyConsoleSaved)
	t.Logf("saved: %s", got)
	for _, want := range []string{"RDS", "lock-all", SavedLockModeEndpoint, `{"value":"lock-all"}`, `{"use_startup":true}`} {
		if !strings.Contains(got, want) {
			t.Errorf("saved hint lacks %q: %q", want, got)
		}
	}
	if strings.Contains(got, "BINTRAIL_CONSOLE_BASELINE_LOCK_MODE") {
		t.Errorf("saved hint names the variable the saved value wins over: %q", got)
	}
	for _, r := range []Remedy{RemedyCLI, RemedyConsole, RemedyConsoleSaved} {
		if h := FTWRLDeniedHint(baseline.LockModeFTWRL, rdsGlobalLockDenied, r); strings.Contains(h, "the default") {
			t.Errorf("%s hint calls ftwrl the default: %q", r, h)
		}
	}
}

func TestRemedyForMode_savedSetting(t *testing.T) {
	got := RemedyConsoleSaved.forMode(baseline.LockModeLockAll)
	if !strings.Contains(got, SavedLockModeEndpoint) || !strings.Contains(got, `{"value":"lock-all"}`) ||
		!strings.Contains(got, `{"use_startup":true}`) || strings.Contains(got, "BINTRAIL_") {
		t.Errorf("saved remedy = %q", got)
	}
	if got := SavedLockModeEndpoint; got != "PUT /api/backup-settings/daemon/lock_mode" {
		t.Errorf("endpoint %q is not the console's route", got)
	}
}
