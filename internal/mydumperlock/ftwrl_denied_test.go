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
		{RemedyCLI, []string{"RDS", "Aurora", "--lock-mode lock-all"}},
		{RemedyConsole, []string{"RDS", "Aurora", "Lock while dumping", "lock-all", "BINTRAIL_CONSOLE_BASELINE_LOCK_MODE=lock-all"}},
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
		{baseline.LockModeLockAll, rdsGlobalLockDenied},
		{baseline.LockModeSafeNoLock, rdsGlobalLockDenied},
		{baseline.LockModeNoLock, rdsGlobalLockDenied},
	} {
		if got := FTWRLDeniedHint(c.mode, c.out, RemedyCLI); got != "" {
			t.Errorf("mode %s, output %q: hint %q, want none", c.mode, c.out, got)
		}
	}
}
