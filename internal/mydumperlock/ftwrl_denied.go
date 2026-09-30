package mydumperlock

import (
	"strings"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// FTWRLDeniedHint explains one mydumper failure the privilege check cannot
// catch beforehand. On RDS and Aurora no user may run FLUSH TABLES WITH READ
// LOCK, not even the master user, who does hold RELOAD: measured on RDS for
// MariaDB 11.4, where mydumper stopped with "Couldn't acquire global lock ...
// Access denied". CheckPrivileges passes that user, since RELOAD is what the
// statement asks for everywhere else, and the bare "Access denied" then reads
// like a wrong password.
//
// It returns a sentence naming lock-all, and this surface's way to select it,
// when the mode is ftwrl and mydumper's output shows the global lock refused
// with "Access denied". Otherwise it returns "": any other failure keeps
// mydumper's own words.
func FTWRLDeniedHint(mode baseline.LockMode, output string, remedy Remedy) string {
	if mode != baseline.LockModeFTWRL {
		return ""
	}
	// Both phrases on ONE line: the console hands over combined output, and an
	// unrelated "Access denied" (a missing SHOW VIEW, say) next to some other
	// line about the lock must not be blamed on RDS.
	found := false
	for _, line := range strings.Split(strings.ToLower(output), "\n") {
		if strings.Contains(line, "global lock") && strings.Contains(line, "access denied") {
			found = true
			break
		}
	}
	if !found {
		return ""
	}
	how := "pass --lock-mode lock-all"
	if remedy == RemedyConsole {
		how = "set Lock while dumping to lock-all in the snapshot settings (or BINTRAIL_CONSOLE_BASELINE_LOCK_MODE=lock-all)"
	}
	return "mydumper could not take the global read lock (FLUSH TABLES WITH READ LOCK) that lock mode ftwrl, the default, uses. " +
		"On RDS and Aurora no user may take it, not even the master user with RELOAD, so there the lock mode must be lock-all: " +
		how + ". It needs only LOCK TABLES. On a server you run yourself, GRANT RELOAD to this user instead"
}
