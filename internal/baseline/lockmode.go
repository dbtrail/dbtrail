package baseline

import "fmt"

// LockMode selects how mydumper synchronizes its worker threads onto a single
// point in time. It is shared by `bintrail dump` and the console's in-process
// baseline trigger so the two cannot drift: a snapshot's trustworthiness must
// not depend on which surface produced it.
//
// The distinction that matters is NOT "does it lock" but "can it hand back a
// TORN snapshot without saying so". A baseline is the seed state reconstruct
// merges deltas onto, so a snapshot stitched from several instants yields a
// table that never existed — and every downstream answer inherits it silently.
//
// Measured against mydumper v1.0.3-1 and a real MySQL 8.0 with a
// least-privilege user (SELECT + REPLICATION CLIENT):
//
//   - FTWRL succeeds and is genuinely point-consistent, but needs RELOAD /
//     FLUSH_TABLES plus BACKUP_ADMIN on MySQL 8.0+ (it issues LOCK INSTANCE
//     FOR BACKUP first). The least-privilege user got
//     "ERROR 1227: Access denied" on that statement.
//   - SAFE_NO_LOCK runs at least-privilege, but it does not PREVENT skew — it
//     DETECTS it: mydumper compares the binlog position before and after
//     syncing threads and, on any difference, stops with "we cannot guarantee
//     the backup to be consistent. Stopping backup due to the use of
//     SAFE_NO_LOCK." Verified: under sustained concurrent writes it aborts.
//     So it never lies, but on a write-active source it mostly refuses.
//   - LOCK_ALL synchronizes workers by locking the exported tables instead of
//     the instance, so it needs only LOCK TABLES — at any scope covering the
//     dumped tables, not necessarily globally. mydumper names it itself: "We
//     support LOCK_ALL and SAFE_NO_LOCK modes for RDS/Aurora" (string in the
//     pinned v1.0.3-1 binary). Measured separately from the users above, on an
//     RDS MySQL 8.4 master account and on a local user granted only
//     `SELECT, LOCK TABLES, SHOW VIEW` on the dumped schema: FTWRL fails for
//     both ("ERROR 1227 ... you need the RDSADMIN USER privilege" when granting
//     BACKUP_ADMIN on RDS), LOCK_ALL succeeds for both.
//   - NO_LOCK always succeeds and is the only mode that can silently produce a
//     torn snapshot. mydumper's help presents it as the choice for when "you
//     don't need a consistent backup"; it is also the one value NOT in the help
//     text's list of sync modes ("SAFE_NO_LOCK, FTWRL, LOCK_ALL and GTID"),
//     which is the closest thing to a deprecation the pinned build states.
//
// Two properties are NOT mode-specific and catch people out:
//
//   - Every point-consistent mode — LOCK_ALL included, verified — REFUSES the
//     whole dump when it meets a non-transactional table, because bintrail
//     passes --trx-tables: "Non transactional table found: `db`.`t` on a
//     consistent backup attempt". Switching FTWRL→LOCK_ALL does not avoid it.
//   - LOCK TABLES also implies SELECT on the locked tables, which mydumper
//     needs regardless, so "needs only LOCK TABLES" is relative to that floor.
//
// The default stays FTWRL for the reason #1377 chose it: it is the one
// point-consistent mode that needs no per-object privilege, so it works on a
// self-hosted source with a single global grant and on every flavor including
// MariaDB and MySQL 5.7. LOCK_ALL is not a worse mode — the locking-cost
// comparison between the two has NOT been measured here, and no claim is made
// either way — it is simply the one that requires knowing which objects will be
// dumped. It is the right answer where FTWRL cannot run, which the refusal in
// internal/mydumperlock names first. SAFE_NO_LOCK is unusable as a default on
// the sources that most need a baseline, and NO_LOCK is not a backup.
type LockMode string

const (
	// LockModeFTWRL holds one global read lock only long enough for every
	// worker to open its snapshot at the same instant, then releases it.
	LockModeFTWRL LockMode = "ftwrl"
	// LockModeLockAll locks the exported tables long enough for every worker to
	// open its snapshot together. Needs LOCK TABLES and no more, which makes it
	// the point-consistent mode available on managed MySQL where BACKUP_ADMIN
	// cannot be granted.
	LockModeLockAll LockMode = "lock-all"
	// LockModeSafeNoLock needs no elevated privilege and refuses rather than
	// emit a torn snapshot. Expect it to abort on a write-active source.
	LockModeSafeNoLock LockMode = "safe-no-lock"
	// LockModeNoLock accepts a torn snapshot. Reachable only by explicit
	// choice; never a default and never a fallback.
	LockModeNoLock LockMode = "no-lock"
)

// DefaultLockMode is what every surface uses when the operator says nothing.
const DefaultLockMode = LockModeFTWRL

// LockModeValues lists the accepted spellings. Test-only today: the flag
// help, ParseLockMode's error and the compose case statement each spell all
// four names out, so adding a mode means updating every one of them by hand.
var LockModeValues = []LockMode{LockModeFTWRL, LockModeLockAll, LockModeSafeNoLock, LockModeNoLock}

// ParseLockMode maps an operator-supplied string to a LockMode. It is
// deliberately strict: a typo must not silently land on a weaker mode, because
// the difference between them is whether a wrong answer can ship unannounced.
func ParseLockMode(s string) (LockMode, error) {
	switch LockMode(s) {
	case LockModeFTWRL:
		return LockModeFTWRL, nil
	case LockModeLockAll:
		return LockModeLockAll, nil
	case LockModeSafeNoLock:
		return LockModeSafeNoLock, nil
	case LockModeNoLock:
		return LockModeNoLock, nil
	case "":
		return DefaultLockMode, nil
	}
	return "", fmt.Errorf("unknown lock mode %q: want one of %s, %s, %s, %s",
		s, LockModeFTWRL, LockModeLockAll, LockModeSafeNoLock, LockModeNoLock)
}

// MydumperValue is the --sync-thread-lock-mode argument for this mode.
func (m LockMode) MydumperValue() string {
	switch m {
	case LockModeLockAll:
		return "LOCK_ALL"
	case LockModeSafeNoLock:
		return "SAFE_NO_LOCK"
	case LockModeNoLock:
		return "NO_LOCK"
	default:
		return "FTWRL"
	}
}

// PointConsistent reports whether a snapshot this mode PRODUCES can be trusted
// to represent one instant. SAFE_NO_LOCK counts: it aborts instead of writing
// a torn snapshot, so anything it does write is consistent. Only NO_LOCK can
// hand back skew without saying so.
//
// The mode is recorded in the snapshot (MetaKeyLockMode, #1380) and read
// back through ReadConsistencyOf, which calls this.
func (m LockMode) PointConsistent() bool { return m != LockModeNoLock }

// NeedsElevatedPrivileges reports whether the mode issues statements a
// least-privilege replication user cannot run (verified empirically — see the
// type comment). Callers use this to check privileges BEFORE launching
// mydumper: the pinned build segfaults on a half-granted state rather than
// failing cleanly.
func (m LockMode) NeedsElevatedPrivileges() bool {
	// Written as "not one of the low-privilege modes" rather than "== FTWRL"
	// so the three methods agree on an UNKNOWN value: MydumperValue's default
	// arm already sends FTWRL, and answering false here would send FTWRL while
	// skipping the preflight — reopening the segfault path internal/mydumperlock
	// exists to make unreachable. No caller passes the zero value today; this
	// keeps that from becoming load-bearing.
	return m != LockModeSafeNoLock && m != LockModeNoLock
}
