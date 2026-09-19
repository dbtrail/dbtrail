package mydumperlock

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// LockModeFloor is the first mydumper build that accepts --sync-thread-lock-mode
// and --trx-tables. No 0.18.0 was ever released, so the 0.18 series starts here.
const LockModeFloor = "0.18.1"

// ProbeTimeout bounds `mydumper --version`. The console's startup line runs the
// probe before capture starts, so a binary that never answers must not hold
// the daemon up. A variable so a test can shorten it.
var ProbeTimeout = 10 * time.Second

// PositionFloor is the first mydumper that reads the binlog position on MySQL
// 8.2 and newer (SHOW BINARY LOG STATUS). An older build uses SHOW MASTER
// STATUS, which MySQL 8.4 removed; it ignores the error, exits 0 and writes no
// position (measured with 0.10 against 8.4; 0.16.3 against 8.4 records it).
var PositionFloor = Version{0, 16, 3}

// ErrNotRunnable marks a mydumper binary that did not run at all (#1699): it
// could not be started, or it exited non-zero without a readable version. That is a
// different fact from "it ran and printed a version this release cannot read",
// and it has a different remedy: fix or replace the binary, which will not
// complete a dump either.
var ErrNotRunnable = errors.New("mydumper did not run")

// Version is a mydumper build's version triple, as `mydumper --version` prints it.
type Version struct {
	Major, Minor, Patch int
}

func (v Version) String() string { return fmt.Sprintf("%d.%d.%d", v.Major, v.Minor, v.Patch) }

// SupportsLockMode reports whether this build understands --sync-thread-lock-mode
// and --trx-tables. The flags landed in mydumper 0.18.1, NOT 0.11, whose
// light-locking options were --less-locking / --trx-consistency-only (which
// --trx-tables replaced; --no-locks survives in modern versions). The gate
// previously sat at 0.11, handing 0.11-0.17 builds flags they reject with
// "unknown option" (#460). No 0.18.0 was ever released, so gating on
// (major, minor) alone is exact.
func (v Version) SupportsLockMode() bool {
	return v.Major > 0 || v.Minor >= 18
}

// Less reports whether v is an older build than w.
func (v Version) Less(w Version) bool {
	if v.Major != w.Major {
		return v.Major < w.Major
	}
	if v.Minor != w.Minor {
		return v.Minor < w.Minor
	}
	return v.Patch < w.Patch
}

// RecordsPositionOn reports whether this build can record the binlog position
// on a server whose SELECT VERSION() is serverVersion. Only MySQL (and Percona
// Server) 8.4 and newer lack SHOW MASTER STATUS; MariaDB kept it. A version
// string that does not parse is answered true: this is a pre-dump shortcut, and
// the dump's own metadata is checked afterwards either way.
func (v Version) RecordsPositionOn(serverVersion string) bool {
	if !v.Less(PositionFloor) || strings.Contains(strings.ToLower(serverVersion), "mariadb") {
		return true
	}
	parts := strings.SplitN(serverVersion, ".", 3)
	if len(parts) < 2 {
		return true
	}
	major, err1 := strconv.Atoi(parts[0])
	minor, err2 := strconv.Atoi(strings.TrimFunc(parts[1], func(r rune) bool { return r < '0' || r > '9' }))
	if err1 != nil || err2 != nil {
		return true
	}
	return major < 8 || (major == 8 && minor < 4)
}

// ProbeVersion runs `<path> --version` and parses what it prints.
//
// Three outcomes, and callers must keep them apart:
//
//   - a Version and nil: the build answered;
//   - an error wrapping ErrNotRunnable: the binary did not start, or exited
//     non-zero without printing a version (a dynamic linker that cannot resolve
//     a library exits 127 with its complaint on stderr) (#1699);
//   - any other error: it ran and printed something that is not a version this
//     release can read. That is UNKNOWN, which is NOT the same as "old" (#1686).
//
// A build that prints a readable version and then exits non-zero still told us
// what it is, so the version wins over the exit status.
func ProbeVersion(path string) (Version, error) {
	ctx, cancel := context.WithTimeout(context.Background(), ProbeTimeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, path, "--version").CombinedOutput()
	if ctx.Err() != nil {
		return Version{}, fmt.Errorf("%w: %s --version did not answer within %s", ErrNotRunnable, path, ProbeTimeout)
	}
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			if v, perr := ParseVersion(string(out)); perr == nil {
				return v, nil
			}
			return Version{}, fmt.Errorf("%w: %s --version exited with status %d: %s",
				ErrNotRunnable, path, exitErr.ExitCode(), firstLines(string(out)))
		}
		return Version{}, fmt.Errorf("%w: %s: %v", ErrNotRunnable, path, err)
	}
	return ParseVersion(string(out))
}

// firstLines keeps the start of a failed binary's output for an error message:
// enough to carry a loader complaint, never a page of help text.
func firstLines(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "no output"
	}
	lines := strings.SplitN(s, "\n", 3)
	if len(lines) > 2 {
		lines = lines[:2]
	}
	s = strings.Join(lines, " / ")
	if len(s) > 300 {
		s = s[:300] + "..."
	}
	return s
}

// ParseVersion extracts the major.minor.patch triple from mydumper --version
// output. TWO shapes ship in the wild and both have to parse; newer builds
// prefix the version with "v" (#1686):
//
//	mydumper 0.10.0, built against MySQL 8.0.36                      → 0, 10, 0
//	mydumper v1.0.5-1, built against MariaDB 10.8.8 with SSL support → 1,  0, 5
//
// The first line is what Ubuntu 24.04's package (0.10.1-1ubuntu3) prints,
// measured. The prefix is NOT 1.x-only. #1686 assumed it was, which understates
// the damage. Measured directly: the mydumper/mydumper images v0.16.3-6 and
// v1.0.3-1 and a current Homebrew build all print it, while the 0.10.x builds
// Ubuntu 24.04 and Debian bookworm package do not. So every build from at least
// 0.16.3 on was unreadable here, including the 0.18 series, the FIRST that
// accepts the very flags this version gate exists to decide about. The oldest
// tag that prints it was NOT pinned down: no image below 0.16.3-6 is published
// to measure, so this says "from at least" rather than naming a boundary.
//
// That prefix is the ONLY thing needing removal. Sscanf stops at the "-" by
// itself, so the "-N" package-revision suffix already parses (measured, and
// pinned by the table cases that carry one). Do not add code for it: a guard
// that cannot fire reads as protection and is none.
//
// A version that does not parse is reported as such rather than defaulted, so
// the caller can tell "this build is old" from "this build is unreadable".
func ParseVersion(output string) (Version, error) {
	line := strings.SplitN(output, "\n", 2)[0]
	parts := strings.Fields(line)
	if len(parts) < 2 {
		return Version{}, fmt.Errorf("unexpected --version output: %q", line)
	}
	// raw is kept for the error message: quoting the post-strip value would
	// report a string mydumper never printed ("ersion" for a "version" field).
	raw := strings.TrimRight(parts[1], ",")
	var v Version
	n, scanErr := fmt.Sscanf(strings.TrimPrefix(raw, "v"), "%d.%d.%d", &v.Major, &v.Minor, &v.Patch)
	if scanErr != nil || n != 3 {
		return Version{}, fmt.Errorf("cannot parse version %q from %q", raw, line)
	}
	return v, nil
}
