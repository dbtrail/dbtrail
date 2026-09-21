// Package verdict is the one rule that turns a verify run's per-table counts
// into its run-level outcome. It lives apart from internal/verify so that the
// web interface (internal/console), which must not link the verify engine and
// what it imports, can apply the SAME rule instead of a copy (#1127 was a
// copy drifting): `bintrail verify` exits on it, and the page says what it
// says.
//
// It imports nothing on purpose.
package verdict

// Run-level outcomes. The literals are the wire format of `bintrail verify
// --format json` and of the web interface's verify endpoints.
const (
	// Verified: at least one table was proven and nothing diverged.
	Verified = "verified"
	// Mismatch: at least one table diverged from the comparison.
	Mismatch = "mismatch"
	// Error: no mismatch, but at least one table hit a hard error.
	Error = "error"
	// Unproven: tables were reported but none could be proven (every one
	// inconclusive, or none at all). An all-inconclusive run must never read
	// as "recovery verified".
	Unproven = "unproven"
	// NoPredecessor: the source has exactly one baseline, so there is nothing
	// to compare against yet. Reported, not failed.
	NoPredecessor = "no_predecessor"
)

// Of is the run verdict for the counts of one run, in the precedence the exit
// code uses: a divergence outranks an error, and a run with neither that
// proved no table is unproven, whatever inconclusive tables it holds.
// Inconclusive tables do not appear here: they neither prove nor disprove, so
// a run with some matches and some inconclusive tables is Verified, and the
// counts say how much of it was.
func Of(match, mismatch, errs int) string {
	switch {
	case mismatch > 0:
		return Mismatch
	case errs > 0:
		return Error
	case match == 0:
		return Unproven
	}
	return Verified
}
