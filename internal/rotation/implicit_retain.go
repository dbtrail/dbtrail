package rotation

import (
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/dbtrail/dbtrail/internal/cliutil"
)

// LegacyRetain is what an index keeps while its operator sets no retention and
// the index itself records none (#1709): the built-in default every index ran
// under before the record existed. It is a fact about the past, so it never
// follows indexer.DefaultRotateRetain — an upgrade must not silently shorten
// the window an operator has been relying on.
const LegacyRetain = "30d"

// legacyRetainDur is LegacyRetain as a duration; pinned to it by a test.
const legacyRetainDur = 30 * 24 * time.Hour

// implicitRetain is the retention one index rotates at when the operator
// configured none.
type implicitRetain struct {
	retain time.Duration
	// raw is the operator-facing spelling that goes into the log lines and the
	// "no partitions older than %s" message, always describing retain.
	raw string
	// recorded is true when the index carries the retention it was created
	// under. False means an index an older build created — or one whose record
	// could not be read — which keeps LegacyRetain and stays under the upgrade
	// guard, exactly as it did before this record existed.
	recorded bool
}

// implicitRetainFrom is the pure decision behind the implicit retention: what
// indexer.ReadInitialRetain answered, turned into the window this cycle uses.
//
// Every uncertain answer falls back to LegacyRetain, never to the current
// default: a read that failed, a record this build cannot parse and a record of
// zero all describe an index we know nothing reliable about, and keeping more
// data than the policy asks for is recoverable while dropping it is not. The
// returned error is for the log line; the retention is usable either way.
func implicitRetainFrom(value string, found bool, readErr error) (implicitRetain, error) {
	legacy := implicitRetain{retain: legacyRetainDur, raw: LegacyRetain}
	switch {
	case readErr != nil:
		return legacy, readErr
	case !found:
		return legacy, nil
	}
	d, err := cliutil.ParseRetain(value)
	if err != nil {
		return legacy, fmt.Errorf("recorded retention %q is not a window this build can read: %w", value, err)
	}
	if d <= 0 {
		return legacy, fmt.Errorf("recorded retention %q is not a positive window", value)
	}
	return implicitRetain{retain: d, raw: value, recorded: true}, nil
}

// keptRetainNoticed remembers which indexes this process has already told the
// operator about, keyed by target DSN. The line below is news about a policy,
// not a per-cycle event: an hourly loop repeating it would teach the operator
// to stop reading the log (the same reasoning as the escalation streak).
var keptRetainNoticed sync.Map

// noticeKeptRetain says, once per index per process, that this index keeps a
// retention that is no longer what a new index would start with — the upgrade
// case the record exists for. Silence here is the failure mode: an operator who
// is never told believes every index follows the documented default.
func noticeKeptRetain(key, dbName, kept, current string) {
	if _, seen := keptRetainNoticed.LoadOrStore(key, struct{}{}); seen {
		return
	}
	slog.Warn("built-in rotation: this index keeps the retention it was created under, not the current default",
		"db", dbName, "keeps", kept, "current_default", current,
		"action", "set --rotate-retain (or BINTRAIL_ROTATE_RETAIN, or the console's rotation settings) to choose one yourself")
}
