package rotation

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/status"
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
	// recordedAt is when that record was written, i.e. when the index was
	// created. History OLDER than this instant did not accumulate under the
	// recorded window: it was loaded in afterwards (restore-index rebuilding
	// an index from the archives, `bintrail index` over old binlog files), so
	// the record says nothing about it and the upgrade guard still applies.
	recordedAt time.Time
}

// implicitRetainFrom is the pure decision behind the implicit retention: what
// indexer.ReadInitialRetain answered, turned into the window this cycle uses.
//
// Every uncertain answer falls back to LegacyRetain, never to the current
// default: a read that failed, a record this build cannot parse and a record of
// zero all describe an index we know nothing reliable about, and keeping more
// data than the policy asks for is recoverable while dropping it is not. The
// returned error is for the log line; the retention is usable either way.
func implicitRetainFrom(value string, recordedAt time.Time, found bool, readErr error) (implicitRetain, error) {
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
	return implicitRetain{retain: d, raw: value, recorded: true, recordedAt: recordedAt}, nil
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

// RetainSource says where an implicit window came from, for a surface that
// reports it to an operator.
type RetainSource string

const (
	// RetainRecorded: the index carries the retention it was created under.
	RetainRecorded RetainSource = "recorded"
	// RetainLegacy: the index carries no record, so it keeps the window every
	// such index ran under before the record existed.
	RetainLegacy RetainSource = "legacy"
	// RetainUnreadable: the record could not be read, so the legacy window is
	// used — same window as RetainLegacy, different reason, and a surface must
	// not present a failed read as a fact about the index.
	RetainUnreadable RetainSource = "unreadable"
)

// Effective is the window one index rotates on while the operator sets none,
// and where it came from. It is what every SURFACE should report (doctor's
// projection, the console's capacity card and rotation panel, status), so a
// screen cannot name a window the loop does not use.
type Effective struct {
	Retain time.Duration
	Raw    string
	Source RetainSource
	// Err is set when Source is RetainUnreadable, for the log line; the window
	// is usable either way.
	Err error
}

// ResolveEffective answers for one index database, exactly as the rotation
// loop does. It never fails: an index it cannot ask keeps the legacy window,
// which is the same direction the loop takes.
func ResolveEffective(ctx context.Context, db *sql.DB, dbName string) Effective {
	value, recordedAt, found, readErr := indexer.ReadInitialRetain(ctx, db, dbName)
	imp, err := implicitRetainFrom(value, recordedAt, found, readErr)
	switch {
	case err != nil:
		return Effective{Retain: imp.retain, Raw: imp.raw, Source: RetainUnreadable, Err: err}
	case imp.recorded:
		return Effective{Retain: imp.retain, Raw: imp.raw, Source: RetainRecorded}
	default:
		return Effective{Retain: imp.retain, Raw: imp.raw, Source: RetainLegacy}
	}
}

// DescribeSource is the one sentence a surface puts beside the window, so the
// three of them cannot drift into three wordings.
func (e Effective) DescribeSource() string {
	switch e.Source {
	case RetainRecorded:
		return "the retention this index was created under"
	case RetainUnreadable:
		return "the retention indexes kept before it was recorded (this index's own record could not be read)"
	default:
		return "the retention indexes kept before it was recorded"
	}
}

// StatusInfo renders this answer for the status report, which says which
// window is in effect and where it came from. It lives here so the screen and
// the loop cannot drift: one decision, two surfaces (#1709). nil when there is
// no usable window to report.
func (e Effective) StatusInfo() *status.RetentionInfo {
	if e.Retain <= 0 {
		return nil
	}
	return &status.RetentionInfo{Raw: e.Raw, Source: e.DescribeSource(), Basis: string(e.Source)}
}
