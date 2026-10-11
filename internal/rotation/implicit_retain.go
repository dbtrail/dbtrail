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
	// RetainConfigured: the operator set the window themselves
	// (--rotate-retain or BINTRAIL_ROTATE_RETAIN; Settings.Explicit), so
	// every index rotates on it and the record is not consulted. Never
	// answered by ResolveEffective, which exists for the implicit case: a
	// surface reports it when the loop's settings say Explicit (#2315), so the
	// screen cannot project over a record the loop is not using.
	RetainConfigured RetainSource = "configured"
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
	// RecordedAt is when the record was written (the index's creation), set
	// only for RetainRecorded. The upgrade guard exempts history that
	// accumulated at or after it — see UpgradeGuard.
	RecordedAt time.Time
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
		return Effective{Retain: imp.retain, Raw: imp.raw, Source: RetainRecorded, RecordedAt: imp.recordedAt}
	default:
		return Effective{Retain: imp.retain, Raw: imp.raw, Source: RetainLegacy}
	}
}

// DropWindow is the window the loop drops ONE index's partitions on under s,
// and whether that is known. It is for a caller that has to bound something
// against what rotation WILL drop rather than report it, so every doubt
// answers not known:
//
//   - rotation off: this loop drops nothing, and an operator who turned it off
//     usually rotates on a schedule of their own that nothing here can read;
//   - an explicit window: every index drops on it;
//   - no explicit window: the index's own (effective is ResolveEffective for
//     it, asked only in this case). A record that could not be read is not
//     known, although the loop then keeps the legacy window: the loop's own
//     read, a cycle later, may succeed and drop on a shorter one.
//
// The loop can drop LESS than this says (the upgrade guard, a partition not
// archived yet), never more.
func DropWindow(s Settings, effective func() Effective) (time.Duration, bool) {
	switch {
	case !s.Enabled:
		return 0, false
	case s.Explicit:
		return s.Retain, s.Retain > 0
	}
	e := effective()
	if e.Source == RetainUnreadable || e.Retain <= 0 {
		return 0, false
	}
	return e.Retain, true
}

// DescribeSource is the one sentence a surface puts beside the window, so the
// three of them cannot drift into three wordings.
func (e Effective) DescribeSource() string {
	switch e.Source {
	case RetainRecorded:
		return "the retention this index was created under"
	case RetainUnreadable:
		return "the retention indexes kept before it was recorded (this index's own record could not be read)"
	case RetainConfigured:
		return "the retention set when the daemon was started (--rotate-retain / BINTRAIL_ROTATE_RETAIN)"
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

// Guard is the upgrade guard's answer for one index under an implicit window:
// whether the loop is refusing to drop (#2323). Blocked means every cycle
// tops up future partitions and drops NOTHING until the operator sets
// --rotate-retain, so a surface that reports the window must not present it
// as the size the index settles at — the index is growing unbounded.
type Guard struct {
	Blocked bool
	// Oldest is the oldest named hourly partition, for the sentence that says
	// how far back the history the loop will not touch reaches.
	Oldest time.Time
	// HistoryPredatesRecord is true when the index carries a record and yet
	// holds history older than it: created empty and then filled (a restore,
	// `bintrail index` over old binlog files). False for an index with no
	// record, the pre-record deployment the guard was written for.
	HistoryPredatesRecord bool
}

// guardBlocks is the pure decision the loop and every surface share: the
// guard trips on history more than twice the window old (guardTrips), and an
// index is exempt only while that history accumulated under the window it
// records — oldest partition at or after the record's own timestamp. An
// index with no record (or an unreadable one) is never exempt, and a window
// of zero has nothing to measure against. Any divergence between this and
// rotateOneIndex would be #2323 again: a card grading a window the loop is
// not enforcing.
func guardBlocks(partitions []partitionInfo, retain time.Duration, recorded bool, recordedAt time.Time, now time.Time) Guard {
	if retain <= 0 {
		return Guard{}
	}
	trips, oldest := guardTrips(partitions, retain, now)
	exempt := recorded && !oldest.IsZero() && !oldest.Before(recordedAt)
	return Guard{
		Blocked:               trips && !exempt,
		Oldest:                oldest,
		HistoryPredatesRecord: trips && !exempt && recorded,
	}
}

// UpgradeGuard evaluates the guard for one index exactly as the loop does on
// its next cycle, for a surface that reports e (ResolveEffective's answer).
// It is only meaningful under an implicit window: with Settings.Explicit the
// loop never consults the guard, and a caller that knows the window was set
// explicitly must not call this. The error is the partition listing failing;
// the loop then skips drops for that one cycle and retries, so a surface
// treats it as "not known to be blocked", never as blocked.
func UpgradeGuard(ctx context.Context, db *sql.DB, dbName string, e Effective) (Guard, error) {
	partitions, err := listPartitions(ctx, db, dbName)
	if err != nil {
		return Guard{}, err
	}
	return guardBlocks(partitions, e.Retain, e.Source == RetainRecorded, e.RecordedAt, time.Now()), nil
}
