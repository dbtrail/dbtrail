package pgcapture

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
)

// PostgreSQL pg_replication_slots.wal_status values. They are named constants so the
// safety-critical comparisons — the capture-time fail-loud in ensureSlot and the
// doctor's FAIL/WARN mapping — do not hinge on bare string literals, where a typo
// would silently disable the data-loss guard.
const (
	WalStatusReserved   = "reserved"   // within max_wal_size
	WalStatusExtended   = "extended"   // beyond max_wal_size, still retained (by the slot or wal_keep_size)
	WalStatusUnreserved = "unreserved" // past max_slot_wal_keep_size; the next checkpoint may invalidate the slot
	WalStatusLost       = "lost"       // invalidated — the WAL it needed has been removed
)

// SlotHealth is the live WAL-retention state of a replication slot, read from
// pg_replication_slots on a plain query connection. It is the foundation for #532
// WAL-retention monitoring: bintrail-pg doctor reports it on demand. (Capture startup
// uses the lighter, primary-agnostic querySlotState instead — see ensureSlot.)
//
// Health-only: callers REPORT a SlotHealth, they do not fail-loud on it. Failing
// loud on an invalidated slot is ensureSlot's job (at capture time, where skipping
// WAL would silently lose data); a health/doctor surface must keep working precisely
// so it can SHOW the bad state.
type SlotHealth struct {
	// Exists is false when no slot of that name is present (first run, or torn
	// down). All other fields are zero in that case.
	Exists bool
	// Active is true when a consumer (a walsender) currently holds the slot.
	Active bool
	// WalStatus is the slot's WAL-retention state — one of the WalStatus* constants:
	// reserved (within max_wal_size), extended (beyond max_wal_size, still retained by
	// the slot or wal_keep_size), unreserved (past max_slot_wal_keep_size; the next
	// checkpoint may invalidate it), or lost (invalidated, its WAL removed). Empty when
	// the slot is absent.
	WalStatus string
	// RestartLSN is the oldest WAL the slot still needs; the WAL the slot pins runs
	// from here to the server head. 0 when NULL (a just-created slot) or absent.
	RestartLSN pglogrepl.LSN
	// ConfirmedFlushLSN is how far the consumer has durably acked. 0 when NULL/absent.
	ConfirmedFlushLSN pglogrepl.LSN
	// CurrentWalLSN is the server's current WAL write head (pg_current_wal_lsn()).
	CurrentWalLSN pglogrepl.LSN
	// RetainedBytes is the WAL the slot pins on the source disk
	// (pg_current_wal_lsn() - restart_lsn). This is the continuous early-warning
	// signal — it rises before wal_status flips to lost. 0 when restart_lsn is NULL.
	RetainedBytes int64
	// SafeWalSize is how much more WAL can be written before this slot risks
	// invalidation (pg_replication_slots.safe_wal_size). Invalid (NULL) when
	// max_slot_wal_keep_size = -1 (unlimited retention — the production red line the
	// doctor warns about), because there is then no bound to be safely under; PostgreSQL
	// also returns NULL once a slot is already lost.
	SafeWalSize sql.NullInt64
}

// slotHealthQuery reads one slot's retention state. LSN columns are cast to ::text so
// they scan into a plain string regardless of the pg_lsn codec; restart_lsn /
// confirmed_flush_lsn / safe_wal_size can be NULL (just-created slot, or unlimited
// retention). retained_bytes is derived in SQL via pg_wal_lsn_diff. pg_current_wal_lsn()
// assumes a primary (the source we capture from); on a standby it would error, which a
// health surface reports rather than hides. The columns exist on every supported
// version (PG15+).
const slotHealthQuery = `SELECT
	s.active,
	s.wal_status,
	s.restart_lsn::text,
	s.confirmed_flush_lsn::text,
	pg_current_wal_lsn()::text,
	pg_wal_lsn_diff(pg_current_wal_lsn(), s.restart_lsn)::bigint,
	s.safe_wal_size
FROM pg_replication_slots s
WHERE s.slot_name = $1`

// slotHealthRow is the raw catalog row, scanned with NULL-tolerant types before being
// folded into a SlotHealth by toHealth (a pure step, unit-tested without a live PG).
type slotHealthRow struct {
	active            bool
	walStatus         sql.NullString
	restartLSN        sql.NullString
	confirmedFlushLSN sql.NullString
	currentWalLSN     sql.NullString
	retainedBytes     sql.NullInt64
	safeWalSize       sql.NullInt64
}

// toHealth converts a scanned row into a SlotHealth (Exists is always true here — the
// absent case is handled by QuerySlotHealth before this is called). It parses the
// text-form LSNs, treating NULL/empty as 0.
func (r slotHealthRow) toHealth() (SlotHealth, error) {
	h := SlotHealth{
		Exists:        true,
		Active:        r.active,
		WalStatus:     r.walStatus.String,
		RetainedBytes: r.retainedBytes.Int64, // .Int64 is 0 when NULL
		SafeWalSize:   r.safeWalSize,
	}
	var err error
	if h.RestartLSN, err = parseNullLSN(r.restartLSN); err != nil {
		return SlotHealth{}, fmt.Errorf("restart_lsn: %w", err)
	}
	if h.ConfirmedFlushLSN, err = parseNullLSN(r.confirmedFlushLSN); err != nil {
		return SlotHealth{}, fmt.Errorf("confirmed_flush_lsn: %w", err)
	}
	if h.CurrentWalLSN, err = parseNullLSN(r.currentWalLSN); err != nil {
		return SlotHealth{}, fmt.Errorf("current_wal_lsn: %w", err)
	}
	return h, nil
}

// parseNullLSN parses a text-form LSN ("X/Y"), returning 0 for NULL/empty.
func parseNullLSN(s sql.NullString) (pglogrepl.LSN, error) {
	if !s.Valid || s.String == "" {
		return 0, nil
	}
	return pglogrepl.ParseLSN(s.String)
}

// QuerySlotHealth reads the live WAL-retention state of slotName on a plain query
// connection (never the replication conn, which is a walsender and cannot run SQL).
// It returns SlotHealth{Exists: false} with a nil error when the slot is absent — an
// expected state (first run, torn down), not an error — so callers can distinguish
// "no slot" from "query failed".
func QuerySlotHealth(ctx context.Context, conn *pgx.Conn, slotName string) (SlotHealth, error) {
	var r slotHealthRow
	err := conn.QueryRow(ctx, slotHealthQuery, slotName).Scan(
		&r.active, &r.walStatus, &r.restartLSN, &r.confirmedFlushLSN,
		&r.currentWalLSN, &r.retainedBytes, &r.safeWalSize,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return SlotHealth{Exists: false}, nil
	}
	if err != nil {
		return SlotHealth{}, fmt.Errorf("pgcapture: querying slot health for %q: %w", slotName, err)
	}
	return r.toHealth()
}

// HealthSnapshot is a point-in-time source-side health reading the streaming daemon
// persists to the index for the console (#599): the replication slot's WAL-retention
// state plus any published table not at REPLICA IDENTITY FULL. pgstreamrun owns the
// serialized wire shape written to stream_state.source_health; this is the in-memory
// form the probe returns.
type HealthSnapshot struct {
	Slot                   SlotHealth
	ReplicaIdentityNotFull []string
	// FlushedLSN is how far the source's WAL was flushed when the probe ran
	// (QueryFlushedLSN): where a capture has to have read to for it to hold
	// everything the source had at that moment (#2845). 0 when it could not
	// be read, with FlushedLSNErr saying why; that read failing does not fail
	// the probe, since the health panel does not use it.
	FlushedLSN    uint64
	FlushedLSNErr error
}

// ProbeHealth opens a short-lived plain connection to queryDSN and reads current source
// health: the slot's WAL-retention state (QuerySlotHealth) and the published tables not
// at REPLICA IDENTITY FULL (QueryReplicaIdentityNotFull). Connect-per-call — the daemon
// polls infrequently, so a dropped connection self-heals on the next poll; the caller
// bounds the whole probe with a timeout. It never mutates the source.
func ProbeHealth(ctx context.Context, queryDSN, slotName, publication string) (HealthSnapshot, error) {
	conn, err := pgx.Connect(ctx, queryDSN)
	if err != nil {
		return HealthSnapshot{}, fmt.Errorf("pgcapture: health probe connect: %w", err)
	}
	defer conn.Close(context.Background())

	slot, err := QuerySlotHealth(ctx, conn, slotName)
	if err != nil {
		return HealthSnapshot{}, err
	}
	notFull, err := QueryReplicaIdentityNotFull(ctx, conn, publication)
	if err != nil {
		return HealthSnapshot{}, err
	}
	snap := HealthSnapshot{Slot: slot, ReplicaIdentityNotFull: notFull}
	snap.FlushedLSN, snap.FlushedLSNErr = QueryFlushedLSN(ctx, conn)
	return snap, nil
}

// DropSlot idempotently drops the replication slot on a plain query connection, for
// teardown/re-baseline (bintrail-pg reset). It returns dropped=false with a nil error
// when the slot is already absent. It errors if the slot is ACTIVE — a live consumer
// (a running `bintrail-pg stream`) holds it — surfacing PostgreSQL's "replication slot
// is active for PID N" message; the caller must stop the stream first.
func DropSlot(ctx context.Context, conn *pgx.Conn, slotName string) (bool, error) {
	// The WHERE EXISTS guard makes this a no-op (0 rows) when the slot is absent, so it
	// is idempotent; an active slot still raises an error from pg_drop_replication_slot.
	tag, err := conn.Exec(ctx,
		`SELECT pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)`,
		slotName)
	if err != nil {
		return false, fmt.Errorf("pgcapture: dropping replication slot %q: %w", slotName, err)
	}
	return tag.RowsAffected() > 0, nil
}

// querySlotState reads only a slot's existence and wal_status — a primary-agnostic
// catalog read. It is deliberately lighter than QuerySlotHealth, which adds the
// primary-only pg_current_wal_lsn()/pg_wal_lsn_diff retention metrics (those error on
// a standby, "recovery is in progress"). ensureSlot uses this at capture STARTUP so
// capture is not gratuitously broken on a standby source; the richer QuerySlotHealth
// is for the doctor health surface (documented primary). Returns exists=false with a
// nil error when the slot is absent.
func querySlotState(ctx context.Context, conn *pgx.Conn, slotName string) (exists bool, walStatus string, err error) {
	var ws sql.NullString
	err = conn.QueryRow(ctx, `SELECT wal_status FROM pg_replication_slots WHERE slot_name = $1`, slotName).Scan(&ws)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, "", nil
	}
	if err != nil {
		return false, "", fmt.Errorf("pgcapture: checking replication slot %q: %w", slotName, err)
	}
	return true, ws.String, nil
}
