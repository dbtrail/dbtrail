package streamrun

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	drivermysql "github.com/go-sql-driver/mysql"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/observe"
	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/serverid"
)

// ─── streamState ───────────────────────────────────────────────────────────────────

// streamState holds the current replication position and counters used for
// checkpointing. It is persisted to stream_state after each checkpoint interval.
type streamState struct {
	mode       string // "position" or "gtid"
	binlogFile string
	binlogPos  uint64 // per-EVENT byte offset — advances on every event (metrics/display; GTID-mode checkpoint + dedup)
	// safeFile/safePos hold the last POSITION-mode SAFE resume point: a byte
	// offset that only advances at a statement (STMT_END_F) / commit / DDL
	// boundary, never mid-statement. saveCheckpoint persists these in position
	// mode so a resume can never land between the ROWS_EVENTs of one split
	// statement and crash StartSync with "invalid table id, no corresponding
	// table map event" (#775). Mirrors the commit-only gtidSet semantics of #491,
	// but works for non-GTID sources too (which emit no EventCommit). Unused in
	// GTID mode (that path keeps checkpointing the per-event binlogPos, which
	// deleteEventsSinceCheckpointGTID relies on).
	safeFile      string
	safePos       uint64
	gtidSet       string // serialized GTID set (GTID mode only)
	flavor        string // source flavor: "mysql" (default) or "mariadb"; selects the GTID parser on resume
	eventsIndexed int64
	lastEventTime sql.NullTime
	serverID      uint32
	bintrailID    string // resolved server identity (empty = unknown, stored as NULL)

	// skips is the shared capture-skip tally (#1034): the StreamParser records
	// every event it read and dropped (column-count mismatch, statement-format
	// DML, ...) and saveCheckpoint persists the counters to
	// stream_state.capture_skips alongside the position, so `status` can render
	// the Capture health verdict. nil (external/test callers, --reset's fresh
	// state, the gap auto-advance stamp) persists NULL, which the upsert's
	// COALESCE turns into "preserve whatever is already there".
	skips *parser.SkipCounters

	// accGTID is the in-memory accumulated GTID set (GTID mode only).
	// It is serialized to gtidSet on checkpoint. Typed as the gomysql.GTIDSet
	// interface so it can hold either a *MysqlGTIDSet or, for a MariaDB source,
	// a *MariadbGTIDSet. Position mode leaves it a true nil interface, which the
	// advanceGTID guard relies on.
	accGTID gomysql.GTIDSet
}

// loadStreamState loads the saved stream_state row, returning nil if no row exists.
func loadStreamState(db *sql.DB) (*streamState, error) {
	var s streamState
	var gtidSet, bintrailID sql.NullString
	err := db.QueryRow(`
		SELECT mode, binlog_file, binlog_position, gtid_set, flavor,
		       events_indexed, last_event_time, server_id, bintrail_id
		FROM stream_state WHERE id = 1`).Scan(
		&s.mode, &s.binlogFile, &s.binlogPos, &gtidSet, &s.flavor,
		&s.eventsIndexed, &s.lastEventTime, &s.serverID, &bintrailID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("query stream_state: %w", err)
	}
	if gtidSet.Valid {
		s.gtidSet = gtidSet.String
	}
	if bintrailID.Valid {
		s.bintrailID = bintrailID.String
	}
	return &s, nil
}

// checkpointPosition returns the (binlog_file, binlog_position) to persist for a
// checkpoint. POSITION mode persists the last statement/commit/DDL boundary
// (safeFile/safePos) instead of the per-event binlogFile/binlogPos, so a resume
// never lands on a byte offset mid-statement — StartSync from between the
// ROWS_EVENTs of one split statement closes the stream with "invalid table id, no
// corresponding table map event" and crash-loops the supervisor (#775). GTID mode
// keeps persisting the per-event binlogPos: its resume is transaction-aligned
// (StartSyncGTID) and deleteEventsSinceCheckpointGTID relies on that offset. The
// safeFile=="" fallback keeps external/test callers that never populate safe*
// (e.g. a hand-built streamState) on the pre-#775 behavior.
func checkpointPosition(state *streamState) (string, uint64) {
	if state.mode == "position" && state.safeFile != "" {
		return state.safeFile, state.safePos
	}
	return state.binlogFile, state.binlogPos
}

// isPositionCheckpointBoundary reports whether an event marks a valid POSITION-mode
// resume point: a statement end (a row event of the ROWS_EVENT carrying STMT_END_F),
// a transaction commit (EventCommit), or a DDL (EventDDL). Every other position —
// notably the byte offset between two ROWS_EVENTs of one split statement — is unsafe
// to resume from (#775).
func isPositionCheckpointBoundary(ev parser.Event) bool {
	switch ev.EventType {
	case parser.EventCommit, parser.EventDDL:
		return true
	}
	return ev.StmtEnd
}

// checkpointInsertArgs builds the placeholder arguments for the shared
// stream_state INSERT column list (id=1, mode, binlog_file, binlog_position,
// gtid_set, flavor, events_indexed, last_event_time, last_checkpoint=UTC_TIMESTAMP(),
// server_id, bintrail_id) used by saveCheckpoint and the persistGapAutoAdvance
// stamp: NULLs for empty gtid_set/last_event_time/bintrail_id, the flavor
// canonicalized (empty→mysql; an invalid flavor fails loud rather than
// persisting garbage into the NOT NULL column), and the #775 safe-boundary
// position (see checkpointPosition).
func checkpointInsertArgs(state *streamState) ([]any, error) {
	var gtidSet any
	if state.gtidSet != "" {
		gtidSet = state.gtidSet
	}
	var lastEventTime any
	if state.lastEventTime.Valid {
		lastEventTime = state.lastEventTime.Time
	}
	var bintrailIDArg any
	if state.bintrailID != "" {
		bintrailIDArg = state.bintrailID
	}
	flavor, err := normalizeFlavor(state.flavor)
	if err != nil {
		return nil, err
	}
	file, pos := checkpointPosition(state)
	return []any{state.mode, file, pos, gtidSet, flavor,
		state.eventsIndexed, lastEventTime, state.serverID, bintrailIDArg}, nil
}

// saveCheckpoint persists the current stream state to the stream_state table.
//
// capture_skips (#1034) rides every checkpoint — it is the only writer of that
// column. A nil state.skips serializes to NULL and the update arm's COALESCE
// preserves the existing value, so the checkpoint-shaped writers that build a
// bare streamState (--reset's fresh state, the gap auto-advance stamp, tests)
// never wipe the monotonic counters a daemon persisted.
func saveCheckpoint(db *sql.DB, state *streamState) error {
	args, err := checkpointInsertArgs(state)
	if err != nil {
		return err
	}
	var captureSkips any
	if state.skips != nil {
		snap, err := state.skips.Snapshot()
		if err != nil {
			return fmt.Errorf("serialize capture-skip counters: %w", err)
		}
		captureSkips = snap
	}
	args = append(args, captureSkips)
	// mode is in the UPDATE arm for the cross-mode --reset path (#1079): the
	// reset no longer DELETEs the row, so the mode switch must land through
	// this upsert. For every other caller mode is invariant across the run.
	// #959: bound the checkpoint write with a deadline (see indexer.WriteTimeout)
	// so a mid-statement network stall fails fast instead of freezing the daemon
	// on kernel TCP retransmission — invisible to the supervisor.
	ctx, cancel := context.WithTimeout(context.Background(), indexer.WriteTimeout)
	defer cancel()
	_, err = db.ExecContext(ctx, `
		INSERT INTO stream_state
		    (id, mode, binlog_file, binlog_position, gtid_set, flavor,
		     events_indexed, last_event_time, last_checkpoint, server_id, bintrail_id,
		     capture_skips)
		VALUES (1, ?, ?, ?, ?, ?, ?, ?, UTC_TIMESTAMP(), ?, ?, ?)
		ON DUPLICATE KEY UPDATE
		    mode            = VALUES(mode),
		    binlog_file     = VALUES(binlog_file),
		    binlog_position = VALUES(binlog_position),
		    gtid_set        = VALUES(gtid_set),
		    flavor          = VALUES(flavor),
		    events_indexed  = VALUES(events_indexed),
		    last_event_time = VALUES(last_event_time),
		    last_checkpoint = UTC_TIMESTAMP(),
		    server_id       = VALUES(server_id),
		    bintrail_id     = VALUES(bintrail_id),
		    capture_skips   = COALESCE(VALUES(capture_skips), capture_skips)`,
		args...)
	return err
}

// loadCaptureSkips reads the persisted capture-skip counters (#1034) so a
// restarting daemon resumes the monotonic tallies instead of zeroing the
// DEGRADED verdict. Runs after EnsureSchema (One's step order), so the column
// exists; an empty table returns "" (nothing persisted yet).
func loadCaptureSkips(db *sql.DB) (string, error) {
	var raw sql.NullString
	err := db.QueryRow(`SELECT capture_skips FROM stream_state WHERE id = 1`).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return raw.String, nil
}

// deleteEventsSinceCheckpoint removes rows at or beyond (file, pos) from
// binlog_events — the resume-time dedup for #759 (see the call site in One).
// file == "" (no prior checkpoint) is a no-op. "Later file" is
// length-then-lexicographic (mirrors buildQuery's UntilPos cut in
// internal/query/query.go, #840): MySQL pads the numeric suffix to a fixed
// width, so after mysql-bin.999999 comes mysql-bin.1000000 and plain
// `binlog_file > ?` would invert — a straight lexicographic compare puts the
// pre-rollover file "greater" and wrongly deletes already-indexed rows on
// every resume that straddles a rollover. Equal-length names — the same
// padded width — keep the plain string comparison as the fast path.
// binlog_events is partitioned by event_timestamp, not binlog_file, so this
// is a full scan on every resume.
func deleteEventsSinceCheckpoint(db *sql.DB, file string, pos uint64) (int64, error) {
	if file == "" {
		return 0, nil
	}
	res, err := db.Exec(`
		DELETE FROM binlog_events
		WHERE (CHAR_LENGTH(binlog_file) > CHAR_LENGTH(?)
		    OR (CHAR_LENGTH(binlog_file) = CHAR_LENGTH(?) AND binlog_file > ?))
		    OR (binlog_file = ? AND start_pos >= ?)`,
		file, file, file, file, pos)
	if err != nil {
		return 0, fmt.Errorf("delete events since checkpoint %s:%d: %w", file, pos, err)
	}
	return res.RowsAffected()
}

// deleteEventsSinceCheckpointGTID is the GTID-mode sibling of
// deleteEventsSinceCheckpoint. GTID mode needs an extra pass: binlog_position
// advances on EVERY event (streamLoop), but gtid_set only advances at commit
// (#491), so a time-based checkpoint tick can persist a binlog_position that
// sits mid-transaction — ahead of what the durable gtid_set covers. Rows from
// that still-open transaction may already be flushed to binlog_events (batch
// flushes are not commit-aligned) with start_pos BELOW the checkpoint's
// binlog_position. On resume, StartSyncGTID replays the whole open
// transaction from its start (GTID replay is transaction-, not
// byte-aligned), so those straggler rows would duplicate.
//
// After the position-keyed delete, this scans the same binlog_file for
// distinct GTIDs below pos and removes any not contained in savedSet — i.e.
// transactions the checkpoint's gtid_set had not yet durably recorded.
// Scoped to one file: a transaction never spans a binlog rotation, so an
// open transaction's rows always live in the file the checkpoint was last
// written against.
func deleteEventsSinceCheckpointGTID(db *sql.DB, file string, pos uint64, savedSet gomysql.GTIDSet, flavor string) (int64, error) {
	n, err := deleteEventsSinceCheckpoint(db, file, pos)
	if err != nil || file == "" || savedSet == nil {
		return n, err
	}

	rows, err := db.Query(`
		SELECT DISTINCT gtid FROM binlog_events
		WHERE binlog_file = ? AND start_pos < ? AND gtid IS NOT NULL`,
		file, pos)
	if err != nil {
		return n, fmt.Errorf("scan pre-checkpoint gtids for stragglers: %w", err)
	}
	defer rows.Close()

	var stragglers []string
	for rows.Next() {
		var g string
		if err := rows.Scan(&g); err != nil {
			return n, fmt.Errorf("scan straggler gtid: %w", err)
		}
		single, parseErr := parseGTIDSetForFlavor(flavor, g)
		if parseErr != nil {
			slog.Warn("dedup-on-resume: failed to parse a row's gtid; deleting it defensively as a possible straggler",
				"gtid", g, "error", parseErr)
			stragglers = append(stragglers, g)
			continue
		}
		if !savedSet.Contain(single) {
			stragglers = append(stragglers, g)
		}
	}
	if err := rows.Err(); err != nil {
		return n, fmt.Errorf("iterate straggler gtids: %w", err)
	}
	if len(stragglers) == 0 {
		return n, nil
	}

	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(stragglers)), ",")
	args := make([]any, len(stragglers))
	for i, g := range stragglers {
		args[i] = g
	}
	res, err := db.Exec(`DELETE FROM binlog_events WHERE gtid IN (`+placeholders+`)`, args...)
	if err != nil {
		return n, fmt.Errorf("delete straggler gtid events: %w", err)
	}
	n2, err := res.RowsAffected()
	return n + n2, err
}

// persistGapAutoAdvance durably records an unfillable-gap auto-advance. It stamps
// gap_lost_at FIRST, then writes the advanced checkpoint — never the reverse.
// Ordering is a data-loss-safety invariant: once the checkpoint is advanced past
// the purge floor the next restart sees no gap, so a loss record written
// afterwards (and failing) would let the advanced checkpoint silently outlive the
// only durable trace of the permanently lost events — the console would show a
// healthy RUNNING badge over a stream that skipped data (#402). Both writes fail
// loud; a failed stamp aborts startup with the OLD checkpoint intact, so the gap
// is re-detected and re-recorded on the next start. saveCheckpoint's upsert does
// not touch the gap_lost_* columns, so the stamp survives it. Cleared by an
// explicit monitor Stop; a jumping --reset re-stamps it with the discarded
// range (#1079) and a same-position --reset leaves it intact.
//
// The stamp is an INSERT…ON DUPLICATE KEY UPDATE, not a bare UPDATE (#1081): the
// stream_state row can vanish between loadStreamState and this stamp (operator
// DELETE, a concurrent pre-#1079 --reset, a concurrent bintrail-pg reset on a
// shared index) — a bare UPDATE would match zero rows without error and the
// subsequent saveCheckpoint would seed a fresh row with NULL gap_lost_* columns:
// checkpoint advanced, loss unrecorded. The existing-row arm updates ONLY the
// gap_lost_* columns — the checkpoint advance still lands exclusively in the
// second statement, preserving the stamp-before-advance ordering; the
// missing-row arm seeds the advanced state and the stamp atomically, so on that
// arm the advance cannot land without the stamp. That guarantee is per-arm, not
// per-pair: a row DELETEd between an existing-row stamp and saveCheckpoint is
// re-seeded by saveCheckpoint's INSERT arm without gap_lost_* — a residual
// milliseconds-wide window (vs the minutes-wide one this closes) that only a
// single combined stamp+advance statement would eliminate. Deliberately no RowsAffected check:
// go-sql-driver reports CHANGED rows, not matched, so an identical re-stamp
// within the same UTC_TIMESTAMP() second (supervisor crash-loop) would report 0
// and abort a healthy startup.
func persistGapAutoAdvance(db *sql.DB, advanced *streamState, gapMessage string) error {
	args, err := checkpointInsertArgs(advanced)
	if err != nil {
		return fmt.Errorf("failed to persist gap-loss record before auto-advance: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), indexer.WriteTimeout)
	defer cancel()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO stream_state
		    (id, mode, binlog_file, binlog_position, gtid_set, flavor,
		     events_indexed, last_event_time, last_checkpoint, server_id, bintrail_id,
		     gap_lost_at, gap_lost_detail)
		VALUES (1, ?, ?, ?, ?, ?, ?, ?, UTC_TIMESTAMP(), ?, ?, UTC_TIMESTAMP(), ?)
		ON DUPLICATE KEY UPDATE
		    gap_lost_at     = UTC_TIMESTAMP(),
		    gap_lost_detail = VALUES(gap_lost_detail)`,
		append(args, gapMessage)...); err != nil {
		return fmt.Errorf("failed to persist gap-loss record before auto-advance: %w", err)
	}
	if err := saveCheckpoint(db, advanced); err != nil {
		return fmt.Errorf("failed to save advanced checkpoint: %w", err)
	}
	return nil
}

// resetJumpDetail classifies a --reset checkpoint discard. It returns
// noop=true when the new start position equals the discarded checkpoint —
// nothing was skipped. Otherwise it returns a human-readable description of
// the jumped-over range, suitable for gap_lost_detail. The check is
// deliberately conservative in three ways: cross-mode discards (position→gtid
// or the reverse) have no comparable coordinates and are treated as a jump;
// direction is not inferred, so a reset to an EARLIER position also records a
// loss verdict; and equality is coordinates-only — after a source rebuild
// (RESET MASTER + restore) the same coordinates can name a different history,
// which no checkpoint comparison can see. GTID sets are compared structurally
// with the parser for flavor (MySQL UUID sets vs MariaDB domain-server-seq),
// so formatting differences never fake a jump on either flavor.
func resetJumpDetail(old *streamState, flavor, mode, file string, pos uint32, gtidSet string) (noop bool, detail string) {
	switch {
	case old.mode == "position" && mode == "position":
		if old.binlogFile == file && old.binlogPos == uint64(pos) {
			return true, ""
		}
	case old.mode == "gtid" && mode == "gtid":
		equal := gtidSetsEqual
		if flavor == gomysql.MariaDBFlavor {
			equal = mariadbGTIDSetsEqual
		}
		if equal(old.gtidSet, gtidSet) {
			return true, ""
		}
	}
	return false, fmt.Sprintf(
		"checkpoint discarded via --reset: was %s, restarting at %s; events in between were skipped and are permanently lost",
		describeCheckpoint(old.mode, old.binlogFile, old.binlogPos, old.gtidSet),
		describeCheckpoint(mode, file, uint64(pos), gtidSet))
}

// classifyResetDiscard wraps resetJumpDetail with one refinement: a --reset
// that discarded a POSITION-mode checkpoint and restarted in AUTO-DISCOVERED
// GTID mode (the #1131 first-run path on a gtid_mode=ON source) always lands
// in resetJumpDetail's cross-mode arm, which cannot compare coordinates and
// so reports a jump — stamping gap_lost_at and printing "permanently lost"
// even when the old checkpoint sits exactly at the source's current position,
// i.e. nothing was skipped. That stamp is sticky: it permanently fails
// `status --fail-on-gap` and degrades `verify --check recover` to
// inconclusive, a false-alarm class worse than the noise it prevents.
//
// For exactly that combination (old.mode=="position", new mode=="gtid",
// start point auto-discovered, not operator-specified) the source's current
// binlog position is consulted: old == current ⇒ no-op, no stamp. On any
// discovery error or coordinate inequality the conservative jump
// classification stands — over-reporting a gap is recoverable review noise;
// under-reporting one is silent data loss.
func classifyResetDiscard(old *streamState, flavor, mode, file string, pos uint32, gtidSet string,
	autoDiscovered bool, currentPosition func() (string, uint32, error),
) (noop bool, detail string) {
	noop, detail = resetJumpDetail(old, flavor, mode, file, pos, gtidSet)
	if noop || !autoDiscovered || old.mode != "position" || mode != "gtid" || currentPosition == nil {
		return noop, detail
	}
	curFile, curPos, err := currentPosition()
	if err != nil {
		slog.Warn("could not read the source's current binlog position to refine the reset classification; keeping the conservative gap record",
			"error", err)
		return noop, detail
	}
	if old.binlogFile == curFile && old.binlogPos == uint64(curPos) {
		slog.Info("reset switched position→GTID mode at the source's current coordinates; no events were skipped",
			"file", curFile, "pos", curPos)
		return true, ""
	}
	return noop, detail
}

// persistResetDiscard durably persists a --reset checkpoint discard once the
// new start position is resolved. A jump is recorded exactly like an
// unfillable-gap auto-advance: gap_lost stamp FIRST, then the fresh
// checkpoint (the #402 ordering invariant, via persistGapAutoAdvance),
// user-visible output and the supervisor hook only after the record is
// durable. A no-op discard skipped nothing, so only the fresh checkpoint is
// written and any prior gap_lost record is left untouched; a jump replaces a
// prior record's detail with the reset detail (the loss flag itself never
// clears here — monitor Stop remains the acknowledgment path).
func persistResetDiscard(db *sql.DB, fresh *streamState, noop bool, detail string, hooks *Hooks) error {
	if noop {
		if err := saveCheckpoint(db, fresh); err != nil {
			return fmt.Errorf("failed to persist reset checkpoint: %w", err)
		}
		slog.Info("--reset discarded a checkpoint at the same position; nothing was skipped")
		return nil
	}
	slog.Warn("--reset is skipping binlog history", "detail", detail)
	if err := persistGapAutoAdvance(db, fresh, detail); err != nil {
		return err
	}
	fmt.Printf("Reset: %s\n", detail)
	if hooks != nil && hooks.OnGapAutoAdvance != nil {
		hooks.OnGapAutoAdvance(detail)
	}
	return nil
}

// describeCheckpoint renders a checkpoint's coordinates for human-readable
// loss records: "file:pos" in position mode, "gtid_set <set>" in GTID mode.
func describeCheckpoint(mode, file string, pos uint64, gtidSet string) string {
	if mode == "gtid" {
		return fmt.Sprintf("gtid_set %s", gtidSet)
	}
	return fmt.Sprintf("%s:%d", file, pos)
}

// ─── TLS configuration ───────────────────────────────────────────────────────────────

// buildTLSConfig returns a *tls.Config for the given ssl-mode, or nil for
// "disabled". serverName is the target host (used only for verify-identity).
func buildTLSConfig(mode, ca, cert, key, serverName string) (*tls.Config, error) {
	if mode == "disabled" {
		return nil, nil
	}
	switch mode {
	case "preferred", "required", "verify-ca", "verify-identity":
	default:
		return nil, fmt.Errorf("invalid --ssl-mode %q: must be one of disabled, preferred, required, verify-ca, verify-identity", mode)
	}
	if (cert == "") != (key == "") {
		return nil, fmt.Errorf("--ssl-cert and --ssl-key must both be specified together")
	}

	cfg := &tls.Config{}

	// Load CA pool (optional — system CAs used when empty).
	var caPool *x509.CertPool
	if ca != "" {
		pem, err := os.ReadFile(ca)
		if err != nil {
			return nil, fmt.Errorf("read --ssl-ca %q: %w", ca, err)
		}
		caPool = x509.NewCertPool()
		if !caPool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("--ssl-ca %q: no valid certificates found", ca)
		}
		cfg.RootCAs = caPool
	}

	// Load client certificate for mutual TLS.
	if cert != "" {
		kp, err := tls.LoadX509KeyPair(cert, key)
		if err != nil {
			return nil, fmt.Errorf("load --ssl-cert/--ssl-key: %w", err)
		}
		cfg.Certificates = []tls.Certificate{kp}
	}

	switch mode {
	case "preferred", "required":
		// Encrypt the connection but skip server certificate verification.
		cfg.InsecureSkipVerify = true //nolint:gosec // intentional for these modes
	case "verify-ca":
		// Verify the certificate chain against the CA pool but not the hostname.
		cfg.InsecureSkipVerify = true //nolint:gosec // hostname check done via VerifyConnection
		cfg.VerifyConnection = func(cs tls.ConnectionState) error {
			if len(cs.PeerCertificates) == 0 {
				return errors.New("server presented no certificate")
			}
			opts := x509.VerifyOptions{
				Roots:         caPool, // nil → system CAs
				Intermediates: x509.NewCertPool(),
			}
			for _, c := range cs.PeerCertificates[1:] {
				opts.Intermediates.AddCert(c)
			}
			_, err := cs.PeerCertificates[0].Verify(opts)
			return err
		}
	case "verify-identity":
		// Full TLS verification: certificate chain + hostname.
		cfg.ServerName = serverName
	}

	return cfg, nil
}

// connectHelper opens an index or source helper connection (config.ConnectWithTLS)
// honoring --ssl-mode, mirroring the binlog syncer's TLS via the same
// buildTLSConfig. These connections carry full row images (PII) and credentials,
// so --ssl-mode must protect them too — not only the replication stream (#946).
// label names the connection in log/error messages.
//
// Under "preferred" it does the SAME explicit two-step as the syncer: try TLS,
// and only if the server genuinely lacks TLS log a loud warning and retry in
// cleartext — never the driver's OWN silent AllowFallbackToPlaintext, so every
// connection that may send data unencrypted warns identically. required/verify-*
// never retry: a TLS-incapable server fails closed with an actionable hint.
func connectHelper(dsn, label, mode, ca, cert, key string) (*sql.DB, error) {
	tlsCfg, err := buildTLSConfig(mode, ca, cert, key, config.DSNHost(dsn))
	if err != nil {
		return nil, err
	}
	// A tls= in the DSN wins (operator override) — but under a mandatory
	// --ssl-mode that override can silently WEAKEN the connection (e.g. a stale
	// tls=skip-verify under --ssl-mode=verify-identity → encrypted but
	// unauthenticated). Surface the precedence so it is a conscious choice. The
	// message stays neutral: the DSN's own tls= may also be *stronger* than the
	// mode, and we can't tell which here (#946).
	if tlsHint(mode) != "" && config.DSNHasExplicitTLS(dsn) {
		slog.Warn("this connection's DSN sets its own tls= parameter, which takes "+
			"precedence over --ssl-mode; verify it meets your security requirement",
			"connection", label, "ssl_mode", mode)
	}

	db, err := config.ConnectWithTLS(dsn, tlsCfg)
	if err == nil {
		return db, nil
	}
	// preferred: retry in cleartext ONLY when the server genuinely lacks TLS, and
	// log it loudly (never the driver's silent AllowFallbackToPlaintext). An
	// explicit tls= in the DSN survives this nil retry (applyTLS keeps it), so the
	// retry re-attempts the same config and fails closed rather than downgrading.
	if mode == "preferred" && isTLSUnsupportedError(err) {
		slog.Warn("server does not support TLS; connecting WITHOUT encryption "+
			"(--ssl-mode preferred) — credentials and data will be sent in cleartext",
			"connection", label, "error", err)
		db, err = config.ConnectWithTLS(dsn, nil)
		if err != nil {
			return nil, fmt.Errorf("connect %s (cleartext retry): %w", label, err)
		}
		return db, nil
	}
	return nil, fmt.Errorf("connect %s%s: %w", label, tlsHint(mode), err)
}

// tlsHint returns a parenthetical remediation appended to a helper-connection
// error when --ssl-mode mandates TLS, so a server that lacks TLS fails with an
// actionable message instead of a bare handshake error (#946).
func tlsHint(mode string) string {
	switch mode {
	case "required", "verify-ca", "verify-identity":
		return fmt.Sprintf(" (--ssl-mode=%s requires TLS; enable TLS on the server, "+
			"or use --ssl-mode=preferred to fall back to an unencrypted connection)", mode)
	default:
		return ""
	}
}

// isTLSUnsupportedError reports whether err means the server does not support
// TLS at all — the ONLY condition under which --ssl-mode=preferred may retry in
// cleartext. The go-sql-driver helper connections return the sentinel
// drivermysql.ErrNoTLS; the go-mysql binlog syncer returns a fixed message when
// the server omits the CLIENT_SSL capability (client/auth.go); a mid-handshake
// plaintext reply surfaces as tls.RecordHeaderError. Every other failure (auth
// denied, unreachable host, bad binlog position) must NOT trigger a downgrade,
// or credentials and data would be resent unencrypted on an unrelated error
// (#947).
func isTLSUnsupportedError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, drivermysql.ErrNoTLS) {
		return true
	}
	var rhe tls.RecordHeaderError
	if errors.As(err, &rhe) {
		return true
	}
	return strings.Contains(err.Error(), "the MySQL Server does not support TLS")
}

// ─── Start position resolution ───────────────────────────────────────────────

// NormalizeGTIDSet zero-pads each UUID in a GTID set to the standard
// 8-4-4-4-12 format. Some MySQL-compatible services (e.g. Amazon RDS) return
// GTIDs with leading zeros stripped from UUID segments, producing UUIDs shorter
// than 36 characters (e.g. "5512139-1432-11f1-8d8d-0693b428a89b" instead of
// "05512139-1432-11f1-8d8d-0693b428a89b"). The go-mysql library requires
// standard-length UUIDs, so this function normalizes before parsing.
func NormalizeGTIDSet(s string) string {
	// Expected segment lengths in a UUID: 8-4-4-4-12.
	segLens := [5]int{8, 4, 4, 4, 12}

	// A GTID set is comma-separated entries like "uuid:intervals,uuid:intervals".
	entries := strings.Split(s, ",")
	for i, entry := range entries {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		// Split "uuid:intervals" at the first colon.
		colon := strings.IndexByte(entry, ':')
		if colon < 0 {
			continue // malformed, let the parser handle it
		}
		uuid := entry[:colon]
		rest := entry[colon:] // includes the ":"

		parts := strings.Split(uuid, "-")
		if len(parts) != 5 {
			continue // not a UUID shape, let the parser handle it
		}

		changed := false
		for j, seg := range parts {
			if len(seg) < segLens[j] {
				parts[j] = strings.Repeat("0", segLens[j]-len(seg)) + seg
				changed = true
			}
		}
		if changed {
			entries[i] = strings.Join(parts, "-") + rest
		} else {
			entries[i] = entry
		}
	}
	return strings.Join(entries, ",")
}

// parseGTIDSetForFlavor parses a GTID set string with the parser for the given
// flavor: ParseMariadbGTIDSet for "mariadb", ParseMysqlGTIDSet otherwise. Both
// library functions return the gomysql.GTIDSet interface (and a true-nil
// interface on error), so this never produces a typed-nil. The caller is
// responsible for any flavor-appropriate normalization first (see
// normalizeGTIDForFlavor).
func parseGTIDSetForFlavor(flavor, s string) (gomysql.GTIDSet, error) {
	if flavor == gomysql.MariaDBFlavor {
		return gomysql.ParseMariadbGTIDSet(s)
	}
	return gomysql.ParseMysqlGTIDSet(s)
}

// normalizeGTIDForFlavor zero-pads UUIDs for MySQL (see NormalizeGTIDSet) and
// leaves MariaDB domain-server-seq GTIDs untouched (they have no UUID to pad).
func normalizeGTIDForFlavor(flavor, s string) string {
	if flavor == gomysql.MariaDBFlavor {
		return s
	}
	return NormalizeGTIDSet(s)
}

// normalizeFlavor canonicalizes a source flavor: empty defaults to MySQL,
// "mysql"/"mariadb" pass through, and anything else is rejected. It is the
// single home for both the empty→mysql default and the supported-flavor check,
// used at stream startup (One) and on checkpoint persistence (saveCheckpoint).
func normalizeFlavor(flavor string) (string, error) {
	switch flavor {
	case "":
		return gomysql.MySQLFlavor, nil
	case gomysql.MySQLFlavor, gomysql.MariaDBFlavor:
		return flavor, nil
	default:
		return "", fmt.Errorf("invalid source flavor %q: must be %q or %q", flavor, gomysql.MySQLFlavor, gomysql.MariaDBFlavor)
	}
}

// resolveStart determines the start position for replication for a MySQL source.
// It is a thin wrapper over resolveStartForFlavor; see that function for the
// full contract. Kept so existing MySQL callers and tests are unchanged.
func resolveStart(
	startFile, startGTID string, startPos uint32,
	saved *streamState,
) (mode, file, gtidStr string, pos uint32, accGTID gomysql.GTIDSet, err error) {
	return resolveStartForFlavor(startFile, startGTID, startPos, saved, gomysql.MySQLFlavor)
}

// resolveStartForFlavor determines the start position for replication. It returns
// the mode ("position" or "gtid"), file, GTID string, pos, and an optional
// pre-parsed GTID set (non-nil only in GTID mode). flavor selects the GTID
// parser; for "mariadb" the set is parsed as domain-server-seq and not UUID
// zero-padded. Position mode returns a true-nil accGTID interface.
func resolveStartForFlavor(
	startFile, startGTID string, startPos uint32,
	saved *streamState, flavor string,
) (mode, file, gtidStr string, pos uint32, accGTID gomysql.GTIDSet, err error) {
	// Saved checkpoint takes priority — makes re-running the same command
	// idempotent (the user doesn't need to remove --start-file to resume).
	// Exception: if the user explicitly requests a *different* mode than the
	// saved checkpoint (e.g. saved=position but --start-gtid given, or
	// saved=gtid but --start-file given), honor the flag to allow seamless
	// mode switching without deleting stream_state.
	if saved != nil {
		if startFile != "" && startGTID != "" {
			return "", "", "", 0, nil, fmt.Errorf("--start-file and --start-gtid are mutually exclusive")
		}

		// Reject resuming a checkpoint under a different source flavor. The saved
		// set's format is fixed by the flavor that wrote it; continuing under
		// another flavor would parse the saved set one way while the BinlogSyncer
		// handshakes — and the next checkpoint persists — as another, a latent
		// corruption. Rows predating the column read back as the migration default
		// 'mysql'; only a genuinely empty flavor (defensive) adopts the requested
		// one. --reset clears the checkpoint (saved == nil) and bypasses this.
		if saved.flavor != "" && saved.flavor != flavor {
			return "", "", "", 0, nil, fmt.Errorf(
				"saved checkpoint is source flavor %q but %q was requested; pass --source-flavor %s (or --reset to start fresh)",
				saved.flavor, flavor, saved.flavor)
		}

		// Detect mode switch: user explicitly requests a different mode.
		switchToGTID := saved.mode == "position" && startGTID != "" && startFile == ""
		switchToPosition := saved.mode == "gtid" && startFile != "" && startGTID == ""

		if switchToGTID {
			slog.Warn("switching from position mode to GTID mode", "old_file", saved.binlogFile, "old_pos", saved.binlogPos)
			startGTID = normalizeGTIDForFlavor(flavor, startGTID)
			gs, parseErr := parseGTIDSetForFlavor(flavor, startGTID)
			if parseErr != nil {
				return "", "", "", 0, nil, fmt.Errorf("invalid --start-gtid: %w", parseErr)
			}
			return "gtid", "", startGTID, 0, gs, nil
		}
		if switchToPosition {
			slog.Warn("switching from GTID mode to position mode", "old_gtid_set", saved.gtidSet)
			return "position", startFile, "", startPos, nil, nil
		}

		// Same mode or no flags — resume from saved state.
		if startFile != "" || startGTID != "" {
			slog.Warn("checkpoint exists; ignoring --start-file/--start-gtid and resuming from saved state")
		}
		if saved.mode == "gtid" {
			// flavor is authoritative here: the mismatch guard above guarantees
			// saved.flavor is either empty (legacy) or equal to flavor.
			normalized := normalizeGTIDForFlavor(flavor, saved.gtidSet)
			slog.Info("resuming from GTID set", "gtid_set", normalized, "flavor", flavor)
			gs, parseErr := parseGTIDSetForFlavor(flavor, normalized)
			if parseErr != nil {
				return "", "", "", 0, nil, fmt.Errorf("invalid saved gtid_set %q: %w", saved.gtidSet, parseErr)
			}
			return "gtid", "", normalized, 0, gs, nil
		}
		// stream_state.binlog_position is BIGINT UNSIGNED but COM_BINLOG_DUMP
		// carries the offset as uint32, so a checkpoint past 4GiB (a single
		// transaction larger than max_binlog_size delays rotation until commit)
		// cannot be addressed in position mode. Casting would silently wrap to
		// an arbitrary earlier offset and re-index history; fail loud instead.
		//
		// NOTE (#845): every current writer of binlogPos derives it from
		// replication.EventHeader.LogPos, itself a uint32 wire field, so
		// saved.binlogPos can in practice never exceed math.MaxUint32 through
		// this codebase's own capture path — the wraparound happens upstream,
		// at the source, before this value is ever read here. The guard that
		// actually catches the wrap is in parser.StreamParser.Run
		// (internal/parser/stream.go): it detects a same-file LogPos going
		// backward with no intervening RotateEvent and fails loud LIVE, during
		// streaming, before a corrupt checkpoint can ever be persisted. This
		// check is kept as cheap defensive insurance (a hand-edited
		// stream_state row, or a future writer that stops deriving from
		// LogPos) — it must not be relied on as the primary fix.
		if saved.binlogPos > math.MaxUint32 {
			return "", "", "", 0, nil, fmt.Errorf(
				"saved binlog position %d in %q exceeds 4GiB, the COM_BINLOG_DUMP protocol limit for position-mode resume; "+
					"switch to GTID mode, which has no such limit: pass --start-gtid with the source's current executed GTID set (%s)",
				saved.binlogPos, saved.binlogFile, parser.GTIDExecutedHint(flavor))
		}
		slog.Info("resuming from position", "file", saved.binlogFile, "pos", saved.binlogPos)
		return "position", saved.binlogFile, "", uint32(saved.binlogPos), nil, nil
	}

	// No checkpoint — use flags for initial start position (first run).
	if startFile != "" && startGTID != "" {
		return "", "", "", 0, nil, fmt.Errorf("--start-file and --start-gtid are mutually exclusive")
	}
	if startGTID != "" {
		startGTID = normalizeGTIDForFlavor(flavor, startGTID)
		gs, parseErr := parseGTIDSetForFlavor(flavor, startGTID)
		if parseErr != nil {
			return "", "", "", 0, nil, fmt.Errorf("invalid --start-gtid: %w", parseErr)
		}
		return "gtid", "", startGTID, 0, gs, nil
	}
	if startFile != "" {
		return "position", startFile, "", startPos, nil, nil
	}

	return "", "", "", 0, nil, fmt.Errorf(
		"no start position specified and no saved stream state found; " +
			"provide --start-file or --start-gtid to begin streaming")
}

// resolveStartWithAutoDiscoverForFlavor wraps resolveStartForFlavor with an
// auto-discover fallback for the first-run, no-flags case. When neither
// --start-file nor --start-gtid is set AND no checkpoint exists yet, the
// provided callbacks are invoked to query the source's current coordinates
// (matching the behavior of `bintrail agent` BYOS mode); they are typically
// config.CurrentGTIDExecuted / config.CurrentBinlogPosition over sourceDB, and
// exist as callbacks so the discovery side-effect can be unit-tested without a
// real *sql.DB.
//
// All other paths (saved checkpoint, explicit flags, mutually-exclusive flags,
// invalid GTID, etc.) delegate to resolveStartForFlavor unchanged. flavor is
// threaded into resolveStartForFlavor so saved/flag GTID sets parse with the
// right flavor.
//
// gtidAutoDiscover is tried BEFORE the position callback, and only for the
// MySQL flavor: a non-empty executed set selects GTID mode (parsed with the
// same flavor-aware parser as the --start-gtid branch), so a fresh stream on a
// gtid_mode=ON source checkpoints in GTID mode and live-source verify works
// out of the box (#1131). An empty set — gtid_mode not ON, or a fresh server
// with zero transactions — falls back to the position callback unchanged
// (starting GTID replication from an empty set would replay the binlog from
// the beginning, not "start from now"). A gtidAutoDiscover ERROR is fatal, not
// a fallback: an operator on a GTID source must not silently end up in
// position mode.
//
// MariaDB deliberately keeps position-only auto-discovery: its GTID
// coordinates (domain-server-seq, @@gtid_current_pos) have different discovery
// semantics, and the live-source verify coverage check that motivates GTID
// mode only consumes MySQL GTID sets — auto-selecting GTID mode there would be
// an untested behavior change with no consumer.
func resolveStartWithAutoDiscoverForFlavor(
	startFile, startGTID string, startPos uint32,
	saved *streamState, flavor string,
	autoDiscover func() (string, uint32, error),
	gtidAutoDiscover func() (string, error),
) (mode, file, gtidStr string, pos uint32, accGTID gomysql.GTIDSet, err error) {
	mode, file, gtidStr, pos, accGTID, err = resolveStartForFlavor(startFile, startGTID, startPos, saved, flavor)
	if err == nil {
		return
	}
	// Only fall back to auto-discover when the failure is the specific
	// "no start position" case (saved == nil and no flags). Other errors
	// (mutually-exclusive flags, invalid GTID, corrupt saved state) must
	// propagate so the operator sees the real problem.
	if saved != nil || startFile != "" || startGTID != "" {
		return
	}
	if gtidAutoDiscover != nil && flavor == gomysql.MySQLFlavor {
		set, dErr := gtidAutoDiscover()
		if dErr != nil {
			return "", "", "", 0, nil, fmt.Errorf("auto-discover executed GTID set: %w", dErr)
		}
		if set != "" {
			set = normalizeGTIDForFlavor(flavor, set)
			gs, parseErr := parseGTIDSetForFlavor(flavor, set)
			if parseErr != nil {
				return "", "", "", 0, nil, fmt.Errorf("auto-discovered gtid_executed %q is unparseable: %w", set, parseErr)
			}
			return "gtid", "", set, 0, gs, nil
		}
		// Empty set: fall through to position discovery below.
	}
	if autoDiscover == nil {
		return // the original "no start position" error
	}
	af, ap, dErr := autoDiscover()
	if dErr != nil {
		return "", "", "", 0, nil, fmt.Errorf("auto-discover binlog position: %w", dErr)
	}
	return "position", af, "", ap, nil, nil
}

// truncateForDisplay shortens s for a single terminal line. GTID sets are
// ASCII, so byte slicing cannot split a rune. Callers log the full value via
// slog before printing the truncated form.
func truncateForDisplay(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "… (truncated; full set in the log)"
}

// ─── Gap detection ──────────────────────────────────────────────────────────────

// gapResult describes a binlog gap between the saved checkpoint and the source.
type gapResult struct {
	HasGap   bool   // true if a gap exists between checkpoint and source
	Fillable bool   // true if the gap can be filled (binlogs still available)
	Message  string // human-readable description of the gap

	// For unfillable gaps in position mode: the earliest available binlog.
	EarliestFile string
	EarliestPos  uint32

	// For unfillable gaps in GTID mode: the purged GTID set to use as base.
	PurgedGTIDSet string

	// RebuildUndetectable is set by position-mode gap detection when it resumes
	// reading from an existing binlog at a valid offset (no gap, or a fillable
	// gap). In that case a source rebuild (RESET MASTER + restore) that regrew a
	// same-named binlog past the checkpoint offset is INVISIBLE here — the file
	// exists and checkpointPos <= size, so the check passes and the stream reads
	// a potentially DIFFERENT history. GTID mode detects this divergence (the
	// executed-set comparison in detectGTIDGap); position mode has no identity
	// signal at gap-detection time. Callers surface this as an escalated warning.
	// Never set by the GTID detectors — GTID resumes stay false.
	RebuildUndetectable bool
}

// detectPositionGap queries the source MySQL for available binary logs and
// checks whether the checkpoint file still exists. Returns a gapResult.
// timeout caps how long SHOW BINARY LOGS may take — RDS instances with many
// binlog files can take >10s, so callers should pass a generous value.
func detectPositionGap(sourceDB *sql.DB, checkpointFile string, checkpointPos uint32, timeout time.Duration) (*gapResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	rows, err := sourceDB.QueryContext(ctx, "SHOW BINARY LOGS")
	if err != nil {
		return nil, fmt.Errorf("SHOW BINARY LOGS: %w", err)
	}
	defer rows.Close()

	// SHOW BINARY LOGS returns varying columns across MySQL versions;
	// we only need the first two (Log_name, File_size).
	cols, colErr := rows.Columns()
	if colErr != nil {
		return nil, fmt.Errorf("SHOW BINARY LOGS columns: %w", colErr)
	}
	if len(cols) < 2 {
		return nil, fmt.Errorf("SHOW BINARY LOGS returned %d columns, expected at least 2", len(cols))
	}

	type binlogEntry struct {
		name string
		size int64
	}
	var logs []binlogEntry
	for rows.Next() {
		var name string
		var size int64
		vals := make([]any, len(cols))
		vals[0] = &name
		vals[1] = &size
		for i := 2; i < len(cols); i++ {
			vals[i] = new(sql.RawBytes)
		}
		if err := rows.Scan(vals...); err != nil {
			return nil, fmt.Errorf("scan SHOW BINARY LOGS: %w", err)
		}
		logs = append(logs, binlogEntry{name, size})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate SHOW BINARY LOGS: %w", err)
	}
	if len(logs) == 0 {
		return nil, fmt.Errorf("SHOW BINARY LOGS returned no results")
	}

	// Check if the checkpoint file exists in the list.
	for _, l := range logs {
		if l.name == checkpointFile {
			// File exists but checkpoint position exceeds file size — the file
			// may have been regenerated (e.g., RESET MASTER). Treat as unfillable.
			if int64(checkpointPos) > l.size {
				return &gapResult{
					HasGap:       true,
					Fillable:     false,
					EarliestFile: logs[0].name,
					EarliestPos:  4,
					Message: fmt.Sprintf(
						"binlog gap: file %s exists but checkpoint position %d exceeds file size %d; "+
							"file may have been regenerated after RESET MASTER",
						checkpointFile, checkpointPos, l.size),
				}, nil
			}
			// File still exists and position is valid — gap is fillable.
			// Either way we resume reading the existing file from checkpointPos,
			// so a same-named regrown binlog after a source rebuild is undetectable
			// here (see gapResult.RebuildUndetectable).
			currentFile := logs[len(logs)-1].name
			if checkpointFile == currentFile {
				return &gapResult{HasGap: false, RebuildUndetectable: true}, nil
			}
			return &gapResult{
				HasGap:              true,
				Fillable:            true,
				RebuildUndetectable: true,
				Message: fmt.Sprintf(
					"gap detected: checkpoint is at %s:%d, source is at %s; replaying missed events",
					checkpointFile, checkpointPos, currentFile),
			}, nil
		}
	}

	// File not found — binlogs have been purged.
	earliest := logs[0]
	return &gapResult{
		HasGap:       true,
		Fillable:     false,
		EarliestFile: earliest.name,
		EarliestPos:  4, // first 4 bytes are the magic number header, not event data
		Message: fmt.Sprintf(
			"binlog gap detected but CANNOT be filled: required file %s has been purged; "+
				"earliest available binlog is %s; events between these positions are permanently lost",
			checkpointFile, earliest.name),
	}, nil
}

// gtidSetsEqual parses two GTID set strings and compares them structurally,
// avoiding false mismatches from formatting differences (UUID case, ordering).
// Returns false (not an error) if either string cannot be parsed as a valid
// GTID set — the caller should treat this as "not equal" and proceed with
// gap detection.
func gtidSetsEqual(a, b string) bool {
	if a == "" && b == "" {
		return true
	}
	ga, err := gomysql.ParseMysqlGTIDSet(NormalizeGTIDSet(a))
	if err != nil {
		slog.Debug("gtidSetsEqual: failed to parse first GTID set", "gtid_set", a, "error", err)
		return false
	}
	gb, err := gomysql.ParseMysqlGTIDSet(NormalizeGTIDSet(b))
	if err != nil {
		slog.Debug("gtidSetsEqual: failed to parse second GTID set", "gtid_set", b, "error", err)
		return false
	}
	return ga.Equal(gb)
}

// detectGTIDGap queries the source MySQL for @@gtid_purged and @@gtid_executed,
// then checks whether the checkpoint GTID set requires any purged GTIDs.
// timeout caps how long the GTID system-variable queries may take.
func detectGTIDGap(sourceDB *sql.DB, checkpointGTID string, timeout time.Duration) (*gapResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	checkpointGTID = NormalizeGTIDSet(strings.TrimSpace(checkpointGTID))

	var purgedStr, executedStr string
	if err := sourceDB.QueryRowContext(ctx, "SELECT @@gtid_purged").Scan(&purgedStr); err != nil {
		return nil, fmt.Errorf("query @@gtid_purged: %w", err)
	}
	if err := sourceDB.QueryRowContext(ctx, "SELECT @@gtid_executed").Scan(&executedStr); err != nil {
		return nil, fmt.Errorf("query @@gtid_executed: %w", err)
	}

	purgedStr = NormalizeGTIDSet(strings.TrimSpace(purgedStr))
	executedStr = NormalizeGTIDSet(strings.TrimSpace(executedStr))

	// If purged is empty, all GTIDs are still available.
	if purgedStr == "" {
		if gtidSetsEqual(checkpointGTID, executedStr) {
			return &gapResult{HasGap: false}, nil
		}
		return &gapResult{
			HasGap:   true,
			Fillable: true,
			Message:  "gap detected: checkpoint GTID set is behind source @@gtid_executed; replaying missed events",
		}, nil
	}

	if checkpointGTID == "" {
		return nil, fmt.Errorf("checkpoint GTID set is empty; cannot perform gap detection")
	}

	// Parse both GTID sets.
	checkpoint, err := gomysql.ParseMysqlGTIDSet(checkpointGTID)
	if err != nil {
		return nil, fmt.Errorf("parse checkpoint GTID set: %w", err)
	}
	purged, err := gomysql.ParseMysqlGTIDSet(purgedStr)
	if err != nil {
		return nil, fmt.Errorf("parse @@gtid_purged: %w", err)
	}

	cpSet, ok := checkpoint.(*gomysql.MysqlGTIDSet)
	if !ok {
		return nil, fmt.Errorf("unexpected GTID set type %T from checkpoint parse", checkpoint)
	}
	purgedSet, ok := purged.(*gomysql.MysqlGTIDSet)
	if !ok {
		return nil, fmt.Errorf("unexpected GTID set type %T from purged parse", purged)
	}

	// Check if the stream would need any purged GTIDs on resume. MySQL sends
	// all GTIDs NOT in the checkpoint set, so we must check two directions:
	//
	// 1. Forward: for each UUID in the checkpoint, does the purged set contain
	//    any transaction IDs that the checkpoint doesn't have? If so, MySQL
	//    would need to send those GTIDs but they've been purged.
	// 2. Reverse: are there UUIDs in the purged set that the checkpoint has
	//    never seen? MySQL would try to send all of that UUID's GTIDs, but
	//    some have been purged.
	needsPurged := false

	// Direction 1: for each UUID in the purged set, check if the checkpoint
	// fully covers all purged intervals. If the checkpoint doesn't contain all
	// purged GTIDs for a UUID, MySQL would need to send them but they're gone.
	// This correctly handles non-contiguous intervals (e.g., checkpoint has
	// uuid:1-100,200-300 but purged has uuid:1-150 — GTIDs 101-150 are lost).
	//
	// Direction 2 (inline): if a purged UUID is absent from the checkpoint
	// entirely, MySQL would try to send all of that UUID's GTIDs.
	//
	// go-mysql v1.15.0+ shapes MysqlGTIDSet as map[uuid]map[tag]intervals to
	// carry MySQL 8.4 tagged GTIDs; untagged GTIDs live under the zero Tag, so
	// on an untagged source the inner loop runs exactly once per UUID and
	// reduces to the old per-UUID interval check. A tag present in the purged
	// set but absent from the checkpoint is the tagged analog of Direction 2.
	for uuid, purgedTags := range *purgedSet {
		cpTags, exists := (*cpSet)[uuid]
		if !exists {
			// Purged UUID the checkpoint has never seen.
			needsPurged = true
			break
		}
		for tag, purgedIntervals := range purgedTags {
			if len(purgedIntervals) == 0 {
				continue
			}
			cpIntervals, ok := cpTags[tag]
			if !ok || len(cpIntervals) == 0 {
				needsPurged = true
				break
			}
			// Check if the checkpoint's intervals fully contain the purged
			// intervals. IntervalSlice.Contain(sub) returns true if sub is a
			// subset of s.
			if !cpIntervals.Contain(purgedIntervals) {
				needsPurged = true
				break
			}
		}
		if needsPurged {
			break
		}
	}

	if needsPurged {
		return &gapResult{
			HasGap:        true,
			Fillable:      false,
			PurgedGTIDSet: purgedStr,
			Message: fmt.Sprintf(
				"GTID gap detected but CANNOT be filled: required GTIDs have been purged from the source; "+
					"purged set: %s; events in the purged range are permanently lost",
				purgedStr),
		}, nil
	}

	// Checkpoint is ahead of (or equal to) the purged set — gap is fillable.
	if gtidSetsEqual(checkpointGTID, executedStr) {
		return &gapResult{HasGap: false}, nil
	}
	return &gapResult{
		HasGap:   true,
		Fillable: true,
		Message:  "gap detected: checkpoint GTID set is behind source @@gtid_executed; replaying missed events",
	}, nil
}

// detectMariaDBGTIDGap is the MariaDB analog of detectGTIDGap. MariaDB has no
// @@gtid_purged, so the purge floor is derived from BINLOG_GTID_POS(<earliest
// surviving binlog>, 4) — the GTID state recorded at the start of the oldest
// binlog the source still has. Every GTID strictly before that floor is gone.
// The floor is empty when nothing has been purged (the very first binlog, whose
// starting state is "nothing executed", still exists).
//
// The three one-shot queries — SHOW BINARY LOGS, BINLOG_GTID_POS, and
// @@gtid_binlog_pos (the executed set, MariaDB's @@gtid_executed analog) — are
// bounded by timeout.
func detectMariaDBGTIDGap(sourceDB *sql.DB, checkpointGTID string, timeout time.Duration) (*gapResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	checkpointGTID = strings.TrimSpace(checkpointGTID)

	// 1. The oldest surviving binlog (first row of SHOW BINARY LOGS).
	earliestFile, err := earliestBinlogFile(ctx, sourceDB)
	if err != nil {
		return nil, err
	}

	// 2. The purge floor: the GTID state at the start of that file. BINLOG_GTID_POS
	//    returns SQL NULL (not an empty string) when the oldest binlog is the
	//    first-ever one — i.e. nothing has been purged — so scan through a
	//    NullString and treat NULL as "no floor". A plain string Scan would error
	//    on NULL, which is the common no-purge case.
	var floorNS sql.NullString
	if err := sourceDB.QueryRowContext(ctx, "SELECT BINLOG_GTID_POS(?, 4)", earliestFile).Scan(&floorNS); err != nil {
		return nil, fmt.Errorf("query BINLOG_GTID_POS(%q, 4): %w", earliestFile, err)
	}
	floorStr := strings.TrimSpace(floorNS.String)

	// 3. The executed set.
	var executedStr string
	if err := sourceDB.QueryRowContext(ctx, "SELECT @@gtid_binlog_pos").Scan(&executedStr); err != nil {
		return nil, fmt.Errorf("query @@gtid_binlog_pos: %w", err)
	}
	executedStr = strings.TrimSpace(executedStr)

	// Nothing purged: every GTID is still available, so the only question is
	// whether the checkpoint is caught up or merely behind.
	if floorStr == "" {
		if mariadbGTIDSetsEqual(checkpointGTID, executedStr) {
			return &gapResult{HasGap: false}, nil
		}
		return &gapResult{
			HasGap:   true,
			Fillable: true,
			Message:  "gap detected: checkpoint MariaDB GTID set is behind source @@gtid_binlog_pos; replaying missed events",
		}, nil
	}

	if checkpointGTID == "" {
		return nil, fmt.Errorf("checkpoint GTID set is empty; cannot perform gap detection")
	}

	checkpoint, err := parseMariadbSet(checkpointGTID)
	if err != nil {
		return nil, fmt.Errorf("parse checkpoint GTID set: %w", err)
	}
	floor, err := parseMariadbSet(floorStr)
	if err != nil {
		return nil, fmt.Errorf("parse purge floor (BINLOG_GTID_POS): %w", err)
	}

	// The gap is unfillable iff the checkpoint has NOT seen everything up to the
	// purge floor: some purged GTID the checkpoint never indexed is permanently
	// gone. PurgedGTIDSet carries the floor so the caller can auto-advance to it.
	if !mariadbCheckpointCoversFloor(checkpoint, floor) {
		return &gapResult{
			HasGap:        true,
			Fillable:      false,
			PurgedGTIDSet: floorStr,
			Message: fmt.Sprintf(
				"MariaDB GTID gap detected but CANNOT be filled: required GTIDs have been purged from the source "+
					"(purge floor %s is beyond checkpoint %s); events in the purged range are permanently lost",
				floorStr, checkpointGTID),
		}, nil
	}

	// Checkpoint covers the floor — any gap is fillable.
	if mariadbGTIDSetsEqual(checkpointGTID, executedStr) {
		return &gapResult{HasGap: false}, nil
	}
	return &gapResult{
		HasGap:   true,
		Fillable: true,
		Message:  "gap detected: checkpoint MariaDB GTID set is behind source @@gtid_binlog_pos; replaying missed events",
	}, nil
}

// earliestBinlogFile returns the name of the oldest binlog the source still
// retains — the first row of SHOW BINARY LOGS (both MySQL and MariaDB order it
// oldest-first). The column-tolerant scan mirrors detectPositionGap: only the
// first column (Log_name) is needed, and extra columns vary across versions.
func earliestBinlogFile(ctx context.Context, sourceDB *sql.DB) (string, error) {
	rows, err := sourceDB.QueryContext(ctx, "SHOW BINARY LOGS")
	if err != nil {
		return "", fmt.Errorf("SHOW BINARY LOGS: %w", err)
	}
	defer rows.Close()

	cols, colErr := rows.Columns()
	if colErr != nil {
		return "", fmt.Errorf("SHOW BINARY LOGS columns: %w", colErr)
	}
	if len(cols) < 1 {
		return "", fmt.Errorf("SHOW BINARY LOGS returned no columns")
	}

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return "", fmt.Errorf("iterate SHOW BINARY LOGS: %w", err)
		}
		return "", fmt.Errorf("SHOW BINARY LOGS returned no results")
	}

	var name string
	vals := make([]any, len(cols))
	vals[0] = &name
	for i := 1; i < len(cols); i++ {
		vals[i] = new(sql.RawBytes)
	}
	if err := rows.Scan(vals...); err != nil {
		return "", fmt.Errorf("scan SHOW BINARY LOGS: %w", err)
	}
	return name, nil
}

// parseMariadbSet parses a MariaDB GTID set string into the concrete
// *MariadbGTIDSet — the gap logic needs the per-domain map, not the interface.
func parseMariadbSet(s string) (*gomysql.MariadbGTIDSet, error) {
	set, err := gomysql.ParseMariadbGTIDSet(s)
	if err != nil {
		return nil, err
	}
	ms, ok := set.(*gomysql.MariadbGTIDSet)
	if !ok {
		return nil, fmt.Errorf("unexpected GTID set type %T from MariaDB parse", set)
	}
	return ms, nil
}

// mariadbGTIDSetsEqual parses two MariaDB GTID set strings and compares them
// structurally, so ordering differences never cause a false mismatch. It returns
// false (not an error) on a parse failure — the caller treats that as "not equal"
// and proceeds with gap detection. The MariaDB sibling of gtidSetsEqual.
//
// Like the MySQL gtidSetsEqual, this uses go-mysql's set Equal, which keys by
// (domain, server). Its only failure mode here is reporting "behind, replaying"
// when the stream is actually caught up — harmless: the syncer resumes from the
// checkpoint, finds nothing past it, and tails live. The unfillable decision (the
// path that records permanent data loss) uses mariadbCheckpointCoversFloor, which
// is server-agnostic.
func mariadbGTIDSetsEqual(a, b string) bool {
	if a == "" && b == "" {
		return true
	}
	ga, err := gomysql.ParseMariadbGTIDSet(a)
	if err != nil {
		slog.Debug("mariadbGTIDSetsEqual: failed to parse first GTID set", "gtid_set", a, "error", err)
		return false
	}
	gb, err := gomysql.ParseMariadbGTIDSet(b)
	if err != nil {
		slog.Debug("mariadbGTIDSetsEqual: failed to parse second GTID set", "gtid_set", b, "error", err)
		return false
	}
	return ga.Equal(gb)
}

// mariadbCheckpointCoversFloor reports whether the checkpoint has already seen
// every GTID at or below the purge floor — i.e. resuming from the checkpoint will
// not require any GTID the source has purged.
//
// It compares the sequence number PER DOMAIN and deliberately ignores the
// server_id, because a MariaDB GTID sequence is domain-GLOBAL: within one
// replication domain the sequence increments monotonically regardless of which
// server writes the event. So the next GTID a checkpoint at domainSeq needs is
// domainSeq+1, which is still available iff checkpointSeq >= floorSeq. This is
// the "per-domain seq >= other.seq" semantics gate #1 was designed around.
//
// go-mysql v1.15.0+ holds ONE GTID per domain in MariadbGTIDSet, matching
// MariaDB's own position semantics (@@gtid_binlog_pos and BINLOG_GTID_POS
// carry exactly one GTID per domain), so the domain's frontier is the entry
// itself — the old max-across-servers aggregation has no input to aggregate
// anymore. Upstream's set-level Contain now performs this same
// ignore-the-server_id comparison (pre-v1.15.0 it re-keyed by (domain,
// server) and false-alarmed across a failover); the explicit loop is kept
// because this check is a data-loss gate — the comparison it performs should
// be readable here, not inherited from a dependency's semantics.
//
// Legacy-checkpoint corner: a checkpoint accumulated by go-mysql <= v1.14.0
// after a mid-capture primary failover could serialize TWO server_ids for one
// domain (its set was keyed by (domain, server), serialized in map order).
// v1.15.0+ parses such a string by collapsing the domain to the LAST entry,
// warning "out of order binlog" when the higher sequence came first. The
// mid-capture failover topology remains explicitly untested either way
// (docs/mariadb.md); a fresh checkpoint clears the legacy shape.
func mariadbCheckpointCoversFloor(checkpoint, floor *gomysql.MariadbGTIDSet) bool {
	for domain, floorGTID := range floor.Sets {
		cpGTID, ok := checkpoint.Sets[domain]
		if !ok {
			// The source purged GTIDs in a domain the checkpoint never indexed —
			// those events are unreachable.
			return false
		}
		if cpGTID.SequenceNumber < floorGTID.SequenceNumber {
			return false
		}
	}
	return true
}

// ─── Stream loop ────────────────────────────────────────────────────────────────

// streamLoop consumes parser events, flushes batches to MySQL, and writes
// checkpoints to stream_state at the given interval.
//
// DDL events flush the current batch and are never inserted. The DDL
// HANDLING (auto-snapshot + resolver swap + schema_changes record) is NOT
// done here on purpose: this loop runs behind the parser through a buffered
// channel, so by the time a DDL reaches it the parser has already decoded —
// and, for a new table, skipped — the rows that followed the DDL in the
// binlog. That work lives in the parser's synchronous DDL hook
// (StreamParser.SetSyncDDLHook, #396).
func streamLoop(
	ctx context.Context,
	events <-chan parser.Event,
	idx *indexer.Indexer,
	db *sql.DB,
	checkpointInterval time.Duration,
	state *streamState,
	m *observe.StreamMetrics,
	hooks *Hooks,
) error {
	batch := make([]parser.Event, 0, idx.BatchSize())
	ticker := time.NewTicker(checkpointInterval)
	defer ticker.Stop()

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		m.BatchSize.Observe(float64(len(batch)))
		n, err := idx.InsertBatch(batch)
		state.eventsIndexed += n
		m.EventsIndexed.Add(float64(n))
		m.BatchFlushes.Inc()
		if err == nil {
			// T2 (#1225): the batch is durable, so its events are queryable and
			// recoverable NOW. Only on success — a failed InsertBatch may have
			// written some rows, but nothing about that partial state is an
			// availability guarantee worth publishing. Read the batch before the
			// truncation below.
			observeCommitLag(m, batch, time.Now())
		}
		batch = batch[:0]
		if err != nil {
			m.Errors.WithLabelValues("batch_flush").Inc()
			return err
		}
		if n > 0 && hooks != nil && hooks.OnIndexed != nil {
			hooks.OnIndexed(n)
		}
		return nil
	}

	// checkpoint flushes the pending batch, then persists the stream position.
	// A flush failure is RETURNED to the caller so the stream aborts loudly and
	// replays from the last durable checkpoint on restart — the same fail-loud
	// contract the batch-full (len>=BatchSize) and DDL flushes already follow.
	// Swallowing it here let an un-indexable event (e.g. one over the server's
	// max_allowed_packet) be silently skipped (#652). A saveCheckpoint failure
	// is NOT data loss — it only re-streams from an older checkpoint on restart —
	// so it stays a warning.
	checkpoint := func() error {
		if err := flush(); err != nil {
			return err
		}
		if err := saveCheckpoint(db, state); err != nil {
			slog.Warn("saveCheckpoint failed", "error", err)
			m.Errors.WithLabelValues("checkpoint").Inc()
		} else {
			m.CheckpointSaves.Inc()
			if hooks != nil && hooks.OnCheckpoint != nil {
				hooks.OnCheckpoint()
			}
			slog.Info("checkpoint saved",
				"file", state.binlogFile,
				"pos", state.binlogPos,
				"events_indexed", state.eventsIndexed)
		}
		return nil
	}

	// advanceGTID adds a committed transaction's GTID to the durable set. It is
	// called only at commit boundaries (EventCommit / EventDDL), never at the
	// leading EventGTID, so a checkpoint can never persist a GTID whose rows were
	// not fully received and indexed (#491). No-op in position mode (accGTID nil)
	// and for non-GTID sources (empty gtid).
	advanceGTID := func(gtid string) {
		if gtid == "" || state.accGTID == nil {
			return
		}
		if err := state.accGTID.Update(gtid); err != nil {
			// This is the sole path to durable GTID progress; on a persistent
			// failure the checkpoint silently freezes while rows keep indexing and
			// checkpoints keep "succeeding" with the stale set. Log loudly (Error,
			// not Warn) — a frozen-but-green checkpoint means a full re-stream on
			// the next restart. Update only errors on a malformed GTID, which the
			// parser does not produce, so this should never fire in practice.
			slog.Error("failed to update GTID set — durable checkpoint is not advancing",
				"gtid", gtid, "error", err)
			m.Errors.WithLabelValues("gtid_update").Inc()
			return
		}
		state.gtidSet = state.accGTID.String()
	}

	for {
		select {
		case <-ctx.Done():
			if err := checkpoint(); err != nil {
				return err
			}
			return nil

		case <-ticker.C:
			if err := checkpoint(); err != nil {
				return err
			}

		case ev, ok := <-events:
			if !ok {
				if err := checkpoint(); err != nil {
					return err
				}
				return nil
			}
			// Update position tracking from each event.
			if ev.BinlogFile != "" {
				state.binlogFile = ev.BinlogFile
			}
			state.binlogPos = ev.EndPos
			// Advance the POSITION-mode safe checkpoint only at statement/commit/DDL
			// boundaries, never mid-statement — a resume from between the ROWS_EVENTs
			// of one split statement crashes StartSync (#775). The per-event
			// binlogPos above stays for metrics/display and GTID-mode checkpointing.
			if isPositionCheckpointBoundary(ev) {
				state.safeFile, state.safePos = state.binlogFile, ev.EndPos
			}
			if !ev.Timestamp.IsZero() {
				state.lastEventTime = sql.NullTime{Time: ev.Timestamp, Valid: true}
			}

			switch ev.EventType {
			case parser.EventGTID:
				// Transaction start marker — attribution only. The GTID is NOT
				// added to the durable set here; that happens at EventCommit, so a
				// checkpoint mid-transaction can't claim a half-streamed
				// transaction (#491).
				continue
			case parser.EventCommit:
				// Transaction committed: all its rows have been received, so it is
				// now safe to advance the durable GTID checkpoint.
				advanceGTID(ev.GTID)
				continue
			case parser.EventDDL:
				// DDL flushes pending rows, then auto-commits its own GTID
				// (insertion itself is handled by the parser's synchronous hook).
				if err := flush(); err != nil {
					return err
				}
				advanceGTID(ev.GTID)
				continue
			}

			m.EventsReceived.Inc()
			if !ev.Timestamp.IsZero() {
				ts := float64(ev.Timestamp.Unix())
				m.LastEventTimestamp.Set(ts)
				m.ReplicationLag.Set(float64(time.Now().Unix()) - ts)
			}
			slog.Debug("event received",
				"schema", ev.Schema,
				"table", ev.Table,
				"type", ev.EventType,
				"gtid", ev.GTID)

			batch = append(batch, ev)
			if len(batch) >= idx.BatchSize() {
				if err := flush(); err != nil {
					return err
				}
			}
		}
	}
}

// ─── Config / One ───────────────────────────────────────────────

// Config carries everything ONE replication stream needs — the by-value
// equivalent of the strm* package globals. It exists so the control plane can
// run N streams concurrently in one process (each with its own config), while
// `bintrail stream` keeps wiring its flags through a single instance
// unchanged. Plain data plus optional observer hooks and required Deps: the
// zero value is invalid; build it from the cobra layer's streamConfigFromFlags
// or populate it explicitly.
type Config struct {
	IndexDSN  string
	SourceDSN string
	// Flavor is the source database flavor: "mysql" (default) or "mariadb".
	// Empty is normalized to "mysql" in One(). It selects the GTID parser and
	// the BinlogSyncer flavor, and is persisted to stream_state for resume.
	Flavor      string
	ServerID    uint32
	StartFile   string
	StartPos    uint32
	StartGTID   string
	BatchSize   int
	Schemas     string
	Tables      string
	Checkpoint  int // seconds
	MetricsAddr string
	// MetricsSource is the value of the Prometheus "source" label for this
	// stream — the supervisor sets it to the registry entry ID so N
	// concurrent streams stay distinguishable on one /metrics endpoint.
	// Empty = fall back to the resolved bintrail_id (or "default").
	MetricsSource string
	// MetricsScrapeInterval is how often (seconds) the bintrail_index_* gauges
	// are refreshed from a status snapshot. 0 = the 60s default.
	MetricsScrapeInterval int
	// IndexMetrics forces the bintrail_index_* scraper on even when this stream
	// sets neither MetricsAddr nor MetricsSource — the `bintrail-console watch`
	// daemon's OWN primary stream needs this, since it serves /metrics centrally
	// (MetricsAddr empty) and isn't supervisor-launched (MetricsSource empty).
	IndexMetrics bool
	SSLMode      string
	SSLCA        string
	SSLCert      string
	SSLKey       string
	Format       string
	Reset        bool
	NoGapFill    bool
	GapTimeout   int // seconds
	// Hooks, when non-nil, lets a supervisor observe this stream's liveness
	// without polling. Plain `bintrail stream` leaves it nil.
	Hooks *Hooks
	// Deps are the host-supplied helper functions One needs (see Deps). All
	// fields are required.
	Deps Deps
}

// Hooks are liveness callbacks a supervisor attaches to one stream.
// All fields are optional. They are invoked synchronously from the stream
// loop — implementations must be fast and non-blocking.
type Hooks struct {
	// OnCheckpoint fires after every successful checkpoint save — the
	// "attached and healthy" signal (the ticker checkpoints even with zero
	// events, so an idle source still reports progress).
	OnCheckpoint func()
	// OnIndexed fires after every successful batch flush with rows written.
	OnIndexed func(n int64)
	// OnGapAutoAdvance fires when an unfillable binlog gap forced the stream
	// to advance past purged events — data in the gap is permanently lost.
	OnGapAutoAdvance func(detail string)
	// OnSourceConnected fires once per run, after the source connection is
	// open and its binlog settings pass validation, before the schema
	// snapshot: the console's first-run list shows the connection as its own
	// step (#1606).
	OnSourceConnected func()
}

// Deps are the host-supplied functions One needs that are NOT part of the
// streaming engine itself — source preflight/validation, server-identity
// resolution, schema snapshot/resolver/filters, DDL schema-change recording,
// source-DSN parsing, and JSON summary output. They live in the binary that
// builds the Config so streamrun stays a pure engine and never imports the
// cmd layer (or anything it would drag in). Every field must be set.
type Deps struct {
	ValidateBinlogFormat   func(db *sql.DB) error
	ValidateBinlogRowImage func(db *sql.DB) error
	ValidateNoFKCascades   func(db *sql.DB, schemas []string) error
	ParseSchemaList        func(s string) []string
	ResolveServerIdentity  func(ctx context.Context, sourceDB, indexDB *sql.DB, sourceDSN string) (string, error)
	EnsureResolver         func(indexDB, sourceDB *sql.DB, schemas []string) (*metadata.Resolver, error)
	BuildIndexFilters      func(schemas, tables string) parser.Filters
	InsertSchemaChange     func(db *sql.DB, ev parser.Event, snapshotID *int) error
	ParseSourceDSN         func(dsn string) (host string, port uint16, user, password string, err error)
	OutputJSON             func(v any) error
}

// validate fails fast when a required dependency is unset. One calls every Deps
// field with no nil guard, so without this a missing one would nil-panic deep in
// the stream (after two DB connections) with a message that names no field — and
// the compiler can't catch a newly-added field that wasn't wired into the host's
// streamDeps(). Named errors here turn that into an immediate, actionable failure.
func (d Deps) validate() error {
	switch {
	case d.ValidateBinlogFormat == nil:
		return errors.New("streamrun.Deps.ValidateBinlogFormat is nil")
	case d.ValidateBinlogRowImage == nil:
		return errors.New("streamrun.Deps.ValidateBinlogRowImage is nil")
	case d.ValidateNoFKCascades == nil:
		return errors.New("streamrun.Deps.ValidateNoFKCascades is nil")
	case d.ParseSchemaList == nil:
		return errors.New("streamrun.Deps.ParseSchemaList is nil")
	case d.ResolveServerIdentity == nil:
		return errors.New("streamrun.Deps.ResolveServerIdentity is nil")
	case d.EnsureResolver == nil:
		return errors.New("streamrun.Deps.EnsureResolver is nil")
	case d.BuildIndexFilters == nil:
		return errors.New("streamrun.Deps.BuildIndexFilters is nil")
	case d.InsertSchemaChange == nil:
		return errors.New("streamrun.Deps.InsertSchemaChange is nil")
	case d.ParseSourceDSN == nil:
		return errors.New("streamrun.Deps.ParseSourceDSN is nil")
	case d.OutputJSON == nil:
		return errors.New("streamrun.Deps.OutputJSON is nil")
	}
	return nil
}

// drainParser stops the stream's parser goroutine and returns its final error.
//
// While parked, the parser (sp.Run) returns only on context cancellation — it
// is blocked in GetEvent waiting for the next binlog event, or on a send to a
// full events buffer (every such send is a select with a <-ctx.Done() arm).
// One derives a cancellable context whose deferred cancel runs only once One
// returns, so when streamLoop returns an error mid-stream (the ticker /
// batch-full / DDL flush paths, with the caller's ctx still live) a bare
// `<-parseErrCh` would block forever and One would hang instead of surfacing the
// failure to its supervisor — turning a fail-loud abort into a silent wedge
// (#652). Cancelling BEFORE the receive guarantees the parser unblocks.
// Idempotent on the clean-exit paths (the context is already cancelled, or the
// parser already returned via close(events)). sp.Run converts cancellation to a
// nil return, so the resulting parseErr is nil and dropped by the caller's
// `parseErr != nil` guard; a genuine upstream parser failure (ctx still live)
// is non-nil and surfaced.
func drainParser(cancel context.CancelFunc, parseErrCh <-chan error) error {
	cancel()
	return <-parseErrCh
}

// One runs one complete replication stream — connect, validate,
// resolve identity, snapshot, gap-check, sync, index, checkpoint — until ctx
// is cancelled or a fatal error occurs. It is self-contained by design: no
// package globals, no signal handling, safe to run N instances concurrently
// (each against its own index database).

func One(ctx context.Context, cfg Config) error {
	if !cliutil.IsValidOutputFormat(cfg.Format) {
		return fmt.Errorf("invalid --format %q; must be text or json", cfg.Format)
	}
	// Reject non-positive --gap-timeout values: a zero or negative timeout
	// would produce an immediately-cancelled context inside detectPositionGap
	// / detectGTIDGap, surfacing as a misleading "context deadline exceeded"
	// error whose recovery hint (`--reset`) discards the saved checkpoint.
	if cfg.GapTimeout <= 0 {
		return fmt.Errorf("invalid --gap-timeout %d: must be a positive number of seconds", cfg.GapTimeout)
	}
	// A non-positive --write-timeout would make every index write's
	// context.WithTimeout fire immediately, turning each write into an instant
	// failure instead of bounding a genuine stall (#959).
	if indexer.WriteTimeout <= 0 {
		return fmt.Errorf("invalid --write-timeout %s: must be a positive duration", indexer.WriteTimeout)
	}
	// Fail fast on an unwired dependency (named field) before opening any
	// connection, rather than nil-panicking on first use further down.
	if err := cfg.Deps.validate(); err != nil {
		return err
	}

	// Normalize the source flavor once so every downstream use (GTID parsing,
	// BinlogSyncerConfig, persistence) sees a concrete value. Empty defaults to
	// MySQL, keeping every existing caller (which never sets Flavor) unchanged;
	// an unsupported flavor is rejected here, before any connection is opened.
	normalizedFlavor, err := normalizeFlavor(cfg.Flavor)
	if err != nil {
		return err
	}
	cfg.Flavor = normalizedFlavor

	// Derived cancel: internal failures (e.g. the stream loop erroring) must
	// stop the parser goroutine even when the caller's ctx stays live.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// ── 1. Connect to index database ─────────────────────────────────────────
	// #946: the index write connection carries full row images (PII) and the
	// index credentials, so --ssl-mode must encrypt it too — not only the binlog
	// syncer.
	indexDB, err := connectHelper(cfg.IndexDSN, "index database", cfg.SSLMode, cfg.SSLCA, cfg.SSLCert, cfg.SSLKey)
	if err != nil {
		return err
	}
	defer indexDB.Close()

	if err := indexer.EnsureSchema(indexDB); err != nil {
		return fmt.Errorf("schema migration: %w", err)
	}

	// ── 2. Connect to source database: validate binlog_row_image ─────────────
	// #946: same --ssl-mode TLS for the source helper connection (schema reads +
	// source credentials) as for the binlog syncer built below.
	sourceDB, err := connectHelper(cfg.SourceDSN, "source MySQL", cfg.SSLMode, cfg.SSLCA, cfg.SSLCert, cfg.SSLKey)
	if err != nil {
		return err
	}
	defer sourceDB.Close()

	if err := cfg.Deps.ValidateBinlogFormat(sourceDB); err != nil {
		return err
	}
	fmt.Println("Source: binlog_format=ROW \u2713")

	if err := cfg.Deps.ValidateBinlogRowImage(sourceDB); err != nil {
		return err
	}
	fmt.Println("Source: binlog_row_image=FULL \u2713")

	if err := cfg.Deps.ValidateNoFKCascades(sourceDB, cfg.Deps.ParseSchemaList(cfg.Schemas)); err != nil {
		if !errors.Is(err, metadata.ErrFKCascadesFound) {
			return err // genuine query/connection failure: abort as before
		}
		slog.Warn("FK cascade constraints present on source; streaming will proceed, "+
			"but InnoDB executes cascades below the binlog (MySQL Bug #32506) so cascaded "+
			"child-row deletes are NOT captured \u2014 plain `recover` cannot restore them. "+
			"Reconstruct them with `bintrail recover-cascade`.",
			"detail", err.Error())
	} else {
		fmt.Println("Source: no FK cascades \u2713")
	}

	// Advisory only: warn (never block) when the declared flavor disagrees with
	// the server's actual VERSION(). A mismatch \u2014 e.g. the default --source-flavor
	// mysql pointed at a MariaDB server \u2014 makes the GTID handshake misbehave.
	// Detection never flips the configured flavor, so it can't silently
	// mis-handshake a MySQL source.
	if detected := metadata.DetectFlavor(sourceDB); detected != "" && detected != cfg.Flavor {
		slog.Warn("source flavor mismatch: configured flavor differs from the detected server flavor \u2014 GTID handling may misbehave; set --source-flavor to match",
			"configured", cfg.Flavor, "detected", detected)
	}

	if cfg.Hooks != nil && cfg.Hooks.OnSourceConnected != nil {
		cfg.Hooks.OnSourceConnected()
	}

	// ── 3. Resolve server identity ────────────────────────────────────────────
	bintrailID, err := cfg.Deps.ResolveServerIdentity(ctx, sourceDB, indexDB, cfg.SourceDSN)
	if err != nil {
		if errors.Is(err, serverid.ErrConflict) {
			return fmt.Errorf("cannot stream: %w", err)
		}
		slog.Warn("server identity resolution failed; proceeding without bintrail_id", "error", err)
	} else {
		slog.Info("server identity resolved", "bintrail_id", bintrailID)
	}

	// ── 4. Schema snapshot + resolver ─────────────────────────────────────────
	resolver, err := cfg.Deps.EnsureResolver(indexDB, sourceDB, cfg.Deps.ParseSchemaList(cfg.Schemas))
	if err != nil {
		return err
	}
	fmt.Printf("Snapshot: id=%d, tables=%d\n", resolver.SnapshotID(), resolver.TableCount())

	// ── 5. Filters ────────────────────────────────────────────────────────────
	filters := cfg.Deps.BuildIndexFilters(cfg.Schemas, cfg.Tables)

	// ── 6. Determine start position ───────────────────────────────────────────
	saved, err := loadStreamState(indexDB)
	if err != nil {
		return fmt.Errorf("failed to load stream state: %w", err)
	}

	// --reset discards the saved checkpoint so start resolution runs as a
	// first run. The old state is kept aside instead of DELETEd here (#1079):
	// the discard is persisted only after the new start position is known, so
	// the jumped-over binlog range can be stamped as lost FIRST — and a
	// startup failure in between leaves the old checkpoint (and any prior
	// gap_lost record) intact.
	var resetDiscarded *streamState
	if cfg.Reset {
		if saved != nil {
			resetDiscarded = saved
			slog.Warn("discarding saved checkpoint (--reset)", "old_mode", saved.mode,
				"old_file", saved.binlogFile, "old_pos", saved.binlogPos)
			saved = nil
		} else {
			slog.Info("--reset specified but no saved checkpoint exists; ignoring")
		}
	}

	mode, startFile, startGTIDStr, startPos, accGTID, err := resolveStartWithAutoDiscoverForFlavor(
		cfg.StartFile, cfg.StartGTID, cfg.StartPos, saved, cfg.Flavor,
		func() (string, uint32, error) { return config.CurrentBinlogPosition(sourceDB) },
		func() (string, error) { return config.CurrentGTIDExecuted(sourceDB) })
	if err != nil {
		return err
	}
	// Surface the auto-discovered start point when this was a first-run,
	// no-flags invocation. Mirrors the agent BYOS startup checkmark style.
	if saved == nil && cfg.StartFile == "" && cfg.StartGTID == "" {
		switch mode {
		case "position":
			slog.Info("auto-discovered current binlog position", "file", startFile, "pos", startPos)
			fmt.Printf("Start position: auto-discovered %s:%d ✓\n", startFile, startPos)
		case "gtid":
			// The full set goes to the structured log; the terminal line is
			// truncated (a long-lived multi-source server's gtid_executed can
			// span many UUID blocks).
			slog.Info("auto-discovered executed GTID set (gtid_mode=ON)", "gtid_set", startGTIDStr)
			fmt.Printf("Start position: auto-discovered GTID set %s ✓\n", truncateForDisplay(startGTIDStr, 120))
		}
	}

	// Persist the --reset discard now that the new start position is known
	// (#1079) — see persistResetDiscard for the jump/no-op semantics. The
	// counters restart with the stream: the in-memory streaming state below
	// starts from zero when saved is nil, so eventsIndexed/lastEventTime are
	// deliberately NOT carried from the discarded checkpoint (the first
	// checkpoint tick would overwrite a carried value anyway).
	if resetDiscarded != nil {
		fresh := &streamState{
			mode:       mode,
			binlogFile: startFile,
			binlogPos:  uint64(startPos),
			safeFile:   startFile,
			safePos:    uint64(startPos),
			gtidSet:    startGTIDStr,
			flavor:     cfg.Flavor,
			serverID:   cfg.ServerID,
			bintrailID: bintrailID,
		}
		noop, detail := classifyResetDiscard(resetDiscarded, cfg.Flavor, mode, startFile, startPos, startGTIDStr,
			cfg.StartFile == "" && cfg.StartGTID == "",
			func() (string, uint32, error) { return config.CurrentBinlogPosition(sourceDB) })
		if err := persistResetDiscard(indexDB, fresh, noop, detail, cfg.Hooks); err != nil {
			return err
		}
	}

	// ── 6b. Detect binlog gap ────────────────────────────────────────────
	// Only check for gaps when resuming from a saved checkpoint (not on first run).
	// gtidAdvanced tracks whether the GTID auto-advance branch below ran — see
	// the dedup-on-resume comment after this block for why it matters.
	gtidAdvanced := false
	if saved != nil {
		var gap *gapResult
		var gapErr error

		gapTimeout := time.Duration(cfg.GapTimeout) * time.Second

		switch mode {
		case "position":
			gap, gapErr = detectPositionGap(sourceDB, startFile, startPos, gapTimeout)
		case "gtid":
			// MySQL and MariaDB expose the purge boundary differently
			// (@@gtid_purged vs BINLOG_GTID_POS over the oldest surviving binlog),
			// so each flavor has its own detector; both return the same gapResult
			// and feed the shared auto-advance / gap_lost_at machinery below.
			if cfg.Flavor == gomysql.MariaDBFlavor {
				gap, gapErr = detectMariaDBGTIDGap(sourceDB, startGTIDStr, gapTimeout)
			} else {
				gap, gapErr = detectGTIDGap(sourceDB, startGTIDStr, gapTimeout)
			}
		default:
			slog.Warn("gap detection not implemented for mode", "mode", mode)
		}

		if gapErr != nil {
			// Gap detection failure means we cannot verify whether binlogs have
			// been purged. Fail hard — proceeding could mask data loss.
			slog.Error("gap detection failed", "error", gapErr)
			return fmt.Errorf("gap detection failed: %w (use --reset to skip gap detection and start from a new position)", gapErr)
		} else if gap != nil && gap.HasGap {
			if gap.Fillable {
				slog.Info(gap.Message)
				fmt.Println("Gap: fillable — replaying missed events before live tailing")
			} else {
				// Unfillable gap — binlogs/GTIDs have been purged.
				slog.Warn(gap.Message)

				if cfg.NoGapFill {
					return &GapRefusedError{msg: gap.Message}
				}

				// Auto-advance past the gap.
				switch mode {
				case "position":
					slog.Warn("auto-advancing to earliest available binlog",
						"old_file", startFile, "old_pos", startPos,
						"new_file", gap.EarliestFile, "new_pos", gap.EarliestPos)
					fmt.Printf("Gap: UNFILLABLE — advancing from %s:%d to %s:%d (events in between are permanently lost)\n",
						startFile, startPos, gap.EarliestFile, gap.EarliestPos)
					startFile = gap.EarliestFile
					startPos = gap.EarliestPos

				case "gtid":
					slog.Warn("auto-advancing past purged GTIDs",
						"old_gtid_set", startGTIDStr,
						"purged_gtid_set", gap.PurgedGTIDSet)
					fmt.Printf("Gap: UNFILLABLE — checkpoint GTID set includes purged GTIDs; advancing past purged set (events are permanently lost)\n")
					// Adopt the purged set (MySQL: @@gtid_purged; MariaDB: the
					// BINLOG_GTID_POS purge floor) as the checkpoint — this tells the
					// source we have already seen everything up to the purge boundary,
					// so it sends only the surviving GTIDs that remain in the executed
					// set. Flavor-aware: for a MySQL source normalizeGTIDForFlavor /
					// parseGTIDSetForFlavor reduce to NormalizeGTIDSet /
					// ParseMysqlGTIDSet (byte-identical to the original), and a MariaDB
					// source parses domain-server-seq instead.
					startGTIDStr = normalizeGTIDForFlavor(cfg.Flavor, gap.PurgedGTIDSet)
					gs, parseErr := parseGTIDSetForFlavor(cfg.Flavor, startGTIDStr)
					if parseErr != nil {
						return fmt.Errorf("failed to parse purged GTID set for auto-advance: %w", parseErr)
					}
					accGTID = gs
					gtidAdvanced = true
				}

				// Durably record the loss and advance the checkpoint, in that order
				// (see persistGapAutoAdvance — the stamp must precede the advance so
				// the loss record can never desync from an advanced checkpoint, #402).
				advancedState := &streamState{
					mode:          mode,
					binlogFile:    startFile,
					binlogPos:     uint64(startPos),
					safeFile:      startFile,
					safePos:       uint64(startPos),
					gtidSet:       startGTIDStr,
					flavor:        cfg.Flavor,
					serverID:      cfg.ServerID,
					bintrailID:    bintrailID,
					eventsIndexed: saved.eventsIndexed,
					lastEventTime: saved.lastEventTime,
				}
				if err := persistGapAutoAdvance(indexDB, advancedState, gap.Message); err != nil {
					return err
				}
				slog.Info("saved advanced checkpoint after gap auto-advance",
					"file", startFile, "pos", startPos, "gtid_set", startGTIDStr)
				// Tell the supervisor events were lost — without this the
				// auto-advance is only a log line and the console shows a
				// healthy RUNNING badge over a stream that silently skipped
				// data (#402).
				if cfg.Hooks != nil && cfg.Hooks.OnGapAutoAdvance != nil {
					cfg.Hooks.OnGapAutoAdvance(gap.Message)
				}
			}
		}

		// Position-mode resume cannot detect a source rebuild. When the checkpoint
		// file still exists and checkpointPos <= size (no gap, or a fillable gap),
		// detectPositionGap resumes reading from checkpointPos — but if the source
		// was rebuilt (RESET MASTER + restore) and a same-named binlog regrew past
		// that offset, we would silently index a DIFFERENT binlog history. GTID
		// mode detects this (the executed-set comparison in detectGTIDGap
		// diverges); position mode has no identity signal at gap-detection time.
		// Escalate loudly rather than resume silently. Non-blocking: a hard error
		// here would break every legitimate position-mode resume.
		if gap != nil && gap.RebuildUndetectable {
			slog.Warn("position-mode resume cannot detect a source rebuild: if the source was "+
				"rebuilt (RESET MASTER + restore) and a same-named binlog regrew past the checkpoint "+
				"offset, streaming will silently index a divergent binlog history. GTID mode detects "+
				"this; position mode does not. To protect against rebuilds, run in GTID mode "+
				"(start with --start-gtid); if this source was already rebuilt, pass --reset to "+
				"discard the stale checkpoint before restarting.",
				"checkpoint_file", startFile, "checkpoint_pos", startPos)
			fmt.Printf("Warning: position-mode resume cannot verify the source was not rebuilt "+
				"(RESET MASTER + restore) at %s:%d; if it was, streaming will index a divergent "+
				"history. GTID mode (--start-gtid) detects this; pass --reset if the source was rebuilt.\n",
				startFile, startPos)
		}
	}

	// Dedup-on-resume (#759): batches flush to binlog_events independently of
	// the checkpoint ticker, so a crash between a flush and the next
	// checkpoint leaves already-indexed events beyond the durable checkpoint.
	// Streaming resumes from that stale (behind) position and MySQL re-sends
	// — and this process would re-insert — those same events under new
	// event_ids. Since this is the only writer for this index, it is safe to
	// delete anything at or beyond the actual replay start before streaming:
	// it will be re-received and re-inserted cleanly.
	//
	// This MUST run after 6b (not right on load): position mode's replay
	// start (startFile/startPos) only equals the saved checkpoint when no
	// gap auto-advance happened; on an unfillable-gap advance it moves
	// forward past purged binlogs, and events already captured in that
	// purged range must be kept, not deleted, because the source can never
	// re-send them. Keying the delete on the post-6b value handles both
	// cases with one comparison.
	//
	// GTID mode has no equivalent post-advance binlog coordinate (the purge
	// floor is a GTID set, not a (file,pos) tuple), so an advance there skips
	// the delete rather than risk deleting captured pre-floor rows; the
	// trade-off is a rare residual duplicate instead of destroyed data.
	//
	// That skip is a KNOWN AND ACCEPTED duplicate window, not an oversight
	// (#500): re-received events in the advanced-over range may be indexed
	// twice. Closing it would mean deleting on the stale checkpoint's
	// coordinates, which sit BELOW the purge floor — destroying rows the source
	// has already purged and can never re-send. A duplicate is visible and
	// repairable; a deleted pre-floor row is the only surviving copy. The
	// window is documented for operators in docs/streaming.md ("Known and
	// accepted: duplicate rows after a GTID-mode auto-advance") and announced
	// at runtime by the slog.Warn below — it is never silent.
	//
	// Guarded to same-mode resumes only (saved.mode == mode): an explicit
	// --start-file/--start-gtid mode switch is a deliberate one-off operator
	// action, not the crash-replay case this fix targets, and the saved
	// binlog coordinates don't correspond to the newly chosen start point.
	if saved != nil && saved.mode == mode && mode == "position" {
		if n, err := deleteEventsSinceCheckpoint(indexDB, startFile, uint64(startPos)); err != nil {
			return fmt.Errorf("failed to dedup events since checkpoint: %w", err)
		} else if n > 0 {
			slog.Warn("deleted events at or beyond the replay start to avoid re-insert duplicates on resume",
				"file", startFile, "pos", startPos, "rows_deleted", n)
		}
	} else if saved != nil && saved.mode == mode && mode == "gtid" {
		if gtidAdvanced {
			slog.Warn("skipping dedup-on-resume after GTID gap auto-advance; " +
				"re-received events in this window may be duplicated — this is a known, " +
				"accepted trade-off (deleting on the pre-advance coordinates would destroy " +
				"already-captured rows below the purge floor); see docs/streaming.md")
		} else if n, err := deleteEventsSinceCheckpointGTID(indexDB, saved.binlogFile, saved.binlogPos, accGTID, cfg.Flavor); err != nil {
			return fmt.Errorf("failed to dedup events since checkpoint: %w", err)
		} else if n > 0 {
			slog.Warn("deleted events at or beyond the saved checkpoint (incl. any open-transaction stragglers) to avoid re-insert duplicates on resume",
				"file", saved.binlogFile, "pos", saved.binlogPos, "rows_deleted", n)
		}
	}

	state := &streamState{
		mode: mode,
		// Seed the position with the resolved start so the first ticker
		// checkpoint (which fires even before any event arrives) persists a
		// valid resume point instead of an empty file / position 0. safeFile/
		// safePos seed identically — the resolved start is always a boundary, and
		// this keeps the position-mode checkpoint at a valid resume point until the
		// first statement/commit/DDL boundary advances it (#775).
		binlogFile: startFile,
		binlogPos:  uint64(startPos),
		safeFile:   startFile,
		safePos:    uint64(startPos),
		flavor:     cfg.Flavor,
		serverID:   cfg.ServerID,
		accGTID:    accGTID,
		bintrailID: bintrailID,
	}
	if saved != nil {
		state.eventsIndexed = saved.eventsIndexed
	}
	if startGTIDStr != "" {
		state.gtidSet = startGTIDStr
	}

	// Capture-skip counters (#1034): shared between the StreamParser (which
	// records each discarded event) and saveCheckpoint (which persists them).
	// Re-seeded from the previous run's persisted document so a restart never
	// silently zeroes the DEGRADED verdict `status` renders from them. Since
	// #999 the ledger is exit-code-bearing (--fail-on-gap), so an unreadable
	// document must not be laundered into fresh counters + a clean "{}" at the
	// next checkpoint: SeedPreserving records the failure itself under the
	// unreadable_previous_ledger meta-reason (#1206) — status stays non-clean
	// until the operator acknowledges. Never a startup abort either way.
	skips := parser.NewSkipCounters(nil)
	if raw, err := loadCaptureSkips(indexDB); err != nil {
		slog.Warn("could not load persisted capture-skip counters; starting from zero", "error", err)
	} else if err := skips.SeedPreserving(raw); err != nil {
		slog.Error("could not parse persisted capture-skip counters — the previous ledger may have recorded permanent loss; preserving the fact under reason unreadable_previous_ledger", "error", err)
	}
	state.skips = skips

	// ── 7. Parse source DSN for BinlogSyncer ─────────────────────────────
	host, port, user, password, err := cfg.Deps.ParseSourceDSN(cfg.SourceDSN)
	if err != nil {
		return err
	}

	// ── 6b. Build TLS config ──────────────────────────────────────────────────
	tlsCfg, err := buildTLSConfig(cfg.SSLMode, cfg.SSLCA, cfg.SSLCert, cfg.SSLKey, host)
	if err != nil {
		return err
	}

	// ── 7. Create BinlogSyncer ────────────────────────────────────────────────────
	syncerCfg := replication.BinlogSyncerConfig{
		ServerID:                cfg.ServerID,
		Flavor:                  cfg.Flavor,
		Host:                    host,
		Port:                    port,
		User:                    user,
		Password:                password,
		HeartbeatPeriod:         30 * time.Second,
		MaxReconnectAttempts:    0, // infinite retry
		TLSConfig:               tlsCfg,
		TimestampStringLocation: time.UTC, // see internal/parser/parser.go (#757)
	}
	if cfg.Flavor == "mariadb" {
		// Ask the MariaDB source to send ANNOTATE_ROWS events (the original
		// SQL statement, MariaDB's sibling of MySQL's ROWS_QUERY_EVENT) over
		// the replication stream. Unlike MySQL — which sends ROWS_QUERY
		// unconditionally when binlog_rows_query_log_events=ON — MariaDB only
		// forwards ANNOTATE events to a replica that set this dump flag, even
		// when binlog_annotate_row_events=ON wrote them to the binlog (#699).
		// Harmless when the source has annotation off: no events, no cost.
		syncerCfg.DumpCommandFlag |= replication.BINLOG_SEND_ANNOTATE_ROWS_EVENT
		// MariaDB 11.4+ writes events that pass through the transaction or
		// statement cache (TABLE_MAP, row events, ANNOTATE) with LogPos=0 in
		// the binlog itself — only directly-written events (GTID, XID) carry a
		// real end position. Without this, every captured row stores
		// start_pos = 2^64-EventSize (underflow) and end_pos = 0, and the
		// resume-time dedup (deleteEventsSinceCheckpoint's start_pos >= pos
		// cut) then deletes every already-indexed row in the checkpoint's file
		// on every restart (#1117). FillZeroLogPos makes go-mysql recompute
		// LogPos for those events from the running position (exact, because it
		// also forces BINLOG_SEND_ANNOTATE_ROWS_EVENT so no in-file event is
		// missing from the wire). The option is library-gated to the MariaDB
		// flavor and inert elsewhere.
		syncerCfg.FillZeroLogPos = true
	}

	// Use a closure defer so the active syncer is always closed on exit,
	// even if we replace it during the preferred-mode TLS fallback below.
	// The nil guard prevents a panic if an early-return is added before
	// syncer is assigned.
	var syncer *replication.BinlogSyncer
	defer func() {
		if syncer != nil {
			syncer.Close()
		}
	}()
	syncer = replication.NewBinlogSyncer(syncerCfg)

	// startStreamer starts sync from the resolved position/GTID set.
	startStreamer := func() (*replication.BinlogStreamer, error) {
		switch mode {
		case "position":
			s, startErr := syncer.StartSync(gomysql.Position{Name: startFile, Pos: startPos})
			if startErr != nil {
				return nil, fmt.Errorf("StartSync(%s, %d): %w", startFile, startPos, parser.WrapReplicationError(startErr))
			}
			return s, nil
		case "gtid":
			gset, parseErr := gomysql.ParseGTIDSet(cfg.Flavor, startGTIDStr)
			if parseErr != nil {
				return nil, fmt.Errorf("parse start GTID set: %w", parseErr)
			}
			s, startErr := syncer.StartSyncGTID(gset)
			if startErr != nil {
				return nil, fmt.Errorf("StartSyncGTID: %w", parser.WrapReplicationError(startErr))
			}
			return s, nil
		default:
			return nil, fmt.Errorf("unexpected mode %q", mode)
		}
	}

	// ── 8. Start sync ───────────────────────────────────────────────────────────────
	streamer, startErr := startStreamer()
	if startErr != nil && cfg.SSLMode == "preferred" && isTLSUnsupportedError(startErr) {
		// #947: downgrade to plaintext ONLY when the server genuinely does not
		// support TLS. Any other failure (auth denied, unreachable host, bad
		// binlog position) must NOT retry unencrypted — that would resend the
		// source credentials in the clear on an error unrelated to TLS. This
		// path DOES transmit credentials unencrypted, so warn loudly.
		slog.Warn("source does not support TLS; retrying WITHOUT encryption "+
			"(--ssl-mode preferred) — credentials and data will be sent in cleartext",
			"error", startErr)
		syncer.Close()
		syncerCfg.TLSConfig = nil
		syncer = replication.NewBinlogSyncer(syncerCfg)
		streamer, startErr = startStreamer()
	}
	if startErr != nil {
		return startErr
	}

	switch mode {
	case "position":
		fmt.Printf("Streaming from %s position %d\n", startFile, startPos)
	case "gtid":
		fmt.Printf("Streaming from GTID set: %s\n", startGTIDStr)
	}

	// (Signal handling lives in runStream — the process owner. One only
	// honors ctx, so a supervisor can run several instances under one
	// lifecycle without competing signal handlers.)

	// ── 9. Optional Prometheus metrics HTTP server ─────────────────────────
	// Under `bintrail-console watch` the daemon serves one endpoint for all streams
	// instead (cfg.MetricsAddr is empty there) — the registry is process-
	// global, so one handler exposes every per-source series.
	if cfg.MetricsAddr != "" {
		stopMetrics, err := StartMetricsServer(cfg.MetricsAddr)
		if err != nil {
			return err
		}
		defer stopMetrics()
	}

	// All stream metrics carry a "source" label: the supervisor's entry ID
	// when monitored, else the resolved bintrail_id ("default" if unknown).
	metricsSource := cfg.MetricsSource
	if metricsSource == "" {
		metricsSource = bintrailID
	}
	metrics := observe.ForSource(metricsSource)

	// Index-state gauges (bintrail_index_*, #351): refresh periodically from a
	// status snapshot whenever metrics are exposed — standalone --metrics-addr,
	// a supervisor-launched stream (MetricsSource set), or the watch daemon's
	// own primary stream (IndexMetrics set, since it serves /metrics centrally
	// with MetricsAddr/MetricsSource both empty). The scraper stops with ctx.
	if cfg.MetricsAddr != "" || cfg.MetricsSource != "" || cfg.IndexMetrics {
		startIndexMetricsScraper(ctx, indexDB, cfg.IndexDSN, metricsSource, cfg.MetricsScrapeInterval)
	}

	// ── 10. StreamParser + its synchronous DDL hook ──────────────────────────
	sp := parser.NewStreamParser(resolver, filters, nil)
	// Wire the shared capture-skip tally (#1034) before Run's goroutine spawns
	// (the SetFlavor happens-before contract).
	sp.SetSkipCounters(state.skips)
	// cfg.Flavor is normalized (empty→"mysql") by this point (step 1 above) —
	// wires the source flavor into the live position-wraparound guard inside
	// Run (internal/parser/stream.go) so its GTID-mode remediation names the
	// right system variable (#845).
	sp.SetFlavor(cfg.Flavor)
	idx := indexer.New(indexDB, cfg.BatchSize)

	// ── 11. DDL auto-snapshot hook — registered BEFORE Run starts so even a
	// DDL arriving in the first events cannot miss it ─────────────────────────
	// Registered on the parser, not on streamLoop: the binlog is sequential
	// (`CREATE TABLE t; INSERT INTO t;`), so the resolver must be refreshed
	// before the parser decodes the events that FOLLOW the DDL — a
	// consumer-side handler ran too late and the trailing rows were skipped
	// as "table not in snapshot" (#396).
	// TRUNCATE does not change schema structure, so it only records the change.
	//
	// Fail-loud on snapshot/resolver failure (#760, same philosophy as the
	// #652 flush fix): a stale resolver here means every row event that
	// follows this DDL in the binlog gets silently skipped as "column count
	// mismatch" / "table not in snapshot" while streamLoop's checkpoint keeps
	// advancing past them — a permanent, unmarked loss, since the checkpoint
	// never revisits those events. Returning an error aborts sp.Run BEFORE it
	// even emits this DDL event (see SetSyncDDLHook — the hook now runs
	// before the DDL is sent, not after), so streamLoop never observes it and
	// the checkpoint stays exactly where it was before this DDL. The process
	// exits non-zero; a supervisor restart resumes from there, re-reads this
	// same DDL off the binlog, retries the snapshot, and only then decodes
	// the rows that follow — with a fresh resolver.
	//
	// One carve-out from fail-loud (#1051): VALIDATION failures. The strict
	// TakeSnapshot rejects the whole snapshot when any base table in scope is
	// non-InnoDB or PK-less — but here that table still exists on restart, so
	// the retry loop above never converges and one unrelated no-PK table turns
	// any DDL into an indefinite crash-loop. Since such tables' row events are
	// already skipped (they contribute no recoverable data), the hook uses
	// TakeSnapshotExcludingInvalid, which drops them from the snapshot and
	// reports them for the loud warning below. Real snapshot failures (source
	// unreachable, index write error, nothing valid left to capture) still
	// abort exactly as described above.
	schemas := cfg.Deps.ParseSchemaList(cfg.Schemas)
	sp.SetSyncDDLHook(func(ev parser.Event) error {
		if ev.DDLType == parser.DDLTruncateTable {
			slog.Info("DDL detected (no snapshot needed)",
				"file", ev.BinlogFile, "pos", ev.EndPos,
				"ddl_type", ev.DDLType, "schema", ev.Schema, "table", ev.Table)
			if err := cfg.Deps.InsertSchemaChange(indexDB, ev, nil); err != nil {
				slog.Warn("failed to record schema change", "error", err)
			}
			return nil
		}

		slog.Info("DDL detected — taking auto-snapshot",
			"file", ev.BinlogFile, "pos", ev.EndPos,
			"ddl_type", ev.DDLType, "schema", ev.Schema, "table", ev.Table)

		stats, snapErr := metadata.TakeSnapshotExcludingInvalid(sourceDB, indexDB, schemas)
		if snapErr != nil {
			// Best-effort history record of the attempt, then abort — see the
			// fail-loud rationale above.
			if err := cfg.Deps.InsertSchemaChange(indexDB, ev, nil); err != nil {
				slog.Warn("failed to record schema change", "error", err)
			}
			return fmt.Errorf("auto-snapshot after DDL on %s.%s: %w", ev.Schema, ev.Table, snapErr)
		}
		if len(stats.ExcludedTables) > 0 {
			// #1051: prominent, so the operator learns these tables are
			// invisible to bintrail from the log rather than from a failed
			// recovery attempt.
			slog.Warn("auto-snapshot EXCLUDED tables that are not InnoDB or lack an explicit primary key — "+
				"their row events are not captured and they are absent from this snapshot; "+
				"add a primary key (and use InnoDB) to capture them",
				"excluded_tables", strings.Join(stats.ExcludedTables, ", "),
				"snapshot_id", stats.SnapshotID)
		}

		snapID := &stats.SnapshotID
		newResolver, resolverErr := metadata.NewResolver(indexDB, stats.SnapshotID)
		if resolverErr != nil {
			if err := cfg.Deps.InsertSchemaChange(indexDB, ev, snapID); err != nil {
				slog.Warn("failed to record schema change", "error", err)
			}
			return fmt.Errorf("load resolver after DDL snapshot on %s.%s: %w", ev.Schema, ev.Table, resolverErr)
		}

		sp.SwapResolver(newResolver)
		slog.Info("auto-snapshot taken; resolver updated",
			"snapshot_id", stats.SnapshotID,
			"tables", stats.TableCount,
			"columns", stats.ColumnCount)

		if err := cfg.Deps.InsertSchemaChange(indexDB, ev, snapID); err != nil {
			slog.Warn("failed to record schema change", "error", err)
		}
		return nil
	})

	events := make(chan parser.Event, 1000)
	parseErrCh := make(chan error, 1)

	go func() {
		defer close(events)
		parseErrCh <- sp.Run(ctx, streamer, events)
	}()

	// ── 12. Run stream loop with checkpointing ──────────────────────────────────
	fmt.Printf("Streaming started (server-id=%d, checkpoint=%ds)\n", cfg.ServerID, cfg.Checkpoint)
	loopErr := streamLoop(ctx, events, idx, indexDB,
		time.Duration(cfg.Checkpoint)*time.Second, state, metrics, cfg.Hooks)

	parseErr := drainParser(cancel, parseErrCh)

	// ── 12. Summary ───────────────────────────────────────────────────────────────
	if loopErr != nil {
		return loopErr
	}
	if parseErr != nil && !errors.Is(parseErr, context.Canceled) {
		return parseErr
	}

	if cfg.Format == "json" {
		return cfg.Deps.OutputJSON(struct {
			EventsIndexed int64  `json:"events_indexed"`
			LastFile      string `json:"last_file"`
			LastPosition  uint64 `json:"last_position"`
		}{
			EventsIndexed: state.eventsIndexed,
			LastFile:      state.binlogFile,
			LastPosition:  state.binlogPos,
		})
	}

	fmt.Printf("\nEvents indexed: %d\n", state.eventsIndexed)
	fmt.Printf("Last position:  %s:%d\n", state.binlogFile, state.binlogPos)
	return nil
}

// StartMetricsServer binds the Prometheus /metrics endpoint and returns a
// shutdown func. The bind is SYNCHRONOUS so a bad --metrics-addr fails the
// command fast — the operator explicitly asked for metrics; silently running
// without them (scrapes getting connection-refused) is worse than refusing
// to start. Shared by `bintrail stream` (one endpoint per stream process)
// and the `bintrail-console watch` daemon (one endpoint for all supervised streams;
// the default registry is process-global).
func StartMetricsServer(addr string) (shutdown func(), err error) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	srv := &http.Server{Handler: mux}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("metrics server: cannot bind %s: %w", addr, err)
	}
	go func() {
		slog.Info("metrics server starting", "addr", addr)
		if serveErr := srv.Serve(ln); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			slog.Error("metrics server error", "error", serveErr)
		}
	}()
	return func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
	}, nil
}

// GapRefusedError is the --no-gap-fill refusal: the checkpoint names a
// position the source can no longer serve (binlogs purged, GTIDs inside the
// source's purged set, or a same-named file regenerated after RESET MASTER)
// and the operator asked not to auto-advance past it. msg is
// the gap detector's human description, which names binlog files and GTIDs —
// operator-facing only; the type's usage-telemetry class is binlog_not_found,
// the same bucket as the server's own 1236 for the same condition.
type GapRefusedError struct{ msg string }

func (e *GapRefusedError) Error() string {
	return "binlog gap detected and --no-gap-fill is set: " + e.msg
}

// TelemetryClass implements telemetry.Classed.
func (e *GapRefusedError) TelemetryClass() string { return "binlog_not_found" }
