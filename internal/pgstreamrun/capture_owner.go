package pgstreamrun

import (
	"context"
	"crypto/rand"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/dbtrail/dbtrail/internal/indexer"
)

// The capture owner (#3114): which run of a PostgreSQL capture may save the
// position. Each run of One claims the index's capture_owner row with a token
// of its own before it reads the saved position, and every save checks that
// token, so a save that a run sent before another one started, and that
// reached the index only after, is refused instead of applied.
//
// Without it, that late save was the one write the capture lock cannot stop:
// the new run had read the older position, removed what the index held past
// it, and was indexing it again; the late, higher position then made the run
// after it resume past changes that were neither in the index nor sent again.
// The MySQL capture has no equivalent: it narrows the same window with the
// lock's takeover delay (consoleapp.captureLockTakeoverDelay), which does not
// close it.
//
// The token lives in its own table (indexer's capture_owner), not in
// stream_state: a first run has no stream_state row yet, and a late save that
// inserts it must be refused as well. Claiming does not touch stream_state, so
// its readers see the same row they did.
//
// Only the position is fenced. A batch of rows that an earlier run sent and
// that reaches the index after the new run's start cleanup stays, a duplicate:
// gating every batch on this row would put the row lock and two more round
// trips into the index writer the MySQL capture shares, and a long index
// transaction would hold the row against the next run's claim.

// errCaptureOwnerChanged: another run claimed the capture after this one, so
// this one saves nothing more and acknowledges nothing to the slot.
var errCaptureOwnerChanged = errors.New("pgstreamrun: another capture run took over this index since this one started, so this one stops without saving its position; the run that holds it now resumes from the last position saved")

// claimCaptureOwner makes a new token the owner. It waits for a save holding
// the row to commit, so the position read after it is the newest one saved,
// and from then on a save by any earlier run changes nothing.
func claimCaptureOwner(ctx context.Context, db *sql.DB) (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("pgstreamrun: make a capture owner token: %w", err)
	}
	token := hex.EncodeToString(b[:])
	wctx, cancel := context.WithTimeout(ctx, indexer.WriteTimeout)
	defer cancel()
	if _, err := db.ExecContext(wctx, `INSERT INTO capture_owner (id, owner, claimed_at) VALUES (1, ?, UTC_TIMESTAMP(6))
		ON DUPLICATE KEY UPDATE owner = VALUES(owner), claimed_at = VALUES(claimed_at)`, token); err != nil {
		return "", fmt.Errorf("pgstreamrun: claim the capture: %w", err)
	}
	return token, nil
}

// ownedSaveIdleSeconds is the wait_timeout of the connection a save holds the
// owner row on, as for the index writer's transactions (indexer.Begin): a
// connection the network cut is ended by the server soon, not after eight
// hours during which the next run's claim would wait on the row.
const ownedSaveIdleSeconds = 300

// saveOwned runs one position write while token owns the capture, in one index
// transaction that holds the owner row (SELECT ... FOR UPDATE) from the check
// to the COMMIT. A locking read sees the newest committed owner under any
// isolation level, and a claim, which updates the same row, waits for the
// COMMIT: either the claim comes first and this save is refused, or this save
// commits first and the claiming run reads its position. A plain read inside
// the write (INSERT ... SELECT) would be a snapshot under READ COMMITTED, and
// would not wait for a claim that is about to commit.
func saveOwned(db *sql.DB, token, stmt string, args ...any) error {
	ctx, cancel := context.WithTimeout(context.Background(), indexer.WriteTimeout)
	defer cancel()
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("pgstreamrun: open a connection to save the position: %w", err)
	}
	done := false
	defer func() {
		if !done {
			dropConn(conn)
		}
	}()
	for _, q := range []string{fmt.Sprintf("SET SESSION wait_timeout = %d", ownedSaveIdleSeconds), "START TRANSACTION"} {
		if _, err := conn.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("pgstreamrun: start the position save: %w", err)
		}
	}
	var owner string
	err = conn.QueryRowContext(ctx, `SELECT owner FROM capture_owner WHERE id = 1 FOR UPDATE`).Scan(&owner)
	switch {
	case errors.Is(err, sql.ErrNoRows), err == nil && owner != token:
		return errCaptureOwnerChanged // the deferred drop rolls back
	case err != nil:
		return fmt.Errorf("pgstreamrun: read the capture owner: %w", err)
	}
	if beforeWrite != nil {
		beforeWrite("owned")
	}
	if _, err := conn.ExecContext(ctx, stmt, args...); err != nil {
		return err
	}
	// A COMMIT that times out is ambiguous, as for any index write: the
	// stream stops, and the next start reads whichever position is there.
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		return fmt.Errorf("pgstreamrun: commit the position save: %w", err)
	}
	if _, err := conn.ExecContext(ctx, "SET SESSION wait_timeout = @@GLOBAL.wait_timeout"); err != nil {
		return nil // committed; the deferred drop only discards the connection
	}
	done = true
	return conn.Close()
}

// dropConn closes conn's underlying connection instead of returning it to the
// pool, which also rolls back a transaction left open on it: a pooled
// connection inside a transaction would swallow the next writer's statements.
// As indexer's discard.
func dropConn(conn *sql.Conn) {
	_ = conn.Raw(func(any) error { return driver.ErrBadConn })
	_ = conn.Close()
}
