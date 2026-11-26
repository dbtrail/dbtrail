package pgstreamrun

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/dbtrail/dbtrail/internal/indexer"
)

// The capture owner (#3114): which run of a PostgreSQL capture may save the
// position. Each run of One claims it with a token of its own before it reads
// the saved position, and every save names that token, so a save that a run
// sent before another one started, and that reached the index only after, is
// refused instead of applied.
//
// Without it, that late save was the one write the capture lock cannot stop:
// the new run had read the older position, removed what the index held past
// it, and was indexing it again; the late, higher position then made the run
// after it resume past changes that were neither in the index nor sent again.
// The MySQL capture has no equivalent: it narrows the same window with the
// lock's takeover delay (consoleapp.captureLockTakeoverDelay), which does not
// close it.
//
// The token lives in a table of its own, not in stream_state: a first run has
// no stream_state row yet, and a late save that inserts it must be refused as
// well, which a condition on the update arm alone cannot do. Claiming does not
// touch stream_state either, so its readers see the same row they did.
const ddlCaptureOwner = `CREATE TABLE IF NOT EXISTS pg_capture_owner (
    id         TINYINT UNSIGNED NOT NULL PRIMARY KEY,
    owner      CHAR(32) NOT NULL COMMENT 'the token of the PostgreSQL capture run that may save the position in stream_state (#3114); each run writes a new one when it starts, and a save naming another changes nothing',
    claimed_at DATETIME(6) NOT NULL COMMENT 'when that run claimed it (UTC)',
    CONSTRAINT pg_capture_owner_one_row CHECK (id = 1)
) ENGINE=InnoDB`

// errCaptureOwnerChanged: another run claimed the capture after this one, so
// this one saves nothing more and acknowledges nothing to the slot.
var errCaptureOwnerChanged = errors.New("pgstreamrun: another capture run took over this index since this one started, so this one stops without saving its position; the run that holds it now resumes from the last position saved")

// claimCaptureOwner creates the owner table if needed and makes a new token
// the owner. From this statement on, a save by any earlier run changes
// nothing, so the position read after it is final until this run saves.
func claimCaptureOwner(ctx context.Context, db *sql.DB) (string, error) {
	if _, err := db.ExecContext(ctx, ddlCaptureOwner); err != nil {
		return "", fmt.Errorf("pgstreamrun: create the capture owner table: %w", err)
	}
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("pgstreamrun: make a capture owner token: %w", err)
	}
	token := hex.EncodeToString(b[:])
	wctx, cancel := context.WithTimeout(ctx, indexer.WriteTimeout)
	defer cancel()
	if _, err := db.ExecContext(wctx, `INSERT INTO pg_capture_owner (id, owner, claimed_at) VALUES (1, ?, UTC_TIMESTAMP(6))
		ON DUPLICATE KEY UPDATE owner = VALUES(owner), claimed_at = VALUES(claimed_at)`, token); err != nil {
		return "", fmt.Errorf("pgstreamrun: claim the capture: %w", err)
	}
	return token, nil
}

// stillCaptureOwner reports whether token is still the owner, asked after a
// save. A save that names a token no longer there writes nothing, and MySQL
// answers a write that changed nothing the same as one refused, so this read
// is what tells them apart. If the token is the owner here, the save was
// applied: one claimed after it reads the position this save wrote.
func stillCaptureOwner(db *sql.DB, token string) error {
	ctx, cancel := context.WithTimeout(context.Background(), indexer.WriteTimeout)
	defer cancel()
	var owner string
	err := db.QueryRowContext(ctx, `SELECT owner FROM pg_capture_owner WHERE id = 1`).Scan(&owner)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return errCaptureOwnerChanged
	case err != nil:
		return fmt.Errorf("pgstreamrun: read the capture owner after a save: %w", err)
	case owner != token:
		return errCaptureOwnerChanged
	}
	return nil
}
