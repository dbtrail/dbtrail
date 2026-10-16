package indexer

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"

	"github.com/dbtrail/dbtrail/internal/event"
)

// Txn is one transaction in the index: batches written through it are
// invisible to every reader until Commit, and gone after Rollback (#2391). The
// stream uses it so a source transaction larger than one batch is never seen
// half written.
//
// It pins one pooled connection and runs START TRANSACTION / COMMIT /
// ROLLBACK as statements on it, rather than using database/sql's Tx, so that
// every one of them is bounded by WriteTimeout like any other index write
// (#959): a sql.Tx's COMMIT takes no deadline of its own.
//
// A Txn is used from one goroutine. After Commit or Rollback it is spent.
type Txn struct {
	idx  *Indexer
	conn *sql.Conn
}

// Begin opens an index transaction on a connection of its own.
func (idx *Indexer) Begin() (*Txn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), WriteTimeout)
	defer cancel()
	conn, err := idx.db.Conn(ctx)
	if err != nil {
		return nil, deadlineTagged(fmt.Errorf("open an index transaction: %w", err))
	}
	// A connection cut by the network (a half-open socket) is never seen
	// closing by the server, which then keeps its open transaction, its row
	// locks and its hold on binlog_events until wait_timeout: eight hours by
	// default, during which rotation and the resume cleanup wait on it. A
	// short one ends it soon after the client is gone. Restored at Commit.
	for _, q := range []string{fmt.Sprintf("SET SESSION wait_timeout = %d", txnIdleSeconds), "START TRANSACTION"} {
		if _, err := conn.ExecContext(ctx, q); err != nil {
			discard(conn)
			return nil, deadlineTagged(fmt.Errorf("open an index transaction: %w", err))
		}
	}
	return &Txn{idx: idx, conn: conn}, nil
}

// InsertBatch writes a batch inside the transaction, with the same statement
// and deadline as Indexer.InsertBatch. A failed INSERT leaves the transaction
// open: the caller rolls it back.
func (t *Txn) InsertBatch(batch []event.Event) (int64, error) {
	if t.conn == nil {
		return 0, errTxnSpent
	}
	return t.idx.insertBatchOn(t.conn, batch)
}

// Commit makes every batch written through t visible at once.
//
// A COMMIT that runs out of WriteTimeout is ErrWriteDeadline, and like a timed
// out INSERT it is AMBIGUOUS: the server may have committed after the client
// gave up. The caller must not assume either outcome; the stream restarts and
// its resume cleanup removes whatever did land.
func (t *Txn) Commit() error {
	if t.conn == nil {
		return errTxnSpent
	}
	conn := t.conn
	t.conn = nil
	ctx, cancel := context.WithTimeout(context.Background(), WriteTimeout)
	defer cancel()
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		discard(conn)
		return deadlineTagged(fmt.Errorf("commit an index transaction: %w", err))
	}
	// Back to the server's own idle timeout before the pool reuses it. The
	// rows are committed either way: a failure here only drops the connection.
	if _, err := conn.ExecContext(ctx, "SET SESSION wait_timeout = @@GLOBAL.wait_timeout"); err != nil {
		discard(conn)
		return nil
	}
	return conn.Close()
}

// txnIdleSeconds is the wait_timeout of a connection holding an index
// transaction: far above how long capture leaves one idle (it commits within
// DefaultMaxTxnOpen of opening it, or at the next row or tick after that),
// far below the server's default eight hours.
const txnIdleSeconds = 300

// Rollback discards every batch written through t. It is safe to call on a
// spent Txn, so a deferred Rollback after Commit is a no-op.
//
// The connection is DISCARDED, never returned to the pool, whether or not the
// ROLLBACK statement succeeded: a pooled connection left inside a transaction
// would silently swallow the next writer's statements (a checkpoint save among
// them) into a transaction nobody commits.
func (t *Txn) Rollback() error {
	if t.conn == nil {
		return nil
	}
	conn := t.conn
	t.conn = nil
	ctx, cancel := context.WithTimeout(context.Background(), WriteTimeout)
	defer cancel()
	_, err := conn.ExecContext(ctx, "ROLLBACK")
	discard(conn)
	if err != nil {
		return fmt.Errorf("roll back an index transaction: %w", err)
	}
	return nil
}

var errTxnSpent = errors.New("index transaction already committed or rolled back")

// discard closes conn's underlying connection instead of returning it to the
// pool: Raw's callback returning driver.ErrBadConn tells database/sql to drop
// it. The server rolls back whatever transaction was open on it.
func discard(conn *sql.Conn) {
	_ = conn.Raw(func(any) error { return driver.ErrBadConn })
	_ = conn.Close()
}

// deadlineTagged tags err with ErrWriteDeadline when it is a deadline, the way
// insertBatchOn does, so the stream's restart policy treats a slow COMMIT like
// a slow INSERT (#1482).
func deadlineTagged(err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return &writeDeadlineError{err}
	}
	return err
}
