package indexer

import (
	"log/slog"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
)

// The default caps on how much of ONE source transaction a TxnBatcher holds
// in an open index transaction before it makes what it has visible anyway
// (#2391). Past either, the transaction is split: committed in parts.
//
//   - DefaultMaxTxnRows bounds the index transaction's size. An index whose
//     server limits one transaction (max_binlog_cache_size on a BYO index, a
//     group replication size limit) would refuse the COMMIT of an oversized
//     one; capture would restart, read the same transaction and fail again,
//     forever.
//   - DefaultMaxTxnOpen bounds how long it stays open. While it is open,
//     partition rotation cannot take the table: its ALTER waits a few seconds
//     per attempt and gives up after its last one (#2280).
const (
	DefaultMaxTxnRows int64 = 100_000
	DefaultMaxTxnOpen       = 10 * time.Second
)

// CommitStamp is the part of a written row the commit-side metrics need. Rows
// waiting in an open index transaction keep only this until it commits, not
// their row images.
type CommitStamp struct {
	ReadAt, Timestamp time.Time
}

// TxnBatcherHooks are a TxnBatcher's ties to its caller. All are optional.
type TxnBatcherHooks struct {
	// Stopped, when it returns an error, forbids every write: the batcher
	// returns that error instead of writing (a process that lost its capture
	// lock must write nothing after it knows).
	Stopped func() error
	// Statement runs after each INSERT statement, with its row count.
	Statement func(rows int)
	// WriteError runs once per failed write (an INSERT, a BEGIN or a COMMIT).
	WriteError func()
	// Published runs once rows are committed: queryable and recoverable.
	Published func(n int64, stamps []CommitStamp)
	// Split runs each time the open transaction is committed in parts; first
	// says it is the first split of that transaction.
	Split func(why string, rows int64, open time.Duration, first bool)
}

// TxnBatcher writes captured rows to the index so that a reader never sees a
// source transaction in part (#2391).
//
// Rows of COMPLETE transactions are batched together, up to the batch size,
// and each batch commits in one statement (or one index transaction when it
// needs more than one). Rows of the transaction still being read are never
// committed with them: once they fill a batch they go into an index
// transaction of their own, which commits at the source transaction's End,
// or in parts past the row or time cap.
//
// Used from one goroutine. Close it when done: it rolls back an index
// transaction left open.
type TxnBatcher struct {
	idx     *Indexer
	maxRows int64
	maxOpen time.Duration
	hooks   TxnBatcherHooks

	// batch holds rows read and not yet written. batch[:done] belong to
	// transactions whose end has arrived; batch[done:] to the one still being
	// read. tx, when open, holds ONLY rows of that open transaction, written
	// but not committed, so whenever tx != nil, done == 0.
	batch    []event.Event
	done     int
	inTxn    bool
	tx       *Txn
	txBegan  time.Time
	txRows   int64
	txStamps []CommitStamp
	capTimer *time.Timer
	splits   int
}

// NewTxnBatcher returns a batcher writing through idx. maxRows and maxOpen
// are the caps (DefaultMaxTxnRows, DefaultMaxTxnOpen).
func NewTxnBatcher(idx *Indexer, maxRows int64, maxOpen time.Duration, hooks TxnBatcherHooks) *TxnBatcher {
	return &TxnBatcher{idx: idx, maxRows: maxRows, maxOpen: maxOpen, hooks: hooks,
		batch: make([]event.Event, 0, idx.BatchSize())}
}

// InTxn reports whether a source transaction is being read: rows arrived
// since the last End.
func (b *TxnBatcher) InTxn() bool { return b.inTxn }

// CapC fires when the open index transaction reaches the time cap; the
// caller then calls OnCap. Nil (never fires) when none is open.
func (b *TxnBatcher) CapC() <-chan time.Time {
	if b.capTimer == nil {
		return nil
	}
	return b.capTimer.C
}

func (b *TxnBatcher) stopped() error {
	if b.hooks.Stopped != nil {
		return b.hooks.Stopped()
	}
	return nil
}

func (b *TxnBatcher) writeFailed() {
	if b.hooks.WriteError != nil {
		b.hooks.WriteError()
	}
}

func (b *TxnBatcher) published(n int64, stamps []CommitStamp) {
	if b.hooks.Published != nil {
		b.hooks.Published(n, stamps)
	}
}

func appendStamps(dst []CommitStamp, rows []event.Event) []CommitStamp {
	for i := range rows {
		dst = append(dst, CommitStamp{ReadAt: rows[i].ReadAt, Timestamp: rows[i].Timestamp})
	}
	return dst
}

// writeChunks INSERTs rows through ins, at most one batch per statement. A
// failure is reported to WriteError once.
func (b *TxnBatcher) writeChunks(rows []event.Event, ins func([]event.Event) (int64, error)) (int64, error) {
	var total int64
	for len(rows) > 0 {
		k := min(len(rows), b.idx.BatchSize())
		n, err := ins(rows[:k])
		if b.hooks.Statement != nil {
			b.hooks.Statement(k)
		}
		total += n
		if err != nil {
			b.writeFailed()
			return total, err
		}
		rows = rows[k:]
	}
	return total, nil
}

// Add takes one row of the transaction being read.
func (b *TxnBatcher) Add(ev event.Event) error {
	b.inTxn = true
	b.batch = append(b.batch, ev)
	if len(b.batch)-b.done >= b.idx.BatchSize() {
		// The open transaction fills a batch on its own. What came before it
		// is complete: commit that first, so the index transaction it moves
		// into holds nothing else.
		if err := b.CommitComplete(); err != nil {
			return err
		}
		if err := b.spill(); err != nil {
			return err
		}
	}
	return b.OnCap()
}

// End says the transaction being read ended (a commit, or a DDL that ends
// whatever came before it). Its rows become visible now if they were already
// in an index transaction, or at the latest when the batch fills.
func (b *TxnBatcher) End() error {
	b.inTxn, b.splits = false, 0
	if b.tx != nil {
		// batch holds only this transaction's remaining rows (done == 0).
		return b.commitTx()
	}
	b.done = len(b.batch)
	if len(b.batch) >= b.idx.BatchSize() {
		return b.CommitComplete()
	}
	return nil
}

// CommitComplete makes the rows of complete transactions durable and
// visible, all at once. The transaction being read is left as it is.
func (b *TxnBatcher) CommitComplete() error {
	if err := b.stopped(); err != nil {
		return err
	}
	rows := b.batch[:b.done]
	if len(rows) == 0 {
		return nil
	}
	var n int64
	var err error
	if len(rows) <= b.idx.BatchSize() {
		n, err = b.writeChunks(rows, b.idx.InsertBatch) // one statement commits on its own
	} else {
		// More than one statement: they must land together, or a reader
		// between them could see a transaction cut at the chunk edge.
		var t *Txn
		if t, err = b.idx.Begin(); err != nil {
			b.writeFailed()
		} else {
			if n, err = b.writeChunks(rows, t.InsertBatch); err == nil {
				if err = t.Commit(); err != nil {
					b.writeFailed()
				}
			}
			if rerr := t.Rollback(); rerr != nil { // a no-op after a successful Commit
				slog.Warn("rolling back an index transaction failed; its connection was dropped", "error", rerr)
			}
		}
	}
	if err != nil {
		return err
	}
	b.published(n, appendStamps(nil, rows))
	b.batch = b.batch[:copy(b.batch, b.batch[b.done:])]
	b.done = 0
	return nil
}

// spill writes the open transaction's rows into its index transaction,
// uncommitted. Requires done == 0: CommitComplete runs first.
func (b *TxnBatcher) spill() error {
	if err := b.stopped(); err != nil {
		return err
	}
	if len(b.batch) == 0 {
		return nil
	}
	if b.tx == nil {
		t, err := b.idx.Begin()
		if err != nil {
			b.writeFailed()
			return err
		}
		b.tx, b.txBegan = t, time.Now()
		b.capTimer = time.NewTimer(b.maxOpen)
	}
	n, err := b.writeChunks(b.batch, b.tx.InsertBatch)
	if err != nil {
		return err
	}
	b.txRows += n
	b.txStamps = appendStamps(b.txStamps, b.batch)
	b.batch = b.batch[:0]
	return nil
}

func (b *TxnBatcher) clearTx() {
	b.tx, b.txRows, b.txStamps = nil, 0, nil
	if b.capTimer != nil {
		b.capTimer.Stop()
		b.capTimer = nil
	}
}

// commitTx commits the open index transaction, spilling first.
func (b *TxnBatcher) commitTx() error {
	if err := b.spill(); err != nil {
		return err
	}
	if b.tx == nil {
		return nil
	}
	if err := b.stopped(); err != nil {
		return err
	}
	n, stamps := b.txRows, b.txStamps
	err := b.tx.Commit()
	b.clearTx()
	if err != nil {
		b.writeFailed()
		return err
	}
	b.published(n, stamps)
	return nil
}

// OnCap commits the open transaction's rows before its end arrives when the
// open index transaction is past a cap. Its later rows start a new index
// transaction. The caller's checkpoint must not move: it still names the
// transaction's start, so a resume reads it whole again.
func (b *TxnBatcher) OnCap() error {
	var why string
	switch {
	case b.tx == nil:
		return nil
	case b.txRows >= b.maxRows:
		why = "row cap"
	case time.Since(b.txBegan) >= b.maxOpen:
		why = "time cap"
	default:
		return nil
	}
	rows, open := b.txRows, time.Since(b.txBegan)
	if err := b.commitTx(); err != nil {
		return err
	}
	b.splits++
	if b.hooks.Split != nil {
		b.hooks.Split(why, rows, open, b.splits == 1)
	}
	return nil
}

// Discard drops every row of the transaction being read, written or not, and
// returns how many. Rows of complete transactions are kept for
// CommitComplete. Already split parts stay committed.
func (b *TxnBatcher) Discard() int64 {
	n := int64(len(b.batch)-b.done) + b.txRows
	b.batch = b.batch[:b.done]
	b.inTxn, b.splits = false, 0
	b.Close()
	return n
}

// Close rolls back an open index transaction.
func (b *TxnBatcher) Close() {
	if b.tx == nil {
		return
	}
	if err := b.tx.Rollback(); err != nil {
		slog.Warn("rolling back the open index transaction failed; its connection was dropped, which rolls it back", "error", err)
	}
	b.clearTx()
}
