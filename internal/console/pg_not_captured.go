package console

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/dbtrail/dbtrail/internal/status"
	"github.com/dbtrail/dbtrail/internal/views"
)

// PGTableNotCaptured is a table of a PostgreSQL server's copy whose capture
// lost or dropped changes since its snapshot's rows were read from the
// source (PGTablesNotCaptured).
type PGTableNotCaptured struct {
	Schema, Table, Why string
	// SnapshotRead is when its snapshot's rows were read from the source
	// (the source's clock), for the notice of a connection that cannot send
	// the statement there; zero when not known.
	SnapshotRead time.Time
}

// PGTablesNotCaptured lists the tables of this connection's copy whose tail
// is refused for a loss of capture (pgTailCaptureLoss): changes the capture
// dropped (a table below REPLICA IDENTITY FULL, a deferrable key), a ledger
// of drops that does not say which tables, or cuts its list short, a ledger
// that cannot be read, a slot lost since the snapshot's rows were read. Such
// a table is read from its snapshot alone, which no later change reaches: the
// PostgreSQL port sends a statement that reads one to the source
// (pgshim.Binding.NotCaptured).
//
// A snapshot, or the table delta beside it, published over a known loss of
// captured changes is listed too: its rows are known to miss changes.
//
// In fresh mode and in fast mode alike. In fast mode every table is read
// from its snapshot, but a table capture drops is carried forward as the
// file of its last full read and its snapshot does not move, and the copy's
// age is not computed for a PostgreSQL server, so no lag limit would catch
// it either.
//
// A table whose tail is refused for another reason (its snapshot does not
// record a position, or when its rows were read; a MySQL delta beside it) is
// listed only when the loss rule lists it, asked with what its footer says.
//
// The ledger is read strictly: status.LoadStreamState reads it best effort,
// and an error there would read as no drop. When the capture's state cannot
// be read, every table is listed.
func (q *SQLOnCopy) PGTablesNotCaptured(ctx context.Context) ([]PGTableNotCaptured, error) {
	if q == nil || q.s == nil || q.b == nil || q.b.db == nil || !q.s.pgTailsFor(q.id, q.b) {
		return nil, nil
	}
	in, err := q.s.buildViewsInput(ctx, q.b, viewsRequest{PinSnapshot: true, OmitEvents: true, StateOnly: true, ForStatement: true, LocalCopy: true, ApplyHidden: true})
	if err != nil {
		return nil, fmt.Errorf("list the copy's tables: %w", err)
	}
	if len(in.Baselines) == 0 {
		return nil, nil
	}
	st, unreadable := pgCaptureLossState(ctx, q.b.db)
	src := newPGTailSource(q.s, q.b)
	return pgNotCapturedOf(in.Baselines, src.cut, st, unreadable, time.Now(), pgFileWritten), nil
}

// pgNoCaptureStateBound is how long after a table's snapshot was read the
// PostgreSQL port takes a capture that has saved no state in the index yet
// (no stream_state row) for a young one, and answers the table from the
// copy. Past it, the capture is taken as one that cannot save (a refused
// write, an account without the right, a capture that stops before its first
// save), whose drops live only in its memory: the table is listed. Six of the
// daemon's saves (consoleapp/monitor.go runs PostgreSQL capture with a 10 s
// checkpoint; bintrail-pg's default is 5 s), a minute.
const pgNoCaptureStateBound = time.Minute

// PGNoCaptureStateBound is pgNoCaptureStateBound for the PostgreSQL port's
// rule on a capture that saved its state and stopped saving it (#3202,
// consoleapp): one bound for "capture is not saving".
const PGNoCaptureStateBound = pgNoCaptureStateBound

// pgFileWritten is when t's file was last written, by this host's clock;
// zero when it cannot be told.
func pgFileWritten(t views.BaselineTable) time.Time {
	fi, err := os.Stat(t.Path)
	if err != nil {
		return time.Time{}
	}
	return fi.ModTime()
}

// pgSnapshotAge is how old t's snapshot is at now, for pgNoCaptureStateBound:
// the larger of the age by its rows' read (the source server's clock) and by
// its file's write (this host's), so that neither clock being behind the
// other makes it look younger than it is. ok false: neither is known.
func pgSnapshotAge(now, read, written time.Time) (age time.Duration, ok bool) {
	if !read.IsZero() {
		age, ok = now.Sub(read), true
	}
	if !written.IsZero() {
		if a := now.Sub(written); !ok || a > age {
			age, ok = a, true
		}
	}
	return age, ok
}

// pgNotCapturedOf is PGTablesNotCaptured over tables, with each one's cut
// read by cutOf and the capture's state st; unreadable, when not "", is why
// that state could not be read, and lists every table.
//
// now and written (pgFileWritten) date the snapshots for a capture that has
// saved no state (pgNoCaptureStateBound).
func pgNotCapturedOf(tables []views.BaselineTable, cutOf func(views.BaselineTable) pgCut, st *status.StreamStateInfo, unreadable string, now time.Time, written func(views.BaselineTable) time.Time) []PGTableNotCaptured {
	var out []PGTableNotCaptured
	for _, t := range tables {
		if unreadable != "" {
			out = append(out, PGTableNotCaptured{Schema: t.Schema, Table: t.Table, Why: unreadable})
			continue
		}
		c := cutOf(t)
		if c.gap {
			// Published over a known loss: its rows are known to miss
			// changes, a loss of capture whatever the ledger says.
			out = append(out, PGTableNotCaptured{Schema: t.Schema, Table: t.Table, Why: c.refusal})
			continue
		}
		// A tail refused for another reason, or a snapshot that records
		// neither what its rows reach nor when they were read: not a loss in
		// itself, and the loss rule is asked with what is known (nothing,
		// when the footer was not read), so that a drop or a lost slot that
		// covers the table still lists it.
		var lsn uint64
		var read time.Time
		if c.footer {
			lsn, read = c.readPoint()
		}
		if st == nil {
			// Capture has saved no state: young if the snapshot is, else a
			// capture that cannot save, whose drops are nowhere on record.
			age, known := pgSnapshotAge(now, read, written(t))
			if !known || age >= pgNoCaptureStateBound {
				out = append(out, PGTableNotCaptured{Schema: t.Schema, Table: t.Table, SnapshotRead: read,
					Why: "capture has saved no state in the index since this table's snapshot was read, so its dropped changes are not on record"})
			}
			continue
		}
		if why := pgCaptureLossOf(st, lsn, read, t.Schema, t.Table); why != "" {
			out = append(out, PGTableNotCaptured{Schema: t.Schema, Table: t.Table, Why: why, SnapshotRead: read})
		}
	}
	return out
}

// pgCaptureLossState is the capture's state with its ledger of dropped
// changes read strictly, or why it could not be read. A ledger column the
// index does not have yet is no ledger, as LoadStreamState reads it.
func pgCaptureLossState(ctx context.Context, db *sql.DB) (*status.StreamStateInfo, string) {
	st, err := status.LoadStreamState(ctx, db)
	if err != nil {
		// The error's own text (an address, an account of the index) is the
		// log's: the reason reaches a client of the port.
		pgWarnLossStateUnread(err)
		return nil, "the capture's state could not be read from the index (DBTrail's log has the details)"
	}
	if err := pgReadLedgerStrict(ctx, db, st); err != nil {
		pgWarnLossStateUnread(err)
		return nil, "the capture's record of dropped changes could not be read from the index (DBTrail's log has the details)"
	}
	return st, ""
}

// pgLossStateWarned is when pgWarnLossStateUnread last said so at warn
// level, for the process.
var pgLossStateWarned struct {
	mu sync.Mutex
	at time.Time
}

// pgWarnLossStateUnread logs that the capture's state could not be read for
// the PostgreSQL port: at warn level at most once a minute, at debug level
// in between, since it is asked before every statement for the copy.
func pgWarnLossStateUnread(err error) {
	pgLossStateWarned.mu.Lock()
	due := time.Since(pgLossStateWarned.at) >= time.Minute
	if due {
		pgLossStateWarned.at = time.Now()
	}
	pgLossStateWarned.mu.Unlock()
	const msg = "console: the capture's state could not be read; the PostgreSQL port takes every table of the copy as not kept whole"
	if due {
		slog.Warn(msg+" (said once a minute)", "error", err)
		return
	}
	slog.Debug(msg, "error", err)
}
