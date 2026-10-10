package rotation

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"time"

	"github.com/go-sql-driver/mysql"
)

// How long one ALTER of binlog_events may wait for the table, and how often
// it is tried (#2280).
//
// An ALTER needs the table to itself for an instant. While a statement that
// reads the table is running, MySQL makes the ALTER wait for it, and puts
// every statement that arrives meanwhile behind the ALTER: capture's INSERTs
// among them. So an ALTER that waits freezes capture for as long as the
// longest read then running still has to run, which was two minutes an hour
// in the run this was found in. With a short lock_wait_timeout the ALTER
// gives up instead, the statements behind it go through, and it is tried
// again after a pause that lets them. A table still busy after the last
// attempt is left for the next cycle.
//
// Variables so a test can shorten them. lock_wait_timeout is whole seconds.
var (
	ddlLockWait     = 2 * time.Second
	ddlLockAttempts = 5
	ddlLockPause    = 3 * time.Second
)

// errTableBusy: the ALTER never got the table within its attempts. Nothing
// was changed. What held the table is not known here: a running statement,
// or a transaction left open after reading or writing it.
var errTableBusy = errors.New("binlog_events was in use every time the statement was tried")

// errChangedWhileWaiting: a partition that matched its archive before the
// first attempt to drop it did not match before a later one.
var errChangedWhileWaiting = errors.New("the partition received rows while its drop waited for the table")

// alterBinlogEvents runs one ALTER of binlog_events without letting it wait
// in front of capture: see ddlLockWait. It returns errTableBusy (wrapped)
// when the table was in use at every attempt, ctx's error when ctx ended
// between attempts, and any other error of the statement as it is.
//
// stillSafe, when not nil, runs before every attempt after the first and its
// error ends the retries: a caller that checked something before the ALTER
// checks it again, because the pause lets other statements change the table.
func alterBinlogEvents(ctx context.Context, db *sql.DB, q string, stillSafe func(context.Context) error) error {
	for attempt := 1; ; attempt++ {
		if attempt > 1 && stillSafe != nil {
			if err := stillSafe(ctx); err != nil {
				return err
			}
		}
		err := alterOnce(ctx, db, q)
		if err == nil || !isLockWaitTimeout(err) {
			return err
		}
		if attempt >= ddlLockAttempts {
			return fmt.Errorf("%w (%d attempts, %s each): %v", errTableBusy, ddlLockAttempts, ddlLockWait, err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(ddlLockPause):
		}
	}
}

// alterOnce runs q on a connection of its own whose lock_wait_timeout is
// ddlLockWait, and does not give that connection back to the pool: the
// setting is the session's, and the next statement to borrow the connection
// would wait under it.
func alterOnce(ctx context.Context, db *sql.DB, q string) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() {
		_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		_ = conn.Close()
	}()
	secs := max(1, int(ddlLockWait/time.Second))
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET SESSION lock_wait_timeout = %d", secs)); err != nil {
		return fmt.Errorf("set lock_wait_timeout: %w", err)
	}
	_, err = conn.ExecContext(ctx, q)
	return err
}

// isLockWaitTimeout reports whether err is MySQL's 1205, which is what a
// statement gets when lock_wait_timeout ends its wait for a metadata lock.
func isLockWaitTimeout(err error) bool {
	var me *mysql.MySQLError
	return errors.As(err, &me) && me.Number == 1205
}
