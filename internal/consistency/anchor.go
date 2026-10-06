package consistency

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-sql-driver/mysql"
)

// How TableChecksum.GTIDSet was tied to the snapshot (#2150).
const (
	// AnchorNative: the server reported the snapshot's own position
	// (Percona Server's Binlog_snapshot_gtid_executed; MariaDB's
	// binlog_snapshot_file/position, turned into a GTID position by
	// BINLOG_GTID_POS). Exact, and takes no lock.
	AnchorNative = "native"
	// AnchorTableLock: the snapshot opened, and the position was read, while
	// a second connection held a read lock on the table (FLUSH TABLES <table>
	// WITH READ LOCK, or LOCK TABLES <table> READ), so no transaction writing
	// the table was in flight. Exact for that table.
	AnchorTableLock = "table-lock"
)

// ErrAnchorBusy: the table's writers never paused long enough to take the
// read lock the anchor needs (every attempt hit lock_wait_timeout). The scan
// was not run; the caller reports the check inconclusive.
var ErrAnchorBusy = errors.New("the table was never free of open write transactions long enough to pin the snapshot's position")

// anchorLockAttempts and anchorLockWait bound what the lock may cost: while a
// table lock waits (for a transaction that wrote the table and is
// still open), new writes to the table queue behind it. Each attempt waits at
// most anchorLockWait (lock_wait_timeout, whole seconds), so the table's
// writers stall at most anchorLockAttempts * anchorLockWait in the worst case,
// and for the time the snapshot takes to open in the ordinary one.
const (
	anchorLockAttempts = 3
	anchorLockWait     = 1 // seconds
)

// anchorLockPause is the pause between two lock attempts, letting the queued
// writers through. A variable so tests do not sleep.
var anchorLockPause = 500 * time.Millisecond

// snapshotAnchor is what openAnchoredSnapshot reports about the snapshot it
// left open on the scan connection.
type snapshotAnchor struct {
	set, flavor string
	// method is AnchorNative or AnchorTableLock; "" when the set is the one
	// read just after the snapshot opened, which can be off by transactions
	// in flight (see openAnchoredSnapshot).
	method string
	// lockRefused: the anchor needed the table lock and the account may not
	// take it (no LOCK TABLES privilege).
	lockRefused bool
	// gtidMode is MySQL's @@gtid_mode when it is not ON (nothing anchored).
	gtidMode string
}

// openAnchoredSnapshot opens START TRANSACTION WITH CONSISTENT SNAPSHOT on
// conn and returns the snapshot's GTID position, exact where it can be.
//
// Why not the set read before and after opening the snapshot: measured on
// MySQL 8.0.46 and 8.4.9 with 48 concurrent committers (#2150), a snapshot
// regularly sees transactions that @@gtid_executed does not list yet, also
// when the two reads around it are equal: InnoDB makes a commit visible before
// the server adds its GTID to the executed set. So on stock MySQL only a lock
// makes the position exact: a read lock on the table (flushLockStmt, else
// readLockStmt) is granted once no transaction that wrote the table is open,
// and a transaction's metadata lock is released only after its commit is
// complete, GTID included. With it held,
// every change to the table the snapshot sees is in the set read under it, and
// none can commit until it is released. It is per table and held for the time
// the snapshot takes to open; see anchorLockAttempts for the worst case.
//
// Where the server has no GTIDs (MySQL gtid_mode=OFF; a MariaDB that has
// written no GTID yet) nothing is anchored and no lock is taken. Without the
// LOCK TABLES privilege the snapshot opens unlocked and the result says so.
func openAnchoredSnapshot(ctx context.Context, db *sql.DB, conn *sql.Conn, schema, table string) (snapshotAnchor, error) {
	if err := startSnapshot(ctx, conn); err != nil {
		return snapshotAnchor{}, err
	}
	set, flavor, err := capturedGTID(ctx, conn)
	if err != nil {
		return snapshotAnchor{}, err
	}
	a := snapshotAnchor{set: set, flavor: flavor}
	if flavor == "" || set == "" {
		return a, nil
	}
	if flavor == GTIDFlavorMySQL {
		// A server that ran with GTIDs and turned them off keeps its old
		// executed set, and ON_PERMISSIVE still commits transactions with
		// no GTID: in both, the set does not name what the snapshot holds.
		var mode string
		if err := conn.QueryRowContext(ctx, "SELECT @@global.gtid_mode").Scan(&mode); err != nil {
			return snapshotAnchor{}, fmt.Errorf("read @@gtid_mode: %w", err)
		}
		if !strings.EqualFold(mode, "ON") {
			a.gtidMode = mode
			return a, nil
		}
	}
	native, ok, err := nativeSnapshotPosition(ctx, conn, flavor)
	if err != nil {
		if ctx.Err() != nil {
			return snapshotAnchor{}, err
		}
		// A server that cannot report it is one without it: degrade to
		// the next way, never fail the table over it.
		slog.Warn("could not read the snapshot's own binlog position; pinning it another way", "error", err)
		ok = false
	}
	if ok {
		a.set, a.method = native, AnchorNative
		return a, nil
	}
	if flavor != GTIDFlavorMySQL {
		return a, nil // MariaDB without a usable snapshot coordinate
	}

	// Stock MySQL: reopen the snapshot under the table lock.
	if _, err := conn.ExecContext(ctx, "ROLLBACK"); err != nil {
		return snapshotAnchor{}, fmt.Errorf("close the unanchored snapshot: %w", err)
	}
	stmt := flushLockStmt
	for attempt := 1; ; attempt++ {
		a, err = lockedSnapshot(ctx, db, conn, schema, table, stmt)
		if errors.Is(err, errLockNeedsReload) {
			// No RELOAD / FLUSH_TABLES: the plain read lock, which a table
			// with steady writes can starve (see flushLockStmt).
			stmt = readLockStmt
			attempt--
			continue
		}
		if !errors.Is(err, errLockWaitTimeout) {
			return a, err
		}
		if attempt >= anchorLockAttempts {
			return snapshotAnchor{}, fmt.Errorf("%w (%d attempts of %ds)", ErrAnchorBusy, attempt, anchorLockWait)
		}
		select {
		case <-ctx.Done():
			return snapshotAnchor{}, ctx.Err()
		case <-time.After(anchorLockPause):
		}
	}
}

var (
	errLockWaitTimeout = errors.New("lock wait timeout")
	errLockNeedsReload = errors.New("FLUSH TABLES needs RELOAD or FLUSH_TABLES")
)

// The two table locks, in the order they are tried.
//
// flushLockStmt takes the table's metadata lock as SHARED_NO_WRITE, a lock
// MySQL queues AHEAD of new writers, so it is granted as soon as the
// transactions already writing the table finish, however steady the writes.
// It needs RELOAD (or FLUSH_TABLES) besides LOCK TABLES, the grants mydumper
// also asks for.
//
// readLockStmt (LOCK TABLES ... READ) needs only LOCK TABLES, but its
// SHARED_READ_ONLY lock does not hold new writers back: measured (#2150), a
// table with six writers kept it waiting past every attempt. It is the
// fallback when RELOAD is missing.
const (
	flushLockStmt = "FLUSH TABLES %s WITH READ LOCK"
	readLockStmt  = "LOCK TABLES %s READ"
)

// lockedSnapshot is one attempt: take the table's read lock on a connection
// of its own, open the snapshot on conn and read the position, release the
// lock. The lock's connection is discarded, never returned to the pool, so
// neither the lock nor its session timeout can outlive the attempt.
func lockedSnapshot(ctx context.Context, db *sql.DB, conn *sql.Conn, schema, table, lockStmt string) (snapshotAnchor, error) {
	lk, err := db.Conn(ctx)
	if err != nil {
		return snapshotAnchor{}, fmt.Errorf("open the lock connection: %w", err)
	}
	defer func() {
		_ = lk.Raw(func(any) error { return driver.ErrBadConn })
		_ = lk.Close()
	}()
	if _, err := lk.ExecContext(ctx, fmt.Sprintf("SET SESSION lock_wait_timeout = %d", anchorLockWait)); err != nil {
		return snapshotAnchor{}, fmt.Errorf("set lock_wait_timeout: %w", err)
	}
	if _, err := lk.ExecContext(ctx, fmt.Sprintf(lockStmt, quoteIdent(schema)+"."+quoteIdent(table))); err != nil {
		var me *mysql.MySQLError
		if errors.As(err, &me) {
			switch {
			case me.Number == 1205: // ER_LOCK_WAIT_TIMEOUT
				return snapshotAnchor{}, errLockWaitTimeout
			case me.Number == 1227 && lockStmt == flushLockStmt: // ER_SPECIFIC_ACCESS_DENIED: no RELOAD
				return snapshotAnchor{}, errLockNeedsReload
			case me.Number == 1044 || me.Number == 1142 || me.Number == 1227: // no LOCK TABLES
				return unlockedSnapshot(ctx, conn)
			}
		}
		return snapshotAnchor{}, fmt.Errorf("lock %s.%s to pin the snapshot's position: %w", schema, table, err)
	}
	if err := startSnapshot(ctx, conn); err != nil {
		return snapshotAnchor{}, err
	}
	set, flavor, err := capturedGTID(ctx, conn)
	if err != nil {
		return snapshotAnchor{}, err
	}
	if _, err := lk.ExecContext(ctx, "UNLOCK TABLES"); err != nil {
		// The position is already read under the lock; discarding the
		// connection (deferred above) releases the lock anyway.
		slog.Warn("could not unlock the table after pinning the snapshot's position; closing its connection releases it", "table", schema+"."+table, "error", err)
	}
	return snapshotAnchor{set: set, flavor: flavor, method: AnchorTableLock}, nil
}

// unlockedSnapshot opens the snapshot without the lock the account may not
// take: today's position, read just after it opens, and the refusal noted.
func unlockedSnapshot(ctx context.Context, conn *sql.Conn) (snapshotAnchor, error) {
	if err := startSnapshot(ctx, conn); err != nil {
		return snapshotAnchor{}, err
	}
	set, flavor, err := capturedGTID(ctx, conn)
	if err != nil {
		return snapshotAnchor{}, err
	}
	return snapshotAnchor{set: set, flavor: flavor, lockRefused: true}, nil
}

func startSnapshot(ctx context.Context, conn *sql.Conn) error {
	if _, err := conn.ExecContext(ctx, "START TRANSACTION WITH CONSISTENT SNAPSHOT"); err != nil {
		return fmt.Errorf("start consistent snapshot: %w", err)
	}
	return nil
}

// nativeSnapshotPosition reads the position the server itself ties to the
// consistent snapshot open on conn. ok is false where the server has none
// (stock MySQL lists no binlog_snapshot_* status; a Percona Server without
// GTIDs reports an empty set; a MariaDB coordinate BINLOG_GTID_POS cannot
// place).
func nativeSnapshotPosition(ctx context.Context, conn *sql.Conn, flavor string) (string, bool, error) {
	rows, err := conn.QueryContext(ctx, "SHOW STATUS LIKE 'binlog_snapshot%'")
	if err != nil {
		return "", false, fmt.Errorf("read the snapshot's binlog status: %w", err)
	}
	st := map[string]string{}
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			rows.Close()
			return "", false, fmt.Errorf("read the snapshot's binlog status: %w", err)
		}
		st[strings.ToLower(k)] = v
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return "", false, fmt.Errorf("read the snapshot's binlog status: %w", err)
	}
	if flavor == GTIDFlavorMySQL {
		set := strings.Join(strings.Fields(st["binlog_snapshot_gtid_executed"]), "")
		if set == "" {
			return "", false, nil
		}
		// Only a value that is a GTID set is a position; anything else
		// (a placeholder outside a snapshot) is no native position.
		if _, err := gomysql.ParseMysqlGTIDSet(set); err != nil {
			return "", false, nil
		}
		return set, true, nil
	}
	file, posText := st["binlog_snapshot_file"], st["binlog_snapshot_position"]
	if file == "" || posText == "" {
		return "", false, nil
	}
	pos, err := strconv.ParseUint(posText, 10, 64)
	if err != nil {
		return "", false, nil
	}
	var g sql.NullString
	if err := conn.QueryRowContext(ctx, "SELECT BINLOG_GTID_POS(?, ?)", file, pos).Scan(&g); err != nil {
		return "", false, fmt.Errorf("turn the snapshot's binlog coordinate into a GTID position: %w", err)
	}
	set := strings.Join(strings.Fields(g.String), "")
	return set, set != "", nil
}
