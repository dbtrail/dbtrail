package consoleapp

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/console"
)

// sourcePositions reads each server's source binary log position for fresh
// mode's wait (#2645), over a small pool of its own per server, opened with
// the account and TLS capture uses: that account has REPLICATION CLIENT,
// which the read needs and a client's own account usually lacks. A pool
// whose source changed (another DSN or TLS) is closed and opened again.
type sourcePositions struct {
	mu    sync.Mutex
	pools map[string]*sourcePositionPool
	// connect opens a pool; connectSource unless a test sets it.
	connect func(dsn string, ssl config.SSL) (*sql.DB, error)
}

// sourcePositionPool is one server's: its own lock, so a source that takes
// its time to connect holds back no other server.
type sourcePositionPool struct {
	mu  sync.Mutex
	key string
	db  *sql.DB
	// failedUntil: a connection that failed is not tried again before then,
	// so a source that is down costs one attempt, not one per statement.
	failedUntil time.Time
	failed      error
	// identity is the server's (@@server_uuid, or server_id:<n> on MariaDB,
	// as capture records it), read at identityAt; "" when it could not be.
	identity   string
	identityAt time.Time
}

const (
	sourcePositionRetry    = 30 * time.Second
	sourceIdentityKeepFor = 30 * time.Second
)

func newSourcePositions() *sourcePositions {
	return &sourcePositions{pools: map[string]*sourcePositionPool{}, connect: connectSource}
}

// SourcePosition implements console.SourcePositionReader.
func (p *sourcePositions) SourcePosition(ctx context.Context, e console.ServerEntry) (console.SourcePosition, error) {
	pool, db, err := p.pool(e)
	if err != nil {
		return console.SourcePosition{}, sourcePositionError(err, e.SourceDSN)
	}
	file, pos, err := readBinlogPosition(ctx, db)
	if err != nil {
		return console.SourcePosition{}, sourcePositionError(err, e.SourceDSN)
	}
	return console.SourcePosition{File: file, Pos: pos, Identity: pool.identityOf(ctx, db, e.SourceFlavor())}, nil
}

func (p *sourcePositions) pool(e console.ServerEntry) (*sourcePositionPool, *sql.DB, error) {
	ssl := e.SourceSSL()
	key := e.SourceDSN + "\x00" + fmt.Sprintf("%+v", ssl)
	p.mu.Lock()
	pool := p.pools[e.ID]
	if pool == nil {
		pool = &sourcePositionPool{}
		p.pools[e.ID] = pool
	}
	p.mu.Unlock()
	pool.mu.Lock()
	defer pool.mu.Unlock()
	if pool.key != key {
		if pool.db != nil {
			pool.db.Close()
		}
		pool.key, pool.db, pool.failed, pool.failedUntil, pool.identity, pool.identityAt = key, nil, nil, time.Time{}, "", time.Time{}
	}
	if pool.db != nil {
		return pool, pool.db, nil
	}
	if time.Now().Before(pool.failedUntil) {
		return pool, nil, pool.failed
	}
	db, err := p.connect(e.SourceDSN, ssl)
	if err != nil {
		pool.failed, pool.failedUntil = err, time.Now().Add(sourcePositionRetry)
		return pool, nil, err
	}
	// One read at a time per server is what the wait asks (statements share
	// it); a second connection covers one that is being replaced.
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(2)
	db.SetConnMaxIdleTime(5 * time.Minute)
	pool.db = db
	return pool, db, nil
}

// identityOf is the server's identity as capture records it in
// stream_state.source_identity, read again every sourceIdentityKeepFor;
// "" when it cannot be read (then no comparison is made).
func (pool *sourcePositionPool) identityOf(ctx context.Context, db *sql.DB, flavor string) string {
	pool.mu.Lock()
	if !pool.identityAt.IsZero() && time.Since(pool.identityAt) < sourceIdentityKeepFor {
		id := pool.identity
		pool.mu.Unlock()
		return id
	}
	pool.mu.Unlock()
	id := ""
	if flavor == console.FlavorMariaDB {
		var n uint32
		if db.QueryRowContext(ctx, "SELECT @@server_id").Scan(&n) == nil {
			id = fmt.Sprintf("server_id:%d", n)
		}
	} else {
		var u string
		if db.QueryRowContext(ctx, "SELECT @@server_uuid").Scan(&u) == nil {
			id = strings.ToLower(strings.TrimSpace(u))
		}
	}
	pool.mu.Lock()
	pool.identity, pool.identityAt = id, time.Now()
	pool.mu.Unlock()
	return id
}

// sourcePositionError scrubs the DSN out of err and marks a refusal: an
// account the server turns away (a missing privilege, a login refused) is
// console.ErrSourcePositionRefused, which stops the wait for a while instead
// of failing every statement's read the same way.
func sourcePositionError(err error, dsn string) error {
	text := config.ScrubDSNError(err, dsn)
	var me *mysql.MySQLError
	if errors.As(err, &me) {
		switch me.Number {
		case 1227, 1044, 1045, 1142:
			return fmt.Errorf("%w: %s", console.ErrSourcePositionRefused, text)
		}
	}
	return errors.New(text)
}

// readBinlogPosition is config.CurrentBinlogPosition with a context: SHOW
// BINARY LOG STATUS (MySQL 8.4 and later), else SHOW MASTER STATUS (older
// MySQL, MariaDB). A refusal on the first is returned at once: the second
// needs the same privilege.
func readBinlogPosition(ctx context.Context, db *sql.DB) (string, uint64, error) {
	file, pos, err := scanBinlogStatusCtx(ctx, db, "SHOW BINARY LOG STATUS")
	if err == nil {
		return file, pos, nil
	}
	var me *mysql.MySQLError
	if ctx.Err() != nil || (errors.As(err, &me) && me.Number != 1064) {
		return "", 0, err
	}
	first := err
	file, pos, err = scanBinlogStatusCtx(ctx, db, "SHOW MASTER STATUS")
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) || errors.Is(first, sql.ErrNoRows) {
			return "", 0, errors.New("the source reports no binary log position: binary logging appears to be off")
		}
		return "", 0, err
	}
	return file, pos, nil
}

func scanBinlogStatusCtx(ctx context.Context, db *sql.DB, stmt string) (string, uint64, error) {
	rows, err := db.QueryContext(ctx, stmt)
	if err != nil {
		return "", 0, err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return "", 0, err
	}
	if len(cols) < 2 {
		return "", 0, fmt.Errorf("%s returned %d column(s), expected at least File and Position", stmt, len(cols))
	}
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return "", 0, err
		}
		return "", 0, sql.ErrNoRows
	}
	vals := make([]any, len(cols))
	for i := range vals {
		vals[i] = new(sql.RawBytes)
	}
	if err := rows.Scan(vals...); err != nil {
		return "", 0, err
	}
	file := string(*vals[0].(*sql.RawBytes))
	pos, err := strconv.ParseUint(string(*vals[1].(*sql.RawBytes)), 10, 64)
	if err != nil || file == "" {
		return "", 0, fmt.Errorf("%s returned a position that does not read (%q, %q)", stmt, file, string(*vals[1].(*sql.RawBytes)))
	}
	return file, pos, rows.Err()
}
