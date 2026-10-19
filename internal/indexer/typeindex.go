package indexer

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"slices"
	"strings"
	"sync"
	"time"

	mysql "github.com/go-sql-driver/mysql"
)

// TypeIndexName is the secondary index that makes an event-type filter
// seekable: (event_type, schema_name, table_name, event_timestamp). With the
// type pinned it serves the three shapes a type filter takes: one table
// (all four columns by equality and range, already in time order), the tables
// one partition holds rows of that type for (a loose scan of its first three
// columns), and the type alone (a range on its first column, covering the
// sort keys, so no row image is read to test the type).
//
// It leads with event_type, not with the table, on purpose: an index starting
// (schema_name, table_name) would share idx_row_lookup's prefix, and a query
// on schema and table alone could then be planned on it and sort, where
// idx_row_lookup returns the rows in time order already.
const TypeIndexName = "idx_type_lookup"

// typeIndexColumns is TypeIndexName's key, in order.
var typeIndexColumns = []string{"event_type", "schema_name", "table_name", "event_timestamp"}

// typeIndexDDL builds TypeIndexName on an existing binlog_events. ALGORITHM
// and LOCK are spelled out so the server refuses, instead of silently falling
// back to a build that blocks writes, should it ever be unable to build the
// index online: capture keeps inserting for the whole build.
const typeIndexDDL = "ALTER TABLE binlog_events ADD INDEX " + TypeIndexName +
	" (event_type, schema_name, table_name, event_timestamp), ALGORITHM=INPLACE, LOCK=NONE"

// typeIndexLockWait bounds each metadata-lock wait of the build, on its own
// connection. The online build still takes a brief exclusive lock when it
// starts and when it finishes; while it waits for one (behind a long read),
// every new INSERT queues behind it, so the wait is what capture can stall
// for. A build that cannot get the lock in time fails with 1205 and is retried
// later instead. A variable so tests can shorten it.
var typeIndexLockWait = 5 * time.Second

// TypeIndexState is what binlog_events carries in place of TypeIndexName.
type TypeIndexState int

const (
	// TypeIndexMissing: no index serves an event-type filter; a build is due.
	TypeIndexMissing TypeIndexState = iota
	// TypeIndexPresent: TypeIndexName exists with the expected key.
	TypeIndexPresent
	// TypeIndexOtherName: an index with the expected leading key exists under
	// another name (built by hand). Nothing is built: the server can use it,
	// a FORCE INDEX naming TypeIndexName cannot.
	TypeIndexOtherName
	// TypeIndexWrongShape: TypeIndexName exists with a different key. Left
	// alone: it is not this build's to drop, and a second one would need
	// another name.
	TypeIndexWrongShape
	// TypeIndexNoTable: binlog_events does not exist (not initialized).
	TypeIndexNoTable
)

// TypeIndexStatus reports which TypeIndexState binlog_events is in, with the
// name of the index found under another name, or the key found under ours.
func TypeIndexStatus(ctx context.Context, db *sql.DB) (TypeIndexState, string, error) {
	rows, err := db.QueryContext(ctx, `SELECT INDEX_NAME, COLUMN_NAME FROM information_schema.STATISTICS
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'binlog_events'
		ORDER BY INDEX_NAME, SEQ_IN_INDEX`)
	if err != nil {
		return 0, "", fmt.Errorf("check binlog_events index %s: %w", TypeIndexName, err)
	}
	defer rows.Close()
	var order []string
	keys := map[string][]string{}
	for rows.Next() {
		var name string
		var col sql.NullString // NULL for a functional key part
		if err := rows.Scan(&name, &col); err != nil {
			return 0, "", fmt.Errorf("check binlog_events index %s: %w", TypeIndexName, err)
		}
		if _, seen := keys[name]; !seen {
			order = append(order, name)
		}
		keys[name] = append(keys[name], strings.ToLower(col.String))
	}
	if err := rows.Err(); err != nil {
		return 0, "", fmt.Errorf("check binlog_events index %s: %w", TypeIndexName, err)
	}
	if len(order) == 0 {
		// Every binlog_events has a PRIMARY key: no row at all is no table.
		return TypeIndexNoTable, "", nil
	}
	for _, name := range order {
		if !strings.EqualFold(name, TypeIndexName) {
			continue
		}
		if servesTypeFilter(keys[name]) {
			return TypeIndexPresent, "", nil
		}
		return TypeIndexWrongShape, strings.Join(keys[name], ", "), nil
	}
	for _, name := range order {
		if servesTypeFilter(keys[name]) {
			return TypeIndexOtherName, name, nil
		}
	}
	return TypeIndexMissing, "", nil
}

// TypeIndexColumns is TypeIndexName's key, in order (a copy).
func TypeIndexColumns() []string { return slices.Clone(typeIndexColumns) }

// servesTypeFilter reports whether an index key starts with typeIndexColumns:
// trailing columns do not change what the leading ones serve.
func servesTypeFilter(key []string) bool {
	return len(key) >= len(typeIndexColumns) && slices.Equal(key[:len(typeIndexColumns)], typeIndexColumns)
}

// ErrTypeIndexBuildRunning: the server is already building TypeIndexName on
// this database, typically for a previous run of the daemon that the server
// kept executing after the client went away.
var ErrTypeIndexBuildRunning = errors.New("the index build is already running on the index server")

// typeIndexBuildRunning reports a statement adding TypeIndexName to this
// database's binlog_events that the server is executing now: its thread id
// and for how long it has been running.
func TypeIndexBuildRunning(ctx context.Context, db *sql.DB) (id int64, secs int64, found bool, err error) {
	err = db.QueryRowContext(ctx, `SELECT ID, TIME FROM information_schema.PROCESSLIST
		WHERE DB = DATABASE() AND COMMAND = 'Query' AND ID <> CONNECTION_ID()
		  AND INFO LIKE 'ALTER TABLE%' AND INFO LIKE ? LIMIT 1`, "%ADD INDEX "+TypeIndexName+"%").Scan(&id, &secs)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, 0, false, nil
	}
	if err != nil {
		return 0, 0, false, fmt.Errorf("look for a running build of %s: %w", TypeIndexName, err)
	}
	return id, secs, true, nil
}

// TypeIndexBuildError is a failed build, with whether waiting and trying
// again can fix it.
type TypeIndexBuildError struct {
	err error
	// Retry is true for a lock-wait timeout (a long read held the table when
	// the build needed its brief exclusive lock) and for a lost connection
	// (the server keeps running the ALTER, which the next attempt finds),
	// false for anything trying again at the same load cannot fix (the online
	// log or the temporary space ran out, the account lacks the privilege).
	Retry bool
	hint  string
}

func (e *TypeIndexBuildError) Error() string {
	if e.hint == "" {
		return e.err.Error()
	}
	return e.err.Error() + " — " + e.hint
}

func (e *TypeIndexBuildError) Unwrap() error { return e.err }

// classifyTypeIndexBuildErr turns a failed ALTER into a TypeIndexBuildError
// that names the fix.
func classifyTypeIndexBuildErr(err error) error {
	be := &TypeIndexBuildError{err: fmt.Errorf("build binlog_events index %s: %w", TypeIndexName, err)}
	if errors.Is(err, driver.ErrBadConn) || errors.Is(err, mysql.ErrInvalidConn) {
		be.Retry = true
		be.hint = "the connection to the index server was lost; the server may still be running the build"
		return be
	}
	var me *mysql.MySQLError
	if errors.As(err, &me) {
		switch me.Number {
		case 2013: // CR_SERVER_LOST
			be.Retry = true
			be.hint = "the connection to the index server was lost; the server may still be running the build"
		case 1205: // ER_LOCK_WAIT_TIMEOUT
			be.Retry = true
			be.hint = "a long read held binlog_events when the build needed its brief exclusive lock"
		case 1799: // ER_INNODB_ONLINE_LOG_TOO_BIG
			be.hint = "more rows were written during the build than innodb_online_alter_log_max_size holds; " +
				"raise that server variable, or run `bintrail init` against this index at a quieter hour"
		case 1114, 1878, 1021: // table full, temp file write failure, disk full
			be.hint = "the index server ran out of space for the build's temporary files; free space (or point innodb_tmpdir at a larger disk) " +
				"and run `bintrail init` against this index"
		case 1142, 1044: // no ALTER / INDEX privilege
			be.hint = "the index account lacks the ALTER, CREATE and INSERT privileges ALTER TABLE needs on binlog_events; event-type filters keep working, unindexed"
		case 1845, 1846: // the server cannot build it online
			be.hint = "this server cannot build the index online, so it is not built (a locking build would stop capture); " +
				"build it in a maintenance window with `ALTER TABLE binlog_events ADD INDEX " + TypeIndexName +
				" (event_type, schema_name, table_name, event_timestamp)`"
		}
	}
	return be
}

// EnsureTypeIndex builds TypeIndexName on binlog_events if no index serves an
// event-type filter yet, online, on one connection of db, and logs that it
// started, on how many rows, and how long it took. It returns nil when the
// index is there (or an equivalent one is), ErrTypeIndexBuildRunning when the
// server is already building it, and a *TypeIndexBuildError when the build
// failed.
//
// It is deliberately NOT part of EnsureSchema: that runs at the start of every
// read command and of the console, and a column added there is instant where
// this is a build over every row the index keeps (minutes on a large one). It
// runs from `bintrail init` (explicit, synchronous) and from the capture
// daemons in the background (StartTypeIndexBuild).
func EnsureTypeIndex(ctx context.Context, db *sql.DB, logger *slog.Logger) error {
	if logger == nil {
		logger = slog.Default()
	}
	state, name, err := TypeIndexStatus(ctx, db)
	if err != nil {
		return err
	}
	switch state {
	case TypeIndexPresent, TypeIndexNoTable:
		return nil
	case TypeIndexOtherName:
		logger.Info("binlog_events already has an index on (event_type, schema_name, table_name, event_timestamp) under another name, so "+TypeIndexName+
			" is not built; the newest-first read looks for it by name, so rename it to have it used: ALTER TABLE binlog_events RENAME INDEX `"+name+"` TO "+TypeIndexName,
			"index", name)
		return nil
	case TypeIndexWrongShape:
		logger.Warn("binlog_events has an index named "+TypeIndexName+" with a different key; leaving it alone, so event-type filters may stay slow",
			"key", name, "expected", strings.Join(typeIndexColumns, ", "))
		return nil
	}
	if id, secs, found, err := TypeIndexBuildRunning(ctx, db); err != nil {
		return err
	} else if found {
		logger.Info("the index server is already building "+TypeIndexName+" on binlog_events; not starting another",
			"thread_id", id, "running_for", (time.Duration(secs) * time.Second).String())
		return ErrTypeIndexBuildRunning
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("build binlog_events index %s: %w", TypeIndexName, err)
	}
	defer conn.Close()
	// Session-scoped and on the connection the ALTER runs on: a pooled Exec
	// could set it on one connection and build on another.
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET SESSION lock_wait_timeout = %d", max(1, int(typeIndexLockWait.Seconds())))); err != nil {
		return fmt.Errorf("build binlog_events index %s: %w", TypeIndexName, err)
	}
	// The connection goes back to db's pool, which may be the caller's (init):
	// it must not keep the short bound for that caller's later statements.
	defer conn.ExecContext(context.Background(), "SET SESSION lock_wait_timeout = DEFAULT") //nolint:errcheck // best effort; a dead connection is discarded anyway
	var rowsEstimate sql.NullInt64
	estimate := []any{}
	if err := conn.QueryRowContext(ctx, `SELECT TABLE_ROWS FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'binlog_events'`).Scan(&rowsEstimate); err != nil {
		logger.Debug("cannot estimate the rows of binlog_events", "error", err)
	} else if rowsEstimate.Valid {
		estimate = []any{"rows_estimate", rowsEstimate.Int64}
	}
	logger.Info("building index "+TypeIndexName+" on binlog_events so event-type filters stop reading every row; "+
		"the build is online (capture keeps writing) and takes minutes on a large index", estimate...)
	start := time.Now()
	if _, err := conn.ExecContext(ctx, typeIndexDDL); err != nil {
		var me *mysql.MySQLError
		if errors.As(err, &me) && me.Number == 1061 { // ER_DUP_KEYNAME: built meanwhile
			return nil
		}
		be := classifyTypeIndexBuildErr(err)
		logger.Warn("building index "+TypeIndexName+" on binlog_events failed",
			"after", time.Since(start).Round(time.Millisecond).String(), "error", be)
		return be
	}
	logger.Info("built index "+TypeIndexName+" on binlog_events",
		append([]any{"took", time.Since(start).Round(time.Millisecond).String()}, estimate...)...)
	return nil
}

// typeIndexBuilds records the index databases this process has started a
// background build for, so a supervised stream that restarts (every 15s to
// 5m in a crash loop) never starts a second one; buildMu runs one build at a
// time per process, so the sources of one `watch` that share an index server
// do not build theirs at once.
var (
	typeIndexBuilds sync.Map // key string → struct{}
	buildMu         sync.Mutex
)

// typeIndexRetryWaits is the wait before each further attempt after a
// retriable failure; its length caps the attempts. typeIndexRunningPoll is
// how often a build another thread is running is checked for its end.
// Variables for tests.
var (
	typeIndexRetryWaits  = []time.Duration{2 * time.Minute, 10 * time.Minute}
	typeIndexRunningPoll = time.Minute
)

// StartTypeIndexBuild runs EnsureTypeIndex in the background for the index
// database key names (its DSN), once per process: capture starts at once and
// keeps writing while the server builds. open returns a connection pool of
// its own, closed when the build ends, so a stream restart closing its pool
// never waits for the build (sql.DB.Close waits for running statements). A
// lock-wait failure or a lost connection is tried again after
// typeIndexRetryWaits; a build already running on the server (a previous
// run's) is waited for and the index checked again when it ends; any other
// build failure is logged with its fix and not retried by this process, so a
// stream that restarts never repeats a build that failed at its end. A failure
// before the build (no connection, a panic) frees the key: the next stream
// start tries again.
//
// done, if not nil, is called with the outcome when the goroutine ends.
func StartTypeIndexBuild(key string, open func() (*sql.DB, error), logger *slog.Logger, done func(error)) {
	if _, loaded := typeIndexBuilds.LoadOrStore(key, struct{}{}); loaded {
		return
	}
	if logger == nil {
		logger = slog.Default()
	}
	go func() {
		err := runTypeIndexBuild(context.Background(), open, logger)
		var be *TypeIndexBuildError
		if err != nil && !errors.As(err, &be) {
			typeIndexBuilds.Delete(key)
		}
		if done != nil {
			done(err)
		}
	}()
}

func runTypeIndexBuild(ctx context.Context, open func() (*sql.DB, error), logger *slog.Logger) (err error) {
	defer func() {
		if r := recover(); r != nil {
			logger.Error("building index "+TypeIndexName+" panicked; capture is unaffected", "panic", r, "stack", string(debug.Stack()))
			err = fmt.Errorf("build binlog_events index %s: panic: %v", TypeIndexName, r)
		}
	}()
	db, err := open()
	if err != nil {
		logger.Warn("cannot open a connection to build index "+TypeIndexName, "error", err)
		return err
	}
	defer db.Close()
	for attempt := 0; ; {
		// Held per attempt, not across the waits between them, so one
		// index's retries never hold up another's build.
		err = func() error {
			buildMu.Lock()
			defer buildMu.Unlock() // a panic below must not leave it held
			return EnsureTypeIndex(ctx, db, logger)
		}()
		if errors.Is(err, ErrTypeIndexBuildRunning) {
			// Wait for the other build to end, then look again: it may have
			// failed, with nobody left to say so.
			for {
				time.Sleep(typeIndexRunningPoll)
				_, _, found, perr := TypeIndexBuildRunning(ctx, db)
				if perr != nil {
					logger.Warn("cannot tell whether the running "+TypeIndexName+" build ended; not trying again", "error", perr)
					return perr
				}
				if !found {
					break
				}
			}
			continue
		}
		var be *TypeIndexBuildError
		if err != nil && !errors.As(err, &be) {
			// Failed before any ALTER ran (the index check, the process
			// list, the connection): EnsureTypeIndex returns these unlogged,
			// and in the background nobody reads the return value.
			logger.Warn("could not start building index "+TypeIndexName+" on binlog_events; event-type filters keep working, unindexed. "+
				"The next capture start tries again, or run `bintrail init --index-dsn <this index>` to build it now", "error", err)
			return err
		}
		if be == nil || !be.Retry {
			return err
		}
		if attempt >= len(typeIndexRetryWaits) {
			logger.Error("gave up building index "+TypeIndexName+" on binlog_events after "+fmt.Sprint(attempt+1)+
				" attempts; event-type filters keep working, unindexed. Run `bintrail init --index-dsn <this index>` at a quieter hour to build it",
				"error", err)
			return err
		}
		logger.Info("trying the "+TypeIndexName+" build again later", "in", typeIndexRetryWaits[attempt].String())
		time.Sleep(typeIndexRetryWaits[attempt])
		attempt++
	}
}
