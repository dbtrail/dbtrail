//go:build integration

package streamrun

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	drivermysql "github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/observe"
	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2151, the premise, on a real server and through the real capture.
//
// event_timestamp is the timestamp in a row event's header. The server takes
// it from the statement's START: a statement that waits on a row lock, or that
// runs for a while before it reaches a row, carries the time it began. The
// binlog holds transactions in COMMIT order. So for one row, the change the
// binlog holds LAST (the row's real final value) can carry an EARLIER time
// than the change before it. Ordering one row's changes by
// (event_timestamp, event_id) then puts them in the wrong order; ordering by
// position, or by event_id on an index a stream wrote, does not.
//
// Four shapes, one row each:
//
//	wait:    A locks the row (SELECT ... FOR UPDATE). B starts its UPDATE and
//	         waits. Two seconds later A updates the row and commits; B's update
//	         then goes through. Binlog: A, B. Times: B is two seconds BEFORE A.
//	twice:   the same with A holding the lock from an earlier UPDATE of the
//	         row. Binlog: A1, A2, B. Times: B is before A2.
//	long:    one UPDATE walks three rows slowly. While it is on the first, a
//	         quick UPDATE of the third commits. The long one then reaches the
//	         third. Binlog: quick, long. Times: long is before quick.
//	clock:   a session with SET TIMESTAMP an hour in the past (a restore or a
//	         replay does that) updates a row last.

// stmtTimeRow is one indexed change of one row, in the order the index holds.
type stmtTimeRow struct {
	id       uint64
	pos      uint64
	at       time.Time
	commitUS sql.NullInt64
	v        string
}

func stmtTimeRows(t *testing.T, indexDB *sql.DB, schema, pk string) []stmtTimeRow {
	t.Helper()
	rows, err := indexDB.Query(`SELECT event_id, start_pos, event_timestamp, commit_ts_us,
			JSON_UNQUOTE(JSON_EXTRACT(row_after, '$.v'))
		FROM binlog_events WHERE schema_name = ? AND table_name = 't' AND pk_values = ?
		ORDER BY event_id`, schema, pk)
	if err != nil {
		t.Fatalf("read indexed changes of row %s: %v", pk, err)
	}
	defer rows.Close()
	var out []stmtTimeRow
	for rows.Next() {
		var r stmtTimeRow
		if err := rows.Scan(&r.id, &r.pos, &r.at, &r.commitUS, &r.v); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	return out
}

// waitStatementAge blocks until a statement containing needle has been running
// on the server for at least age seconds, by the server's own count.
func waitStatementAge(t *testing.T, db *sql.DB, needle string, age int) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		var secs sql.NullInt64
		if err := db.QueryRow(`SELECT MAX(TIME) FROM information_schema.PROCESSLIST
			WHERE ID <> CONNECTION_ID() AND INFO LIKE CONCAT('%', ?, '%')`, needle).Scan(&secs); err != nil {
			t.Fatalf("processlist: %v", err)
		}
		if secs.Valid && secs.Int64 >= int64(age) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("statement %q never reached %d s of age (last seen: %v)", needle, age, secs)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func stmtTimeExec(t *testing.T, ctx context.Context, c *sql.Conn, q string) {
	t.Helper()
	if _, err := c.ExecContext(ctx, q); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
}

// runStatementTimeShapes runs the four shapes against the source.
func runStatementTimeShapes(t *testing.T, sourceDB *sql.DB) {
	t.Helper()
	ctx := context.Background()
	conn := func() *sql.Conn {
		c, err := sourceDB.Conn(ctx)
		if err != nil {
			t.Fatalf("conn: %v", err)
		}
		t.Cleanup(func() { c.Close() })
		return c
	}
	a, b := conn(), conn()

	blocked := func(q string) chan error {
		done := make(chan error, 1)
		go func() {
			_, err := b.ExecContext(ctx, q)
			done <- err
		}()
		return done
	}

	// wait
	stmtTimeExec(t, ctx, a, "BEGIN")
	stmtTimeExec(t, ctx, a, "SELECT v FROM t WHERE id = 1 FOR UPDATE")
	done := blocked("UPDATE t SET v = 'wait-B' WHERE id = 1")
	waitStatementAge(t, sourceDB, "wait-B", 2)
	stmtTimeExec(t, ctx, a, "UPDATE t SET v = 'wait-A' WHERE id = 1")
	stmtTimeExec(t, ctx, a, "COMMIT")
	if err := <-done; err != nil {
		t.Fatalf("wait-B: %v", err)
	}

	// twice
	stmtTimeExec(t, ctx, a, "BEGIN")
	stmtTimeExec(t, ctx, a, "UPDATE t SET v = 'twice-A1' WHERE id = 2")
	done = blocked("UPDATE t SET v = 'twice-B' WHERE id = 2")
	waitStatementAge(t, sourceDB, "twice-B", 2)
	stmtTimeExec(t, ctx, a, "UPDATE t SET v = 'twice-A2' WHERE id = 2")
	stmtTimeExec(t, ctx, a, "COMMIT")
	if err := <-done; err != nil {
		t.Fatalf("twice-B: %v", err)
	}

	// long: rows 41, 42, 43 are read in key order, 1.5 s each.
	done = blocked("UPDATE t SET v = 'long' WHERE id BETWEEN 41 AND 43 AND SLEEP(1.5) = 0")
	waitStatementAge(t, sourceDB, "'long'", 1)
	stmtTimeExec(t, ctx, a, "UPDATE t SET v = 'quick' WHERE id = 43")
	if err := <-done; err != nil {
		t.Fatalf("long: %v", err)
	}

	// clock
	stmtTimeExec(t, ctx, a, "UPDATE t SET v = 'clock-now' WHERE id = 5")
	stmtTimeExec(t, ctx, a, "SET TIMESTAMP = UNIX_TIMESTAMP() - 3600")
	stmtTimeExec(t, ctx, a, "UPDATE t SET v = 'clock-hour-ago' WHERE id = 5")
	stmtTimeExec(t, ctx, a, "SET TIMESTAMP = DEFAULT")
}

// assertStatementTimeShapes checks what the capture indexed for each shape.
func assertStatementTimeShapes(t *testing.T, sourceDB, indexDB *sql.DB, schema string) {
	t.Helper()
	for _, tc := range []struct {
		name string
		pk   string
		want string // the row's value on the source
	}{
		{"wait", "1", "wait-B"},
		{"twice", "2", "twice-B"},
		{"long", "43", "long"},
		{"clock", "5", "clock-hour-ago"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var truth string
			if err := sourceDB.QueryRow("SELECT v FROM t WHERE id = ?", tc.pk).Scan(&truth); err != nil {
				t.Fatalf("read source row: %v", err)
			}
			if truth != tc.want {
				t.Fatalf("source row %s = %q, want %q: the shape did not happen", tc.pk, truth, tc.want)
			}
			rows := stmtTimeRows(t, indexDB, schema, tc.pk)
			if len(rows) < 2 {
				t.Fatalf("row %s: %d indexed changes, want 2 or more", tc.pk, len(rows))
			}
			for i, r := range rows {
				commit := "NULL"
				if r.commitUS.Valid {
					commit = time.UnixMicro(r.commitUS.Int64).UTC().Format("15:04:05.000000")
				}
				t.Logf("row %s change %d: event_id=%d start_pos=%d event_timestamp=%s commit_ts_us=%s v=%s",
					tc.pk, i, r.id, r.pos, r.at.UTC().Format("15:04:05"), commit, r.v)
			}

			// The index a stream writes holds one row's changes in binlog
			// order: ids and positions both ascend, and the last is the truth.
			for i := 1; i < len(rows); i++ {
				if rows[i].pos <= rows[i-1].pos {
					t.Errorf("row %s: event_id %d is at position %d, not after event_id %d at %d",
						tc.pk, rows[i].id, rows[i].pos, rows[i-1].id, rows[i-1].pos)
				}
			}
			last := rows[len(rows)-1]
			if last.v != truth {
				t.Errorf("row %s: the last indexed change is %q, the source row is %q", tc.pk, last.v, truth)
			}

			// The premise: that last change carries an EARLIER time than the
			// one before it. If this stops holding, the server changed what it
			// writes in a row event's header and #2151's reasoning with it.
			prev := rows[len(rows)-2]
			if !last.at.Before(prev.at) {
				t.Errorf("row %s: the last change (%q, %s) is not earlier than the one before it (%q, %s); "+
					"the shape did not produce the disagreement between statement time and binlog order",
					tc.pk, last.v, last.at.UTC().Format(time.RFC3339), prev.v, prev.at.UTC().Format(time.RFC3339))
			}
		})
	}
}

// captureStatementTimeShapes streams the source into the index while the
// shapes run.
func captureStatementTimeShapes(t *testing.T, flavor, sourceDSN string, sourceDB, indexDB *sql.DB, sourceName string) {
	t.Helper()
	testutil.MustExec(t, sourceDB, "CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(32) NOT NULL) ENGINE=InnoDB")
	testutil.MustExec(t, sourceDB, "INSERT INTO t VALUES (1,'seed'),(2,'seed'),(5,'seed'),(41,'seed'),(42,'seed'),(43,'seed')")

	binlogFile, binlogPos, err := config.CurrentBinlogPosition(sourceDB)
	if err != nil {
		t.Fatalf("CurrentBinlogPosition: %v", err)
	}
	if _, err := metadata.TakeSnapshot(sourceDB, indexDB, []string{sourceName}); err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}
	mc, err := drivermysql.ParseDSN(sourceDSN)
	if err != nil {
		t.Fatalf("ParseDSN: %v", err)
	}
	hostStr, portStr, err := net.SplitHostPort(mc.Addr)
	if err != nil {
		t.Fatalf("SplitHostPort: %v", err)
	}
	portN, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil {
		t.Fatalf("ParseUint(port): %v", err)
	}

	const serverID = 92151
	sc := replication.BinlogSyncerConfig{
		ServerID: serverID, Flavor: flavor, Host: hostStr, Port: uint16(portN),
		User: mc.User, Password: mc.Passwd, TimestampStringLocation: time.UTC,
	}
	if flavor == gomysql.MariaDBFlavor {
		sc.FillZeroLogPos = true
	}
	syncer := replication.NewBinlogSyncer(sc)
	defer syncer.Close()
	streamer, err := syncer.StartSync(gomysql.Position{Name: binlogFile, Pos: binlogPos})
	if err != nil {
		t.Fatalf("StartSync: %v", err)
	}
	resolver, err := metadata.NewResolver(indexDB, 0)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	sp := parser.NewStreamParser(resolver, parser.Filters{Schemas: map[string]bool{sourceName: true}}, nil)
	sp.SetFlavor(flavor)
	idx := indexer.New(indexDB, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	events := make(chan parser.Event, 100)
	parseErrCh := make(chan error, 1)
	go func() {
		defer close(events)
		parseErrCh <- sp.Run(ctx, streamer, events)
	}()
	state := &streamState{mode: "position", flavor: flavor, serverID: serverID}
	loopErrCh := make(chan error, 1)
	go func() {
		loopErrCh <- streamLoop(ctx, events, idx, indexDB, time.Minute, state, observe.ForSource("test-2151-"+flavor), nil)
	}()

	runStatementTimeShapes(t, sourceDB)

	// wait 2, twice 3, long 3 rows and quick 1, clock 2.
	waitIndexedCount(t, indexDB, sourceName, 11, 20*time.Second)
	cancel()
	if err := <-loopErrCh; err != nil {
		t.Fatalf("streamLoop: %v", err)
	}
	if perr := <-parseErrCh; perr != nil &&
		!errors.Is(perr, context.Canceled) && !errors.Is(perr, context.DeadlineExceeded) {
		t.Fatalf("StreamParser: %v", perr)
	}
	var v string
	if err := sourceDB.QueryRow("SELECT VERSION()").Scan(&v); err != nil {
		t.Fatalf("version: %v", err)
	}
	t.Logf("source: %s (%s)", v, strings.ToLower(fmt.Sprint(flavor)))
	assertStatementTimeShapes(t, sourceDB, indexDB, sourceName)
}

func TestIntegrationStatementTimeIsNotBinlogOrder_mysql(t *testing.T) {
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	sourceDB, sourceName := testutil.CreateTestDB(t)
	var logBin string
	if err := sourceDB.QueryRow("SELECT @@log_bin").Scan(&logBin); err != nil || logBin != "1" {
		t.Skip("skipping: binary logging not enabled on test MySQL")
	}
	captureStatementTimeShapes(t, gomysql.MySQLFlavor, testutil.IntegrationDSN(sourceName), sourceDB, indexDB, sourceName)
}

func TestIntegrationStatementTimeIsNotBinlogOrder_mariadb(t *testing.T) {
	sourceDB, sourceName := testutil.CreateTestMariaDB(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	captureStatementTimeShapes(t, gomysql.MariaDBFlavor,
		testutil.MariaDBBaseDSN()+"/"+sourceName+"?parseTime=true", sourceDB, indexDB, sourceName)
}
