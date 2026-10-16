//go:build integration

package streamrun

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/observe"
	"github.com/dbtrail/dbtrail/internal/parser"
)

// #2391, second half: a reader of the index sees a source transaction whole or
// not at all.

const atomicGTID = "3f2a1c88-0000-0000-0000-000000002391:1"

// liveLoop runs the real streamLoop on an UNBUFFERED channel. A send returns
// only once the loop has taken that event, which it does only after it has
// fully handled the one before: so after send(ev[k]) returns, every effect of
// ev[k-1] is in the database, and a probe there sees exactly what a reader
// between those two events would.
type liveLoop struct {
	ch   chan parser.Event
	errc chan error
	m    *observe.StreamMetrics
}

func startLoop(t *testing.T, idx *indexer.Indexer, db *sql.DB, state *streamState, interval time.Duration, source string) *liveLoop {
	t.Helper()
	l := &liveLoop{ch: make(chan parser.Event), errc: make(chan error, 1), m: observe.ForSource(source)}
	go func() { l.errc <- streamLoop(context.Background(), l.ch, idx, db, interval, state, l.m, nil) }()
	t.Cleanup(func() {
		select {
		case <-l.errc:
		default:
			close(l.ch)
			<-l.errc
		}
	})
	return l
}

func (l *liveLoop) send(t *testing.T, ev parser.Event) {
	t.Helper()
	select {
	case l.ch <- ev:
	case err := <-l.errc:
		t.Fatalf("streamLoop ended early: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("streamLoop stopped taking events")
	}
}

// settle makes sure the last event sent has been fully handled: a GTID marker
// is taken only after it, and changes nothing.
func (l *liveLoop) settle(t *testing.T) {
	t.Helper()
	l.send(t, parser.Event{EventType: parser.EventGTID, BinlogFile: "binlog.000007"})
}

func (l *liveLoop) finish(t *testing.T) {
	t.Helper()
	close(l.ch)
	if err := <-l.errc; err != nil {
		t.Fatalf("streamLoop: %v", err)
	}
	l.errc <- nil // for the cleanup
}

func positionState() *streamState {
	return &streamState{mode: "position", serverID: 1, binlogFile: "binlog.000007", binlogPos: 50,
		safeFile: "binlog.000007", safePos: 50, flavor: gomysql.MySQLFlavor}
}

func gtidState(t *testing.T) *streamState {
	acc, err := parseGTIDSetForFlavor(gomysql.MySQLFlavor, "")
	if err != nil {
		t.Fatal(err)
	}
	return &streamState{mode: "gtid", serverID: 1, accGTID: acc, flavor: gomysql.MySQLFlavor}
}

// The acceptance test of #2391: a transaction of 2500 rows with batches of
// 1000. A reader running between the loop's writes sees none of it, then all
// of it.
func TestIntegrationReaderSeesAWholeTransactionOrNone_2391(t *testing.T) {
	for _, tc := range []struct {
		name  string
		gtid  string
		state func(*testing.T) *streamState
	}{
		{"position mode", "", func(*testing.T) *streamState { return positionState() }},
		{"gtid mode", atomicGTID, gtidState},
	} {
		t.Run(tc.name, func(t *testing.T) {
			idx, db := floorIndex(t, 1000)
			l := startLoop(t, idx, db, tc.state(t), time.Hour, "atomic-"+tc.name)
			for n := 1; n <= 2500; n++ {
				l.send(t, floorEvent(n, tc.gtid))
				if n%100 == 1 && n > 1 {
					if seen := countRows(t, db); seen != 0 {
						t.Fatalf("after %d of the transaction's 2500 rows were read, a reader sees %d of them; want 0", n-1, seen)
					}
				}
			}
			l.send(t, floorCommit(2501, tc.gtid))
			l.settle(t)
			if seen := countRows(t, db); seen != 2500 {
				t.Fatalf("after the commit a reader sees %d rows, want all 2500", seen)
			}
			l.finish(t)
		})
	}
}

// Many small transactions still share batches: batching across transaction
// ends is what keeps capture fast, and committing at every source commit
// would cost one round trip per transaction.
func TestIntegrationSmallTransactionsStillShareBatches_2391(t *testing.T) {
	idx, db := floorIndex(t, 1000)
	l := startLoop(t, idx, db, positionState(), time.Hour, "atomic-small")
	before := promtest.ToFloat64(l.m.BatchFlushes)
	for n := 1; n <= 3000; n++ {
		l.send(t, floorEvent(2*n, ""))
		l.send(t, floorCommit(2*n+1, ""))
	}
	l.finish(t)
	if got := countRows(t, db); got != 3000 {
		t.Fatalf("want 3000 rows indexed, got %d", got)
	}
	if inserts := promtest.ToFloat64(l.m.BatchFlushes) - before; inserts > 4 {
		t.Fatalf("3000 one-row transactions took %v INSERTs with batches of 1000; want at most 4", inserts)
	}
}

// Complete transactions that add up to more than one batch at a transaction
// end are written in several statements, inside one index transaction.
func TestIntegrationCompleteRowsOverOneBatchStillLand_2391(t *testing.T) {
	idx, db := floorIndex(t, 1000)
	l := startLoop(t, idx, db, positionState(), time.Hour, "atomic-chunks")
	n := 1
	for range 999 { // 999 one-row transactions: just under one batch
		l.send(t, floorEvent(n, ""))
		l.send(t, floorCommit(n+1, ""))
		n += 2
	}
	for range 999 { // then one transaction of 999 rows: 1998 complete rows at its end
		l.send(t, floorEvent(n, ""))
		n++
	}
	l.send(t, floorCommit(n, ""))
	l.settle(t)
	if got := countRows(t, db); got != 1998 {
		t.Fatalf("want all 1998 rows written at the transaction end, got %d", got)
	}
	l.finish(t)
}

// Capture stops while a transaction is half read and partly written: nothing
// of it is kept, everything before it is, and the checkpoint names the end of
// the last complete transaction.
func TestIntegrationStopInsideATransactionKeepsOnlyCompleteOnes_2391(t *testing.T) {
	idx, db := floorIndex(t, 1000)
	state := positionState()
	l := startLoop(t, idx, db, state, time.Hour, "atomic-stop")
	l.send(t, floorEvent(1, ""))
	l.send(t, floorEvent(2, ""))
	l.send(t, floorCommit(3, "")) // ends at 400
	for n := 4; n <= 1503; n++ {  // 1500 rows: one batch goes into the index transaction
		l.send(t, floorEvent(n, ""))
	}
	l.finish(t)
	if got := countRows(t, db); got != 2 {
		t.Fatalf("want only the complete transaction's 2 rows, got %d", got)
	}
	if saved := savedCheckpoint(t, db); saved.binlogPos != 400 {
		t.Fatalf("the checkpoint must name the end of the last complete transaction (400), got %d", saved.binlogPos)
	}
	var open int
	if err := db.QueryRow(`SELECT COUNT(*) FROM information_schema.innodb_trx t
		JOIN information_schema.processlist p ON p.id = t.trx_mysql_thread_id WHERE p.db = DATABASE()`).Scan(&open); err != nil {
		t.Fatal(err)
	}
	if open != 0 {
		t.Fatalf("%d index transactions are still open after the loop returned", open)
	}
}

// A checkpoint that comes due in the middle of a transaction is still saved,
// at the end of the last complete one, and does not make the open one visible.
func TestIntegrationCheckpointInsideATransactionDoesNotPublishIt_2391(t *testing.T) {
	idx, db := floorIndex(t, 1000)
	l := startLoop(t, idx, db, positionState(), 20*time.Millisecond, "atomic-tick")
	l.send(t, floorEvent(1, ""))
	l.send(t, floorCommit(2, "")) // ends at 300
	for n := 3; n <= 1502; n++ {
		l.send(t, floorEvent(n, ""))
	}
	time.Sleep(150 * time.Millisecond) // several ticks while the transaction is open
	l.settle(t)
	if got := countRows(t, db); got != 1 {
		t.Fatalf("a checkpoint inside the transaction left %d rows visible, want only the 1 before it", got)
	}
	if saved := savedCheckpoint(t, db); saved.binlogPos != 300 {
		t.Fatalf("the checkpoint saved inside the transaction must name 300, the end of the last complete one; got %d", saved.binlogPos)
	}
	l.send(t, floorCommit(1503, ""))
	l.settle(t)
	if got := countRows(t, db); got != 1501 {
		t.Fatalf("after the commit want 1501 rows, got %d", got)
	}
	l.finish(t)
}

// Past the row cap, the transaction is made visible in parts and the split is
// counted. The checkpoint still names its start, so a stop before its end
// leaves parts the resume cleanup takes back.
func TestIntegrationRowCapSplitsAndTheResumeTakesThePartsBack_2391(t *testing.T) {
	prev := maxTxnRows
	maxTxnRows = 2000
	t.Cleanup(func() { maxTxnRows = prev })

	idx, db := floorIndex(t, 1000)
	l := startLoop(t, idx, db, positionState(), time.Hour, "atomic-rowcap")
	splitsBefore := promtest.ToFloat64(l.m.TxnSplits)
	for n := 1; n <= 3000; n++ {
		l.send(t, floorEvent(n, ""))
	}
	l.settle(t)
	if got := countRows(t, db); got != 2000 {
		t.Fatalf("past the 2000-row cap want the first 2000 rows visible, got %d", got)
	}
	if splits := promtest.ToFloat64(l.m.TxnSplits) - splitsBefore; splits != 1 {
		t.Fatalf("want 1 split counted, got %v", splits)
	}
	l.finish(t) // stops inside the transaction
	saved := savedCheckpoint(t, db)
	if saved.binlogPos != 50 {
		t.Fatalf("a split must not move the checkpoint past the transaction's start (50), got %d", saved.binlogPos)
	}
	if _, err := deleteEventsSinceCheckpoint(db, saved.binlogFile, saved.binlogPos, saved.dedupFloorID); err != nil {
		t.Fatalf("resume cleanup: %v", err)
	}
	if got := countRows(t, db); got != 0 {
		t.Fatalf("the resume cleanup left %d rows of the split transaction", got)
	}
}

// Past the time cap, the open index transaction is committed by its own timer:
// not when the next row happens to arrive, nor at the next checkpoint tick,
// which can be much later than the cap.
func TestIntegrationTimeCapSplits_2391(t *testing.T) {
	prev := maxTxnOpen
	maxTxnOpen = 50 * time.Millisecond
	t.Cleanup(func() { maxTxnOpen = prev })

	t.Run("with no row and no tick", func(t *testing.T) {
		idx, db := floorIndex(t, 2)
		l := startLoop(t, idx, db, positionState(), time.Hour, "atomic-timecap-idle")
		l.send(t, floorEvent(1, ""))
		l.send(t, floorEvent(2, ""))
		deadline := time.Now().Add(5 * time.Second)
		for countRows(t, db) != 2 {
			if time.Now().After(deadline) {
				t.Fatal("nothing committed the open index transaction past its time cap")
			}
			time.Sleep(20 * time.Millisecond)
		}
	})
}

// CREATE TABLE ... SELECT logs its DDL, then its rows, then an XID, in ONE
// transaction, and in GTID mode the DDL already puts its GTID in the durable
// set. A resume would not read that transaction again, so a stop inside it
// must keep its rows: discarding them would lose them for good.
func TestIntegrationStopInsideCreateTableSelectKeepsItsRows_2391(t *testing.T) {
	idx, db := floorIndex(t, 1000)
	state := gtidState(t)
	l := startLoop(t, idx, db, state, time.Hour, "atomic-ctas")
	l.send(t, parser.Event{EventType: parser.EventGTID, BinlogFile: "binlog.000007", EndPos: 150, GTID: atomicGTID})
	l.send(t, parser.Event{EventType: parser.EventDDL, BinlogFile: "binlog.000007", EndPos: 250, GTID: atomicGTID,
		Schema: "testdb", Table: "orders"})
	l.send(t, floorEvent(3, ""))
	l.send(t, floorEvent(4, ""))
	l.finish(t) // stops before the XID
	if got := countRows(t, db); got != 2 {
		t.Fatalf("the CREATE ... SELECT rows must be kept when its GTID is already claimed; got %d rows, want 2", got)
	}
	saved := savedCheckpoint(t, db)
	if !strings.Contains(saved.gtidSet, "3f2a1c88-0000-0000-0000-000000002391") {
		t.Fatalf("the fixture must have claimed the DDL's GTID (saved set %q); it cannot discriminate", saved.gtidSet)
	}
}
