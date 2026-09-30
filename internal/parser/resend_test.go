package parser

import (
	"context"
	"testing"

	"github.com/go-mysql-org/go-mysql/replication"
)

// After a transparent reconnect in GTID mode, go-mysql resumes from the set it
// had BEFORE the last GTID event it read (replication.BinlogSyncer.retrySync),
// so the source sends that last transaction again. When the parser had already
// seen that transaction's commit, indexing it again would duplicate its rows
// forever. These tests pin the guard: a re-sent transaction that was already
// committed is dropped, and nothing else ever is.

// runResend feeds evs through a StreamParser for shop.orders and returns what
// it emitted.
func runResend(t *testing.T, flavor string, evs ...*replication.BinlogEvent) []Event {
	t.Helper()
	sp := NewStreamParser(makeOrdersResolver(), Filters{}, nil)
	if flavor != "" {
		sp.SetFlavor(flavor)
	}
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 64)
	ctx, cancel := context.WithCancel(context.Background())
	feedThenCancel(t, streamer, cancel, evs...)
	if err := sp.Run(ctx, streamer, out); err != nil {
		t.Fatalf("Run: %v", err)
	}
	return drainAll(out)
}

// insertAt is makeOrdersInsertEvent ending at logPos, for sequences that must
// keep positions increasing within one file.
func insertAt(id int64, logPos uint32) *replication.BinlogEvent {
	ev := makeOrdersInsertEvent(id, 10)
	ev.Header.LogPos = logPos
	return ev
}

func idsOf(evs []Event) []any {
	var ids []any
	for _, ev := range dmlOf(evs) {
		ids = append(ids, ev.RowAfter["id"])
	}
	return ids
}

func commitsOf(evs []Event) []string {
	var gtids []string
	for _, ev := range evs {
		if ev.EventType == EventCommit {
			gtids = append(gtids, ev.GTID)
		}
	}
	return gtids
}

func equalAny(a, b []any) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestStreamParser_resentCommittedTransactionDropped_mysql(t *testing.T) {
	evs := runResend(t, "",
		artificialRotate("binlog.000001", 4),
		makeGTIDEvent(1), makeQueryEvent("BEGIN"), makeOrdersInsertEvent(1, 10), makeXIDEvent(300),
		// reconnect: the source re-sends GTID 1, then carries on with 2
		artificialRotate("binlog.000001", 4),
		makeGTIDEvent(1), makeQueryEvent("BEGIN"), makeOrdersInsertEvent(1, 10), makeXIDEvent(300),
		makeGTIDEvent(2), makeQueryEvent("BEGIN"), insertAt(2, 350), makeXIDEvent(400),
	)
	if got := idsOf(evs); !equalAny(got, []any{int64(1), int64(2)}) {
		t.Errorf("rows = %v, want [1 2] (the re-sent transaction must not be indexed twice)", got)
	}
	if got := commitsOf(evs); len(got) != 2 {
		t.Errorf("commits = %v, want one per transaction", got)
	}
}

func TestStreamParser_resentCommittedTransactionDropped_mariadb(t *testing.T) {
	evs := runResend(t, "mariadb",
		artificialRotate("mariadb-bin.000001", 4),
		makeMariadbGTIDEvent(0, 1, 7), makeOrdersInsertEvent(1, 10), makeXIDEvent(300),
		artificialRotate("mariadb-bin.000001", 4),
		makeMariadbGTIDEvent(0, 1, 7), makeOrdersInsertEvent(1, 10), makeXIDEvent(300),
		makeMariadbGTIDEvent(0, 1, 8), insertAt(2, 350), makeXIDEvent(400),
	)
	if got := idsOf(evs); !equalAny(got, []any{int64(1), int64(2)}) {
		t.Errorf("rows = %v, want [1 2]", got)
	}
	if got := commitsOf(evs); len(got) != 2 || got[0] != "0-1-7" || got[1] != "0-1-8" {
		t.Errorf("commits = %v, want [0-1-7 0-1-8]", got)
	}
}

// A transaction cut by the disconnect (no commit seen) is NOT committed, so its
// re-send is the only complete copy and must pass through.
func TestStreamParser_resentUncommittedTransactionKept(t *testing.T) {
	evs := runResend(t, "",
		artificialRotate("binlog.000001", 4),
		makeGTIDEvent(1), makeQueryEvent("BEGIN"), makeOrdersInsertEvent(1, 10),
		artificialRotate("binlog.000001", 4),
		makeGTIDEvent(1), makeQueryEvent("BEGIN"), makeOrdersInsertEvent(1, 10), makeXIDEvent(300),
	)
	if got := commitsOf(evs); len(got) != 1 {
		t.Fatalf("commits = %v, want the re-sent transaction committed once", got)
	}
	if got := idsOf(evs); len(got) != 2 {
		t.Errorf("rows = %v, want the re-sent copy emitted (the cut copy was never committed)", got)
	}
}

// A reconnect that resumes exactly after the last commit (next GTID is new)
// drops nothing.
func TestStreamParser_reconnectWithoutResendKeepsEverything(t *testing.T) {
	evs := runResend(t, "mariadb",
		artificialRotate("mariadb-bin.000001", 4),
		makeMariadbGTIDEvent(0, 1, 7), makeOrdersInsertEvent(1, 10), makeXIDEvent(300),
		artificialRotate("mariadb-bin.000001", 4),
		makeMariadbGTIDEvent(0, 1, 8), makeOrdersInsertEvent(2, 20), makeXIDEvent(400),
	)
	if got := idsOf(evs); !equalAny(got, []any{int64(1), int64(2)}) {
		t.Errorf("rows = %v, want [1 2]", got)
	}
}

// The same GTID with no reconnect in between is never a re-send the guard
// acts on: it only looks at the first transaction after a reconnect.
func TestStreamParser_repeatWithoutReconnectNotDropped(t *testing.T) {
	evs := runResend(t, "mariadb",
		makeMariadbGTIDEvent(0, 1, 7), makeOrdersInsertEvent(1, 10), makeXIDEvent(300),
		makeMariadbGTIDEvent(0, 1, 7), insertAt(1, 450), makeXIDEvent(500),
	)
	if got := idsOf(evs); len(got) != 2 {
		t.Errorf("rows = %v, want both copies (no reconnect happened)", got)
	}
}

// A re-sent DDL transaction is dropped too: it would re-run the DDL hook and
// record the schema change twice.
func TestStreamParser_resentCommittedDDLDropped(t *testing.T) {
	sp := NewStreamParser(makeOrdersResolver(), Filters{}, nil)
	hookRuns := 0
	sp.SetSyncDDLHook(func(Event) error { hookRuns++; return nil })
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 64)
	ctx, cancel := context.WithCancel(context.Background())
	feedThenCancel(t, streamer, cancel,
		artificialRotate("binlog.000001", 4),
		makeGTIDEvent(1), makeQueryEventWithSchema("shop", "ALTER TABLE orders ADD COLUMN note INT"),
		artificialRotate("binlog.000001", 4),
		makeGTIDEvent(1), makeQueryEventWithSchema("shop", "ALTER TABLE orders ADD COLUMN note INT"),
	)
	if err := sp.Run(ctx, streamer, out); err != nil {
		t.Fatalf("Run: %v", err)
	}
	ddl := 0
	for _, ev := range drainAll(out) {
		if ev.EventType == EventDDL {
			ddl++
		}
	}
	if ddl != 1 || hookRuns != 1 {
		t.Errorf("DDL events = %d, hook runs = %d, want 1 and 1", ddl, hookRuns)
	}
}

// A compressed transaction (Transaction_payload) re-sent after a reconnect is
// dropped as a whole.
func TestStreamParser_resentCommittedPayloadDropped(t *testing.T) {
	payload := func(id int64) *replication.BinlogEvent {
		ins := makeOrdersInsertEvent(id, 10)
		ins.Header.LogPos = 0
		return makePayloadEvent(500, 300,
			&replication.BinlogEvent{Header: &replication.EventHeader{EventType: replication.QUERY_EVENT}, Event: &replication.QueryEvent{Query: []byte("BEGIN")}},
			ins,
			&replication.BinlogEvent{Header: &replication.EventHeader{EventType: replication.XID_EVENT}, Event: &replication.XIDEvent{XID: 1}},
		)
	}
	evs := runResend(t, "",
		artificialRotate("binlog.000001", 4),
		makeGTIDEvent(1), payload(1),
		artificialRotate("binlog.000001", 4),
		makeGTIDEvent(1), payload(1),
		makeGTIDEvent(2), payload(2),
	)
	if got := idsOf(evs); !equalAny(got, []any{int64(1), int64(2)}) {
		t.Errorf("rows = %v, want [1 2]", got)
	}
}
