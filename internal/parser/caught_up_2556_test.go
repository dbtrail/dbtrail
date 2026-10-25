package parser

import (
	"context"
	"testing"

	"github.com/go-mysql-org/go-mysql/replication"
)

// #2556: the source's replication heartbeat, sent when it has nothing newer
// to send, becomes an EventCaughtUp marker when it shows the stream has
// received every event the source had. Nothing is written to the source: the
// heartbeat is a message of the replication protocol itself.

// makeHeartbeat is a HEARTBEAT_EVENT (v1: the file in the payload, the
// position in the header) or a HEARTBEAT_LOG_EVENT_V2 (both in the payload,
// the header's position zero).
func makeHeartbeat(file string, pos uint32, v2 bool) *replication.BinlogEvent {
	if v2 {
		return &replication.BinlogEvent{
			Header: &replication.EventHeader{EventType: replication.HEARTBEAT_LOG_EVENT_V2},
			Event:  &replication.HeartbeatEvent{Version: 2, Filename: file, Offset: uint64(pos)},
		}
	}
	return &replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.HEARTBEAT_EVENT, LogPos: pos},
		Event:  &replication.HeartbeatEvent{Version: 1, Filename: file},
	}
}

// makeOtherAt is an event the parser does not turn into anything, ending at
// pos: it only moves the received position, like a row event of a table
// capture filters out.
func makeOtherAt(pos uint32) *replication.BinlogEvent {
	return &replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.PREVIOUS_GTIDS_EVENT, LogPos: pos},
		Event:  &replication.PreviousGTIDsEvent{},
	}
}

func caughtUpMarkers(t *testing.T, evs ...*replication.BinlogEvent) int {
	t.Helper()
	sp := NewStreamParser(nil, Filters{}, nil)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 64)
	ctx, cancel := context.WithCancel(context.Background())
	feedThenCancel(t, streamer, cancel, evs...)
	if err := sp.Run(ctx, streamer, out); err != nil {
		t.Fatalf("Run: %v", err)
	}
	close(out)
	n := 0
	for ev := range out {
		if ev.EventType == EventCaughtUp {
			if ev.BinlogFile != "" || ev.EndPos != 0 || !ev.Timestamp.IsZero() {
				t.Errorf("a caught-up marker carries a position or a time (%q %d %v): the checkpoint must never move on it", ev.BinlogFile, ev.EndPos, ev.Timestamp)
			}
			n++
		}
	}
	return n
}

func TestCaughtUp_heartbeatAtTheReceivedPosition_2556(t *testing.T) {
	if n := caughtUpMarkers(t, makeRotate("binlog.000003"), makeOtherAt(500), makeHeartbeat("binlog.000003", 500, false)); n != 1 {
		t.Fatalf("heartbeat at the received position: %d markers, want 1", n)
	}
}

func TestCaughtUp_heartbeatV2ReadsItsOwnPosition_2556(t *testing.T) {
	if n := caughtUpMarkers(t, makeRotate("binlog.000003"), makeOtherAt(500), makeHeartbeat("binlog.000003", 500, true)); n != 1 {
		t.Fatalf("v2 heartbeat at the received position: %d markers, want 1", n)
	}
	if n := caughtUpMarkers(t, makeRotate("binlog.000003"), makeOtherAt(500), makeHeartbeat("binlog.000003", 900, true)); n != 0 {
		t.Fatalf("v2 heartbeat ahead of the received position: %d markers, want 0", n)
	}
}

// A heartbeat ahead of what was received is the source skipping events this
// stream already has (a reconnect under GTID auto-position): not caught up.
// Two in a row at the same place, nothing in between, is the source idle at
// the end of its binlog: caught up.
func TestCaughtUp_aheadThenIdle_2556(t *testing.T) {
	if n := caughtUpMarkers(t, makeRotate("binlog.000003"), makeOtherAt(500), makeHeartbeat("binlog.000003", 900, false)); n != 0 {
		t.Fatalf("one heartbeat ahead: %d markers, want 0", n)
	}
	if n := caughtUpMarkers(t, makeRotate("binlog.000003"), makeOtherAt(500),
		makeHeartbeat("binlog.000003", 900, false), makeHeartbeat("binlog.000003", 900, false)); n != 1 {
		t.Fatalf("two heartbeats at the same place ahead: %d markers, want 1", n)
	}
}

// While the source skips a backlog its heartbeats move: never caught up.
func TestCaughtUp_movingHeartbeatsAreSkipping_2556(t *testing.T) {
	if n := caughtUpMarkers(t, makeRotate("binlog.000003"), makeOtherAt(500),
		makeHeartbeat("binlog.000003", 900, false), makeHeartbeat("binlog.000003", 1400, false),
		makeHeartbeat("binlog.000003", 2000, false)); n != 0 {
		t.Fatalf("moving heartbeats: %d markers, want 0", n)
	}
}

// An event between two heartbeats at the same place breaks the pair.
func TestCaughtUp_anEventBetweenBreaksThePair_2556(t *testing.T) {
	if n := caughtUpMarkers(t, makeRotate("binlog.000003"), makeOtherAt(500),
		makeHeartbeat("binlog.000003", 900, false), makeOtherAt(950), makeHeartbeat("binlog.000003", 900, false)); n != 0 {
		t.Fatalf("an event between: %d markers, want 0", n)
	}
}

func TestCaughtUp_anotherFile_2556(t *testing.T) {
	if n := caughtUpMarkers(t, makeRotate("binlog.000003"), makeOtherAt(500), makeHeartbeat("binlog.000004", 500, false)); n != 0 {
		t.Fatalf("heartbeat for another file: %d markers, want 0", n)
	}
}

// Before anything was received there is no position to compare with, and an
// unnamed file is never a pair.
func TestCaughtUp_nothingReceivedYet_2556(t *testing.T) {
	if n := caughtUpMarkers(t, makeHeartbeat("binlog.000003", 500, false)); n != 0 {
		t.Fatalf("heartbeat before any event: %d markers, want 0", n)
	}
	if n := caughtUpMarkers(t, makeHeartbeat("", 0, false), makeHeartbeat("", 0, false)); n != 0 {
		t.Fatalf("unnamed heartbeats: %d markers, want 0", n)
	}
}

// While the parser drops a transaction the source re-sends after a
// reconnect, the stream is mid-way through it: never caught up, even on a
// pair of idle heartbeats.
func TestCaughtUp_notWhileSkippingAResentTransaction_2556(t *testing.T) {
	sp := NewStreamParser(makeOrdersResolver(), Filters{}, nil)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 64)
	ctx, cancel := context.WithCancel(context.Background())
	feedThenCancel(t, streamer, cancel,
		artificialRotate("binlog.000001", 4),
		makeGTIDEvent(1), makeQueryEvent("BEGIN"), makeOrdersInsertEvent(1, 10), makeXIDEvent(300),
		artificialRotate("binlog.000001", 4),
		makeGTIDEvent(1), makeQueryEvent("BEGIN"),
		makeHeartbeat("binlog.000001", 900, false), makeHeartbeat("binlog.000001", 900, false))
	if err := sp.Run(ctx, streamer, out); err != nil {
		t.Fatalf("Run: %v", err)
	}
	n := 0
	for _, ev := range drainAll(out) {
		if ev.EventType == EventCaughtUp {
			n++
		}
	}
	if n != 0 {
		t.Fatalf("idle heartbeats inside a re-sent transaction: %d markers, want 0", n)
	}
}

// Right after a rotate nothing was received in the new file yet: a heartbeat
// naming it at position zero is no proof of anything.
func TestCaughtUp_nothingReceivedInTheFileYet_2556(t *testing.T) {
	if n := caughtUpMarkers(t, makeRotate("binlog.000003"), makeHeartbeat("binlog.000003", 0, false)); n != 0 {
		t.Fatalf("heartbeat at zero right after a rotate: %d markers, want 0", n)
	}
}
