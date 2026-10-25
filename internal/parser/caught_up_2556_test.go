package parser

import (
	"context"
	"testing"
	"time"

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

// testHeartbeatPeriod is the period these tests tell the parser; a heartbeat
// counts only when the parser waited at least half of it before reading it.
const testHeartbeatPeriod = 200 * time.Millisecond

// feedLikeAQuietSource adds evs to the streamer from a goroutine and pauses a
// full heartbeat period before each heartbeat, as a source with nothing to
// send does, so the parser blocks in GetEvent before reading it. queued
// skips the pauses: every event, heartbeats included, is already waiting,
// as behind a backlog. Cancels when done.
func feedLikeAQuietSource(t *testing.T, streamer *replication.BinlogStreamer, cancel context.CancelFunc, queued bool, evs ...*replication.BinlogEvent) {
	t.Helper()
	if queued {
		feedThenCancel(t, streamer, cancel, evs...)
		return
	}
	go func() {
		defer cancel()
		for _, ev := range evs {
			if _, ok := ev.Event.(*replication.HeartbeatEvent); ok {
				time.Sleep(testHeartbeatPeriod)
			}
			if err := streamer.AddEventToStreamer(ev); err != nil {
				t.Errorf("AddEventToStreamer: %v", err)
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}()
}

func runCaughtUp(t *testing.T, sp *StreamParser, queued bool, evs ...*replication.BinlogEvent) []Event {
	t.Helper()
	sp.SetHeartbeatPeriod(testHeartbeatPeriod)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 64)
	ctx, cancel := context.WithCancel(context.Background())
	feedLikeAQuietSource(t, streamer, cancel, queued, evs...)
	if err := sp.Run(ctx, streamer, out); err != nil {
		t.Fatalf("Run: %v", err)
	}
	return drainAll(out)
}

func caughtUpMarkers(t *testing.T, evs ...*replication.BinlogEvent) int {
	t.Helper()
	return countMarkers(t, runCaughtUp(t, NewStreamParser(nil, Filters{}, nil), false, evs...))
}

func countMarkers(t *testing.T, evs []Event) int {
	t.Helper()
	n := 0
	for _, ev := range evs {
		if ev.EventType == EventCaughtUp {
			if ev.BinlogFile != "" || ev.EndPos != 0 || !ev.Timestamp.IsZero() {
				t.Errorf("a caught-up marker carries a position or a time (%q %d %v): the checkpoint must never move on it", ev.BinlogFile, ev.EndPos, ev.Timestamp)
			}
			n++
		}
	}
	return n
}

// A heartbeat already queued when the parser asks for it waited behind a
// backlog: changes the source made after sending it may sit behind it. Not
// caught up, even at the received position, even as a pair. And with no
// period known, never.
func TestCaughtUp_aQueuedHeartbeatDoesNotCount_2556(t *testing.T) {
	evs := []*replication.BinlogEvent{makeRotate("binlog.000003"), makeOtherAt(500),
		makeHeartbeat("binlog.000003", 500, false), makeHeartbeat("binlog.000003", 500, false)}
	if n := countMarkers(t, runCaughtUp(t, NewStreamParser(nil, Filters{}, nil), true, evs...)); n != 0 {
		t.Fatalf("queued heartbeats: %d markers, want 0", n)
	}
	sp := NewStreamParser(nil, Filters{}, nil)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 64)
	ctx, cancel := context.WithCancel(context.Background())
	feedLikeAQuietSource(t, streamer, cancel, false, evs...)
	if err := sp.Run(ctx, streamer, out); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if n := countMarkers(t, drainAll(out)); n != 0 {
		t.Fatalf("no heartbeat period set: %d markers, want 0", n)
	}
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
	n := countMarkers(t, runCaughtUp(t, NewStreamParser(makeOrdersResolver(), Filters{}, nil), false,
		artificialRotate("binlog.000001", 4),
		makeGTIDEvent(1), makeQueryEvent("BEGIN"), makeOrdersInsertEvent(1, 10), makeXIDEvent(300),
		artificialRotate("binlog.000001", 4),
		makeGTIDEvent(1), makeQueryEvent("BEGIN"),
		makeHeartbeat("binlog.000001", 900, false), makeHeartbeat("binlog.000001", 900, false)))
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

// A heartbeat held up on the way (a network stall, a paused process) is read
// long after the source sent it, and changes made after it may be right
// behind. Its marker time is capped at the previous packet's read time plus
// one period: no later than the source can have sent it.
func TestCaughtUp_aStalledHeartbeatIsCappedAtWhenItWasSent_2556(t *testing.T) {
	sp := NewStreamParser(nil, Filters{}, nil)
	sp.SetHeartbeatPeriod(testHeartbeatPeriod)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 64)
	ctx, cancel := context.WithCancel(context.Background())
	var lastEventFed, heartbeatFed time.Time
	go func() {
		defer cancel()
		for _, ev := range []*replication.BinlogEvent{makeRotate("binlog.000003"), makeOtherAt(500)} {
			if err := streamer.AddEventToStreamer(ev); err != nil {
				t.Errorf("AddEventToStreamer: %v", err)
				return
			}
		}
		lastEventFed = time.Now()
		time.Sleep(4 * testHeartbeatPeriod) // the stall: 3 periods late
		heartbeatFed = time.Now()
		if err := streamer.AddEventToStreamer(makeHeartbeat("binlog.000003", 500, false)); err != nil {
			t.Errorf("AddEventToStreamer: %v", err)
			return
		}
		time.Sleep(50 * time.Millisecond)
	}()
	if err := sp.Run(ctx, streamer, out); err != nil {
		t.Fatalf("Run: %v", err)
	}
	var marker *Event
	for _, ev := range drainAll(out) {
		if ev.EventType == EventCaughtUp {
			marker = &ev
		}
	}
	if marker == nil {
		t.Fatal("no marker for a heartbeat at the received position")
	}
	if !marker.ReadAt.Before(heartbeatFed) {
		t.Fatalf("marker time %v is not before the stalled heartbeat's arrival %v: it would claim changes still on the way", marker.ReadAt, heartbeatFed)
	}
	if limit := lastEventFed.Add(2 * testHeartbeatPeriod); marker.ReadAt.After(limit) {
		t.Fatalf("marker time %v is past the previous packet's read time plus one period (%v)", marker.ReadAt, limit)
	}
}
