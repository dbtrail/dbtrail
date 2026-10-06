package parser

import (
	"context"
	"testing"

	"github.com/go-mysql-org/go-mysql/replication"
)

func makeFDE(ts, serverID uint32) *replication.BinlogEvent {
	return &replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.FORMAT_DESCRIPTION_EVENT, Timestamp: ts, ServerID: serverID},
		Event:  &replication.FormatDescriptionEvent{},
	}
}

func TestBinlogFileIdentity(t *testing.T) {
	for _, c := range []struct {
		name string
		hdr  *replication.EventHeader
		want string
	}{
		{"nil header", nil, ""},
		{"no timestamp", &replication.EventHeader{ServerID: 1}, ""},
		{"timestamp and server", &replication.EventHeader{Timestamp: 1791271554, ServerID: 1}, "fde:1791271554:1"},
		{"another server, same second", &replication.EventHeader{Timestamp: 1791271554, ServerID: 2}, "fde:1791271554:2"},
	} {
		if got := BinlogFileIdentity(c.hdr); got != c.want {
			t.Errorf("%s: BinlogFileIdentity = %q, want %q", c.name, got, c.want)
		}
	}
}

// TestStreamParser_fileOpenedHook: every file the stream enters is reported
// with the identity of its FORMAT_DESCRIPTION event, under the name the rotate
// before it gave, and nothing is reported without a name or a timestamp.
func TestStreamParser_fileOpenedHook(t *testing.T) {
	type call struct{ file, id string }
	var got []call
	sp := NewStreamParser(nil, Filters{}, nil)
	sp.SetFileOpenedHook(func(file, id string) { got = append(got, call{file, id}) })
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 10)
	ctx, cancel := context.WithCancel(context.Background())
	feedThenCancel(t, streamer, cancel,
		makeFDE(50, 1), // no rotate yet: no file name, not reported
		makeRotate("binlog.000003"),
		makeFDE(100, 1),
		makeRotate("binlog.000004"),
		makeFDE(0, 1), // no timestamp: not reported
		makeRotate("binlog.000005"),
		makeFDE(300, 7),
	)
	if err := sp.Run(ctx, streamer, out); err != nil {
		t.Fatalf("Run: %v", err)
	}
	want := []call{{"binlog.000003", "fde:100:1"}, {"binlog.000005", "fde:300:7"}}
	if len(got) != len(want) {
		t.Fatalf("hook calls = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("hook call %d = %v, want %v", i, got[i], want[i])
		}
	}
}

// TestStreamParser_noFileOpenedHook: without a hook, an FDE is harmless.
func TestStreamParser_noFileOpenedHook(t *testing.T) {
	sp := NewStreamParser(nil, Filters{}, nil)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 10)
	ctx, cancel := context.WithCancel(context.Background())
	feedThenCancel(t, streamer, cancel, makeRotate("binlog.000003"), makeFDE(100, 1))
	if err := sp.Run(ctx, streamer, out); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(out) != 0 {
		t.Errorf("an FDE emitted %d events, want none", len(out))
	}
}
