package streamrun

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/observe"
	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/telemetry"
	"github.com/dbtrail/dbtrail/internal/testutil/fakemysql"
)

// TestGapRefusedErrorIsClassed: the --no-gap-fill refusal keeps the message
// the integration test pins (naming the flag) and classifies as
// binlog_not_found — the same bucket as the server's own 1236 (#1503).
func TestGapRefusedErrorIsClassed(t *testing.T) {
	err := &GapRefusedError{msg: "binlog mysql-bin.000042 purged on db.internal"}
	if !strings.HasPrefix(err.Error(), "binlog gap detected and --no-gap-fill is set: ") || !strings.Contains(err.Error(), "000042") {
		t.Errorf("message = %q", err.Error())
	}
	if got := telemetry.ClassifyError(fmt.Errorf("stream: %w", err)); got != telemetry.ClassBinlogNotFound {
		t.Errorf("ClassifyError = %q, want %q", got, telemetry.ClassBinlogNotFound)
	}
}

// oneRowEvent is a single committed INSERT, enough to make streamLoop flush.
func oneRowEvent() []parser.Event {
	return []parser.Event{{
		BinlogFile: "binlog.000001", StartPos: 4, EndPos: 120,
		Schema: "shop", Table: "orders", PKValues: "1",
		EventType: parser.EventInsert,
		Timestamp: time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC),
		RowAfter:  map[string]any{"id": 1},
	}}
}

// runStreamLoopAgainst drives the real streamLoop, fed one row event and then
// a closed channel, against the index at dsn, and returns what it returned.
func runStreamLoopAgainst(t *testing.T, dsn string) error {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	evs := oneRowEvent()
	ch := make(chan parser.Event, len(evs))
	for _, ev := range evs {
		ch <- ev
	}
	close(ch)
	return streamLoop(context.Background(), ch, indexer.New(db, 100), db, time.Hour,
		&streamState{mode: "position"}, observe.ForSource("test-1630"), nil)
}

// TestStreamLoop_indexServerErrorIsClassed is the #1630 wiring test for the
// capture loop: what streamrun.One returns when the index refuses a flush
// after capture started must carry a class. The error is the one the real
// loop returned from the real flush against a server answering with that
// number, not a value built here.
func TestStreamLoop_indexServerErrorIsClassed(t *testing.T) {
	for _, c := range []struct {
		name   string
		number uint16
		want   string
	}{
		{"table is full", 1114, telemetry.ClassStorageIO},
		{"too many connections", 1040, telemetry.ClassDBConnection},
		{"deadlock", 1213, telemetry.ClassDBConnection},
		{"no partition for value", 1526, telemetry.ClassNotFound},
		{"packet too large", 1153, telemetry.ClassConfigInvalid},
		{"no bucket", 1064, telemetry.ClassUnknown},
	} {
		t.Run(c.name, func(t *testing.T) {
			err := runStreamLoopAgainst(t, fakemysql.RefuseEverything(t, c.number))
			if err == nil {
				t.Fatal("the refused flush must end the loop with an error")
			}
			var drv *mysql.MySQLError
			if !errors.As(err, &drv) || drv.Number != c.number {
				t.Fatalf("setup: want the driver's *MySQLError %d, got %T: %v", c.number, err, err)
			}
			if errors.Is(err, indexer.ErrWriteDeadline) {
				t.Errorf("a server refusal must not read as a write deadline (it would restart capture): %v", err)
			}
			if got := telemetry.ClassifyError(err); got != c.want {
				t.Errorf("ClassifyError(%d) = %q, want %q", c.number, got, c.want)
			}
		})
	}
}

// TestStreamLoop_writeDeadlineKeepsIdentityAndClass: the write deadline
// leaves the loop still matching indexer.ErrWriteDeadline, which is what the
// console's restart policy keys on, and still reporting db_connection. Must
// NOT call t.Parallel(): it mutates indexer.WriteTimeout.
func TestStreamLoop_writeDeadlineKeepsIdentityAndClass(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		var held []net.Conn
		for {
			c, err := ln.Accept()
			if err != nil {
				for _, h := range held {
					h.Close()
				}
				return
			}
			held = append(held, c) // hold open, never respond
		}
	}()
	prev := indexer.WriteTimeout
	indexer.WriteTimeout = 250 * time.Millisecond
	t.Cleanup(func() { indexer.WriteTimeout = prev })

	err = runStreamLoopAgainst(t, "root:x@tcp("+ln.Addr().String()+")/db?timeout=30s")
	if err == nil {
		t.Fatal("the stalled flush must end the loop with an error")
	}
	if !errors.Is(err, indexer.ErrWriteDeadline) {
		t.Errorf("errors.Is(err, indexer.ErrWriteDeadline) = false: %v", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("errors.Is(err, context.DeadlineExceeded) = false: %v", err)
	}
	if got := telemetry.ClassifyError(err); got != telemetry.ClassDBConnection {
		t.Errorf("ClassifyError = %q, want %q", got, telemetry.ClassDBConnection)
	}
}
