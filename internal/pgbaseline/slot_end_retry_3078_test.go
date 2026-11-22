package pgbaseline

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/pgcapture"
)

// freshConn stands for the new connection checkSlotOnce opens; never used
// as a connection, only told apart from the anchor's (nil here).
var freshConn = &pgx.Conn{}

// stubSlotEnd makes checkSlotOnce's two calls answer from the test (connect
// nil = the new connection opens), and counts the asks of the slot.
func stubSlotEnd(t *testing.T, connect error, check func(conn *pgx.Conn) error) *int {
	t.Helper()
	prevC, prevK, prevA, prevP := connectSlotCheck, checkSlotStillAnchors, slotEndAttempts, slotEndPause
	t.Cleanup(func() {
		connectSlotCheck, checkSlotStillAnchors, slotEndAttempts, slotEndPause = prevC, prevK, prevA, prevP
	})
	slotEndPause = 10 * time.Millisecond
	asks := 0
	connectSlotCheck = func(context.Context, string) (*pgx.Conn, func(), error) {
		if connect != nil {
			return nil, nil, connect
		}
		return freshConn, func() {}, nil
	}
	checkSlotStillAnchors = func(_ context.Context, conn *pgx.Conn, _ string, _ pglogrepl.LSN) error {
		asks++
		return check(conn)
	}
	return &asks
}

// The slot asked about once the tables are read (#3078): a read that failed
// is asked again a few times, so a one-second cut does not throw hours of
// reading away; a verdict is returned at once.
func TestCheckSlotAtEnd_RetriesOnlyAFailedRead_3078(t *testing.T) {
	cut := pgcapture.SlotReadFailed(errors.New("pgcapture: checking replication slot \"s\": unexpected EOF"))
	gone := errors.New(`pgcapture: replication slot "s" no longer exists: it was removed while the snapshot was read`)
	ctx := context.Background()
	cfg := Config{SlotName: "s", QueryDSN: "postgres://x"}

	t.Run("a cut, then the slot as it should be", func(t *testing.T) {
		n := 0
		asks := stubSlotEnd(t, nil, func(conn *pgx.Conn) error {
			if conn != freshConn {
				t.Error("asked on the anchor connection while a new one opened")
			}
			if n++; n < 3 {
				return cut
			}
			return nil
		})
		if err := checkSlotAtEnd(ctx, cfg, nil, 1); err != nil || *asks != 3 {
			t.Fatalf("err = %v after %d asks, want the third ask to go through", err, *asks)
		}
	})
	t.Run("a verdict is not asked again", func(t *testing.T) {
		asks := stubSlotEnd(t, nil, func(*pgx.Conn) error { return gone })
		if err := checkSlotAtEnd(ctx, cfg, nil, 1); !errors.Is(err, gone) || *asks != 1 {
			t.Fatalf("err = %v after %d asks, want the verdict at the first", err, *asks)
		}
	})
	t.Run("a read that never goes through", func(t *testing.T) {
		asks := stubSlotEnd(t, nil, func(*pgx.Conn) error { return cut })
		err := checkSlotAtEnd(ctx, cfg, nil, 1)
		if !errors.Is(err, cut) || *asks != 3 || !strings.Contains(err.Error(), "3 attempts") {
			t.Fatalf("err = %v after %d asks, want the failed read after 3", err, *asks)
		}
	})
	t.Run("no new connection, and the anchor connection cannot read either", func(t *testing.T) {
		refused := errors.New("dial tcp: connection refused")
		closed := pgcapture.SlotReadFailed(errors.New("pgcapture: checking replication slot \"s\": conn closed"))
		asks := stubSlotEnd(t, refused, func(conn *pgx.Conn) error {
			if conn == freshConn {
				t.Error("asked on a new connection that could not be opened")
			}
			return closed
		})
		err := checkSlotAtEnd(ctx, cfg, nil, 1)
		if !errors.Is(err, refused) || !errors.Is(err, closed) || *asks != 3 {
			t.Fatalf("err = %v after %d asks, want both causes, after 3", err, *asks)
		}
		for _, want := range []string{"connection refused", "conn closed"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("the error does not say %q: %v", want, err)
			}
		}
	})
	t.Run("no new connection, and the anchor connection has the verdict", func(t *testing.T) {
		asks := stubSlotEnd(t, errors.New("dial tcp: connection refused"), func(*pgx.Conn) error { return gone })
		if err := checkSlotAtEnd(ctx, cfg, nil, 1); !errors.Is(err, gone) || *asks != 1 {
			t.Fatalf("err = %v after %d asks, want the verdict at once", err, *asks)
		}
	})
}
