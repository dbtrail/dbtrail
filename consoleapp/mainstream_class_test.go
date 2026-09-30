package consoleapp

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/streamrun"
	"github.com/dbtrail/dbtrail/internal/telemetry"
	"github.com/dbtrail/dbtrail/internal/testutil/fakemysql"
)

// realIndexRefusal is the error the real batch INSERT returns when the index
// server answers with the given error number.
func realIndexRefusal(t *testing.T, number uint16) error {
	t.Helper()
	db, err := sql.Open("mysql", fakemysql.RefuseEverything(t, number))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	_, err = indexer.New(db, 1).InsertBatch(nil)
	if err == nil {
		t.Fatal("setup: the refused write must fail")
	}
	return err
}

// #1630 gave the errors below a usage-telemetry class. The class is a label
// on the way out and nothing else: which failures restart capture is decided
// by errors.Is(err, indexer.ErrWriteDeadline) alone. A lock wait timeout now
// shares the write deadline's class, so this pins that sharing a class does
// not mean sharing the restart. Each error comes from the real write path.
//
// Must not t.Parallel(): mutates the monitor policy globals.
func TestRunMainStream_classedErrorsStillDoNotRestart(t *testing.T) {
	shrinkMonitorBackoff(t)
	prev := mainStreamFn
	t.Cleanup(func() { mainStreamFn = prev })

	for _, c := range []struct {
		name   string
		number uint16
		class  string
	}{
		{"lock wait timeout", 1205, telemetry.ClassDBConnection},
		{"deadlock", 1213, telemetry.ClassDBConnection},
		{"too many connections", 1040, telemetry.ClassDBConnection},
		{"table is full", 1114, telemetry.ClassStorageIO},
	} {
		t.Run(c.name, func(t *testing.T) {
			want := realIndexRefusal(t, c.number)
			if got := telemetry.ClassifyError(want); got != c.class {
				t.Fatalf("ClassifyError = %q, want %q", got, c.class)
			}
			calls := 0
			mainStreamFn = func(ctx context.Context, cfg streamrun.Config) error {
				calls++
				return want
			}
			err := runMainStreamWithWriteDeadlineRetry(context.Background(), streamrun.Config{})
			if err != want {
				t.Fatalf("the error must surface unchanged, got: %v", err)
			}
			if calls != 1 {
				t.Fatalf("stream ran %d times; only the write deadline restarts capture", calls)
			}
		})
	}
}

// The other half: the write deadline still matches the sentinel the restart
// keys on, and still reports the class it reported before #1630.
//
// Must not t.Parallel(): mutates indexer.WriteTimeout.
func TestWriteDeadline_restartKeyAndClassUnchanged(t *testing.T) {
	err := realWriteDeadlineError(t)
	if !errors.Is(err, indexer.ErrWriteDeadline) {
		t.Fatalf("errors.Is(err, indexer.ErrWriteDeadline) = false: %v", err)
	}
	if got := telemetry.ClassifyError(err); got != telemetry.ClassDBConnection {
		t.Errorf("ClassifyError = %q, want %q", got, telemetry.ClassDBConnection)
	}
}

// A transaction cut by a disconnect is restarted by streamrun.One itself; when
// One gives up after its quick restarts, the main source of watch still rides
// it out under the daemon's crash-loop policy instead of stopping the whole
// daemon (console and every capture with it).
//
// Must not t.Parallel(): mutates the monitor policy globals.
func TestRunMainStream_cutTransactionRestarts(t *testing.T) {
	shrinkMonitorBackoff(t)
	prev := mainStreamFn
	t.Cleanup(func() { mainStreamFn = prev })
	calls := 0
	mainStreamFn = func(ctx context.Context, cfg streamrun.Config) error {
		calls++
		if calls == 1 {
			return fmt.Errorf("gave up: %w", &parser.ResentCutTransactionError{GTID: "0-1-7", Rows: 2})
		}
		return nil
	}
	if err := runMainStreamWithWriteDeadlineRetry(context.Background(), streamrun.Config{}); err != nil || calls != 2 {
		t.Fatalf("err=%v runs=%d, want the stream restarted once and then fine", err, calls)
	}
}
