package indexer

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/telemetry"
	"github.com/dbtrail/dbtrail/internal/testutil/fakemysql"
)

// TestInsertBatch_indexServerErrorsAreClassed is the #1630 wiring test for the
// index side: what an unwell index server answers to the batch INSERT must
// reach usage telemetry with a class, not as unknown. Each case drives the
// real InsertBatch against a server that answers with that error number, so
// the error under test is the one the driver built from the wire and
// InsertBatch wrapped, not a value assembled here.
func TestInsertBatch_indexServerErrorsAreClassed(t *testing.T) {
	cases := []struct {
		name   string
		number uint16
		want   string
	}{
		{"disk full", 1021, telemetry.ClassStorageIO},
		{"table is full", 1114, telemetry.ClassStorageIO},
		{"too many connections", 1040, telemetry.ClassDBConnection},
		{"too many user connections", 1203, telemetry.ClassDBConnection},
		{"lock wait timeout", 1205, telemetry.ClassDBConnection},
		{"deadlock", 1213, telemetry.ClassDBConnection},
		{"no partition for value", 1526, telemetry.ClassNotFound},
		{"packet too large", 1153, telemetry.ClassConfigInvalid},
		{"server has gone away", 2006, telemetry.ClassDBConnection},
		{"lost connection during query", 2013, telemetry.ClassDBConnection},
		// A number with no bucket stays an honest unknown.
		{"syntax error has no bucket", 1064, telemetry.ClassUnknown},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			db, err := sql.Open("mysql", fakemysql.RefuseEverything(t, c.number))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()

			_, err = New(db, 1).InsertBatch([]event.Event{{
				Schema: "shop", Table: "orders", PKValues: "1",
				EventType: event.EventInsert,
				Timestamp: time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC),
				RowAfter:  map[string]any{"id": 1},
			}})
			if err == nil {
				t.Fatal("setup: the refused INSERT must fail")
			}
			// Setup proof: this is the driver's own error carrying the number,
			// wrapped by the real failure path.
			var drv *mysql.MySQLError
			if !errors.As(err, &drv) || drv.Number != c.number {
				t.Fatalf("setup: want the driver's *MySQLError %d in the chain, got %T: %v", c.number, err, err)
			}
			if !strings.HasPrefix(err.Error(), "batch INSERT of 1 events failed: ") {
				t.Fatalf("setup: the error did not come through InsertBatch's wrapping: %v", err)
			}

			got := telemetry.ClassifyError(fmt.Errorf("stream: %w", err))
			if got != c.want {
				t.Errorf("ClassifyError(%d) = %q, want %q", c.number, got, c.want)
			}
			for _, secret := range fakemysql.SecretFragments {
				if strings.Contains(got, secret) {
					t.Errorf("class %q carries server message text %q", got, secret)
				}
			}
		})
	}
}

// stalledWriteError returns the error the real write path produces when the
// index accepts the connection and then never answers.
func stalledWriteError(t *testing.T) error {
	t.Helper()
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

	prev := WriteTimeout
	WriteTimeout = 250 * time.Millisecond
	t.Cleanup(func() { WriteTimeout = prev })

	db, err := sql.Open("mysql", "root:x@tcp("+ln.Addr().String()+")/db?timeout=30s")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	_, err = New(db, 1).InsertBatch(nil)
	if err == nil {
		t.Fatal("setup: the stalled write must fail")
	}
	return err
}

// TestWriteDeadline_classAndIdentity pins the two things #1630 must not move.
// The class the write deadline reports stays db_connection (no class in the
// closed set separates "the index is slow" from "the index is unreachable"),
// and the error keeps matching both sentinels a supervisor and the older
// callers key on. Must NOT call t.Parallel(): it mutates WriteTimeout.
func TestWriteDeadline_classAndIdentity(t *testing.T) {
	err := stalledWriteError(t)

	if !errors.Is(err, ErrWriteDeadline) {
		t.Errorf("errors.Is(err, ErrWriteDeadline) = false; the restart policy keys on it: %v", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("errors.Is(err, context.DeadlineExceeded) = false: %v", err)
	}
	wrapped := fmt.Errorf("stream: %w", fmt.Errorf("flush: %w", err))
	if !errors.Is(wrapped, ErrWriteDeadline) {
		t.Errorf("wrapping lost ErrWriteDeadline: %v", wrapped)
	}
	for _, e := range []error{err, wrapped} {
		if got := telemetry.ClassifyError(e); got != telemetry.ClassDBConnection {
			t.Errorf("ClassifyError(write deadline) = %q, want %q", got, telemetry.ClassDBConnection)
		}
	}
	// The class is declared, not inferred from the wrapped context error.
	var classed telemetry.Classed
	if !errors.As(err, &classed) {
		t.Error("the write deadline error must declare its class (telemetry.Classed)")
	}
}

// TestInsertBatch_pkTooLongIsClassed: a primary key wider than the index
// column is a property of the source table's shape, reported as
// schema_mismatch. Driven through the real InsertBatch; the refusal fires
// before any statement is sent, so no server is needed.
func TestInsertBatch_pkTooLongIsClassed(t *testing.T) {
	_, err := New(nil, 1).InsertBatch([]event.Event{{
		Schema: "shop", Table: "orders",
		PKValues:  strings.Repeat("a", event.MaxPKValuesLen+1),
		EventType: event.EventInsert,
	}})
	if err == nil {
		t.Fatal("an over-long primary key must be refused")
	}
	if !strings.Contains(err.Error(), "exceeding the 512-character limit") {
		t.Fatalf("the error did not come from the primary key guard: %v", err)
	}
	if got := telemetry.ClassifyError(fmt.Errorf("stream: %w", err)); got != telemetry.ClassSchemaMismatch {
		t.Errorf("ClassifyError = %q, want %q", got, telemetry.ClassSchemaMismatch)
	}
}

// TestInsertBatch_unencodableRowIsClassed: a row image that cannot be encoded
// is a defect on our side of the wire, reported as internal. NaN is the value
// encoding/json refuses.
func TestInsertBatch_unencodableRowIsClassed(t *testing.T) {
	for _, c := range []struct {
		name   string
		ev     event.Event
		prefix string
	}{
		{"row_after", event.Event{Schema: "shop", Table: "orders", PKValues: "1", EventType: event.EventInsert,
			RowAfter: map[string]any{"v": math.NaN()}}, "marshal row_after for shop.orders: "},
		{"row_before", event.Event{Schema: "shop", Table: "orders", PKValues: "1", EventType: event.EventDelete,
			RowBefore: map[string]any{"v": math.NaN()}}, "marshal row_before for shop.orders: "},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, err := New(nil, 1).InsertBatch([]event.Event{c.ev})
			if err == nil {
				t.Fatal("an unencodable row must be refused")
			}
			if !strings.HasPrefix(err.Error(), c.prefix) {
				t.Fatalf("message changed: %v", err)
			}
			if got := telemetry.ClassifyError(fmt.Errorf("stream: %w", err)); got != telemetry.ClassInternal {
				t.Errorf("ClassifyError = %q, want %q", got, telemetry.ClassInternal)
			}
		})
	}
}
