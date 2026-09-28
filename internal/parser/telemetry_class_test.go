package parser

import (
	"context"
	"errors"
	"fmt"
	"testing"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/dbtrail/dbtrail/internal/telemetry"
)

// TestHandleRows_driftErrorIsSchemaDriftError: the #700 hard error is a
// *SchemaDriftError so usage telemetry can report it as schema_mismatch
// instead of unknown (#1503). Driven through the real handleRows path, not a
// hand-built value, so a refactor that goes back to fmt.Errorf fails here.
func TestHandleRows_driftErrorIsSchemaDriftError(t *testing.T) {
	_, err := runHandleRows(t, driftRowsEvent([]string{"id", "total"}))
	if err == nil {
		t.Fatal("post-snapshot drift must hard-error")
	}
	var de *SchemaDriftError
	if !errors.As(err, &de) {
		t.Fatalf("drift error is %T, want *SchemaDriftError", err)
	}
	if got := telemetry.ClassifyError(err); got != telemetry.ClassSchemaMismatch {
		t.Errorf("ClassifyError(drift) = %q, want %q", got, telemetry.ClassSchemaMismatch)
	}
}

// TestSchemaGapTrackerErrIsClassed: the file-mode "schema gap" verdict is
// typed like its drift sibling, so `bintrail index` reports it as
// schema_mismatch instead of unknown (#1503 review).
func TestSchemaGapTrackerErrIsClassed(t *testing.T) {
	if err := (&schemaGapTracker{}).err("binlog.000001"); err != nil {
		t.Fatalf("no gaps must be nil, got %v", err)
	}
	g := &schemaGapTracker{}
	g.record("shop.orders at binlog.000001:4")
	err := g.err("binlog.000001")
	var gap *SchemaGapError
	if !errors.As(err, &gap) {
		t.Fatalf("got %T (%v), want *SchemaGapError", err, err)
	}
	if got := telemetry.ClassifyError(err); got != telemetry.ClassSchemaMismatch {
		t.Errorf("ClassifyError = %q, want %q", got, telemetry.ClassSchemaMismatch)
	}
}

// wraparoundError drives the real StreamParser.Run into the #845 guard.
func wraparoundError(t *testing.T) error {
	t.Helper()
	sp := NewStreamParser(nil, Filters{}, nil)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 16)
	ctx, cancel := context.WithCancel(context.Background())
	feedThenCancel(t, streamer, cancel,
		makeRotate("binlog.000009"),
		makeGTIDEvent(1),
		makeQueryEvent("BEGIN"),
		makeXIDEvent(4_294_967_290),
		makeGTIDEvent(2),
		makeQueryEvent("BEGIN"),
		makeXIDEvent(1000),
	)
	err := sp.Run(ctx, streamer, out)
	if err == nil {
		t.Fatal("setup: expected the wraparound guard to fire")
	}
	return err
}

// TestStreamParser_wraparoundIsClassed: position-mode capture that cannot
// continue past a 4GiB binlog file is fixed by changing how the stream is
// started (GTID mode), so usage telemetry reports config_invalid instead of
// unknown (#1630). The message the operator reads is pinned byte for byte.
func TestStreamParser_wraparoundIsClassed(t *testing.T) {
	err := wraparoundError(t)
	var we *PositionWraparoundError
	if !errors.As(err, &we) {
		t.Fatalf("wraparound error is %T, want *PositionWraparoundError", err)
	}
	const want = `binlog position wraparound detected in "binlog.000009": position went from 4294967290 back to 1000 with no intervening ` +
		`file rotation — this file has grown past the 4GiB wire-format limit for a single binlog position ` +
		`(typically one oversized transaction delaying rotation), and the source is truncating end_log_pos ` +
		`on the wire; position-mode streaming cannot safely continue past this point. Switch to GTID mode, ` +
		`which has no positional limit: restart with --start-gtid using the source's current executed ` +
		`GTID set (SELECT @@GLOBAL.gtid_executed)`
	if err.Error() != want {
		t.Errorf("operator message changed:\n got: %s\nwant: %s", err.Error(), want)
	}
	if got := telemetry.ClassifyError(fmt.Errorf("stream: %w", err)); got != telemetry.ClassConfigInvalid {
		t.Errorf("ClassifyError = %q, want %q", got, telemetry.ClassConfigInvalid)
	}
}

// TestStreamParser_unestablishedPositionIsClassed: the #1117 belt fires only
// when a producer failed to give a row event a real position, which is a
// defect on our side, so usage telemetry reports internal (#1630).
func TestStreamParser_unestablishedPositionIsClassed(t *testing.T) {
	sp := NewStreamParser(makeOrdersResolver(), Filters{}, nil)
	streamer := replication.NewBinlogStreamer()
	out := make(chan Event, 16)
	zeroPosRows := &replication.BinlogEvent{
		Header: &replication.EventHeader{
			EventType: replication.WRITE_ROWS_EVENTv1,
			LogPos:    0,
			EventSize: 53,
		},
		Event: &replication.RowsEvent{
			Table: &replication.TableMapEvent{
				Schema:      []byte("shop"),
				Table:       []byte("orders"),
				ColumnCount: 2,
			},
			Rows: [][]any{{int64(1), int64(10)}},
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	feedThenCancel(t, streamer, cancel, makeRotate("mariadb-bin.000002"), zeroPosRows)

	err := sp.Run(ctx, streamer, out)
	if err == nil {
		t.Fatal("expected the fail-loud belt to fire")
	}
	const want = "row event at mariadb-bin.000002 has end position 0 smaller than its size 53 (shop.orders) — the binlog position for " +
		"this event could not be established (MariaDB 11.4+ writes cache-buffered events with end_log_pos=0; " +
		"the zero-LogPos fill should have replaced it before this point); refusing to index the row with an " +
		"underflowed start_pos, which the resume-time dedup would treat as beyond every checkpoint"
	if err.Error() != want {
		t.Errorf("operator message changed:\n got: %s\nwant: %s", err.Error(), want)
	}
	if got := telemetry.ClassifyError(fmt.Errorf("stream: %w", err)); got != telemetry.ClassInternal {
		t.Errorf("ClassifyError = %q, want %q", got, telemetry.ClassInternal)
	}
}

// TestReplicationError_sourceSideNumbersShareTheIndexSideClasses: the class
// names the kind of failure, not the server that had it. A number arriving
// from the SOURCE through the replication client lands in the same class the
// index side reports for it (#1630).
func TestReplicationError_sourceSideNumbersShareTheIndexSideClasses(t *testing.T) {
	for _, c := range []struct {
		code uint16
		want string
	}{
		{1040, telemetry.ClassDBConnection},
		{1205, telemetry.ClassDBConnection},
		{1021, telemetry.ClassStorageIO},
		{1153, telemetry.ClassConfigInvalid},
		{1064, telemetry.ClassUnknown},
	} {
		err := fmt.Errorf("stream: %w", WrapReplicationError(&gomysql.MyError{Code: c.code, Message: "on db.internal"}))
		if got := telemetry.ClassifyError(err); got != c.want {
			t.Errorf("ClassifyError(source %d) = %q, want %q", c.code, got, c.want)
		}
	}
}
